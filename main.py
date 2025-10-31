import os
import requests
from urllib.parse import urljoin
import pandas as pd
import json
import re
import math
import threading
import time
from io import StringIO
from flask import Flask, jsonify

# === FLASK APP ===
app = Flask(__name__)

# === LOAD ENV ===
# We keep load_dotenv here to load from a .env file during local testing/build.
from dotenv import load_dotenv
load_dotenv()

API_TOKEN = os.getenv('API_TOKEN')
BASE_URL = os.getenv('OPENPROJECT_URL')
PROJECT_ID = os.getenv('GCP_PROJECT_ID')
DATASET_ID = os.getenv('BIGQUERY_DATASET_ID')
GCS_BUCKET = os.getenv('GCS_BUCKET')
PAGE_SIZE = int(os.getenv('PAGE_SIZE', 5000))
TIMEOUT_SECS = int(os.getenv('TIMEOUT_SECS', 120))

# We remove the failing environment check here.

# === AUTO-DETECT: LOCAL OR CLOUD ===
IS_LOCAL = os.getenv('LOCAL_TEST', 'false').lower() == 'true'

# === GLOBAL CLIENT HANDLES (Initialized later) ===
storage_client = None
bq_client = None
sm_imported = False # Flag to track if GCP modules were imported

def log(m):
    print(f"[LOG] {m}", flush=True)

# === STARTUP LOGS ===
log("Starting OpenProject → BigQuery Sync Service...")
log(f"Running in {'LOCAL' if IS_LOCAL else 'CLOUD'} mode")
log(f"Target: {BASE_URL} → gs://{GCS_BUCKET} → {PROJECT_ID}.{DATASET_ID}")

# === RETRY SESSION ===
session = requests.Session()
session.auth = ("apikey", API_TOKEN)
session.headers.update({"Accept": "application/hal+json"})

# ----------------------------------------------------
# 🌟 NEW FUNCTION: Client Initialization
# This runs only when a request hits the '/' endpoint.
# ----------------------------------------------------
def _init_clients():
    global storage_client, bq_client, sm_imported

    # 1. Check if required environment variables are present
    if not all([API_TOKEN, BASE_URL, PROJECT_ID, DATASET_ID, GCS_BUCKET]):
        raise EnvironmentError("Missing required env vars. Check Cloud Run configuration.")

    # 2. Skip client init if already done
    if storage_client and bq_client:
        return

    if not IS_LOCAL:
        log("Initializing GCP clients...")
        # Only import GCP modules if we need them, using the flag to avoid repeated import
        if not sm_imported:
            from google.cloud import storage, bigquery, secretmanager
            from google.oauth2 import service_account
            sm_imported = True

        try:
            # Access the secret key for service account credentials
            client = secretmanager.SecretManagerServiceClient()
            name = f"projects/{PROJECT_ID}/secrets/openproject-service-account-key/versions/latest"
            response = client.access_secret_version(name=name)
            secret_value = response.payload.data.decode('UTF-8')
            credentials = service_account.Credentials.from_service_account_info(json.loads(secret_value))

            # Initialize global client objects
            storage_client = storage.Client(credentials=credentials, project=PROJECT_ID)
            bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
            log("GCP clients initialized successfully.")
        except Exception as e:
            # Raising a RuntimeError will stop the sync thread.
            raise RuntimeError(f"Failed to init GCP clients (Check permissions): {e}")
    else:
        log("Running in local mode, skipping GCP client init.")

# === RETRY DECORATOR ===
def retry(max_attempts=3, delay=10, backoff=2):
    def decorator(func):
        def wrapper(*args, **kwargs):
            attempt = 0
            while attempt < max_attempts:
                try:
                    return func(*args, **kwargs)
                except (requests.exceptions.ConnectTimeout,
                        requests.exceptions.ReadTimeout,
                        requests.exceptions.ConnectionError,
                        requests.exceptions.HTTPError) as e:
                    attempt += 1
                    wait = delay * (backoff ** (attempt - 1))
                    log(f"[RETRY] {func.__name__} failed: {e}. Retrying in {wait}s (attempt {attempt}/{max_attempts})")
                    time.sleep(wait)
            log(f"[ERROR] {func.__name__} failed after {max_attempts} attempts")
            return None
        return wrapper
    return decorator

# === HELPER: Extract ID from href ===
def id_from_href(href, resource):
    if not href: return None
    m = re.search(rf"/api/v3/{re.escape(resource)}/(\d+)", href)
    return m.group(1) if m else None

# === TIME PARSING ===
def parse_iso8601_duration_to_hours(dur):
    if not dur or not dur.startswith("P"): return None
    try:
        t = dur.split("T", 1)[1] if "T" in dur else ""
        h = 0.0
        mh = re.search(r"(\d+(?:\.\d+)?)H", t)
        mm = re.search(r"(\d+(?:\.\d+)?)M", t)
        if mh: h += float(mh.group(1))
        if mm: h += float(mm.group(1)) / 60.0
        return round(h, 2)
    except: return None

def format_hours_hm(h):
    if h is None: return ""
    mins = int(round(h * 60))
    return f"{mins // 60}h {mins % 60}m"

# === FETCH ALL WORK PACKAGES ===
@retry(max_attempts=3, delay=10, backoff=2)
def fetch_all_work_packages_for_project(project_id):
    base = urljoin(BASE_URL, "/api/v3/work_packages")
    filters = [{"project": {"operator": "=", "values": [str(project_id)]}}]
    sort_by = [["id", "asc"]]
    seen = set()
    total_fetched = 0
    total_expected = None
    page_size = PAGE_SIZE

    qp = f"filters={requests.utils.quote(json.dumps(filters))}&sortBy={requests.utils.quote(json.dumps(sort_by))}&pageSize=1&offset=1"
    url = f"{base}?{qp}"
    r = session.get(url, timeout=TIMEOUT_SECS)
    r.raise_for_status()
    data = r.json()
    total_expected = data.get("total", 0)
    if total_expected == 0:
        return []

    total_pages = math.ceil(total_expected / page_size)
    log(f"Project {project_id}: {total_expected} WPs → {total_pages} page(s)")

    offset = 1
    while total_fetched < total_expected:
        qp = (
            f"filters={requests.utils.quote(json.dumps(filters))}"
            f"&sortBy={requests.utils.quote(json.dumps(sort_by))}"
            f"&pageSize={page_size}&offset={offset}"
        )
        url = f"{base}?{qp}"

        r = session.get(url, timeout=TIMEOUT_SECS)
        r.raise_for_status()

        data = r.json()
        items = data.get("_embedded", {}).get("elements", [])
        for it in items:
            wid = it.get("id")
            if wid and wid not in seen:
                seen.add(wid)
                total_fetched += 1
                yield it

        current_page = (offset - 1) // page_size + 1
        log(f"  → Page {current_page}/{total_pages} | Fetched {len(items)} | Total: {total_fetched}/{total_expected}")
        offset += len(items) if items else page_size

# === TASK CATEGORY HELPERS ===
_category_name_cache = {}
_schema_cf_map_cache = {}
_cf_option_name_cache = {}

@retry(max_attempts=2)
def fetch_category_name_by_href(href):
    cid = id_from_href(href, "categories")
    if not cid or cid in _category_name_cache: return _category_name_cache.get(cid)
    url = urljoin(BASE_URL, href)
    r = session.get(url, timeout=TIMEOUT_SECS)
    if r.status_code >= 400: return None
    name = r.json().get("name")
    _category_name_cache[cid] = name
    return name

@retry(max_attempts=2)
def fetch_schema_field_names(project_id, type_id):
    key = (str(project_id), str(type_id))
    if key in _schema_cf_map_cache: return _schema_cf_map_cache[key]
    url = urljoin(BASE_URL, f"/api/v3/work_packages/schemas/{project_id}-{type_id}")
    r = session.get(url, timeout=TIMEOUT_SECS)
    if r.status_code >= 400:
        _schema_cf_map_cache[key] = {}
        return {}
    data = r.json()
    props = data.get("_embedded", {}).get("schema", data) if "_embedded" in data else data
    cf_map = {}
    for k, v in props.items():
        if k.startswith("customField"):
            nm = (v.get("name") or v.get("title") or "").strip()
            if nm: cf_map[k] = nm
    _schema_cf_map_cache[key] = cf_map
    return cf_map

def resolve_custom_option_name(opt):
    if isinstance(opt, dict):
        title = opt.get("title")
        if title: return title
        href = opt.get("href")
        oid = id_from_href(href, "custom_options")
        if not oid: return None
        if oid in _cf_option_name_cache: return _cf_option_name_cache[oid]
        url = urljoin(BASE_URL, href)
        r = session.get(url, timeout=TIMEOUT_SECS)
        if r.status_code >= 400: return None
        name = r.json().get("name") or r.json().get("title")
        _cf_option_name_cache[oid] = name
        return name
    return opt

def get_task_category_for_wp(wp, project_id):
    link = wp.get("_links", {}).get("category", {})
    if link:
        title = link.get("title")
        if title: return title
        href = link.get("href")
        if href:
            name = fetch_category_name_by_href(href)
            if name: return name

    type_href = wp.get("_links", {}).get("type", {}).get("href")
    type_id = id_from_href(type_href, "types")
    if type_id:
        cf_map = fetch_schema_field_names(project_id, type_id)
        task_cat_key = next((k for k, v in cf_map.items() if v.lower() == "task category"), None)
        if task_cat_key and task_cat_key in wp:
            val = wp.get(task_cat_key)
            if isinstance(val, list):
                names = [resolve_custom_option_name(x) for x in val if resolve_custom_option_name(x)]
                return ", ".join(names) if names else "No Category"
            return resolve_custom_option_name(val) or "No Category"
    return "No Category"

# === UPLOAD CSV TO GCS ===
def upload_csv_to_gcs(df, bucket_name, blob_name):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    if IS_LOCAL:
        os.makedirs("output", exist_ok=True)
        path = f"output/{blob_name}"
        with open(path, 'w', encoding='utf-8') as f:
            f.write(csv_buffer.getvalue())
        log(f"[LOCAL GCS] Saved {blob_name}")
        return f"file://{path}"
    else:
        # Use the globally defined storage_client
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")
        log(f"Uploaded gs://{bucket_name}/{blob_name}")
        return f"gs://{bucket_name}/{blob_name}"

# === LOAD FROM GCS TO BIGQUERY ===
def load_gcs_to_bigquery(gcs_uri, table_name):
    if IS_LOCAL:
        log(f"[LOCAL BQ] Would load {gcs_uri} to {DATASET_ID}.{table_name}")
        return

    # Use the globally defined bq_client
    dataset_ref = bq_client.dataset(DATASET_ID)
    table_ref = dataset_ref.table(table_name)

    try:
        bq_client.get_dataset(dataset_ref)
    except:
        bq_client.create_dataset(dataset_ref)
        log(f"Created dataset {DATASET_ID}")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        autodetect=False,
        schema=[
            bigquery.SchemaField("ID", "INTEGER"),
            bigquery.SchemaField("SUBJECT", "STRING"),
            bigquery.SchemaField("TYPE", "STRING"),
            bigquery.SchemaField("STATUS", "STRING"),
            bigquery.SchemaField("ASSIGNEE", "STRING"),
            bigquery.SchemaField("PRIORITY", "STRING"),
            bigquery.SchemaField("CIRCLE", "STRING"),
            bigquery.SchemaField("ESTIMATED_TIME", "STRING"),
            bigquery.SchemaField("SPENT_TIME", "STRING"),
            bigquery.SchemaField("DATE_CREATED", "DATE"),
            bigquery.SchemaField("DATE_UPDATED", "DATE"),
            bigquery.SchemaField("START_DATE", "DATE"),
            bigquery.SchemaField("FINISH_DATE", "DATE"),
            bigquery.SchemaField("ACCOUNTABLE", "STRING"),
            bigquery.SchemaField("TASK_CATEGORY", "STRING"),
            bigquery.SchemaField("RECURRING", "BOOLEAN"),
        ]
    )
    job = bq_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    job.result()
    log(f"Loaded {gcs_uri} to {DATASET_ID}.{table_name}")

# === MAIN SYNC ===
def openproject_to_bigquery():
    # 🌟 NEW: Initialize clients at the start of the sync job
    try:
        _init_clients()
    except Exception as e:
        log(f"Sync failed at initialization: {e}")
        return

    url = urljoin(BASE_URL + "/", "api/v3/projects?offset=1&pageSize=100")
    projects = []
    while url:
        try:
            r = session.get(url, timeout=TIMEOUT_SECS)
            r.raise_for_status()
            data = r.json()
            projects.extend(data.get("_embedded", {}).get("elements", []))
            total = data.get("total", 0)
            count = data.get("count", 0)
            offset = data.get("offset", 0)
            if count == 0 or offset + count >= total: break
            url = f"{BASE_URL}/api/v3/projects?offset={offset + count}&pageSize=100"
        except Exception as e:
            log(f"[ERROR] Failed to fetch projects page: {e}")
            break

    log(f"Found {len(projects)} projects")

    for project in projects:
        project_id = project['id']
        project_name = project['name']
        safe_name = re.sub(r'[^\w]', '_', project_name).lower()
        csv_filename = f"{safe_name}.csv"
        table_name = f"{safe_name}_tasks"

        if project_name == "Deep Funding Circles":
            log(f"SKIPPED: '{project_name}'")
            continue

        log(f"Fetching: {project_name}")
        work_packages = list(fetch_all_work_packages_for_project(project_id))
        if not work_packages:
            log(f"No data for {project_name}")
            continue

        rows = []
        for wp in work_packages:
            spent_h = parse_iso8601_duration_to_hours(wp.get("spentTime"))
            row = {
                'ID': wp.get('id'),
                'SUBJECT': wp.get('subject'),
                'TYPE': wp.get('_links', {}).get('type', {}).get('title', 'Unknown'),
                'STATUS': wp.get('_links', {}).get('status', {}).get('title', 'Unknown'),
                'ASSIGNEE': wp.get('_links', {}).get('assignee', {}).get('title', 'None'),
                'PRIORITY': wp.get('_links', {}).get('priority', {}).get('title', 'Unknown'),
                'CIRCLE': wp.get('_links', {}).get('project', {}).get('title', 'No Project'),
                'ESTIMATED_TIME': wp.get('customField6', 'N/A'),
                'SPENT_TIME': format_hours_hm(spent_h),
                'DATE_CREATED': wp.get('createdAt'),
                'DATE_UPDATED': wp.get('updatedAt'),
                'START_DATE': wp.get('startDate'),
                'FINISH_DATE': wp.get('dueDate'),
                'ACCOUNTABLE': wp.get('_links', {}).get('responsible', {}).get('title', 'None'),
                'TASK_CATEGORY': get_task_category_for_wp(wp, project_id),
                'RECURRING': str(wp.get('customField10', False)) == "True",
            }
            rows.append(row)

        df = pd.DataFrame(rows)
        for col in ['DATE_CREATED', 'DATE_UPDATED', 'START_DATE', 'FINISH_DATE']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%-m-%d')

        try:
            gcs_uri = upload_csv_to_gcs(df, GCS_BUCKET, csv_filename)
            load_gcs_to_bigquery(gcs_uri, table_name)
        except Exception as e:
            log(f"[ERROR] Failed to process {project_name}: {e}")

    log("Sync completed")

# === HTTP ENDPOINTS ===
@app.route('/', methods=['GET'])
def trigger():
    threading.Thread(target=openproject_to_bigquery).start()
    return jsonify({"status": "Sync started"}), 200

@app.route('/health')
def health():
    return "OK", 200

@app.route('/ready')
def ready():
    return "OK", 200

# === NO SERVER START HERE — DOCKER DOES IT ===
