FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# Use PORT env var (Cloud Run sets it)
CMD ["python", "-c", "import os; from waitress import serve; from main import app; serve(app, host='0.0.0.0', port=int(os.getenv('PORT', 8080)))"]
