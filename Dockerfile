# Gunakan image Python resmi
FROM python:3.11-slim

# Install system dependencies untuk psycopg2
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements dan install dependencies Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy seluruh kode aplikasi
COPY . .

# Jalankan aplikasi dengan gunicorn (sesuai kebutuhan)
CMD ["gunicorn", "app:app", "--bind", "0.0.0.0:5000", "--worker-class", "eventlet", "--workers", "1"]
