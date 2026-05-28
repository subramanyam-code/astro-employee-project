# Dockerfile for Astronomer Airflow Project
FROM quay.io/astronomer/astro-runtime:11.0.0

# Copy requirements.txt and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
