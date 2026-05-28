# Dockerfile for Astronomer Airflow Project
FROM astrocrpublic.azurecr.io/runtime:3.2-4

# Copy requirements.txt and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
