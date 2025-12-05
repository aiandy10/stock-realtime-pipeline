FROM python:3.12-slim

WORKDIR /app
COPY app.py .

# Install dependencies
RUN pip install --no-cache-dir psycopg2-binary

CMD ["python", "app.py"]