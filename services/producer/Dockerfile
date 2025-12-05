FROM python:3.12-slim
RUN pip install kafka-python
WORKDIR /app
COPY app.py .
CMD ["python", "app.py"]