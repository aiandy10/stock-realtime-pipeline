FROM apache/spark:3.5.0
WORKDIR /app
COPY stream.py .
CMD ["/opt/spark/bin/spark-submit", "/app/stream.py"]