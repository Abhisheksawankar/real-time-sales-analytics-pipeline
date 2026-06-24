FROM ubuntu:latest
LABEL authors="sawan"

ENTRYPOINT ["top", "-b"]
FROM bitnami/spark:3.5
COPY src/consumer/ /app/consumer/
WORKDIR /app
CMD ["spark-submit", "--packages", \
     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", \
     "consumer/stream_job.py"]
