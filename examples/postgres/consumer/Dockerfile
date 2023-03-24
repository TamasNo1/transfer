FROM python:3

RUN pip install kafka-python psycopg2-binary

COPY consumer.py ./consumer.py

CMD ["python", "consumer.py"]