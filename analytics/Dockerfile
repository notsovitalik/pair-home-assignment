FROM python:3-slim
RUN apt-get update \
    && apt-get -y install libpq-dev gcc \
    && pip install psycopg2 sqlalchemy
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt