
FROM python:3.9-slim-bullseye

WORKDIR /airflow

COPY ./requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY ./airflow . 

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]