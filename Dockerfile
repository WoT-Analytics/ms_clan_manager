FROM python:latest

WORKDIR /service

COPY ./requirements.txt /service
RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY ./service /service

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]