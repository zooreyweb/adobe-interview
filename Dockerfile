FROM python:3.6.5
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
ENTRYPOINT ["python3","./data_preparation.py"]