FROM python:3.9

RUN apt-get install wget
WORKDIR /app
COPY requirements.txt ./
COPY ingest_data.py ingest_data.py
RUN pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org -r requirements.txt

ENTRYPOINT ["python", "ingest_data.py"]
