FROM python:3.9

RUN apt-install wget
WORKDIR /app
COPY ingest_data.py ingest_data.py
COPY requirements.txt ./
RUN pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org -r requirements.txt

ENTRYPOINT [ "python", "ingest_data.py" ]