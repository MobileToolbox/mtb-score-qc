
FROM python:3.9-slim-buster

RUN apt-get update
RUN apt-get install -y --no-install-recommends git

WORKDIR /app
COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY create_summary.py ./create_summary.py

ENTRYPOINT [ "python", "./create_summary.py" ]

