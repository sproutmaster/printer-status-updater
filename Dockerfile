FROM python:3.11-alpine

COPY . /printer-status-updater

WORKDIR /printer-status-updater

RUN pip install -r requirements.txt

CMD ["python3", "main.py"]
