FROM python:3.7.3-stretch

RUN mkdir -p /app
WORKDIR /app
COPY . /app
RUN pip install -e '.'

CMD ["python", "tap_pendo.py", "-c", "config/tap.json", "|", "nc", "-l", "8080"]
