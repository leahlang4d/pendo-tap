FROM python:3.7.3-alpine

RUN apk add --update alpine-sdk netcat-openbsd
RUN mkdir -p /app
WORKDIR /app
COPY . /app
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

CMD ["sh", "-c", "python tap_pendo.py -c /etc/config/tap.json | nc -N -l 8080"]
