FROM python:3.7.3-alpine

RUN apk add --update alpine-sdk
RUN mkdir -p /app
WORKDIR /app
COPY . /app
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# CMD ["python", "tap_pendo.py", "-c", "/etc/config/tap.json", "|", "nc", "-l", "8080"]
