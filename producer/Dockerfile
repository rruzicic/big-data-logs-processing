FROM python:3

WORKDIR /usr/src/app

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir boto3

COPY . .

CMD [ "python", "-u", "produce.py" ]