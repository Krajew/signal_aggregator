FROM python:3

WORKDIR /app/

RUN pip install 'paho-mqtt<2.0.0' psycopg2 

COPY . /app/

CMD ["python","signal_aggregator.py","10","5"]

