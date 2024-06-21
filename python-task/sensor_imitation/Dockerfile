FROM python:3

WORKDIR /appSender

RUN pip install 'paho-mqtt<2.0.0'

COPY . /appSender/

CMD ["python","sensor_imitation.py"]