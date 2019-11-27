import datetime
import json

import pika
import schedule

channel = None
connection = None

run_for_duration_in_minutes = 1

run_every_seconds = 1
total_no_messages_per_given_time = 100

rabbit_mq_host = ''
exchange_name = 'toris-ens-events-dx'
routing_key = 'objCat.vehicles'


def start():
    end_time = add_seconds_to_current_time(10)
    message_published = 0
    while datetime.datetime.now() < end_time and message_published < total_no_messages_per_given_time:
        publish_vehicles()
        message_published += 1
    print('published ' + str(total_no_messages_per_given_time) + ' messsages')


def publish_vehicles():
    payload = load_message_from_file()
    publish_to_rabbitmq(exchange_name, routing_key, payload)


def add_seconds_to_current_time(seconds):
    start_time = datetime.datetime.now()
    return start_time + datetime.timedelta(seconds=seconds)


def add_minute_to_current_time(minute):
    start_time = datetime.datetime.now()
    return start_time + datetime.timedelta(minutes=minute)


def publish_to_rabbitmq(exchange_name, routing_key, payload):
    global channel
    if channel is None:
        channel = get_channel()
    channel.basic_publish(exchange=exchange_name, routing_key=routing_key, body=json.dumps(payload))


def get_channel():
    params = pika.URLParameters(rabbit_mq_host)
    params.socket_timeout = 5

    global connection
    connection = pika.BlockingConnection(params)  # Connect to CloudAMQP
    return connection.channel()


def close_channel():
    connection.close()


def load_message_from_file():
    message = ''
    with open('vehicle.json') as json_file:
        message = json.load(json_file)
    return message


schedule.every(run_every_seconds).seconds.do(start)

run_end_time = add_minute_to_current_time(run_for_duration_in_minutes)

while datetime.datetime.now() < run_end_time:
    schedule.run_pending()
close_channel()
