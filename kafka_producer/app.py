from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json

app = Flask(__name__)
kafka_bootstrap_server = '192.168.0.101:9092'
topic_name = 'hentai2read'
producer_instance = KafkaProducer(bootstrap_servers=kafka_bootstrap_server)


def produce_message_sync(producer, message):
    future = producer.send(topic_name, message)
    result = future.get(timeout=60)
    producer.flush()


@app.route('/', methods=['POST'])
def kafka_producer():
    data = request.get_json()
    json_message = json.dumps(data)
    kafka_message = json_message.encode()
    produce_message_sync(producer=producer_instance, message=kafka_message)
    print('Sent message to Kafka Broker')
    response_message = jsonify({'message': 'Message processed', 'status': 'Pass'})
    return response_message


if __name__ == '__main__':
    app.run(debug=True, host='192.168.0.100', port=9090)
