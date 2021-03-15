from tests.lib.gcp import pubsub_v1, bigquery, firestore
from unittest.mock import patch
import json
import os

env = "integration_test"

publisher = pubsub_v1.PublisherClient(unittest=False)
path = 'tests/resources/{}/{}'
topic = "ibc.client-activity-input"
data_tag = 'attributes'

for file in os.listdir(path.format(env, topic)):
    file_path = os.path.join(path.format(env, topic), file)
    with open(file_path) as f:
        event_data = json.loads(f.read())

    with patch('lib.rules.inbound_converter.firestore.Client') as mock_firestore:
        with patch('main.bigquery.Client') as mock_bigquery:
            with patch('main.pubsub_v1.PublisherClient') as mock_pubsub:
                with patch('main.os') as mock_os:
                    mock_bigquery.return_value = bigquery.Client()
                    mock_firestore.return_value = firestore.Client()
                    mock_pubsub.return_value = publisher

                    topic_path = publisher.topic_path(env, topic)
                    mock_os.getenv.return_value = env

                    if data_tag == 'attributes':
                        message_bytes = "nothing".encode('utf-8')
                        publisher.publish(topic_path, message_bytes, **event_data)

                    elif data_tag == 'data':
                        message_bytes = json.dumps(event_data).encode('utf-8')
                        publisher.publish(topic_path, message_bytes)
