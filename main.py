"""
main.py contains the main processing logic for processing
"""
import json
import os
import base64

from google.cloud import firestore, pubsub_v1, bigquery

CONFIG = os.getenv('CONF')
DOCUMENT = "STATIC_TO_BE_MOCKED"


def my_func(event, context):
    """

    """
    bq_client = bigquery.Client()
    fs_client = firestore.Client()
    config = fs_client.collection(DOCUMENT).document(CONFIG).get().to_dict()

    data = json.loads(base64.b64decode(event['data']).decode('utf-8'))

    table = config['table']

    bq_client.insert_rows_json(table, data)


def this_other_func(event, context):
    """
    Main procesing logic to receive data from pub/sub, apply rules to data, and load to BQ
    :param event: (dict) The dictionary with data specific to this type of
         event. The `data` field contains the PubsubMessage message. The
         `attributes` field will contain custom attributes if there are any.
    :param context: (google.cloud.functions.Context) The Cloud Functions event
         metadata. The `event_id` field contains the Pub/Sub message ID. The
         `timestamp` field contains the publish time
    :return: (dict) post-processed data dictionary
    """

    fs_client = firestore.Client()
    config = fs_client.collection(DOCUMENT).document(CONFIG).get().to_dict()

    if config['data_tag'] == 'attributes':
        data = json.loads(json.dumps(event['attributes']))
    elif config['data_tag'] == 'data':
        data = json.loads(base64.b64decode(event['data']).decode('utf-8'))
    else:
        raise Exception('No Data tag found in config')

    messages = []
    if type(data) == list:
        messages = data
    else:
        messages.append(data)

    publish_list_to_pubsub(output_list=messages, config=config)

    return messages


def publish_list_to_pubsub(output_list, config):
    """
    A method to take the list of messages and check if there are any postprocessing steps that need to occur. If so,
    we process the messages to a post-processing topic. If not, we process to the next topic in the natural process.
    :param output_list: (list) A list of the messages that have been prepared for the next topic
    :param config: (dict) Main processing configuration
    :return: (dict) A dictionary of topics and their respective processing blocks
    """

    if output_list:
        publisher = pubsub_v1.PublisherClient()

        data = json.dumps(output_list).encode("utf-8")
        # When you publish a message, the client returns a future.
        topic_path = publisher.topic_path(config['project'], config['topic'])
        publisher.publish(topic_path, data)

    return output_list
