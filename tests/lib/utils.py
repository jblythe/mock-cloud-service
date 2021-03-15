"""
This module is intended to house all of hte information that the mock will need to process between the topics
"""

from unittest.mock import patch

from tests.lib.gcp import firestore, bigquery, pubsub_v1
from main import my_func, this_other_func


def get_method_info(topic):

    topic_id = topic.split('/')[-1]
    topic_info = {
        "my.cool.topic": {"function": my_func, "kwargs": {"other_args": "my_fun_val"}},
        "some-other-topic": {"function": this_other_func, "kwargs": {"arg1": "val1", "arg2": "val2"}}
    }

    method = topic_info[topic_id]

    return method


def get_libs_to_patch(topic, **kwargs):

    topic_id = topic.split('/')[-1]
    topic_info = {
        "my.cool.topic": [
            {"lib": 'main.pubsub_v1.PublisherClient', "return_value": pubsub_v1.PublisherClient(unittest=False)},
            {"lib": 'main.bigquery.Client', "return_value": bigquery.Client()},
            {"lib": 'main.firestore.Client', "return_value": firestore.Client()},
            {"lib": 'main.CONFIG', "new": kwargs['config']}
        ],
        "some-other-topic": [
            {"lib": 'main.pubsub_v1.PublisherClient', "return_value": pubsub_v1.PublisherClient(unittest=False)},
            {"lib": 'main.bigquery.Client', "return_value": bigquery.Client()},
            {"lib": 'main.firestore.Client', "return_value": firestore.Client()},
            {"lib": 'main.CONFIG', "new": kwargs['config']}
        ]
    }

    libs_to_patch = topic_info[topic_id]

    return libs_to_patch


def patch_libs(topic, libs, patched={}):

    if not patched.get(topic):
        patched[topic] = []
    for lib in libs:
        if 'new' in lib:
            patcher = patch(lib['lib'], lib['new'])
        else:
            patcher = patch(lib['lib'])
        mock_class = patcher.start()
        if "return_value" in lib:
            mock_class.return_value = lib["return_value"]
            patched[topic].append({"lib": lib, "patcher": patcher})

    return patched


def stop_patcher(topic, patchers):
    for patcher in patchers[topic]:
        patcher['patcher'].stop()
