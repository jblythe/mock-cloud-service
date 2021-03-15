"""
conftest.py is a module used by pytest as a helper for fixtures and other processing logic. If the file exists, PyTest
knows to import it and use those methods internally. The trickiest part is that there isnt a direct way to call the
helpers from the testing files, you simply pass variables in with the parameters of the test. For instance, we have
a handful of functions (generate_expected, generate_input, generate_config, etc) that looks exactly the same but take
slightly different parameters. In the test, we pass the raw object to the test along with the expected parameters
and PyTest knows how to handle this without calling the function directly.
"""

import json
import base64
from types import SimpleNamespace
import os
import pytest
from tests.lib.gcp import firestore


@pytest.fixture()
def set_ibc_env_vars(monkeypatch):
    monkeypatch.setenv("ENV", "TESTING")
    monkeypatch.setenv("CONFIG", "InboundConverter")


@pytest.fixture()
def get_clients():
    """
    Function to return a test firestore client in the way we expect during processing
    :return: (dict) A test client config
    """
    return {
        "fs_client": firestore.Client()
    }


@pytest.fixture
def generate_context():
    """
    Function to return a test representation of the context provided to a cloud functions
    :return: (dict) A test context
    """
    context = {
        "event_id": 12345678,
        "timestamp": "2020-08-20T21:26:30.600Z"
    }
    return SimpleNamespace(**context)


@pytest.fixture
def create_kafka_event_from_json(directory, event_filename):
    """
    Function to return a json representation of a kafka event
    :param directory: (str) the directory the kafka event is located in
    :param event_filename: (str) the name of the kafka event
    :return: (dict) A json representation of the kafka event
    """
    event = read_resource_file_as_json(directory, event_filename)
    return {"attributes": event}


@pytest.fixture
def create_event_from_json(directory, event_filename, event_data_tag):
    """
    Function to return a json representation of a pubsub event
    :param directory: (str) the directory the pubsub event is located in
    :param event_filename: (str) the name of the pubsub event
    :return: (dict) A json representation of the pubsub event
    """
    event = read_resource_file_as_json(directory, event_filename)
    if event_data_tag == "attributes":
        return {"attributes": event}
    else:
        message_bytes = json.dumps(event).encode('utf-8')
        return {"data": base64.b64encode(message_bytes)}


@pytest.fixture
def generate_expected(directory, expected_filename):
    """
    Function to return a json representation of an expected output
    :param directory: (str) the directory the expected output is located in
    :param expected_filename: (str) the name of the expected output
    :return: (dict) A json representation of the expected output
    """
    return read_resource_file_as_json(directory, expected_filename)


@pytest.fixture
def generate_input(directory, input_filename):
    """
    Function to return a json representation of an input
    :param directory: (str) the directory the input is located in
    :param input_filename: (str) the name of the input
    :return: (dict) A json representation of the input
    """
    return read_resource_file_as_json(directory, input_filename)


@pytest.fixture
def generate_config(directory, config_filename):
    """
    Function to return a json representation of a config
    :param directory: (str) the directory the config is located in
    :param config_filename: (str) the name of the config
    :return: (dict) A json representation of the config
    """
    return read_resource_file_as_json(directory, config_filename)


def read_resource_file_as_json(directory, file_name):
    """
    Function to return a json representation of a resource
    :param directory: (str) the directory the resource is located in
    :param file_name: (str) the name of the file
    :return: (dict) A json representation of the resource
    """
    return json.loads(read_resource_file(directory, file_name))


def read_resource_file(directory, file_name):
    """
    Function to return a string representation of the contents of a file
    :param directory: (str) the directory the file is located in
    :param file_name: (str) the name of the file
    :return: (str) A string representation of the file
    """
    with open(os.path.join(get_base_dir(), directory, file_name)) as file:
        data = file.read()
    return data


@pytest.fixture
def generate_html(directory, html_filename):
    """

    """
    return read_resource_file(directory, html_filename)


def get_base_dir():
    """
    Function to return the base directory of the file for reference in testing
    :return: (str) The path to the file that is being tested
    """
    my_path = os.path.abspath(os.path.dirname(__file__))
    base_dir = os.path.join(my_path, 'resources')
    return base_dir


if __name__ == '__main__':
    get_base_dir()
