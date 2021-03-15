import base64
import copy
import json

import pytest
import main
from unittest.mock import patch
from tests.lib.gcp import firestore, pubsub_v1, bigquery


@pytest.mark.parametrize(
    "directory, event_filename, expected_filename, config, event_data_tag", [
        ("process_event", "1_log.json", "1_expected.json", "InboundConverter", "attributes"),
    ]
)
def test_process_event(
    directory, create_event_from_json, event_data_tag,
    event_filename, generate_context,
    generate_expected, expected_filename, config, monkeypatch
):
    with patch('main.firestore.Client') as mock_firestore:
        with patch('main.pubsub_v1.PublisherClient') as mock_pubsub_v1:
            with patch("main.CONFIG", config):
                # we have to patch the return of apply_config_rules as it generates a real time datetime and we dont
                # have a way of mocking that part that deep. We have to rely on the results of the individual rule
                # testing to verify this part, as well as a test of apply_config_rules itself to verify
                with patch("main.apply_config_rules") as config_rules:
                    config_rules.return_value = generate_expected
                    monkeypatch.setenv("IBC_STORE_UNMATCHED", "1")
                    mock_firestore.return_value = firestore.Client()
                    mock_pubsub_v1.return_value = pubsub_v1.PublisherClient()

                    assert main.process_event(create_event_from_json, generate_context) == generate_expected


@pytest.mark.parametrize(
    "directory, event_filename, expected_filename, config, event_data_tag", [
        ("handle_failed_event", "failed_event_1.json", "1_expected.json", "WriteInboundConverterErrors", "data"),
    ]
)
def test_handle_failed_event(directory, create_event_from_json, event_filename,
                             generate_context, generate_expected, expected_filename,
                             config, event_data_tag):
    with patch('main.bigquery.Client') as mock_bigquery:
        with patch.object(mock_bigquery, 'insert_rows_json', None):
            with patch("main.CONFIG", config):
                mock_bigquery.return_value = bigquery.Client()
                data = base64.b64decode(copy.deepcopy(create_event_from_json)['data'].decode('UTF-8')).decode('UTF-8')
                main.handle_failed_event(create_event_from_json, generate_context)

