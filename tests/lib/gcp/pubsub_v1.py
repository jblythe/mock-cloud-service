from tests.lib.utils import patch_libs, stop_patcher, get_libs_to_patch, get_method_info
from types import SimpleNamespace
import base64


class PublisherClient:

    def __init__(self, unittest=True):
        self.unittest = unittest

    def topic_path(self, project, topic):
        path = 'projects/{}/topics/{}'
        return path.format(project, topic)

    def publish(self, topic, data, **kwargs):
        if self.unittest:
            pass

        else:

            try:
                topic_vars = get_method_info(topic)
                func = topic_vars["function"]

                libs_to_patch = get_libs_to_patch(topic, **topic_vars['kwargs'])

                patched_libs = patch_libs(topic, libs_to_patch)

                message, context = generate_event_data(data, **kwargs)

                func(message, SimpleNamespace(**context))

            except Exception as e:
                print(e)

            finally:
                stop_patcher(topic, patched_libs)


def generate_event_data(data, **kwargs):
    message = {
        "data": base64.b64encode(data),
        "attributes": kwargs
    }
    context = {
        "event_id": 12345678,
        "timestamp": "2020-08-20T21:26:30.600Z"
    }
    return message, context
