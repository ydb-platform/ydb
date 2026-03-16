from .base import BaseSerializer

import json
import os


class JSONSerializer(BaseSerializer):
    # Serializes and deserializes a cassette to JSON
    name = 'json'
    stored_as_binary = False

    @staticmethod
    def generate_cassette_name(cassette_library_dir, cassette_name):
        return os.path.join(cassette_library_dir,
                            '{0}.{1}'.format(cassette_name, 'json'))

    def serialize(self, cassette_data):
        return json.dumps(cassette_data)

    def deserialize(self, cassette_data):
        try:
            deserialized_data = json.loads(cassette_data)
        except ValueError:
            deserialized_data = {}

        return deserialized_data
