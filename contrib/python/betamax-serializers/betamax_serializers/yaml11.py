"""Module providing a serializer for YAML cassette_data.

:author: Louis Taylor
:maintainer: Louis Taylor
"""

import os

import yaml
from betamax.serializers import BaseSerializer


class YAMLSerializer(BaseSerializer):
    name = 'yaml11'

    @staticmethod
    def generate_cassette_name(cassette_library_dir, cassette_name):
        return os.path.join(cassette_library_dir,
                            '{0}.{1}'.format(cassette_name, 'yml'))

    def serialize(self, cassette_data):
        return yaml.dump(cassette_data)

    def deserialize(self, cassette_data):
        try:
            deserialized_data = yaml.load(cassette_data) or {}
        except yaml.YAMLError:
            deserialized_data = {}

        return deserialized_data
