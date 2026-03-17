"""Module providing a pretty-printer for JSON cassette_data.

:author: Jeremy Thurgood
:maintainer: Ian Cordasco
"""
import json

from betamax.serializers import JSONSerializer


class PrettyJSONSerializer(JSONSerializer):
    name = 'prettyjson'

    def serialize(self, cassette_data):
        return json.dumps(
            cassette_data,
            sort_keys=True,
            indent=2,
            separators=(',', ': '),
        )
