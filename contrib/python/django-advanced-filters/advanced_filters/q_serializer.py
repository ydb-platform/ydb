"""This is a module to serializers/deserializes Django Q (query) object."""
from datetime import datetime, date
import base64
import time

from django.db.models import Q
from django.core.serializers.base import SerializationError

import simplejson as json


try:
    min_ts = time.mktime(datetime.min.timetuple())
except OverflowError:
    min_ts = 0

try:
    max_ts = time.mktime((3000,) + (0,) * 8)  # avoid OverflowError on windows
except OverflowError:
    max_ts = time.mktime((2038,) + (0,) * 8)  # limit 32bits


def dt2ts(obj):
    return time.mktime(obj.timetuple()) if isinstance(obj, date) else obj


class QSerializer:
    """
    A Q object serializer base class. Pass base64=True when initializing
    to Base-64 encode/decode the returned/passed string.

    By default the class provides loads/dumps methods that wrap around
    json serialization, but they may be easily overwritten to serialize
    into other formats (i.e XML, YAML, etc...)
    """
    b64_enabled = False

    def __init__(self, base64=False):
        if base64:
            self.b64_enabled = True

    @staticmethod
    def _is_range(qtuple):
        return qtuple[0].endswith("__range") and len(qtuple[1]) == 2

    def prepare_value(self, qtuple):
        if self._is_range(qtuple):
            qtuple[1][0] = qtuple[1][0] or min_ts
            qtuple[1][1] = qtuple[1][1] or max_ts
            qtuple[1] = (datetime.fromtimestamp(qtuple[1][0]),
                         datetime.fromtimestamp(qtuple[1][1]))
        return qtuple

    def serialize(self, q):
        """
        Serialize a Q object into a (possibly nested) dict.
        """
        children = []
        for child in q.children:
            if isinstance(child, Q):
                children.append(self.serialize(child))
            else:
                children.append(child)
        serialized = q.__dict__
        serialized['children'] = children
        return serialized

    def deserialize(self, d):
        """
        De-serialize a Q object from a (possibly nested) dict.
        """
        children = []
        for child in d.pop('children'):
            if isinstance(child, dict):
                children.append(self.deserialize(child))
            else:
                children.append(self.prepare_value(child))
        query = Q()
        query.children = children
        query.connector = d['connector']
        query.negated = d['negated']
        if 'subtree_parents' in d:
            query.subtree_parents = d['subtree_parents']
        return query

    def get_field_values_list(self, d):
        """
        Iterate over a (possibly nested) dict, and return a list
        of all children queries, as a dict of the following structure:
        {
            'field': 'some_field__iexact',
            'value': 'some_value',
            'value_from': 'optional_range_val1',
            'value_to': 'optional_range_val2',
            'negate': True,
        }

        OR relations are expressed as an extra "line" between queries.
        """
        fields = []
        children = d.get('children', [])
        for child in children:
            if isinstance(child, dict):
                fields.extend(self.get_field_values_list(child))
            else:
                f = {'field': child[0], 'value': child[1]}
                if self._is_range(child):
                    f['value_from'] = child[1][0]
                    f['value_to'] = child[1][1]
                f['negate'] = d.get('negated', False)
                fields.append(f)

            # add _OR line
            if d['connector'] == 'OR' and children[-1] != child:
                fields.append({'field': '_OR', 'value': 'null'})
        return fields

    def dumps(self, obj):
        if not isinstance(obj, Q):
            raise SerializationError
        string = json.dumps(self.serialize(obj), default=dt2ts)
        if self.b64_enabled:
            return base64.b64encode(string.encode("latin-1")).decode("utf-8")
        return string

    def loads(self, string, raw=False):
        if self.b64_enabled:
            d = json.loads(base64.b64decode(string))
        else:
            d = json.loads(string)
        if raw:
            return d
        return self.deserialize(d)
