try:
    import cPickle as pickle
except ImportError:
    import pickle

import json

try:
    import msgpack
except ImportError:
    pass

try:
    import yaml
except ImportError:
    pass

from django.utils.encoding import force_bytes, force_str


class BaseSerializer(object):

    def __init__(self, **kwargs):
        super(BaseSerializer, self).__init__(**kwargs)

    def serialize(self, value):
        raise NotImplementedError

    def deserialize(self, value):
        raise NotImplementedError


class PickleSerializer(object):

    def __init__(self, pickle_version=-1):
        self.pickle_version = pickle_version

    def serialize(self, value):
        return pickle.dumps(value, self.pickle_version)

    def deserialize(self, value):
        return pickle.loads(force_bytes(value))


class JSONSerializer(BaseSerializer):

    def __init__(self, **kwargs):
        super(JSONSerializer, self).__init__(**kwargs)

    def serialize(self, value):
        return force_bytes(json.dumps(value))

    def deserialize(self, value):
        return json.loads(force_str(value))


class MSGPackSerializer(BaseSerializer):

    def serialize(self, value):
        return msgpack.dumps(value)

    def deserialize(self, value):
        return msgpack.loads(value, encoding='utf-8')


class YAMLSerializer(BaseSerializer):

    def serialize(self, value):
        return yaml.dump(value, encoding='utf-8', Dumper=yaml.Dumper)

    def deserialize(self, value):
        return yaml.load(value, Loader=yaml.FullLoader)


class DummySerializer(BaseSerializer):

    def __init__(self, **kwargs):
        super(DummySerializer, self).__init__(**kwargs)

    def serialize(self, value):
        return value

    def deserialize(self, value):
        return value
