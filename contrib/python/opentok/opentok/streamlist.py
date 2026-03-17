from six import u
import json
from .opentok import Stream
from .exceptions import GetStreamError


class StreamList(object):
    """
    Represents a list of OpenTok stream objects
    """

    def __init__(self, values):
        self.count = values.get("count")
        self.items = list(map(lambda x: Stream(x), values.get("items", [])))

    def __iter__(self):
        for x in self.items:
            yield x

    def json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)

    def __getitem__(self, key):
        return self.items.get(key)

    def __setitem__(self, key, item):
        raise GetStreamError(
            u("Cannot set item {0} for key {1} in Archive object").format(item, key)
        )

    def __len__(self):
        return len(self.items)