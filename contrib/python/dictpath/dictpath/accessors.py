"""Dictpath accessors module"""
from contextlib import contextmanager


class DictOrListAccessor(object):

    def __init__(self, dict_or_list):
        self.dict_or_list = dict_or_list

    def stat(self, parts):
        return NotImplementedError

    def keys(self, parts):
        with self.open(parts) as d:
            return d.keys()

    def len(self, parts):
        with self.open(parts) as d:
            return len(d)

    @contextmanager
    def open(self, parts):
        content = self.dict_or_list
        for part in parts:
            content = content[part]
        try:
            yield content
        finally:
            pass
