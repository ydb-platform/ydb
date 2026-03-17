# -*- encoding: utf-8 -*-
from __future__ import unicode_literals

import struct


class Definition(object):

    _definition = tuple()
    _struct_rule = ''
    _struct_size = None
    _names = []

    def __init__(self, data):
        self.data = data

    @classmethod
    def init_cache(cls):
        names = []
        rules = []

        for name, rule in cls._definition:
            names.append(name)
            rules.append(rule)

        cls._names = names
        rule = '<' + ''.join(rules)
        cls._struct_rule = rule
        cls._struct_size = struct.calcsize(rule)

    @classmethod
    def _unpack(cls, raw):
        unpacked = struct.unpack(cls._struct_rule, raw)
        return dict(zip(cls._names, unpacked))

    @classmethod
    def from_file(cls, fileobj):
        data = fileobj.read(cls._struct_size)
        return cls(cls._unpack(data))
