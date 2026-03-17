# coding: utf-8

"""
This provides Path objects with a .msgpack attribute that has a load() and a dump() method.

actual library used ruamel.ext.msgpack which is msgpack with some parameters set
and handling of naive datetime and ruamel extension types
"""

import os
from ruamel.std.pathlib import Path
from ruamel.ext.msgpack import pack, unpackb

if not hasattr(Path, 'msgpack'):

    class MsgPack:
        def __init__(self, path):
            self._path = path

        def dump(self, data, **kw):
            with self._path.open('wb') as fp:
                pack(data, fp, **kw)

        def load(self, **kw):
            with self._path.open('rb') as fp:
                return unpackb(fp.read(), **kw)

    class MyPath:
        @property
        def msgpack(self):
            return MsgPack(self)

    Path.msgpack = MyPath.msgpack
