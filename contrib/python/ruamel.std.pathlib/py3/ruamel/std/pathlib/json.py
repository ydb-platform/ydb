# coding: utf-8

"""
This provides Path objects with a .json attribute that has a load() and a dump() method.

actual library used is based on speed preferences: orjson, ujson, json

By default tries to use orjson, falling back to standard json. You can set
RUAMEL_STD_PATHLIB_JSON env. var to 'orjson', 'ujson', or 'standard'. If set
the import must succeed

support for JSON Lines https://jsonlines.org/
"""

import os
from ruamel.std.pathlib import Path

import json


def std_json_dump(data, p, indent=0):
    if isinstance(p, Path):
        return json.dump(
            data, p.open('w'), ensure_ascii=False, check_circular=False, indent=indent
        )
    return json.dump(data, p, ensure_ascii=False, check_circular=False, indent=indent)


def std_json_load(p):
    if isinstance(p, Path):
        return json.load(p.open())
    return json.load(p)


_available_json = [(std_json_dump, std_json_load, json.loads)]


def add_orjson():
    try:
        import orjson

        def orjson_dump(data, p, indent=0):
            if indent == 0:
                opts = orjson.OPT_APPEND_NEWLINE | orjson.OPT_SORT_KEYS
            else:
                opts = orjson.OPT_INDENT_2 | orjson.OPT_APPEND_NEWLINE | orjson.OPT_SORT_KEYS
            if isinstance(p, Path):
                return p.write_bytes(orjson.dumps(data, option=opts))
            return p.write(orjson.dumps(data, option=opts))

        def orjson_load(p):
            if isinstance(p, Path):
                return orjson.loads(p.read_bytes())
            return orjson.loads(p.read())

        _available_json.insert(0, (orjson_dump, orjson_load, orjson.loads))
    except ImportError:
        if os.environ.get('RUAMEL_STD_PATHLIB_JSON') == 'orjson':
            raise


if os.environ.get('RUAMEL_STD_PATHLIB_JSON') in [None, 'orjson']:
    add_orjson()


def add_ujson():
    import ujson  # do not try, it has to be there if specified

    def ujson_dump(data, p, indent=0):
        if isinstance(p, Path):
            return ujson.dump(
                data,
                p.open('w'),
                ensure_ascii=False,
                indent=indent,
                escape_forward_slashes=False,
            )
        return ujson.dump(
            data, p, ensure_ascii=False, indent=indent, escape_forward_slashes=False
        )

    def ujson_load(p):
        if isinstance(p, Path):
            return ujson.load(p.open())
        return ujson.load(p)

    # insert before std, but not before orjson
    _available_json.insert(-1, (ujson_dump, ujson_load, ujson.loads))


if os.environ.get('RUAMEL_STD_PATHLIB_JSON') == 'ujson':
    add_ujson()

if not hasattr(Path, 'json'):

    class JSON:
        def __init__(self, path):
            self._path = path
            self.dump_load = _available_json

        def dump(self, data, indent=2):
            # handle paths in dump, as e.g. orjson requires a byte stream
            # with self._path.open('w') as fp:
            #    json_dump(data, fp)
            self.dump_load[0][0](data, self._path, indent)

        def load(self):
            # with self._path.open() as fp:
            #    return json.load(fp)
            return self.dump_load[0][1](self._path)

        def lines(self):
            for line in self._path.open():
                print('line', line)
                yield self.dump_load[0][2](line)


    class MyPath:
        @property
        def json(self):
            return JSON(self)

    Path.json = MyPath.json
