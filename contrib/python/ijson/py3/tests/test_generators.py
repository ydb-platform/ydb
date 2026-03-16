import io

import pytest

from ijson import common

from .test_base import JSON,  JSON_EVENTS, PARTIAL_ARRAY_JSONS, INVALID_JSONS


class SingleReadFile:
    '''A bytes file that can be read only once'''

    def __init__(self, raw_value):
        self.raw_value = raw_value

    def read(self, size=-1):
        if size == 0:
            return bytes()
        val = self.raw_value
        if not val:
            raise AssertionError('read twice')
        self.raw_value = bytes()
        return val


def test_utf8_split(backend):
    buf_size = JSON.index(b'\xd1') + 1
    try:
        list(backend.basic_parse_gen(io.BytesIO(JSON), buf_size=buf_size))
    except UnicodeDecodeError:
        pytest.fail('UnicodeDecodeError raised')

def test_lazy(backend):
    # shouldn't fail since iterator is not exhausted
    basic_parse = backend.basic_parse_gen(io.BytesIO(INVALID_JSONS[0]))
    assert basic_parse is not None

def test_extra_content_still_generates_results(backend):
    """Extra content raises an error, but still generates all events"""
    with pytest.raises(common.JSONError):
        events = []
        for event in backend.basic_parse(JSON + b"#"):
            events.append(event)
    assert events == JSON_EVENTS

def test_boundary_lexeme(backend):
    buf_size = JSON.index(b'false') + 1
    events = list(backend.basic_parse(JSON, buf_size=buf_size))
    assert events == JSON_EVENTS

def test_boundary_whitespace(backend):
    buf_size = JSON.index(b'   ') + 1
    events = list(backend.basic_parse_gen(io.BytesIO(JSON), buf_size=buf_size))
    assert events == JSON_EVENTS

def test_item_building_greediness(backend):
    _test_item_iteration_validity(backend, io.BytesIO)

def test_lazy_file_reading(backend):
    _test_item_iteration_validity(backend, SingleReadFile)

def _test_item_iteration_validity(backend, file_type):
    for json in PARTIAL_ARRAY_JSONS:
        json, expected_items = json[0], json[1:]
        iterable = backend.items_gen(file_type(json), 'item')
        for expected_item in expected_items:
            assert expected_item == next(iterable)


COMMON_DATA = b'''
    {
        "skip": "skip_value",
        "c": {"d": "e", "f": "g"},
        "list": [{"o1": 1}, {"o2": 2}]
    }'''

COMMON_PARSE = [
    ('', 'start_map', None),
    ('', 'map_key', 'skip'),
    ('skip', 'string', 'skip_value'),
    ('', 'map_key', 'c'),
    ('c', 'start_map', None),
    ('c', 'map_key', 'd'),
    ('c.d', 'string', 'e'),
    ('c', 'map_key', 'f'),
    ('c.f', 'string', 'g'),
    ('c', 'end_map', None),
    ('', 'map_key', 'list'),
    ('list', 'start_array', None),
    ('list.item', 'start_map', None),
    ('list.item', 'map_key', 'o1'),
    ('list.item.o1', 'number', 1),
    ('list.item', 'end_map', None),
    ('list.item', 'start_map', None),
    ('list.item', 'map_key', 'o2'),
    ('list.item.o2', 'number', 2),
    ('list.item', 'end_map', None),
    ('list', 'end_array', None),
    ('', 'end_map', None),
]

def _skip_parse_events(events):
    skip_value = None
    for prefix, _, value in events:
        if prefix == 'skip':
            skip_value = value
            break
    assert skip_value == 'skip_value'

def _test_common_routine(backend, routine, *args, **kwargs):
    base_routine_name = kwargs.pop('base_routine_name', 'parse')
    base_routine = getattr(backend, base_routine_name)
    events = base_routine(io.BytesIO(COMMON_DATA))
    if base_routine_name == 'parse':
        _skip_parse_events(events)
    # Rest of events can still be used
    return list(routine(events, *args))

def test_common_parse(backend):
    with pytest.warns() as warns:
        results = _test_common_routine(
            backend, common.parse, base_routine_name='basic_parse'
        )
    assert COMMON_PARSE == results
    assert 1 == len(warns)

def test_common_kvitems(backend):
    with pytest.warns() as warns:
        results = _test_common_routine(backend, common.kvitems, 'c')
    assert [("d", "e"), ("f", "g")] == results
    assert 1 == len(warns)

def test_common_items(backend):
    with pytest.warns() as warns:
        results = _test_common_routine(backend, common.items, 'list.item')
    assert [{"o1": 1}, {"o2": 2}] == results
    assert 1 == len(warns)