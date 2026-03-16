from ijson import common, compat

from .test_base import (JSON, FileBasedTests, JSON_EVENTS,
    PARTIAL_ARRAY_JSONS, warning_catcher, INVALID_JSONS, IS_PY2,
    generate_test_cases)



class SingleReadFile(object):
    '''A bytes file that can be read only once'''

    def __init__(self, raw_value):
        self.raw_value = raw_value

    def read(self, size=-1):
        if size == 0:
            return compat.bytetype()
        val = self.raw_value
        if not val:
            raise AssertionError('read twice')
        self.raw_value = compat.bytetype()
        return val


class GeneratorSpecificTests(FileBasedTests):
    '''
    Base class for parsing tests that is used to create test cases for each
    available backends.
    '''

    def test_utf8_split(self):
        buf_size = JSON.index(b'\xd1') + 1
        try:
            self.get_all(self.basic_parse, JSON, buf_size=buf_size)
        except UnicodeDecodeError:
            self.fail('UnicodeDecodeError raised')

    def test_lazy(self):
        # shouldn't fail since iterator is not exhausted
        self.backend.basic_parse(compat.BytesIO(INVALID_JSONS[0]))
        self.assertTrue(True)

    def test_boundary_lexeme(self):
        buf_size = JSON.index(b'false') + 1
        events = self.get_all(self.basic_parse, JSON, buf_size=buf_size)
        self.assertEqual(events, JSON_EVENTS)

    def test_boundary_whitespace(self):
        buf_size = JSON.index(b'   ') + 1
        events = self.get_all(self.basic_parse, JSON, buf_size=buf_size)
        self.assertEqual(events, JSON_EVENTS)

    def test_item_building_greediness(self):
        self._test_item_iteration_validity(compat.BytesIO)

    def test_lazy_file_reading(self):
        if self.backend_name == 'python' and IS_PY2:
            # We know it doesn't work because because the decoder itself
            # is quite eager on its reading
            return
        self._test_item_iteration_validity(SingleReadFile)

    def _test_item_iteration_validity(self, file_type):
        for json in PARTIAL_ARRAY_JSONS:
            json, expected_items = json[0], json[1:]
            iterable = self.backend.items(file_type(json), 'item')
            for expected_item in expected_items:
                self.assertEqual(expected_item, next(iterable))


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

    def _skip_parse_events(self, events):
        skip_value = None
        for prefix, _, value in events:
            if prefix == 'skip':
                skip_value = value
                break
        self.assertEqual(skip_value, 'skip_value')

    def _test_common_routine(self, routine, *args, **kwargs):
        base_routine_name = kwargs.pop('base_routine_name', 'parse')
        base_routine = getattr(self.backend, base_routine_name)
        events = base_routine(compat.BytesIO(self.COMMON_DATA))
        if base_routine_name == 'parse':
            self._skip_parse_events(events)
        # Rest of events can still be used
        return list(routine(events, *args))

    def test_common_parse(self):
        with warning_catcher() as warns:
            results = self._test_common_routine(common.parse,
                                                base_routine_name='basic_parse')
        self.assertEqual(self.COMMON_PARSE, results)
        self.assertEqual(len(warns), 1)

    def test_common_kvitems(self):
        with warning_catcher() as warns:
            results = self._test_common_routine(common.kvitems, 'c')
        self.assertEqual([("d", "e"), ("f", "g")], results)
        self.assertEqual(len(warns), 1)

    def test_common_items(self):
        with warning_catcher() as warns:
            results = self._test_common_routine(common.items, 'list.item')
        self.assertEqual([{"o1": 1}, {"o2": 2}], results)
        self.assertEqual(len(warns), 1)


def _reader(json):
    if type(json) == compat.bytetype:
        return compat.BytesIO(json)
    return compat.StringIO(json)


def get_all(routine, json_content, *args, **kwargs):
    return list(routine(_reader(json_content), *args, **kwargs))


def get_first(routine, json_content, *args, **kwargs):
    return next(routine(_reader(json_content), *args, **kwargs))


generate_test_cases(globals(), 'Generators', '_gen', GeneratorSpecificTests)