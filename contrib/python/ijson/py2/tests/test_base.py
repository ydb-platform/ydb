# -*- coding:utf-8 -*-
from __future__ import unicode_literals
import collections
import ctypes
import unittest
from decimal import Decimal
import threading

import ijson
from ijson import common, compat
from ijson.compat import b2s, IS_PY2
import warnings


JSON = b'''
{
  "docs": [
    {
      "null": null,
      "boolean": false,
      "true": true,
      "integer": 0,
      "double": 0.5,
      "exponent": 1.0e+2,
      "long": 10000000000,
      "string": "\\u0441\\u0442\\u0440\\u043e\\u043a\\u0430 - \xd1\x82\xd0\xb5\xd1\x81\xd1\x82",
      "\xc3\xb1and\xc3\xba": null
    },
    {
      "meta": [[1], {}]
    },
    {
      "meta": {"key": "value"}
    },
    {
      "meta": null
    },
    {
      "meta": []
    }
  ]
}
'''
JSON_OBJECT = {
    "docs": [
        {
            "null": None,
            "boolean": False,
            "true": True,
            "integer": 0,
            "double": Decimal("0.5"),
            "exponent": 1e+2,
            "long": 10000000000,
            "string": "строка - тест",
            "ñandú": None
        },
        {
            "meta": [[1], {}]
        },
        {
            "meta": {
                "key": "value"
            }
        },
        {
            "meta": None
        },
        {
            "meta": []
        }
    ]
}
JSON_PARSE_EVENTS = [
    ('', 'start_map', None),
    ('', 'map_key', 'docs'),
    ('docs', 'start_array', None),
    ('docs.item', 'start_map', None),
    ('docs.item', 'map_key', 'null'),
    ('docs.item.null', 'null', None),
    ('docs.item', 'map_key', 'boolean'),
    ('docs.item.boolean', 'boolean', False),
    ('docs.item', 'map_key', 'true'),
    ('docs.item.true', 'boolean', True),
    ('docs.item', 'map_key', 'integer'),
    ('docs.item.integer', 'number', 0),
    ('docs.item', 'map_key', 'double'),
    ('docs.item.double', 'number', Decimal('0.5')),
    ('docs.item', 'map_key', 'exponent'),
    ('docs.item.exponent', 'number', Decimal('1.0E+2')),
    ('docs.item', 'map_key', 'long'),
    ('docs.item.long', 'number', 10000000000),
    ('docs.item', 'map_key', 'string'),
    ('docs.item.string', 'string', 'строка - тест'),
    ('docs.item', 'map_key', 'ñandú'),
    ('docs.item.ñandú', 'null', None),
    ('docs.item', 'end_map', None),
    ('docs.item', 'start_map', None),
    ('docs.item', 'map_key', 'meta'),
    ('docs.item.meta', 'start_array', None),
    ('docs.item.meta.item', 'start_array', None),
    ('docs.item.meta.item.item', 'number', 1),
    ('docs.item.meta.item', 'end_array', None),
    ('docs.item.meta.item', 'start_map', None),
    ('docs.item.meta.item', 'end_map', None),
    ('docs.item.meta', 'end_array', None),
    ('docs.item', 'end_map', None),
    ('docs.item', 'start_map', None),
    ('docs.item', 'map_key', 'meta'),
    ('docs.item.meta', 'start_map', None),
    ('docs.item.meta', 'map_key', 'key'),
    ('docs.item.meta.key', 'string', 'value'),
    ('docs.item.meta', 'end_map', None),
    ('docs.item', 'end_map', None),
    ('docs.item', 'start_map', None),
    ('docs.item', 'map_key', 'meta'),
    ('docs.item.meta', 'null', None),
    ('docs.item', 'end_map', None),
    ('docs.item', 'start_map', None),
    ('docs.item', 'map_key', 'meta'),
    ('docs.item.meta', 'start_array', None),
    ('docs.item.meta', 'end_array', None),
    ('docs.item', 'end_map', None),
    ('docs', 'end_array', None),
    ('', 'end_map', None)
]
JSON_KVITEMS = [
    ("null", None),
    ("boolean", False),
    ("true", True),
    ("integer", 0),
    ("double", Decimal("0.5")),
    ("exponent", 1e+2),
    ("long", 10000000000),
    ("string", "строка - тест"),
    ("ñandú", None),
    ("meta", [[1], {}]),
    ("meta", {"key": "value"}),
    ("meta", None),
    ("meta", [])
]
JSON_KVITEMS_META = [
    ('key', 'value')
]
JSON_EVENTS = [
    ('start_map', None),
        ('map_key', 'docs'),
        ('start_array', None),
            ('start_map', None),
                ('map_key', 'null'),
                ('null', None),
                ('map_key', 'boolean'),
                ('boolean', False),
                ('map_key', 'true'),
                ('boolean', True),
                ('map_key', 'integer'),
                ('number', 0),
                ('map_key', 'double'),
                ('number', Decimal('0.5')),
                ('map_key', 'exponent'),
                ('number', 100),
                ('map_key', 'long'),
                ('number', 10000000000),
                ('map_key', 'string'),
                ('string', 'строка - тест'),
                ('map_key', 'ñandú'),
                ('null', None),
            ('end_map', None),
            ('start_map', None),
                ('map_key', 'meta'),
                ('start_array', None),
                    ('start_array', None),
                        ('number', 1),
                    ('end_array', None),
                    ('start_map', None),
                    ('end_map', None),
                ('end_array', None),
            ('end_map', None),
            ('start_map', None),
                ('map_key', 'meta'),
                ('start_map', None),
                    ('map_key', 'key'),
                    ('string', 'value'),
                ('end_map', None),
            ('end_map', None),
            ('start_map', None),
                ('map_key', 'meta'),
                ('null', None),
            ('end_map', None),
            ('start_map', None),
                ('map_key', 'meta'),
                ('start_array', None),
                ('end_array', None),
            ('end_map', None),
        ('end_array', None),
    ('end_map', None),
]

# Like JSON, but with an additional top-level array structure
ARRAY_JSON = b'[' + JSON + b']'
ARRAY_JSON_EVENTS = (
    [('start_array', None)] +
    JSON_EVENTS +
    [('end_array', None)]
)
ARRAY_JSON_PARSE_EVENTS = (
    [('', 'start_array', None)] +
    [('.'.join(filter(None, ('item', p))), t, e) for p, t, e in JSON_PARSE_EVENTS] +
    [('', 'end_array', None)]
)
ARRAY_JSON_OBJECT = [JSON_OBJECT]



SCALAR_JSON = b'0'
INVALID_JSONS = [
    b'["key", "value",]',      # trailing comma
    b'["key"  "value"]',       # no comma
    b'{"key": "value",}',      # trailing comma
    b'{"key": "value" "key"}', # no comma
    b'{"key"  "value"}',       # no colon
    b'invalid',                # unknown lexeme
    b'[1, 2] dangling junk',   # dangling junk
    b'}',                      # no corresponding opening token
    b']',                      # no corresponding opening token
    b'"\xa8"'                  # invalid UTF-8 byte sequence
]
YAJL1_PASSING_INVALID = INVALID_JSONS[6]
INCOMPLETE_JSONS = [
    b'',
    b'"test',
    b'[',
    b'[1',
    b'[1,',
    b'{',
    b'{"key"',
    b'{"key":',
    b'{"key": "value"',
    b'{"key": "value",',
]
INCOMPLETE_JSON_TOKENS = [
    b'n',
    b'nu',
    b'nul',
    b't',
    b'tr',
    b'tru',
    b'f',
    b'fa',
    b'fal',
    b'fals',
    b'[f',
    b'[fa',
    b'[fal',
    b'[fals',
    b'[t',
    b'[tr',
    b'[tru',
    b'[n',
    b'[nu',
    b'[nul',
    b'{"key": t',
    b'{"key": tr',
    b'{"key": tru',
    b'{"key": f',
    b'{"key": fa',
    b'{"key": fal',
    b'{"key": fals',
    b'{"key": n',
    b'{"key": nu',
    b'{"key": nul',
]
STRINGS_JSON = br'''
{
    "str1": "",
    "str2": "\"",
    "str3": "\\",
    "str4": "\\\\",
    "special\t": "\b\f\n\r\t"
}
'''
SURROGATE_PAIRS_JSON = br'"\uD83D\uDCA9"'
PARTIAL_ARRAY_JSONS = [
    (b'[1,', 1),
    (b'[1, 2 ', 1, 2),
    (b'[1, "abc"', 1, 'abc'),
    (b'[{"abc": [0, 1]}', {'abc': [0, 1]}),
    (b'[{"abc": [0, 1]},', {'abc': [0, 1]}),
]

items_test_case = collections.namedtuple('items_test_case', 'json, prefix, kvitems, items')
EMPTY_MEMBER_TEST_CASES = {
    'simple': items_test_case(
        b'{"a": {"": {"b": 1, "c": 2}}}',
        'a.',
        [("b", 1), ("c", 2)],
        [{"b": 1, "c": 2}]
    ),
    'embedded': items_test_case(
        b'{"a": {"": {"": {"b": 1, "c": 2}}}}',
        'a..',
        [("b", 1), ("c", 2)],
        [{"b": 1, "c": 2}]
    ),
    'top_level': items_test_case(
        b'{"": 1, "a": 2}',
        '',
        [("", 1), ("a", 2)],
        [{"": 1, "a": 2}]
    ),
    'top_level_embedded': items_test_case(
        b'{"": {"": 1}, "a": 2}',
        '',
        [("", {"": 1}), ("a", 2)],
        [{"": {"": 1}, "a": 2}]
    )
}


class warning_catcher(object):
    '''Encapsulates proper warning catch-all logic in python 2.7 and 3'''

    def __init__(self):
        self.catcher = warnings.catch_warnings(record=True)

    def __enter__(self):
        ret = self.catcher.__enter__()
        if compat.IS_PY2:
            warnings.simplefilter("always")
        return ret

    def __exit__(self, *args):
        self.catcher.__exit__(*args)


class BackendSpecificTestCase(object):
    '''
    Base class for backend-specific tests, gives ability to easily and
    generically reference different methods on the backend. It requires
    subclasses to define a `backend` member with the backend module, and a
    `suffix` attribute indicating the method flavour to obtain.
    '''

    def __getattr__(self, name):
        return getattr(self.backend, name + self.method_suffix)


class IJsonTestsBase(object):
    '''
    Base class with common tests for all iteration methods.
    Subclasses implement `all()` and `first()` to collect events coming from
    a particuliar method.
    '''

    def test_basic_parse(self):
        events = self.get_all(self.basic_parse, JSON)
        self.assertEqual(events, JSON_EVENTS)

    def test_basic_parse_threaded(self):
        thread = threading.Thread(target=self.test_basic_parse)
        thread.start()
        thread.join()

    def test_parse(self):
        events = self.get_all(self.parse, JSON)
        self.assertEqual(events, JSON_PARSE_EVENTS)

    def test_items(self):
        events = self.get_all(self.items, JSON, '')
        self.assertEqual(events, [JSON_OBJECT])

    def test_items_twodictlevels(self):
        json = b'{"meta":{"view":{"columns":[{"id": -1}, {"id": -2}]}}}'
        ids = self.get_all(self.items, json, 'meta.view.columns.item.id')
        self.assertEqual(2, len(ids))
        self.assertListEqual([-2,-1], sorted(ids))

    def test_items_with_dotted_name(self):
        json = b'{"0.1": 0}'
        self.assertListEqual([0], self.get_all(self.items, json, '0.1'))
        json = b'{"0.1": [{"a.b": 0}]}'
        self.assertListEqual([0], self.get_all(self.items, json, '0.1.item.a.b'))
        json = b'{"0.1": 0, "0": {"1": 1}}'
        self.assertListEqual([0, 1], self.get_all(self.items, json, '0.1'))
        json = b'{"abc.def": 0}'
        self.assertListEqual([0], self.get_all(self.items, json, 'abc.def'))
        self.assertListEqual([], self.get_all(self.items, json, 'abc'))
        self.assertListEqual([], self.get_all(self.items, json, 'def'))

    def test_map_type(self):
        obj = self.get_first(self.items, JSON, '')
        self.assertTrue(isinstance(obj, dict))
        obj = self.get_first(self.items, JSON, '', map_type=collections.OrderedDict)
        self.assertTrue(isinstance(obj, collections.OrderedDict))

    def test_kvitems(self):
        kvitems = self.get_all(self.kvitems, JSON, 'docs.item')
        self.assertEqual(JSON_KVITEMS, kvitems)

    def test_kvitems_toplevel(self):
        kvitems = self.get_all(self.kvitems, JSON, '')
        self.assertEqual(1, len(kvitems))
        key, value = kvitems[0]
        self.assertEqual('docs', key)
        self.assertEqual(JSON_OBJECT['docs'], value)

    def test_kvitems_empty(self):
        kvitems = self.get_all(self.kvitems, JSON, 'docs')
        self.assertEqual([], kvitems)

    def test_kvitems_twodictlevels(self):
        json = b'{"meta":{"view":{"columns":[{"id": -1}, {"id": -2}]}}}'
        view = self.get_all(self.kvitems, json, 'meta.view')
        self.assertEqual(1, len(view))
        key, value = view[0]
        self.assertEqual('columns', key)
        self.assertEqual([{'id': -1}, {'id': -2}], value)

    def test_kvitems_different_underlying_types(self):
        kvitems = self.get_all(self.kvitems, JSON, 'docs.item.meta')
        self.assertEqual(JSON_KVITEMS_META, kvitems)

    def test_basic_parse_array(self):
        events = self.get_all(self.basic_parse, ARRAY_JSON)
        self.assertEqual(events, ARRAY_JSON_EVENTS)

    def test_basic_parse_array_threaded(self):
        thread = threading.Thread(target=self.test_basic_parse_array)
        thread.start()
        thread.join()

    def test_parse_array(self):
        events = self.get_all(self.parse, ARRAY_JSON)
        self.assertEqual(events, ARRAY_JSON_PARSE_EVENTS)

    def test_items_array(self):
        events = self.get_all(self.items, ARRAY_JSON, '')
        self.assertEqual(events, [ARRAY_JSON_OBJECT])

    def test_kvitems_array(self):
        kvitems = self.get_all(self.kvitems, ARRAY_JSON, 'item.docs.item')
        self.assertEqual(JSON_KVITEMS, kvitems)

    def test_scalar(self):
        events = self.get_all(self.basic_parse, SCALAR_JSON)
        self.assertEqual(events, [('number', 0)])

    def test_strings(self):
        events = self.get_all(self.basic_parse, STRINGS_JSON)
        strings = [value for event, value in events if event == 'string']
        self.assertEqual(strings, ['', '"', '\\', '\\\\', '\b\f\n\r\t'])
        self.assertTrue(('map_key', 'special\t') in events)

    def test_surrogate_pairs(self):
        event = self.get_first(self.basic_parse, SURROGATE_PAIRS_JSON)
        parsed_string = event[1]
        self.assertEqual(parsed_string, '💩')

    def test_numbers(self):
        """Check that numbers are correctly parsed"""

        def get_numbers(json, **kwargs):
            events = self.get_all(self.basic_parse, json, **kwargs)
            return events, [value for event, value in events if event == 'number']

        def assert_numbers(json, expected_float_type, *numbers, **kwargs):
            events, values = get_numbers(json, **kwargs)
            float_types = set(type(value) for event, value in events if event == 'number')
            float_types -= {int}
            self.assertEqual(1, len(float_types))
            self.assertEqual(next(iter(float_types)), expected_float_type)
            self.assertSequenceEqual(numbers, values)

        NUMBERS_JSON = b'[1, 1.0, 1E2]'
        assert_numbers(NUMBERS_JSON, Decimal, 1, Decimal("1.0"), Decimal("1e2"))
        assert_numbers(NUMBERS_JSON, float, 1, 1., 100., use_float=True)
        assert_numbers(b'1e400', Decimal, Decimal('1e400'))
        assert_numbers(b'1e-400', Decimal, Decimal('1e-400'))
        assert_numbers(b'1e-400', float, 0., use_float=True)
        # Test for 64-bit integers support when using use_float=True
        try:
            past32bits = 2 ** 32 + 1
            received = get_numbers(('%d' % past32bits).encode('utf8'), use_float=True)[1][0]
            self.assertTrue(self.supports_64bit_integers)
            self.assertEqual(past32bits, received)
        except common.JSONError:
            self.assertFalse(self.supports_64bit_integers)
        # Check that numbers bigger than MAX_DOUBLE cannot be represented
        try:
            get_numbers(b'1e400', use_float=True)
            self.fail("Overflow error expected")
        except common.JSONError:
            pass

    def test_invalid_numbers(self):
        # leading zeros
        if self.detects_leading_zeros:
            for case in (b'00',   b'01',   b'001'):
                for base in (case, case + b'.0', case + b'e0', case + b'E0'):
                    for n in (base, b'-' + base):
                        with self.assertRaises(common.JSONError):
                            self.get_all(self.basic_parse, n)
        # incomplete exponents
        for n in (b'1e', b'0.1e', b'0E'):
            with self.assertRaises(common.JSONError):
                self.get_all(self.basic_parse, n)
        # incomplete fractions
        for n in (b'1.', b'.1'):
            with self.assertRaises(common.JSONError):
                self.get_all(self.basic_parse, n)

    def test_incomplete(self):
        for json in INCOMPLETE_JSONS:
            with self.assertRaises(common.IncompleteJSONError):
                self.get_all(self.basic_parse, json)

    def test_incomplete_tokens(self):
        if not self.handles_incomplete_json_tokens:
            return
        for json in INCOMPLETE_JSON_TOKENS:
            with self.assertRaises(common.IncompleteJSONError):
                self.get_all(self.basic_parse, json)

    def test_invalid(self):
        for json in INVALID_JSONS:
            # Yajl1 doesn't complain about additional data after the end
            # of a parsed object. Skipping this test.
            if self.backend_name == 'yajl' and json == YAJL1_PASSING_INVALID:
                continue
            with self.assertRaises(common.JSONError):
                self.get_all(self.basic_parse, json)

    def test_multiple_values(self):
        """Test that the multiple_values flag works"""
        if not self.supports_multiple_values:
            with self.assertRaises(ValueError):
                self.get_all(self.basic_parse, "", multiple_values=True)
            return
        multiple_json = JSON + JSON + JSON
        items = lambda x, **kwargs: self.items(x, '', **kwargs)
        for func in (self.basic_parse, items):
            with self.assertRaises(common.JSONError):
                self.get_all(func, multiple_json)
            with self.assertRaises(common.JSONError):
                self.get_all(func, multiple_json, multiple_values=False)
            result = self.get_all(func, multiple_json, multiple_values=True)
            if func == items:
                self.assertEqual(result, [JSON_OBJECT, JSON_OBJECT, JSON_OBJECT])
            else:
                self.assertEqual(result, JSON_EVENTS + JSON_EVENTS + JSON_EVENTS)

    def test_comments(self):
        json = b'{"a": 2 /* a comment */}'
        try:
            self.get_all(self.basic_parse, json, allow_comments=True)
        except ValueError:
            if self.supports_comments:
                raise

    def _test_empty_member(self, test_case):
        pairs = self.get_all(self.kvitems, test_case.json, test_case.prefix)
        self.assertEqual(test_case.kvitems, pairs)
        objects = self.get_all(self.items, test_case.json, test_case.prefix)
        self.assertEqual(test_case.items, objects)

    def test_empty_member(self):
        self._test_empty_member(EMPTY_MEMBER_TEST_CASES['simple'])

    def test_embedded_empty_member(self):
        self._test_empty_member(EMPTY_MEMBER_TEST_CASES['embedded'])

    def test_top_level_empty_member(self):
        self._test_empty_member(EMPTY_MEMBER_TEST_CASES['top_level'])

    def test_top_level_embedded_empty_member(self):
        self._test_empty_member(EMPTY_MEMBER_TEST_CASES['top_level_embedded'])


class FileBasedTests(object):

    def test_string_stream(self):
        with warning_catcher() as warns:
            events = self.get_all(self.basic_parse, b2s(JSON))
            self.assertEqual(events, JSON_EVENTS)
        if self.warn_on_string_stream:
            self.assertEqual(len(warns), 1)
            self.assertEqual(DeprecationWarning, warns[0].category)

    def test_different_buf_sizes(self):
        for buf_size in (1, 4, 16, 64, 256, 1024, 4098):
            events = self.get_all(self.basic_parse, JSON, buf_size=buf_size)
            self.assertEqual(events, JSON_EVENTS)


def generate_backend_specific_tests(module, classname_prefix, method_suffix,
                                    *bases, **kwargs):
    for backend in ['python', 'yajl', 'yajl2', 'yajl2_cffi', 'yajl2_c']:
        try:
            classname = '%s%sTests' % (
                ''.join(p.capitalize() for p in backend.split('_')),
                classname_prefix
            )
            if IS_PY2:
                classname = classname.encode('ascii')

            _bases = bases + (BackendSpecificTestCase, unittest.TestCase)
            _members = {
                'backend_name': backend,
                'backend': ijson.get_backend(backend),
                'method_suffix': method_suffix,
                'warn_on_string_stream': not IS_PY2,
                'supports_64bit_integers': not (backend == 'yajl' and ctypes.sizeof(ctypes.c_long) == 4)
            }
            members = kwargs.get('members', lambda _: {})
            _members.update(members(backend))
            module[classname] = type(classname, _bases, _members)
        except ImportError:
            pass


def generate_test_cases(module, classname, method_suffix, *bases):
        _bases = bases + (IJsonTestsBase,)
        members = lambda name: {
            'get_all': lambda self, *args, **kwargs: module['get_all'](*args, **kwargs),
            'get_first': lambda self, *args, **kwargs: module['get_first'](*args, **kwargs),
            'supports_multiple_values': name != 'yajl',
            'supports_comments': name != 'python',
            'detects_leading_zeros': name != 'yajl',
            'handles_incomplete_json_tokens': name != 'yajl'
        }
        return generate_backend_specific_tests(module, classname, method_suffix,
                                               members=members, *_bases)