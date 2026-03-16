# -*- coding:utf-8 -*-
import collections
from decimal import Decimal


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
INVALID_JSON_WITH_DANGLING_JUNK = INVALID_JSONS[6]
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