from . import base


class TestSeedTuple(base.SchemaNodeTestCase):

    def test_tuple(self):
        self.add_schema({'type': 'array', 'items': []})
        self.add_object([None])
        self.assertResult({'type': 'array', 'items': [{'type': 'null'}]})


class TestPatternProperties(base.SchemaNodeTestCase):

    def test_single_pattern(self):
        self.add_schema({'type': 'object', 'patternProperties': {
            r'^\d$': None}})
        self.add_object({'0': 0, '1': 1, '2': 2})
        self.assertResult({'type': 'object', 'patternProperties': {
            r'^\d$': {'type': 'integer'}}})

    def test_multi_pattern(self):
        self.add_schema({'type': 'object', 'patternProperties': {
            r'^\d$': None,
            r'^[a-z]$': None}})
        self.add_object({'0': 0, '1': 1, 'a': True, 'b': False})
        self.assertResult({'type': 'object', 'patternProperties': {
            r'^\d$': {'type': 'integer'},
            r'^[a-z]$': {'type': 'boolean'}}})

    def test_multi_pattern_multi_object(self):
        self.add_schema({'type': 'object', 'patternProperties': {
            r'^\d$': None,
            r'^[a-z]$': None}})
        self.add_object({'0': 0})
        self.add_object({'1': 1})
        self.add_object({'a': True})
        self.add_object({'b': False})
        self.assertResult({'type': 'object', 'patternProperties': {
            r'^\d$': {'type': 'integer'},
            r'^[a-z]$': {'type': 'boolean'}}})

    def test_existing_schema(self):
        self.add_schema({'type': 'object', 'patternProperties': {
            r'^\d$': {'type': 'boolean'}}})
        self.add_object({'0': 0, '1': 1, '2': 2})
        self.assertResult({'type': 'object', 'patternProperties': {
            r'^\d$': {'type': ['boolean', 'integer']}}})

    def test_prefers_existing_properties(self):
        self.add_schema({'type': 'object',
                         'properties': {'0': None},
                         'patternProperties': {r'^\d$': None}})
        self.add_object({'0': 0, '1': 1, '2': 2})
        self.assertResult({'type': 'object',
                           'properties': {'0': {'type': 'integer'}},
                           'patternProperties': {r'^\d$': {'type': 'integer'}},
                           'required': ['0']})

    def test_keeps_unrecognized_properties(self):
        self.add_schema({'type': 'object',
                         'patternProperties': {r'^\d$': None}})
        self.add_object({'0': 0, '1': 1, '2': 2, 'a': True})
        self.assertResult({'type': 'object',
                           'properties': {'a': {'type': 'boolean'}},
                           'patternProperties': {r'^\d$': {'type': 'integer'}},
                           'required': ['a']})
