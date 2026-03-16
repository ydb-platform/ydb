from . import base


class TestType(base.SchemaNodeTestCase):

    def test_single_type(self):
        schema = {'type': 'string'}
        self.add_schema(schema)
        self.assertResult(schema)

    def test_single_type_unicode(self):
        schema = {u'type': u'string'}
        self.add_schema(schema)
        self.assertResult(schema)

    def test_typeless(self):
        schema = {}
        self.add_schema(schema)
        self.assertResult(schema)

    def test_array_type_no_items(self):
        schema = {'type': 'array'}
        self.add_schema(schema)
        self.assertResult(schema)


class TestAnyOf(base.SchemaNodeTestCase):

    def test_multi_type(self):
        schema = {'type': ['boolean', 'null', 'number', 'string']}
        self.add_schema(schema)
        self.assertResult(schema)

    def test_multi_type_with_extra_keywords(self):
        schema = {'type': ['boolean', 'null', 'number', 'string'],
                  'title': 'this will be duplicated'}
        self.add_schema(schema)
        self.assertResult({'anyOf': [
            {'type': 'boolean', 'title': 'this will be duplicated'},
            {'type': 'null', 'title': 'this will be duplicated'},
            {'type': 'number', 'title': 'this will be duplicated'},
            {'type': 'string', 'title': 'this will be duplicated'}
        ]})

    def test_anyof(self):
        schema = {"anyOf": [
            {"type": "null"},
            {"type": "boolean", "title": "Gruyere"}
        ]}
        self.add_schema(schema)
        self.assertResult(schema)

    def test_recursive(self):
        schema = {"anyOf": [
            {"type": ["integer", "string"]},
            {"anyOf": [
                {"type": "null"},
                {"type": "boolean", "title": "Gruyere"}
            ]}
        ]}
        self.add_schema(schema)
        # recursive anyOf will be flattened
        self.assertResult({"anyOf": [
            {"type": ["integer", "null", "string"]},
            {"type": "boolean", "title": "Gruyere"}
        ]})


class TestRequired(base.SchemaNodeTestCase):

    def test_preserves_empty_required(self):
        schema = {'type': 'object', 'required': []}
        self.add_schema(schema)
        self.assertResult(schema)


class TestPreserveExtraKeywords(base.SchemaNodeTestCase):

    def test_basic_type(self):
        schema = {'type': 'boolean', 'const': False, 'myKeyword': True}
        self.add_schema(schema)
        self.assertResult(schema)

    def test_number(self):
        schema = {'type': 'number', 'const': 5, 'myKeyword': True}
        self.add_schema(schema)
        self.assertResult(schema)

    def test_list(self):
        schema = {'type': 'array', 'items': {"type": "null"},
                  'const': [], 'myKeyword': True}
        self.add_schema(schema)
        self.assertResult(schema)

    def test_tuple(self):
        schema = {'type': 'array', 'items': [{"type": "null"}],
                  'const': [], 'myKeyword': True}
        self.add_schema(schema)
        self.assertResult(schema)

    def test_object(self):
        schema = {'type': 'object', 'const': {}, 'myKeyword': True}
        self.add_schema(schema)
        self.assertResult(schema)

    def test_typeless(self):
        schema = {'const': 5, 'myKeyword': True}
        self.add_schema(schema)
        self.assertResult(schema)
