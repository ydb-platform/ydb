from . import base


class TestType(base.SchemaNodeTestCase):

    def test_single_type(self):
        schema = {'type': 'null'}
        self.add_schema(schema)
        self.add_schema(schema)
        self.assertResult(schema)

    def test_single_type_unicode(self):
        schema = {u'type': u'string'}
        self.add_schema(schema)
        self.assertResult(schema)

    def test_redundant_integer_type(self):
        self.add_schema({'type': 'integer'})
        self.add_schema({'type': 'number'})
        self.assertResult({'type': 'number'})

    def test_typeless(self):
        schema1 = {"title": "ambiguous schema"}
        schema2 = {"grail": "We've already got one"}
        result = dict(schema1)
        result.update(schema2)
        self.add_schema(schema1)
        self.add_schema(schema2)
        self.assertResult(result)

    def test_typeless_incorporated(self):
        schema1 = {"title": "Gruyere"}
        schema2 = {"type": "boolean"}
        self.add_schema(schema1)
        self.add_schema(schema2)
        self.assertResult({"type": "boolean", "title": "Gruyere"})

    def test_typeless_instantly_incorporated(self):
        schema1 = {"type": "boolean"}
        schema2 = {"title": "Gruyere"}
        self.add_schema(schema1)
        self.add_schema(schema2)
        self.assertResult({"type": "boolean", "title": "Gruyere"})


class TestRequired(base.SchemaNodeTestCase):

    def test_combines(self):
        schema1 = {"type": "object", "required": [
            "series of statements", "definite proposition"]}
        schema2 = {"type": "object", "required": ["definite proposition"]}
        self.add_schema(schema1)
        self.add_schema(schema2)
        self.assertResult({"type": "object", "required": [
            "definite proposition"]})

    def test_ignores_missing(self):
        schema1 = {"type": "object"}
        schema2 = {"type": "object", "required": ["definite proposition"]}
        self.add_schema(schema1)
        self.add_schema(schema2)
        self.assertResult({"type": "object", "required": [
            "definite proposition"]})

    def test_omits_all_missing(self):
        schema1 = {"type": "object", "properties": {"spam": {}}}
        schema2 = {"type": "object", "properties": {"eggs": {}}}
        self.add_schema(schema1)
        self.add_schema(schema2)
        self.assertResult(
            {"type": "object", "properties": {"spam": {}, "eggs": {}}})

    def test_maintains_empty(self):
        seed = {"required": []}
        schema1 = {"type": "object", "required": ["series of statements"]}
        schema2 = {"type": "object", "required": ["definite proposition"]}
        self.add_schema(seed)
        self.add_schema(schema1)
        self.add_schema(schema2)
        self.assertResult({"type": "object", "required": []})


class TestAnyOf(base.SchemaNodeTestCase):

    def test_multi_type(self):
        self.add_schema({'type': 'boolean'})
        self.add_schema({'type': 'null'})
        self.add_schema({'type': 'string'})
        self.assertResult({'type': ['boolean', 'null', 'string']})

    def test_anyof_generated(self):
        schema1 = {"type": "null", "title": "African or European Swallow?"}
        schema2 = {"type": "boolean", "title": "Gruyere"}
        self.add_schema(schema1)
        self.add_schema(schema2)
        self.assertResult({"anyOf": [
            schema1,
            schema2
        ]})

    def test_anyof_seeded(self):
        schema1 = {"type": "null", "title": "African or European Swallow?"}
        schema2 = {"type": "boolean", "title": "Gruyere"}
        self.add_schema({"anyOf": [
            {"type": "null"},
            schema2
        ]})
        self.add_schema(schema1)
        self.assertResult({"anyOf": [
            schema1,
            schema2
        ]})

    def test_list_plus_tuple(self):
        schema1 = {"type": "array", "items": {"type": "null"}}
        schema2 = {"type": "array", "items": [{"type": "null"}]}
        self.add_schema(schema1)
        self.add_schema(schema2)
        self.assertResult({"anyOf": [
            schema1,
            schema2
        ]})

    def test_multi_type_and_anyof(self):
        schema1 = {'type': ['boolean', 'null', 'string']}
        schema2 = {"type": "boolean", "title": "Gruyere"}
        self.add_schema(schema1)
        self.add_schema(schema2)
        self.assertResult({"anyOf": [
            {'type': ['null', 'string']},
            schema2
        ]})
