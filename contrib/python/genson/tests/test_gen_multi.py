from . import base


class TestBasicTypes(base.SchemaNodeTestCase):

    def test_single_type(self):
        self.add_object("bacon")
        self.add_object("egg")
        self.add_object("spam")
        self.assertResult({"type": "string"})

    def test_redundant_integer_type(self):
        self.add_object(1)
        self.add_object(1.1)
        self.assertResult({"type": "number"})


class TestAnyOf(base.SchemaNodeTestCase):

    def test_simple(self):
        self.add_object("string")
        self.add_object(1.1)
        self.add_object(True)
        self.add_object(None)
        self.assertResult({"type": ["boolean", "null", "number", "string"]})

    def test_complex(self):
        self.add_object({})
        self.add_object([None])
        self.assertResult({"anyOf": [
            {"type": "object"},
            {"type": "array", "items": {"type": "null"}}
        ]})

    def test_simple_and_complex(self):
        self.add_object(None)
        self.add_object([None])
        self.assertResult({"anyOf": [
            {"type": "null"},
            {"type": "array", "items": {"type": "null"}}
        ]})


class TestArrayList(base.SchemaNodeTestCase):

    def setUp(self):
        base.SchemaNodeTestCase.setUp(self)

    def test_empty(self):
        self.add_object([])
        self.add_object([])

        self.assertResult({"type": "array"})

    def test_monotype(self):
        self.add_object(["spam", "spam", "spam", "eggs", "spam"])
        self.add_object(["spam", "bacon", "eggs", "spam"])

        self.assertResult({"type": "array", "items": {"type": "string"}})

    def test_multitype(self):
        self.add_object([1, "2", "3", None, False])
        self.add_object([1, 2, "3", False])

        self.assertObjectValidates([1, "2", 3, None, False])
        self.assertResult({
            "type": "array",
            "items": {
                "type": ["boolean", "integer", "null", "string"]}
        })

    def test_nested(self):
        self.add_object([
            ["surprise"],
            ["fear", "surprise"]
        ])
        self.add_object([
            ["fear", "surprise", "ruthless efficiency"],
            ["fear", "surprise", "ruthless efficiency",
             "an almost fanatical devotion to the Pope"]
        ])
        self.assertResult({
            "type": "array",
            "items": {
                "type": "array",
                "items": {"type": "string"}}
        })


class TestArrayTuple(base.SchemaNodeTestCase):

    def test_empty(self):
        self.add_schema({"type": "array", "items": []})

        self.add_object([])
        self.add_object([])

        self.assertResult({"type": "array", "items": [{}]})

    def test_multitype(self):
        self.add_schema({"type": "array", "items": []})

        self.add_object([1, "2", "3", None, False])
        self.add_object([1, 2, "3", False])

        self.assertObjectDoesNotValidate([1, "2", 3, None, False])
        self.assertResult({
            "type": "array",
            "items": [
                {"type": "integer"},
                {"type": ["integer", "string"]},
                {"type": "string"},
                {"type": ["boolean", "null"]},
                {"type": "boolean"}]
        })

    def test_nested(self):
        self.add_schema(
            {"type": "array", "items": {"type": "array", "items": []}})

        self.add_object([
            ["surprise"],
            ["fear", "surprise"]
        ])
        self.add_object([
            ["fear", "surprise", "ruthless efficiency"],
            ["fear", "surprise", "ruthless efficiency",
             "an almost fanatical devotion to the Pope"]
        ])

        self.assertResult({
            "type": "array",
            "items": {
                "type": "array",
                "items": [
                    {"type": "string"},
                    {"type": "string"},
                    {"type": "string"},
                    {"type": "string"}
                ]
            }
        })
