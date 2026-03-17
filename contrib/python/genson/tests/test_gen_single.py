from . import base


class TestBasicTypes(base.SchemaNodeTestCase):

    def test_no_object(self):
        self.assertResult({})

    def test_string(self):
        self.add_object("string")
        self.assertResult({"type": "string"})

    def test_integer(self):
        self.add_object(1)
        self.assertResult({"type": "integer"})

    def test_number(self):
        self.add_object(1.1)
        self.assertResult({"type": "number"})

    def test_boolean(self):
        self.add_object(True)
        self.assertResult({"type": "boolean"})

    def test_null(self):
        self.add_object(None)
        self.assertResult({"type": "null"})


class TestArrayList(base.SchemaNodeTestCase):

    def setUp(self):
        base.SchemaNodeTestCase.setUp(self)

    def test_empty(self):
        self.add_object([])
        self.assertResult({"type": "array"})

    def test_monotype(self):
        self.add_object(["spam", "spam", "spam", "eggs", "spam"])
        self.assertResult({"type": "array", "items": {"type": "string"}})

    def test_multitype(self):
        self.add_object([1, "2", None, False])
        self.assertResult({
            "type": "array",
            "items": {
                "type": ["boolean", "integer", "null", "string"]}
        })
        self.assertObjectValidates([False, None, "2", 1])

    def test_nested(self):
        self.add_object([
            ["surprise"],
            ["fear", "surprise"],
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

    def setUp(self):
        base.SchemaNodeTestCase.setUp(self)

    def test_empty(self):
        self.add_schema({"type": "array", "items": []})

        self.add_object([])
        self.assertResult({"type": "array", "items": [{}]})

    def test_empty_schema(self):
        self.add_schema({"type": "array", "items": [{}]})

        self.add_object([])
        self.assertResult({"type": "array", "items": [{}]})

    def test_multitype(self):
        self.add_schema({"type": "array", "items": []})

        self.add_object([1, "2", "3", None, False])

        self.assertResult({
            "type": "array",
            "items": [
                {"type": "integer"},
                {"type": "string"},
                {"type": "string"},
                {"type": "null"},
                {"type": "boolean"}]
        })
        self.assertObjectDoesNotValidate([1, 2, "3", None, False])

    def test_nested(self):
        self.add_schema(
            {"type": "array", "items": {"type": "array", "items": []}})

        self.add_object([
            ["surprise"],
            ["fear", "surprise"],
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


class TestObject(base.SchemaNodeTestCase):

    def test_empty_object(self):
        self.add_object({})
        self.assertResult({"type": "object"})

    def test_basic_object(self):
        self.add_object({
            "Red Windsor": "Normally, but today the van broke down.",
            "Stilton": "Sorry.",
            "Gruyere": False})
        self.assertResult({
            "required": ["Gruyere", "Red Windsor", "Stilton"],
            "type": "object",
            "properties": {
                "Red Windsor": {"type": "string"},
                "Gruyere": {"type": "boolean"},
                "Stilton": {"type": "string"}
            }
        })


class TestComplex(base.SchemaNodeTestCase):

    def test_array_in_object(self):
        self.add_object({"a": "b", "c": [1, 2, 3]})
        self.assertResult({
            "required": ["a", "c"],
            "type": "object",
            "properties": {
                "a": {"type": "string"},
                "c": {
                    "type": "array",
                    "items": {"type": "integer"}
                }
            }
        })

    def test_object_in_array(self):
        self.add_object([
            {"name": "Sir Lancelot of Camelot",
             "quest": "to seek the Holy Grail",
             "favorite colour": "blue"},
            {"name": "Sir Robin of Camelot",
             "quest": "to seek the Holy Grail",
             "capitol of Assyria": None}])
        self.assertResult({
            "type": "array",
            "items": {
                "type": "object",
                "required": ["name", "quest"],
                "properties": {
                    "quest": {"type": "string"},
                    "name": {"type": "string"},
                    "favorite colour": {"type": "string"},
                    "capitol of Assyria": {"type": "null"}
                }
            }
        })

    def test_three_deep(self):
        self.add_object({"matryoshka": {"design": {"principle": "FTW!"}}})
        self.assertResult({
            "type": "object",
            "required": ["matryoshka"],
            "properties": {
                "matryoshka": {
                    "type": "object",
                    "required": ["design"],
                    "properties": {
                        "design": {
                            "type": "object",
                            "required": ["principle"],
                            "properties": {
                                "principle": {"type": "string"}
                            }
                        }
                    }
                }
            }
        })
