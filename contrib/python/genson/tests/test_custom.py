from genson import SchemaBuilder
from genson.schema.strategies import SchemaStrategy, Number
from . import base


class MaxTenStrategy(Number):
    KEYWORDS = tuple(list(Number.KEYWORDS) + ['maximum'])

    def to_schema(self):
        schema = super().to_schema()
        schema['maximum'] = 10
        return schema


class FalseStrategy(SchemaStrategy):
    KEYWORDS = tuple(list(SchemaStrategy.KEYWORDS) + ['const'])

    @classmethod
    def match_schema(self, schema):
        return True

    @classmethod
    def match_object(self, obj):
        return True

    def to_schema(self):
        schema = super().to_schema()
        schema['type'] = 'boolean'
        schema['const'] = False
        return schema


class MaxTenSchemaBuilder(SchemaBuilder):
    EXTRA_STRATEGIES = (MaxTenStrategy,)


class FalseSchemaBuilder(SchemaBuilder):
    STRATEGIES = (FalseStrategy,)


class TestExtraStrategies(base.SchemaNodeTestCase):
    CLASS = MaxTenSchemaBuilder

    def test_add_object(self):
        self.add_object(5)
        self.assertResult({
            '$schema': 'http://json-schema.org/schema#',
            'type': 'integer',
            'maximum': 10})

    def test_add_schema(self):
        self.add_schema({'type': 'integer'})
        self.assertResult({
            '$schema': 'http://json-schema.org/schema#',
            'type': 'integer',
            'maximum': 10})


class TestClobberStrategies(base.SchemaNodeTestCase):
    CLASS = FalseSchemaBuilder

    def test_add_object(self):
        self.add_object("Any Norwegian Jarlsberger?")
        self.assertResult({
            '$schema': 'http://json-schema.org/schema#',
            'type': 'boolean',
            'const': False}, enforceUserContract=False)

    def test_add_schema(self):
        self.add_schema({'type': 'string'})
        self.assertResult({
            '$schema': 'http://json-schema.org/schema#',
            'type': 'boolean',
            'const': False}, enforceUserContract=False)
