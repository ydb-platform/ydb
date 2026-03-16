from openapi_core.casting.schemas.casters import (
    PrimitiveCaster, DummyCaster, ArrayCaster
)
from openapi_core.casting.schemas.util import forcebool


class SchemaCastersFactory(object):

    DUMMY_CASTERS = [
        'string', 'object', 'any',
    ]
    PRIMITIVE_CASTERS = {
        'integer': int,
        'number': float,
        'boolean': forcebool,
    }
    COMPLEX_CASTERS = {
        'array': ArrayCaster,
    }

    def create(self, schema):
        schema_type = schema.getkey('type', 'any')
        if schema_type in self.DUMMY_CASTERS:
            return DummyCaster()
        elif schema_type in self.PRIMITIVE_CASTERS:
            caster_callable = self.PRIMITIVE_CASTERS[schema_type]
            return PrimitiveCaster(schema, caster_callable)
        elif schema_type in self.COMPLEX_CASTERS:
            caster_class = self.COMPLEX_CASTERS[schema_type]
            return caster_class(schema, self)
