from jsonschema.exceptions import best_match

from openapi_schema_validator.validators import OAS30Validator


def validate(instance, schema, cls=OAS30Validator, *args, **kwargs):
    cls.check_schema(schema)
    validator = cls(schema, *args, **kwargs)
    error = best_match(validator.iter_errors(instance))
    if error is not None:
        raise error
