"""OpenAPI spec validator generators module."""
import logging

from six import iteritems
from jsonschema.validators import Draft4Validator

from openapi_spec_validator.decorators import DerefValidatorDecorator

log = logging.getLogger(__name__)


class SpecValidatorsGeneratorFactory:
    """Generator factory for customized validators that follows $refs
    in the schema being validated.
    """
    validators = {
        '$ref',
        'properties',
        'additionalProperties',
        'patternProperties',
        'type',
        'dependencies',
        'required',
        'minProperties',
        'maxProperties',
        'allOf',
        'oneOf',
        'anyOf',
        'not',
    }

    @classmethod
    def from_spec_resolver(cls, spec_resolver):
        """Creates validators generator for the spec resolver.

        :param spec_resolver: resolver for the spec
        :type instance_resolver: :class:`jsonschema.RefResolver`
        """
        deref = DerefValidatorDecorator(spec_resolver)
        for key, validator_callable in iteritems(Draft4Validator.VALIDATORS):
            if key in cls.validators:
                yield key, deref(validator_callable)
