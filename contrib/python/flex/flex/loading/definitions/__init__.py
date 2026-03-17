from six.moves import urllib_parse as urlparse

import jsonpointer

from flex.exceptions import ErrorDict
from flex.error_messages import MESSAGES
from flex.datastructures import (
    ValidationDict,
)
from flex.constants import (
    OBJECT,
)
from flex.validation.common import (
    generate_object_validator,
)
from flex.decorators import (
    skip_if_not_of_type,
)

from .schema_definitions import schema_definitions_validator
from .parameters import (
    parameters_validator,
)
from .responses import (
    responses_validator,
)


__ALL__ = [
    'schema_definitions_validator',
    'parameters_validator',
    'responses_validator',
]

definitions_schema = {
    'type': OBJECT,
}


@skip_if_not_of_type(OBJECT)
def validate_deferred_references(schema, context, **kwargs):
    try:
        deferred_references = context['deferred_references']
    except KeyError:
        raise KeyError("`deferred_references` not found in context")

    with ErrorDict() as errors:
        for reference in deferred_references:
            parts = urlparse.urlparse(reference)
            if any((parts.scheme, parts.netloc, parts.path, parts.params, parts.query)):
                errors.add_error(
                    reference,
                    MESSAGES['reference']['unsupported'].format(reference),
                )
                continue
            try:
                jsonpointer.resolve_pointer(schema, parts.fragment)
            except jsonpointer.JsonPointerException:
                errors.add_error(
                    reference,
                    MESSAGES['reference']['undefined'].format(reference),
                )


field_validators = ValidationDict()
field_validators.add_property_validator('parameters', parameters_validator)
field_validators.add_property_validator('responses', responses_validator)
field_validators.add_property_validator('definitions', schema_definitions_validator)

non_field_validators = ValidationDict()
non_field_validators.add_validator('definitions', validate_deferred_references)


definitions_validator = generate_object_validator(
    schema=definitions_schema,
    field_validators=field_validators,
    non_field_validators=non_field_validators,
)
