from flex.constants import (
    STRING,
)
from flex.datastructures import (
    ValidationList,
)
from flex.validation.common import (
    generate_object_validator,
)
from flex.decorators import (
    skip_if_not_of_type,
    skip_if_empty,
)


@skip_if_empty
@skip_if_not_of_type(STRING)
def defer_reference(reference, context, **kwargs):
    try:
        deferred_references = context['deferred_references']
    except KeyError:
        raise KeyError("`deferred_references` not found in validation context")
    deferred_references.add(reference)


ref_schema = {
    'type': STRING,
}

non_field_validators = ValidationList()
non_field_validators.add_validator(defer_reference)

ref_validator = generate_object_validator(
    schema=ref_schema,
    non_field_validators=non_field_validators,
)
