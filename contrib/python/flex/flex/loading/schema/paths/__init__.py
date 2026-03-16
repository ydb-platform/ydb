from flex.datastructures import (
    ValidationList,
)
from flex.exceptions import ValidationError
from flex.constants import (
    OBJECT,
)
from flex.error_messages import MESSAGES
from flex.decorators import (
    skip_if_not_of_type,
    skip_if_empty,
)
from flex.validation.common import (
    generate_object_validator,
)
from flex.context_managers import (
    ErrorDict,
)
from .path_item import (
    path_item_validator,
)


@skip_if_empty
@skip_if_not_of_type(OBJECT)
def validate_path_items(paths, **kwargs):
    with ErrorDict() as errors:
        for path, path_definition in paths.items():
            # TODO: move this to its own validation function that validates the keys.
            if not path.startswith('/'):
                errors.add_error(path, MESSAGES['path']['must_start_with_slash'])

            try:
                path_item_validator(path_definition, **kwargs)
            except ValidationError as err:
                errors.add_error(path, err.detail)


paths_schema = {
    'type': OBJECT,
}
non_field_validators = ValidationList()
non_field_validators.add_validator(validate_path_items)

paths_validator = generate_object_validator(
    schema=paths_schema,
    non_field_validators=non_field_validators,
)
