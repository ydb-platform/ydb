import collections

from flex._compat import Mapping
from flex.exceptions import (
    ValidationError,
    ErrorDict,
    ErrorList,
)
from flex.constants import (
    OBJECT,
)
from flex.utils import (
    is_non_string_iterable,
)
from flex.decorators import (
    skip_if_not_of_type,
    skip_if_empty,
)
from flex.functional import (
    apply_functions_to_key,
)


class ValidationList(list):
    def __init__(self, value=None):
        super(ValidationList, self).__init__()
        if value:
            self.add_validator(value)

    def add_validator(self, validator):
        if is_non_string_iterable(validator)\
           and not isinstance(validator, Mapping):
            for value in validator:
                self.add_validator(value)
        else:
            self.append(validator)

    def validate_object(self, obj, **kwargs):
        with ErrorList() as errors:
            for validator in self:
                try:
                    validator(obj, **kwargs)
                except ValidationError as err:
                    errors.add_error(err.detail)

    def __call__(self, *args, **kwargs):
        return self.validate_object(*args, **kwargs)


class ValidationDict(collections.defaultdict):
    def __init__(self, validators=None):
        super(ValidationDict, self).__init__(ValidationList)
        if validators is not None:
            if not isinstance(validators, Mapping):
                raise ValueError("ValidationDict may only be instantiated with a mapping")
            for key, validator in validators.items():
                self.add_validator(key, validator)

    def add_validator(self, key, validator):
        self[key].add_validator(validator)

    def add_property_validator(self, key, validator):
        self.add_validator(
            key,
            skip_if_empty(skip_if_not_of_type(OBJECT)(apply_functions_to_key(key, validator)))
        )

    def update(self, other):
        for key, value in other.items():
            self.add_validator(key, value)

    def validate_object(self, obj, **kwargs):
        with ErrorDict() as errors:
            for key, validator in self.items():
                try:
                    validator(obj, **kwargs)
                except ValidationError as err:
                    errors.add_error(key, err.detail)

    def __call__(self, *args, **kwargs):
        return self.validate_object(*args, **kwargs)
