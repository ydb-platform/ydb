import functools

from flex.exceptions import (
    ValidationError,
    ErrorDict,
)


def any_validator(obj, validators, **kwargs):
    """
    Attempt multiple validators on an object.

    - If any pass, then all validation passes.
    - Otherwise, raise all of the errors.
    """
    if not len(validators) > 1:
        raise ValueError(
            "any_validator requires at least 2 validator.  Only got "
            "{0}".format(len(validators))
        )
    errors = ErrorDict()
    for key, validator in validators.items():
        try:
            validator(obj, **kwargs)
        except ValidationError as err:
            errors[key] = err.detail
        else:
            break
    else:
        if len(errors) == 1:
            # Special case for a single error.  Just raise it as if it was the
            # only validator run.
            error = errors.values()[0]
            raise ValidationError(error)
        else:
            # Raise all of the errors with the key namespaces.
            errors.raise_()


def generate_any_validator(**validators):
    return functools.partial(
        any_validator, validators=validators,
    )
