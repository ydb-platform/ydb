from classes.contrib.mypy.typeops.instance_context import InstanceContext
from classes.contrib.mypy.validation.validate_instance import (
    validate_instance_args,
    validate_runtime,
    validate_signature,
)


def check_type(instance_context: InstanceContext) -> bool:
    """Checks that instance definition is correct."""
    return all([
        validate_signature.check_type(instance_context),
        validate_runtime.check_type(instance_context),
        validate_instance_args.check_type(instance_context),
    ])
