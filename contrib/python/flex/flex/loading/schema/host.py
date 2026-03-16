import re

from flex.datastructures import (
    ValidationList,
)
from flex.constants import (
    STRING,
)
from flex.error_messages import MESSAGES
from flex.validation.common import (
    generate_object_validator,
    generate_type_validator,
)
from flex.decorators import (
    skip_if_empty,
    skip_if_not_of_type,
)
from flex.context_managers import ErrorDict


string_type_validator = generate_type_validator(STRING)


def decompose_hostname(value):
    scheme, _, right = value.rpartition('://')
    left, _, right = right.partition(':')
    if right:
        hostname = left
        port, _, path = right.partition('/')
    else:
        port = ''
        hostname, _, path = left.partition('/')

    return scheme, hostname, port, path


@skip_if_empty
@skip_if_not_of_type(STRING)
def host_validator(value, **kwargs):
    """
    From: http://stackoverflow.com/questions/2532053/validate-a-hostname-string
    According to: http://en.wikipedia.org/wiki/Hostname#Restrictions_on_valid_host_names
    """
    scheme, hostname, port, path = decompose_hostname(value)

    if len(hostname) > 255:
        return False
    if hostname[-1] == ".":
        hostname = hostname[:-1]  # strip exactly one dot from the right, if present
    allowed = re.compile("(?!-)[A-Z\d-]{1,63}(?<!-)$", re.IGNORECASE)

    with ErrorDict() as errors:
        if not all(allowed.match(x) for x in hostname.split(".")):
            errors.add_error(
                'invalid',
                MESSAGES['host']['invalid'].format(value),
            )

        if path:
            errors.add_error(
                'path',
                MESSAGES['host']['may_not_include_path'].format(value),
            )

        if scheme:
            errors.add_error(
                'scheme',
                MESSAGES['host']['may_not_include_scheme'].format(value),
            )


host_schema = {
    'type': STRING,
}

non_field_validators = ValidationList()
non_field_validators.add_validator(host_validator)

host_validator = generate_object_validator(
    schema=host_schema,
    non_field_validators=non_field_validators,
)
