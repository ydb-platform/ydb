from base64 import b64encode, b64decode
import binascii
from datetime import datetime
from uuid import UUID

from jsonschema._format import FormatChecker
from jsonschema.exceptions import FormatError
from six import binary_type, text_type, integer_types

DATETIME_HAS_RFC3339_VALIDATOR = False
DATETIME_HAS_STRICT_RFC3339 = False
DATETIME_HAS_ISODATE = False
DATETIME_RAISES = ()

try:
    import isodate
except ImportError:
    pass
else:
    DATETIME_HAS_ISODATE = True
    DATETIME_RAISES += (ValueError, isodate.ISO8601Error)

try:
    from rfc3339_validator import validate_rfc3339
except ImportError:
    pass
else:
    DATETIME_HAS_RFC3339_VALIDATOR = True
    DATETIME_RAISES += (ValueError, TypeError)

try:
    import strict_rfc3339
except ImportError:
    pass
else:
    DATETIME_HAS_STRICT_RFC3339 = True
    DATETIME_RAISES += (ValueError, TypeError)


def is_int32(instance):
    return isinstance(instance, integer_types)


def is_int64(instance):
    return isinstance(instance, integer_types)


def is_float(instance):
    return isinstance(instance, float)


def is_double(instance):
    # float has double precision in Python
    # It's double in CPython and Jython
    return isinstance(instance, float)


def is_binary(instance):
    return isinstance(instance, binary_type)


def is_byte(instance):
    if isinstance(instance, text_type):
        instance = instance.encode()

    try:
        return b64encode(b64decode(instance)) == instance
    except TypeError:
        return False


def is_datetime(instance):
    if not isinstance(instance, (binary_type, text_type)):
        return False

    if DATETIME_HAS_RFC3339_VALIDATOR:
        return validate_rfc3339(instance)

    if DATETIME_HAS_STRICT_RFC3339:
        return strict_rfc3339.validate_rfc3339(instance)

    if DATETIME_HAS_ISODATE:
        return isodate.parse_datetime(instance)

    return True


def is_date(instance):
    if not isinstance(instance, (binary_type, text_type)):
        return False

    if isinstance(instance, binary_type):
        instance = instance.decode()

    return datetime.strptime(instance, "%Y-%m-%d")


def is_uuid(instance):
    if not isinstance(instance, (binary_type, text_type)):
        return False

    if isinstance(instance, binary_type):
        instance = instance.decode()

    return text_type(UUID(instance)).lower() == instance.lower()


def is_password(instance):
    return True


class OASFormatChecker(FormatChecker):

    checkers = {
        'int32': (is_int32, ()),
        'int64': (is_int64, ()),
        'float': (is_float, ()),
        'double': (is_double, ()),
        'byte': (is_byte, (binascii.Error, TypeError)),
        'binary': (is_binary, ()),
        'date': (is_date, (ValueError, )),
        'date-time': (is_datetime, DATETIME_RAISES),
        'password': (is_password, ()),
        # non standard
        'uuid': (is_uuid, (AttributeError, ValueError)),
    }

    def check(self, instance, format):
        if format not in self.checkers:
            raise FormatError(
                "Format checker for %r format not found" % (format, ))

        func, raises = self.checkers[format]
        result, cause = None, None
        try:
            result = func(instance)
        except raises as e:
            cause = e

        if not result:
            raise FormatError(
                "%r is not a %r" % (instance, format), cause=cause,
            )
        return result


oas30_format_checker = OASFormatChecker()
