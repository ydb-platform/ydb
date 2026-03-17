"""Tools for specifying BSON codec options."""

import collections

from packaging import version

from mongomock import helpers

try:
    from bson import codec_options
except ImportError:
    codec_options = None


class TypeRegistry:
    pass


_FIELDS = (
    "document_class",
    "tz_aware",
    "uuid_representation",
    "unicode_decode_error_handler",
    "tzinfo",
)

if codec_options and helpers.PYMONGO_VERSION >= version.parse("3.8"):
    _DEFAULT_TYPE_REGISTRY = codec_options.TypeRegistry()
    _FIELDS += ("type_registry",)
else:
    _DEFAULT_TYPE_REGISTRY = TypeRegistry()

if codec_options and helpers.PYMONGO_VERSION >= version.parse("4.3"):
    _DEFAULT_DATETIME_CONVERSION = codec_options.DatetimeConversion.DATETIME
    _FIELDS += ("datetime_conversion",)
else:
    _DEFAULT_DATETIME_CONVERSION = 1

# New default in Pymongo v4:
# https://pymongo.readthedocs.io/en/stable/examples/uuid.html#unspecified
if helpers.PYMONGO_VERSION >= version.parse("4.0"):
    _DEFAULT_UUID_REPRESENTATION = 0
else:
    _DEFAULT_UUID_REPRESENTATION = 3


class CodecOptions(collections.namedtuple("CodecOptions", _FIELDS)):
    def __new__(
        cls,
        document_class=dict,
        tz_aware=False,
        uuid_representation=_DEFAULT_UUID_REPRESENTATION,
        unicode_decode_error_handler="strict",
        tzinfo=None,
        type_registry=_DEFAULT_TYPE_REGISTRY,
        datetime_conversion=_DEFAULT_DATETIME_CONVERSION,
    ):
        if document_class != dict:
            raise NotImplementedError(
                'Mongomock does not implement custom document_class yet: %r' % document_class)

        if not isinstance(tz_aware, bool):
            raise TypeError('tz_aware must be True or False')

        if uuid_representation != _DEFAULT_UUID_REPRESENTATION:
            raise NotImplementedError('Mongomock does not handle custom uuid_representation yet')

        if unicode_decode_error_handler not in ('strict', None):
            raise NotImplementedError(
                'Mongomock does not handle custom unicode_decode_error_handler yet')

        if tzinfo is not None:
            raise NotImplementedError("Mongomock does not handle custom tzinfo yet")

        values = (
            document_class,
            tz_aware,
            uuid_representation,
            unicode_decode_error_handler,
            tzinfo,
        )

        if "type_registry" in _FIELDS:
            type_registry = type_registry or _DEFAULT_TYPE_REGISTRY
            if type_registry != _DEFAULT_TYPE_REGISTRY:
                raise NotImplementedError(
                    "Mongomock does not handle custom type_registry yet %r"
                    % type_registry
                )
            values += (type_registry,)

        if "datetime_conversion" in _FIELDS:
            datetime_conversion = datetime_conversion or _DEFAULT_DATETIME_CONVERSION
            if datetime_conversion != _DEFAULT_DATETIME_CONVERSION:
                raise NotImplementedError(
                    f"Mongomock does not handle custom datetime_conversion "
                    f"yet {datetime_conversion}"
                )
            values += (datetime_conversion,)

        return tuple.__new__(cls, values)

    def with_options(self, **kwargs):
        opts = self._asdict()
        opts.update(kwargs)
        return CodecOptions(**opts)


def is_supported(custom_codec_options):

    if not custom_codec_options:
        return None

    return CodecOptions(**custom_codec_options._asdict())
