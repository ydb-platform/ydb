import enum
import typing
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from ipaddress import IPv4Address, IPv6Address
from uuid import UUID

_KnownPythonTypes = typing.Type[typing.Union[
    str, float, bool, bytes, int, dict, UUID, Decimal, datetime, date, time,
    timedelta, IPv4Address, IPv6Address,
]]


class OpenApiTypes(enum.Enum):
    """
    Basic types known to the OpenAPI specification or at least common format extension of it.

    - Use ``BYTE`` for base64-encoded data wrapped in a string
    - Use ``BINARY`` for raw binary data
    - Use ``OBJECT`` for arbitrary free-form object (usually a :py:class:`dict`)
    """
    #: Converted to ``{"type": "number"}``.
    NUMBER = enum.auto()
    #: Converted to ``{"type": "number", "format": "float"}``.
    #: Equivalent to :py:class:`float`.
    FLOAT = enum.auto()
    #: Converted to ``{"type": "number", "format": "double"}``.
    DOUBLE = enum.auto()
    #: Converted to ``{"type": "boolean"}``.
    #: Equivalent to :py:class:`bool`.
    BOOL = enum.auto()
    #: Converted to ``{"type": "string"}``.
    #: Equivalent to :py:class:`str`.
    STR = enum.auto()
    #: Converted to ``{"type": "string", "format": "byte"}``.
    #: Use this for base64-encoded data wrapped in a string.
    BYTE = enum.auto()
    #: Converted to ``{"type": "string", "format": "binary"}``.
    #: Equivalent to :py:class:`bytes`.
    #: Use this for raw binary data.
    BINARY = enum.auto()
    #: Converted to ``{"type": "string", "format": "password"}``.
    PASSWORD = enum.auto()
    #: Converted to ``{"type": "integer"}``.
    #: Equivalent to :py:class:`int`.
    INT = enum.auto()
    #: Converted to ``{"type": "integer", "format": "int32"}``.
    INT32 = enum.auto()
    #: Converted to ``{"type": "integer", "format": "int64"}``.
    INT64 = enum.auto()
    #: Converted to ``{"type": "string", "format": "uuid"}``.
    #: Equivalent to :py:class:`~uuid.UUID`.
    UUID = enum.auto()
    #: Converted to ``{"type": "string", "format": "uri"}``.
    URI = enum.auto()
    #: Converted to ``{"type": "string", "format": "uri-reference"}``.
    URI_REF = enum.auto()
    #: Converted to ``{"type": "string", "format": "uri-template"}``.
    URI_TPL = enum.auto()
    #: Converted to ``{"type": "string", "format": "iri"}``.
    IRI = enum.auto()
    #: Converted to ``{"type": "string", "format": "iri-reference"}``.
    IRI_REF = enum.auto()
    #: Converted to ``{"type": "string", "format": "ipv4"}``.
    #: Equivalent to :py:class:`~ipaddress.IPv4Address`.
    IP4 = enum.auto()
    #: Converted to ``{"type": "string", "format": "ipv6"}``.
    #: Equivalent to :py:class:`~ipaddress.IPv6Address`.
    IP6 = enum.auto()
    #: Converted to ``{"type": "string", "format": "hostname"}``.
    HOSTNAME = enum.auto()
    #: Converted to ``{"type": "string", "format": "idn-hostname"}``.
    IDN_HOSTNAME = enum.auto()
    #: Converted to ``{"type": "number", "format": "double"}``.
    #: The same as :py:attr:`~drf_spectacular.types.OpenApiTypes.DOUBLE`.
    #: Equivalent to :py:class:`~decimal.Decimal`.
    DECIMAL = enum.auto()
    #: Converted to ``{"type": "string", "format": "date-time"}``.
    #: Equivalent to :py:class:`~datetime.datetime`.
    DATETIME = enum.auto()
    #: Converted to ``{"type": "string", "format": "date"}``.
    #: Equivalent to :py:class:`~datetime.date`.
    DATE = enum.auto()
    #: Converted to ``{"type": "string", "format": "time"}``.
    #: Equivalent to :py:class:`~datetime.time`.
    TIME = enum.auto()
    #: Converted to ``{"type": "string", "format": "duration"}``.
    #: Equivalent to :py:class:`~datetime.timedelta`.
    #: Expressed according to ISO 8601.
    DURATION = enum.auto()
    #: Converted to ``{"type": "string", "format": "email"}``.
    EMAIL = enum.auto()
    #: Converted to ``{"type": "string", "format": "idn-email"}``.
    IDN_EMAIL = enum.auto()
    #: Converted to ``{"type": "string", "format": "json-pointer"}``.
    JSON_PTR = enum.auto()
    #: Converted to ``{"type": "string", "format": "relative-json-pointer"}``.
    JSON_PTR_REL = enum.auto()
    #: Converted to ``{"type": "string", "format": "regex"}``.
    REGEX = enum.auto()
    #: Converted to ``{"type": "object", ...}``.
    #: Use this for arbitrary free-form objects (usually a :py:class:`dict`).
    #: The ``additionalProperties`` item is added depending on the ``GENERIC_ADDITIONAL_PROPERTIES`` setting.
    OBJECT = enum.auto()
    #: Equivalent to :py:data:`None`.
    #: This signals that the request or response is empty.
    NONE = enum.auto()
    #: Converted to ``{}`` which sets no type and format.
    #: Equivalent to :py:class:`typing.Any`.
    ANY = enum.auto()


# make a copy with dict() before modifying returned dict
OPENAPI_TYPE_MAPPING = {
    OpenApiTypes.NUMBER: {'type': 'number'},
    OpenApiTypes.FLOAT: {'type': 'number', 'format': 'float'},
    OpenApiTypes.DOUBLE: {'type': 'number', 'format': 'double'},
    OpenApiTypes.BOOL: {'type': 'boolean'},
    OpenApiTypes.STR: {'type': 'string'},
    OpenApiTypes.BYTE: {'type': 'string', 'format': 'byte'},
    OpenApiTypes.BINARY: {'type': 'string', 'format': 'binary'},
    OpenApiTypes.PASSWORD: {'type': 'string', 'format': 'password'},
    OpenApiTypes.INT: {'type': 'integer'},
    OpenApiTypes.INT32: {'type': 'integer', 'format': 'int32'},
    OpenApiTypes.INT64: {'type': 'integer', 'format': 'int64'},
    OpenApiTypes.UUID: {'type': 'string', 'format': 'uuid'},
    OpenApiTypes.URI: {'type': 'string', 'format': 'uri'},
    OpenApiTypes.URI_REF: {'type': 'string', 'format': 'uri-reference'},
    OpenApiTypes.URI_TPL: {'type': 'string', 'format': 'uri-template'},
    OpenApiTypes.IRI: {'type': 'string', 'format': 'iri'},
    OpenApiTypes.IRI_REF: {'type': 'string', 'format': 'iri-reference'},
    OpenApiTypes.IP4: {'type': 'string', 'format': 'ipv4'},
    OpenApiTypes.IP6: {'type': 'string', 'format': 'ipv6'},
    OpenApiTypes.HOSTNAME: {'type': 'string', 'format': 'hostname'},
    OpenApiTypes.IDN_HOSTNAME: {'type': 'string', 'format': 'idn-hostname'},
    OpenApiTypes.DECIMAL: {'type': 'number', 'format': 'double'},
    OpenApiTypes.DATETIME: {'type': 'string', 'format': 'date-time'},
    OpenApiTypes.DATE: {'type': 'string', 'format': 'date'},
    OpenApiTypes.TIME: {'type': 'string', 'format': 'time'},
    OpenApiTypes.DURATION: {'type': 'string', 'format': 'duration'},  # ISO 8601
    OpenApiTypes.EMAIL: {'type': 'string', 'format': 'email'},
    OpenApiTypes.IDN_EMAIL: {'type': 'string', 'format': 'idn-email'},
    OpenApiTypes.JSON_PTR: {'type': 'string', 'format': 'json-pointer'},
    OpenApiTypes.JSON_PTR_REL: {'type': 'string', 'format': 'relative-json-pointer'},
    OpenApiTypes.REGEX: {'type': 'string', 'format': 'regex'},
    OpenApiTypes.ANY: {},
    OpenApiTypes.NONE: None,
    # OpenApiTypes.OBJECT is inserted at runtime due to dependency on settings
}

PYTHON_TYPE_MAPPING = {
    str: OpenApiTypes.STR,
    float: OpenApiTypes.DOUBLE,
    bool: OpenApiTypes.BOOL,
    bytes: OpenApiTypes.BINARY,
    int: OpenApiTypes.INT,
    UUID: OpenApiTypes.UUID,
    Decimal: OpenApiTypes.DECIMAL,
    datetime: OpenApiTypes.DATETIME,
    date: OpenApiTypes.DATE,
    time: OpenApiTypes.TIME,
    timedelta: OpenApiTypes.DURATION,
    IPv4Address: OpenApiTypes.IP4,
    IPv6Address: OpenApiTypes.IP6,
    dict: OpenApiTypes.OBJECT,
    typing.Any: OpenApiTypes.ANY,
    None: OpenApiTypes.NONE,
}

DJANGO_PATH_CONVERTER_MAPPING = {
    'int': OpenApiTypes.INT,
    'path': OpenApiTypes.STR,
    'slug': OpenApiTypes.STR,
    'str': OpenApiTypes.STR,
    'uuid': OpenApiTypes.UUID,
    'drf_format_suffix': OpenApiTypes.STR,
}
