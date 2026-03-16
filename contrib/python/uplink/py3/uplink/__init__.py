# Local imports
from uplink import returns, types
from uplink.__about__ import __version__
from uplink._extras import install
from uplink._extras import load_entry_points as _load_entry_points
from uplink.arguments import (
    Body,
    Context,
    Field,
    FieldMap,
    Header,
    HeaderMap,
    Part,
    PartMap,
    Path,
    Query,
    QueryMap,
    Timeout,
    Url,
)
from uplink.builder import Consumer, build
from uplink.clients import AiohttpClient, RequestsClient, TwistedClient
from uplink.commands import delete, get, head, patch, post, put

# todo: remove this in v1.0.0
from uplink.converters import MarshmallowConverter
from uplink.decorators import (
    args,
    error_handler,
    form_url_encoded,
    headers,
    inject,
    json,
    multipart,
    params,
    response_handler,
    timeout,
)
from uplink.exceptions import (
    AnnotationError,
    Error,
    InvalidRequestDefinition,
    UplinkBuilderError,
)
from uplink.models import dumps, loads
from uplink.ratelimit import ratelimit
from uplink.retry import retry

__all__ = [
    "AiohttpClient",
    "AnnotationError",
    "Body",
    "Consumer",
    "Context",
    "Error",
    "Field",
    "FieldMap",
    "Header",
    "HeaderMap",
    "InvalidRequestDefinition",
    "MarshmallowConverter",
    "Part",
    "PartMap",
    "Path",
    "Query",
    "QueryMap",
    "RequestsClient",
    "Timeout",
    "TwistedClient",
    "UplinkBuilderError",
    "Url",
    "__version__",
    "args",
    "build",
    "delete",
    "dumps",
    "error_handler",
    "form_url_encoded",
    "get",
    "head",
    "headers",
    "inject",
    "install",
    "json",
    "loads",
    "multipart",
    "params",
    "patch",
    "post",
    "put",
    "ratelimit",
    "response_handler",
    "retry",
    "returns",
    "timeout",
    "types",
]

_load_entry_points()
