# -*- coding: utf-8 -*-
import logging
import typing
from importlib import import_module

from bravado_core.operation import Operation
from bravado_core.response import IncomingResponse

from bravado.response import BravadoResponseMetadata

try:
    from typing import Type
except ImportError:
    # Python 3.5.0 / 3.5.1
    from typing_extensions import Type


log = logging.getLogger(__name__)


CONFIG_DEFAULTS = {
    # See the constructor of :class:`bravado.http_future.HttpFuture` for an
    # in depth explanation of what this means.
    'also_return_response': False,
    # Kill switch to disable returning fallback results even if provided.
    'disable_fallback_results': False,
    'response_metadata_class': 'bravado.response.BravadoResponseMetadata',
    # Headers excluded from debug logs
    'sensitive_headers': ['Authorization'],
}


BravadoConfig = typing.NamedTuple(
    'BravadoConfig',
    (
        ('also_return_response', bool),
        ('disable_fallback_results', bool),
        ('response_metadata_class', Type[BravadoResponseMetadata]),
        ('sensitive_headers', list),
    ),
)


def bravado_config_from_config_dict(config_dict):
    # type: (typing.Mapping[str, typing.Any]) -> 'BravadoConfig'
    if config_dict is None:
        config_dict = {}
    bravado_config = {key: value for key, value in config_dict.items() if key in BravadoConfig._fields}
    bravado_config = dict(CONFIG_DEFAULTS, **bravado_config)
    bravado_config['response_metadata_class'] = _get_response_metadata_class(
        bravado_config['response_metadata_class'],
    )
    return BravadoConfig(
        **bravado_config
    )


class RequestConfig(object):

    also_return_response = False  # type: bool
    force_fallback_result = False  # type: bool

    # List of callbacks that are executed after the incoming response has been
    # validated and the swagger_result has been unmarshalled.
    #
    # The callback should expect two arguments:
    #   param : incoming_response
    #   type  : subclass of class:`bravado_core.response.IncomingResponse`
    #   param : operation
    #   type  : class:`bravado_core.operation.Operation`
    response_callbacks = []  # type: typing.List[typing.Callable[[IncomingResponse, Operation], None]]

    # options used to construct the request params
    connect_timeout = None  # type: typing.Optional[float]
    headers = {}  # type: typing.Mapping[str, str]
    use_msgpack = False  # type: bool
    timeout = None  # type: typing.Optional[float]

    # Extra options passed in that we don't know about
    additional_properties = {}  # type: typing.Mapping[str, typing.Any]

    def __init__(self, request_options, also_return_response_default):
        # type: (typing.Dict[str, typing.Any], bool) -> None
        request_options = request_options.copy()  # don't modify the original object
        self.also_return_response = also_return_response_default

        for key in list(request_options.keys()):
            if hasattr(self, key):
                setattr(self, key, request_options.pop(key))

        self.additional_properties = request_options


def _get_response_metadata_class(fully_qualified_class_str):
    # type: (str) -> typing.Type[BravadoResponseMetadata]

    class_to_import = _import_class(fully_qualified_class_str)
    if not class_to_import:
        return BravadoResponseMetadata

    if issubclass(class_to_import, BravadoResponseMetadata):
        return class_to_import

    log.warning(
        'bravado configuration error: the metadata class \'%s\' does not extend '
        'BravadoResponseMetadata. Using default class instead.',
        fully_qualified_class_str,
    )
    return BravadoResponseMetadata


def _import_class(fully_qualified_class_str):
    # type: (str) -> typing.Optional[typing.Type]
    try:
        module_name, class_name = fully_qualified_class_str.rsplit('.', 1)
        return getattr(import_module(module_name), class_name)
    except (ImportError, AttributeError, ValueError) as e:
        log.warning(
            'Error while importing \'%s\': %s: %s',
            fully_qualified_class_str,
            type(e),
            str(e),
        )
        return None
