#  Licensed to Elasticsearch B.V. under one or more contributor
#  license agreements. See the NOTICE file distributed with
#  this work for additional information regarding copyright
#  ownership. Elasticsearch B.V. licenses this file to you under
#  the Apache License, Version 2.0 (the "License"); you may
#  not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

# flake8: noqa
from __future__ import absolute_import

import logging
import re
import sys
import warnings

from ._version import __versionstr__

_major, _minor, _patch = [
    int(x) for x in re.search(r"^(\d+)\.(\d+)\.(\d+)", __versionstr__).groups()
]
VERSION = __version__ = (_major, _minor, _patch)

logger = logging.getLogger("elasticsearch")
logger.addHandler(logging.NullHandler())

from .client import Elasticsearch
from .connection import Connection, RequestsHttpConnection, Urllib3HttpConnection
from .connection_pool import ConnectionPool, ConnectionSelector, RoundRobinSelector
from .exceptions import (
    AuthenticationException,
    AuthorizationException,
    ConflictError,
    ConnectionError,
    ConnectionTimeout,
    ElasticsearchDeprecationWarning,
    ElasticsearchException,
    ElasticsearchWarning,
    ImproperlyConfigured,
    NotFoundError,
    RequestError,
    SerializationError,
    SSLError,
    TransportError,
    UnsupportedProductError,
)
from .serializer import JSONSerializer
from .transport import Transport

# Only raise one warning per deprecation message so as not
# to spam up the user if the same action is done multiple times.
warnings.simplefilter("default", category=ElasticsearchDeprecationWarning, append=True)

__all__ = [
    "Elasticsearch",
    "Transport",
    "ConnectionPool",
    "ConnectionSelector",
    "RoundRobinSelector",
    "JSONSerializer",
    "Connection",
    "RequestsHttpConnection",
    "Urllib3HttpConnection",
    "ImproperlyConfigured",
    "ElasticsearchException",
    "SerializationError",
    "TransportError",
    "NotFoundError",
    "ConflictError",
    "RequestError",
    "ConnectionError",
    "SSLError",
    "ConnectionTimeout",
    "AuthenticationException",
    "AuthorizationException",
    "UnsupportedProductError",
    "ElasticsearchWarning",
    "ElasticsearchDeprecationWarning",
]

try:
    # Asyncio only supported on Python 3.6+
    if sys.version_info < (3, 6):
        raise ImportError

    from ._async.client import AsyncElasticsearch
    from ._async.http_aiohttp import AIOHttpConnection, AsyncConnection
    from ._async.transport import AsyncTransport

    __all__ += [
        "AIOHttpConnection",
        "AsyncConnection",
        "AsyncTransport",
        "AsyncElasticsearch",
    ]
except (ImportError, SyntaxError):
    pass

# Python earlier than 3.6 is deprecated and will be removed in 8.0.0
if sys.version_info < (3, 6):
    warnings.warn(
        "Support for Python 3.5 and earlier is deprecated and will be removed "
        "in v8.0.0 (current instance is Python %d.%d) See https://github.com/elastic"
        "/elasticsearch-py/issues/1696 for details." % sys.version_info[:2],
        category=DeprecationWarning,
        stacklevel=2,
    )
