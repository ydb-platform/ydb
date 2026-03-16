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

import sys
from typing import Tuple

from .client import Elasticsearch as Elasticsearch
from .connection import Connection as Connection
from .connection import RequestsHttpConnection as RequestsHttpConnection
from .connection import Urllib3HttpConnection as Urllib3HttpConnection
from .connection_pool import ConnectionPool as ConnectionPool
from .connection_pool import ConnectionSelector as ConnectionSelector
from .connection_pool import RoundRobinSelector as RoundRobinSelector
from .exceptions import AuthenticationException as AuthenticationException
from .exceptions import AuthorizationException as AuthorizationException
from .exceptions import ConflictError as ConflictError
from .exceptions import ConnectionError as ConnectionError
from .exceptions import ConnectionTimeout as ConnectionTimeout
from .exceptions import (
    ElasticsearchDeprecationWarning as ElasticsearchDeprecationWarning,
)
from .exceptions import ElasticsearchException as ElasticsearchException
from .exceptions import ImproperlyConfigured as ImproperlyConfigured
from .exceptions import NotFoundError as NotFoundError
from .exceptions import RequestError as RequestError
from .exceptions import SerializationError as SerializationError
from .exceptions import SSLError as SSLError
from .exceptions import TransportError as TransportError
from .exceptions import UnsupportedProductError as UnsupportedProductError
from .serializer import JSONSerializer as JSONSerializer
from .transport import Transport as Transport

try:
    if sys.version_info < (3, 6):
        raise ImportError

    from ._async.client import AsyncElasticsearch as AsyncElasticsearch
    from ._async.http_aiohttp import AIOHttpConnection as AIOHttpConnection
    from ._async.transport import AsyncTransport as AsyncTransport
except (ImportError, SyntaxError):
    pass

VERSION: Tuple[int, int, int]
__version__: Tuple[int, int, int]
__versionstr__: str
