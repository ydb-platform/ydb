# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
#
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

import logging
import re
import warnings

from ._version import __versionstr__

_major, _minor, _patch = (
    int(x) for x in re.search(r"^(\d+)\.(\d+)\.(\d+)", __versionstr__).groups()  # type: ignore
)

VERSION = __version__ = (_major, _minor, _patch)

logger = logging.getLogger("opensearch")
logger.addHandler(logging.NullHandler())

from .client import OpenSearch
from .connection import (
    Connection,
    RequestsHttpConnection,
    Urllib3HttpConnection,
    connections,
)
from .connection_pool import ConnectionPool, ConnectionSelector, RoundRobinSelector
from .exceptions import (
    AuthenticationException,
    AuthorizationException,
    ConflictError,
    ConnectionError,
    ConnectionTimeout,
    IllegalOperation,
    ImproperlyConfigured,
    NotFoundError,
    OpenSearchDeprecationWarning,
    OpenSearchDslException,
    OpenSearchException,
    OpenSearchWarning,
    RequestError,
    SerializationError,
    SSLError,
    TransportError,
    UnknownDslObject,
    ValidationException,
)
from .helpers import AWSV4SignerAuth, RequestsAWSV4SignerAuth, Urllib3AWSV4SignerAuth
from .helpers.aggs import A
from .helpers.analysis import analyzer, char_filter, normalizer, token_filter, tokenizer
from .helpers.document import Document, InnerDoc, MetaField
from .helpers.faceted_search import (
    DateHistogramFacet,
    Facet,
    FacetedResponse,
    FacetedSearch,
    HistogramFacet,
    NestedFacet,
    RangeFacet,
    TermsFacet,
)
from .helpers.field import (
    Binary,
    Boolean,
    Byte,
    Completion,
    CustomField,
    Date,
    DateRange,
    Double,
    DoubleRange,
    Field,
    Float,
    FloatRange,
    GeoPoint,
    GeoShape,
    HalfFloat,
    Integer,
    IntegerRange,
    Ip,
    IpRange,
    Join,
    Keyword,
    KnnVector,
    Long,
    LongRange,
    Murmur3,
    Nested,
    Object,
    Percolator,
    RangeField,
    RankFeature,
    RankFeatures,
    ScaledFloat,
    SearchAsYouType,
    Short,
    SparseVector,
    Text,
    TokenCount,
    construct_field,
)
from .helpers.function import SF
from .helpers.index import Index, IndexTemplate
from .helpers.mapping import Mapping
from .helpers.query import Q
from .helpers.search import MultiSearch, Search
from .helpers.update_by_query import UpdateByQuery
from .helpers.utils import AttrDict, AttrList, DslBase
from .helpers.wrappers import Range
from .metrics import Metrics, MetricsEvents, MetricsNone
from .serializer import JSONSerializer
from .transport import Transport

# Only raise one warning per deprecation message so as not
# to spam up the user if the same action is done multiple times.
warnings.simplefilter("default", category=OpenSearchDeprecationWarning, append=True)

__all__ = [
    "OpenSearch",
    "Transport",
    "ConnectionPool",
    "ConnectionSelector",
    "RoundRobinSelector",
    "JSONSerializer",
    "Connection",
    "RequestsHttpConnection",
    "Urllib3HttpConnection",
    "ImproperlyConfigured",
    "OpenSearchException",
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
    "OpenSearchWarning",
    "OpenSearchDeprecationWarning",
    "AWSV4SignerAuth",
    "Urllib3AWSV4SignerAuth",
    "RequestsAWSV4SignerAuth",
    "A",
    "AttrDict",
    "AttrList",
    "Binary",
    "Boolean",
    "Byte",
    "Completion",
    "CustomField",
    "Date",
    "DateHistogramFacet",
    "DateRange",
    "KnnVector",
    "Document",
    "Double",
    "DoubleRange",
    "DslBase",
    "Facet",
    "FacetedResponse",
    "FacetedSearch",
    "Field",
    "Float",
    "FloatRange",
    "GeoPoint",
    "GeoShape",
    "HalfFloat",
    "HistogramFacet",
    "IllegalOperation",
    "Index",
    "IndexTemplate",
    "InnerDoc",
    "Integer",
    "IntegerRange",
    "Ip",
    "IpRange",
    "Join",
    "Keyword",
    "Long",
    "LongRange",
    "Mapping",
    "MetaField",
    "MultiSearch",
    "Murmur3",
    "Nested",
    "NestedFacet",
    "Object",
    "OpenSearchDslException",
    "Percolator",
    "Q",
    "Range",
    "RangeFacet",
    "RangeField",
    "RankFeature",
    "RankFeatures",
    "SF",
    "ScaledFloat",
    "Search",
    "SearchAsYouType",
    "Short",
    "SparseVector",
    "TermsFacet",
    "Text",
    "TokenCount",
    "UnknownDslObject",
    "UpdateByQuery",
    "ValidationException",
    "analyzer",
    "char_filter",
    "connections",
    "construct_field",
    "normalizer",
    "token_filter",
    "tokenizer",
    "__versionstr__",
    "Metrics",
    "MetricsEvents",
    "MetricsNone",
]

try:
    from ._async.client import AsyncOpenSearch
    from ._async.http_aiohttp import AIOHttpConnection, AsyncConnection
    from ._async.transport import AsyncTransport
    from .connection import AsyncHttpConnection
    from .helpers import AWSV4SignerAsyncAuth

    __all__ += [
        "AIOHttpConnection",
        "AsyncConnection",
        "AsyncTransport",
        "AsyncOpenSearch",
        "AsyncHttpConnection",
        "AWSV4SignerAsyncAuth",
    ]
except (ImportError, SyntaxError):
    pass
