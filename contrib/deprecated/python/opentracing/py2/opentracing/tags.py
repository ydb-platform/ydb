# Copyright The OpenTracing Authors
# Copyright Uber Technologies, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import

# The following tags are described in greater detail at the following url:
# https://github.com/opentracing/specification/blob/master/semantic_conventions.md

# Here we define standard names for tags that can be added to spans by the
# instrumentation code. The actual tracing systems are not required to
# retain these as tags in the stored spans if they have other means of
# representing the same data. For example, the SPAN_KIND='server' can be
# inferred from a Zipkin span by the presence of ss/sr annotations.

# ---------------------------------------------------------------------------
# SPAN_KIND hints at relationship between spans, e.g. client/server
# ---------------------------------------------------------------------------
SPAN_KIND = 'span.kind'

# Marks a span representing the client-side of an RPC or other remote call
SPAN_KIND_RPC_CLIENT = 'client'

# Marks a span representing the server-side of an RPC or other remote call
SPAN_KIND_RPC_SERVER = 'server'

# Marks a span representing the consumer-side of a messaging call
SPAN_KIND_CONSUMER = 'consumer'

# Marks a span representing the producer-side of a messaging call
SPAN_KIND_PRODUCER = 'producer'

# ---------------------------------------------------------------------------
# SERVICE indicates the service name for a span, which overrides any default
# "service name" property defined in a tracer's config. The meaning of
# service should correspond to the value set in peer.service, except it is
# applied to the current span. This tag is meant to only be used when a
# tracer is reporting spans on behalf of another service. This tag does not
# need to be used when reporting spans for the service the tracer is running
# in.
# ---------------------------------------------------------------------------
SERVICE = 'service'

# ---------------------------------------------------------------------------
# ERROR indicates whether a Span ended in an error state.
# ---------------------------------------------------------------------------
ERROR = 'error'

# ---------------------------------------------------------------------------
# COMPONENT (string) ia s low-cardinality identifier of the module, library,
# or package that is generating a span.
# ---------------------------------------------------------------------------
COMPONENT = 'component'

# ---------------------------------------------------------------------------
# SAMPLING_PRIORITY (uint16) determines the priority of sampling this Span.
# ---------------------------------------------------------------------------
SAMPLING_PRIORITY = 'sampling.priority'

# ---------------------------------------------------------------------------
# PEER_* tags can be emitted by either client-side of server-side to describe
# the other side/service in a peer-to-peer communications, like an RPC call.
# ---------------------------------------------------------------------------

# PEER_SERVICE (string) records the service name of the peer
PEER_SERVICE = 'peer.service'

# PEER_HOSTNAME (string) records the host name of the peer
PEER_HOSTNAME = 'peer.hostname'

# PEER_ADDRESS (string) suitable for use in a networking client library.
# This may be a "ip:port", a bare "hostname", a FQDN, or even a
# JDBC substring like "mysql://prod-db:3306"
PEER_ADDRESS = 'peer.address'

# PEER_HOST_IPV4 (uint32) records IP v4 host address of the peer
PEER_HOST_IPV4 = 'peer.ipv4'

# PEER_HOST_IPV6 (string) records IP v6 host address of the peer
PEER_HOST_IPV6 = 'peer.ipv6'

# PEER_PORT (uint16) records port number of the peer
PEER_PORT = 'peer.port'

# ---------------------------------------------------------------------------
# HTTP tags
# ---------------------------------------------------------------------------

# HTTP_URL (string) should be the URL of the request being handled in this
# segment of the trace, in standard URI format. The protocol is optional.
HTTP_URL = 'http.url'

# HTTP_METHOD (string) is the HTTP method of the request.
# Both upper/lower case values are allowed.
HTTP_METHOD = 'http.method'

# HTTP_STATUS_CODE (int) is the numeric HTTP status code (200, 404, etc)
# of the HTTP response.
HTTP_STATUS_CODE = 'http.status_code'

# ---------------------------------------------------------------------------
# DATABASE tags
# ---------------------------------------------------------------------------

# DATABASE_INSTANCE (string) The database instance name. E.g., In java, if
# the jdbc.url="jdbc:mysql://127.0.0.1:3306/customers", the instance
# name is "customers"
DATABASE_INSTANCE = 'db.instance'

# DATABASE_STATEMENT (string) A database statement for the given database
# type. E.g., for db.type="SQL", "SELECT * FROM user_table";
# for db.type="redis", "SET mykey 'WuValue'".
DATABASE_STATEMENT = 'db.statement'

# DATABASE_TYPE (string) For any SQL database, "sql". For others,
# the lower-case database category, e.g. "cassandra", "hbase", or "redis".
DATABASE_TYPE = 'db.type'

# DATABASE_USER (string) Username for accessing database. E.g.,
# "readonly_user" or "reporting_user"
DATABASE_USER = 'db.user'

# ---------------------------------------------------------------------------
# MESSAGE_BUS tags
# ---------------------------------------------------------------------------

# MESSAGE_BUS_DESTINATION (string) An address at which messages can be
# exchanged. E.g. A Kafka record has an associated "topic name" that can
# be extracted by the instrumented producer or consumer and stored
# using this tag.
MESSAGE_BUS_DESTINATION = 'message_bus.destination'
