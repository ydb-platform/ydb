# Copyright (c) 2016-2018 Uber Technologies, Inc.
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


from . import __version__


# DEPRECATED: max number of bits to use when generating random ID
MAX_ID_BITS = 64

# Max number of bits allowed to use when generating Trace ID
_max_trace_id_bits = 128

# Max number of bits to use when generating random ID
_max_id_bits = 64

# How often remotely controlled sampler polls for sampling strategy
DEFAULT_SAMPLING_INTERVAL = 60

# How often remote reporter does a preemptive flush of its buffers
DEFAULT_FLUSH_INTERVAL = 1

# Name of the HTTP header used to encode trace ID
TRACE_ID_HEADER = 'uber-trace-id'

# Prefix for HTTP headers used to record baggage items
BAGGAGE_HEADER_PREFIX = 'uberctx-'

# The name of HTTP header or a TextMap carrier key which, if found in the
# carrier, forces the trace to be sampled as "debug" trace. The value of the
# header is recorded as the tag on the # root span, so that the trace can
# be found in the UI using this value as a correlation ID.
DEBUG_ID_HEADER_KEY = 'jaeger-debug-id'

# The name of HTTP header or a TextMap carrier key that can be used to pass
# additional baggage to the span, e.g. when executing an ad-hoc curl request:
# curl -H 'jaeger-baggage: k1=v1,k2=v2' http://...
BAGGAGE_HEADER_KEY = 'jaeger-baggage'


JAEGER_CLIENT_VERSION = 'Python-%s' % __version__

# Tracer-scoped tag that tells the version of Jaeger client library
JAEGER_VERSION_TAG_KEY = 'jaeger.version'

# Tracer-scoped tag that contains the hostname
JAEGER_HOSTNAME_TAG_KEY = 'hostname'

# Tracer-scoped tag that is used to report ip of the process.
JAEGER_IP_TAG_KEY = 'ip'

# the type of sampler that always makes the same decision.
SAMPLER_TYPE_CONST = 'const'

# the type of sampler that polls Jaeger agent for sampling strategy.
SAMPLER_TYPE_REMOTE = 'remote'

# the type of sampler that samples traces with a certain fixed probability.
SAMPLER_TYPE_PROBABILISTIC = 'probabilistic'

# the type of sampler that samples only up to a fixed number
# of traces per second.
# noinspection SpellCheckingInspection
SAMPLER_TYPE_RATE_LIMITING = 'ratelimiting'

# the type of sampler that samples only up to a fixed number
# of traces per second.
# noinspection SpellCheckingInspection
SAMPLER_TYPE_LOWER_BOUND = 'lowerbound'

# Tag key for unique client identifier. Used in throttler implementation.
CLIENT_UUID_TAG_KEY = 'client-uuid'

# max length for tag values. Longer values will be truncated.
MAX_TAG_VALUE_LENGTH = 1024

# max length for traceback data. Longer values will be truncated.
MAX_TRACEBACK_LENGTH = 4096

# Constant for sampled flag
SAMPLED_FLAG = 0x01

# Constant for debug flag
DEBUG_FLAG = 0x02

# How often throttler polls for credits
DEFAULT_THROTTLER_REFRESH_INTERVAL = 5
