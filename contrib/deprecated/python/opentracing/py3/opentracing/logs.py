# Copyright The OpenTracing Authors
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

# The following log fields are described in greater detail at the
# following url:
# https://github.com/opentracing/specification/blob/master/semantic_conventions.md

# ---------------------------------------------------------------------------
# The type or "kind" of an error (only for event="error" logs). E.g.,
# "Exception", "OSError"
# ---------------------------------------------------------------------------
ERROR_KIND = 'error.kind'

# ---------------------------------------------------------------------------
# The actual Exception/Error object instance itself. E.g., A python
# exceptions.NameError instance
# ---------------------------------------------------------------------------
ERROR_OBJECT = 'error.object'

# ---------------------------------------------------------------------------
# A stable identifier for some notable moment in the lifetime of a Span.
# For instance, a mutex lock acquisition or release or the sorts of lifetime
# events in a browser page load described in the Performance.timing
# specification. E.g., from Zipkin, "cs", "sr", "ss", or "cr". Or, more
# generally, "initialized" or "timed out". For errors, "error"
# ---------------------------------------------------------------------------
EVENT = 'event'

# ---------------------------------------------------------------------------
# A concise, human-readable, one-line message explaining the event. E.g.,
# "Could not connect to backend", "Cache invalidation succeeded"
# ---------------------------------------------------------------------------
MESSAGE = 'message'

# ---------------------------------------------------------------------------
# A stack trace in platform-conventional format; may or may not pertain to
# an error.
# ---------------------------------------------------------------------------
STACK = 'stack'
