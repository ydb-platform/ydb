# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from typing import Final

ERROR_TYPE: Final = "error.type"
"""
Describes a class of error the operation ended with.
Note: The `error.type` SHOULD be predictable, and SHOULD have low cardinality.

When `error.type` is set to a type (e.g., an exception type), its
canonical class name identifying the type within the artifact SHOULD be used.

If the recorded error type is a wrapper that is not meaningful for
failure classification, instrumentation MAY use the type of the inner
error instead. For example, in Go, errors created with `fmt.Errorf`
using `%w` MAY be unwrapped when the wrapper type does not help
classify the failure.

Instrumentations SHOULD document the list of errors they report.

The cardinality of `error.type` within one instrumentation library SHOULD be low.
Telemetry consumers that aggregate data from multiple instrumentation libraries and applications
should be prepared for `error.type` to have high cardinality at query time when no
additional filters are applied.

If the operation has completed successfully, instrumentations SHOULD NOT set `error.type`.

If a specific domain defines its own set of error identifiers (such as HTTP or RPC status codes),
it's RECOMMENDED to:

- Use a domain-specific attribute
- Set `error.type` to capture all errors, regardless of whether they are defined within the domain-specific set or not.
"""


class ErrorTypeValues(Enum):
    OTHER = "_OTHER"
    """A fallback error value to be used when the instrumentation doesn't define a custom value."""
