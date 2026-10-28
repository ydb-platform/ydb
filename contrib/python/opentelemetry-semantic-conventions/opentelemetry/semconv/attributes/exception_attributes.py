# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from typing import Final

EXCEPTION_ESCAPED: Final = "exception.escaped"
"""
Deprecated: It's no longer recommended to record exceptions that are handled and do not escape the scope of a span.
"""

EXCEPTION_MESSAGE: Final = "exception.message"
"""
The exception message.
Note: > [!WARNING]
>
> This attribute may contain sensitive information.
"""

EXCEPTION_STACKTRACE: Final = "exception.stacktrace"
"""
A stacktrace as a string in the natural representation for the language runtime. The representation is to be determined and documented by each language SIG.
"""

EXCEPTION_TYPE: Final = "exception.type"
"""
The type of the exception (its fully-qualified class name, if applicable). The dynamic type of the exception should be preferred over the static type in languages that support it.
Note: If the recorded exception type is a wrapper that is not meaningful for
failure classification, instrumentation MAY use the type of the inner
exception instead. For example, in Go, errors created with `fmt.Errorf`
using `%w` MAY be unwrapped when the wrapper type does not help
classify the failure.
"""
