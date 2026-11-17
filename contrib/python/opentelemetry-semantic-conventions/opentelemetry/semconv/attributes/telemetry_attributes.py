# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from typing import Final

TELEMETRY_DISTRO_NAME: Final = "telemetry.distro.name"
"""
The name of the auto instrumentation agent or distribution, if used.
Note: Official auto instrumentation agents and distributions SHOULD set the `telemetry.distro.name` attribute to
a string starting with `opentelemetry-`, e.g. `opentelemetry-java-instrumentation`.
"""

TELEMETRY_DISTRO_VERSION: Final = "telemetry.distro.version"
"""
The version string of the auto instrumentation agent or distribution, if used.
"""

TELEMETRY_SDK_LANGUAGE: Final = "telemetry.sdk.language"
"""
The language of the telemetry SDK.
"""

TELEMETRY_SDK_NAME: Final = "telemetry.sdk.name"
"""
The name of the telemetry SDK as defined above.
Note: The OpenTelemetry SDK MUST set the `telemetry.sdk.name` attribute to `opentelemetry`.
If another SDK, like a fork or a vendor-provided implementation, is used, this SDK MUST set the
`telemetry.sdk.name` attribute to the fully-qualified class or module name of this SDK's main entry point
or another suitable identifier depending on the language.
The identifier `opentelemetry` is reserved and MUST NOT be used in this case.
All custom identifiers SHOULD be stable across different versions of an implementation.
"""

TELEMETRY_SDK_VERSION: Final = "telemetry.sdk.version"
"""
The version string of the telemetry SDK.
"""


class TelemetrySdkLanguageValues(Enum):
    CPP = "cpp"
    """cpp."""
    DOTNET = "dotnet"
    """dotnet."""
    ERLANG = "erlang"
    """erlang."""
    GO = "go"
    """go."""
    JAVA = "java"
    """java."""
    KOTLIN = "kotlin"
    """kotlin."""
    NODEJS = "nodejs"
    """nodejs."""
    PHP = "php"
    """php."""
    PYTHON = "python"
    """python."""
    RUBY = "ruby"
    """ruby."""
    RUST = "rust"
    """rust."""
    SWIFT = "swift"
    """swift."""
    WEBJS = "webjs"
    """webjs."""
