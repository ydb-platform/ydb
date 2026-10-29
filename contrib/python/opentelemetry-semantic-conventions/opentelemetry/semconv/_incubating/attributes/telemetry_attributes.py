# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from typing import Final

from typing_extensions import deprecated

TELEMETRY_DISTRO_NAME: Final = "telemetry.distro.name"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.telemetry_attributes.TELEMETRY_DISTRO_NAME`.
"""

TELEMETRY_DISTRO_VERSION: Final = "telemetry.distro.version"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.telemetry_attributes.TELEMETRY_DISTRO_VERSION`.
"""

TELEMETRY_SDK_LANGUAGE: Final = "telemetry.sdk.language"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.telemetry_attributes.TELEMETRY_SDK_LANGUAGE`.
"""

TELEMETRY_SDK_NAME: Final = "telemetry.sdk.name"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.telemetry_attributes.TELEMETRY_SDK_NAME`.
"""

TELEMETRY_SDK_VERSION: Final = "telemetry.sdk.version"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.telemetry_attributes.TELEMETRY_SDK_VERSION`.
"""


@deprecated(
    "Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.telemetry_attributes.TelemetrySdkLanguageValues`."
)
class TelemetrySdkLanguageValues(Enum):
    CPP = "cpp"
    """Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.telemetry_attributes.TelemetrySdkLanguageValues.CPP`."""
    DOTNET = "dotnet"
    """Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.telemetry_attributes.TelemetrySdkLanguageValues.DOTNET`."""
    ERLANG = "erlang"
    """Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.telemetry_attributes.TelemetrySdkLanguageValues.ERLANG`."""
    GO = "go"
    """Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.telemetry_attributes.TelemetrySdkLanguageValues.GO`."""
    JAVA = "java"
    """Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.telemetry_attributes.TelemetrySdkLanguageValues.JAVA`."""
    KOTLIN = "kotlin"
    """Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.telemetry_attributes.TelemetrySdkLanguageValues.KOTLIN`."""
    NODEJS = "nodejs"
    """Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.telemetry_attributes.TelemetrySdkLanguageValues.NODEJS`."""
    PHP = "php"
    """Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.telemetry_attributes.TelemetrySdkLanguageValues.PHP`."""
    PYTHON = "python"
    """Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.telemetry_attributes.TelemetrySdkLanguageValues.PYTHON`."""
    RUBY = "ruby"
    """Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.telemetry_attributes.TelemetrySdkLanguageValues.RUBY`."""
    RUST = "rust"
    """Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.telemetry_attributes.TelemetrySdkLanguageValues.RUST`."""
    SWIFT = "swift"
    """Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.telemetry_attributes.TelemetrySdkLanguageValues.SWIFT`."""
    WEBJS = "webjs"
    """Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.telemetry_attributes.TelemetrySdkLanguageValues.WEBJS`."""
