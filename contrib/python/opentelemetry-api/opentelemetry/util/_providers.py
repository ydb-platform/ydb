# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from logging import getLogger
from os import environ
from typing import TYPE_CHECKING, TypeVar, cast

if TYPE_CHECKING:
    from opentelemetry.metrics import MeterProvider
    from opentelemetry.trace import TracerProvider

Provider = TypeVar("Provider", "TracerProvider", "MeterProvider")

logger = getLogger(__name__)


def _load_provider(
    provider_environment_variable: str, provider: str
) -> Provider:  # type: ignore[type-var]
    # pylint: disable=import-outside-toplevel,no-name-in-module
    from opentelemetry.util._importlib_metadata import (  # noqa: PLC0415
        entry_points,
    )

    try:
        provider_name = cast(
            str,
            environ.get(provider_environment_variable, f"default_{provider}"),
        )

        return cast(
            Provider,
            next(  # type: ignore
                iter(  # type: ignore
                    entry_points(  # type: ignore
                        group=f"opentelemetry_{provider}",
                        name=provider_name,
                    )
                )
            ).load()(),
        )
    except Exception:  # pylint: disable=broad-exception-caught
        logger.exception("Failed to load configured provider %s", provider)
        raise
