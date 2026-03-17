import warnings

from dishka.entities.validation_settings import (
    DEFAULT_VALIDATION,
    STRICT_VALIDATION,
    ValidationSettings,
)

warnings.warn(
    "`dishka.entities.validation_settigs`"
    " is deprecated and will be removed in 2.0"
    ", use `from dishka import ...` instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = [
    "DEFAULT_VALIDATION",
    "STRICT_VALIDATION",
    "ValidationSettings",
]
