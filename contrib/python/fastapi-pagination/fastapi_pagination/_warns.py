import warnings

from fastapi_pagination.consts import IS_FASTAPI_V_0_128_0_OR_NEWER
from fastapi_pagination.pydantic import IS_PYDANTIC_V2
from fastapi_pagination.pydantic.consts import IS_PYDANTIC_V2_12_5_OR_HIGHER
from fastapi_pagination.utils import FastAPIPaginationWarning

if not IS_PYDANTIC_V2:
    warnings.warn(
        "Pydantic v1 is deprecated and support for it will be removed in fastapi-pagination v0.16.0. ",
        FastAPIPaginationWarning,
        stacklevel=3,
    )
elif not IS_PYDANTIC_V2_12_5_OR_HIGHER:
    warnings.warn(
        "Pydantic v2.12.5 or higher is required for full compatibility. "
        "Please upgrade your Pydantic version to avoid potential issues."
        "Support for Pydantic versions lower than v2.12.5 will be dropped in fastapi-pagination v0.16.0.",
        FastAPIPaginationWarning,
        stacklevel=3,
    )

if not IS_FASTAPI_V_0_128_0_OR_NEWER:
    warnings.warn(
        "FastAPI v0.128.0 or higher is required for full compatibility. "
        "Please upgrade your FastAPI version to avoid potential issues. "
        "Support for FastAPI versions lower than v0.128.0 will be dropped in fastapi-pagination v0.16.0.",
        FastAPIPaginationWarning,
        stacklevel=3,
    )

__all__ = []
