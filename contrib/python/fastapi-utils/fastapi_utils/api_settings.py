from __future__ import annotations

from functools import lru_cache
from typing import Any

import pydantic

PYDANTIC_VERSION = pydantic.VERSION

if PYDANTIC_VERSION[0] == "2":
    from pydantic_settings import BaseSettings, SettingsConfigDict
else:
    from pydantic import BaseSettings  # type: ignore[no-redef]


class APISettings(BaseSettings):
    """
    This class enables the configuration of your FastAPI instance through the use of environment variables.

    Any of the instance attributes can be overridden upon instantiation by either passing the desired value to the
    initializer, or by setting the corresponding environment variable.

    Attribute `xxx_yyy` corresponds to environment variable `API_XXX_YYY`. So, for example, to override
    `openapi_prefix`, you would set the environment variable `API_OPENAPI_PREFIX`.

    Note that assignments to variables are also validated, ensuring that even if you make runtime-modifications
    to the config, they should have the correct types.
    """

    # fastapi.applications.FastAPI initializer kwargs
    debug: bool = False
    docs_url: str = "/docs"
    openapi_prefix: str = ""
    openapi_url: str = "/openapi.json"
    redoc_url: str = "/redoc"
    title: str = "FastAPI"
    version: str = "0.1.0"

    # Custom settings
    disable_docs: bool = False

    @property
    def fastapi_kwargs(self) -> dict[str, Any]:
        """
        This returns a dictionary of the most commonly used keyword arguments when initializing a FastAPI instance

        If `self.disable_docs` is True, the various docs-related arguments are disabled, preventing your spec from being
        published.
        """
        fastapi_kwargs: dict[str, Any] = {
            "debug": self.debug,
            "docs_url": self.docs_url,
            "openapi_prefix": self.openapi_prefix,
            "openapi_url": self.openapi_url,
            "redoc_url": self.redoc_url,
            "title": self.title,
            "version": self.version,
        }
        if self.disable_docs:
            fastapi_kwargs.update({"docs_url": None, "openapi_url": None, "redoc_url": None})
        return fastapi_kwargs

    if PYDANTIC_VERSION[0] == "2":
        model_config = SettingsConfigDict(env_prefix="api_", validate_assignment=True)
    else:

        class Config:
            env_prefix = "api_"
            validate_assignment = True


@lru_cache()
def get_api_settings() -> APISettings:
    """
    This function returns a cached instance of the APISettings object.

    Caching is used to prevent re-reading the environment every time the API settings are used in an endpoint.

    If you want to change an environment variable and reset the cache (e.g., during testing), this can be done
    using the `lru_cache` instance method `get_api_settings.cache_clear()`.
    """
    return APISettings()
