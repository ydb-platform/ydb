from __future__ import annotations

import importlib.metadata
import importlib.util
from typing import Any

from django.db import connections


def connector_params(alias: str = "default") -> dict[str, Any]:
    """
    Returns parameters for in a format that is suitable to be passed to a
    connector constructor (see `howto/django`).

    Parameters
    ----------
    alias :
        Alias of the database, to read in the keys of settings.DATABASES,
        by default ``default``.

    Returns
    -------
    :
        Provide these keyword arguments when instantiating your connector
    """
    wrapper = connections[alias]
    params = wrapper.get_connection_params()
    params.pop("cursor_factory", None)
    params.pop("context", None)
    return params


def package_is_installed(name: str) -> bool:
    return bool(importlib.util.find_spec(name))


def package_is_version(name: str, major: int) -> bool:
    """
    Check if package's (PEP440) version matches given major version number
    """
    version = importlib.metadata.version(name)
    return bool(version) and version.split(".")[0] == f"{major}"
