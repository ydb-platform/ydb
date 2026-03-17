"""
Module for managing the PROJ network settings.
"""

import os
from pathlib import Path

import certifi.source as certifi

from pyproj._context import _set_context_ca_bundle_path
from pyproj._network import (  # noqa: F401 pylint: disable=unused-import
    is_network_enabled,
    set_network_enabled,
)


def set_ca_bundle_path(ca_bundle_path: Path | str | bool | None = None) -> None:
    """
    .. versionadded:: 3.0.0

    Sets the path to the CA Bundle used by the `curl`
    built into PROJ when PROJ network is enabled.

    See: :c:func:`proj_context_set_ca_bundle_path`

    Environment variables:

    - PROJ_CURL_CA_BUNDLE
    - CURL_CA_BUNDLE
    - SSL_CERT_FILE

    Parameters
    ----------
    ca_bundle_path: Path | str | bool | None, optional
        Default is None, which only uses the `certifi` package path as a fallback if
        the environment variables are not set. If a path is passed in, then
        that will be the path used. If it is set to True, then it will default
        to using the path provided, by the `certifi` package. If it is set to False
        or an empty string then it will default to the system settings or environment
        variables.
    """
    env_var_names = ("PROJ_CURL_CA_BUNDLE", "CURL_CA_BUNDLE", "SSL_CERT_FILE")
    if ca_bundle_path is False:
        # need to reset CA Bundle path to use system settings
        # or environment variables because it
        # could have been changed by the user previously
        ca_bundle_path = ""
    elif isinstance(ca_bundle_path, (str, Path)):
        ca_bundle_path = str(ca_bundle_path)
    elif (ca_bundle_path is True) or not any(
        env_var_name in os.environ for env_var_name in env_var_names
    ):
        ca_bundle_path = certifi.where()
    else:
        # reset CA Bundle path to use system settings
        # or environment variables
        ca_bundle_path = ""

    _set_context_ca_bundle_path(ca_bundle_path)
