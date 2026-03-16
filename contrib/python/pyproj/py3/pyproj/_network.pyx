include "proj.pxi"

import os

from pyproj.utils import strtobool

from pyproj._compat cimport cstrencode
from pyproj._context cimport pyproj_context_create
from pyproj._context import _set_context_network_enabled


def set_network_enabled(active=None):
    """
    .. versionadded:: 3.0.0

    Set whether PROJ network is enabled by default. This has the same
    behavior as the `PROJ_NETWORK` environment variable.

    See: :c:func:`proj_context_set_enable_network`

    Parameters
    ----------
    active: bool, optional
        Default is None, which uses the system defaults for networking.
        If True, it will force the use of network for grids regardless of
        any other network setting. If False, it will force disable use of
        network for grids regardless of any other network setting.
    """
    if active is None:
        # in the case of the global context, need to reset network
        # setting based on the environment variable every time if None
        # because it could have been changed by the user previously
        active = strtobool(os.environ.get("PROJ_NETWORK", "OFF"))
    _set_context_network_enabled(bool(active))


def is_network_enabled():
    """
    .. versionadded:: 3.0.0

    See: :c:func:`proj_context_is_network_enabled`

    Returns
    -------
    bool:
        If PROJ network is enabled by default.
    """
    return proj_context_is_network_enabled(pyproj_context_create()) == 1
