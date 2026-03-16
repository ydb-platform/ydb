include "proj.pxi"

from pyproj._context cimport pyproj_context_create


def get_proj_endpoint() -> str:
    """
    Returns
    -------
    str:
        URL of the endpoint where PROJ grids are stored.
    """
    return proj_context_get_url_endpoint(pyproj_context_create())
