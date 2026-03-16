include "proj.pxi"

from collections import namedtuple

from pyproj.compat import pystrdecode


def get_proj_operations_map():
    """
    Returns
    -------
    dict: operations supported by PROJ.
    """
    cdef PJ_OPERATIONS *proj_operations = proj_list_operations()
    cdef int iii = 0
    operations_map = {}
    while proj_operations[iii].id != NULL:
        operations_map[pystrdecode(proj_operations[iii].id)] = \
            pystrdecode(proj_operations[iii].descr[0]).split("\n\t")[0]
        iii += 1
    return operations_map


def get_ellps_map():
    """
    Returns
    -------
    dict: ellipsoids supported by PROJ.
    """
    cdef PJ_ELLPS *proj_ellps = proj_list_ellps()
    cdef int iii = 0
    ellps_map = {}
    while proj_ellps[iii].id != NULL:
        major_key, major_val = pystrdecode(proj_ellps[iii].major).split("=")
        ell_key, ell_val = pystrdecode(proj_ellps[iii].ell).split("=")
        ellps_map[pystrdecode(proj_ellps[iii].id)] = {
            major_key: float(major_val),
            ell_key: float(ell_val),
            "description": pystrdecode(proj_ellps[iii].name)
        }
        iii += 1
    return ellps_map


def get_prime_meridians_map():
    """
    Returns
    -------
    dict: prime meridians supported by PROJ.
    """
    cdef PJ_PRIME_MERIDIANS *prime_meridians = proj_list_prime_meridians()
    cdef int iii = 0
    prime_meridians_map = {}
    while prime_meridians[iii].id != NULL:
        prime_meridians_map[pystrdecode(prime_meridians[iii].id)] = \
            pystrdecode(prime_meridians[iii].defn)
        iii += 1
    return prime_meridians_map


Unit = namedtuple("Unit", ["id", "to_meter", "name", "factor"])


def get_units_map():
    """
    Returns
    -------
    dict: units supported by PROJ
    """
    cdef PJ_UNITS *proj_units = proj_list_units()
    cdef int iii = 0
    units_map = {}
    while proj_units[iii].id != NULL:
        units_map[pystrdecode(proj_units[iii].id)] = Unit(
            id=pystrdecode(proj_units[iii].id),
            to_meter=pystrdecode(proj_units[iii].to_meter),
            name=pystrdecode(proj_units[iii].name),
            factor=proj_units[iii].factor,
        )
        iii += 1
    return units_map


def get_angular_units_map():
    """
    Returns
    -------
    dict: angular units supported by PROJ
    """
    cdef PJ_UNITS *proj_units = proj_list_angular_units()
    cdef int iii = 0
    units_map = {}
    while proj_units[iii].id != NULL:
        units_map[pystrdecode(proj_units[iii].id)] = Unit(
            id=pystrdecode(proj_units[iii].id),
            to_meter=pystrdecode(proj_units[iii].to_meter),
            name=pystrdecode(proj_units[iii].name),
            factor=proj_units[iii].factor,
        )
        iii += 1
    return units_map
