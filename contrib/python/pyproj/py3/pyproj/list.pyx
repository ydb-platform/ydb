include "proj.pxi"


def get_proj_operations_map():
    """
    Returns
    -------
    dict:
        Operations supported by PROJ.
    """
    cdef const PJ_OPERATIONS *proj_operations = proj_list_operations()
    cdef int iii = 0
    operations_map = {}
    while proj_operations[iii].id != NULL:
        operations_map[proj_operations[iii].id] = \
            proj_operations[iii].descr[0].split("\n\t")[0]
        iii += 1
    return operations_map


def get_ellps_map():
    """
    Returns
    -------
    dict:
        Ellipsoids supported by PROJ.
    """
    cdef const PJ_ELLPS *proj_ellps = proj_list_ellps()
    cdef int iii = 0
    ellps_map = {}
    while proj_ellps[iii].id != NULL:
        major_key, major_val = proj_ellps[iii].major.split("=")
        ell_key, ell_val = proj_ellps[iii].ell.split("=")
        ellps_map[proj_ellps[iii].id] = {
            major_key: float(major_val),
            ell_key: float(ell_val),
            "description": proj_ellps[iii].name
        }
        iii += 1
    return ellps_map


def get_prime_meridians_map():
    """
    Returns
    -------
    dict:
        Prime Meridians supported by PROJ.
    """
    cdef const PJ_PRIME_MERIDIANS *prime_meridians = proj_list_prime_meridians()
    cdef int iii = 0
    prime_meridians_map = {}
    while prime_meridians[iii].id != NULL:
        prime_meridians_map[prime_meridians[iii].id] = \
            prime_meridians[iii].defn
        iii += 1
    return prime_meridians_map
