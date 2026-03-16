import os

from libc.stdlib cimport malloc, free

from pyproj.compat import cstrencode, pystrdecode
from pyproj.datadir import get_data_dir
from pyproj.exceptions import ProjError

cdef void pyproj_log_function(void *user_data, int level, const char *error_msg):
    """
    Log function for catching PROJ errors.
    """
    if level == PJ_LOG_ERROR:
        ProjError.internal_proj_error = pystrdecode(error_msg)


cdef PJ_CONTEXT* get_pyproj_context() except *:
    data_dir = get_data_dir()
    data_dir_list = data_dir.split(os.pathsep)
    cdef PJ_CONTEXT* pyproj_context = NULL
    cdef char **c_data_dir = <char **>malloc(len(data_dir_list) * sizeof(char*))
    try:
        pyproj_context = proj_context_create()
        for iii in range(len(data_dir_list)):
            b_data_dir = cstrencode(data_dir_list[iii])
            c_data_dir[iii] = b_data_dir
        proj_context_set_search_paths(pyproj_context, len(data_dir_list), c_data_dir)
    except:
        if pyproj_context != NULL:
            proj_context_destroy(pyproj_context)
        raise
    finally:
        free(c_data_dir)
    proj_context_use_proj4_init_rules(pyproj_context, 1)
    proj_log_func(pyproj_context, NULL, pyproj_log_function)

    return pyproj_context
