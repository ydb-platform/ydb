import logging
import os
import threading
import warnings

from cpython.pythread cimport PyThread_tss_create, PyThread_tss_get, PyThread_tss_set
from libc.stdlib cimport free, malloc

from pyproj._compat cimport cstrencode

from pyproj.utils import strtobool

# for logging the internal PROJ messages
# https://docs.python.org/3/howto/logging.html#configuring-logging-for-a-library
_LOGGER = logging.getLogger("pyproj")
_LOGGER.addHandler(logging.NullHandler())
# static user data directory to prevent core dumping
# see: https://github.com/pyproj4/pyproj/issues/678
cdef const char* _USER_DATA_DIR = proj_context_get_user_writable_directory(NULL, False)
# Store the message from any internal PROJ errors
cdef str _INTERNAL_PROJ_ERROR = None
# global variables
cdef bint _NETWORK_ENABLED = strtobool(os.environ.get("PROJ_NETWORK", "OFF"))
cdef char* _CA_BUNDLE_PATH = ""
# The key to get the context in each thread
cdef Py_tss_t CONTEXT_THREAD_KEY


def set_use_global_context(active=None):
    """
    .. deprecated:: 3.7.0 No longer necessary as there is only one context per thread now.

    .. versionadded:: 3.0.0

    Activates the usage of the global context. Using this
    option can enhance the performance of initializing objects
    in single-threaded applications.

    .. warning:: The global context is not thread safe.
    .. warning:: The global context maintains a connection to the database
                 through the duration of each python session and is closed
                 once the program terminates.

    .. note:: To modify network settings see: :ref:`network`.

    Parameters
    ----------
    active: bool, optional
        If True, it activates the use of the global context. If False,
        the use of the global context is deactivated. If None, it uses
        the environment variable PYPROJ_GLOBAL_CONTEXT and defaults
        to False if it is not found.
    """
    if active is None:
        active = strtobool(os.environ.get("PYPROJ_GLOBAL_CONTEXT", "OFF"))
    if active:
        warnings.warn(
            (
                "PYPROJ_GLOBAL_CONTEXT is no longer necessary in pyproj 3.7+ "
                "and does not do anything."
            ),
            FutureWarning,
            stacklevel=2,
        )


def get_user_data_dir(create=False):
    """
    .. versionadded:: 3.0.0

    Get the PROJ user writable directory for datumgrid files.

    See: :c:func:`proj_context_get_user_writable_directory`

    This is where grids will be downloaded when
    :ref:`PROJ network <network>` capabilities
    are enabled. It is also the default download location for the
    :ref:`projsync` command line program.

    Parameters
    ----------
    create: bool, default=False
        If True, it will create the directory if it does not already exist.

    Returns
    -------
    str:
        The user writable data directory.
    """
    return proj_context_get_user_writable_directory(
        pyproj_context_create(), bool(create)
    )


cpdef str _get_proj_error():
    """
    Get the internal PROJ error message. Returns None if no error was set.
    """
    return _INTERNAL_PROJ_ERROR


cpdef void _clear_proj_error() noexcept:
    """
    Clear the internal PROJ error message.
    """
    global _INTERNAL_PROJ_ERROR
    _INTERNAL_PROJ_ERROR = None


cdef void pyproj_log_function(void *user_data, int level, const char *error_msg) noexcept nogil:
    """
    Log function for catching PROJ errors.
    """
    # from pyproj perspective, everything from PROJ is for debugging.
    # The verbosity should be managed via the
    # PROJ_DEBUG environment variable.
    if level == PJ_LOG_ERROR:
        with gil:
            global _INTERNAL_PROJ_ERROR
            _INTERNAL_PROJ_ERROR = error_msg
            _LOGGER.debug(f"PROJ_ERROR: {_INTERNAL_PROJ_ERROR}")
    elif level == PJ_LOG_DEBUG:
        with gil:
            _LOGGER.debug(f"PROJ_DEBUG: {error_msg}")
    elif level == PJ_LOG_TRACE:
        with gil:
            _LOGGER.debug(f"PROJ_TRACE: {error_msg}")


cdef void set_context_data_dir(PJ_CONTEXT* context) except *:
    """
    Setup the data directory for the context for pyproj
    """
    from pyproj.datadir import get_data_dir

    data_dir_list = get_data_dir().split(os.pathsep)
    # the first path will always have the database
    cdef bytes b_database_path = cstrencode(os.path.join(data_dir_list[0], "proj.db"))
    cdef const char* c_database_path = b_database_path
    if not proj_context_set_database_path(context, c_database_path, NULL, NULL):
        warnings.warn("pyproj unable to set PROJ database path.")
    cdef int dir_list_len = len(data_dir_list)
    cdef const char **c_data_dir = <const char **>malloc(
        (dir_list_len + 1) * sizeof(const char*)
    )
    cdef bytes b_data_dir
    try:
        for iii in range(dir_list_len):
            b_data_dir = cstrencode(data_dir_list[iii])
            c_data_dir[iii] = b_data_dir
        c_data_dir[dir_list_len] = _USER_DATA_DIR
        proj_context_set_search_paths(context, dir_list_len + 1, c_data_dir)
    finally:
        free(c_data_dir)


cdef void pyproj_context_initialize(PJ_CONTEXT* context) except *:
    """
    Setup the context for pyproj
    """
    proj_log_func(context, NULL, pyproj_log_function)
    proj_context_use_proj4_init_rules(context, 1)
    set_context_data_dir(context)
    proj_context_set_ca_bundle_path(context, _CA_BUNDLE_PATH)
    proj_context_set_enable_network(context, _NETWORK_ENABLED)


cdef class ContextManager:
    """
    The only purpose of this class is
    to ensure the context is cleaned up properly.
    """
    cdef PJ_CONTEXT* context

    def __cinit__(self):
        self.context = NULL

    def __dealloc__(self):
        if self.context != NULL:
            proj_context_destroy(self.context)

    @staticmethod
    cdef create(PJ_CONTEXT* context):
        cdef ContextManager context_manager = ContextManager()
        context_manager.context = context
        return context_manager


class ContextManagerLocal(threading.local):
    """
    Threading local instance for cython ContextManager class.
    """

    def __init__(self):
        self.context_manager = None  # Initialises in each thread
        super().__init__()


_CONTEXT_MANAGER_LOCAL = ContextManagerLocal()

cdef PJ_CONTEXT* pyproj_context_create() except *:
    """
    Create and initialize the context(s) for pyproj.
    This also manages whether the global context is used.
    """
    global _CONTEXT_MANAGER_LOCAL

    if PyThread_tss_create(&CONTEXT_THREAD_KEY) != 0:
        raise MemoryError("Unable to create key for PROJ context in thread.")
    cdef const void *thread_pyproj_context = PyThread_tss_get(&CONTEXT_THREAD_KEY)
    cdef PJ_CONTEXT* pyproj_context = NULL
    if thread_pyproj_context == NULL:
        pyproj_context = proj_context_create()
        pyproj_context_initialize(pyproj_context)
        PyThread_tss_set(&CONTEXT_THREAD_KEY, pyproj_context)
        _CONTEXT_MANAGER_LOCAL.context_manager = ContextManager.create(pyproj_context)
    else:
        pyproj_context = <PJ_CONTEXT*>thread_pyproj_context
    return pyproj_context


def get_context_manager():
    """
    This returns the manager for the context
    responsible for cleanup
    """
    return _CONTEXT_MANAGER_LOCAL.context_manager


cpdef _set_context_data_dir():
    """
    Python compatible function to set the
    data directory on the current context
    """
    set_context_data_dir(pyproj_context_create())


cpdef _set_context_ca_bundle_path(str ca_bundle_path):
    """
    Python compatible function to set the
    CA Bundle path on the current context
    and cache for future generated contexts
    """
    global _CA_BUNDLE_PATH

    b_ca_bundle_path = cstrencode(ca_bundle_path)
    _CA_BUNDLE_PATH = b_ca_bundle_path
    proj_context_set_ca_bundle_path(pyproj_context_create(), _CA_BUNDLE_PATH)


cpdef _set_context_network_enabled(bint enabled):
    """
    Python compatible function to set the
    network enables on the current context
    and cache for future generated contexts
    """
    global _NETWORK_ENABLED

    _NETWORK_ENABLED = enabled
    proj_context_set_enable_network(pyproj_context_create(), _NETWORK_ENABLED)
