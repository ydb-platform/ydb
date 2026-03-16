__doc__ = """
Message Passing Interface.
"""

from mpi4py.libmpi cimport *

include "stdlib.pxi"
include "typing.pxi"
include "atimport.pxi"

bootstrap()
initialize()

include "asstring.pxi"
include "asbuffer.pxi"
include "asmemory.pxi"
include "asarray.pxi"
include "helpers.pxi"
include "attrimpl.pxi"
include "mpierrhdl.pxi"
include "msgbuffer.pxi"
include "msgpickle.pxi"
include "CAPI.pxi"


# Assorted constants
# ------------------

UNDEFINED = MPI_UNDEFINED
#: Undefined integer value

ANY_SOURCE = MPI_ANY_SOURCE
#: Wildcard source value for receives

ANY_TAG = MPI_ANY_TAG
#: Wildcard tag value for receives

PROC_NULL = MPI_PROC_NULL
#: Special process rank for send/receive

ROOT = MPI_ROOT
#: Root process for collective inter-communications

BOTTOM = __BOTTOM__
#: Special address for buffers

IN_PLACE = __IN_PLACE__
#: *In-place* option for collective communications


# Predefined Attribute Keyvals
# ----------------------------

KEYVAL_INVALID    = MPI_KEYVAL_INVALID

TAG_UB            = MPI_TAG_UB
HOST              = MPI_HOST
IO                = MPI_IO
WTIME_IS_GLOBAL   = MPI_WTIME_IS_GLOBAL

UNIVERSE_SIZE     = MPI_UNIVERSE_SIZE
APPNUM            = MPI_APPNUM

LASTUSEDCODE      = MPI_LASTUSEDCODE

WIN_BASE          = MPI_WIN_BASE
WIN_SIZE          = MPI_WIN_SIZE
WIN_DISP_UNIT     = MPI_WIN_DISP_UNIT
WIN_CREATE_FLAVOR = MPI_WIN_CREATE_FLAVOR
WIN_FLAVOR        = MPI_WIN_CREATE_FLAVOR
WIN_MODEL         = MPI_WIN_MODEL


include "Exception.pyx"
include "Datatype.pyx"
include "Status.pyx"
include "Request.pyx"
include "Message.pyx"
include "Op.pyx"
include "Group.pyx"
include "Info.pyx"
include "Errhandler.pyx"
include "Comm.pyx"
include "Win.pyx"
include "File.pyx"


# Memory Allocation
# -----------------

def Alloc_mem(Aint size: int, Info info: Info = INFO_NULL) -> memory:
    """
    Allocate memory for message passing and RMA
    """
    cdef void *base = NULL
    CHKERR( MPI_Alloc_mem(size, info.ob_mpi, &base) )
    return tomemory(base, size)

def Free_mem(mem: memory) -> None:
    """
    Free memory allocated with `Alloc_mem()`
    """
    cdef void *base = NULL
    cdef memory m = asmemory(mem, &base, NULL)
    CHKERR( MPI_Free_mem(base) )
    m.release()

# Initialization and Exit
# -----------------------

def Init() -> None:
    """
    Initialize the MPI execution environment
    """
    CHKERR( MPI_Init(NULL, NULL) )
    initialize()

def Finalize() -> None:
    """
    Terminate the MPI execution environment
    """
    finalize()
    CHKERR( MPI_Finalize() )

# Levels of MPI threading support
# -------------------------------

THREAD_SINGLE     = MPI_THREAD_SINGLE
#: Only one thread will execute

THREAD_FUNNELED   = MPI_THREAD_FUNNELED
#: MPI calls are *funneled* to the main thread

THREAD_SERIALIZED = MPI_THREAD_SERIALIZED
#: MPI calls are *serialized*

THREAD_MULTIPLE   = MPI_THREAD_MULTIPLE
#: Multiple threads may call MPI

def Init_thread(int required: int = THREAD_MULTIPLE) -> int:
    """
    Initialize the MPI execution environment
    """
    cdef int provided = MPI_THREAD_SINGLE
    CHKERR( MPI_Init_thread(NULL, NULL, required, &provided) )
    initialize()
    return provided

def Query_thread() -> int:
    """
    Return the level of thread support provided by the MPI library
    """
    cdef int provided = MPI_THREAD_SINGLE
    CHKERR( MPI_Query_thread(&provided) )
    return provided

def Is_thread_main() -> bool:
    """
    Indicate whether this thread called `Init` or `Init_thread`
    """
    cdef int flag = 1
    CHKERR( MPI_Is_thread_main(&flag) )
    return <bint>flag

def Is_initialized() -> bool:
    """
    Indicates whether `Init` has been called
    """
    cdef int flag = 0
    CHKERR( MPI_Initialized(&flag) )
    return <bint>flag

def Is_finalized() -> bool:
    """
    Indicates whether `Finalize` has completed
    """
    cdef int flag = 0
    CHKERR( MPI_Finalized(&flag) )
    return <bint>flag

# Implementation Information
# --------------------------

# MPI Version Number
# -----------------

VERSION    = MPI_VERSION
SUBVERSION = MPI_SUBVERSION

def Get_version() -> Tuple[int, int]:
    """
    Obtain the version number of the MPI standard supported
    by the implementation as a tuple ``(version, subversion)``
    """
    cdef int version = 1
    cdef int subversion = 0
    CHKERR( MPI_Get_version(&version, &subversion) )
    return (version, subversion)

def Get_library_version() -> str:
    """
    Obtain the version string of the MPI library
    """
    cdef char name[MPI_MAX_LIBRARY_VERSION_STRING+1]
    cdef int nlen = 0
    CHKERR( MPI_Get_library_version(name, &nlen) )
    return tompistr(name, nlen)

# Environmental Inquires
# ----------------------

def Get_processor_name() -> str:
    """
    Obtain the name of the calling processor
    """
    cdef char name[MPI_MAX_PROCESSOR_NAME+1]
    cdef int nlen = 0
    CHKERR( MPI_Get_processor_name(name, &nlen) )
    return tompistr(name, nlen)

# Timers and Synchronization
# --------------------------

def Wtime() -> float:
    """
    Return an elapsed time on the calling processor
    """
    return MPI_Wtime()

def Wtick() -> float:
    """
    Return the resolution of `Wtime`
    """
    return MPI_Wtick()

# Control of Profiling
# --------------------

def Pcontrol(int level: int) -> None:
    """
    Control profiling
    """
    if level < 0 or level > 2: CHKERR( MPI_ERR_ARG )
    CHKERR( MPI_Pcontrol(level) )


# Maximum string sizes
# --------------------

# MPI-1
MAX_PROCESSOR_NAME = MPI_MAX_PROCESSOR_NAME
MAX_ERROR_STRING   = MPI_MAX_ERROR_STRING
# MPI-2
MAX_PORT_NAME      = MPI_MAX_PORT_NAME
MAX_INFO_KEY       = MPI_MAX_INFO_KEY
MAX_INFO_VAL       = MPI_MAX_INFO_VAL
MAX_OBJECT_NAME    = MPI_MAX_OBJECT_NAME
MAX_DATAREP_STRING = MPI_MAX_DATAREP_STRING
# MPI-3
MAX_LIBRARY_VERSION_STRING = MPI_MAX_LIBRARY_VERSION_STRING

# --------------------------------------------------------------------

cdef extern from *:
    int PyMPI_Get_vendor(const char**,int*,int*,int*) nogil

def get_vendor() -> Tuple[str, Tuple[int, int, int]]:
    """
    Infomation about the underlying MPI implementation

    Returns:
      - a string with the name of the MPI implementation
      - an integer 3-tuple version ``(major, minor, micro)``
    """
    cdef const char *name=NULL
    cdef int major=0, minor=0, micro=0
    CHKERR( PyMPI_Get_vendor(&name, &major, &minor, &micro) )
    return (mpistr(name), (major, minor, micro))

# --------------------------------------------------------------------

cdef extern from "Python.h":
    ctypedef ssize_t Py_intptr_t
    ctypedef size_t  Py_uintptr_t

cdef inline int _mpi_type(object arg, type cls) except -1:
    if isinstance(arg, type):
        if issubclass(arg, cls): return 1
    else:
        if isinstance(arg, cls): return 1
    return 0

def _sizeof(arg: Any) -> int:
    """
    Size in bytes of the underlying MPI handle
    """
    if _mpi_type(arg, Status):     return sizeof(MPI_Status)
    if _mpi_type(arg, Datatype):   return sizeof(MPI_Datatype)
    if _mpi_type(arg, Request):    return sizeof(MPI_Request)
    if _mpi_type(arg, Message):    return sizeof(MPI_Message)
    if _mpi_type(arg, Op):         return sizeof(MPI_Op)
    if _mpi_type(arg, Group):      return sizeof(MPI_Group)
    if _mpi_type(arg, Info):       return sizeof(MPI_Info)
    if _mpi_type(arg, Errhandler): return sizeof(MPI_Errhandler)
    if _mpi_type(arg, Comm):       return sizeof(MPI_Comm)
    if _mpi_type(arg, Win):        return sizeof(MPI_Win)
    if _mpi_type(arg, File):       return sizeof(MPI_File)
    raise TypeError("expecting an MPI type or instance")

def _addressof(arg: Any) -> int:
    """
    Memory address of the underlying MPI handle
    """
    cdef void *ptr = NULL
    if isinstance(arg, Status):
        ptr = <void*>&(<Status>arg).ob_mpi
    elif isinstance(arg, Datatype):
        ptr = <void*>&(<Datatype>arg).ob_mpi
    elif isinstance(arg, Request):
        ptr = <void*>&(<Request>arg).ob_mpi
    elif isinstance(arg, Message):
        ptr = <void*>&(<Message>arg).ob_mpi
    elif isinstance(arg, Op):
        ptr = <void*>&(<Op>arg).ob_mpi
    elif isinstance(arg, Group):
        ptr = <void*>&(<Group>arg).ob_mpi
    elif isinstance(arg, Info):
        ptr = <void*>&(<Info>arg).ob_mpi
    elif isinstance(arg, Errhandler):
        ptr = <void*>&(<Errhandler>arg).ob_mpi
    elif isinstance(arg, Comm):
        ptr = <void*>&(<Comm>arg).ob_mpi
    elif isinstance(arg, Win):
        ptr = <void*>&(<Win>arg).ob_mpi
    elif isinstance(arg, File):
        ptr = <void*>&(<File>arg).ob_mpi
    else:
        raise TypeError("expecting an MPI instance")
    return PyLong_FromVoidPtr(ptr)

def _handleof(arg: Any) -> int:
    """
    Unsigned integer value with the underlying MPI handle
    """
    if isinstance(arg, Status):
        raise NotImplementedError
    elif isinstance(arg, Datatype):
        return <Py_uintptr_t>((<Datatype>arg).ob_mpi)
    elif isinstance(arg, Request):
        return <Py_uintptr_t>((<Request>arg).ob_mpi)
    elif isinstance(arg, Message):
        return <Py_uintptr_t>((<Message>arg).ob_mpi)
    elif isinstance(arg, Op):
        return <Py_uintptr_t>((<Op>arg).ob_mpi)
    elif isinstance(arg, Group):
        return <Py_uintptr_t>((<Group>arg).ob_mpi)
    elif isinstance(arg, Info):
        return <Py_uintptr_t>((<Info>arg).ob_mpi)
    elif isinstance(arg, Errhandler):
        return <Py_uintptr_t>((<Errhandler>arg).ob_mpi)
    elif isinstance(arg, Comm):
        return <Py_uintptr_t>((<Comm>arg).ob_mpi)
    elif isinstance(arg, Win):
        return <Py_uintptr_t>((<Win>arg).ob_mpi)
    elif isinstance(arg, File):
        return <Py_uintptr_t>((<File>arg).ob_mpi)
    else:
        raise TypeError("expecting an MPI instance")

# --------------------------------------------------------------------
