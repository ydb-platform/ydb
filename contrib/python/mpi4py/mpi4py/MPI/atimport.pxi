# -----------------------------------------------------------------------------

cdef extern from "atimport.h": pass

# -----------------------------------------------------------------------------

cdef extern from "Python.h":
    enum: PY3 "(PY_MAJOR_VERSION>=3)"
    enum: PY2 "(PY_MAJOR_VERSION==2)"
    enum: PYPY "PyMPI_RUNTIME_PYPY"

    void PySys_WriteStderr(char*,...)
    int Py_AtExit(void (*)())

    ctypedef struct PyObject
    PyObject *Py_None
    void Py_CLEAR(PyObject*)

    void Py_INCREF(object)
    void Py_DECREF(object)

# -----------------------------------------------------------------------------

cdef extern from *:
    const char *Py_GETENV(const char[])
    enum: USE_MATCHED_RECV "PyMPI_USE_MATCHED_RECV"

ctypedef struct Options:
    int initialize
    int threads
    int thread_level
    int finalize
    int fast_reduce
    int recv_mprobe
    int errors

cdef Options options
options.initialize = 1
options.threads = 1
options.thread_level = MPI_THREAD_MULTIPLE
options.finalize = 1
options.fast_reduce = 1
options.recv_mprobe = 1
options.errors = 1

cdef object getEnv(object rc, const char name[], object value):
    cdef bytes oname = b"MPI4PY_RC_" + name.upper()
    cdef const char *cvalue = Py_GETENV(oname)
    if cvalue == NULL: return value
    cdef object ovalue = pystr(cvalue)
    cdef bytes  bvalue = PyBytes_FromString(cvalue).lower()
    if bvalue in (b'true',  b'yes', b'on',  b'y', b'1'): ovalue = True
    if bvalue in (b'false', b'no',  b'off', b'n', b'0'): ovalue = False
    try: setattr(rc, pystr(name), ovalue)
    except: pass
    return ovalue

cdef int warnOpt(object name, object value) except -1:
    cdef object warn
    from warnings import warn
    warn("mpi4py.rc: '%s': unexpected value %r" % (name, value))

cdef int getOptions(Options* opts) except -1:
    cdef object rc
    opts.initialize = 1
    opts.threads = 1
    opts.thread_level = MPI_THREAD_MULTIPLE
    opts.finalize = 1
    opts.fast_reduce = 1
    opts.recv_mprobe = 1
    opts.errors = 1
    try: from mpi4py import rc
    except: return 0
    #
    cdef object initialize = True
    cdef object threads = True
    cdef object thread_level = 'multiple'
    cdef object finalize = None
    cdef object fast_reduce = True
    cdef object recv_mprobe = True
    cdef object errors = 'exception'
    try: initialize = rc.initialize
    except: pass
    try: threads = rc.threads
    except: pass
    try: threads = rc.threaded # backward
    except: pass               # compatibility
    try: thread_level = rc.thread_level
    except: pass
    try: finalize = rc.finalize
    except: pass
    try: fast_reduce = rc.fast_reduce
    except: pass
    try: recv_mprobe = rc.recv_mprobe
    except: pass
    try: errors = rc.errors
    except: pass
    initialize   = getEnv(rc, b"initialize",   initialize)
    threads      = getEnv(rc, b"threads",      threads)
    thread_level = getEnv(rc, b"thread_level", thread_level)
    finalize     = getEnv(rc, b"finalize",     finalize)
    fast_reduce  = getEnv(rc, b"fast_reduce",  fast_reduce)
    recv_mprobe  = getEnv(rc, b"recv_mprobe",  recv_mprobe)
    errors       = getEnv(rc, b"errors",       errors)
    #
    if initialize in (True, 'yes'):
        opts.initialize = 1
    elif initialize in (False, 'no'):
        opts.initialize = 0
    else:
        warnOpt("initialize", initialize)
    #
    if threads in (True, 'yes'):
        opts.threads = 1
    elif threads in (False, 'no'):
        opts.threads = 0
    else:
        warnOpt("threads", threads)
    #
    if thread_level == 'single':
        opts.thread_level = MPI_THREAD_SINGLE
    elif thread_level == 'funneled':
        opts.thread_level = MPI_THREAD_FUNNELED
    elif thread_level == 'serialized':
        opts.thread_level = MPI_THREAD_SERIALIZED
    elif thread_level == 'multiple':
        opts.thread_level = MPI_THREAD_MULTIPLE
    else:
        warnOpt("thread_level", thread_level)
    #
    if finalize is None:
        opts.finalize = opts.initialize
    elif finalize in (True, 'yes'):
        opts.finalize = 1
    elif finalize in (False, 'no'):
        opts.finalize = 0
    else:
        warnOpt("finalize", finalize)
    #
    if fast_reduce in (True, 'yes'):
        opts.fast_reduce = 1
    elif fast_reduce in (False, 'no'):
        opts.fast_reduce = 0
    else:
        warnOpt("fast_reduce", fast_reduce)
    #
    if recv_mprobe in (True, 'yes'):
        opts.recv_mprobe = 1 and USE_MATCHED_RECV
    elif recv_mprobe in (False, 'no'):
        opts.recv_mprobe = 0
    else:
        warnOpt("recv_mprobe", recv_mprobe)
    #
    if errors == 'default':
        opts.errors = 0
    elif errors == 'exception':
        opts.errors = 1
    elif errors == 'fatal':
        opts.errors = 2
    else:
        warnOpt("errors", errors)
    #
    return 0

# -----------------------------------------------------------------------------

cdef extern from *:
    int PyMPI_Commctx_finalize() nogil

cdef int bootstrap() except -1:
    # Get options from 'mpi4py.rc' module
    getOptions(&options)
    # Cleanup at (the very end of) Python exit
    if Py_AtExit(atexit) < 0:
        PySys_WriteStderr(b"warning: could not register "
                          b"cleanup with Py_AtExit()%s", b"\n")
    # Do we have to initialize MPI?
    cdef int initialized = 1
    <void>MPI_Initialized(&initialized)
    if initialized:
        options.finalize = 0
        return 0
    if not options.initialize:
        return 0
    # MPI initialization
    cdef int ierr = MPI_SUCCESS
    cdef int required = MPI_THREAD_SINGLE
    cdef int provided = MPI_THREAD_SINGLE
    if options.threads:
        required = options.thread_level
        ierr = MPI_Init_thread(NULL, NULL, required, &provided)
        if ierr != MPI_SUCCESS: raise RuntimeError(
            "MPI_Init_thread() failed [error code: %d]" % ierr)
    else:
        ierr = MPI_Init(NULL, NULL)
        if ierr != MPI_SUCCESS: raise RuntimeError(
            "MPI_Init() failed [error code: %d]" % ierr)
    return 0

cdef inline int mpi_active() nogil:
    cdef int ierr = MPI_SUCCESS
    # MPI initialized ?
    cdef int initialized = 0
    ierr = MPI_Initialized(&initialized)
    if not initialized or ierr != MPI_SUCCESS: return 0
    # MPI finalized ?
    cdef int finalized = 1
    ierr = MPI_Finalized(&finalized)
    if finalized or ierr != MPI_SUCCESS: return 0
    # MPI should be active ...
    return 1

cdef int initialize() nogil except -1:
    if not mpi_active(): return 0
    comm_set_eh(MPI_COMM_SELF)
    comm_set_eh(MPI_COMM_WORLD)
    return 0

cdef void finalize() nogil:
    if not mpi_active(): return
    <void>PyMPI_Commctx_finalize()

cdef int abort_status = 0

cdef void atexit() nogil:
    if not mpi_active(): return
    if abort_status:
        <void>MPI_Abort(MPI_COMM_WORLD, abort_status)
    finalize()
    if options.finalize:
        <void>MPI_Finalize()

def _set_abort_status(object status: Any) -> None:
    "Helper for ``python -m mpi4py.run ...``"
    global abort_status
    try:
        abort_status = status
    except:
        abort_status = 1 if status else 0

# -----------------------------------------------------------------------------

# Vile hack for raising a exception and not contaminate the traceback

cdef extern from *:
    enum: PyMPI_ERR_UNAVAILABLE

cdef extern from "Python.h":
    void PyErr_SetObject(object, object)
    void *PyExc_RuntimeError
    void *PyExc_NotImplementedError

cdef object MPIException = <object>PyExc_RuntimeError

cdef int PyMPI_Raise(int ierr) except -1 with gil:
    if ierr == PyMPI_ERR_UNAVAILABLE:
        PyErr_SetObject(<object>PyExc_NotImplementedError, None)
        return 0
    if (<void*>MPIException) != NULL:
        PyErr_SetObject(MPIException, <long>ierr)
    else:
        PyErr_SetObject(<object>PyExc_RuntimeError, <long>ierr)
    return 0

cdef inline int CHKERR(int ierr) nogil except -1:
    if ierr == MPI_SUCCESS: return 0
    PyMPI_Raise(ierr)
    return -1

cdef inline void print_traceback():
    cdef object sys, traceback
    import sys, traceback
    traceback.print_exc()
    try: sys.stderr.flush()
    except: pass

# -----------------------------------------------------------------------------

# PyPy: Py_IsInitialized() cannot be called without the GIL

cdef extern from "Python.h":
    int _Py_IsInitialized"Py_IsInitialized"() nogil

cdef object _pypy_sentinel = None

cdef inline int Py_IsInitialized() nogil:
    if PYPY and (<void*>_pypy_sentinel) == NULL: return 0
    return _Py_IsInitialized()

# -----------------------------------------------------------------------------
