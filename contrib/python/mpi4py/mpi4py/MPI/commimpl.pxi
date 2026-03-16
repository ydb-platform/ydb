# -----------------------------------------------------------------------------

cdef memory _buffer = None

cdef inline int attach_buffer(ob, void **p, int *n) except -1:
    global _buffer
    cdef void *bptr = NULL
    cdef MPI_Aint blen = 0
    _buffer = getbuffer_w(ob, &bptr, &blen)
    p[0] = bptr
    n[0] = clipcount(blen)
    return 0

cdef inline object detach_buffer(void *p, int n):
    global _buffer
    cdef object ob = None
    try:
        if (_buffer is not None and
            _buffer.view.buf == p and
            _buffer.view.obj != NULL):
            ob = <object>_buffer.view.obj
        else:
            ob = tomemory(p, n)
    finally:
        _buffer = None
    return ob

# -----------------------------------------------------------------------------

cdef object __UNWEIGHTED__    = <MPI_Aint>MPI_UNWEIGHTED

cdef object __WEIGHTS_EMPTY__ = <MPI_Aint>MPI_WEIGHTS_EMPTY


cdef inline bint is_weights(object obj, object CONST):
    if PYPY: return type(obj) is type(CONST) and obj == CONST
    else:    return obj is CONST

cdef inline bint is_UNWEIGHTED(object weights):
    return is_weights(weights, __UNWEIGHTED__)

cdef inline bint is_WEIGHTS_EMPTY(object weights):
    return is_weights(weights, __WEIGHTS_EMPTY__)

cdef object asarray_weights(object weights, int nweight, int **iweight):
    if weights is None:
        iweight[0] = MPI_UNWEIGHTED
        return None
    if is_UNWEIGHTED(weights):
        iweight[0] = MPI_UNWEIGHTED
        return None
    if is_WEIGHTS_EMPTY(weights):
        if nweight > 0: raise ValueError("empty weights but nonzero degree")
        iweight[0] = MPI_WEIGHTS_EMPTY
        return None
    return chkarray(weights, nweight, iweight)

# -----------------------------------------------------------------------------

cdef inline int comm_neighbors_count(MPI_Comm comm,
                                     int *incoming,
                                     int *outgoing,
                                     ) except -1:
    cdef int topo = MPI_UNDEFINED
    cdef int size=0, ndims=0, rank=0, nneighbors=0
    cdef int indegree=0, outdegree=0, weighted=0
    CHKERR( MPI_Topo_test(comm, &topo) )
    if topo == MPI_UNDEFINED: # XXX
        CHKERR( MPI_Comm_size(comm, &size) )
        indegree = outdegree = size
    elif topo == MPI_CART:
        CHKERR( MPI_Cartdim_get(comm, &ndims) )
        indegree = outdegree = <int>2*ndims
    elif topo == MPI_GRAPH:
        CHKERR( MPI_Comm_rank(comm, &rank) )
        CHKERR( MPI_Graph_neighbors_count(
                comm, rank, &nneighbors) )
        indegree = outdegree = nneighbors
    elif topo == MPI_DIST_GRAPH:
        CHKERR( MPI_Dist_graph_neighbors_count(
                comm, &indegree, &outdegree, &weighted) )
    if incoming != NULL: incoming[0] = indegree
    if outgoing != NULL: outgoing[0] = outdegree
    return 0

# -----------------------------------------------------------------------------

cdef object Lock = None
if PY3:
    try:
        from _thread import allocate_lock as Lock
    except ImportError:
        from _dummy_thread import allocate_lock as Lock
else:
    try:
        from thread  import allocate_lock as Lock
    except ImportError:
        from dummy_thread import allocate_lock as Lock

cdef int  lock_keyval   = MPI_KEYVAL_INVALID
cdef dict lock_registry = {}

cdef inline int lock_free_cb(MPI_Comm comm) \
    except MPI_ERR_UNKNOWN with gil:
    try: del lock_registry[<Py_uintptr_t>comm]
    except KeyError: pass
    return MPI_SUCCESS

@cython.callspec("MPIAPI")
cdef int lock_free_fn(MPI_Comm comm, int keyval,
                      void *attrval, void *xstate) nogil:
    if comm == MPI_COMM_SELF:
        return MPI_Comm_free_keyval(&lock_keyval)
    if not Py_IsInitialized():
        return MPI_SUCCESS
    if <void*>lock_registry == NULL:
        return MPI_SUCCESS
    return lock_free_cb(comm)

cdef inline dict PyMPI_Lock_table(MPI_Comm comm):
    cdef dict table
    cdef int  found = 0
    cdef void *attrval = NULL
    if lock_keyval == MPI_KEYVAL_INVALID:
        CHKERR( MPI_Comm_create_keyval(
            MPI_COMM_NULL_COPY_FN, lock_free_fn, &lock_keyval, NULL) )
        lock_registry[<Py_uintptr_t>MPI_COMM_SELF] = table = {}
        CHKERR( MPI_Comm_set_attr(MPI_COMM_SELF, lock_keyval, <void*> table) )
    CHKERR( MPI_Comm_get_attr(comm, lock_keyval, &attrval, &found) )
    if not found:
        lock_registry[<Py_uintptr_t>comm] = table = {}
        CHKERR( MPI_Comm_set_attr(comm, lock_keyval, <void*> table) )
    else:
        if PYPY: table = lock_registry[<Py_uintptr_t>comm]
        else:    table = <dict> attrval
    return table

cdef inline object PyMPI_Lock(MPI_Comm comm, object key):
    cdef dict   table = PyMPI_Lock_table(comm)
    cdef object lock
    try:
        lock = table[key]
    except KeyError:
        lock = table[key] = Lock()
    return lock


def _comm_lock(Comm comm: Comm, object key: Hashable = None) -> Lock:
    "Create/get communicator lock"
    return PyMPI_Lock(comm.ob_mpi, key)

def _comm_lock_table(Comm comm: Comm) -> Dict[Hashable, Lock]:
    "Internal communicator lock table"
    return PyMPI_Lock_table(comm.ob_mpi)

_lock_table = _comm_lock_table  # backward-compatibility

# -----------------------------------------------------------------------------
