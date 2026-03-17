# -----------------------------------------------------------------------------

cdef dict datarep_registry = {}

@cython.final
@cython.internal
cdef class _p_datarep:

    cdef object read_fn
    cdef object write_fn
    cdef object extent_fn

    def __cinit__(self, read_fn, write_fn, extent_fn):
        self.read_fn   = read_fn
        self.write_fn  = write_fn
        self.extent_fn = extent_fn

    cdef int read(self,
                  void *userbuf,
                  MPI_Datatype datatype,
                  int count,
                  void *filebuf,
                  MPI_Offset position,
                  ) except -1:
        cdef MPI_Aint lb=0, extent=0
        cdef int ierr = MPI_Type_get_extent(datatype, &lb, &extent)
        if ierr != MPI_SUCCESS: return ierr
        cdef MPI_Aint ulen = <MPI_Aint>(position+count) * extent
        cdef MPI_Aint flen = <MPI_Aint>PY_SSIZE_T_MAX # XXX
        cdef object ubuf = tomemory(userbuf, ulen)
        cdef object fbuf = tomemory(filebuf, flen)
        cdef Datatype dtype = Datatype.__new__(Datatype)
        dtype.ob_mpi = datatype
        try: self.read_fn(ubuf, dtype, count, fbuf, position)
        finally: dtype.ob_mpi = MPI_DATATYPE_NULL
        return MPI_SUCCESS

    cdef int write(self,
                  void *userbuf,
                  MPI_Datatype datatype,
                  int count,
                  void *filebuf,
                  MPI_Offset position,
                  ) except -1:
        cdef MPI_Aint lb=0, extent=0
        cdef int ierr = MPI_Type_get_extent(datatype, &lb, &extent)
        if ierr != MPI_SUCCESS: return ierr
        cdef MPI_Aint ulen = <MPI_Aint>(position+count) * extent
        cdef MPI_Aint flen = <MPI_Aint>PY_SSIZE_T_MAX # XXX
        cdef object ubuf = tomemory(userbuf, ulen)
        cdef object fbuf = tomemory(filebuf, flen)
        cdef Datatype dtype = Datatype.__new__(Datatype)
        dtype.ob_mpi = datatype
        try: self.write_fn(ubuf, dtype, count, fbuf, position)
        finally: dtype.ob_mpi = MPI_DATATYPE_NULL
        return MPI_SUCCESS

    cdef int extent(self,
                    MPI_Datatype datatype,
                    MPI_Aint *file_extent,
                    ) except -1:
        cdef Datatype dtype = Datatype.__new__(Datatype)
        dtype.ob_mpi = datatype
        try: file_extent[0] = self.extent_fn(dtype)
        finally: dtype.ob_mpi = MPI_DATATYPE_NULL
        return MPI_SUCCESS

# ---

cdef int datarep_read(
    void *userbuf,
    MPI_Datatype datatype,
    int count,
    void *filebuf,
    MPI_Offset position,
    void *extra_state,
    ) except MPI_ERR_UNKNOWN with gil:
    cdef _p_datarep state = <_p_datarep>extra_state
    cdef int ierr = MPI_SUCCESS
    cdef object exc
    try:
        state.read(userbuf, datatype, count, filebuf, position)
    except MPIException as exc:
        print_traceback()
        ierr = exc.Get_error_code()
    except:
        print_traceback()
        ierr = MPI_ERR_OTHER
    return ierr

cdef int datarep_write(
    void *userbuf,
    MPI_Datatype datatype,
    int count,
    void *filebuf,
    MPI_Offset position,
    void *extra_state,
    ) except MPI_ERR_UNKNOWN with gil:
    cdef _p_datarep state = <_p_datarep>extra_state
    cdef int ierr = MPI_SUCCESS
    cdef object exc
    try:
        state.write(userbuf, datatype, count, filebuf, position)
    except MPIException as exc:
        print_traceback()
        ierr = exc.Get_error_code()
    except:
        print_traceback()
        ierr = MPI_ERR_OTHER
    return ierr

cdef int datarep_extent(
    MPI_Datatype datatype,
    MPI_Aint *file_extent,
    void *extra_state,
    ) except MPI_ERR_UNKNOWN with gil:
    cdef _p_datarep state = <_p_datarep>extra_state
    cdef int ierr = MPI_SUCCESS
    cdef object exc
    try:
        state.extent(datatype, file_extent)
    except MPIException as exc:
        print_traceback()
        ierr = exc.Get_error_code()
    except:
        print_traceback()
        ierr = MPI_ERR_OTHER
    return ierr

# ---

@cython.callspec("MPIAPI")
cdef int datarep_read_fn(
    void *userbuf,
    MPI_Datatype datatype,
    int count,
    void *filebuf,
    MPI_Offset position,
    void *extra_state
    ) nogil:
    if extra_state == NULL:
        return MPI_ERR_INTERN
    if not Py_IsInitialized():
        return MPI_ERR_INTERN
    return datarep_read(userbuf, datatype, count,
                        filebuf, position, extra_state)

@cython.callspec("MPIAPI")
cdef int datarep_write_fn(
    void *userbuf,
    MPI_Datatype datatype,
    int count,
    void *filebuf,
    MPI_Offset position,
    void *extra_state
    ) nogil:
    if extra_state == NULL:
        return MPI_ERR_INTERN
    if not Py_IsInitialized():
        return MPI_ERR_INTERN
    return datarep_write(userbuf, datatype, count,
                         filebuf, position, extra_state)

@cython.callspec("MPIAPI")
cdef int datarep_extent_fn(
    MPI_Datatype datatype,
    MPI_Aint *file_extent,
    void *extra_state
    ) nogil:
    if extra_state == NULL:
        return MPI_ERR_INTERN
    if not Py_IsInitialized():
        return MPI_ERR_INTERN
    return datarep_extent(datatype, file_extent, extra_state)

# -----------------------------------------------------------------------------
