# -----------------------------------------------------------------------------

cdef inline int comm_set_eh(MPI_Comm ob) nogil except -1:
    if ob == MPI_COMM_NULL: return 0
    cdef int opt = options.errors
    if   opt == 0: pass
    elif opt == 1: CHKERR( MPI_Comm_set_errhandler(ob, MPI_ERRORS_RETURN) )
    elif opt == 2: CHKERR( MPI_Comm_set_errhandler(ob, MPI_ERRORS_ARE_FATAL) )
    return 0

cdef inline int win_set_eh(MPI_Win ob) nogil except -1:
    if ob == MPI_WIN_NULL: return 0
    cdef int opt = options.errors
    if   opt == 0: pass
    elif opt == 1: CHKERR( MPI_Win_set_errhandler(ob, MPI_ERRORS_RETURN) )
    elif opt == 2: CHKERR( MPI_Win_set_errhandler(ob, MPI_ERRORS_ARE_FATAL) )
    return 0

cdef inline int file_set_eh(MPI_File ob) nogil except -1:
    if ob == MPI_FILE_NULL: return 0
    cdef int opt = options.errors
    if   opt == 0: pass
    elif opt == 1: CHKERR( MPI_File_set_errhandler(ob, MPI_ERRORS_RETURN) )
    elif opt == 2: CHKERR( MPI_File_set_errhandler(ob, MPI_ERRORS_ARE_FATAL) )
    return 0

# -----------------------------------------------------------------------------
