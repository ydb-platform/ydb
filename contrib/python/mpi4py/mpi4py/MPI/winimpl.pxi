#------------------------------------------------------------------------------

cdef inline int win_get_base(MPI_Win win,void **base) except -1:
    cdef int flag = 0
    cdef void *attr = NULL
    CHKERR( MPI_Win_get_attr(win, MPI_WIN_BASE, &attr, &flag) )
    base[0] = attr if flag and attr != NULL else NULL
    return 0

cdef inline int win_get_size(MPI_Win win,MPI_Aint *size) except -1:
    cdef int flag = 0
    cdef MPI_Aint *attr = NULL
    CHKERR( MPI_Win_get_attr(win, MPI_WIN_SIZE, &attr, &flag) )
    size[0] = attr[0] if flag and attr != NULL else 0
    return 0

cdef inline int win_get_unit(MPI_Win win,int *disp_unit) except -1:
    cdef int flag  = 0
    cdef int *attr = NULL
    CHKERR( MPI_Win_get_attr(win, MPI_WIN_DISP_UNIT, &attr, &flag) )
    disp_unit[0] = attr[0] if flag and attr != NULL else 1
    return 0

#------------------------------------------------------------------------------
