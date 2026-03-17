cdef class Errhandler:

    """
    Error handler
    """

    def __cinit__(self, Errhandler errhandler: Optional[Errhandler] = None):
        self.ob_mpi = MPI_ERRHANDLER_NULL
        if errhandler is None: return
        self.ob_mpi = errhandler.ob_mpi

    def __dealloc__(self):
        if not (self.flags & PyMPI_OWNED): return
        CHKERR( del_Errhandler(&self.ob_mpi) )

    def __richcmp__(self, other, int op):
        if not isinstance(other, Errhandler): return NotImplemented
        cdef Errhandler s = <Errhandler>self, o = <Errhandler>other
        if   op == Py_EQ: return (s.ob_mpi == o.ob_mpi)
        elif op == Py_NE: return (s.ob_mpi != o.ob_mpi)
        cdef mod = type(self).__module__
        cdef cls = type(self).__name__
        raise TypeError("unorderable type: '%s.%s'" % (mod, cls))

    def __bool__(self) -> bool:
        return self.ob_mpi != MPI_ERRHANDLER_NULL

    def Free(self) -> None:
        """
        Free an error handler
        """
        CHKERR( MPI_Errhandler_free(&self.ob_mpi) )
        if self is __ERRORS_RETURN__:    self.ob_mpi = MPI_ERRORS_RETURN
        if self is __ERRORS_ARE_FATAL__: self.ob_mpi = MPI_ERRORS_ARE_FATAL

    # Fortran Handle
    # --------------

    def py2f(self) -> int:
        """
        """
        return MPI_Errhandler_c2f(self.ob_mpi)

    @classmethod
    def f2py(cls, arg: int) -> Errhandler:
        """
        """
        cdef Errhandler errhandler = Errhandler.__new__(Errhandler)
        errhandler.ob_mpi = MPI_Errhandler_f2c(arg)
        return errhandler



cdef Errhandler __ERRHANDLER_NULL__  = new_Errhandler(MPI_ERRHANDLER_NULL)
cdef Errhandler __ERRORS_RETURN__    = new_Errhandler(MPI_ERRORS_RETURN)
cdef Errhandler __ERRORS_ARE_FATAL__ = new_Errhandler(MPI_ERRORS_ARE_FATAL)


# Predefined errhandler handles
# -----------------------------

ERRHANDLER_NULL  = __ERRHANDLER_NULL__  #: Null error handler
ERRORS_RETURN    = __ERRORS_RETURN__    #: Errors return error handler
ERRORS_ARE_FATAL = __ERRORS_ARE_FATAL__ #: Errors are fatal error handler
