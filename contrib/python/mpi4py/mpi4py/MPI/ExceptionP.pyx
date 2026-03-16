class Exception(RuntimeError):

    """
    Exception class
    """

    def __init__(self, int ierr: int = 0):
        if ierr < MPI_SUCCESS: ierr = MPI_ERR_UNKNOWN
        self.ob_mpi = ierr
        RuntimeError.__init__(self, self.ob_mpi)

    def __eq__(self, object error) -> bool:
        cdef int ierr = self.ob_mpi
        return <bint> (ierr == error)

    def __ne__(self, object error) -> bool:
        cdef int ierr = self.ob_mpi
        return <bint> (ierr != error)

    def __lt__(self, object error) -> bool:
        cdef int ierr = self.ob_mpi
        return <bint> (ierr < error)

    def __le__(self, object error) -> bool:
        cdef int ierr = self.ob_mpi
        return <bint> (ierr <= error)

    def __gt__(self, object error) -> bool:
        cdef int ierr = self.ob_mpi
        return <bint> (ierr > error)

    def __ge__(self, object error) -> bool:
        cdef int ierr = self.ob_mpi
        return <bint> (ierr >= error)

    def __hash__(self) -> int:
        return hash(self.ob_mpi)

    def __bool__(self) -> bool:
        cdef int ierr = self.ob_mpi
        return ierr != MPI_SUCCESS

    def __int__(self) -> int:
        return self.ob_mpi

    def __repr__(self) -> str:
        return "MPI.Exception(%d)" % self.ob_mpi

    def __str__(self) -> str:
        if not mpi_active():
            return "error code: %d" % self.ob_mpi
        return self.Get_error_string()

    def Get_error_code(self) -> int:
        """
        Error code
        """
        cdef int errorcode = MPI_SUCCESS
        errorcode = self.ob_mpi
        return errorcode

    error_code = property(Get_error_code, doc="error code")

    def Get_error_class(self) -> int:
        """
        Error class
        """
        cdef int errorclass = MPI_SUCCESS
        CHKERR( MPI_Error_class(self.ob_mpi, &errorclass) )
        return errorclass

    error_class = property(Get_error_class, doc="error class")

    def Get_error_string(self) -> str:
        """
        Error string
        """
        cdef char string[MPI_MAX_ERROR_STRING+1]
        cdef int resultlen = 0
        CHKERR( MPI_Error_string(self.ob_mpi, string, &resultlen) )
        return tompistr(string, resultlen)

    error_string = property(Get_error_string, doc="error string")

    if PY2: __nonzero__ = __bool__
