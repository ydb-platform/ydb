cdef class Status:

    """
    Status object
    """

    def __cinit__(self, Status status: Optional[Status] = None):
        cdef MPI_Status *s = &self.ob_mpi
        CHKERR( PyMPI_Status_set_source (s, MPI_ANY_SOURCE ) )
        CHKERR( PyMPI_Status_set_tag    (s, MPI_ANY_TAG    ) )
        CHKERR( PyMPI_Status_set_error  (s, MPI_SUCCESS    ) )
        if status is None: return
        self.ob_mpi = status.ob_mpi

    def __richcmp__(self, other, int op):
        if not isinstance(other, Status): return NotImplemented
        cdef Status s = <Status>self, o = <Status>other
        cdef int ne = memcmp(&s.ob_mpi, &o.ob_mpi, sizeof(MPI_Status))
        if   op == Py_EQ: return (ne == 0)
        elif op == Py_NE: return (ne != 0)
        cdef mod = type(self).__module__
        cdef cls = type(self).__name__
        raise TypeError("unorderable type: '%s.%s'" % (mod, cls))

    def Get_source(self) -> int:
        """
        Get message source
        """
        cdef int source = MPI_ANY_SOURCE
        CHKERR( PyMPI_Status_get_source(&self.ob_mpi, &source) )
        return source

    def Set_source(self, int source: int) -> None:
        """
        Set message source
        """
        CHKERR( PyMPI_Status_set_source(&self.ob_mpi, source) )

    property source:
        """source"""
        def __get__(self) -> int:
            return self.Get_source()
        def __set__(self, value: int):
            self.Set_source(value)

    def Get_tag(self) -> int:
        """
        Get message tag
        """
        cdef int tag = MPI_ANY_TAG
        CHKERR( PyMPI_Status_get_tag(&self.ob_mpi, &tag) )
        return tag

    def Set_tag(self, int tag: int) -> None:
        """
        Set message tag
        """
        CHKERR( PyMPI_Status_set_tag(&self.ob_mpi, tag) )

    property tag:
        """tag"""
        def __get__(self) -> int:
            return self.Get_tag()
        def __set__(self, value: int):
            self.Set_tag(value)

    def Get_error(self) -> int:
        """
        Get message error
        """
        cdef int error = MPI_SUCCESS
        CHKERR( PyMPI_Status_get_error(&self.ob_mpi, &error) )
        return error

    def Set_error(self, int error: int) -> None:
        """
        Set message error
        """
        CHKERR( PyMPI_Status_set_error(&self.ob_mpi, error) )

    property error:
        """error"""
        def __get__(self) -> int:
            return self.Get_error()
        def __set__(self, value: int):
            self.Set_error(value)

    def Get_count(self, Datatype datatype: Datatype = BYTE) -> int:
        """
        Get the number of *top level* elements
        """
        cdef MPI_Datatype dtype = datatype.ob_mpi
        cdef int count = MPI_UNDEFINED
        CHKERR( MPI_Get_count(&self.ob_mpi, dtype, &count) )
        return count

    property count:
        """byte count"""
        def __get__(self) -> int:
            return self.Get_count(__BYTE__)

    def Get_elements(self, Datatype datatype: Datatype) -> int:
        """
        Get the number of basic elements in a datatype
        """
        cdef MPI_Datatype dtype = datatype.ob_mpi
        cdef MPI_Count elements = MPI_UNDEFINED
        CHKERR( MPI_Get_elements_x(&self.ob_mpi, dtype, &elements) )
        return elements

    def Set_elements(
        self,
        Datatype datatype: Datatype,
        Count count: int,
    ) -> None:
        """
        Set the number of elements in a status

        .. note:: This should be only used when implementing
           query callback functions for generalized requests
        """
        cdef MPI_Datatype dtype = datatype.ob_mpi
        CHKERR( MPI_Status_set_elements_x(&self.ob_mpi, dtype, count) )

    def Is_cancelled(self) -> bool:
        """
        Test to see if a request was cancelled
        """
        cdef int flag = 0
        CHKERR( MPI_Test_cancelled(&self.ob_mpi, &flag) )
        return <bint>flag

    def Set_cancelled(self, bint flag: bool) -> None:
        """
        Set the cancelled state associated with a status

        .. note:: This should be only used when implementing
           query callback functions for generalized requests
        """
        CHKERR( MPI_Status_set_cancelled(&self.ob_mpi, flag) )

    property cancelled:
        """
        cancelled state
        """
        def __get__(self) -> bool:
            return self.Is_cancelled()
        def __set__(self, value: int):
            self.Set_cancelled(value)

    # Fortran Handle
    # --------------

    def py2f(self) -> List[int]:
        """
        """
        cdef Status status = <Status> self
        cdef Py_ssize_t i = 0
        cdef Py_ssize_t n = <int>(sizeof(MPI_Status)/sizeof(int))
        cdef MPI_Status *c_status = &status.ob_mpi
        cdef MPI_Fint *f_status = NULL
        cdef tmp = allocate(n+1, sizeof(MPI_Fint), &f_status)
        CHKERR( MPI_Status_c2f(c_status, f_status) )
        return [f_status[i] for i from 0 <= i < n]

    @classmethod
    def f2py(cls, arg: List[int]) -> Status:
        """
        """
        cdef Status status = Status.__new__(Status)
        cdef Py_ssize_t i = 0
        cdef Py_ssize_t n = <int>(sizeof(MPI_Status)/sizeof(int))
        cdef MPI_Status *c_status = &status.ob_mpi
        cdef MPI_Fint *f_status = NULL
        cdef tmp = allocate(n+1, sizeof(MPI_Fint), &f_status)
        for i from 0 <= i < n: f_status[i] = arg[i]
        CHKERR( MPI_Status_f2c(f_status, c_status) )
        return status
