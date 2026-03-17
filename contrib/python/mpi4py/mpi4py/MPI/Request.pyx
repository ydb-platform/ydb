cdef class Request:

    """
    Request handle
    """

    def __cinit__(self, Request request: Optional[Request] = None):
        self.ob_mpi = MPI_REQUEST_NULL
        if request is None: return
        self.ob_mpi = request.ob_mpi
        self.ob_buf = request.ob_buf

    def __dealloc__(self):
        if not (self.flags & PyMPI_OWNED): return
        CHKERR( del_Request(&self.ob_mpi) )

    def __richcmp__(self, other, int op):
        if not isinstance(other, Request): return NotImplemented
        cdef Request s = <Request>self, o = <Request>other
        if   op == Py_EQ: return (s.ob_mpi == o.ob_mpi)
        elif op == Py_NE: return (s.ob_mpi != o.ob_mpi)
        cdef mod = type(self).__module__
        cdef cls = type(self).__name__
        raise TypeError("unorderable type: '%s.%s'" % (mod, cls))

    def __bool__(self) -> bool:
        return self.ob_mpi != MPI_REQUEST_NULL

    # Completion Operations
    # ---------------------

    def Wait(self, Status status: Optional[Status] = None) -> Literal[True]:
        """
        Wait for a send or receive to complete
        """
        cdef MPI_Status *statusp = arg_Status(status)
        with nogil: CHKERR( MPI_Wait(
            &self.ob_mpi, statusp) )
        if self.ob_mpi == MPI_REQUEST_NULL:
            self.ob_buf = None
        return True

    def Test(self, Status status: Optional[Status] = None) -> bool:
        """
        Test for the completion of a send or receive
        """
        cdef int flag = 0
        cdef MPI_Status *statusp = arg_Status(status)
        with nogil: CHKERR( MPI_Test(
            &self.ob_mpi, &flag, statusp) )
        if self.ob_mpi == MPI_REQUEST_NULL:
            self.ob_buf = None
        return <bint>flag

    def Free(self) -> None:
        """
        Free a communication request
        """
        with nogil: CHKERR( MPI_Request_free(&self.ob_mpi) )

    def Get_status(self, Status status: Optional[Status] = None) -> bool:
        """
        Non-destructive test for the completion of a request
        """
        cdef int flag = 0
        cdef MPI_Status *statusp = arg_Status(status)
        with nogil: CHKERR( MPI_Request_get_status(
            self.ob_mpi, &flag, statusp) )
        return <bint>flag

    # Multiple Completions
    # --------------------

    @classmethod
    def Waitany(
        cls,
        requests: Sequence[Request],
        Status status: Optional[Status] = None,
    ) -> int:
        """
        Wait for any previously initiated request to complete
        """
        cdef int count = 0
        cdef MPI_Request *irequests = NULL
        cdef int index = MPI_UNDEFINED
        cdef MPI_Status *statusp = arg_Status(status)
        #
        cdef tmp = acquire_rs(requests, None, &count, &irequests, NULL)
        try:
            with nogil: CHKERR( MPI_Waitany(
                count, irequests, &index, statusp) )
        finally:
            release_rs(requests, None, count, irequests, 0, NULL)
        return index

    @classmethod
    def Testany(
        cls,
        requests: Sequence[Request],
        Status status: Optional[Status] = None,
    ) -> Tuple[int, bool]:
        """
        Test for completion of any previously initiated request
        """
        cdef int count = 0
        cdef MPI_Request *irequests = NULL
        cdef int index = MPI_UNDEFINED
        cdef int flag = 0
        cdef MPI_Status *statusp = arg_Status(status)
        #
        cdef tmp = acquire_rs(requests, None, &count, &irequests, NULL)
        try:
            with nogil: CHKERR( MPI_Testany(
                count, irequests, &index, &flag, statusp) )
        finally:
            release_rs(requests, None, count, irequests, 0, NULL)
        #
        return (index, <bint>flag)

    @classmethod
    def Waitall(
        cls,
        requests: Sequence[Request],
        statuses: Optional[List[Status]] = None,
    ) -> Literal[True]:
        """
        Wait for all previously initiated requests to complete
        """
        cdef int count = 0
        cdef MPI_Request *irequests = NULL
        cdef MPI_Status *istatuses = MPI_STATUSES_IGNORE
        #
        cdef tmp = acquire_rs(requests, statuses,
                              &count, &irequests, &istatuses)
        try:
            with nogil: CHKERR( MPI_Waitall(
                count, irequests, istatuses) )
        finally:
            release_rs(requests, statuses, count, irequests, count, istatuses)
        return True

    @classmethod
    def Testall(
        cls,
        requests: Sequence[Request],
        statuses: Optional[List[Status]] = None,
    ) -> bool:
        """
        Test for completion of all previously initiated requests
        """
        cdef int count = 0
        cdef MPI_Request *irequests = NULL
        cdef int flag = 0
        cdef MPI_Status *istatuses = MPI_STATUSES_IGNORE
        #
        cdef tmp = acquire_rs(requests, statuses,
                              &count, &irequests, &istatuses)
        try:
            with nogil: CHKERR( MPI_Testall(
                count, irequests, &flag, istatuses) )
        finally:
            release_rs(requests, statuses,count, irequests, count, istatuses)
        return <bint>flag

    @classmethod
    def Waitsome(
        cls,
        requests: Sequence[Request],
        statuses: Optional[List[Status]] = None,
    ) -> Optional[List[int]]:
        """
        Wait for some previously initiated requests to complete
        """
        cdef int incount = 0
        cdef MPI_Request *irequests = NULL
        cdef int outcount = MPI_UNDEFINED, *iindices = NULL
        cdef MPI_Status *istatuses = MPI_STATUSES_IGNORE
        #
        cdef tmp1 = acquire_rs(requests, statuses,
                               &incount, &irequests, &istatuses)
        cdef tmp2 = newarray(incount, &iindices)
        try:
            with nogil: CHKERR( MPI_Waitsome(
                incount, irequests, &outcount, iindices, istatuses) )
        finally:
            release_rs(requests, statuses,
                       incount, irequests,
                       outcount, istatuses)
        #
        cdef int i = 0
        cdef object indices = None
        if outcount != MPI_UNDEFINED:
            indices = [iindices[i] for i from 0 <= i < outcount]
        return indices

    @classmethod
    def Testsome(
        cls,
        requests: Sequence[Request],
        statuses: Optional[List[Status]] = None,
    ) -> Optional[List[int]]:
        """
        Test for completion of some previously initiated requests
        """
        cdef int incount = 0
        cdef MPI_Request *irequests = NULL
        cdef int outcount = MPI_UNDEFINED, *iindices = NULL
        cdef MPI_Status *istatuses = MPI_STATUSES_IGNORE
        #
        cdef tmp1 = acquire_rs(requests, statuses,
                               &incount, &irequests, &istatuses)
        cdef tmp2 = newarray(incount, &iindices)
        try:
            with nogil: CHKERR( MPI_Testsome(
                incount, irequests, &outcount, iindices, istatuses) )
        finally:
            release_rs(requests, statuses,
                       incount, irequests,
                       outcount, istatuses)
        #
        cdef int i = 0
        cdef object indices = None
        if outcount != MPI_UNDEFINED:
            indices = [iindices[i] for i from 0 <= i < outcount]
        return indices

    # Cancel
    # ------

    def Cancel(self) -> None:
        """
        Cancel a communication request
        """
        with nogil: CHKERR( MPI_Cancel(&self.ob_mpi) )

    # Fortran Handle
    # --------------

    def py2f(self) -> int:
        """
        """
        return MPI_Request_c2f(self.ob_mpi)

    @classmethod
    def f2py(cls, arg: int) -> Request:
        """
        """
        cdef Request request = Request.__new__(Request)
        if issubclass(cls, Prequest):
            request = <Request>Prequest.__new__(Prequest)
        if issubclass(cls, Grequest):
            request = <Request>Grequest.__new__(Grequest)
        request.ob_mpi = MPI_Request_f2c(arg)
        return request

    # Python Communication
    # --------------------
    #
    def wait(
        self,
        Status status: Optional[Status] = None,
    ) -> Any:
        """
        Wait for a send or receive to complete
        """
        cdef msg = PyMPI_wait(self, status)
        return msg
    #
    def test(
        self,
            Status status: Optional[Status] = None,
    ) -> Tuple[bool, Optional[Any]]:
        """
        Test for the completion of a send or receive
        """
        cdef int flag = 0
        cdef msg = PyMPI_test(self, &flag, status)
        return (<bint>flag, msg)
    #
    def get_status(
        self,
        Status status: Optional[Status] = None,
    ) -> bool:
        """
        Non-destructive test for the completion of a request
        """
        cdef int flag = 0
        cdef MPI_Status *statusp = arg_Status(status)
        with nogil: CHKERR( MPI_Request_get_status(
            self.ob_mpi, &flag, statusp) )
        return <bint>flag
    #
    @classmethod
    def waitany(
        cls,
        requests: Sequence[Request],
        Status status: Optional[Status] = None
    ) -> Tuple[int, Any]:
        """
        Wait for any previously initiated request to complete
        """
        cdef int index = MPI_UNDEFINED
        cdef msg = PyMPI_waitany(requests, &index, status)
        return (index, msg)
    #
    @classmethod
    def testany(
        cls,
        requests: Sequence[Request],
        Status status: Optional[Status] = None,
    ) -> Tuple[int, bool, Optional[Any]]:
        """
        Test for completion of any previously initiated request
        """
        cdef int index = MPI_UNDEFINED
        cdef int flag  = 0
        cdef msg = PyMPI_testany(requests, &index, &flag, status)
        return (index, <bint>flag, msg)
    #
    @classmethod
    def waitall(
        cls,
        requests: Sequence[Request],
        statuses: Optional[List[Status]] = None,
    ) -> List[Any]:
        """
        Wait for all previously initiated requests to complete
        """
        cdef msg = PyMPI_waitall(requests, statuses)
        return msg
    #
    @classmethod
    def testall(
        cls,
        requests: Sequence[Request],
        statuses: Optional[List[Status]] = None
    ) -> Tuple[bool, Optional[List[Any]]]:
        """
        Test for completion of all previously initiated requests
        """
        cdef int flag = 0
        cdef msg = PyMPI_testall(requests, &flag, statuses)
        return (<bint>flag, msg)
    #
    @classmethod
    def waitsome(
        cls,
        requests: Sequence[Request],
        statuses: Optional[List[Status]] = None,
    ) -> Tuple[Optional[List[int]], Optional[List[Any]]]:
        """
        Wait for some previously initiated requests to complete
        """
        return PyMPI_waitsome(requests, statuses)
    #
    @classmethod
    def testsome(
        cls,
        requests: Sequence[Request],
        statuses: Optional[List[Status]] = None,
    ) -> Tuple[Optional[List[int]], Optional[List[Any]]]:
        """
        Test for completion of some previously initiated requests
        """
        return PyMPI_testsome(requests, statuses)
    #
    def cancel(self) -> None:
        """
        Cancel a communication request
        """
        with nogil: CHKERR( MPI_Cancel(&self.ob_mpi) )


cdef class Prequest(Request):

    """
    Persistent request handle
    """

    def __cinit__(self, Request request: Optional[Request] = None):
        if self.ob_mpi == MPI_REQUEST_NULL: return
        <void>(<Prequest?>request)

    def Start(self) -> None:
        """
        Initiate a communication with a persistent request
        """
        with nogil: CHKERR( MPI_Start(&self.ob_mpi) )

    @classmethod
    def Startall(cls, requests: List[Prequest]) -> None:
        """
        Start a collection of persistent requests
        """
        cdef int count = 0
        cdef MPI_Request *irequests = NULL
        cdef tmp = acquire_rs(requests, None, &count, &irequests, NULL)
        #
        try:
            with nogil: CHKERR( MPI_Startall(count, irequests) )
        finally:
            release_rs(requests, None, count, irequests, 0, NULL)



cdef class Grequest(Request):

    """
    Generalized request handle
    """

    def __cinit__(self, Request request: Optional[Request] = None):
        self.ob_grequest = self.ob_mpi
        if self.ob_mpi == MPI_REQUEST_NULL: return
        <void>(<Grequest?>request)

    @classmethod
    def Start(
        cls,
        query_fn: Callable[..., None],
        free_fn: Callable[..., None],
        cancel_fn: Callable[..., None],
        args: Optional[Tuple[Any]] = None,
        kargs: Optional[Dict[str, Any]] = None,
    ) -> Grequest:
        """
        Create and return a user-defined request
        """
        cdef Grequest request = Grequest.__new__(Grequest)
        cdef _p_greq state = \
             _p_greq(query_fn, free_fn, cancel_fn, args, kargs)
        with nogil: CHKERR( MPI_Grequest_start(
            greq_query_fn, greq_free_fn, greq_cancel_fn,
            <void*>state, &request.ob_mpi) )
        Py_INCREF(state)
        request.ob_grequest = request.ob_mpi
        return request

    def Complete(self) -> None:
        """
        Notify that a user-defined request is complete
        """
        if self.ob_mpi != MPI_REQUEST_NULL:
            if self.ob_mpi != self.ob_grequest:
                raise MPIException(MPI_ERR_REQUEST)
        cdef MPI_Request grequest = self.ob_grequest
        self.ob_grequest = self.ob_mpi ## or MPI_REQUEST_NULL ??
        with nogil: CHKERR( MPI_Grequest_complete(grequest) )
        self.ob_grequest = self.ob_mpi ## or MPI_REQUEST_NULL ??



cdef Request __REQUEST_NULL__ = new_Request(MPI_REQUEST_NULL)


# Predefined request handles
# --------------------------

REQUEST_NULL = __REQUEST_NULL__  #: Null request handle
