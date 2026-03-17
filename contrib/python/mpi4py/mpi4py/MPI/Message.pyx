cdef class Message:

    """
    Matched message handle
    """

    def __cinit__(self, Message message: Optional[Message] = None):
        self.ob_mpi = MPI_MESSAGE_NULL
        if message is None: return
        self.ob_mpi = message.ob_mpi
        self.ob_buf = message.ob_buf

    def __dealloc__(self):
        if not (self.flags & PyMPI_OWNED): return
        CHKERR( del_Message(&self.ob_mpi) )

    def __richcmp__(self, other, int op):
        if not isinstance(other, Message): return NotImplemented
        cdef Message s = <Message>self, o = <Message>other
        if   op == Py_EQ: return (s.ob_mpi == o.ob_mpi)
        elif op == Py_NE: return (s.ob_mpi != o.ob_mpi)
        cdef mod = type(self).__module__
        cdef cls = type(self).__name__
        raise TypeError("unorderable type: '%s.%s'" % (mod, cls))

    def __bool__(self) -> bool:
        return self.ob_mpi != MPI_MESSAGE_NULL

    # Matching Probe
    # --------------

    @classmethod
    def Probe(
        cls,
        Comm comm: Comm,
        int source: int = ANY_SOURCE,
        int tag: int = ANY_TAG,
        Status status: Optional[Status] = None,
    ) -> Message:
        """
        Blocking test for a matched message
        """
        cdef MPI_Message cmessage = MPI_MESSAGE_NULL
        cdef MPI_Status *statusp = arg_Status(status)
        with nogil: CHKERR( MPI_Mprobe(
            source, tag, comm.ob_mpi, &cmessage, statusp) )
        cdef Message message = <Message>Message.__new__(cls)
        message.ob_mpi = cmessage
        return message

    @classmethod
    def Iprobe(
        cls,
        Comm comm: Comm,
        int source: int = ANY_SOURCE,
        int tag: int = ANY_TAG,
        Status status: Optional[Status] = None,
    ) -> Optional[Message]:
        """
        Nonblocking test for a matched message
        """
        cdef int flag = 0
        cdef MPI_Message cmessage = MPI_MESSAGE_NULL
        cdef MPI_Status *statusp = arg_Status(status)
        with nogil: CHKERR( MPI_Improbe(
             source, tag, comm.ob_mpi, &flag, &cmessage, statusp) )
        if flag == 0: return None
        cdef Message message = <Message>Message.__new__(cls)
        message.ob_mpi = cmessage
        return message

    # Matched receives
    # ----------------

    def Recv(
        self,
        buf: BufSpec,
        Status status: Optional[Status] = None,
    ) -> None:
        """
        Blocking receive of matched message
        """
        cdef MPI_Message message = self.ob_mpi
        cdef int source = MPI_ANY_SOURCE
        if message == MPI_MESSAGE_NO_PROC:
            source = MPI_PROC_NULL
        cdef _p_msg_p2p rmsg = message_p2p_recv(buf, source)
        cdef MPI_Status *statusp = arg_Status(status)
        with nogil: CHKERR( MPI_Mrecv(
            rmsg.buf, rmsg.count, rmsg.dtype,
            &message, statusp) )
        if self is not __MESSAGE_NO_PROC__:
            self.ob_mpi = message

    def Irecv(self, buf: BufSpec) -> Request:
        """
        Nonblocking receive of matched message
        """
        cdef MPI_Message message = self.ob_mpi
        cdef int source = MPI_ANY_SOURCE
        if message == MPI_MESSAGE_NO_PROC:
            source = MPI_PROC_NULL
        cdef _p_msg_p2p rmsg = message_p2p_recv(buf, source)
        cdef Request request = Request.__new__(Request)
        with nogil: CHKERR( MPI_Imrecv(
            rmsg.buf, rmsg.count, rmsg.dtype,
            &message, &request.ob_mpi) )
        if self is not __MESSAGE_NO_PROC__:
            self.ob_mpi = message
        request.ob_buf = rmsg
        return request

    # Python Communication
    # --------------------
    #
    @classmethod
    def probe(
        cls,
        Comm comm: Comm,
        int source: int = ANY_SOURCE,
        int tag: int = ANY_TAG,
        Status status: Optional[Status] = None,
    ) -> Message:
        """Blocking test for a matched message"""
        cdef Message message = <Message>Message.__new__(cls)
        cdef MPI_Status *statusp = arg_Status(status)
        message.ob_buf = PyMPI_mprobe(source, tag, comm.ob_mpi,
                                      &message.ob_mpi, statusp)
        return message
    #
    @classmethod
    def iprobe(
        cls, Comm comm: Comm,
        int source: int = ANY_SOURCE,
        int tag: int = ANY_TAG,
        Status status: Optional[Status] = None,
    ) -> Optional[Message]:
        """Nonblocking test for a matched message"""
        cdef int flag = 0
        cdef Message message = <Message>Message.__new__(cls)
        cdef MPI_Status *statusp = arg_Status(status)
        message.ob_buf = PyMPI_improbe(source, tag, comm.ob_mpi, &flag,
                                       &message.ob_mpi, statusp)
        if flag == 0: return None
        return message
    #
    def recv(self, Status status: Optional[Status] = None) -> Any:
        """Blocking receive of matched message"""
        cdef object rmsg = self.ob_buf
        cdef MPI_Message message = self.ob_mpi
        cdef MPI_Status *statusp = arg_Status(status)
        rmsg = PyMPI_mrecv(rmsg, &message, statusp)
        if self is not __MESSAGE_NO_PROC__: self.ob_mpi = message
        if self.ob_mpi == MPI_MESSAGE_NULL: self.ob_buf = None
        return rmsg
    #
    def irecv(self) -> Request:
        """Nonblocking receive of matched message"""
        cdef object rmsg = self.ob_buf
        cdef MPI_Message message = self.ob_mpi
        cdef Request request = Request.__new__(Request)
        request.ob_buf = PyMPI_imrecv(rmsg, &message, &request.ob_mpi)
        if self is not __MESSAGE_NO_PROC__: self.ob_mpi = message
        if self.ob_mpi == MPI_MESSAGE_NULL: self.ob_buf = None
        return request

    # Fortran Handle
    # --------------

    def py2f(self) -> int:
        """
        """
        return MPI_Message_c2f(self.ob_mpi)

    @classmethod
    def f2py(cls, arg: int) -> Message:
        """
        """
        cdef Message message = Message.__new__(Message)
        message.ob_mpi = MPI_Message_f2c(arg)
        return message


cdef Message __MESSAGE_NULL__    = new_Message ( MPI_MESSAGE_NULL    )
cdef Message __MESSAGE_NO_PROC__ = new_Message ( MPI_MESSAGE_NO_PROC )


# Predefined message handles
# --------------------------

MESSAGE_NULL    = __MESSAGE_NULL__    #: Null message handle
MESSAGE_NO_PROC = __MESSAGE_NO_PROC__ #: No-proc message handle
