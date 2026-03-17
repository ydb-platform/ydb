# Communicator Comparisons
# ------------------------

IDENT     = MPI_IDENT     #: Groups are identical, contexts are the same
CONGRUENT = MPI_CONGRUENT #: Groups are identical, contexts are different
SIMILAR   = MPI_SIMILAR   #: Groups are similar, rank order differs
UNEQUAL   = MPI_UNEQUAL   #: Groups are different


# Communicator Topologies
# -----------------------

CART       = MPI_CART       #: Cartesian topology
GRAPH      = MPI_GRAPH      #: General graph topology
DIST_GRAPH = MPI_DIST_GRAPH #: Distributed graph topology


# Graph Communicator Weights
# --------------------------

UNWEIGHTED    = __UNWEIGHTED__     #: Unweighted graph
WEIGHTS_EMPTY = __WEIGHTS_EMPTY__  #: Empty graph weights


# Communicator Split Type
# -----------------------

COMM_TYPE_SHARED = MPI_COMM_TYPE_SHARED


cdef class Comm:

    """
    Communicator
    """

    def __cinit__(self, Comm comm: Optional[Comm] = None):
        self.ob_mpi = MPI_COMM_NULL
        if comm is None: return
        self.ob_mpi = comm.ob_mpi

    def __dealloc__(self):
        if not (self.flags & PyMPI_OWNED): return
        CHKERR( del_Comm(&self.ob_mpi) )

    def __richcmp__(self, other, int op):
        if not isinstance(other, Comm): return NotImplemented
        cdef Comm s = <Comm>self, o = <Comm>other
        if   op == Py_EQ: return (s.ob_mpi == o.ob_mpi)
        elif op == Py_NE: return (s.ob_mpi != o.ob_mpi)
        cdef mod = type(self).__module__
        cdef cls = type(self).__name__
        raise TypeError("unorderable type: '%s.%s'" % (mod, cls))

    def __bool__(self) -> bool:
        return self.ob_mpi != MPI_COMM_NULL

    # Group
    # -----

    def Get_group(self) -> Group:
        """
        Access the group associated with a communicator
        """
        cdef Group group = Group.__new__(Group)
        with nogil: CHKERR( MPI_Comm_group(self.ob_mpi, &group.ob_mpi) )
        return group

    property group:
        """communicator group"""
        def __get__(self) -> Group:
            return self.Get_group()

    # Communicator Accessors
    # ----------------------

    def Get_size(self) -> int:
        """
        Return the number of processes in a communicator
        """
        cdef int size = -1
        CHKERR( MPI_Comm_size(self.ob_mpi, &size) )
        return size

    property size:
        """number of processes in communicator"""
        def __get__(self) -> int:
            return self.Get_size()

    def Get_rank(self) -> int:
        """
        Return the rank of this process in a communicator
        """
        cdef int rank = MPI_PROC_NULL
        CHKERR( MPI_Comm_rank(self.ob_mpi, &rank) )
        return rank

    property rank:
        """rank of this process in communicator"""
        def __get__(self) -> int:
            return self.Get_rank()

    @classmethod
    def Compare(cls, Comm comm1: Comm, Comm comm2: Comm) -> int:
        """
        Compare two communicators
        """
        cdef int flag = MPI_UNEQUAL
        with nogil: CHKERR( MPI_Comm_compare(
            comm1.ob_mpi, comm2.ob_mpi, &flag) )
        return flag

    # Communicator Constructors
    # -------------------------

    def Clone(self) -> Comm:
        """
        Clone an existing communicator
        """
        cdef type comm_type = type(self)
        cdef Comm comm = <Comm>comm_type.__new__(comm_type)
        with nogil: CHKERR( MPI_Comm_dup(self.ob_mpi, &comm.ob_mpi) )
        comm_set_eh(comm.ob_mpi)
        return comm

    def Dup(self, Info info: Optional[Info] = None) -> Comm:
        """
        Duplicate an existing communicator
        """
        cdef MPI_Info cinfo = arg_Info(info)
        cdef type comm_type = type(self)
        cdef Comm comm = <Comm>comm_type.__new__(comm_type)
        if info is None:
            with nogil: CHKERR( MPI_Comm_dup(
                self.ob_mpi, &comm.ob_mpi) )
        else:
            with nogil: CHKERR( MPI_Comm_dup_with_info(
                self.ob_mpi, cinfo, &comm.ob_mpi) )
        comm_set_eh(comm.ob_mpi)
        return comm

    def Dup_with_info(self, Info info: Info) -> Comm:
        """
        Duplicate an existing communicator
        """
        cdef type comm_type = type(self)
        cdef Comm comm = <Comm>comm_type.__new__(comm_type)
        with nogil: CHKERR( MPI_Comm_dup_with_info(
            self.ob_mpi, info.ob_mpi, &comm.ob_mpi) )
        comm_set_eh(comm.ob_mpi)
        return comm

    def Idup(self) -> Tuple[Comm, Request]:
        """
        Nonblocking duplicate an existing communicator
        """
        cdef type comm_type = type(self)
        cdef Comm comm = <Comm>comm_type.__new__(comm_type)
        cdef Request request = Request.__new__(Request)
        with nogil: CHKERR( MPI_Comm_idup(
            self.ob_mpi, &comm.ob_mpi, &request.ob_mpi) )
        comm_set_eh(comm.ob_mpi)
        return (comm, request)

    def Create(self, Group group: Group) -> Comm:
        """
        Create communicator from group
        """
        cdef type comm_type = Comm
        if   isinstance(self, Intracomm): comm_type = Intracomm
        elif isinstance(self, Intercomm): comm_type = Intercomm
        cdef Comm comm = <Comm>comm_type.__new__(comm_type)
        with nogil: CHKERR( MPI_Comm_create(
            self.ob_mpi, group.ob_mpi, &comm.ob_mpi) )
        comm_set_eh(comm.ob_mpi)
        return comm

    def Create_group(self, Group group: Group, int tag: int = 0) -> Comm:
        """
        Create communicator from group
        """
        cdef type comm_type = Comm
        if   isinstance(self, Intracomm): comm_type = Intracomm
        elif isinstance(self, Intercomm): comm_type = Intercomm
        cdef Comm comm = <Comm>comm_type.__new__(comm_type)
        with nogil: CHKERR( MPI_Comm_create_group(
            self.ob_mpi, group.ob_mpi, tag, &comm.ob_mpi) )
        comm_set_eh(comm.ob_mpi)
        return comm

    def Split(self, int color: int = 0, int key: int = 0) -> Comm:
        """
        Split communicator by color and key
        """
        cdef type comm_type = Comm
        if   isinstance(self, Intracomm): comm_type = Intracomm
        elif isinstance(self, Intercomm): comm_type = Intercomm
        cdef Comm comm = <Comm>comm_type.__new__(comm_type)
        with nogil: CHKERR( MPI_Comm_split(
            self.ob_mpi, color, key, &comm.ob_mpi) )
        comm_set_eh(comm.ob_mpi)
        return comm

    def Split_type(
        self,
        int split_type: int,
        int key: int = 0,
        Info info: Info = INFO_NULL,
    ) -> Comm:
        """
        Split communicator by split type
        """
        cdef type comm_type = Comm
        if   isinstance(self, Intracomm): comm_type = Intracomm
        elif isinstance(self, Intercomm): comm_type = Intercomm
        cdef Comm comm = <Comm>comm_type.__new__(comm_type)
        with nogil: CHKERR( MPI_Comm_split_type(
            self.ob_mpi, split_type, key, info.ob_mpi, &comm.ob_mpi) )
        comm_set_eh(comm.ob_mpi)
        return comm

    # Communicator Destructor
    # -----------------------

    def Free(self) -> None:
        """
        Free a communicator
        """
        with nogil: CHKERR( MPI_Comm_free(&self.ob_mpi) )
        if self is __COMM_SELF__:  self.ob_mpi = MPI_COMM_SELF
        if self is __COMM_WORLD__: self.ob_mpi = MPI_COMM_WORLD

    # Communicator Info
    # -----------------

    def Set_info(self, Info info: Info) -> None:
        """
        Set new values for the hints
        associated with a communicator
        """
        with nogil: CHKERR( MPI_Comm_set_info(
            self.ob_mpi, info.ob_mpi) )

    def Get_info(self) -> Info:
        """
        Return the hints for a communicator
        that are currently in use
        """
        cdef Info info = Info.__new__(Info)
        with nogil: CHKERR( MPI_Comm_get_info(
            self.ob_mpi, &info.ob_mpi) )
        return info

    property info:
        """communicator info"""
        def __get__(self) -> Info:
            return self.Get_info()
        def __set__(self, value: Info):
            self.Set_info(value)

    # Point to Point communication
    # ----------------------------

    # Blocking Send and Receive Operations
    # ------------------------------------

    def Send(
        self,
        buf: BufSpec,
        int dest: int,
        int tag: int = 0,
    ) -> None:
        """
        Blocking send

        .. note:: This function may block until the message is
           received. Whether or not `Send` blocks depends on
           several factors and is implementation dependent
        """
        cdef _p_msg_p2p smsg = message_p2p_send(buf, dest)
        with nogil: CHKERR( MPI_Send(
            smsg.buf, smsg.count, smsg.dtype,
            dest, tag, self.ob_mpi) )

    def Recv(
        self,
        buf: BufSpec,
        int source: int = ANY_SOURCE,
        int tag: int = ANY_TAG,
        Status status: Optional[Status] = None,
    ) -> None:
        """
        Blocking receive

        .. note:: This function blocks until the message is received
        """
        cdef _p_msg_p2p rmsg = message_p2p_recv(buf, source)
        cdef MPI_Status *statusp = arg_Status(status)
        with nogil: CHKERR( MPI_Recv(
            rmsg.buf, rmsg.count, rmsg.dtype,
            source, tag, self.ob_mpi, statusp) )

    # Send-Receive
    # ------------

    def Sendrecv(
        self,
        sendbuf: BufSpec,
        int dest: int,
        int sendtag: int = 0,
        recvbuf: BufSpec = None,
        int source: int = ANY_SOURCE,
        int recvtag: int = ANY_TAG,
        Status status: Optional[Status] = None,
    ) -> None:
        """
        Send and receive a message

        .. note:: This function is guaranteed not to deadlock in
           situations where pairs of blocking sends and receives may
           deadlock.

        .. caution:: A common mistake when using this function is to
           mismatch the tags with the source and destination ranks,
           which can result in deadlock.
        """
        cdef _p_msg_p2p smsg = message_p2p_send(sendbuf, dest)
        cdef _p_msg_p2p rmsg = message_p2p_recv(recvbuf, source)
        cdef MPI_Status *statusp = arg_Status(status)
        with nogil: CHKERR( MPI_Sendrecv(
            smsg.buf, smsg.count, smsg.dtype, dest,   sendtag,
            rmsg.buf, rmsg.count, rmsg.dtype, source, recvtag,
            self.ob_mpi, statusp) )

    def Sendrecv_replace(
        self,
        buf: BufSpec,
        int dest: int,
        int sendtag: int = 0,
        int source: int = ANY_SOURCE,
        int recvtag: int = ANY_TAG,
        Status status: Optional[Status] = None,
    ) -> None:
        """
        Send and receive a message

        .. note:: This function is guaranteed not to deadlock in
           situations where pairs of blocking sends and receives may
           deadlock.

        .. caution:: A common mistake when using this function is to
           mismatch the tags with the source and destination ranks,
           which can result in deadlock.
        """
        cdef int rank = MPI_PROC_NULL
        if dest   != MPI_PROC_NULL: rank = dest
        if source != MPI_PROC_NULL: rank = source
        cdef _p_msg_p2p rmsg = message_p2p_recv(buf, rank)
        cdef MPI_Status *statusp = arg_Status(status)
        with nogil: CHKERR( MPI_Sendrecv_replace(
            rmsg.buf, rmsg.count, rmsg.dtype,
            dest, sendtag, source, recvtag,
            self.ob_mpi, statusp) )

    # Nonblocking Communications
    # --------------------------

    def Isend(
        self,
        buf: BufSpec,
        int dest: int,
        int tag: int = 0,
    ) -> Request:
        """
        Nonblocking send
        """
        cdef _p_msg_p2p smsg = message_p2p_send(buf, dest)
        cdef Request request = Request.__new__(Request)
        with nogil: CHKERR( MPI_Isend(
            smsg.buf, smsg.count, smsg.dtype,
            dest, tag, self.ob_mpi, &request.ob_mpi) )
        request.ob_buf = smsg
        return request

    def Irecv(
        self,
        buf: BufSpec,
        int source: int = ANY_SOURCE,
        int tag: int = ANY_TAG,
    ) -> Request:
        """
        Nonblocking receive
        """
        cdef _p_msg_p2p rmsg = message_p2p_recv(buf, source)
        cdef Request request = Request.__new__(Request)
        with nogil: CHKERR( MPI_Irecv(
            rmsg.buf, rmsg.count, rmsg.dtype,
            source, tag, self.ob_mpi, &request.ob_mpi) )
        request.ob_buf = rmsg
        return request

    # Probe
    # -----

    def Probe(
        self,
        int source: int = ANY_SOURCE,
        int tag: int = ANY_TAG,
        Status status: Optional[Status] = None,
    ) -> Literal[True]:
        """
        Blocking test for a message

        .. note:: This function blocks until the message arrives.
        """
        cdef MPI_Status *statusp = arg_Status(status)
        with nogil: CHKERR( MPI_Probe(
            source, tag, self.ob_mpi, statusp) )
        return True

    def Iprobe(
        self,
        int source: int = ANY_SOURCE,
        int tag: int = ANY_TAG,
        Status status: Optional[Status] = None,
    ) -> bool:
        """
        Nonblocking test for a message
        """
        cdef int flag = 0
        cdef MPI_Status *statusp = arg_Status(status)
        with nogil: CHKERR( MPI_Iprobe(
            source, tag, self.ob_mpi, &flag, statusp) )
        return <bint>flag

    # Matching Probe
    # --------------

    def Mprobe(
        self,
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
            source, tag, self.ob_mpi, &cmessage, statusp) )
        cdef Message message = Message.__new__(Message)
        message.ob_mpi = cmessage
        return message

    def Improbe(
        self,
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
             source, tag, self.ob_mpi, &flag, &cmessage, statusp) )
        if flag == 0: return None
        cdef Message message = Message.__new__(Message)
        message.ob_mpi = cmessage
        return message

    # Persistent Communication
    # ------------------------

    def Send_init(
        self,
        buf: BufSpec,
        int dest: int,
        int tag: int = 0,
    ) -> Prequest:
        """
        Create a persistent request for a standard send
        """
        cdef _p_msg_p2p smsg = message_p2p_send(buf, dest)
        cdef Prequest request = Prequest.__new__(Prequest)
        with nogil: CHKERR( MPI_Send_init(
            smsg.buf, smsg.count, smsg.dtype,
            dest, tag, self.ob_mpi, &request.ob_mpi) )
        request.ob_buf = smsg
        return request

    def Recv_init(
        self,
        buf: BufSpec,
        int source: int = ANY_SOURCE,
        int tag: int = ANY_TAG,
    ) -> Prequest:
        """
        Create a persistent request for a receive
        """
        cdef _p_msg_p2p rmsg = message_p2p_recv(buf, source)
        cdef Prequest request = Prequest.__new__(Prequest)
        with nogil: CHKERR( MPI_Recv_init(
            rmsg.buf, rmsg.count, rmsg.dtype,
            source, tag, self.ob_mpi, &request.ob_mpi) )
        request.ob_buf = rmsg
        return request

    # Communication Modes
    # -------------------

    # Blocking calls

    def Bsend(
        self,
        buf: BufSpec,
        int dest: int,
        int tag: int = 0,
    ) -> None:
        """
        Blocking send in buffered mode
        """
        cdef _p_msg_p2p smsg = message_p2p_send(buf, dest)
        with nogil: CHKERR( MPI_Bsend(
            smsg.buf, smsg.count, smsg.dtype,
            dest, tag, self.ob_mpi) )

    def Ssend(
        self,
        buf: BufSpec,
        int dest: int,
        int tag: int = 0,
    ) -> None:
        """
        Blocking send in synchronous mode
        """
        cdef _p_msg_p2p smsg = message_p2p_send(buf, dest)
        with nogil: CHKERR( MPI_Ssend(
            smsg.buf, smsg.count, smsg.dtype,
            dest, tag, self.ob_mpi) )

    def Rsend(
        self,
        buf: BufSpec,
        int dest: int,
        int tag: int = 0,
    ) -> None:
        """
        Blocking send in ready mode
        """
        cdef _p_msg_p2p smsg = message_p2p_send(buf, dest)
        with nogil: CHKERR( MPI_Rsend(
            smsg.buf, smsg.count, smsg.dtype,
            dest, tag, self.ob_mpi) )

    # Nonblocking calls

    def Ibsend(
        self,
        buf: BufSpec,
        int dest: int,
        int tag: int = 0,
    ) -> Request:
        """
        Nonblocking send in buffered mode
        """
        cdef _p_msg_p2p smsg = message_p2p_send(buf, dest)
        cdef Request request = Request.__new__(Request)
        with nogil: CHKERR( MPI_Ibsend(
            smsg.buf, smsg.count, smsg.dtype,
            dest, tag, self.ob_mpi, &request.ob_mpi) )
        request.ob_buf = smsg
        return request

    def Issend(
        self,
        buf: BufSpec,
        int dest: int,
        int tag: int = 0,
    ) -> Request:
        """
        Nonblocking send in synchronous mode
        """
        cdef _p_msg_p2p smsg = message_p2p_send(buf, dest)
        cdef Request request = Request.__new__(Request)
        with nogil: CHKERR( MPI_Issend(
            smsg.buf, smsg.count, smsg.dtype,
            dest, tag, self.ob_mpi, &request.ob_mpi) )
        request.ob_buf = smsg
        return request

    def Irsend(
        self,
        buf: BufSpec,
        int dest: int,
        int tag: int = 0,
    ) -> Request:
        """
        Nonblocking send in ready mode
        """
        cdef _p_msg_p2p smsg = message_p2p_send(buf, dest)
        cdef Request request = Request.__new__(Request)
        with nogil: CHKERR( MPI_Irsend(
            smsg.buf, smsg.count, smsg.dtype,
            dest, tag, self.ob_mpi, &request.ob_mpi) )
        request.ob_buf = smsg
        return request

    # Persistent Requests

    def Bsend_init(
        self,
        buf: BufSpec,
        int dest: int,
        int tag: int = 0,
    ) -> Request:
        """
        Persistent request for a send in buffered mode
        """
        cdef _p_msg_p2p smsg = message_p2p_send(buf, dest)
        cdef Prequest request = Prequest.__new__(Prequest)
        with nogil: CHKERR( MPI_Bsend_init(
            smsg.buf, smsg.count, smsg.dtype,
            dest, tag, self.ob_mpi, &request.ob_mpi) )
        request.ob_buf = smsg
        return request

    def Ssend_init(
        self,
        buf: BufSpec,
        int dest: int,
        int tag: int = 0,
    ) -> Request:
        """
        Persistent request for a send in synchronous mode
        """
        cdef _p_msg_p2p smsg = message_p2p_send(buf, dest)
        cdef Prequest request = Prequest.__new__(Prequest)
        with nogil: CHKERR( MPI_Ssend_init(
            smsg.buf, smsg.count, smsg.dtype,
            dest, tag, self.ob_mpi, &request.ob_mpi) )
        request.ob_buf = smsg
        return request

    def Rsend_init(
        self,
        buf: BufSpec,
        int dest: int,
        int tag: int = 0,
    ) -> Request:
        """
        Persistent request for a send in ready mode
        """
        cdef _p_msg_p2p smsg = message_p2p_send(buf, dest)
        cdef Prequest request = Prequest.__new__(Prequest)
        with nogil: CHKERR( MPI_Rsend_init(
            smsg.buf, smsg.count, smsg.dtype,
            dest, tag, self.ob_mpi, &request.ob_mpi) )
        request.ob_buf = smsg
        return request

    # Collective Communications
    # -------------------------

    # Barrier Synchronization
    # -----------------------

    def Barrier(self) -> None:
        """
        Barrier synchronization
        """
        with nogil: CHKERR( MPI_Barrier(self.ob_mpi) )

    # Global Communication Functions
    # ------------------------------

    def Bcast(
        self,
        buf: BufSpec,
        int root: int = 0,
    ) -> None:
        """
        Broadcast a message from one process
        to all other processes in a group
        """
        cdef _p_msg_cco m = message_cco()
        m.for_bcast(buf, root, self.ob_mpi)
        with nogil: CHKERR( MPI_Bcast(
            m.sbuf, m.scount, m.stype,
            root, self.ob_mpi) )

    def Gather(
        self,
        sendbuf: Union[BufSpec, InPlace],
        recvbuf: Optional[BufSpecB],
        int root: int = 0,
    ) -> None:
        """
        Gather together values from a group of processes
        """
        cdef _p_msg_cco m = message_cco()
        m.for_gather(0, sendbuf, recvbuf, root, self.ob_mpi)
        with nogil: CHKERR( MPI_Gather(
            m.sbuf, m.scount, m.stype,
            m.rbuf, m.rcount, m.rtype,
            root, self.ob_mpi) )

    def Gatherv(
        self,
        sendbuf: Union[BufSpec, InPlace],
        recvbuf: Optional[BufSpecV],
        int root: int = 0,
    ) -> None:
        """
        Gather Vector, gather data to one process from all other
        processes in a group providing different amount of data and
        displacements at the receiving sides
        """
        cdef _p_msg_cco m = message_cco()
        m.for_gather(1, sendbuf, recvbuf, root, self.ob_mpi)
        with nogil: CHKERR( MPI_Gatherv(
            m.sbuf, m.scount,             m.stype,
            m.rbuf, m.rcounts, m.rdispls, m.rtype,
            root, self.ob_mpi) )

    def Scatter(
        self,
        sendbuf: Optional[BufSpecB],
        recvbuf: Union[BufSpec, InPlace],
        int root: int = 0,
    ) -> None:
        """
        Scatter data from one process
        to all other processes in a group
        """
        cdef _p_msg_cco m = message_cco()
        m.for_scatter(0, sendbuf, recvbuf, root, self.ob_mpi)
        with nogil: CHKERR( MPI_Scatter(
            m.sbuf, m.scount, m.stype,
            m.rbuf, m.rcount, m.rtype,
            root, self.ob_mpi) )

    def Scatterv(
        self,
        sendbuf: Optional[BufSpecV],
        recvbuf: Union[BufSpec, InPlace],
        int root: int = 0,
    ) -> None:
        """
        Scatter Vector, scatter data from one process to all other
        processes in a group providing different amount of data and
        displacements at the sending side
        """
        cdef _p_msg_cco m = message_cco()
        m.for_scatter(1, sendbuf, recvbuf, root, self.ob_mpi)
        with nogil: CHKERR( MPI_Scatterv(
            m.sbuf, m.scounts, m.sdispls, m.stype,
            m.rbuf, m.rcount,             m.rtype,
            root, self.ob_mpi) )

    def Allgather(
        self,
        sendbuf: Union[BufSpec, InPlace],
        recvbuf: BufSpecB,
    ) -> None:
        """
        Gather to All, gather data from all processes and
        distribute it to all other processes in a group
        """
        cdef _p_msg_cco m = message_cco()
        m.for_allgather(0, sendbuf, recvbuf, self.ob_mpi)
        with nogil: CHKERR( MPI_Allgather(
            m.sbuf, m.scount, m.stype,
            m.rbuf, m.rcount, m.rtype,
            self.ob_mpi) )

    def Allgatherv(
        self,
        sendbuf: Union[BufSpec, InPlace],
        recvbuf: BufSpecV,
    ) -> None:
        """
        Gather to All Vector, gather data from all processes and
        distribute it to all other processes in a group providing
        different amount of data and displacements
        """
        cdef _p_msg_cco m = message_cco()
        m.for_allgather(1, sendbuf, recvbuf, self.ob_mpi)
        with nogil: CHKERR( MPI_Allgatherv(
            m.sbuf, m.scount,             m.stype,
            m.rbuf, m.rcounts, m.rdispls, m.rtype,
            self.ob_mpi) )

    def Alltoall(
        self,
        sendbuf: Union[BufSpecB, InPlace],
        recvbuf: BufSpecB,
    ) -> None:
        """
        All to All Scatter/Gather, send data from all to all
        processes in a group
        """
        cdef _p_msg_cco m = message_cco()
        m.for_alltoall(0, sendbuf, recvbuf, self.ob_mpi)
        with nogil: CHKERR( MPI_Alltoall(
            m.sbuf, m.scount, m.stype,
            m.rbuf, m.rcount, m.rtype,
            self.ob_mpi) )

    def Alltoallv(
        self,
        sendbuf: Union[BufSpecV, InPlace],
        recvbuf: BufSpecV,
    ) -> None:
        """
        All to All Scatter/Gather Vector, send data from all to all
        processes in a group providing different amount of data and
        displacements
        """
        cdef _p_msg_cco m = message_cco()
        m.for_alltoall(1, sendbuf, recvbuf, self.ob_mpi)
        with nogil: CHKERR( MPI_Alltoallv(
            m.sbuf, m.scounts, m.sdispls, m.stype,
            m.rbuf, m.rcounts, m.rdispls, m.rtype,
            self.ob_mpi) )

    def Alltoallw(
        self,
        sendbuf: Union[BufSpecW, InPlace],
        recvbuf: BufSpecW,
    ) -> None:
        """
        Generalized All-to-All communication allowing different
        counts, displacements and datatypes for each partner
        """
        cdef _p_msg_ccow m = message_ccow()
        m.for_alltoallw(sendbuf, recvbuf, self.ob_mpi)
        with nogil: CHKERR( MPI_Alltoallw(
            m.sbuf, m.scounts, m.sdispls, m.stypes,
            m.rbuf, m.rcounts, m.rdispls, m.rtypes,
            self.ob_mpi) )


    # Global Reduction Operations
    # ---------------------------

    def Reduce(
        self,
        sendbuf: Union[BufSpec, InPlace],
        recvbuf: Optional[BufSpec],
        Op op: Op = SUM,
        int root: int = 0,
    ) -> None:
        """
        Reduce to Root
        """
        cdef _p_msg_cco m = message_cco()
        m.for_reduce(sendbuf, recvbuf, root, self.ob_mpi)
        with nogil: CHKERR( MPI_Reduce(
            m.sbuf, m.rbuf, m.rcount, m.rtype,
            op.ob_mpi, root, self.ob_mpi) )

    def Allreduce(
        self,
        sendbuf: Union[BufSpec, InPlace],
        recvbuf: BufSpec,
        Op op: Op = SUM,
    ) -> None:
        """
        Reduce to All
        """
        cdef _p_msg_cco m = message_cco()
        m.for_allreduce(sendbuf, recvbuf, self.ob_mpi)
        with nogil: CHKERR( MPI_Allreduce(
            m.sbuf, m.rbuf, m.rcount, m.rtype,
            op.ob_mpi, self.ob_mpi) )

    def Reduce_scatter_block(
        self,
        sendbuf: Union[BufSpecB, InPlace],
        recvbuf: Union[BufSpec, BufSpecB],
        Op op: Op = SUM,
    ) -> None:
        """
        Reduce-Scatter Block (regular, non-vector version)
        """
        cdef _p_msg_cco m = message_cco()
        m.for_reduce_scatter_block(sendbuf, recvbuf, self.ob_mpi)
        with nogil: CHKERR( MPI_Reduce_scatter_block(
            m.sbuf, m.rbuf, m.rcount, m.rtype,
            op.ob_mpi, self.ob_mpi) )

    def Reduce_scatter(
        self,
        sendbuf: Union[BufSpec, InPlace],
        recvbuf: BufSpec,
        recvcounts: Optional[Sequence[int]] = None,
        Op op: Op = SUM,
    ) -> None:
        """
        Reduce-Scatter (vector version)
        """
        cdef _p_msg_cco m = message_cco()
        m.for_reduce_scatter(sendbuf, recvbuf,
                             recvcounts, self.ob_mpi)
        with nogil: CHKERR( MPI_Reduce_scatter(
            m.sbuf, m.rbuf, m.rcounts, m.rtype,
            op.ob_mpi, self.ob_mpi) )

    # Nonblocking Collectives
    # -----------------------

    def Ibarrier(self) -> Request:
        """
        Nonblocking Barrier
        """
        cdef Request request = Request.__new__(Request)
        with nogil: CHKERR( MPI_Ibarrier(self.ob_mpi, &request.ob_mpi) )
        return request

    def Ibcast(
        self,
        buf: BufSpec,
        int root: int = 0,
    ) -> Request:
        """
        Nonblocking Broadcast
        """
        cdef _p_msg_cco m = message_cco()
        m.for_bcast(buf, root, self.ob_mpi)
        cdef Request request = Request.__new__(Request)
        with nogil: CHKERR( MPI_Ibcast(
            m.sbuf, m.scount, m.stype,
            root, self.ob_mpi, &request.ob_mpi) )
        request.ob_buf = m
        return request

    def Igather(
        self,
        sendbuf: Union[BufSpec, InPlace],
        recvbuf: Optional[BufSpecB],
        int root: int = 0,
    ) -> Request:
        """
        Nonblocking Gather
        """
        cdef _p_msg_cco m = message_cco()
        m.for_gather(0, sendbuf, recvbuf, root, self.ob_mpi)
        cdef Request request = Request.__new__(Request)
        with nogil: CHKERR( MPI_Igather(
            m.sbuf, m.scount, m.stype,
            m.rbuf, m.rcount, m.rtype,
            root, self.ob_mpi, &request.ob_mpi) )
        request.ob_buf = m
        return request

    def Igatherv(
        self,
        sendbuf: Union[BufSpec, InPlace],
        recvbuf: Optional[BufSpecV],
        int root: int = 0,
    ) -> Request:
        """
        Nonblocking Gather Vector
        """
        cdef _p_msg_cco m = message_cco()
        m.for_gather(1, sendbuf, recvbuf, root, self.ob_mpi)
        cdef Request request = Request.__new__(Request)
        with nogil: CHKERR( MPI_Igatherv(
            m.sbuf, m.scount,             m.stype,
            m.rbuf, m.rcounts, m.rdispls, m.rtype,
            root, self.ob_mpi, &request.ob_mpi) )
        request.ob_buf = m
        return request

    def Iscatter(
        self,
        sendbuf: Optional[BufSpecB],
        recvbuf: Union[BufSpec, InPlace],
        int root: int = 0,
    ) -> Request:
        """
        Nonblocking Scatter
        """
        cdef _p_msg_cco m = message_cco()
        m.for_scatter(0, sendbuf, recvbuf, root, self.ob_mpi)
        cdef Request request = Request.__new__(Request)
        with nogil: CHKERR( MPI_Iscatter(
            m.sbuf, m.scount, m.stype,
            m.rbuf, m.rcount, m.rtype,
            root, self.ob_mpi, &request.ob_mpi) )
        request.ob_buf = m
        return request

    def Iscatterv(
        self,
        sendbuf: Optional[BufSpecV],
        recvbuf: Union[BufSpec, InPlace],
        int root: int = 0,
    ) -> Request:
        """
        Nonblocking Scatter Vector
        """
        cdef _p_msg_cco m = message_cco()
        m.for_scatter(1, sendbuf, recvbuf, root, self.ob_mpi)
        cdef Request request = Request.__new__(Request)
        with nogil: CHKERR( MPI_Iscatterv(
            m.sbuf, m.scounts, m.sdispls, m.stype,
            m.rbuf, m.rcount,             m.rtype,
            root, self.ob_mpi, &request.ob_mpi) )
        request.ob_buf = m
        return request

    def Iallgather(
        self,
        sendbuf: Union[BufSpec, InPlace],
        recvbuf: BufSpecB,
    ) -> Request:
        """
        Nonblocking Gather to All
        """
        cdef _p_msg_cco m = message_cco()
        m.for_allgather(0, sendbuf, recvbuf, self.ob_mpi)
        cdef Request request = Request.__new__(Request)
        with nogil: CHKERR( MPI_Iallgather(
            m.sbuf, m.scount, m.stype,
            m.rbuf, m.rcount, m.rtype,
            self.ob_mpi, &request.ob_mpi) )
        request.ob_buf = m
        return request

    def Iallgatherv(
        self,
        sendbuf: Union[BufSpec, InPlace],
        recvbuf: BufSpecV,
    ) -> Request:
        """
        Nonblocking Gather to All Vector
        """
        cdef _p_msg_cco m = message_cco()
        m.for_allgather(1, sendbuf, recvbuf, self.ob_mpi)
        cdef Request request = Request.__new__(Request)
        with nogil: CHKERR( MPI_Iallgatherv(
            m.sbuf, m.scount,             m.stype,
            m.rbuf, m.rcounts, m.rdispls, m.rtype,
            self.ob_mpi, &request.ob_mpi) )
        return request

    def Ialltoall(
        self,
        sendbuf: Union[BufSpecB, InPlace],
        recvbuf: BufSpecB,
    ) -> Request:
        """
        Nonblocking All to All Scatter/Gather
        """
        cdef _p_msg_cco m = message_cco()
        m.for_alltoall(0, sendbuf, recvbuf, self.ob_mpi)
        cdef Request request = Request.__new__(Request)
        with nogil: CHKERR( MPI_Ialltoall(
            m.sbuf, m.scount, m.stype,
            m.rbuf, m.rcount, m.rtype,
            self.ob_mpi, &request.ob_mpi) )
        request.ob_buf = m
        return request

    def Ialltoallv(
        self,
        sendbuf: Union[BufSpecV, InPlace],
        recvbuf: BufSpecV,
    ) -> Request:
        """
        Nonblocking All to All Scatter/Gather Vector
        """
        cdef _p_msg_cco m = message_cco()
        m.for_alltoall(1, sendbuf, recvbuf, self.ob_mpi)
        cdef Request request = Request.__new__(Request)
        with nogil: CHKERR( MPI_Ialltoallv(
            m.sbuf, m.scounts, m.sdispls, m.stype,
            m.rbuf, m.rcounts, m.rdispls, m.rtype,
            self.ob_mpi, &request.ob_mpi) )
        request.ob_buf = m
        return request

    def Ialltoallw(
        self,
        sendbuf: Union[BufSpecW, InPlace],
        recvbuf: BufSpecW,
    ) -> Request:
        """
        Nonblocking Generalized All-to-All
        """
        cdef _p_msg_ccow m = message_ccow()
        m.for_alltoallw(sendbuf, recvbuf, self.ob_mpi)
        cdef Request request = Request.__new__(Request)
        with nogil: CHKERR( MPI_Ialltoallw(
            m.sbuf, m.scounts, m.sdispls, m.stypes,
            m.rbuf, m.rcounts, m.rdispls, m.rtypes,
            self.ob_mpi, &request.ob_mpi) )
        request.ob_buf = m
        return request

    def Ireduce(
        self,
        sendbuf: Union[BufSpec, InPlace],
        recvbuf: Optional[BufSpec],
        Op op: Op = SUM,
        int root: int = 0,
    ) -> Request:
        """
        Nonblocking Reduce to Root
        """
        cdef _p_msg_cco m = message_cco()
        m.for_reduce(sendbuf, recvbuf, root, self.ob_mpi)
        cdef Request request = Request.__new__(Request)
        with nogil: CHKERR( MPI_Ireduce(
            m.sbuf, m.rbuf, m.rcount, m.rtype,
            op.ob_mpi, root, self.ob_mpi, &request.ob_mpi) )
        return request

    def Iallreduce(
        self,
        sendbuf: Union[BufSpec, InPlace],
        recvbuf: BufSpec,
        Op op: Op = SUM,
    ) -> Request:
        """
        Nonblocking Reduce to All
        """
        cdef _p_msg_cco m = message_cco()
        m.for_allreduce(sendbuf, recvbuf, self.ob_mpi)
        cdef Request request = Request.__new__(Request)
        with nogil: CHKERR( MPI_Iallreduce(
            m.sbuf, m.rbuf, m.rcount, m.rtype,
            op.ob_mpi, self.ob_mpi, &request.ob_mpi) )
        return request

    def Ireduce_scatter_block(
        self,
        sendbuf: Union[BufSpecB, InPlace],
        recvbuf: Union[BufSpec, BufSpecB],
        Op op: Op = SUM,
    ) -> Request:
        """
        Nonblocking Reduce-Scatter Block (regular, non-vector version)
        """
        cdef _p_msg_cco m = message_cco()
        m.for_reduce_scatter_block(sendbuf, recvbuf, self.ob_mpi)
        cdef Request request = Request.__new__(Request)
        with nogil: CHKERR( MPI_Ireduce_scatter_block(
            m.sbuf, m.rbuf, m.rcount, m.rtype,
            op.ob_mpi, self.ob_mpi, &request.ob_mpi) )
        return request

    def Ireduce_scatter(
        self,
        sendbuf: Union[BufSpec, InPlace],
        recvbuf: BufSpec,
        recvcounts: Optional[Sequence[int]] = None,
        Op op: Op = SUM,
    ) -> Request:
        """
        Nonblocking Reduce-Scatter (vector version)
        """
        cdef _p_msg_cco m = message_cco()
        m.for_reduce_scatter(sendbuf, recvbuf,
                             recvcounts, self.ob_mpi)
        cdef Request request = Request.__new__(Request)
        with nogil: CHKERR( MPI_Ireduce_scatter(
            m.sbuf, m.rbuf, m.rcounts, m.rtype,
            op.ob_mpi, self.ob_mpi, &request.ob_mpi) )
        return request

    # Tests
    # -----

    def Is_inter(self) -> bool:
        """
        Test to see if a comm is an intercommunicator
        """
        cdef int flag = 0
        CHKERR( MPI_Comm_test_inter(self.ob_mpi, &flag) )
        return <bint>flag

    property is_inter:
        """is intercommunicator"""
        def __get__(self) -> bool:
            return self.Is_inter()

    def Is_intra(self) -> bool:
        """
        Test to see if a comm is an intracommunicator
        """
        return not self.Is_inter()

    property is_intra:
        """is intracommunicator"""
        def __get__(self) -> bool:
            return self.Is_intra()

    def Get_topology(self) -> int:
        """
        Determine the type of topology (if any)
        associated with a communicator
        """
        cdef int topo = MPI_UNDEFINED
        CHKERR( MPI_Topo_test(self.ob_mpi, &topo) )
        return topo

    property topology:
        """communicator topology type"""
        def __get__(self) -> int:
            return self.Get_topology()

    property is_topo:
        """is a topology communicator"""
        def __get__(self) -> bool:
            return self.Get_topology() != MPI_UNDEFINED

    # Process Creation and Management
    # -------------------------------

    @classmethod
    def Get_parent(cls) -> Intercomm:
        """
        Return the parent intercommunicator for this process
        """
        cdef Intercomm comm = __COMM_PARENT__
        with nogil: CHKERR( MPI_Comm_get_parent(&comm.ob_mpi) )
        comm_set_eh(comm.ob_mpi)
        return comm

    def Disconnect(self) -> None:
        """
        Disconnect from a communicator
        """
        with nogil: CHKERR( MPI_Comm_disconnect(&self.ob_mpi) )

    @classmethod
    def Join(cls, int fd: int) -> Intercomm:
        """
        Create a intercommunicator by joining
        two processes connected by a socket
        """
        cdef Intercomm comm = Intercomm.__new__(Intercomm)
        with nogil: CHKERR( MPI_Comm_join(fd, &comm.ob_mpi) )
        comm_set_eh(comm.ob_mpi)
        return comm

    # Attributes
    # ----------

    def Get_attr(self, int keyval: int) -> Optional[Union[int, Any]]:
        """
        Retrieve attribute value by key
        """
        cdef void *attrval = NULL
        cdef int  flag = 0
        CHKERR( MPI_Comm_get_attr(self.ob_mpi, keyval, &attrval, &flag) )
        if not flag: return None
        if attrval == NULL: return 0
        # MPI-1 predefined attribute keyvals
        if (keyval == MPI_TAG_UB or
            keyval == MPI_HOST or
            keyval == MPI_IO or
            keyval == MPI_WTIME_IS_GLOBAL):
            return (<int*>attrval)[0]
        # MPI-2 predefined attribute keyvals
        elif (keyval == MPI_UNIVERSE_SIZE or
              keyval == MPI_APPNUM or
              keyval == MPI_LASTUSEDCODE):
            return (<int*>attrval)[0]
        # user-defined attribute keyval
        return PyMPI_attr_get(self.ob_mpi, keyval, attrval)

    def Set_attr(self, int keyval: int, attrval: Any) -> None:
        """
        Store attribute value associated with a key
        """
        PyMPI_attr_set(self.ob_mpi, keyval, attrval)

    def Delete_attr(self, int keyval: int) -> None:
        """
        Delete attribute value associated with a key
        """
        CHKERR( MPI_Comm_delete_attr(self.ob_mpi, keyval) )

    @classmethod
    def Create_keyval(
        cls,
        copy_fn: Optional[Callable[[Comm, int, Any], Any]] = None,
        delete_fn: Optional[Callable[[Comm, int, Any], None]] = None,
        nopython: bool = False,
    ) -> int:
        """
        Create a new attribute key for communicators
        """
        cdef object state = _p_keyval(copy_fn, delete_fn, nopython)
        cdef int keyval = MPI_KEYVAL_INVALID
        cdef MPI_Comm_copy_attr_function *_copy = PyMPI_attr_copy_fn
        cdef MPI_Comm_delete_attr_function *_del = PyMPI_attr_delete_fn
        cdef void *extra_state = <void *>state
        CHKERR( MPI_Comm_create_keyval(_copy, _del, &keyval, extra_state) )
        comm_keyval[keyval] = state
        return keyval

    @classmethod
    def Free_keyval(cls, int keyval: int) -> int:
        """
        Free an attribute key for communicators
        """
        cdef int keyval_save = keyval
        CHKERR( MPI_Comm_free_keyval(&keyval) )
        try: del comm_keyval[keyval_save]
        except KeyError: pass
        return keyval

    # Error handling
    # --------------

    def Get_errhandler(self) -> Errhandler:
        """
        Get the error handler for a communicator
        """
        cdef Errhandler errhandler = Errhandler.__new__(Errhandler)
        CHKERR( MPI_Comm_get_errhandler(self.ob_mpi, &errhandler.ob_mpi) )
        return errhandler

    def Set_errhandler(self, Errhandler errhandler: Errhandler) -> None:
        """
        Set the error handler for a communicator
        """
        CHKERR( MPI_Comm_set_errhandler(self.ob_mpi, errhandler.ob_mpi) )

    def Call_errhandler(self, int errorcode: int) -> None:
        """
        Call the error handler installed on a communicator
        """
        CHKERR( MPI_Comm_call_errhandler(self.ob_mpi, errorcode) )


    def Abort(self, int errorcode: int = 0) -> NoReturn:
        """
        Terminate MPI execution environment

        .. warning:: This is a direct call, use it with care!!!.
        """
        CHKERR( MPI_Abort(self.ob_mpi, errorcode) )

    # Naming Objects
    # --------------

    def Get_name(self) -> str:
        """
        Get the print name for this communicator
        """
        cdef char name[MPI_MAX_OBJECT_NAME+1]
        cdef int nlen = 0
        CHKERR( MPI_Comm_get_name(self.ob_mpi, name, &nlen) )
        return tompistr(name, nlen)

    def Set_name(self, name: str) -> None:
        """
        Set the print name for this communicator
        """
        cdef char *cname = NULL
        name = asmpistr(name, &cname)
        CHKERR( MPI_Comm_set_name(self.ob_mpi, cname) )

    property name:
        """communicator name"""
        def __get__(self) -> str:
            return self.Get_name()
        def __set__(self, value: str):
            self.Set_name(value)

    # Fortran Handle
    # --------------

    def py2f(self) -> int:
        """
        """
        return MPI_Comm_c2f(self.ob_mpi)

    @classmethod
    def f2py(cls, arg: int) -> Comm:
        """
        """
        cdef MPI_Comm comm = MPI_Comm_f2c(arg)
        return PyMPIComm_New(comm)

    # Python Communication
    # --------------------
    #
    def send(
        self,
        obj: Any,
        int dest: int,
        int tag: int = 0,
    ) -> None:
        """Send"""
        cdef MPI_Comm comm = self.ob_mpi
        return PyMPI_send(obj, dest, tag, comm)
    #
    def bsend(
        self,
        obj: Any,
        int dest: int,
        int tag: int = 0,
    ) -> None:
        """Send in buffered mode"""
        cdef MPI_Comm comm = self.ob_mpi
        return PyMPI_bsend(obj, dest, tag, comm)
    #
    def ssend(
        self,
        obj: Any,
        int dest: int,
        int tag: int = 0,
    ) -> None:
        """Send in synchronous mode"""
        cdef MPI_Comm comm = self.ob_mpi
        return PyMPI_ssend(obj, dest, tag, comm)
    #
    def recv(
        self,
        buf: Optional[Buffer] = None,
        int source: int = ANY_SOURCE,
        int tag: int = ANY_TAG,
        Status status: Optional[Status] = None,
    ) -> Any:
        """Receive"""
        cdef MPI_Comm comm = self.ob_mpi
        cdef MPI_Status *statusp = arg_Status(status)
        return PyMPI_recv(buf, source, tag, comm, statusp)
    #
    def sendrecv(
        self,
        sendobj: Any,
        int dest: int,
        int sendtag: int = 0,
        recvbuf: Optional[Buffer] = None,
        int source: int = ANY_SOURCE,
        int recvtag: int = ANY_TAG,
        Status status: Optional[Status] = None,
    ) -> Any:
        """Send and Receive"""
        cdef MPI_Comm comm = self.ob_mpi
        cdef MPI_Status *statusp = arg_Status(status)
        return PyMPI_sendrecv(sendobj, dest,   sendtag,
                              recvbuf, source, recvtag,
                              comm, statusp)
    #
    def isend(
        self,
        obj: Any,
        int dest: int,
        int tag: int = 0,
    ) -> Request:
        """Nonblocking send"""
        cdef MPI_Comm comm = self.ob_mpi
        cdef Request request = Request.__new__(Request)
        request.ob_buf = PyMPI_isend(obj, dest, tag, comm, &request.ob_mpi)
        return request
    #
    def ibsend(
        self,
        obj: Any,
        int dest: int,
        int tag: int = 0,
    ) -> Request:
        """Nonblocking send in buffered mode"""
        cdef MPI_Comm comm = self.ob_mpi
        cdef Request request = Request.__new__(Request)
        request.ob_buf = PyMPI_ibsend(obj, dest, tag, comm, &request.ob_mpi)
        return request
    #
    def issend(
        self,
        obj: Any,
        int dest: int,
        int tag: int = 0,
    ) -> Request:
        """Nonblocking send in synchronous mode"""
        cdef MPI_Comm comm = self.ob_mpi
        cdef Request request = Request.__new__(Request)
        request.ob_buf = PyMPI_issend(obj, dest, tag, comm, &request.ob_mpi)
        return request
    #
    def irecv(
        self,
        buf: Optional[Buffer] = None,
        int source: int = ANY_SOURCE,
        int tag: int = ANY_TAG,
    ) -> Request:
        """Nonblocking receive"""
        cdef MPI_Comm comm = self.ob_mpi
        cdef Request request = Request.__new__(Request)
        request.ob_buf = PyMPI_irecv(buf, source, tag, comm, &request.ob_mpi)
        return request
    #
    def probe(
        self,
        int source: int = ANY_SOURCE,
        int tag: int = ANY_TAG,
        Status status: Optional[Status] = None,
    ) -> Literal[True]:
        """Blocking test for a message"""
        cdef MPI_Comm comm = self.ob_mpi
        cdef MPI_Status *statusp = arg_Status(status)
        return PyMPI_probe(source, tag, comm, statusp)
    #
    def iprobe(
        self,
        int source: int = ANY_SOURCE,
        int tag: int = ANY_TAG,
        Status status: Optional[Status] = None,
    ) -> bool:
        """Nonblocking test for a message"""
        cdef MPI_Comm comm = self.ob_mpi
        cdef MPI_Status *statusp = arg_Status(status)
        return PyMPI_iprobe(source, tag, comm, statusp)
    #
    def mprobe(
        self,
        int source: int = ANY_SOURCE,
        int tag: int = ANY_TAG,
        Status status: Optional[Status] = None,
    ) -> Message:
        """Blocking test for a matched message"""
        cdef MPI_Comm comm = self.ob_mpi
        cdef MPI_Status *statusp = arg_Status(status)
        cdef Message message = Message.__new__(Message)
        message.ob_buf = PyMPI_mprobe(source, tag, comm,
                                      &message.ob_mpi, statusp)
        return message
    #
    def improbe(
        self,
        int source: int = ANY_SOURCE,
        int tag: int = ANY_TAG,
        Status status: Optional[Status] = None,
    ) -> Optional[Message]:
        """Nonblocking test for a matched message"""
        cdef int flag = 0
        cdef MPI_Comm comm = self.ob_mpi
        cdef MPI_Status *statusp = arg_Status(status)
        cdef Message message = Message.__new__(Message)
        message.ob_buf = PyMPI_improbe(source, tag, comm, &flag,
                                       &message.ob_mpi, statusp)
        if flag == 0: return None
        return message
    #
    def barrier(self) -> None:
        """Barrier"""
        cdef MPI_Comm comm = self.ob_mpi
        return PyMPI_barrier(comm)
    #
    def bcast(
        self,
        obj: Any,
        int root: int = 0,
    ) -> Any:
        """Broadcast"""
        cdef MPI_Comm comm = self.ob_mpi
        return PyMPI_bcast(obj, root, comm)
    #
    def gather(
        self,
        sendobj: Any,
        int root: int = 0,
    ) -> Optional[List[Any]]:
        """Gather"""
        cdef MPI_Comm comm = self.ob_mpi
        return PyMPI_gather(sendobj, root, comm)
    #
    def scatter(
        self,
        sendobj: Sequence[Any],
        int root: int = 0,
    ) -> Any:
        """Scatter"""
        cdef MPI_Comm comm = self.ob_mpi
        return PyMPI_scatter(sendobj, root, comm)
    #
    def allgather(
        self,
        sendobj: Any,
    ) -> List[Any]:
        """Gather to All"""
        cdef MPI_Comm comm = self.ob_mpi
        return PyMPI_allgather(sendobj, comm)
    #
    def alltoall(
        self,
        sendobj: Sequence[Any],
    ) -> List[Any]:
        """All to All Scatter/Gather"""
        cdef MPI_Comm comm = self.ob_mpi
        return PyMPI_alltoall(sendobj, comm)
    #
    def reduce(
        self,
        sendobj: Any,
        op: Union[Op, Callable[[Any, Any], Any]] = SUM,
        int root: int = 0,
    ) -> Optional[Any]:
        """Reduce to Root"""
        cdef MPI_Comm comm = self.ob_mpi
        return PyMPI_reduce(sendobj, op, root, comm)
    #
    def allreduce(
        self,
        sendobj: Any,
        op: Union[Op, Callable[[Any, Any], Any]] = SUM,
    ) -> Any:
        """Reduce to All"""
        cdef MPI_Comm comm = self.ob_mpi
        return PyMPI_allreduce(sendobj, op, comm)


cdef class Intracomm(Comm):

    """
    Intracommunicator
    """

    def __cinit__(self, Comm comm: Optional[Comm] = None):
        if self.ob_mpi == MPI_COMM_NULL: return
        cdef int inter = 1
        CHKERR( MPI_Comm_test_inter(self.ob_mpi, &inter) )
        if inter: raise TypeError(
            "expecting an intracommunicator")

    # Communicator Constructors
    # -------------------------

    def Create_cart(
        self,
        dims: Sequence[int],
        periods: Optional[Sequence[bool]] = None,
        bint reorder: bool = False,
    ) -> Cartcomm:
        """
        Create cartesian communicator
        """
        cdef int ndims = 0, *idims = NULL, *iperiods = NULL
        dims = getarray(dims, &ndims, &idims)
        if periods is None: periods = False
        if isinstance(periods, bool): periods = [periods] * ndims
        periods = chkarray(periods, ndims, &iperiods)
        #
        cdef Cartcomm comm = Cartcomm.__new__(Cartcomm)
        with nogil: CHKERR( MPI_Cart_create(
            self.ob_mpi, ndims, idims, iperiods, reorder, &comm.ob_mpi) )
        comm_set_eh(comm.ob_mpi)
        return comm

    def Create_graph(
        self,
        index: Sequence[int],
        edges: Sequence[int],
        bint reorder: bool = False,
    ) -> Graphcomm:
        """
        Create graph communicator
        """
        cdef int nnodes = 0, *iindex = NULL
        index = getarray(index, &nnodes, &iindex)
        cdef int nedges = 0, *iedges = NULL
        edges = getarray(edges, &nedges, &iedges)
        # extension: 'standard' adjacency arrays
        if iindex[0]==0 and iindex[nnodes-1]==nedges:
            nnodes -= 1; iindex += 1;
        #
        cdef Graphcomm comm = Graphcomm.__new__(Graphcomm)
        with nogil: CHKERR( MPI_Graph_create(
            self.ob_mpi, nnodes, iindex, iedges, reorder, &comm.ob_mpi) )
        comm_set_eh(comm.ob_mpi)
        return comm

    def Create_dist_graph_adjacent(
        self,
        sources: Sequence[int],
        destinations: Sequence[int],
        sourceweights: Optional[Sequence[int]] = None,
        destweights: Optional[Sequence[int]] = None,
        Info info: Info = INFO_NULL,
        bint reorder: bool = False,
    ) -> Distgraphcomm:
        """
        Create distributed graph communicator
        """
        cdef int indegree  = 0, *isource = NULL
        cdef int outdegree = 0, *idest   = NULL
        cdef int *isourceweight = MPI_UNWEIGHTED
        cdef int *idestweight   = MPI_UNWEIGHTED
        if sources is not None:
            sources = getarray(sources, &indegree, &isource)
        sourceweights = asarray_weights(
            sourceweights, indegree, &isourceweight)
        if destinations is not None:
            destinations = getarray(destinations, &outdegree, &idest)
        destweights = asarray_weights(
            destweights, outdegree, &idestweight)
        #
        cdef Distgraphcomm comm = Distgraphcomm.__new__(Distgraphcomm)
        with nogil: CHKERR( MPI_Dist_graph_create_adjacent(
            self.ob_mpi,
            indegree,  isource, isourceweight,
            outdegree, idest,   idestweight,
            info.ob_mpi, reorder, &comm.ob_mpi) )
        comm_set_eh(comm.ob_mpi)
        return comm

    def Create_dist_graph(
        self,
        sources: Sequence[int],
        degrees: Sequence[int],
        destinations: Sequence[int],
        weights: Optional[Sequence[int]] = None,
        Info info: Info = INFO_NULL,
        bint reorder: bool = False,
    ) -> Distgraphcomm:
        """
        Create distributed graph communicator
        """
        cdef int nv = 0, ne = 0, i = 0
        cdef int *isource = NULL, *idegree = NULL,
        cdef int *idest = NULL, *iweight = MPI_UNWEIGHTED
        sources = getarray(sources, &nv, &isource)
        degrees = chkarray(degrees,  nv, &idegree)
        for i from 0 <= i < nv: ne += idegree[i]
        destinations = chkarray(destinations, ne, &idest)
        weights = asarray_weights(weights, ne, &iweight)
        #
        cdef Distgraphcomm comm = Distgraphcomm.__new__(Distgraphcomm)
        with nogil: CHKERR( MPI_Dist_graph_create(
            self.ob_mpi,
            nv, isource, idegree, idest, iweight,
            info.ob_mpi, reorder, &comm.ob_mpi) )
        comm_set_eh(comm.ob_mpi)
        return comm

    def Create_intercomm(
        self,
        int local_leader: int,
        Intracomm peer_comm: Intracomm,
        int remote_leader: int,
        int tag: int = 0,
    ) -> Intercomm:
        """
        Create intercommunicator
        """
        cdef Intercomm comm = Intercomm.__new__(Intercomm)
        with nogil: CHKERR( MPI_Intercomm_create(
            self.ob_mpi, local_leader,
            peer_comm.ob_mpi, remote_leader,
            tag, &comm.ob_mpi) )
        comm_set_eh(comm.ob_mpi)
        return comm

    # Low-Level Topology Functions
    # ----------------------------

    def Cart_map(
        self,
        dims: Sequence[int],
        periods: Optional[Sequence[bool]] = None,
    ) -> int:
        """
        Return an optimal placement for the
        calling process on the physical machine
        """
        cdef int ndims = 0, *idims = NULL, *iperiods = NULL
        dims = getarray(dims, &ndims, &idims)
        if periods is None: periods = False
        if isinstance(periods, bool): periods = [periods] * ndims
        periods = chkarray(periods, ndims, &iperiods)
        cdef int rank = MPI_PROC_NULL
        CHKERR( MPI_Cart_map(self.ob_mpi, ndims, idims, iperiods, &rank) )
        return rank

    def Graph_map(
        self,
        index: Sequence[int],
        edges: Sequence[int],
    ) -> int:
        """
        Return an optimal placement for the
        calling process on the physical machine
        """
        cdef int nnodes = 0, *iindex = NULL
        index = getarray(index, &nnodes, &iindex)
        cdef int nedges = 0, *iedges = NULL
        edges = getarray(edges, &nedges, &iedges)
        # extension: accept more 'standard' adjacency arrays
        if iindex[0]==0 and iindex[nnodes-1]==nedges:
            nnodes -= 1; iindex += 1;
        cdef int rank = MPI_PROC_NULL
        CHKERR( MPI_Graph_map(self.ob_mpi, nnodes, iindex, iedges, &rank) )
        return rank

    # Global Reduction Operations
    # ---------------------------

    # Inclusive Scan

    def Scan(
        self,
        sendbuf: Union[BufSpec, InPlace],
        recvbuf: BufSpec,
        Op op: Op = SUM,
    ) -> None:
        """
        Inclusive Scan
        """
        cdef _p_msg_cco m = message_cco()
        m.for_scan(sendbuf, recvbuf, self.ob_mpi)
        with nogil: CHKERR( MPI_Scan(
            m.sbuf, m.rbuf, m.rcount, m.rtype,
            op.ob_mpi, self.ob_mpi) )

    # Exclusive Scan

    def Exscan(
        self,
        sendbuf: Union[BufSpec, InPlace],
        recvbuf: BufSpec,
        Op op: Op = SUM,
    ) -> None:
        """
        Exclusive Scan
        """
        cdef _p_msg_cco m = message_cco()
        m.for_exscan(sendbuf, recvbuf, self.ob_mpi)
        with nogil: CHKERR( MPI_Exscan(
            m.sbuf, m.rbuf, m.rcount, m.rtype,
            op.ob_mpi, self.ob_mpi) )

    # Nonblocking

    def Iscan(
        self,
        sendbuf: Union[BufSpec, InPlace],
        recvbuf: BufSpec,
        Op op: Op = SUM,
    ) -> Request:
        """
        Inclusive Scan
        """
        cdef _p_msg_cco m = message_cco()
        m.for_scan(sendbuf, recvbuf, self.ob_mpi)
        cdef Request request = Request.__new__(Request)
        with nogil: CHKERR( MPI_Iscan(
            m.sbuf, m.rbuf, m.rcount, m.rtype,
            op.ob_mpi, self.ob_mpi, &request.ob_mpi) )
        return request

    def Iexscan(
        self,
        sendbuf: Union[BufSpec, InPlace],
        recvbuf: BufSpec,
        Op op: Op = SUM,
    ) -> Request:
        """
        Inclusive Scan
        """
        cdef _p_msg_cco m = message_cco()
        m.for_exscan(sendbuf, recvbuf, self.ob_mpi)
        cdef Request request = Request.__new__(Request)
        with nogil: CHKERR( MPI_Iexscan(
            m.sbuf, m.rbuf, m.rcount, m.rtype,
            op.ob_mpi, self.ob_mpi, &request.ob_mpi) )
        return request

    # Python Communication
    #
    def scan(
        self,
        sendobj: Any,
        op: Union[Op, Callable[[Any, Any], Any]] = SUM,
    ) -> Any:
        """Inclusive Scan"""
        cdef MPI_Comm comm = self.ob_mpi
        return PyMPI_scan(sendobj, op, comm)
    #
    def exscan(
        self,
        sendobj: Any,
        op: Union[Op, Callable[[Any, Any], Any]] = SUM,
    ) -> Any:
        """Exclusive Scan"""
        cdef MPI_Comm comm = self.ob_mpi
        return PyMPI_exscan(sendobj, op, comm)

    # Establishing Communication
    # --------------------------

    # Starting Processes

    def Spawn(
        self,
        command: str,
        args: Optional[Sequence[str]] = None,
        int maxprocs: int = 1,
        Info info: Info = INFO_NULL,
        int root: int = 0,
        errcodes: Optional[list] = None,
    ) -> Intercomm:
        """
        Spawn instances of a single MPI application
        """
        cdef char *cmd = NULL
        cdef char **argv = MPI_ARGV_NULL
        cdef int *ierrcodes = MPI_ERRCODES_IGNORE
        #
        cdef int rank = MPI_UNDEFINED
        CHKERR( MPI_Comm_rank(self.ob_mpi, &rank) )
        cdef tmp1, tmp2, tmp3
        if root == rank:
            tmp1 = asmpistr(command, &cmd)
            tmp2 = asarray_argv(args, &argv)
        if errcodes is not None:
            tmp3 = newarray(maxprocs, &ierrcodes)
        #
        cdef Intercomm comm = Intercomm.__new__(Intercomm)
        with nogil: CHKERR( MPI_Comm_spawn(
            cmd, argv, maxprocs, info.ob_mpi, root,
            self.ob_mpi, &comm.ob_mpi, ierrcodes) )
        #
        cdef int i=0
        if errcodes is not None:
            errcodes[:] = [ierrcodes[i] for i from 0 <= i < maxprocs]
        #
        comm_set_eh(comm.ob_mpi)
        return comm

    def Spawn_multiple(
        self,
        command: Sequence[str],
        args: Optional[Sequence[Sequence[str]]] = None,
        maxprocs: Optional[Sequence[int]] = None,
        info: Union[Info, Sequence[Info]] = INFO_NULL,
        int root: int = 0,
        errcodes: Optional[list] = None,
    ) -> Intercomm:
        """
        Spawn instances of multiple MPI applications
        """
        cdef int count = 0
        cdef char **cmds = NULL
        cdef char ***argvs = MPI_ARGVS_NULL
        cdef MPI_Info *infos = NULL
        cdef int *imaxprocs = NULL
        cdef int *ierrcodes = MPI_ERRCODES_IGNORE
        #
        cdef int rank = MPI_UNDEFINED
        CHKERR( MPI_Comm_rank(self.ob_mpi, &rank) )
        cdef tmp1, tmp2, tmp3, tmp4, tmp5
        if root == rank:
            tmp1 = asarray_cmds(command, &count, &cmds)
            tmp2 = asarray_argvs(args, count, &argvs)
            tmp3 = asarray_nprocs(maxprocs, count, &imaxprocs)
            tmp4 = asarray_Info(info, count, &infos)
        cdef int i=0, np=0
        if errcodes is not None:
            if root != rank:
                count = <int>len(maxprocs)
                tmp3 = asarray_nprocs(maxprocs, count, &imaxprocs)
            for i from 0 <= i < count: np += imaxprocs[i]
            tmp5 = newarray(np, &ierrcodes)
        #
        cdef Intercomm comm = Intercomm.__new__(Intercomm)
        with nogil: CHKERR( MPI_Comm_spawn_multiple(
            count, cmds, argvs, imaxprocs, infos, root,
            self.ob_mpi, &comm.ob_mpi, ierrcodes) )
        #
        cdef int j=0, p=0, q=0
        if errcodes is not None:
            errcodes[:] = [[] for j from 0 <= j < count]
            for i from 0 <= i < count:
                q = p + imaxprocs[i]
                errcodes[i][:] = [ierrcodes[j] for j from p <= j < q]
                p = q
        #
        comm_set_eh(comm.ob_mpi)
        return comm

    # Server Routines

    def Accept(
        self,
        port_name: str,
        Info info: Info = INFO_NULL,
        int root: int = 0,
    ) -> Intercomm:
        """
        Accept a request to form a new intercommunicator
        """
        cdef char *cportname = NULL
        cdef int rank = MPI_UNDEFINED
        CHKERR( MPI_Comm_rank(self.ob_mpi, &rank) )
        if root == rank:
            port_name = asmpistr(port_name, &cportname)
        cdef Intercomm comm = Intercomm.__new__(Intercomm)
        with nogil: CHKERR( MPI_Comm_accept(
            cportname, info.ob_mpi, root,
            self.ob_mpi, &comm.ob_mpi) )
        comm_set_eh(comm.ob_mpi)
        return comm

    # Client Routines

    def Connect(
        self,
        port_name: str,
        Info info: Info = INFO_NULL,
        int root: int = 0,
    ) -> Intercomm:
        """
        Make a request to form a new intercommunicator
        """
        cdef char *cportname = NULL
        cdef int rank = MPI_UNDEFINED
        CHKERR( MPI_Comm_rank(self.ob_mpi, &rank) )
        if root == rank:
            port_name = asmpistr(port_name, &cportname)
        cdef Intercomm comm = Intercomm.__new__(Intercomm)
        with nogil: CHKERR( MPI_Comm_connect(
            cportname, info.ob_mpi, root,
            self.ob_mpi, &comm.ob_mpi) )
        comm_set_eh(comm.ob_mpi)
        return comm


cdef class Topocomm(Intracomm):

    """
    Topology intracommunicator
    """

    def __cinit__(self, Comm comm: Optional[Comm] = None):
        if self.ob_mpi == MPI_COMM_NULL: return
        cdef int topo = MPI_UNDEFINED
        CHKERR( MPI_Topo_test(self.ob_mpi, &topo) )
        if topo == MPI_UNDEFINED: raise TypeError(
            "expecting a topology communicator")

    property degrees:
        "number of incoming and outgoing neighbors"
        def __get__(self) -> Tuple[int, int]:
            cdef object dim, rank
            cdef object nneighbors
            if isinstance(self, Cartcomm):
                dim = self.Get_dim()
                return (2*dim, 2*dim)
            if isinstance(self, Graphcomm):
                rank = self.Get_rank()
                nneighbors = self.Get_neighbors_count(rank)
                return (nneighbors, nneighbors)
            if isinstance(self, Distgraphcomm):
                nneighbors = self.Get_dist_neighbors_count()[:2]
                return nneighbors
            raise TypeError("Not a topology communicator")

    property indegree:
        "number of incoming neighbors"
        def __get__(self) -> int:
            return self.degrees[0]

    property outdegree:
        "number of outgoing neighbors"
        def __get__(self) -> int:
            return self.degrees[1]

    property inoutedges:
        "incoming and outgoing neighbors"
        def __get__(self) -> Tuple[List[int], List[int]]:
            cdef object direction, source, dest, rank
            cdef object neighbors
            if isinstance(self, Cartcomm):
                neighbors = []
                for direction in range(self.Get_dim()):
                    source, dest = self.Shift(direction, 1)
                    neighbors.append(source)
                    neighbors.append(dest)
                return (neighbors, neighbors)
            if isinstance(self, Graphcomm):
                rank = self.Get_rank()
                neighbors = self.Get_neighbors(rank)
                return (neighbors, neighbors)
            if isinstance(self, Distgraphcomm):
                neighbors = self.Get_dist_neighbors()[:2]
                return neighbors
            raise TypeError("Not a topology communicator")

    property inedges:
        "incoming neighbors"
        def __get__(self) -> List[int]:
            return self.inoutedges[0]

    property outedges:
        "outgoing neighbors"
        def __get__(self) -> List[int]:
            return self.inoutedges[1]

    # Neighborhood Collectives
    # ------------------------

    def Neighbor_allgather(
        self,
        sendbuf: BufSpec,
        recvbuf: BufSpecB,
    ) -> None:
        """
        Neighbor Gather to All
        """
        cdef _p_msg_cco m = message_cco()
        m.for_neighbor_allgather(0, sendbuf, recvbuf, self.ob_mpi)
        with nogil: CHKERR( MPI_Neighbor_allgather(
            m.sbuf, m.scount, m.stype,
            m.rbuf, m.rcount, m.rtype,
            self.ob_mpi) )

    def Neighbor_allgatherv(
        self,
        sendbuf: BufSpec,
        recvbuf: BufSpecV,
    ) -> None:
        """
        Neighbor Gather to All Vector
        """
        cdef _p_msg_cco m = message_cco()
        m.for_neighbor_allgather(1, sendbuf, recvbuf, self.ob_mpi)
        with nogil: CHKERR( MPI_Neighbor_allgatherv(
            m.sbuf, m.scount, m.stype,
            m.rbuf, m.rcounts, m.rdispls, m.rtype,
            self.ob_mpi) )

    def Neighbor_alltoall(
        self,
        sendbuf: BufSpecB,
        recvbuf: BufSpecB,
    ) -> None:
        """
        Neighbor All-to-All
        """
        cdef _p_msg_cco m = message_cco()
        m.for_neighbor_alltoall(0, sendbuf, recvbuf, self.ob_mpi)
        with nogil: CHKERR( MPI_Neighbor_alltoall(
            m.sbuf, m.scount, m.stype,
            m.rbuf, m.rcount, m.rtype,
            self.ob_mpi) )

    def Neighbor_alltoallv(
        self,
        sendbuf: BufSpecV,
        recvbuf: BufSpecV,
    ) -> None:
        """
        Neighbor All-to-All Vector
        """
        cdef _p_msg_cco m = message_cco()
        m.for_neighbor_alltoall(1, sendbuf, recvbuf, self.ob_mpi)
        with nogil: CHKERR( MPI_Neighbor_alltoallv(
            m.sbuf, m.scounts, m.sdispls, m.stype,
            m.rbuf, m.rcounts, m.rdispls, m.rtype,
            self.ob_mpi) )

    def Neighbor_alltoallw(
        self,
        sendbuf: BufSpecW,
        recvbuf: BufSpecW,
    ) -> None:
        """
        Neighbor All-to-All Generalized
        """
        cdef _p_msg_ccow m = message_ccow()
        m.for_neighbor_alltoallw(sendbuf, recvbuf, self.ob_mpi)
        with nogil: CHKERR( MPI_Neighbor_alltoallw(
            m.sbuf, m.scounts, m.sdisplsA, m.stypes,
            m.rbuf, m.rcounts, m.rdisplsA, m.rtypes,
            self.ob_mpi) )

    # Nonblocking Neighborhood Collectives
    # ------------------------------------

    def Ineighbor_allgather(
        self,
        sendbuf: BufSpec,
        recvbuf: BufSpecB,
    ) -> Request:
        """
        Nonblocking Neighbor Gather to All
        """
        cdef _p_msg_cco m = message_cco()
        m.for_neighbor_allgather(0, sendbuf, recvbuf, self.ob_mpi)
        cdef Request request = Request.__new__(Request)
        with nogil: CHKERR( MPI_Ineighbor_allgather(
            m.sbuf, m.scount, m.stype,
            m.rbuf, m.rcount, m.rtype,
            self.ob_mpi, &request.ob_mpi) )
        request.ob_buf = m
        return request

    def Ineighbor_allgatherv(
        self,
        sendbuf: BufSpec,
        recvbuf: BufSpecV,
    ) -> Request:
        """
        Nonblocking Neighbor Gather to All Vector
        """
        cdef _p_msg_cco m = message_cco()
        m.for_neighbor_allgather(1, sendbuf, recvbuf, self.ob_mpi)
        cdef Request request = Request.__new__(Request)
        with nogil: CHKERR( MPI_Ineighbor_allgatherv(
            m.sbuf, m.scount, m.stype,
            m.rbuf, m.rcounts, m.rdispls, m.rtype,
            self.ob_mpi, &request.ob_mpi) )
        request.ob_buf = m
        return request

    def Ineighbor_alltoall(
        self,
        sendbuf: BufSpecB,
        recvbuf: BufSpecB,
    ) -> Request:
        """
        Nonblocking Neighbor All-to-All
        """
        cdef _p_msg_cco m = message_cco()
        m.for_neighbor_alltoall(0, sendbuf, recvbuf, self.ob_mpi)
        cdef Request request = Request.__new__(Request)
        with nogil: CHKERR( MPI_Ineighbor_alltoall(
            m.sbuf, m.scount, m.stype,
            m.rbuf, m.rcount, m.rtype,
            self.ob_mpi, &request.ob_mpi) )
        request.ob_buf = m
        return request

    def Ineighbor_alltoallv(
        self,
        sendbuf: BufSpecV,
        recvbuf: BufSpecV,
    ) -> Request:
        """
        Nonblocking Neighbor All-to-All Vector
        """
        cdef _p_msg_cco m = message_cco()
        m.for_neighbor_alltoall(1, sendbuf, recvbuf, self.ob_mpi)
        cdef Request request = Request.__new__(Request)
        with nogil: CHKERR( MPI_Ineighbor_alltoallv(
            m.sbuf, m.scounts, m.sdispls, m.stype,
            m.rbuf, m.rcounts, m.rdispls, m.rtype,
            self.ob_mpi, &request.ob_mpi) )
        request.ob_buf = m
        return request

    def Ineighbor_alltoallw(
        self,
        sendbuf: BufSpecW,
        recvbuf: BufSpecW,
    ) -> Request:
        """
        Nonblocking Neighbor All-to-All Generalized
        """
        cdef _p_msg_ccow m = message_ccow()
        m.for_neighbor_alltoallw(sendbuf, recvbuf, self.ob_mpi)
        cdef Request request = Request.__new__(Request)
        with nogil: CHKERR( MPI_Ineighbor_alltoallw(
            m.sbuf, m.scounts, m.sdisplsA, m.stypes,
            m.rbuf, m.rcounts, m.rdisplsA, m.rtypes,
            self.ob_mpi, &request.ob_mpi) )
        request.ob_buf = m
        return request

    # Python Communication
    #
    def neighbor_allgather(self, sendobj: Any) -> List[Any]:
        """Neighbor Gather to All"""
        cdef MPI_Comm comm = self.ob_mpi
        return PyMPI_neighbor_allgather(sendobj, comm)
    #
    def neighbor_alltoall(self, sendobj: List[Any]) -> List[Any]:
        """Neighbor All to All Scatter/Gather"""
        cdef MPI_Comm comm = self.ob_mpi
        return PyMPI_neighbor_alltoall(sendobj, comm)


cdef class Cartcomm(Topocomm):

    """
    Cartesian topology intracommunicator
    """

    def __cinit__(self, Comm comm: Optional[Comm] = None):
        if self.ob_mpi == MPI_COMM_NULL: return
        cdef int topo = MPI_UNDEFINED
        CHKERR( MPI_Topo_test(self.ob_mpi, &topo) )
        if topo != MPI_CART: raise TypeError(
            "expecting a Cartesian communicator")

    # Cartesian Inquiry Functions
    # ---------------------------

    def Get_dim(self) -> int:
        """
        Return number of dimensions
        """
        cdef int dim = 0
        CHKERR( MPI_Cartdim_get(self.ob_mpi, &dim) )
        return dim

    property dim:
        """number of dimensions"""
        def __get__(self) -> int:
            return self.Get_dim()

    property ndim:
        """number of dimensions"""
        def __get__(self) -> int:
            return self.Get_dim()

    def Get_topo(self) -> Tuple[List[int], List[int], List[int]]:
        """
        Return information on the cartesian topology
        """
        cdef int ndim = 0
        CHKERR( MPI_Cartdim_get(self.ob_mpi, &ndim) )
        cdef int *idims = NULL
        cdef tmp1 = newarray(ndim, &idims)
        cdef int *iperiods = NULL
        cdef tmp2 = newarray(ndim, &iperiods)
        cdef int *icoords = NULL
        cdef tmp3 = newarray(ndim, &icoords)
        CHKERR( MPI_Cart_get(self.ob_mpi, ndim, idims, iperiods, icoords) )
        cdef int i = 0
        cdef object dims    = [idims[i]    for i from 0 <= i < ndim]
        cdef object periods = [iperiods[i] for i from 0 <= i < ndim]
        cdef object coords  = [icoords[i]  for i from 0 <= i < ndim]
        return (dims, periods, coords)

    property topo:
        """topology information"""
        def __get__(self) -> Tuple[List[int], List[int], List[int]]:
            return self.Get_topo()

    property dims:
        """dimensions"""
        def __get__(self) -> List[int]:
            return self.Get_topo()[0]

    property periods:
        """periodicity"""
        def __get__(self) -> List[int]:
            return self.Get_topo()[1]

    property coords:
        """coordinates"""
        def __get__(self) -> List[int]:
            return self.Get_topo()[2]


    # Cartesian Translator Functions
    # ------------------------------

    def Get_cart_rank(self, coords: Sequence[int]) -> int:
        """
        Translate logical coordinates to ranks
        """
        cdef int ndim = 0, *icoords = NULL
        CHKERR( MPI_Cartdim_get( self.ob_mpi, &ndim) )
        coords = chkarray(coords, ndim, &icoords)
        cdef int rank = MPI_PROC_NULL
        CHKERR( MPI_Cart_rank(self.ob_mpi, icoords, &rank) )
        return rank

    def Get_coords(self, int rank: int) -> List[int]:
        """
        Translate ranks to logical coordinates
        """
        cdef int i = 0, ndim = 0, *icoords = NULL
        CHKERR( MPI_Cartdim_get(self.ob_mpi, &ndim) )
        cdef tmp = newarray(ndim, &icoords)
        CHKERR( MPI_Cart_coords(self.ob_mpi, rank, ndim, icoords) )
        cdef object coords = [icoords[i] for i from 0 <= i < ndim]
        return coords

    # Cartesian Shift Function
    # ------------------------

    def Shift(self, int direction: int, int disp: int) -> Tuple[int, int]:
        """
        Return a tuple (source, dest) of process ranks
        for data shifting with Comm.Sendrecv()
        """
        cdef int source = MPI_PROC_NULL, dest = MPI_PROC_NULL
        CHKERR( MPI_Cart_shift(self.ob_mpi, direction, disp, &source, &dest) )
        return (source, dest)

    # Cartesian Partition Function
    # ----------------------------

    def Sub(self, remain_dims: Sequence[bool]) -> Cartcomm:
        """
        Return cartesian communicators
        that form lower-dimensional subgrids
        """
        cdef int ndim = 0, *iremdims = NULL
        CHKERR( MPI_Cartdim_get(self.ob_mpi, &ndim) )
        remain_dims = chkarray(remain_dims, ndim, &iremdims)
        cdef Cartcomm comm = Cartcomm.__new__(Cartcomm)
        with nogil: CHKERR( MPI_Cart_sub(self.ob_mpi, iremdims, &comm.ob_mpi) )
        comm_set_eh(comm.ob_mpi)
        return comm


# Cartesian Convenience Function

def Compute_dims(int nnodes: int, dims: Union[int, Sequence[int]]) -> List[int]:
    """
    Return a balanced distribution of
    processes per coordinate direction
    """
    cdef int i = 0, ndims = 0, *idims = NULL
    try:
        ndims = <int>len(dims)
    except:
        ndims = dims
        dims = [0] * ndims
    cdef tmp = chkarray(dims, ndims, &idims)
    CHKERR( MPI_Dims_create(nnodes, ndims, idims) )
    dims = [idims[i] for i from 0 <= i < ndims]
    return dims


cdef class Graphcomm(Topocomm):

    """
    General graph topology intracommunicator
    """

    def __cinit__(self, Comm comm: Optional[Comm] = None):
        if self.ob_mpi == MPI_COMM_NULL: return
        cdef int topo = MPI_UNDEFINED
        CHKERR( MPI_Topo_test(self.ob_mpi, &topo) )
        if topo != MPI_GRAPH: raise TypeError(
            "expecting a general graph communicator")

    # Graph Inquiry Functions
    # -----------------------

    def Get_dims(self) -> Tuple[int, int]:
        """
        Return the number of nodes and edges
        """
        cdef int nnodes = 0, nedges = 0
        CHKERR( MPI_Graphdims_get(self.ob_mpi, &nnodes, &nedges) )
        return (nnodes, nedges)

    property dims:
        """number of nodes and edges"""
        def __get__(self) -> Tuple[int, int]:
            return self.Get_dims()

    property nnodes:
        """number of nodes"""
        def __get__(self) -> int:
            return self.Get_dims()[0]

    property nedges:
        """number of edges"""
        def __get__(self) -> int:
            return self.Get_dims()[1]

    def Get_topo(self) -> Tuple[List[int], List[int]]:
        """
        Return index and edges
        """
        cdef int nindex = 0, nedges = 0
        CHKERR( MPI_Graphdims_get( self.ob_mpi, &nindex, &nedges) )
        cdef int *iindex = NULL
        cdef tmp1 = newarray(nindex, &iindex)
        cdef int *iedges = NULL
        cdef tmp2 = newarray(nedges, &iedges)
        CHKERR( MPI_Graph_get(self.ob_mpi, nindex, nedges, iindex, iedges) )
        cdef int i = 0
        cdef object index = [iindex[i] for i from 0 <= i < nindex]
        cdef object edges = [iedges[i] for i from 0 <= i < nedges]
        return (index, edges)

    property topo:
        """topology information"""
        def __get__(self) -> Tuple[List[int], List[int]]:
            return self.Get_topo()

    property index:
        """index"""
        def __get__(self) -> List[int]:
            return self.Get_topo()[0]

    property edges:
        """edges"""
        def __get__(self) -> List[int]:
            return self.Get_topo()[1]

    # Graph Information Functions
    # ---------------------------

    def Get_neighbors_count(self, int rank: int) -> int:
        """
        Return number of neighbors of a process
        """
        cdef int nneighbors = 0
        CHKERR( MPI_Graph_neighbors_count(self.ob_mpi, rank, &nneighbors) )
        return nneighbors

    property nneighbors:
        """number of neighbors"""
        def __get__(self) -> int:
            cdef int rank = self.Get_rank()
            return self.Get_neighbors_count(rank)

    def Get_neighbors(self, int rank: int) -> List[int]:
        """
        Return list of neighbors of a process
        """
        cdef int i = 0, nneighbors = 0, *ineighbors = NULL
        CHKERR( MPI_Graph_neighbors_count(
                self.ob_mpi, rank, &nneighbors) )
        cdef tmp = newarray(nneighbors, &ineighbors)
        CHKERR( MPI_Graph_neighbors(
                self.ob_mpi, rank, nneighbors, ineighbors) )
        cdef object neighbors = [ineighbors[i] for i from 0 <= i < nneighbors]
        return neighbors

    property neighbors:
        """neighbors"""
        def __get__(self) -> List[int]:
            cdef int rank = self.Get_rank()
            return self.Get_neighbors(rank)


cdef class Distgraphcomm(Topocomm):

    """
    Distributed graph topology intracommunicator
    """

    def __cinit__(self, Comm comm: Optional[Comm] = None):
        if self.ob_mpi == MPI_COMM_NULL: return
        cdef int topo = MPI_UNDEFINED
        CHKERR( MPI_Topo_test(self.ob_mpi, &topo) )
        if topo != MPI_DIST_GRAPH: raise TypeError(
            "expecting a distributed graph communicator")

    # Topology Inquiry Functions
    # --------------------------

    def Get_dist_neighbors_count(self) -> int:
        """
        Return adjacency information for a distributed graph topology
        """
        cdef int indegree = 0
        cdef int outdegree = 0
        cdef int weighted = 0
        CHKERR( MPI_Dist_graph_neighbors_count(
                self.ob_mpi, &indegree, &outdegree, &weighted) )
        return (indegree, outdegree, <bint>weighted)

    def Get_dist_neighbors(self) \
        -> Tuple[List[int], List[int], Optional[Tuple[List[int], List[int]]]]:
        """
        Return adjacency information for a distributed graph topology
        """
        cdef int maxindegree = 0, maxoutdegree = 0, weighted = 0
        CHKERR( MPI_Dist_graph_neighbors_count(
                self.ob_mpi, &maxindegree, &maxoutdegree, &weighted) )
        #
        cdef int *sources = NULL, *destinations = NULL
        cdef int *sourceweights = MPI_UNWEIGHTED
        cdef int *destweights   = MPI_UNWEIGHTED
        cdef tmp1, tmp2, tmp3, tmp4
        tmp1 = newarray(maxindegree,  &sources)
        tmp2 = newarray(maxoutdegree, &destinations)
        cdef int i = 0
        if weighted:
            tmp3 = newarray(maxindegree,  &sourceweights)
            for i from 0 <= i < maxindegree:  sourceweights[i] = 1
            tmp4 = newarray(maxoutdegree, &destweights)
            for i from 0 <= i < maxoutdegree: destweights[i]   = 1
        #
        CHKERR( MPI_Dist_graph_neighbors(
                self.ob_mpi,
                maxindegree,  sources,      sourceweights,
                maxoutdegree, destinations, destweights) )
        #
        cdef object src = [sources[i]      for i from 0 <= i < maxindegree]
        cdef object dst = [destinations[i] for i from 0 <= i < maxoutdegree]
        if not weighted: return (src, dst, None)
        #
        cdef object sw = [sourceweights[i] for i from 0 <= i < maxindegree]
        cdef object dw = [destweights[i]   for i from 0 <= i < maxoutdegree]
        return (src, dst, (sw, dw))


cdef class Intercomm(Comm):

    """
    Intercommunicator
    """

    def __cinit__(self, Comm comm: Optional[Comm] = None):
        if self.ob_mpi == MPI_COMM_NULL: return
        cdef int inter = 0
        CHKERR( MPI_Comm_test_inter(self.ob_mpi, &inter) )
        if not inter: raise TypeError(
            "expecting an intercommunicator")

    # Intercommunicator Accessors
    # ---------------------------

    def Get_remote_group(self) -> Group:
        """
        Access the remote group associated
        with the inter-communicator
        """
        cdef Group group = Group.__new__(Group)
        with nogil: CHKERR( MPI_Comm_remote_group(
            self.ob_mpi, &group.ob_mpi) )
        return group

    property remote_group:
        """remote group"""
        def __get__(self) -> Group:
            return self.Get_remote_group()

    def Get_remote_size(self) -> int:
        """
        Intercommunicator remote size
        """
        cdef int size = -1
        CHKERR( MPI_Comm_remote_size(self.ob_mpi, &size) )
        return size

    property remote_size:
        """number of remote processes"""
        def __get__(self) -> int:
            return self.Get_remote_size()

    # Communicator Constructors
    # -------------------------

    def Merge(self, bint high: bool = False) -> Intracomm:
        """
        Merge intercommunicator
        """
        cdef Intracomm comm = Intracomm.__new__(Intracomm)
        with nogil: CHKERR( MPI_Intercomm_merge(
            self.ob_mpi, high, &comm.ob_mpi) )
        comm_set_eh(comm.ob_mpi)
        return comm



cdef Comm      __COMM_NULL__   = new_Comm      ( MPI_COMM_NULL  )
cdef Intracomm __COMM_SELF__   = new_Intracomm ( MPI_COMM_SELF  )
cdef Intracomm __COMM_WORLD__  = new_Intracomm ( MPI_COMM_WORLD )
cdef Intercomm __COMM_PARENT__ = new_Intercomm ( MPI_COMM_NULL  )


# Predefined communicators
# ------------------------

COMM_NULL  = __COMM_NULL__   #: Null communicator handle
COMM_SELF  = __COMM_SELF__   #: Self communicator handle
COMM_WORLD = __COMM_WORLD__  #: World communicator handle


# Buffer Allocation and Usage
# ---------------------------

BSEND_OVERHEAD = MPI_BSEND_OVERHEAD
#: Upper bound of memory overhead for sending in buffered mode

def Attach_buffer(buf: Buffer) -> None:
    """
    Attach a user-provided buffer for
    sending in buffered mode
    """
    cdef void *base = NULL
    cdef int size = 0
    attach_buffer(buf, &base, &size)
    with nogil: CHKERR( MPI_Buffer_attach(base, size) )

def Detach_buffer() -> Buffer:
    """
    Remove an existing attached buffer
    """
    cdef void *base = NULL
    cdef int size = 0
    with nogil: CHKERR( MPI_Buffer_detach(&base, &size) )
    return detach_buffer(base, size)


# --------------------------------------------------------------------
# Process Creation and Management
# --------------------------------------------------------------------

# Server Routines
# ---------------

def Open_port(Info info: Info = INFO_NULL) -> str:
    """
    Return an address that can be used to establish
    connections between groups of MPI processes
    """
    cdef char cportname[MPI_MAX_PORT_NAME+1]
    cportname[0] = 0 # just in case
    with nogil: CHKERR( MPI_Open_port(info.ob_mpi, cportname) )
    cportname[MPI_MAX_PORT_NAME] = 0 # just in case
    return mpistr(cportname)

def Close_port(port_name: str) -> None:
    """
    Close a port
    """
    cdef char *cportname = NULL
    port_name = asmpistr(port_name, &cportname)
    with nogil: CHKERR( MPI_Close_port(cportname) )

# Name Publishing
# ---------------

def Publish_name(
    service_name: str,
    port_name: str,
    info: Info = INFO_NULL,
) -> None:
    """
    Publish a service name
    """
    if isinstance(port_name, Info): # backward compatibility
        port_name, info = info, port_name
    cdef char *csrvcname = NULL
    service_name = asmpistr(service_name, &csrvcname)
    cdef char *cportname = NULL
    port_name = asmpistr(port_name, &cportname)
    cdef MPI_Info cinfo = arg_Info(<Info?>info)
    with nogil: CHKERR( MPI_Publish_name(csrvcname, cinfo, cportname) )

def Unpublish_name(
    service_name: str,
    port_name: str,
    info: Info = INFO_NULL,
) -> None:
    """
    Unpublish a service name
    """
    if isinstance(port_name, Info): # backward compatibility
        port_name, info = info, port_name
    cdef char *csrvcname = NULL
    service_name = asmpistr(service_name, &csrvcname)
    cdef char *cportname = NULL
    port_name = asmpistr(port_name, &cportname)
    cdef MPI_Info cinfo = arg_Info(<Info?>info)
    with nogil: CHKERR( MPI_Unpublish_name(csrvcname, cinfo, cportname) )

def Lookup_name(
    service_name: str,
    info: Info = INFO_NULL,
) -> str:
    """
    Lookup a port name given a service name
    """
    cdef char *csrvcname = NULL
    service_name = asmpistr(service_name, &csrvcname)
    cdef MPI_Info cinfo = arg_Info(<Info?>info)
    cdef char cportname[MPI_MAX_PORT_NAME+1]
    cportname[0] = 0 # just in case
    with nogil: CHKERR( MPI_Lookup_name(csrvcname, cinfo, cportname) )
    cportname[MPI_MAX_PORT_NAME] = 0 # just in case
    return mpistr(cportname)
