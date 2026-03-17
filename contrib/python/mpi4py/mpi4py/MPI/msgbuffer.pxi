#------------------------------------------------------------------------------

cdef extern from "Python.h":
    int is_list   "PyList_Check" (object)
    int is_tuple  "PyTuple_Check" (object)

cdef extern from "Python.h":
    int PyIndex_Check(object)
    int PySequence_Check(object)
    object PyNumber_Index(object)
    Py_ssize_t PySequence_Size(object) except -1

cdef inline int is_integral(object ob):
    if not PyIndex_Check(ob): return 0
    if not PySequence_Check(ob): return 1
    try: PySequence_Size(ob)
    except: pass
    else: return 0
    try: PyNumber_Index(ob)
    except: return 0
    else: return 1

cdef inline int is_buffer(object ob):
    if PY3:
        return PyObject_CheckBuffer(ob)
    else:
        return PyObject_CheckBuffer(ob) or _Py2_IsBuffer(ob)

cdef inline int is_dlpack_buffer(object ob):
    return Py_CheckDLPackBuffer(ob)

cdef inline int is_cai_buffer(object ob):
    return Py_CheckCAIBuffer(ob)

cdef inline int is_datatype(object ob):
    if isinstance(ob, Datatype): return 1
    if PY3:
        if isinstance(ob, unicode): return 1
    else:
        if isinstance(ob, bytes): return 1
    return 0

#------------------------------------------------------------------------------

cdef extern from *:
    int INT_MAX

cdef inline int downcast(MPI_Aint value) except? -1:
    if value > <MPI_Aint>INT_MAX:
        raise OverflowError("integer %d does not fit in 'int'" % value)
    else:
        return <int>value

cdef inline int clipcount(MPI_Aint value):
    if value > <MPI_Aint>INT_MAX:
        return INT_MAX
    else:
        return <int>value

#------------------------------------------------------------------------------

@cython.final
@cython.internal
cdef class Bottom(int):
    """
    Type of `BOTTOM`
    """

    def __cinit__(self):
        cdef MPI_Aint a = self, b = <MPI_Aint>MPI_BOTTOM
        if a != b : raise ValueError("cannot create instance")

    def __repr__(self):
        return 'BOTTOM'


@cython.final
@cython.internal
cdef class InPlace(int):
    """
    Type of `IN_PLACE`
    """

    def __cinit__(self):
        cdef MPI_Aint a = self, b = <MPI_Aint>MPI_IN_PLACE
        if a != b : raise ValueError("cannot create instance")

    def __repr__(self):
        return 'IN_PLACE'


cdef object __BOTTOM__   = Bottom(<MPI_Aint>MPI_BOTTOM)

cdef object __IN_PLACE__ = InPlace(<MPI_Aint>MPI_IN_PLACE)


cdef inline bint is_BOTTOM(object obj):
    if PYPY: return type(obj) is type(__BOTTOM__) and obj == __BOTTOM__
    else:    return obj is __BOTTOM__

cdef inline bint is_IN_PLACE(object obj):
    if PYPY: return type(obj) is type(__IN_PLACE__) and obj == __IN_PLACE__
    else:    return obj is __IN_PLACE__

#------------------------------------------------------------------------------

@cython.final
@cython.internal
cdef class _p_message:
    cdef memory buf
    cdef object count
    cdef object displ
    cdef Datatype type

cdef _p_message message_basic(object o_buf,
                              object o_type,
                              bint readonly,
                              #
                              void        **baddr,
                              MPI_Aint     *bsize,
                              MPI_Datatype *btype,
                              ):
    cdef _p_message m = _p_message.__new__(_p_message)
    # special-case for BOTTOM or None,
    # an explicit MPI datatype is required
    if is_BOTTOM(o_buf) or o_buf is None:
        if isinstance(o_type, Datatype):
            m.type = <Datatype>o_type
        else:
            m.type = TypeDict[o_type]
        m.buf = newbuffer()
        baddr[0] = MPI_BOTTOM
        bsize[0] = 0
        btype[0] = m.type.ob_mpi
        return m
    # get buffer base address and length
    cdef bint fmt = (o_type is None)
    m.buf = getbuffer(o_buf, readonly, fmt)
    baddr[0] = <void*>    m.buf.view.buf
    bsize[0] = <MPI_Aint> m.buf.view.len
    # lookup datatype if not provided or not a Datatype
    if isinstance(o_type, Datatype):
        m.type = <Datatype>o_type
    elif o_type is None:
        m.type = TypeDict[getformat(m.buf)]
    else:
        m.type = TypeDict[o_type]
    btype[0] = m.type.ob_mpi
    # and we are done ...
    return m

cdef _p_message message_simple(object msg,
                               int readonly,
                               int rank,
                               int blocks,
                               #
                               void         **_addr,
                               int          *_count,
                               MPI_Datatype *_type,
                               ):
    # special-case PROC_NULL target rank
    if rank == MPI_PROC_NULL:
        _addr[0]  = NULL
        _count[0] = 0
        _type[0]  = MPI_BYTE
        return None
    # unpack message list/tuple
    cdef Py_ssize_t nargs = 0
    cdef object o_buf   = None
    cdef object o_count = None
    cdef object o_displ = None
    cdef object o_type  = None
    if is_buffer(msg):
        o_buf = msg
    elif is_list(msg) or is_tuple(msg):
        nargs = len(msg)
        if nargs == 2:
            (o_buf, o_count) = msg
            if is_datatype(o_count):
                (o_count, o_type) = None, o_count
            elif is_tuple(o_count) or is_list(o_count):
                (o_count, o_displ) = o_count
        elif nargs == 3:
            (o_buf, o_count, o_type) = msg
            if is_tuple(o_count) or is_list(o_count):
                (o_count, o_displ) = o_count
        elif nargs == 4:
            (o_buf, o_count, o_displ, o_type) = msg
        else:
            raise ValueError("message: expecting 2 to 4 items")
    elif is_dlpack_buffer(msg):
        o_buf = msg
    elif is_cai_buffer(msg):
        o_buf = msg
    elif PYPY:
        o_buf = msg
    else:
        raise TypeError("message: expecting buffer or list/tuple")
    # buffer: address, length, and datatype
    cdef void *baddr = NULL
    cdef MPI_Aint bsize = 0
    cdef MPI_Datatype btype = MPI_DATATYPE_NULL
    cdef _p_message m = message_basic(o_buf, o_type, readonly,
                                      &baddr, &bsize, &btype)
    # buffer: count and displacement
    cdef int count = 0 # number of datatype entries
    cdef int displ = 0 # from base buffer, in datatype entries
    cdef MPI_Aint extent = 0, lb = 0 # datatype extent
    cdef MPI_Aint length = bsize     # in bytes
    cdef MPI_Aint offset = 0         # from base buffer, in bytes
    if o_displ is not None:
        displ = <int> o_displ
        if displ < 0: raise ValueError(
            "message: negative diplacement %d" % displ)
        if displ > 0:
            if btype == MPI_DATATYPE_NULL: raise ValueError(
                "message: cannot handle diplacement, datatype is null")
            CHKERR( MPI_Type_get_extent(btype, &lb, &extent) )
            if extent <= 0: raise ValueError(
                ("message: cannot handle diplacement, "
                 "datatype extent %d (lb:%d, ub:%d)"
                 ) % (extent, lb, lb+extent))
            if displ*extent > length: raise ValueError(
                ("message: displacement %d out of bounds, "
                 "number of datatype entries %d"
                 ) % (displ, length//extent))
            offset = displ*extent
            length -= offset
    if o_count is not None:
        count = <int> o_count
        if count < 0: raise ValueError(
            "message: negative count %d" % count)
        if count > 0 and o_buf is None: raise ValueError(
            "message: buffer is None but count is %d" % count)
    elif length > 0:
        if extent == 0:
            if btype == MPI_DATATYPE_NULL: raise ValueError(
                "message: cannot infer count, datatype is null")
            CHKERR( MPI_Type_get_extent(btype, &lb, &extent) )
            if extent <= 0: raise ValueError(
                ("message: cannot infer count, "
                 "datatype extent %d (lb:%d, ub:%d)"
                 ) % (extent, lb, lb+extent))
        if (length % extent) != 0: raise ValueError(
            ("message: cannot infer count, "
             "buffer length %d is not a multiple of "
             "datatype extent %d (lb:%d, ub:%d)"
             ) % (length, extent, lb, lb+extent))
        if blocks < 2:
            count = downcast(length // extent)
        else:
            if ((length // extent) % blocks) != 0: raise ValueError(
                ("message: cannot infer count, "
                 "number of entries %d is not a multiple of "
                 "required number of blocks %d"
                 ) %  (length//extent, blocks))
            count = downcast((length // extent) // blocks)
    # return collected message data
    m.count = o_count if o_count is not None else count
    m.displ = o_displ if o_displ is not None else displ
    _addr[0]  = <void*>(<char*>baddr + offset)
    _count[0] = count
    _type[0]  = btype
    return m

cdef _p_message message_vector(object msg,
                               int readonly,
                               int rank,
                               int blocks,
                               #
                               void        **_addr,
                               int         **_counts,
                               int         **_displs,
                               MPI_Datatype *_type,
                               ):
    # special-case PROC_NULL target rank
    if rank == MPI_PROC_NULL:
        _addr[0]   = NULL
        _counts[0] = NULL
        _displs[0] = NULL
        _type[0]   = MPI_BYTE
        return None
    # unpack message list/tuple
    cdef Py_ssize_t nargs = 0
    cdef object o_buf    = None
    cdef object o_counts = None
    cdef object o_displs = None
    cdef object o_type   = None
    if is_buffer(msg):
        o_buf = msg
    elif is_list(msg) or is_tuple(msg):
        nargs = len(msg)
        if nargs == 2:
            (o_buf, o_counts) = msg
            if is_datatype(o_counts):
                (o_counts, o_type) = None, o_counts
            elif is_tuple(o_counts):
                (o_counts, o_displs) = o_counts
        elif nargs == 3:
            (o_buf, o_counts, o_type) = msg
            if is_tuple(o_counts):
                (o_counts, o_displs) = o_counts
        elif nargs == 4:
            (o_buf, o_counts, o_displs, o_type) = msg
        else:
            raise ValueError("message: expecting 2 to 4 items")
    elif is_dlpack_buffer(msg):
        o_buf = msg
    elif is_cai_buffer(msg):
        o_buf = msg
    elif PYPY:
        o_buf = msg
    else:
        raise TypeError("message: expecting buffer or list/tuple")
    # buffer: address, length, and datatype
    cdef void *baddr = NULL
    cdef MPI_Aint bsize = 0
    cdef MPI_Datatype btype = MPI_DATATYPE_NULL
    cdef _p_message m = message_basic(o_buf, o_type, readonly,
                                      &baddr, &bsize, &btype)
    # counts and displacements
    cdef int *counts = NULL
    cdef int *displs = NULL
    cdef int i=0, val=0
    cdef MPI_Aint extent=0, lb=0
    cdef MPI_Aint asize=0, aval=0
    if o_counts is None:
        if bsize > 0:
            if btype == MPI_DATATYPE_NULL: raise ValueError(
                "message: cannot infer count, "
                "datatype is null")
            CHKERR( MPI_Type_get_extent(btype, &lb, &extent) )
            if extent <= 0: raise ValueError(
                ("message: cannot infer count, "
                 "datatype extent %d (lb:%d, ub:%d)"
                 ) % (extent, lb, lb+extent))
            if (bsize % extent) != 0: raise ValueError(
                ("message: cannot infer count, "
                 "buffer length %d is not a multiple of "
                 "datatype extent %d (lb:%d, ub:%d)"
                 ) % (bsize, extent, lb, lb+extent))
            asize = bsize // extent
        o_counts = newarray(blocks, &counts)
        for i from 0 <= i < blocks:
            aval = (asize // blocks) + (asize % blocks > i)
            counts[i] = downcast(aval)
    elif is_integral(o_counts):
        val = <int> o_counts
        o_counts = newarray(blocks, &counts)
        for i from 0 <= i < blocks:
            counts[i] = val
    else:
        o_counts = chkarray(o_counts, blocks, &counts)
    if o_displs is None: # contiguous
        val = 0
        o_displs = newarray(blocks, &displs)
        for i from 0 <= i < blocks:
            displs[i] = val
            val += counts[i]
    elif is_integral(o_displs): # strided
        val = <int> o_displs
        o_displs = newarray(blocks, &displs)
        for i from 0 <= i < blocks:
            displs[i] = val * i
    else: # general
        o_displs = chkarray(o_displs, blocks, &displs)
    # return collected message data
    m.count = o_counts
    m.displ = o_displs
    _addr[0]   = baddr
    _counts[0] = counts
    _displs[0] = displs
    _type[0]   = btype
    return m

cdef tuple message_vector_w(object msg,
                            int readonly,
                            int blocks,
                            #
                            void         **_addr,
                            int          **_counts,
                            integral_t   **_displs,
                            MPI_Datatype **_types,
                            ):
    cdef int i = 0
    cdef Py_ssize_t nargs = len(msg)
    cdef object o_buffer, o_counts, o_displs, o_types
    if nargs == 2:
        o_buffer, o_types = msg
        o_counts = o_displs = None
    elif nargs == 3:
        o_buffer, (o_counts, o_displs), o_types = msg
    elif nargs == 4:
        o_buffer,  o_counts, o_displs,  o_types = msg
    else:
        raise ValueError("message: expecting 2 to 4 items")
    if is_BOTTOM(o_buffer):
        if o_counts is None:
            raise ValueError("message: BOTTOM requires counts")
        if o_displs is None:
            raise ValueError("message: BOTTOM requires displs")
        _addr[0] = MPI_BOTTOM
    elif readonly:
        o_buffer = getbuffer_r(o_buffer, _addr, NULL)
    else:
        o_buffer = getbuffer_w(o_buffer, _addr, NULL)
    if o_counts is None and o_displs is None:
        o_counts = newarray(blocks, _counts)
        o_displs = newarray(blocks, _displs)
        for i from 0 <= i < blocks:
            _counts[0][i] = 1
            _displs[0][i] = 0
    else:
        o_counts = chkarray(o_counts, blocks, _counts)
        o_displs = chkarray(o_displs, blocks, _displs)
    o_types  = asarray_Datatype(o_types, blocks, _types)
    return (o_buffer, o_counts, o_displs, o_types)

#------------------------------------------------------------------------------

@cython.final
@cython.internal
cdef class _p_msg_p2p:

    # raw C-side arguments
    cdef void         *buf
    cdef int          count
    cdef MPI_Datatype dtype
    # python-side argument
    cdef object _msg

    def __cinit__(self):
        self.buf   = NULL
        self.count = 0
        self.dtype = MPI_DATATYPE_NULL

    cdef int for_send(self, object msg, int rank) except -1:
        self._msg = message_simple(msg, 1, # readonly
                                   rank, 0,
                                   &self.buf,
                                   &self.count,
                                   &self.dtype)
        return 0

    cdef int for_recv(self, object msg, int rank) except -1:
        self._msg = message_simple(msg, 0, # writable
                                   rank, 0,
                                   &self.buf,
                                   &self.count,
                                   &self.dtype)
        return 0

cdef inline _p_msg_p2p message_p2p_send(object sendbuf, int dest):
    cdef _p_msg_p2p msg = _p_msg_p2p.__new__(_p_msg_p2p)
    msg.for_send(sendbuf, dest)
    return msg

cdef inline _p_msg_p2p message_p2p_recv(object recvbuf, int source):
    cdef _p_msg_p2p msg = _p_msg_p2p.__new__(_p_msg_p2p)
    msg.for_recv(recvbuf, source)
    return msg

#------------------------------------------------------------------------------

@cython.final
@cython.internal
cdef class _p_msg_cco:

    # raw C-side arguments
    cdef void *sbuf, *rbuf
    cdef int scount, rcount
    cdef int *scounts, *rcounts
    cdef int *sdispls, *rdispls
    cdef MPI_Datatype stype, rtype
    # python-side arguments
    cdef object _smsg, _rmsg
    cdef object _rcnt

    def __cinit__(self):
        self.sbuf    = self.rbuf    = NULL
        self.scount  = self.rcount  = 0
        self.scounts = self.rcounts = NULL
        self.sdispls = self.rdispls = NULL
        self.stype   = self.rtype   = MPI_DATATYPE_NULL

    # Collective Communication Operations
    # -----------------------------------

    # sendbuf arguments
    cdef int for_cco_send(self, bint VECTOR,
                          object amsg,
                          int rank, int blocks) except -1:
        cdef bint readonly = 1
        if not VECTOR: # block variant
            self._smsg = message_simple(
                amsg, readonly, rank, blocks,
                &self.sbuf, &self.scount, &self.stype)
        else: # vector variant
            self._smsg = message_vector(
                amsg, readonly, rank, blocks,
                &self.sbuf, &self.scounts,
                &self.sdispls, &self.stype)
        return 0

    # recvbuf arguments
    cdef int for_cco_recv(self, bint VECTOR,
                          object amsg,
                          int rank, int blocks) except -1:
        cdef bint readonly = 0
        if not VECTOR: # block variant
            self._rmsg = message_simple(
                amsg, readonly, rank, blocks,
                &self.rbuf, &self.rcount, &self.rtype)
        else: # vector variant
            self._rmsg = message_vector(
                amsg, readonly, rank, blocks,
                &self.rbuf, &self.rcounts,
                &self.rdispls, &self.rtype)
        return 0

    # bcast
    cdef int for_bcast(self,
                       object msg, int root,
                       MPI_Comm comm) except -1:
        if comm == MPI_COMM_NULL: return 0
        cdef int inter=0, rank=0, sending=0
        CHKERR( MPI_Comm_test_inter(comm, &inter) )
        if not inter: # intra-communication
            CHKERR( MPI_Comm_rank(comm, &rank) )
            if root == rank:
                self.for_cco_send(0, msg, root, 0)
                sending = 1
            else:
                self.for_cco_recv(0, msg, root, 0)
                sending = 0
        else: # inter-communication
            if (root == MPI_ROOT or
                root == MPI_PROC_NULL):
                self.for_cco_send(0, msg, root, 0)
                sending = 1
            else:
                self.for_cco_recv(0, msg, root, 0)
                sending = 0
        if sending:
            self.rbuf   = self.sbuf
            self.rcount = self.scount
            self.rtype  = self.stype
        else:
            self.sbuf   = self.rbuf
            self.scount = self.rcount
            self.stype  = self.rtype
        return 0

    # gather/gatherv
    cdef int for_gather(self, int v,
                        object smsg, object rmsg,
                        int root, MPI_Comm comm) except -1:
        if comm == MPI_COMM_NULL: return 0
        cdef int inter=0, size=0, rank=0, null=MPI_PROC_NULL
        CHKERR( MPI_Comm_test_inter(comm, &inter) )
        if not inter: # intra-communication
            CHKERR( MPI_Comm_size(comm, &size) )
            CHKERR( MPI_Comm_rank(comm, &rank) )
            if root == rank:
                self.for_cco_recv(v, rmsg, root, size)
                if is_IN_PLACE(smsg):
                    self.sbuf   = MPI_IN_PLACE
                    self.scount = self.rcount
                    self.stype  = self.rtype
                else:
                    self.for_cco_send(0, smsg, 0, 0)
            else:
                self.for_cco_recv(v, rmsg, null, size)
                self.for_cco_send(0, smsg, root, 0)
        else: # inter-communication
            CHKERR( MPI_Comm_remote_size(comm, &size) )
            if (root == MPI_ROOT or
                root == MPI_PROC_NULL):
                self.for_cco_recv(v, rmsg, root, size)
                self.for_cco_send(0, smsg, null, 0)
            else:
                self.for_cco_recv(v, rmsg, null, size)
                self.for_cco_send(0, smsg, root, 0)
        return 0

    # scatter/scatterv
    cdef int for_scatter(self, int v,
                         object smsg, object rmsg,
                         int root, MPI_Comm comm) except -1:
        if comm == MPI_COMM_NULL: return 0
        cdef int inter=0, size=0, rank=0, null=MPI_PROC_NULL
        CHKERR( MPI_Comm_test_inter(comm, &inter) )
        if not inter: # intra-communication
            CHKERR( MPI_Comm_size(comm, &size) )
            CHKERR( MPI_Comm_rank(comm, &rank) )
            if root == rank:
                self.for_cco_send(v, smsg, root, size)
                if is_IN_PLACE(rmsg):
                    self.rbuf   = MPI_IN_PLACE
                    self.rcount = self.scount
                    self.rtype  = self.stype
                else:
                    self.for_cco_recv(0, rmsg, root, 0)
            else:
                self.for_cco_send(v, smsg, null, size)
                self.for_cco_recv(0, rmsg, root, 0)
        else: # inter-communication
            CHKERR( MPI_Comm_remote_size(comm, &size) )
            if (root == MPI_ROOT or
                root == MPI_PROC_NULL):
                self.for_cco_send(v, smsg, root, size)
                self.for_cco_recv(0, rmsg, null,  0)
            else:
                self.for_cco_send(v, smsg, null, size)
                self.for_cco_recv(0, rmsg, root, 0)
        return 0

    # allgather/allgatherv
    cdef int for_allgather(self, int v,
                           object smsg, object rmsg,
                           MPI_Comm comm) except -1:
        if comm == MPI_COMM_NULL: return 0
        cdef int inter=0, size=0
        CHKERR( MPI_Comm_test_inter(comm, &inter) )
        if not inter: # intra-communication
            CHKERR( MPI_Comm_size(comm, &size) )
        else: # inter-communication
            CHKERR( MPI_Comm_remote_size(comm, &size) )
        #
        self.for_cco_recv(v, rmsg, 0, size)
        if not inter and is_IN_PLACE(smsg):
            self.sbuf   = MPI_IN_PLACE
            self.scount = self.rcount
            self.stype  = self.rtype
        else:
            self.for_cco_send(0, smsg, 0, 0)
        return 0

    # alltoall/alltoallv
    cdef int for_alltoall(self, int v,
                          object smsg, object rmsg,
                          MPI_Comm comm) except -1:
        if comm == MPI_COMM_NULL: return 0
        cdef int inter=0, size=0
        CHKERR( MPI_Comm_test_inter(comm, &inter) )
        if not inter: # intra-communication
            CHKERR( MPI_Comm_size(comm, &size) )
        else: # inter-communication
            CHKERR( MPI_Comm_remote_size(comm, &size) )
        #
        self.for_cco_recv(v, rmsg, 0, size)
        if not inter and is_IN_PLACE(smsg):
            self.sbuf    = MPI_IN_PLACE
            self.scount  = self.rcount
            self.scounts = self.rcounts
            self.sdispls = self.rdispls
            self.stype   = self.rtype
        else:
            self.for_cco_send(v, smsg, 0, size)
        return 0

    # Neighbor Collectives
    # --------------------

    # neighbor allgather/allgatherv
    cdef int for_neighbor_allgather(self, int v,
                                    object smsg, object rmsg,
                                    MPI_Comm comm) except -1:
        if comm == MPI_COMM_NULL: return 0
        cdef int recvsize=0
        comm_neighbors_count(comm, &recvsize, NULL)
        self.for_cco_send(0, smsg, 0, 0)
        self.for_cco_recv(v, rmsg, 0, recvsize)
        return 0

    # neighbor alltoall/alltoallv
    cdef int for_neighbor_alltoall(self, int v,
                                   object smsg, object rmsg,
                                   MPI_Comm comm) except -1:
        if comm == MPI_COMM_NULL: return 0
        cdef int sendsize=0, recvsize=0
        comm_neighbors_count(comm, &recvsize, &sendsize)
        self.for_cco_send(v, smsg, 0, sendsize)
        self.for_cco_recv(v, rmsg, 0, recvsize)
        return 0

    # Collective Reductions Operations
    # --------------------------------

    # sendbuf
    cdef int for_cro_send(self, object amsg, int root) except -1:
        self._smsg = message_simple(amsg, 1, # readonly
                                    root, 0,
                                    &self.sbuf,
                                    &self.scount,
                                    &self.stype)
        return 0

    # recvbuf
    cdef int for_cro_recv(self, object amsg, int root) except -1:
        self._rmsg = message_simple(amsg, 0, # writable
                                    root, 0,
                                    &self.rbuf,
                                    &self.rcount,
                                    &self.rtype)
        return 0

    cdef int for_reduce(self,
                        object smsg, object rmsg,
                        int root, MPI_Comm comm) except -1:
        if comm == MPI_COMM_NULL: return 0
        cdef int inter=0, rank=0, null=MPI_PROC_NULL
        CHKERR( MPI_Comm_test_inter(comm, &inter) )
        if not inter: # intra-communication
            CHKERR( MPI_Comm_rank(comm, &rank) )
            if root == rank:
                self.for_cro_recv(rmsg, root)
                if is_IN_PLACE(smsg):
                    self.sbuf   = MPI_IN_PLACE
                    self.scount = self.rcount
                    self.stype  = self.rtype
                else:
                    self.for_cro_send(smsg, root)
            else:
                self.for_cro_recv(rmsg, null)
                self.for_cro_send(smsg, root)
                self.rcount = self.scount
                self.rtype  = self.stype
        else: # inter-communication
            if (root == MPI_ROOT or
                root == MPI_PROC_NULL):
                self.for_cro_recv(rmsg, root)
                self.scount = self.rcount
                self.stype  = self.rtype
            else:
                self.for_cro_send(smsg, root)
                self.rcount = self.scount
                self.rtype  = self.stype
        return 0

    cdef int for_allreduce(self,
                           object smsg, object rmsg,
                           MPI_Comm comm) except -1:
        if comm == MPI_COMM_NULL: return 0
        cdef int inter=0
        CHKERR( MPI_Comm_test_inter(comm, &inter) )
        # get send and recv buffers
        self.for_cro_recv(rmsg, 0)
        if not inter and is_IN_PLACE(smsg):
            self.sbuf   = MPI_IN_PLACE
            self.scount = self.rcount
            self.stype  = self.rtype
        else:
            self.for_cro_send(smsg, 0)
        # check counts and datatypes
        if self.sbuf != MPI_IN_PLACE:
            if self.stype != self.rtype:
                raise ValueError(
                    "mismatch in send and receive MPI datatypes")
            if self.scount != self.rcount:
                raise ValueError(
                    "mismatch in send count %d and receive count %d" %
                    (self.scount, self.rcount))
        return 0

    cdef int for_reduce_scatter_block(self,
                                      object smsg, object rmsg,
                                      MPI_Comm comm) except -1:
        if comm == MPI_COMM_NULL: return 0
        cdef int inter=0, size=0
        CHKERR( MPI_Comm_test_inter(comm, &inter) )
        CHKERR( MPI_Comm_size(comm, &size) )
        # get send and recv buffers
        if not inter and is_IN_PLACE(smsg):
            self.for_cco_recv(0, rmsg, 0, size)
            self.sbuf = MPI_IN_PLACE
        else:
            self.for_cco_recv(0, rmsg, 0, 0)
            self.for_cco_send(0, smsg, 0, size)
        # check counts and datatypes
        if self.sbuf != MPI_IN_PLACE:
            if self.stype != self.rtype:
                raise ValueError(
                    "mismatch in send and receive MPI datatypes")
            if self.scount != self.rcount:
                raise ValueError(
                    "mismatch in send count %d receive count %d" %
                    (self.scount, self.rcount*size))
        return 0

    cdef int for_reduce_scatter(self,
                                object smsg, object rmsg, object rcnt,
                                MPI_Comm comm) except -1:
        if comm == MPI_COMM_NULL: return 0
        cdef int inter=0, size=0, rank=MPI_PROC_NULL
        CHKERR( MPI_Comm_test_inter(comm, &inter) )
        CHKERR( MPI_Comm_size(comm, &size) )
        CHKERR( MPI_Comm_rank(comm, &rank) )
        # get send and recv buffers
        self.for_cro_recv(rmsg, 0)
        if not inter and is_IN_PLACE(smsg):
            self.sbuf = MPI_IN_PLACE
        else:
            self.for_cro_send(smsg, 0)
        # get receive counts
        if rcnt is None and not inter and self.sbuf != MPI_IN_PLACE:
            self._rcnt = newarray(size, &self.rcounts)
            CHKERR( MPI_Allgather(&self.rcount, 1, MPI_INT,
                                  self.rcounts, 1, MPI_INT, comm) )
        else:
            self._rcnt = chkarray(rcnt, size, &self.rcounts)
        # total sum or receive counts
        cdef int i=0, sumrcounts=0
        for i from 0 <= i < size:
            sumrcounts += self.rcounts[i]
        # check counts and datatypes
        if self.sbuf != MPI_IN_PLACE:
            if self.stype != self.rtype:
                raise ValueError(
                    "mismatch in send and receive MPI datatypes")
            if self.scount != sumrcounts:
                raise ValueError(
                    "mismatch in send count %d and sum(counts) %d" %
                    (self.scount, sumrcounts))
            if self.rcount != self.rcounts[rank]:
                raise ValueError(
                    "mismatch in receive count %d and counts[%d] %d" %
                    (self.rcount, rank, self.rcounts[rank]))
        else:
            if self.rcount != sumrcounts:
                raise ValueError(
                    "mismatch in receive count %d and sum(counts) %d" %
                    (self.rcount, sumrcounts))
        return 0

    cdef int for_scan(self,
                      object smsg, object rmsg,
                      MPI_Comm comm) except -1:
        if comm == MPI_COMM_NULL: return 0
        # get send and recv buffers
        self.for_cro_recv(rmsg, 0)
        if is_IN_PLACE(smsg):
            self.sbuf   = MPI_IN_PLACE
            self.scount = self.rcount
            self.stype  = self.rtype
        else:
            self.for_cro_send(smsg, 0)
        # check counts and datatypes
        if self.sbuf != MPI_IN_PLACE:
            if self.stype != self.rtype:
                raise ValueError(
                    "mismatch in send and receive MPI datatypes")
            if self.scount != self.rcount:
                raise ValueError(
                    "mismatch in send count %d and receive count %d" %
                    (self.scount, self.rcount))
        return 0

    cdef int for_exscan(self,
                        object smsg, object rmsg,
                        MPI_Comm comm) except -1:
        if comm == MPI_COMM_NULL: return 0
        # get send and recv buffers
        self.for_cro_recv(rmsg, 0)
        if is_IN_PLACE(smsg):
            self.sbuf   = MPI_IN_PLACE
            self.scount = self.rcount
            self.stype  = self.rtype
        else:
            self.for_cro_send(smsg, 0)
        # check counts and datatypes
        if self.sbuf != MPI_IN_PLACE:
            if self.stype != self.rtype:
                raise ValueError(
                    "mismatch in send and receive MPI datatypes")
            if self.scount != self.rcount:
                raise ValueError(
                    "mismatch in send count %d and receive count %d" %
                    (self.scount, self.rcount))
        return 0


cdef inline _p_msg_cco message_cco():
    cdef _p_msg_cco msg = _p_msg_cco.__new__(_p_msg_cco)
    return msg

#------------------------------------------------------------------------------

@cython.final
@cython.internal
cdef class _p_msg_ccow:

    # raw C-side arguments
    cdef void *sbuf, *rbuf
    cdef int *scounts, *rcounts
    cdef int *sdispls, *rdispls
    cdef MPI_Aint *sdisplsA, *rdisplsA
    cdef MPI_Datatype *stypes, *rtypes
    # python-side arguments
    cdef object _smsg, _rmsg

    def __cinit__(self):
        self.sbuf     = self.rbuf     = NULL
        self.scounts  = self.rcounts  = NULL
        self.sdispls  = self.rdispls  = NULL
        self.sdisplsA = self.rdisplsA = NULL
        self.stypes   = self.rtypes   = NULL

    # alltoallw
    cdef int for_alltoallw(self,
                          object smsg, object rmsg,
                          MPI_Comm comm) except -1:
        if comm == MPI_COMM_NULL: return 0
        cdef int inter=0, size=0
        CHKERR( MPI_Comm_test_inter(comm, &inter) )
        if not inter: # intra-communication
            CHKERR( MPI_Comm_size(comm, &size) )
        else: # inter-communication
            CHKERR( MPI_Comm_remote_size(comm, &size) )
        #
        self._rmsg = message_vector_w(
            rmsg, 0, size,
            &self.rbuf, &self.rcounts,
            &self.rdispls, &self.rtypes)
        if not inter and is_IN_PLACE(smsg):
            self.sbuf    = MPI_IN_PLACE
            self.scounts = self.rcounts
            self.sdispls = self.rdispls
            self.stypes  = self.rtypes
            return 0
        self._smsg = message_vector_w(
            smsg, 1, size,
            &self.sbuf, &self.scounts,
            &self.sdispls, &self.stypes)
        return 0

    # neighbor alltoallw
    cdef int for_neighbor_alltoallw(self,
                                    object smsg, object rmsg,
                                    MPI_Comm comm) except -1:
        if comm == MPI_COMM_NULL: return 0
        cdef int sendsize=0, recvsize=0
        comm_neighbors_count(comm, &recvsize, &sendsize)
        self._rmsg = message_vector_w(
            rmsg, 0, recvsize,
            &self.rbuf, &self.rcounts,
            &self.rdisplsA, &self.rtypes)
        self._smsg = message_vector_w(
            smsg, 1, sendsize,
            &self.sbuf, &self.scounts,
            &self.sdisplsA, &self.stypes)
        return 0


cdef inline _p_msg_ccow message_ccow():
    cdef _p_msg_ccow msg = _p_msg_ccow.__new__(_p_msg_ccow)
    return msg

#------------------------------------------------------------------------------

@cython.final
@cython.internal
cdef class _p_msg_rma:

    # raw origin arguments
    cdef void*        oaddr
    cdef int          ocount
    cdef MPI_Datatype otype
    # raw compare arguments
    cdef void*        caddr
    cdef int          ccount
    cdef MPI_Datatype ctype
    # raw result arguments
    cdef void*        raddr
    cdef int          rcount
    cdef MPI_Datatype rtype
    # raw target arguments
    cdef MPI_Aint     tdisp
    cdef int          tcount
    cdef MPI_Datatype ttype
    # python-side arguments
    cdef object _origin
    cdef object _compare
    cdef object _result
    cdef object _target

    def __cinit__(self):
        self.oaddr  = NULL
        self.ocount = 0
        self.otype  = MPI_DATATYPE_NULL
        self.raddr  = NULL
        self.rcount = 0
        self.rtype  = MPI_DATATYPE_NULL
        self.tdisp  = 0
        self.tcount = 0
        self.ttype  = MPI_DATATYPE_NULL

    cdef int for_rma(self, int readonly,
                     object origin, int rank, object target) except -1:
        # ORIGIN
        self._origin = message_simple(
            origin, readonly, rank, 0,
            &self.oaddr,  &self.ocount,  &self.otype)
        if ((rank == MPI_PROC_NULL) and
            (origin is not None) and
            (is_list(origin) or is_tuple(origin)) and
            (len(origin) > 0 and isinstance(origin[-1], Datatype))):
            self.otype  = (<Datatype?>origin[-1]).ob_mpi
            self._origin = origin
        # TARGET
        cdef Py_ssize_t nargs = 0
        if target is None:
            self.tdisp  = 0
            self.tcount = self.ocount
            self.ttype  = self.otype
        elif is_integral(target):
            self.tdisp  = <MPI_Aint>target
            self.tcount = self.ocount
            self.ttype  = self.otype
        elif is_list(target) or is_tuple(target):
            self.tdisp  = 0
            self.tcount = self.ocount
            self.ttype  = self.otype
            nargs = len(target)
            if nargs >= 1:
                self.tdisp  = <MPI_Aint>target[0]
            if nargs >= 2:
                self.tcount = <int>target[1]
            if nargs >= 3:
                self.ttype  = (<Datatype?>target[2]).ob_mpi
            if nargs >= 4:
                raise ValueError("target: expecting 3 items at most")
        else:
            raise ValueError("target: expecting integral or list/tuple")
        self._target = target
        return 0

    cdef int for_put(self, object origin, int rank, object target) except -1:
        self.for_rma(1, origin, rank, target)
        return 0

    cdef int for_get(self, object origin, int rank, object target) except -1:
        self.for_rma(0, origin, rank, target)
        return 0

    cdef int for_acc(self, object origin, int rank, object target) except -1:
        self.for_rma(1, origin, rank, target)
        return 0

    cdef int set_origin(self, object origin, int rank) except -1:
        self._origin = message_simple(
            origin, 1, rank, 0,
            &self.oaddr,  &self.ocount,  &self.otype)
        self.tdisp  = 0
        self.tcount = self.ocount
        self.ttype  = self.otype

    cdef int set_compare(self, object compare, int rank) except -1:
        self._compare = message_simple(
            compare, 1, rank, 0,
            &self.caddr,  &self.ccount,  &self.ctype)

    cdef int set_result(self, object result, int rank) except -1:
        self._result = message_simple(
            result, 0, rank, 0,
            &self.raddr,  &self.rcount,  &self.rtype)

    cdef int for_get_acc(self, object origin, object result,
                         int rank, object target) except -1:
        self.for_rma(0, origin, rank, target)
        self.set_result(result, rank)
        return 0

    cdef int for_fetch_op(self, object origin, object result,
                          int rank, MPI_Aint disp) except -1:
        self.set_origin(origin, rank)
        self.set_result(result, rank)
        self.tdisp = disp
        if rank == MPI_PROC_NULL: return 0
        # Check
        if self.ocount != 1:  raise ValueError(
            "origin: expecting a single element, got %d" % self.ocount)
        if self.rcount != 1: raise ValueError(
            "result: expecting a single element, got %d" % self.rcount)
        if self.otype != self.rtype: raise ValueError(
            "mismatch in origin and result MPI datatypes")
        return 0

    cdef int for_cmp_swap(self, object origin, object compare, object result,
                          int rank, MPI_Aint disp) except -1:
        self.set_origin(origin, rank)
        self.set_compare(compare, rank)
        self.set_result(result, rank)
        self.tdisp = disp
        if rank == MPI_PROC_NULL: return 0
        # Check
        if self.ocount != 1:  raise ValueError(
            "origin: expecting a single element, got %d" % self.ocount)
        if self.ccount != 1:  raise ValueError(
            "compare: expecting a single element, got %d" % self.ccount)
        if self.rcount != 1: raise ValueError(
            "result: expecting a single element, got %d" % self.rcount)
        if self.otype != self.ctype: raise ValueError(
            "mismatch in origin and compare MPI datatypes")
        if self.otype != self.rtype: raise ValueError(
            "mismatch in origin and result MPI datatypes")
        return 0

cdef inline _p_msg_rma message_rma():
    cdef _p_msg_rma msg = _p_msg_rma.__new__(_p_msg_rma)
    return msg

#------------------------------------------------------------------------------

@cython.final
@cython.internal
cdef class _p_msg_io:

    # raw C-side data
    cdef void         *buf
    cdef int          count
    cdef MPI_Datatype dtype
    # python-side data
    cdef object _msg

    def __cinit__(self):
        self.buf   = NULL
        self.count = 0
        self.dtype = MPI_DATATYPE_NULL

    cdef int for_read(self, object msg) except -1:
        self._msg = message_simple(msg, 0, # writable
                                   0, 0,
                                   &self.buf,
                                   &self.count,
                                   &self.dtype)
        return 0

    cdef int for_write(self, object msg) except -1:
        self._msg = message_simple(msg, 1, # readonly
                                   0, 0,
                                   &self.buf,
                                   &self.count,
                                   &self.dtype)
        return 0

cdef inline _p_msg_io message_io_read(object buf):
    cdef _p_msg_io msg = _p_msg_io.__new__(_p_msg_io)
    msg.for_read(buf)
    return msg

cdef inline _p_msg_io message_io_write(object buf):
    cdef _p_msg_io msg = _p_msg_io.__new__(_p_msg_io)
    msg.for_write(buf)
    return msg

#------------------------------------------------------------------------------
