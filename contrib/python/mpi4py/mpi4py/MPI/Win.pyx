# Create flavors
# --------------
WIN_FLAVOR_CREATE   = MPI_WIN_FLAVOR_CREATE
WIN_FLAVOR_ALLOCATE = MPI_WIN_FLAVOR_ALLOCATE
WIN_FLAVOR_DYNAMIC  = MPI_WIN_FLAVOR_DYNAMIC
WIN_FLAVOR_SHARED   = MPI_WIN_FLAVOR_SHARED

# Memory model
# ------------
WIN_SEPARATE = MPI_WIN_SEPARATE
WIN_UNIFIED  = MPI_WIN_UNIFIED

# Assertion modes
# ---------------
MODE_NOCHECK   = MPI_MODE_NOCHECK
MODE_NOSTORE   = MPI_MODE_NOSTORE
MODE_NOPUT     = MPI_MODE_NOPUT
MODE_NOPRECEDE = MPI_MODE_NOPRECEDE
MODE_NOSUCCEED = MPI_MODE_NOSUCCEED

# Lock types
# ----------
LOCK_EXCLUSIVE = MPI_LOCK_EXCLUSIVE
LOCK_SHARED    = MPI_LOCK_SHARED


cdef class Win:

    """
    Window handle
    """

    def __cinit__(self, Win win: Optional[Win] = None):
        self.ob_mpi = MPI_WIN_NULL
        if win is None: return
        self.ob_mpi =  win.ob_mpi
        self.ob_mem =  win.ob_mem

    def __dealloc__(self):
        if not (self.flags & PyMPI_OWNED): return
        CHKERR( del_Win(&self.ob_mpi) )

    def __richcmp__(self, other, int op):
        if not isinstance(other, Win): return NotImplemented
        cdef Win s = <Win>self, o = <Win>other
        if   op == Py_EQ: return (s.ob_mpi == o.ob_mpi)
        elif op == Py_NE: return (s.ob_mpi != o.ob_mpi)
        cdef mod = type(self).__module__
        cdef cls = type(self).__name__
        raise TypeError("unorderable type: '%s.%s'" % (mod, cls))

    def __bool__(self) -> bool:
        return self.ob_mpi != MPI_WIN_NULL

    # Window Creation
    # ---------------

    @classmethod
    def Create(
        cls,
        memory: Union[Buffer, Bottom, None],
        int disp_unit: int = 1,
        Info info: Info = INFO_NULL,
        Intracomm comm: Intracomm = COMM_SELF,
    ) -> Win:
        """
        Create an window object for one-sided communication
        """
        cdef void *base = NULL
        cdef MPI_Aint size = 0
        if is_BOTTOM(memory):
            base = MPI_BOTTOM
            memory = None
        elif memory is not None:
            memory = getbuffer_w(memory, &base, &size)
        cdef Win win = Win.__new__(Win)
        with nogil: CHKERR( MPI_Win_create(
            base, size, disp_unit,
            info.ob_mpi, comm.ob_mpi, &win.ob_mpi) )
        win_set_eh(win.ob_mpi)
        win.ob_mem = memory
        return win

    @classmethod
    def Allocate(
        cls,
        Aint size: int,
        int disp_unit: int = 1,
        Info info: Info = INFO_NULL,
        Intracomm comm: Intracomm = COMM_SELF,
    ) -> Win:
        """
        Create an window object for one-sided communication
        """
        cdef void *base = NULL
        cdef Win win = Win.__new__(Win)
        with nogil: CHKERR( MPI_Win_allocate(
            size, disp_unit, info.ob_mpi,
            comm.ob_mpi, &base, &win.ob_mpi) )
        win_set_eh(win.ob_mpi)
        return win

    @classmethod
    def Allocate_shared(
        cls,
        Aint size: int,
        int disp_unit: int = 1,
        Info info: Info = INFO_NULL,
        Intracomm comm: Intracomm = COMM_SELF,
        ) -> Win:
        """
        Create an window object for one-sided communication
        """
        cdef void *base = NULL
        cdef Win win = Win.__new__(Win)
        with nogil: CHKERR( MPI_Win_allocate_shared(
            size, disp_unit, info.ob_mpi,
            comm.ob_mpi, &base, &win.ob_mpi) )
        win_set_eh(win.ob_mpi)
        return win

    def Shared_query(self, int rank: int) -> Tuple[memory, int]:
        """
        Query the process-local address
        for remote memory segments
        created with `Win.Allocate_shared()`
        """
        cdef void *base = NULL
        cdef MPI_Aint size = 0
        cdef int disp_unit = 1
        with nogil: CHKERR( MPI_Win_shared_query(
            self.ob_mpi, rank,
            &size, &disp_unit, &base) )
        return (asbuffer(self, base, size, 0), disp_unit)

    @classmethod
    def Create_dynamic(
        cls,
        Info info: Info = INFO_NULL,
        Intracomm comm: Intracomm = COMM_SELF,
    ) -> Win:
        """
        Create an window object for one-sided communication
        """
        cdef Win win = Win.__new__(Win)
        with nogil: CHKERR( MPI_Win_create_dynamic(
            info.ob_mpi, comm.ob_mpi, &win.ob_mpi) )
        win_set_eh(win.ob_mpi)
        win.ob_mem = {}
        return win

    def Attach(self, memory: Buffer) -> None:
        """
        Attach a local memory region
        """
        cdef void *base = NULL
        cdef MPI_Aint size = 0
        memory = getbuffer_w(memory, &base, &size)
        with nogil: CHKERR( MPI_Win_attach(self.ob_mpi, base, size) )
        try: (<dict>self.ob_mem)[<MPI_Aint>base] = memory
        except: pass

    def Detach(self, memory: Buffer) -> None:
        """
        Detach a local memory region
        """
        cdef void *base = NULL
        memory = getbuffer_w(memory, &base, NULL)
        with nogil: CHKERR( MPI_Win_detach(self.ob_mpi, base) )
        try: del (<dict>self.ob_mem)[<MPI_Aint>base]
        except: pass

    def Free(self) -> None:
        """
        Free a window
        """
        with nogil: CHKERR( MPI_Win_free(&self.ob_mpi) )
        self.ob_mem = None

    # Window Info
    # -----------

    def Set_info(self, Info info: Info) -> None:
        """
        Set new values for the hints
        associated with a window
        """
        with nogil: CHKERR( MPI_Win_set_info(self.ob_mpi, info.ob_mpi) )

    def Get_info(self) -> Info:
        """
        Return the hints for a windows
        that are currently in use
        """
        cdef Info info = Info.__new__(Info)
        with nogil: CHKERR( MPI_Win_get_info( self.ob_mpi, &info.ob_mpi) )
        return info

    property info:
        """window info"""
        def __get__(self) -> Info:
            return self.Get_info()
        def __set__(self, value: Info):
            self.Set_info(value)

    # Window Group
    # -------------

    def Get_group(self) -> Group:
        """
        Return a duplicate of the group of the
        communicator used to create the window
        """
        cdef Group group = Group()
        with nogil: CHKERR( MPI_Win_get_group(self.ob_mpi, &group.ob_mpi) )
        return group

    property group:
        """window group"""
        def __get__(self) -> Group:
            return self.Get_group()

    # Window Attributes
    # -----------------

    def Get_attr(self, int keyval: int) -> Optional[Union[int, Any]]:
        """
        Retrieve attribute value by key
        """
        cdef void *attrval = NULL
        cdef int flag = 0
        CHKERR( MPI_Win_get_attr(self.ob_mpi, keyval, &attrval, &flag) )
        if flag == 0: return None
        if attrval == NULL: return 0
        # MPI-2 predefined attribute keyvals
        if keyval == MPI_WIN_BASE:
            return <MPI_Aint>attrval
        elif keyval == MPI_WIN_SIZE:
            return (<MPI_Aint*>attrval)[0]
        elif keyval == MPI_WIN_DISP_UNIT:
            return (<int*>attrval)[0]
        # MPI-3 predefined attribute keyvals
        elif keyval == MPI_WIN_CREATE_FLAVOR:
            return (<int*>attrval)[0]
        elif keyval == MPI_WIN_MODEL:
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
        CHKERR( MPI_Win_delete_attr(self.ob_mpi, keyval) )

    @classmethod
    def Create_keyval(
        cls,
        copy_fn: Optional[Callable[[Win, int, Any], Any]] = None,
        delete_fn: Optional[Callable[[Win, int, Any], None]] = None,
        nopython: bool = False,
    ) -> int:
        """
        Create a new attribute key for windows
        """
        cdef object state = _p_keyval(copy_fn, delete_fn, nopython)
        cdef int keyval = MPI_KEYVAL_INVALID
        cdef MPI_Win_copy_attr_function *_copy = PyMPI_attr_copy_fn
        cdef MPI_Win_delete_attr_function *_del = PyMPI_attr_delete_fn
        cdef void *extra_state = <void *>state
        CHKERR( MPI_Win_create_keyval(_copy, _del, &keyval, extra_state) )
        win_keyval[keyval] = state
        return keyval

    @classmethod
    def Free_keyval(cls, int keyval: int) -> int:
        """
        Free an attribute key for windows
        """
        cdef int keyval_save = keyval
        CHKERR( MPI_Win_free_keyval(&keyval) )
        try: del win_keyval[keyval_save]
        except KeyError: pass
        return keyval

    property attrs:
        "window attributes"
        def __get__(self) -> Tuple[int, int, int]:
            cdef void *base = NULL
            cdef MPI_Aint size = 0
            cdef int disp_unit = 1
            win_get_base(self.ob_mpi, &base)
            win_get_size(self.ob_mpi, &size)
            win_get_unit(self.ob_mpi, &disp_unit)
            return (<MPI_Aint>base, size, disp_unit)

    property flavor:
        """window create flavor"""
        def __get__(self) -> int:
            cdef int keyval = MPI_WIN_CREATE_FLAVOR
            cdef int *attrval = NULL
            cdef int flag = 0
            if keyval != MPI_KEYVAL_INVALID:
                CHKERR( MPI_Win_get_attr(self.ob_mpi, keyval,
                                         <void*>&attrval, &flag) )
                if flag and attrval != NULL: return attrval[0]
            return MPI_WIN_FLAVOR_CREATE

    property model:
        """window memory model"""
        def __get__(self) -> int:
            cdef int keyval = MPI_WIN_MODEL
            cdef int *attrval = NULL
            cdef int flag = 0
            if keyval != MPI_KEYVAL_INVALID:
                CHKERR( MPI_Win_get_attr(self.ob_mpi, keyval,
                                         <void*>&attrval, &flag) )
                if flag and attrval != NULL: return attrval[0]
            return MPI_WIN_SEPARATE

    def tomemory(self) -> memory:
        """
        Return window memory buffer
        """
        return getbuffer(self, 0, 1)

    # buffer interface (PEP 3118)

    def __getbuffer__(self, Py_buffer *view, int flags):
        if view.obj == Py_None: Py_CLEAR(view.obj)
        cdef void *base = NULL
        cdef MPI_Aint size = 0
        win_get_base(self.ob_mpi, &base)
        win_get_size(self.ob_mpi, &size)
        PyBuffer_FillInfo(view, self, base, size, 0, flags)

    # buffer interface (legacy)

    def __getsegcount__(self, Py_ssize_t *lenp):
        if lenp == NULL: return 1
        cdef MPI_Aint size = 0
        win_get_size(self.ob_mpi, &size)
        lenp[0] = <Py_ssize_t>size
        return 1

    def __getreadbuffer__(self, Py_ssize_t idx, void **bufp):
        if idx != 0:
            raise SystemError("accessing non-existent buffer segment")
        cdef MPI_Aint size = 0
        win_get_base(self.ob_mpi, bufp)
        win_get_size(self.ob_mpi, &size)
        return <Py_ssize_t>size

    def __getwritebuffer__(self, Py_ssize_t idx, void **bufp):
        if idx != 0:
            raise SystemError("accessing non-existent buffer segment")
        cdef MPI_Aint size = 0
        win_get_base(self.ob_mpi, bufp)
        win_get_size(self.ob_mpi, &size)
        return <Py_ssize_t>size

    # Communication Operations
    # ------------------------

    def Put(
        self,
        origin: BufSpec,
        int target_rank: int,
        target: Optional[TargetSpec] = None,
    ) -> None:
        """
        Put data into a memory window on a remote process.
        """
        cdef _p_msg_rma msg = message_rma()
        msg.for_put(origin, target_rank, target)
        with nogil: CHKERR( MPI_Put(
            msg.oaddr, msg.ocount, msg.otype,
            target_rank,
            msg.tdisp, msg.tcount, msg.ttype,
            self.ob_mpi) )

    def Get(
        self,
        origin: BufSpec,
        int target_rank: int,
        target: Optional[TargetSpec] = None,
    ) -> None:
        """
        Get data from a memory window on a remote process.
        """
        cdef _p_msg_rma msg = message_rma()
        msg.for_get(origin, target_rank, target)
        with nogil: CHKERR( MPI_Get(
            msg.oaddr, msg.ocount, msg.otype,
            target_rank,
            msg.tdisp, msg.tcount, msg.ttype,
            self.ob_mpi) )

    def Accumulate(
        self,
        origin: BufSpec,
        int target_rank: int,
        target: Optional[TargetSpec] = None,
        Op op: Op = SUM,
    ) -> None:
        """
        Accumulate data into the target process
        """
        cdef _p_msg_rma msg = message_rma()
        msg.for_acc(origin, target_rank, target)
        with nogil: CHKERR( MPI_Accumulate(
            msg.oaddr, msg.ocount, msg.otype,
            target_rank,
            msg.tdisp, msg.tcount, msg.ttype,
            op.ob_mpi, self.ob_mpi) )

    def Get_accumulate(
        self,
        origin: BufSpec,
        result: BufSpec,
        int target_rank: int,
        target: Optional[TargetSpec] = None,
        Op op: Op = SUM,
    ) -> None:
        """
        Fetch-and-accumulate data into the target process
        """
        cdef _p_msg_rma msg = message_rma()
        msg.for_get_acc(origin, result, target_rank, target)
        with nogil: CHKERR( MPI_Get_accumulate(
            msg.oaddr, msg.ocount, msg.otype,
            msg.raddr, msg.rcount, msg.rtype,
            target_rank,
            msg.tdisp, msg.tcount, msg.ttype,
            op.ob_mpi, self.ob_mpi) )

    def Fetch_and_op(
        self,
        origin: BufSpec,
        result: BufSpec,
        int target_rank: int,
        Aint target_disp: int = 0,
        Op op: Op = SUM,
    ) -> None:
        """
        Perform one-sided read-modify-write
        """
        cdef _p_msg_rma msg = message_rma()
        msg.for_fetch_op(origin, result, target_rank, target_disp)
        with nogil: CHKERR( MPI_Fetch_and_op(
                msg.oaddr, msg.raddr, msg.ttype,
                target_rank, target_disp,
                op.ob_mpi, self.ob_mpi) )

    def Compare_and_swap(
        self,
        origin: BufSpec,
        compare: BufSpec,
        result: BufSpec,
        int target_rank: int,
        Aint target_disp: int = 0,
    ) -> None:
        """
        Perform one-sided atomic compare-and-swap
        """
        cdef _p_msg_rma msg = message_rma()
        msg.for_cmp_swap(origin, compare, result, target_rank, target_disp)
        with nogil: CHKERR( MPI_Compare_and_swap(
                msg.oaddr, msg.caddr, msg.raddr, msg.ttype,
                target_rank, target_disp, self.ob_mpi) )

    # Request-based RMA Communication Operations
    # ------------------------------------------

    def Rput(
        self,
        origin: BufSpec,
        int target_rank: int,
        target: Optional[TargetSpec] = None,
    ) -> Request:
        """
        Put data into a memory window on a remote process.
        """
        cdef _p_msg_rma msg = message_rma()
        msg.for_put(origin, target_rank, target)
        cdef Request request = Request.__new__(Request)
        with nogil: CHKERR( MPI_Rput(
            msg.oaddr, msg.ocount, msg.otype,
            target_rank,
            msg.tdisp, msg.tcount, msg.ttype,
            self.ob_mpi, &request.ob_mpi) )
        request.ob_buf = msg
        return request

    def Rget(
        self,
        origin: BufSpec,
        int target_rank: int,
        target: Optional[TargetSpec] = None,
    ) -> Request:
        """
        Get data from a memory window on a remote process.
        """
        cdef _p_msg_rma msg = message_rma()
        msg.for_get(origin, target_rank, target)
        cdef Request request = Request.__new__(Request)
        with nogil: CHKERR( MPI_Rget(
            msg.oaddr, msg.ocount, msg.otype,
            target_rank,
            msg.tdisp, msg.tcount, msg.ttype,
            self.ob_mpi, &request.ob_mpi) )
        request.ob_buf = msg
        return request

    def Raccumulate(
        self,
        origin: BufSpec,
        int target_rank: int,
        target: Optional[TargetSpec] = None,
        Op op: Op = SUM,
    ) -> Request:
        """
        Fetch-and-accumulate data into the target process
        """
        cdef _p_msg_rma msg = message_rma()
        msg.for_acc(origin, target_rank, target)
        cdef Request request = Request.__new__(Request)
        with nogil: CHKERR( MPI_Raccumulate(
            msg.oaddr, msg.ocount, msg.otype,
            target_rank,
            msg.tdisp, msg.tcount, msg.ttype,
            op.ob_mpi, self.ob_mpi, &request.ob_mpi) )
        request.ob_buf = msg
        return request

    def Rget_accumulate(
        self,
        origin: BufSpec,
        result: BufSpec,
        int target_rank: int,
        target: Optional[TargetSpec] = None,
        Op op: Op = SUM,
    ) -> Request:
        """
        Accumulate data into the target process
        using remote memory access.
        """
        cdef _p_msg_rma msg = message_rma()
        msg.for_get_acc(origin, result, target_rank, target)
        cdef Request request = Request.__new__(Request)
        with nogil: CHKERR( MPI_Rget_accumulate(
            msg.oaddr, msg.ocount, msg.otype,
            msg.raddr, msg.rcount, msg.rtype,
            target_rank,
            msg.tdisp, msg.tcount, msg.ttype,
            op.ob_mpi, self.ob_mpi, &request.ob_mpi) )
        request.ob_buf = msg
        return request

    # Synchronization Calls
    # ---------------------

    # Fence
    # -----

    def Fence(self, int assertion: int = 0) -> None:
        """
        Perform an MPI fence synchronization on a window
        """
        with nogil: CHKERR( MPI_Win_fence(assertion, self.ob_mpi) )

    # General Active Target Synchronization
    # -------------------------------------

    def Start(self, Group group: Group, int assertion: int = 0) -> None:
        """
        Start an RMA access epoch for MPI
        """
        with nogil: CHKERR( MPI_Win_start(
            group.ob_mpi, assertion, self.ob_mpi) )

    def Complete(self) -> None:
        """
        Completes an RMA operations begun after an `Win.Start()`
        """
        with nogil: CHKERR( MPI_Win_complete(self.ob_mpi) )

    def Post(self, Group group: Group, int assertion: int = 0) -> None:
        """
        Start an RMA exposure epoch
        """
        with nogil: CHKERR( MPI_Win_post(
            group.ob_mpi, assertion, self.ob_mpi) )

    def Wait(self) -> Literal[True]:
        """
        Complete an RMA exposure epoch begun with `Win.Post()`
        """
        with nogil: CHKERR( MPI_Win_wait(self.ob_mpi) )
        return True

    def Test(self) -> bool:
        """
        Test whether an RMA exposure epoch has completed
        """
        cdef int flag = 0
        with nogil: CHKERR( MPI_Win_test(self.ob_mpi, &flag) )
        return <bint>flag

    # Lock
    # ----

    def Lock(
        self,
        int rank: int,
        int lock_type: int = LOCK_EXCLUSIVE,
        int assertion: int = 0,
    ) -> None:
        """
        Begin an RMA access epoch at the target process
        """
        with nogil: CHKERR( MPI_Win_lock(
            lock_type, rank, assertion, self.ob_mpi) )

    def Unlock(self, int rank: int) -> None:
        """
        Complete an RMA access epoch at the target process
        """
        with nogil: CHKERR( MPI_Win_unlock(rank, self.ob_mpi) )

    def Lock_all(self, int assertion: int = 0) -> None:
        """
        Begin an RMA access epoch at all processes
        """
        with nogil: CHKERR( MPI_Win_lock_all(assertion, self.ob_mpi) )

    def Unlock_all(self) -> None:
        """
        Complete an RMA access epoch at all processes
        """
        with nogil: CHKERR( MPI_Win_unlock_all(self.ob_mpi) )

    # Flush and Sync
    # --------------

    def Flush(self, int rank: int) -> None:
        """
        Complete all outstanding RMA operations at the given target
        """
        with nogil: CHKERR( MPI_Win_flush(rank, self.ob_mpi) )

    def Flush_all(self) -> None:
        """
        Complete  all  outstanding RMA operations at all targets
        """
        with nogil: CHKERR( MPI_Win_flush_all(self.ob_mpi) )

    def Flush_local(self, int rank: int) -> None:
        """
        Complete locally all outstanding RMA operations at the given target
        """
        with nogil: CHKERR( MPI_Win_flush_local(rank, self.ob_mpi) )

    def Flush_local_all(self) -> None:
        """
        Complete locally all outstanding RMA opera- tions at all targets
        """
        with nogil: CHKERR( MPI_Win_flush_local_all(self.ob_mpi) )

    def Sync(self) -> None:
        """
        Synchronize public and private copies of the given window
        """
        with nogil: CHKERR( MPI_Win_sync(self.ob_mpi) )


    # Error Handling
    # --------------

    def Get_errhandler(self) -> Errhandler:
        """
        Get the error handler for a window
        """
        cdef Errhandler errhandler = Errhandler.__new__(Errhandler)
        CHKERR( MPI_Win_get_errhandler(self.ob_mpi, &errhandler.ob_mpi) )
        return errhandler

    def Set_errhandler(self, Errhandler errhandler: Errhandler) -> None:
        """
        Set the error handler for a window
        """
        CHKERR( MPI_Win_set_errhandler(self.ob_mpi, errhandler.ob_mpi) )

    def Call_errhandler(self, int errorcode: int) -> None:
        """
        Call the error handler installed on a window
        """
        CHKERR( MPI_Win_call_errhandler(self.ob_mpi, errorcode) )


    # Naming Objects
    # --------------

    def Get_name(self) -> str:
        """
        Get the print name associated with the window
        """
        cdef char name[MPI_MAX_OBJECT_NAME+1]
        cdef int nlen = 0
        CHKERR( MPI_Win_get_name(self.ob_mpi, name, &nlen) )
        return tompistr(name, nlen)

    def Set_name(self, name: str) -> None:
        """
        Set the print name associated with the window
        """
        cdef char *cname = NULL
        name = asmpistr(name, &cname)
        CHKERR( MPI_Win_set_name(self.ob_mpi, cname) )

    property name:
        """window name"""
        def __get__(self) -> str:
            return self.Get_name()
        def __set__(self, value: str):
            self.Set_name(value)

    # Fortran Handle
    # --------------

    def py2f(self) -> int:
        """
        """
        return MPI_Win_c2f(self.ob_mpi)

    @classmethod
    def f2py(cls, arg: int) -> Win:
        """
        """
        cdef Win win = Win.__new__(Win)
        win.ob_mpi = MPI_Win_f2c(arg)
        return win



cdef Win __WIN_NULL__ = new_Win(MPI_WIN_NULL)


# Predefined window handles
# -------------------------

WIN_NULL = __WIN_NULL__  #: Null window handle
