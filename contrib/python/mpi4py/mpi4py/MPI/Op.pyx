cdef class Op:

    """
    Operation object
    """

    def __cinit__(self, Op op: Optional[Op] = None):
        self.ob_mpi = MPI_OP_NULL
        if op is None: return
        self.ob_mpi = op.ob_mpi
        self.ob_func = op.ob_func
        self.ob_usrid = op.ob_usrid

    def __dealloc__(self):
        if not (self.flags & PyMPI_OWNED): return
        CHKERR( del_Op(&self.ob_mpi) )
        op_user_del(&self.ob_usrid)

    def __richcmp__(self, other, int op):
        if not isinstance(other, Op): return NotImplemented
        cdef Op s = <Op>self, o = <Op>other
        if   op == Py_EQ: return (s.ob_mpi == o.ob_mpi)
        elif op == Py_NE: return (s.ob_mpi != o.ob_mpi)
        cdef mod = type(self).__module__
        cdef cls = type(self).__name__
        raise TypeError("unorderable type: '%s.%s'" % (mod, cls))

    def __bool__(self) -> bool:
        return self.ob_mpi != MPI_OP_NULL

    def __call__(self, x: Any, y: Any) -> Any:
        if self.ob_func != NULL:
            return self.ob_func(x, y)
        else:
            return op_user_py(self.ob_usrid, x, y, None)

    @classmethod
    def Create(
        cls,
        function: Callable[[Buffer, Buffer, Datatype], None],
        bint commute: bool = False,
    ) -> Op:
        """
        Create a user-defined operation
        """
        cdef Op op = Op.__new__(Op)
        cdef MPI_User_function *cfunction = NULL
        op.ob_usrid = op_user_new(function, &cfunction)
        CHKERR( MPI_Op_create(cfunction, commute, &op.ob_mpi) )
        return op

    def Free(self) -> None:
        """
        Free the operation
        """
        CHKERR( MPI_Op_free(&self.ob_mpi) )
        op_user_del(&self.ob_usrid)
        if   self is __MAX__     : self.ob_mpi =  MPI_MAX
        elif self is __MIN__     : self.ob_mpi =  MPI_MIN
        elif self is __SUM__     : self.ob_mpi =  MPI_SUM
        elif self is __PROD__    : self.ob_mpi =  MPI_PROD
        elif self is __LAND__    : self.ob_mpi =  MPI_LAND
        elif self is __BAND__    : self.ob_mpi =  MPI_BAND
        elif self is __LOR__     : self.ob_mpi =  MPI_LOR
        elif self is __BOR__     : self.ob_mpi =  MPI_BOR
        elif self is __LXOR__    : self.ob_mpi =  MPI_LXOR
        elif self is __BXOR__    : self.ob_mpi =  MPI_BXOR
        elif self is __MAXLOC__  : self.ob_mpi =  MPI_MAXLOC
        elif self is __MINLOC__  : self.ob_mpi =  MPI_MINLOC
        elif self is __REPLACE__ : self.ob_mpi =  MPI_REPLACE
        elif self is __NO_OP__   : self.ob_mpi =  MPI_NO_OP

    # Process-local reduction
    # -----------------------

    def Is_commutative(self) -> bool:
        """
        Query reduction operations for their commutativity
        """
        cdef int flag = 0
        CHKERR( MPI_Op_commutative(self.ob_mpi, &flag) )
        return <bint>flag

    property is_commutative:
        """is commutative"""
        def __get__(self) -> bool:
            return self.Is_commutative()

    def Reduce_local(self, inbuf: BufSpec, inoutbuf: BufSpec) -> None:
        """
        Apply a reduction operator to local data
        """
        # get *in* and *inout* buffers
        cdef _p_msg_cco m = message_cco()
        m.for_cro_send(inbuf, 0)
        m.for_cro_recv(inoutbuf, 0)
        # check counts and datatypes
        if m.scount != m.rcount: raise ValueError(
            "mismatch in inbuf count %d and inoutbuf count %d" %
            (m.scount, m.rcount))
        if (m.stype != m.rtype): raise ValueError(
            "mismatch in inbuf and inoutbuf MPI datatypes")
        # do local reduction
        with nogil: CHKERR( MPI_Reduce_local(
            m.sbuf, m.rbuf, m.rcount, m.rtype, self.ob_mpi) )

    property is_predefined:
        """is a predefined operation"""
        def __get__(self) -> bool:
            cdef MPI_Op op = self.ob_mpi
            return (op == MPI_OP_NULL or
                    op == MPI_MAX or
                    op == MPI_MIN or
                    op == MPI_SUM or
                    op == MPI_PROD or
                    op == MPI_LAND or
                    op == MPI_BAND or
                    op == MPI_LOR or
                    op == MPI_BOR or
                    op == MPI_LXOR or
                    op == MPI_BXOR or
                    op == MPI_MAXLOC or
                    op == MPI_MINLOC or
                    op == MPI_REPLACE or
                    op == MPI_NO_OP)

    # Fortran Handle
    # --------------

    def py2f(self) -> int:
        """
        """
        return MPI_Op_c2f(self.ob_mpi)

    @classmethod
    def f2py(cls, arg: int) -> Op:
        """
        """
        cdef Op op = Op.__new__(Op)
        op.ob_mpi = MPI_Op_f2c(arg)
        return op



cdef Op __OP_NULL__ = new_Op( MPI_OP_NULL )
cdef Op __MAX__     = new_Op( MPI_MAX     )
cdef Op __MIN__     = new_Op( MPI_MIN     )
cdef Op __SUM__     = new_Op( MPI_SUM     )
cdef Op __PROD__    = new_Op( MPI_PROD    )
cdef Op __LAND__    = new_Op( MPI_LAND    )
cdef Op __BAND__    = new_Op( MPI_BAND    )
cdef Op __LOR__     = new_Op( MPI_LOR     )
cdef Op __BOR__     = new_Op( MPI_BOR     )
cdef Op __LXOR__    = new_Op( MPI_LXOR    )
cdef Op __BXOR__    = new_Op( MPI_BXOR    )
cdef Op __MAXLOC__  = new_Op( MPI_MAXLOC  )
cdef Op __MINLOC__  = new_Op( MPI_MINLOC  )
cdef Op __REPLACE__ = new_Op( MPI_REPLACE )
cdef Op __NO_OP__   = new_Op( MPI_NO_OP   )


# Predefined operation handles
# ----------------------------

OP_NULL = __OP_NULL__  #: Null
MAX     = __MAX__      #: Maximum
MIN     = __MIN__      #: Minimum
SUM     = __SUM__      #: Sum
PROD    = __PROD__     #: Product
LAND    = __LAND__     #: Logical and
BAND    = __BAND__     #: Bit-wise and
LOR     = __LOR__      #: Logical or
BOR     = __BOR__      #: Bit-wise or
LXOR    = __LXOR__     #: Logical xor
BXOR    = __BXOR__     #: Bit-wise xor
MAXLOC  = __MAXLOC__   #: Maximum and location
MINLOC  = __MINLOC__   #: Minimum and location
REPLACE = __REPLACE__  #: Replace (for RMA)
NO_OP   = __NO_OP__    #: No-op   (for RMA)
