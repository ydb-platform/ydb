# Storage order for arrays
# ------------------------
ORDER_C       = MPI_ORDER_C       #: C order (a.k.a. row major)
ORDER_FORTRAN = MPI_ORDER_FORTRAN #: Fortran order (a.k.a. column major)
ORDER_F       = MPI_ORDER_FORTRAN #: Convenience alias for ORDER_FORTRAN


# Type classes for Fortran datatype matching
# ------------------------------------------
TYPECLASS_INTEGER = MPI_TYPECLASS_INTEGER
TYPECLASS_REAL    = MPI_TYPECLASS_REAL
TYPECLASS_COMPLEX = MPI_TYPECLASS_COMPLEX


# Type of distributions (HPF-like arrays)
# ---------------------------------------
DISTRIBUTE_NONE      = MPI_DISTRIBUTE_NONE      #: Dimension not distributed
DISTRIBUTE_BLOCK     = MPI_DISTRIBUTE_BLOCK     #: Block distribution
DISTRIBUTE_CYCLIC    = MPI_DISTRIBUTE_CYCLIC    #: Cyclic distribution
DISTRIBUTE_DFLT_DARG = MPI_DISTRIBUTE_DFLT_DARG #: Default distribution


# Combiner values for datatype decoding
# -------------------------------------
COMBINER_NAMED            = MPI_COMBINER_NAMED
COMBINER_DUP              = MPI_COMBINER_DUP
COMBINER_CONTIGUOUS       = MPI_COMBINER_CONTIGUOUS
COMBINER_VECTOR           = MPI_COMBINER_VECTOR
COMBINER_HVECTOR          = MPI_COMBINER_HVECTOR
COMBINER_INDEXED          = MPI_COMBINER_INDEXED
COMBINER_HINDEXED         = MPI_COMBINER_HINDEXED
COMBINER_INDEXED_BLOCK    = MPI_COMBINER_INDEXED_BLOCK
COMBINER_HINDEXED_BLOCK   = MPI_COMBINER_HINDEXED_BLOCK
COMBINER_STRUCT           = MPI_COMBINER_STRUCT
COMBINER_SUBARRAY         = MPI_COMBINER_SUBARRAY
COMBINER_DARRAY           = MPI_COMBINER_DARRAY
COMBINER_RESIZED          = MPI_COMBINER_RESIZED
COMBINER_F90_REAL         = MPI_COMBINER_F90_REAL
COMBINER_F90_COMPLEX      = MPI_COMBINER_F90_COMPLEX
COMBINER_F90_INTEGER      = MPI_COMBINER_F90_INTEGER


cdef class Datatype:

    """
    Datatype object
    """

    def __cinit__(self, Datatype datatype: Optional[Datatype] = None):
        self.ob_mpi = MPI_DATATYPE_NULL
        if datatype is None: return
        self.ob_mpi = datatype.ob_mpi

    def __dealloc__(self):
        if not (self.flags & PyMPI_OWNED): return
        CHKERR( del_Datatype(&self.ob_mpi) )

    def __richcmp__(self, other, int op):
        if not isinstance(other, Datatype): return NotImplemented
        cdef Datatype s = <Datatype>self, o = <Datatype>other
        if   op == Py_EQ: return (s.ob_mpi == o.ob_mpi)
        elif op == Py_NE: return (s.ob_mpi != o.ob_mpi)
        cdef mod = type(self).__module__
        cdef cls = type(self).__name__
        raise TypeError("unorderable type: '%s.%s'" % (mod, cls))

    def __bool__(self) -> bool:
        return self.ob_mpi != MPI_DATATYPE_NULL

    # Datatype Accessors
    # ------------------

    def Get_size(self) -> int:
        """
        Return the number of bytes occupied
        by entries in the datatype
        """
        cdef MPI_Count size = 0
        CHKERR( MPI_Type_size_x(self.ob_mpi, &size) )
        return size

    property size:
        """size (in bytes)"""
        def __get__(self) -> int:
            cdef MPI_Count size = 0
            CHKERR( MPI_Type_size_x(self.ob_mpi, &size) )
            return size

    def Get_extent(self) -> Tuple[int, int]:
        """
        Return lower bound and extent of datatype
        """
        cdef MPI_Count lb = 0, extent = 0
        CHKERR( MPI_Type_get_extent_x(self.ob_mpi, &lb, &extent) )
        return (lb, extent)

    property extent:
        """extent"""
        def __get__(self) -> int:
            cdef MPI_Count lb = 0, extent = 0
            CHKERR( MPI_Type_get_extent_x(self.ob_mpi, &lb, &extent) )
            return extent

    property lb:
        """lower bound"""
        def __get__(self) -> int:
            cdef MPI_Count lb = 0, extent = 0
            CHKERR( MPI_Type_get_extent_x(self.ob_mpi, &lb, &extent) )
            return lb

    property ub:
        """upper bound"""
        def __get__(self) -> int:
            cdef MPI_Count lb = 0, extent = 0
            CHKERR( MPI_Type_get_extent_x(self.ob_mpi, &lb, &extent) )
            return lb + extent

    # Datatype Constructors
    # ---------------------

    def Dup(self) -> Datatype:
        """
        Duplicate a datatype
        """
        cdef Datatype datatype = Datatype.__new__(Datatype)
        CHKERR( MPI_Type_dup(self.ob_mpi, &datatype.ob_mpi) )
        return datatype

    Create_dup = Dup #: convenience alias

    def Create_contiguous(self, int count: int) -> Datatype:
        """
        Create a contiguous datatype
        """
        cdef Datatype datatype = Datatype.__new__(Datatype)
        CHKERR( MPI_Type_contiguous(count, self.ob_mpi,
                                    &datatype.ob_mpi) )
        return datatype

    def Create_vector(
        self,
        int count: int,
        int blocklength: int,
        int stride: int,
    ) -> Datatype:
        """
        Create a vector (strided) datatype
        """
        cdef Datatype datatype = Datatype.__new__(Datatype)
        CHKERR( MPI_Type_vector(count, blocklength, stride,
                                self.ob_mpi, &datatype.ob_mpi) )
        return datatype

    def Create_hvector(
        self,
        int count: int,
        int blocklength: int,
        Aint stride: int,
    ) -> Datatype:
        """
        Create a vector (strided) datatype
        """
        cdef Datatype datatype = Datatype.__new__(Datatype)
        CHKERR( MPI_Type_create_hvector(count, blocklength, stride,
                                        self.ob_mpi,
                                        &datatype.ob_mpi) )
        return datatype

    def Create_indexed(
        self,
        blocklengths: Sequence[int],
        displacements: Sequence[int],
    ) -> Datatype:
        """
        Create an indexed datatype
        """
        cdef int count = 0, *iblen = NULL, *idisp = NULL
        blocklengths  = getarray(blocklengths,  &count, &iblen)
        displacements = chkarray(displacements,  count, &idisp)
        #
        cdef Datatype datatype = Datatype.__new__(Datatype)
        CHKERR( MPI_Type_indexed(count, iblen, idisp,
                                 self.ob_mpi, &datatype.ob_mpi) )
        return datatype

    def Create_hindexed(
        self,
        blocklengths: Sequence[int],
        displacements: Sequence[int],
    ) -> Datatype:
        """
        Create an indexed datatype
        with displacements in bytes
        """
        cdef int count = 0, *iblen = NULL
        blocklengths = getarray(blocklengths, &count, &iblen)
        cdef MPI_Aint *idisp = NULL
        displacements = chkarray(displacements, count, &idisp)
        #
        cdef Datatype datatype = Datatype.__new__(Datatype)
        CHKERR( MPI_Type_create_hindexed(count, iblen, idisp,
                                         self.ob_mpi,
                                         &datatype.ob_mpi) )
        return datatype

    def Create_indexed_block(
        self,
        int blocklength: int,
        displacements: Sequence[int],
    ) -> Datatype:
        """
        Create an indexed datatype
        with constant-sized blocks
        """
        cdef int count = 0, *idisp = NULL
        displacements = getarray(displacements, &count, &idisp)
        #
        cdef Datatype datatype = Datatype.__new__(Datatype)
        CHKERR( MPI_Type_create_indexed_block(count, blocklength,
                                              idisp, self.ob_mpi,
                                              &datatype.ob_mpi) )
        return datatype

    def Create_hindexed_block(
        self,
        int blocklength: int,
        displacements: Sequence[int],
    ) -> Datatype:
        """
        Create an indexed datatype
        with constant-sized blocks
        and displacements in bytes
        """
        cdef int count = 0
        cdef MPI_Aint *idisp = NULL
        displacements = getarray(displacements, &count, &idisp)
        #
        cdef Datatype datatype = Datatype.__new__(Datatype)
        CHKERR( MPI_Type_create_hindexed_block(count, blocklength,
                                               idisp, self.ob_mpi,
                                               &datatype.ob_mpi) )
        return datatype

    @classmethod
    def Create_struct(
        cls,
        blocklengths: Sequence[int],
        displacements: Sequence[int],
        datatypes: Sequence[Datatype],
    ) -> Datatype:
        """
        Create an datatype from a general set of
        block sizes, displacements and datatypes
        """
        cdef int count = 0, *iblen = NULL
        blocklengths = getarray(blocklengths, &count, &iblen)
        cdef MPI_Aint *idisp = NULL
        displacements = chkarray(displacements, count, &idisp)
        cdef MPI_Datatype *ptype = NULL
        datatypes = asarray_Datatype(datatypes, count, &ptype)
        #
        cdef Datatype datatype = Datatype.__new__(Datatype)
        CHKERR( MPI_Type_create_struct(count, iblen, idisp, ptype,
                                       &datatype.ob_mpi) )
        return datatype

    # Subarray Datatype Constructor
    # -----------------------------

    def Create_subarray(
        self,
        sizes: Sequence[int],
        subsizes: Sequence[int],
        starts: Sequence[int],
        int order: int = ORDER_C,
    ) -> Datatype:
        """
        Create a datatype for a subarray of
        a regular, multidimensional array
        """
        cdef int ndims = 0, *isizes = NULL
        cdef int *isubsizes = NULL, *istarts = NULL
        sizes    = getarray(sizes,   &ndims, &isizes   )
        subsizes = chkarray(subsizes, ndims, &isubsizes)
        starts   = chkarray(starts,   ndims, &istarts  )
        #
        cdef Datatype datatype = Datatype.__new__(Datatype)
        CHKERR( MPI_Type_create_subarray(ndims, isizes,
                                         isubsizes, istarts,
                                         order, self.ob_mpi,
                                         &datatype.ob_mpi) )
        return datatype

    # Distributed Array Datatype Constructor
    # --------------------------------------

    def Create_darray(
        self,
        int size: int,
        int rank: int,
        gsizes: Sequence[int],
        distribs: Sequence[int],
        dargs: Sequence[int],
        psizes: Sequence[int],
        int order: int = ORDER_C,
    ) -> Datatype:
        """
        Create a datatype representing an HPF-like
        distributed array on Cartesian process grids
        """
        cdef int ndims = 0, *igsizes = NULL
        cdef int *idistribs = NULL, *idargs = NULL, *ipsizes = NULL
        gsizes   = getarray(gsizes,  &ndims, &igsizes   )
        distribs = chkarray(distribs, ndims, &idistribs )
        dargs    = chkarray(dargs,    ndims, &idargs    )
        psizes   = chkarray(psizes,   ndims, &ipsizes   )
        #
        cdef Datatype datatype = Datatype.__new__(Datatype)
        CHKERR( MPI_Type_create_darray(size, rank, ndims, igsizes,
                                       idistribs, idargs, ipsizes,
                                       order, self.ob_mpi,
                                       &datatype.ob_mpi) )
        return datatype

    # Parametrized and size-specific Fortran Datatypes
    # ------------------------------------------------

    @classmethod
    def Create_f90_integer(cls, int r: int) -> Datatype:
        """
        Return a bounded integer datatype
        """
        cdef Datatype datatype = Datatype.__new__(Datatype)
        CHKERR( MPI_Type_create_f90_integer(r, &datatype.ob_mpi) )
        return datatype

    @classmethod
    def Create_f90_real(cls, int p: int, int r: int) -> Datatype:
        """
        Return a bounded real datatype
        """
        cdef Datatype datatype = Datatype.__new__(Datatype)
        CHKERR( MPI_Type_create_f90_real(p, r, &datatype.ob_mpi) )
        return datatype

    @classmethod
    def Create_f90_complex(cls, int p: int, int r: int) -> Datatype:
        """
        Return a bounded complex datatype
        """
        cdef Datatype datatype = Datatype.__new__(Datatype)
        CHKERR( MPI_Type_create_f90_complex(p, r, &datatype.ob_mpi) )
        return datatype

    @classmethod
    def Match_size(cls, int typeclass: int, int size: int) -> Datatype:
        """
        Find a datatype matching a specified size in bytes
        """
        cdef Datatype datatype = Datatype.__new__(Datatype)
        CHKERR( MPI_Type_match_size(typeclass, size, &datatype.ob_mpi) )
        return datatype

    # Use of Derived Datatypes
    # ------------------------

    def Commit(self) -> Datatype:
        """
        Commit the datatype
        """
        CHKERR( MPI_Type_commit(&self.ob_mpi) )
        return self

    def Free(self) -> None:
        """
        Free the datatype
        """
        CHKERR( MPI_Type_free(&self.ob_mpi) )
        cdef Datatype t = self
        cdef MPI_Datatype p = MPI_DATATYPE_NULL
        if   t is __UB__                     : p = MPI_UB
        elif t is __LB__                     : p = MPI_LB
        elif t is __PACKED__                 : p = MPI_PACKED
        elif t is __BYTE__                   : p = MPI_BYTE
        elif t is __AINT__                   : p = MPI_AINT
        elif t is __OFFSET__                 : p = MPI_OFFSET
        elif t is __COUNT__                  : p = MPI_COUNT
        elif t is __CHAR__                   : p = MPI_CHAR
        elif t is __WCHAR__                  : p = MPI_WCHAR
        elif t is __SIGNED_CHAR__            : p = MPI_SIGNED_CHAR
        elif t is __SHORT__                  : p = MPI_SHORT
        elif t is __INT__                    : p = MPI_INT
        elif t is __LONG__                   : p = MPI_LONG
        elif t is __LONG_LONG__              : p = MPI_LONG_LONG
        elif t is __UNSIGNED_CHAR__          : p = MPI_UNSIGNED_CHAR
        elif t is __UNSIGNED_SHORT__         : p = MPI_UNSIGNED_SHORT
        elif t is __UNSIGNED__               : p = MPI_UNSIGNED
        elif t is __UNSIGNED_LONG__          : p = MPI_UNSIGNED_LONG
        elif t is __UNSIGNED_LONG_LONG__     : p = MPI_UNSIGNED_LONG_LONG
        elif t is __FLOAT__                  : p = MPI_FLOAT
        elif t is __DOUBLE__                 : p = MPI_DOUBLE
        elif t is __LONG_DOUBLE__            : p = MPI_LONG_DOUBLE
        elif t is __C_BOOL__                 : p = MPI_C_BOOL
        elif t is __INT8_T__                 : p = MPI_INT8_T
        elif t is __INT16_T__                : p = MPI_INT16_T
        elif t is __INT32_T__                : p = MPI_INT32_T
        elif t is __INT64_T__                : p = MPI_INT64_T
        elif t is __UINT8_T__                : p = MPI_UINT8_T
        elif t is __UINT16_T__               : p = MPI_UINT16_T
        elif t is __UINT32_T__               : p = MPI_UINT32_T
        elif t is __UINT64_T__               : p = MPI_UINT64_T
        elif t is __C_COMPLEX__              : p = MPI_C_COMPLEX
        elif t is __C_FLOAT_COMPLEX__        : p = MPI_C_FLOAT_COMPLEX
        elif t is __C_DOUBLE_COMPLEX__       : p = MPI_C_DOUBLE_COMPLEX
        elif t is __C_LONG_DOUBLE_COMPLEX__  : p = MPI_C_LONG_DOUBLE_COMPLEX
        elif t is __CXX_BOOL__               : p = MPI_CXX_BOOL
        elif t is __CXX_FLOAT_COMPLEX__      : p = MPI_CXX_FLOAT_COMPLEX
        elif t is __CXX_DOUBLE_COMPLEX__     : p = MPI_CXX_DOUBLE_COMPLEX
        elif t is __CXX_LONG_DOUBLE_COMPLEX__: p = MPI_CXX_LONG_DOUBLE_COMPLEX
        elif t is __SHORT_INT__              : p = MPI_SHORT_INT
        elif t is __TWOINT__                 : p = MPI_2INT
        elif t is __LONG_INT__               : p = MPI_LONG_INT
        elif t is __FLOAT_INT__              : p = MPI_FLOAT_INT
        elif t is __DOUBLE_INT__             : p = MPI_DOUBLE_INT
        elif t is __LONG_DOUBLE_INT__        : p = MPI_LONG_DOUBLE_INT
        elif t is __CHARACTER__              : p = MPI_CHARACTER
        elif t is __LOGICAL__                : p = MPI_LOGICAL
        elif t is __INTEGER__                : p = MPI_INTEGER
        elif t is __REAL__                   : p = MPI_REAL
        elif t is __DOUBLE_PRECISION__       : p = MPI_DOUBLE_PRECISION
        elif t is __COMPLEX__                : p = MPI_COMPLEX
        elif t is __DOUBLE_COMPLEX__         : p = MPI_DOUBLE_COMPLEX
        elif t is __LOGICAL1__               : p = MPI_LOGICAL1
        elif t is __LOGICAL2__               : p = MPI_LOGICAL2
        elif t is __LOGICAL4__               : p = MPI_LOGICAL4
        elif t is __LOGICAL8__               : p = MPI_LOGICAL8
        elif t is __INTEGER1__               : p = MPI_INTEGER1
        elif t is __INTEGER2__               : p = MPI_INTEGER2
        elif t is __INTEGER4__               : p = MPI_INTEGER4
        elif t is __INTEGER8__               : p = MPI_INTEGER8
        elif t is __INTEGER16__              : p = MPI_INTEGER16
        elif t is __REAL2__                  : p = MPI_REAL2
        elif t is __REAL4__                  : p = MPI_REAL4
        elif t is __REAL8__                  : p = MPI_REAL8
        elif t is __REAL16__                 : p = MPI_REAL16
        elif t is __COMPLEX4__               : p = MPI_COMPLEX4
        elif t is __COMPLEX8__               : p = MPI_COMPLEX8
        elif t is __COMPLEX16__              : p = MPI_COMPLEX16
        elif t is __COMPLEX32__              : p = MPI_COMPLEX32
        self.ob_mpi = p

    # Datatype Resizing
    # -----------------

    def Create_resized(self, Aint lb: int, Aint extent: int) -> Datatype:
        """
        Create a datatype with a new lower bound and extent
        """
        cdef Datatype datatype = Datatype.__new__(Datatype)
        CHKERR( MPI_Type_create_resized(self.ob_mpi,
                                        lb, extent,
                                        &datatype.ob_mpi) )
        return datatype

    Resized = Create_resized #: compatibility alias

    def Get_true_extent(self) -> Tuple[int, int]:
        """
        Return the true lower bound and extent of a datatype
        """
        cdef MPI_Count lb = 0, extent = 0
        CHKERR( MPI_Type_get_true_extent_x(self.ob_mpi,
                                           &lb, &extent) )
        return (lb, extent)

    property true_extent:
        """true extent"""
        def __get__(self) -> int:
            cdef MPI_Count lb = 0, extent = 0
            CHKERR( MPI_Type_get_true_extent_x(self.ob_mpi,
                                               &lb, &extent) )
            return extent

    property true_lb:
        """true lower bound"""
        def __get__(self) -> int:
            cdef MPI_Count lb = 0, extent = 0
            CHKERR( MPI_Type_get_true_extent_x(self.ob_mpi,
                                               &lb, &extent) )
            return lb

    property true_ub:
        """true upper bound"""
        def __get__(self) -> int:
            cdef MPI_Count lb = 0, extent = 0
            CHKERR( MPI_Type_get_true_extent_x(self.ob_mpi,
                                               &lb, &extent) )
            return lb + extent

    # Decoding a Datatype
    # -------------------

    def Get_envelope(self) -> Tuple[int, int, int, int]:
        """
        Return information on the number and type of input arguments
        used in the call that created a datatype
        """
        cdef int ni = 0, na = 0, nd = 0, combiner = MPI_UNDEFINED
        CHKERR( MPI_Type_get_envelope(self.ob_mpi, &ni, &na, &nd, &combiner) )
        return (ni, na, nd, combiner)

    property envelope:
        """datatype envelope"""
        def __get__(self) -> Tuple[int, int, int, int]:
            return self.Get_envelope()

    def Get_contents(self) -> Tuple[List[int], List[int], List[Datatype]]:
        """
        Retrieve the actual arguments used in the call that created a
        datatype
        """
        cdef int ni = 0, na = 0, nd = 0, combiner = MPI_UNDEFINED
        CHKERR( MPI_Type_get_envelope(self.ob_mpi, &ni, &na, &nd, &combiner) )
        cdef int *i = NULL
        cdef MPI_Aint *a = NULL
        cdef MPI_Datatype *d = NULL
        cdef tmp1 = allocate(ni, sizeof(int), &i)
        cdef tmp2 = allocate(na, sizeof(MPI_Aint), &a)
        cdef tmp3 = allocate(nd, sizeof(MPI_Datatype), &d)
        CHKERR( MPI_Type_get_contents(self.ob_mpi, ni, na, nd, i, a, d) )
        cdef int k = 0
        cdef object integers  = [i[k] for k from 0 <= k < ni]
        cdef object addresses = [a[k] for k from 0 <= k < na]
        cdef object datatypes = [ref_Datatype(d[k]) for k from 0 <= k < nd]
        return (integers, addresses, datatypes)

    property contents:
        """datatype contents"""
        def __get__(self) -> Tuple[List[int], List[int], List[Datatype]]:
            return self.Get_contents()

    def decode(self) -> Tuple[Datatype, str, Dict[str, Any]]:
        """
        Convenience method for decoding a datatype
        """
        # get the datatype envelope
        cdef int ni = 0, na = 0, nd = 0, combiner = MPI_UNDEFINED
        CHKERR( MPI_Type_get_envelope(self.ob_mpi, &ni, &na, &nd, &combiner) )
        # return self immediately for named datatypes
        if combiner == MPI_COMBINER_NAMED: return self
        # get the datatype contents
        cdef int *i = NULL
        cdef MPI_Aint *a = NULL
        cdef MPI_Datatype *d = NULL
        cdef tmp1 = allocate(ni, sizeof(int), &i)
        cdef tmp2 = allocate(na, sizeof(MPI_Aint), &a)
        cdef tmp3 = allocate(nd, sizeof(MPI_Datatype), &d)
        CHKERR( MPI_Type_get_contents(self.ob_mpi, ni, na, nd, i, a, d) )
        # manage in advance the contained datatypes
        cdef int k = 0, s1, e1, s2, e2, s3, e3, s4, e4
        cdef object oldtype = None
        if combiner == MPI_COMBINER_STRUCT:
            oldtype = [ref_Datatype(d[k]) for k from 0 <= k < nd]
        elif (combiner != MPI_COMBINER_F90_INTEGER and
              combiner != MPI_COMBINER_F90_REAL and
              combiner != MPI_COMBINER_F90_COMPLEX):
            oldtype = ref_Datatype(d[0])
        # dispatch depending on the combiner value
        if combiner == MPI_COMBINER_DUP:
            return (oldtype, ('DUP'), {})
        elif combiner == MPI_COMBINER_CONTIGUOUS:
            return (oldtype, ('CONTIGUOUS'),
                    {('count') : i[0]})
        elif combiner == MPI_COMBINER_VECTOR:
            return (oldtype, ('VECTOR'),
                    {('count')       : i[0],
                     ('blocklength') : i[1],
                     ('stride')      : i[2]})
        elif combiner == MPI_COMBINER_HVECTOR:
            return (oldtype, ('HVECTOR'),
                    {('count')       : i[0],
                     ('blocklength') : i[1],
                     ('stride')      : a[0]})
        elif combiner == MPI_COMBINER_INDEXED:
            s1 =      1; e1 =   i[0]
            s2 = i[0]+1; e2 = 2*i[0]
            return (oldtype, ('INDEXED'),
                    {('blocklengths')  : [i[k] for k from s1 <= k <= e1],
                     ('displacements') : [i[k] for k from s2 <= k <= e2]})
        elif combiner == MPI_COMBINER_HINDEXED:
            s1 = 1; e1 = i[0]
            s2 = 0; e2 = i[0]-1
            return (oldtype, ('HINDEXED'),
                    {('blocklengths')  : [i[k] for k from s1 <= k <= e1],
                     ('displacements') : [a[k] for k from s2 <= k <= e2]})
        elif combiner == MPI_COMBINER_INDEXED_BLOCK:
            s2 = 2; e2 = i[0]+1
            return (oldtype, ('INDEXED_BLOCK'),
                    {('blocklength')   : i[1],
                     ('displacements') : [i[k] for k from s2 <= k <= e2]})
        elif combiner == MPI_COMBINER_HINDEXED_BLOCK:
            s2 = 0; e2 = i[0]-1
            return (oldtype, ('HINDEXED_BLOCK'),
                    {('blocklength')   : i[1],
                     ('displacements') : [a[k] for k from s2 <= k <= e2]})
        elif combiner == MPI_COMBINER_STRUCT:
            s1 = 1; e1 = i[0]
            s2 = 0; e2 = i[0]-1
            return (DATATYPE_NULL, ('STRUCT'),
                    {('blocklengths')  : [i[k] for k from s1 <= k <= e1],
                     ('displacements') : [a[k] for k from s2 <= k <= e2],
                     ('datatypes')     : oldtype})
        elif combiner == MPI_COMBINER_SUBARRAY:
            s1 =        1; e1 =   i[0]
            s2 =   i[0]+1; e2 = 2*i[0]
            s3 = 2*i[0]+1; e3 = 3*i[0]
            return (oldtype, ('SUBARRAY'),
                    {('sizes')    : [i[k] for k from s1 <= k <= e1],
                     ('subsizes') : [i[k] for k from s2 <= k <= e2],
                     ('starts')   : [i[k] for k from s3 <= k <= e3],
                     ('order')    : i[3*i[0]+1]})
        elif combiner == MPI_COMBINER_DARRAY:
            s1 =        3; e1 =   i[2]+2
            s2 =   i[2]+3; e2 = 2*i[2]+2
            s3 = 2*i[2]+3; e3 = 3*i[2]+2
            s4 = 3*i[2]+3; e4 = 4*i[2]+2
            return (oldtype, ('DARRAY'),
                    {('size')     : i[0],
                     ('rank')     : i[1],
                     ('gsizes')   : [i[k] for k from s1 <= k <= e1],
                     ('distribs') : [i[k] for k from s2 <= k <= e2],
                     ('dargs')    : [i[k] for k from s3 <= k <= e3],
                     ('psizes')   : [i[k] for k from s4 <= k <= e4],
                     ('order')    : i[4*i[2]+3]})
        elif combiner == MPI_COMBINER_RESIZED:
            return (oldtype, ('RESIZED'),
                    {('lb')     : a[0],
                     ('extent') : a[1]})
        elif combiner == MPI_COMBINER_F90_INTEGER:
            return (DATATYPE_NULL, ('F90_INTEGER'),
                    {('r') : i[0]})
        elif combiner == MPI_COMBINER_F90_REAL:
            return (DATATYPE_NULL, ('F90_REAL'),
                    {('p') : i[0],
                     ('r') : i[1]})
        elif combiner == MPI_COMBINER_F90_COMPLEX:
            return (DATATYPE_NULL, ('F90_COMPLEX'),
                    {('p') : i[0],
                     ('r') : i[1]})
        else:
            return None

    property combiner:
        """datatype combiner"""
        def __get__(self) -> int:
            return self.Get_envelope()[3]

    property is_named:
        """is a named datatype"""
        def __get__(self) -> bool:
            cdef int combiner = self.Get_envelope()[3]
            return combiner == MPI_COMBINER_NAMED

    property is_predefined:
        """is a predefined datatype"""
        def __get__(self) -> bool:
            if self.ob_mpi == MPI_DATATYPE_NULL: return True
            cdef int combiner = self.Get_envelope()[3]
            return (combiner == MPI_COMBINER_NAMED or
                    combiner == MPI_COMBINER_F90_INTEGER or
                    combiner == MPI_COMBINER_F90_REAL or
                    combiner == MPI_COMBINER_F90_COMPLEX)

    # Pack and Unpack
    # ---------------

    def Pack(
        self,
        inbuf: BufSpec,
        outbuf: BufSpec,
        int position: int,
        Comm comm: Comm,
    ) -> int:
        """
        Pack into contiguous memory according to datatype.
        """
        cdef MPI_Aint lb = 0, extent = 0
        CHKERR( MPI_Type_get_extent(self.ob_mpi, &lb, &extent) )
        #
        cdef void *ibptr = NULL, *obptr = NULL
        cdef MPI_Aint iblen = 0, oblen = 0
        cdef tmp1 = getbuffer_r(inbuf,  &ibptr, &iblen)
        cdef tmp2 = getbuffer_w(outbuf, &obptr, &oblen)
        cdef int icount = downcast(iblen//extent)
        cdef int osize  = clipcount(oblen)
        #
        CHKERR( MPI_Pack(ibptr, icount, self.ob_mpi, obptr, osize,
                         &position, comm.ob_mpi) )
        return position

    def Unpack(
        self,
        inbuf: BufSpec,
        int position: int,
        outbuf: BufSpec,
        Comm comm: Comm,
    ) -> int:
        """
        Unpack from contiguous memory according to datatype.
        """
        cdef MPI_Aint lb = 0, extent = 0
        CHKERR( MPI_Type_get_extent(self.ob_mpi, &lb, &extent) )
        #
        cdef void *ibptr = NULL, *obptr = NULL
        cdef MPI_Aint iblen = 0, oblen = 0
        cdef tmp1 = getbuffer_r(inbuf,  &ibptr, &iblen)
        cdef tmp2 = getbuffer_w(outbuf, &obptr, &oblen)
        cdef int isize  = clipcount(iblen)
        cdef int ocount = downcast(oblen//extent)
        #
        CHKERR( MPI_Unpack(ibptr, isize, &position, obptr, ocount,
                           self.ob_mpi, comm.ob_mpi) )
        return position

    def Pack_size(
        self,
        int count: int,
        Comm comm: Comm,
    ) -> int:
        """
        Return the upper bound on the amount of space (in bytes)
        needed to pack a message according to datatype.
        """
        cdef int size = 0
        CHKERR( MPI_Pack_size(count, self.ob_mpi,
                              comm.ob_mpi, &size) )
        return size

    # Canonical Pack and Unpack
    # -------------------------

    def Pack_external(
        self,
        datarep: str,
        inbuf: BufSpec,
        outbuf: BufSpec,
        Aint position: int,
    ) -> int:
        """
        Pack into contiguous memory according to datatype,
        using a portable data representation (**external32**).
        """
        cdef char *cdatarep = NULL
        datarep = asmpistr(datarep, &cdatarep)
        cdef MPI_Aint lb = 0, extent = 0
        CHKERR( MPI_Type_get_extent(self.ob_mpi, &lb, &extent) )
        #
        cdef void *ibptr = NULL, *obptr = NULL
        cdef MPI_Aint iblen = 0, oblen = 0
        cdef tmp1 = getbuffer_r(inbuf,  &ibptr, &iblen)
        cdef tmp2 = getbuffer_w(outbuf, &obptr, &oblen)
        cdef int icount = downcast(iblen//extent)
        #
        CHKERR( MPI_Pack_external(cdatarep, ibptr, icount,
                                  self.ob_mpi,
                                  obptr, oblen, &position) )
        return position

    def Unpack_external(
        self,
        datarep: str,
        inbuf: BufSpec,
        Aint position: int,
        outbuf: BufSpec,
    ) -> int:
        """
        Unpack from contiguous memory according to datatype,
        using a portable data representation (**external32**).
        """
        cdef char *cdatarep = NULL
        datarep = asmpistr(datarep, &cdatarep)
        cdef MPI_Aint lb = 0, extent = 0
        CHKERR( MPI_Type_get_extent(self.ob_mpi, &lb, &extent) )
        #
        cdef void *ibptr = NULL, *obptr = NULL
        cdef MPI_Aint iblen = 0, oblen = 0
        cdef tmp1 = getbuffer_r(inbuf,  &ibptr, &iblen)
        cdef tmp2 = getbuffer_w(outbuf, &obptr, &oblen)
        cdef int ocount = downcast(oblen//extent)
        #
        CHKERR( MPI_Unpack_external(cdatarep, ibptr, iblen, &position,
                                    obptr, ocount, self.ob_mpi) )
        return position

    def Pack_external_size(
        self,
        datarep: str,
        int count: int,
    ) -> int:
        """
        Return the upper bound on the amount of space (in bytes)
        needed to pack a message according to datatype,
        using a portable data representation (**external32**).
        """
        cdef char *cdatarep = NULL
        cdef MPI_Aint size = 0
        datarep = asmpistr(datarep, &cdatarep)
        CHKERR( MPI_Pack_external_size(cdatarep, count,
                                       self.ob_mpi, &size) )
        return size

    # Attributes
    # ----------

    def Get_attr(self, int keyval: int) -> Optional[Union[int, Any]]:
        """
        Retrieve attribute value by key
        """
        cdef void *attrval = NULL
        cdef int flag = 0
        CHKERR( MPI_Type_get_attr(self.ob_mpi, keyval, &attrval, &flag) )
        if not flag: return None
        if attrval == NULL: return 0
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
        CHKERR( MPI_Type_delete_attr(self.ob_mpi, keyval) )

    @classmethod
    def Create_keyval(
        cls,
        copy_fn: Optional[Callable[[Datatype, int, Any], Any]] = None,
        delete_fn: Optional[Callable[[Datatype, int, Any], None]] = None,
        nopython: bool = False,
    ) -> int:
        """
        Create a new attribute key for datatypes
        """
        cdef object state = _p_keyval(copy_fn, delete_fn, nopython)
        cdef int keyval = MPI_KEYVAL_INVALID
        cdef MPI_Type_copy_attr_function *_copy = PyMPI_attr_copy_fn
        cdef MPI_Type_delete_attr_function *_del = PyMPI_attr_delete_fn
        cdef void *extra_state = <void *>state
        CHKERR( MPI_Type_create_keyval(_copy, _del, &keyval, extra_state) )
        type_keyval[keyval] = state
        return keyval

    @classmethod
    def Free_keyval(cls, int keyval: int) -> int:
        """
        Free an attribute key for datatypes
        """
        cdef int keyval_save = keyval
        CHKERR( MPI_Type_free_keyval(&keyval) )
        try: del type_keyval[keyval_save]
        except KeyError: pass
        return keyval

    # Naming Objects
    # --------------

    def Get_name(self) -> str:
        """
        Get the print name for this datatype
        """
        cdef char name[MPI_MAX_OBJECT_NAME+1]
        cdef int nlen = 0
        CHKERR( MPI_Type_get_name(self.ob_mpi, name, &nlen) )
        return tompistr(name, nlen)

    def Set_name(self, name: str) -> None:
        """
        Set the print name for this datatype
        """
        cdef char *cname = NULL
        name = asmpistr(name, &cname)
        CHKERR( MPI_Type_set_name(self.ob_mpi, cname) )

    property name:
        """datatype name"""
        def __get__(self) -> str:
            return self.Get_name()
        def __set__(self, value: str):
            self.Set_name(value)

    # Fortran Handle
    # --------------

    def py2f(self) -> int:
        """
        """
        return MPI_Type_c2f(self.ob_mpi)

    @classmethod
    def f2py(cls, arg: int) -> Datatype:
        """
        """
        cdef Datatype datatype = Datatype.__new__(Datatype)
        datatype.ob_mpi = MPI_Type_f2c(arg)
        return datatype


# Address Functions
# -----------------

def Get_address(location: Union[Buffer, Bottom]) -> int:
    """
    Get the address of a location in memory
    """
    cdef void *baseptr = MPI_BOTTOM
    if not is_BOTTOM(location):
        getbuffer_r(location, &baseptr, NULL)
    cdef MPI_Aint address = 0
    CHKERR( MPI_Get_address(baseptr, &address) )
    return address

def Aint_add(Aint base: int, Aint disp: int) -> int:
    """
    Return the sum of base address and displacement
    """
    return MPI_Aint_add(base, disp)

def Aint_diff(Aint addr1: int, Aint addr2: int) -> int:
    """
    Return the difference between absolute addresses
    """
    return MPI_Aint_diff(addr1, addr2)


cdef Datatype __DATATYPE_NULL__ = new_Datatype( MPI_DATATYPE_NULL )

cdef Datatype __UB__ = new_Datatype( MPI_UB )
cdef Datatype __LB__ = new_Datatype( MPI_LB )

cdef Datatype __PACKED__ = new_Datatype( MPI_PACKED )
cdef Datatype __BYTE__   = new_Datatype( MPI_BYTE   )
cdef Datatype __AINT__   = new_Datatype( MPI_AINT   )
cdef Datatype __OFFSET__ = new_Datatype( MPI_OFFSET )
cdef Datatype __COUNT__  = new_Datatype( MPI_COUNT  )

cdef Datatype __CHAR__               = new_Datatype( MPI_CHAR               )
cdef Datatype __WCHAR__              = new_Datatype( MPI_WCHAR              )
cdef Datatype __SIGNED_CHAR__        = new_Datatype( MPI_SIGNED_CHAR        )
cdef Datatype __SHORT__              = new_Datatype( MPI_SHORT              )
cdef Datatype __INT__                = new_Datatype( MPI_INT                )
cdef Datatype __LONG__               = new_Datatype( MPI_LONG               )
cdef Datatype __LONG_LONG__          = new_Datatype( MPI_LONG_LONG          )
cdef Datatype __UNSIGNED_CHAR__      = new_Datatype( MPI_UNSIGNED_CHAR      )
cdef Datatype __UNSIGNED_SHORT__     = new_Datatype( MPI_UNSIGNED_SHORT     )
cdef Datatype __UNSIGNED__           = new_Datatype( MPI_UNSIGNED           )
cdef Datatype __UNSIGNED_LONG__      = new_Datatype( MPI_UNSIGNED_LONG      )
cdef Datatype __UNSIGNED_LONG_LONG__ = new_Datatype( MPI_UNSIGNED_LONG_LONG )
cdef Datatype __FLOAT__              = new_Datatype( MPI_FLOAT              )
cdef Datatype __DOUBLE__             = new_Datatype( MPI_DOUBLE             )
cdef Datatype __LONG_DOUBLE__        = new_Datatype( MPI_LONG_DOUBLE        )

cdef Datatype __C_BOOL__                = new_Datatype( MPI_C_BOOL      )
cdef Datatype __INT8_T__                = new_Datatype( MPI_INT8_T      )
cdef Datatype __INT16_T__               = new_Datatype( MPI_INT16_T     )
cdef Datatype __INT32_T__               = new_Datatype( MPI_INT32_T     )
cdef Datatype __INT64_T__               = new_Datatype( MPI_INT64_T     )
cdef Datatype __UINT8_T__               = new_Datatype( MPI_UINT8_T     )
cdef Datatype __UINT16_T__              = new_Datatype( MPI_UINT16_T    )
cdef Datatype __UINT32_T__              = new_Datatype( MPI_UINT32_T    )
cdef Datatype __UINT64_T__              = new_Datatype( MPI_UINT64_T    )
cdef Datatype __C_COMPLEX__             = new_Datatype(
                                              MPI_C_COMPLEX             )
cdef Datatype __C_FLOAT_COMPLEX__       = new_Datatype(
                                              MPI_C_FLOAT_COMPLEX       )
cdef Datatype __C_DOUBLE_COMPLEX__      = new_Datatype(
                                              MPI_C_DOUBLE_COMPLEX      )
cdef Datatype __C_LONG_DOUBLE_COMPLEX__ = new_Datatype(
                                              MPI_C_LONG_DOUBLE_COMPLEX )

cdef Datatype __CXX_BOOL__                = new_Datatype( MPI_CXX_BOOL      )
cdef Datatype __CXX_FLOAT_COMPLEX__       = new_Datatype(
                                                MPI_CXX_FLOAT_COMPLEX       )
cdef Datatype __CXX_DOUBLE_COMPLEX__      = new_Datatype(
                                                MPI_CXX_DOUBLE_COMPLEX      )
cdef Datatype __CXX_LONG_DOUBLE_COMPLEX__ = new_Datatype(
                                                MPI_CXX_LONG_DOUBLE_COMPLEX )

cdef Datatype __SHORT_INT__        = new_Datatype( MPI_SHORT_INT       )
cdef Datatype __TWOINT__           = new_Datatype( MPI_2INT            )
cdef Datatype __LONG_INT__         = new_Datatype( MPI_LONG_INT        )
cdef Datatype __FLOAT_INT__        = new_Datatype( MPI_FLOAT_INT       )
cdef Datatype __DOUBLE_INT__       = new_Datatype( MPI_DOUBLE_INT      )
cdef Datatype __LONG_DOUBLE_INT__  = new_Datatype( MPI_LONG_DOUBLE_INT )

cdef Datatype __CHARACTER__        = new_Datatype( MPI_CHARACTER        )
cdef Datatype __LOGICAL__          = new_Datatype( MPI_LOGICAL          )
cdef Datatype __INTEGER__          = new_Datatype( MPI_INTEGER          )
cdef Datatype __REAL__             = new_Datatype( MPI_REAL             )
cdef Datatype __DOUBLE_PRECISION__ = new_Datatype( MPI_DOUBLE_PRECISION )
cdef Datatype __COMPLEX__          = new_Datatype( MPI_COMPLEX          )
cdef Datatype __DOUBLE_COMPLEX__   = new_Datatype( MPI_DOUBLE_COMPLEX   )

cdef Datatype __LOGICAL1__  = new_Datatype( MPI_LOGICAL1  )
cdef Datatype __LOGICAL2__  = new_Datatype( MPI_LOGICAL2  )
cdef Datatype __LOGICAL4__  = new_Datatype( MPI_LOGICAL4  )
cdef Datatype __LOGICAL8__  = new_Datatype( MPI_LOGICAL8  )
cdef Datatype __INTEGER1__  = new_Datatype( MPI_INTEGER1  )
cdef Datatype __INTEGER2__  = new_Datatype( MPI_INTEGER2  )
cdef Datatype __INTEGER4__  = new_Datatype( MPI_INTEGER4  )
cdef Datatype __INTEGER8__  = new_Datatype( MPI_INTEGER8  )
cdef Datatype __INTEGER16__ = new_Datatype( MPI_INTEGER16 )
cdef Datatype __REAL2__     = new_Datatype( MPI_REAL2     )
cdef Datatype __REAL4__     = new_Datatype( MPI_REAL4     )
cdef Datatype __REAL8__     = new_Datatype( MPI_REAL8     )
cdef Datatype __REAL16__    = new_Datatype( MPI_REAL16    )
cdef Datatype __COMPLEX4__  = new_Datatype( MPI_COMPLEX4  )
cdef Datatype __COMPLEX8__  = new_Datatype( MPI_COMPLEX8  )
cdef Datatype __COMPLEX16__ = new_Datatype( MPI_COMPLEX16 )
cdef Datatype __COMPLEX32__ = new_Datatype( MPI_COMPLEX32 )

include "typemap.pxi"
include "typestr.pxi"


# Predefined datatype handles
# ---------------------------

DATATYPE_NULL = __DATATYPE_NULL__ #: Null datatype handle
# Deprecated datatypes (since MPI-2)
UB = __UB__ #: upper-bound marker
LB = __LB__ #: lower-bound marker
# MPI-specific datatypes
PACKED = __PACKED__
BYTE   = __BYTE__
AINT   = __AINT__
OFFSET = __OFFSET__
COUNT  = __COUNT__
# Elementary C datatypes
CHAR                = __CHAR__
WCHAR               = __WCHAR__
SIGNED_CHAR         = __SIGNED_CHAR__
SHORT               = __SHORT__
INT                 = __INT__
LONG                = __LONG__
LONG_LONG           = __LONG_LONG__
UNSIGNED_CHAR       = __UNSIGNED_CHAR__
UNSIGNED_SHORT      = __UNSIGNED_SHORT__
UNSIGNED            = __UNSIGNED__
UNSIGNED_LONG       = __UNSIGNED_LONG__
UNSIGNED_LONG_LONG  = __UNSIGNED_LONG_LONG__
FLOAT               = __FLOAT__
DOUBLE              = __DOUBLE__
LONG_DOUBLE         = __LONG_DOUBLE__
# C99 datatypes
C_BOOL                = __C_BOOL__
INT8_T                = __INT8_T__
INT16_T               = __INT16_T__
INT32_T               = __INT32_T__
INT64_T               = __INT64_T__
UINT8_T               = __UINT8_T__
UINT16_T              = __UINT16_T__
UINT32_T              = __UINT32_T__
UINT64_T              = __UINT64_T__
C_COMPLEX             = __C_COMPLEX__
C_FLOAT_COMPLEX       = __C_FLOAT_COMPLEX__
C_DOUBLE_COMPLEX      = __C_DOUBLE_COMPLEX__
C_LONG_DOUBLE_COMPLEX = __C_LONG_DOUBLE_COMPLEX__
# C++ datatypes
CXX_BOOL                = __CXX_BOOL__
CXX_FLOAT_COMPLEX       = __CXX_FLOAT_COMPLEX__
CXX_DOUBLE_COMPLEX      = __CXX_DOUBLE_COMPLEX__
CXX_LONG_DOUBLE_COMPLEX = __CXX_LONG_DOUBLE_COMPLEX__
# C Datatypes for reduction operations
SHORT_INT        = __SHORT_INT__
INT_INT = TWOINT = __TWOINT__
LONG_INT         = __LONG_INT__
FLOAT_INT        = __FLOAT_INT__
DOUBLE_INT       = __DOUBLE_INT__
LONG_DOUBLE_INT  = __LONG_DOUBLE_INT__
# Elementary Fortran datatypes
CHARACTER        = __CHARACTER__
LOGICAL          = __LOGICAL__
INTEGER          = __INTEGER__
REAL             = __REAL__
DOUBLE_PRECISION = __DOUBLE_PRECISION__
COMPLEX          = __COMPLEX__
DOUBLE_COMPLEX   = __DOUBLE_COMPLEX__
# Size-specific Fortran datatypes
LOGICAL1  = __LOGICAL1__
LOGICAL2  = __LOGICAL2__
LOGICAL4  = __LOGICAL4__
LOGICAL8  = __LOGICAL8__
INTEGER1  = __INTEGER1__
INTEGER2  = __INTEGER2__
INTEGER4  = __INTEGER4__
INTEGER8  = __INTEGER8__
INTEGER16 = __INTEGER16__
REAL2     = __REAL2__
REAL4     = __REAL4__
REAL8     = __REAL8__
REAL16    = __REAL16__
COMPLEX4  = __COMPLEX4__
COMPLEX8  = __COMPLEX8__
COMPLEX16 = __COMPLEX16__
COMPLEX32 = __COMPLEX32__

# Convenience aliases
UNSIGNED_INT          = __UNSIGNED__
SIGNED_SHORT          = __SHORT__
SIGNED_INT            = __INT__
SIGNED_LONG           = __LONG__
SIGNED_LONG_LONG      = __LONG_LONG__
BOOL                  = __C_BOOL__
SINT8_T               = __INT8_T__
SINT16_T              = __INT16_T__
SINT32_T              = __INT32_T__
SINT64_T              = __INT64_T__
F_BOOL                = __LOGICAL__
F_INT                 = __INTEGER__
F_FLOAT               = __REAL__
F_DOUBLE              = __DOUBLE_PRECISION__
F_COMPLEX             = __COMPLEX__
F_FLOAT_COMPLEX       = __COMPLEX__
F_DOUBLE_COMPLEX      = __DOUBLE_COMPLEX__
