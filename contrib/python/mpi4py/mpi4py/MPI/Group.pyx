cdef class Group:

    """
    Group of processes
    """

    def __cinit__(self, Group group: Optional[Group] = None):
        self.ob_mpi = MPI_GROUP_NULL
        if group is None: return
        self.ob_mpi = group.ob_mpi

    def __dealloc__(self):
        if not (self.flags & PyMPI_OWNED): return
        CHKERR( del_Group(&self.ob_mpi) )

    def __richcmp__(self, other, int op):
        if not isinstance(other, Group): return NotImplemented
        cdef Group s = <Group>self, o = <Group>other
        if   op == Py_EQ: return (s.ob_mpi == o.ob_mpi)
        elif op == Py_NE: return (s.ob_mpi != o.ob_mpi)
        cdef mod = type(self).__module__
        cdef cls = type(self).__name__
        raise TypeError("unorderable type: '%s.%s'" % (mod, cls))

    def __bool__(self) -> bool:
        return self.ob_mpi != MPI_GROUP_NULL

    # Group Accessors
    # ---------------

    def Get_size(self) -> int:
        """
        Return the size of a group
        """
        cdef int size = -1
        CHKERR( MPI_Group_size(self.ob_mpi, &size) )
        return size

    property size:
        """number of processes in group"""
        def __get__(self) -> int:
            return self.Get_size()

    def Get_rank(self) -> int:
        """
        Return the rank of this process in a group
        """
        cdef int rank = -1
        CHKERR( MPI_Group_rank(self.ob_mpi, &rank) )
        return rank

    property rank:
        """rank of this process in group"""
        def __get__(self) -> int:
            return self.Get_rank()

    @classmethod
    def Translate_ranks(
        cls,
        Group group1: Group,
        ranks1: Sequence[int],
        Group group2: Optional[Group] = None,
    ) -> List[int]:
        """
        Translate the ranks of processes in
        one group to those in another group
        """
        cdef MPI_Group grp1 = MPI_GROUP_NULL
        cdef MPI_Group grp2 = MPI_GROUP_NULL
        cdef int i = 0, n = 0, *iranks1 = NULL, *iranks2 = NULL
        cdef tmp1 = getarray(ranks1, &n, &iranks1)
        cdef tmp2 = newarray(n, &iranks2)
        #
        grp1 = group1.ob_mpi
        if group2 is not None:
            grp2 = group2.ob_mpi
        else:
            CHKERR( MPI_Comm_group(MPI_COMM_WORLD, &grp2) )
        try:
            CHKERR( MPI_Group_translate_ranks(grp1, n, iranks1,
                                              grp2, iranks2) )
        finally:
            if group2 is None:
                CHKERR( MPI_Group_free(&grp2) )
        #
        cdef object ranks2 = [iranks2[i] for i from 0 <= i < n]
        return ranks2

    @classmethod
    def Compare(cls, Group group1: Group, Group group2: Group) -> int:
        """
        Compare two groups
        """
        cdef int flag = MPI_UNEQUAL
        CHKERR( MPI_Group_compare(group1.ob_mpi, group2.ob_mpi, &flag) )
        return flag

    # Group Constructors
    # ------------------

    def Dup(self) -> Group:
        """
        Duplicate a group
        """
        cdef Group group = Group.__new__(Group)
        CHKERR( MPI_Group_union(self.ob_mpi, MPI_GROUP_EMPTY, &group.ob_mpi) )
        return group

    @classmethod
    def Union(cls, Group group1: Group, Group group2: Group) -> Group:
        """
        Produce a group by combining
        two existing groups
        """
        cdef Group group = Group.__new__(Group)
        CHKERR( MPI_Group_union(
                group1.ob_mpi, group2.ob_mpi, &group.ob_mpi) )
        return group

    @classmethod
    def Intersection(cls, Group group1: Group, Group group2: Group) -> Group:
        """
        Produce a group as the intersection
        of two existing groups
        """
        cdef Group group = Group.__new__(Group)
        CHKERR( MPI_Group_intersection(
                group1.ob_mpi, group2.ob_mpi, &group.ob_mpi) )
        return group

    Intersect = Intersection

    @classmethod
    def Difference(cls, Group group1: Group, Group group2: Group) -> Group:
        """
        Produce a group from the difference
        of two existing groups
        """
        cdef Group group = Group.__new__(Group)
        CHKERR( MPI_Group_difference(
                group1.ob_mpi, group2.ob_mpi, &group.ob_mpi) )
        return group

    def Incl(self, ranks: Sequence[int]) -> Group:
        """
        Produce a group by reordering an existing
        group and taking only listed members
        """
        cdef int n = 0, *iranks = NULL
        ranks = getarray(ranks, &n, &iranks)
        cdef Group group = Group.__new__(Group)
        CHKERR( MPI_Group_incl(self.ob_mpi, n, iranks, &group.ob_mpi) )
        return group

    def Excl(self, ranks: Sequence[int]) -> Group:
        """
        Produce a group by reordering an existing
        group and taking only unlisted members
        """
        cdef int n = 0, *iranks = NULL
        ranks = getarray(ranks, &n, &iranks)
        cdef Group group = Group.__new__(Group)
        CHKERR( MPI_Group_excl(self.ob_mpi, n, iranks, &group.ob_mpi) )
        return group

    def Range_incl(self, ranks: Sequence[Tuple[int, int, int]]) -> Group:
        """
        Create a new group from ranges of
        of ranks in an existing group
        """
        cdef int *p = NULL, (*ranges)[3]# = NULL ## XXX cython fails
        ranges = NULL
        cdef int i = 0, n = <int>len(ranks)
        cdef tmp1 = allocate(n, sizeof(int[3]), &ranges)
        for i from 0 <= i < n:
            p = <int*> ranges[i]
            p[0], p[1], p[2] = ranks[i]
        cdef Group group = Group.__new__(Group)
        CHKERR( MPI_Group_range_incl(self.ob_mpi, n, ranges, &group.ob_mpi) )
        return group

    def Range_excl(self, ranks: Sequence[Tuple[int, int, int]]) -> Group:
        """
        Create a new group by excluding ranges
        of processes from an existing group
        """
        cdef int *p = NULL, (*ranges)[3]# = NULL ## XXX cython fails
        ranges = NULL
        cdef int i = 0, n = <int>len(ranks)
        cdef tmp1 = allocate(n, sizeof(int[3]), &ranges)
        for i from 0 <= i < n:
            p = <int*> ranges[i]
            p[0], p[1], p[2] = ranks[i]
        cdef Group group = Group.__new__(Group)
        CHKERR( MPI_Group_range_excl(self.ob_mpi, n, ranges, &group.ob_mpi) )
        return group

    # Group Destructor
    # ----------------

    def Free(self) -> None:
        """
        Free a group
        """
        CHKERR( MPI_Group_free(&self.ob_mpi) )
        if self is __GROUP_EMPTY__: self.ob_mpi = MPI_GROUP_EMPTY

    # Fortran Handle
    # --------------

    def py2f(self) -> int:
        """
        """
        return MPI_Group_c2f(self.ob_mpi)

    @classmethod
    def f2py(cls, arg: int) -> Group:
        """
        """
        cdef Group group = Group.__new__(Group)
        group.ob_mpi = MPI_Group_f2c(arg)
        return group



cdef Group __GROUP_NULL__  = new_Group ( MPI_GROUP_NULL  )
cdef Group __GROUP_EMPTY__ = new_Group ( MPI_GROUP_EMPTY )


# Predefined group handles
# ------------------------

GROUP_NULL  = __GROUP_NULL__   #: Null group handle
GROUP_EMPTY = __GROUP_EMPTY__  #: Empty group handle
