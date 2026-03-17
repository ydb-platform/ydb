cdef class Info:

    """
    Info object
    """

    def __cinit__(self, Info info: Optional[Info] =None):
        self.ob_mpi = MPI_INFO_NULL
        if info is None: return
        self.ob_mpi = info.ob_mpi

    def __dealloc__(self):
        if not (self.flags & PyMPI_OWNED): return
        CHKERR( del_Info(&self.ob_mpi) )

    def __richcmp__(self, other, int op):
        if not isinstance(other, Info): return NotImplemented
        cdef Info s = <Info>self, o = <Info>other
        if   op == Py_EQ: return (s.ob_mpi == o.ob_mpi)
        elif op == Py_NE: return (s.ob_mpi != o.ob_mpi)
        cdef mod = type(self).__module__
        cdef cls = type(self).__name__
        raise TypeError("unorderable type: '%s.%s'" % (mod, cls))

    def __bool__(self) -> bool:
        return self.ob_mpi != MPI_INFO_NULL

    @classmethod
    def Create(cls) -> Info:
        """
        Create a new, empty info object
        """
        cdef Info info = Info.__new__(Info)
        CHKERR( MPI_Info_create(&info.ob_mpi) )
        return info

    def Free(self) -> None:
        """
        Free a info object
        """
        CHKERR( MPI_Info_free(&self.ob_mpi) )
        if self is __INFO_ENV__: self.ob_mpi = MPI_INFO_ENV

    def Dup(self) -> Info:
        """
        Duplicate an existing info object, creating a new object, with
        the same (key, value) pairs and the same ordering of keys
        """
        cdef Info info = Info.__new__(Info)
        CHKERR( MPI_Info_dup(self.ob_mpi, &info.ob_mpi) )
        return info

    def Get(self, key: str, int maxlen: int = -1) -> Optional[str]:
        """
        Retrieve the value associated with a key
        """
        if maxlen < 0: maxlen = MPI_MAX_INFO_VAL
        if maxlen > MPI_MAX_INFO_VAL: maxlen = MPI_MAX_INFO_VAL
        cdef char *ckey = NULL
        cdef char *cvalue = NULL
        cdef int flag = 0
        key = asmpistr(key, &ckey)
        cdef tmp = allocate((maxlen+1), sizeof(char), &cvalue)
        cvalue[0] = 0  # just in case
        CHKERR( MPI_Info_get(self.ob_mpi, ckey, maxlen, cvalue, &flag) )
        cvalue[maxlen] = 0 # just in case
        if not flag: return None
        return mpistr(cvalue)

    def Set(self, key: str, value: str) -> None:
        """
        Add the (key, value) pair to info, and overrides the value if
        a value for the same key was previously set
        """
        cdef char *ckey = NULL
        cdef char *cvalue = NULL
        key = asmpistr(key, &ckey)
        value = asmpistr(value, &cvalue)
        CHKERR( MPI_Info_set(self.ob_mpi, ckey, cvalue) )

    def Delete(self, key: str) -> None:
        """
        Remove a (key, value) pair from info
        """
        cdef char *ckey = NULL
        key = asmpistr(key, &ckey)
        CHKERR( MPI_Info_delete(self.ob_mpi, ckey) )

    def Get_nkeys(self) -> int:
        """
        Return the number of currently defined keys in info
        """
        cdef int nkeys = 0
        CHKERR( MPI_Info_get_nkeys(self.ob_mpi, &nkeys) )
        return nkeys

    def Get_nthkey(self, int n: int) -> str:
        """
        Return the nth defined key in info. Keys are numbered in the
        range [0, N) where N is the value returned by
        `Info.Get_nkeys()`
        """
        cdef char ckey[MPI_MAX_INFO_KEY+1]
        ckey[0] = 0 # just in case
        CHKERR( MPI_Info_get_nthkey(self.ob_mpi, n, ckey) )
        ckey[MPI_MAX_INFO_KEY] = 0 # just in case
        return mpistr(ckey)

    # Fortran Handle
    # --------------

    def py2f(self) -> int:
        """
        """
        return MPI_Info_c2f(self.ob_mpi)

    @classmethod
    def f2py(cls, arg: int) -> Info:
        """
        """
        cdef Info info = Info.__new__(Info)
        info.ob_mpi = MPI_Info_f2c(arg)
        return info

    # Python mapping emulation
    # ------------------------

    def __len__(self) -> int:
        if not self: return 0
        return self.Get_nkeys()

    def __contains__(self, key: str) -> bool:
        if not self: return False
        cdef char *ckey = NULL
        cdef int dummy = 0
        cdef int haskey = 0
        key = asmpistr(key, &ckey)
        CHKERR( MPI_Info_get_valuelen(self.ob_mpi, ckey, &dummy, &haskey) )
        return <bint>haskey

    def __iter__(self) -> Iterator[str]:
        return iter(self.keys())

    def __getitem__(self, key: str) -> str:
        if not self: raise KeyError(key)
        cdef object value = self.Get(key)
        if value is None: raise KeyError(key)
        return value

    def __setitem__(self, key: str, value: str) -> None:
        if not self: raise KeyError(key)
        self.Set(key, value)

    def __delitem__(self, key: str) -> None:
        if not self: raise KeyError(key)
        if key not in self: raise KeyError(key)
        self.Delete(key)

    def get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """info get"""
        if not self: return default
        cdef object value = self.Get(key)
        if value is None: return default
        return value

    def keys(self) -> List[str]:
        """info keys"""
        if not self: return []
        cdef list keys = []
        cdef int k = 0, nkeys = self.Get_nkeys()
        cdef object key
        for k from 0 <= k < nkeys:
            key = self.Get_nthkey(k)
            keys.append(key)
        return keys

    def values(self) -> List[str]:
        """info values"""
        if not self: return []
        cdef list values = []
        cdef int k = 0, nkeys = self.Get_nkeys()
        cdef object key, val
        for k from 0 <= k < nkeys:
            key = self.Get_nthkey(k)
            val = self.Get(key)
            values.append(val)
        return values

    def items(self) -> List[Tuple[str, str]]:
        """info items"""
        if not self: return []
        cdef list items = []
        cdef int k = 0, nkeys = self.Get_nkeys()
        cdef object key, value
        for k from 0 <= k < nkeys:
            key = self.Get_nthkey(k)
            value = self.Get(key)
            items.append((key, value))
        return items

    def update(
        self,
        other: Union[Info, Mapping[str, str], Iterable[Tuple[str, str]]] = (),
        **kwds: str,
    ) -> None:
        """info update"""
        if not self: raise KeyError
        cdef object key, value
        if hasattr(other, 'keys'):
            for key in other.keys():
                self.Set(key, other[key])
        else:
            for key, value in other:
                self.Set(key, value)
        for key, value in kwds.items():
            self.Set(key, value)

    def pop(self, key: str, *default: str) -> str:
        """info pop"""
        cdef object value = None
        if self:
            value = self.Get(key)
        if value is not None:
            self.Delete(key)
            return value
        if default:
            value, = default
            return value
        raise KeyError(key)

    def popitem(self) -> Tuple[str, str]:
        """info popitem"""
        if not self: raise KeyError
        cdef object key, value
        cdef int nkeys = self.Get_nkeys()
        if nkeys == 0: raise KeyError
        key = self.Get_nthkey(nkeys - 1)
        value = self.Get(key)
        self.Delete(key)
        return (key, value)

    def copy(self) -> Info:
        """info copy"""
        if not self: return Info()
        return self.Dup()

    def clear(self) -> None:
        """info clear"""
        if not self: return None
        cdef object key
        cdef int k = 0, nkeys = self.Get_nkeys()
        while k < nkeys:
            key = self.Get_nthkey(0)
            self.Delete(key)
            k += 1



cdef Info __INFO_NULL__ = new_Info(MPI_INFO_NULL)
cdef Info __INFO_ENV__  = new_Info(MPI_INFO_ENV)


# Predefined info handles
# -----------------------

INFO_NULL = __INFO_NULL__  #: Null info handle
INFO_ENV  = __INFO_ENV__   #: Environment info handle
