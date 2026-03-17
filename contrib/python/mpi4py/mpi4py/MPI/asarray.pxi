# -----------------------------------------------------------------------------

ctypedef fused integral_t:
    int
    MPI_Aint

cdef inline object newarray(int n, integral_t **p):
    return allocate(n, sizeof(integral_t), p)

cdef inline object getarray(object ob, int *n, integral_t **p):
    cdef Py_ssize_t  olen = len(ob)
    cdef integral_t *base = NULL
    cdef int i = 0, size = downcast(olen)
    cdef object mem = newarray(size, &base)
    for i from 0 <= i < size: base[i] = ob[i]
    n[0] = size
    p[0] = base
    return mem

cdef inline object chkarray(object ob, int n, integral_t **p):
    cdef int size = 0
    cdef object mem = getarray(ob, &size, p)
    if n != size: raise ValueError(
        "expecting %d items, got %d" % (n, size))
    return mem

# -----------------------------------------------------------------------------

cdef inline object asarray_Datatype(object sequence,
                                    int size, MPI_Datatype **p):
     cdef int i = 0
     cdef MPI_Datatype *array = NULL
     if size != len(sequence): raise ValueError(
         "expecting %d items, got %d" % (size, len(sequence)))
     cdef object ob = allocate(size, sizeof(MPI_Datatype), &array)
     for i from 0 <= i < size:
         array[i] = (<Datatype?>sequence[i]).ob_mpi
     p[0] = array
     return ob

cdef inline object asarray_Info(object sequence,
                                int size, MPI_Info **p):
     cdef int i = 0
     cdef MPI_Info *array = NULL
     cdef MPI_Info info = MPI_INFO_NULL
     cdef object ob
     if sequence is None or isinstance(sequence, Info):
         if sequence is not None:
             info = (<Info?>sequence).ob_mpi
         ob = allocate(size, sizeof(MPI_Info), &array)
         for i from 0 <= i < size:
             array[i] = info
     else:
         if size != len(sequence): raise ValueError(
             "expecting %d items, got %d" % (size, len(sequence)))
         ob = allocate(size, sizeof(MPI_Datatype), &array)
         for i from 0 <= i < size:
             array[i] = (<Info?>sequence[i]).ob_mpi
     p[0] = array
     return ob

# -----------------------------------------------------------------------------

cdef inline int is_string(object obj):
     return (isinstance(obj, str) or
             isinstance(obj, bytes) or
             isinstance(obj, unicode))

cdef inline object asstring(object ob, char *s[]):
     cdef Py_ssize_t n = 0
     cdef char *p = NULL, *q = NULL
     ob = asmpistr(ob, &p)
     PyBytes_AsStringAndSize(ob, &p, &n)
     cdef object mem = allocate(n+1, sizeof(char), &q)
     <void>memcpy(q, p, <size_t>n)
     q[n] = 0; s[0] = q;
     return mem

cdef inline object asarray_str(object sequence, char ***p):
     cdef char** array = NULL
     cdef Py_ssize_t i = 0, size = len(sequence)
     cdef object ob = allocate(size+1, sizeof(char*), &array)
     for i from 0 <= i < size:
         sequence[i] = asstring(sequence[i], &array[i])
     array[size] = NULL
     p[0] = array
     return (sequence, ob)

cdef inline object asarray_argv(object sequence, char ***p):
     if sequence is None:
         p[0] = MPI_ARGV_NULL
         return None
     if is_string(sequence):
         sequence = [sequence]
     else:
         sequence = list(sequence)
     return asarray_str(sequence, p)

cdef inline object asarray_cmds(object sequence, int *count, char ***p):
     if is_string(sequence):
         raise ValueError("expecting a sequence of strings")
     sequence = list(sequence)
     count[0] = <int>len(sequence)
     return asarray_str(sequence, p)

cdef inline object asarray_argvs(object sequence, int size, char ****p):
     if sequence is None:
         p[0] = MPI_ARGVS_NULL
         return None
     if is_string(sequence):
         sequence = [sequence] * size
     else:
         sequence = list(sequence)
         if size != len(sequence): raise ValueError(
             "expecting %d items, got %d" % (size, len(sequence)))
     cdef int i = 0
     cdef char*** array = NULL
     cdef object ob = allocate(size+1, sizeof(char**), &array)
     cdef object argv
     for i from 0 <= i < size:
         argv = sequence[i]
         if argv is None: argv = []
         sequence[i] = asarray_argv(argv, &array[i])
     array[size] = NULL
     p[0] = array
     return (sequence, ob)

cdef inline object asarray_nprocs(object sequence, int size, int **p):
     cdef object ob
     cdef int *array = NULL
     cdef int i = 0, value = 1
     if sequence is None or is_integral(sequence):
         if sequence is not None:
             value = sequence
         ob = newarray(size, &array)
         for i from 0 <= i < size:
             array[i] = value
     else:
         ob = chkarray(sequence, size, &array)
     p[0] = array
     return ob

# -----------------------------------------------------------------------------
