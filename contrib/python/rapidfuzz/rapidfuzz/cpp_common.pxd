from cpython.object cimport PyObject
from cpython.pycapsule cimport PyCapsule_GetPointer, PyCapsule_IsValid, PyCapsule_New
from libc.stddef cimport wchar_t
from libc.stdint cimport int64_t, uint64_t, SIZE_MAX, UINT64_MAX
from libc.stdlib cimport free, malloc
from libc.math cimport NAN
from libcpp cimport bool
from libcpp.cmath cimport isnan
from libcpp.utility cimport move, pair
from libcpp.vector cimport vector

from rapidfuzz cimport (
    PREPROCESSOR_STRUCT_VERSION,
    SCORER_STRUCT_VERSION,
    RF_GetScorerFlags,
    RF_Kwargs,
    RF_KwargsInit,
    RF_Preprocess,
    RF_Preprocessor,
    RF_Scorer,
    RF_ScorerFlags,
    RF_ScorerFuncInit,
    RF_UncachedScorerFunc,
    RF_String,
    RF_StringType,
)

# these are only for the cython compiler
# each module still has to define them on it's own
from array import array
pandas_NA = None

cdef extern from "rapidfuzz/details/types.hpp" namespace "rapidfuzz" nogil:
    cdef enum class EditType:
        None    = 0,
        Replace = 1,
        Insert  = 2,
        Delete  = 3

    ctypedef struct RfEditOp "rapidfuzz::EditOp":
        EditType type
        size_t src_pos
        size_t dest_pos

    cdef cppclass RfScoreAlignment "rapidfuzz::ScoreAlignment"[T]:
        T score
        size_t src_start
        size_t src_end
        size_t dest_start
        size_t dest_end

        RfScoreAlignment()
        RfScoreAlignment(T score, size_t src_start, size_t src_end, size_t dest_start, size_t dest_end)

    cdef cppclass RfOpcodes "rapidfuzz::Opcodes"

    cdef cppclass RfEditops "rapidfuzz::Editops":
        ctypedef size_t size_type
        ctypedef ptrdiff_t difference_type

        cppclass iterator:
            iterator() except +
            iterator(iterator&) except +
            RfEditOp& operator*()
            iterator operator++()
            iterator operator--()
            iterator operator++(int)
            iterator operator--(int)
            iterator operator+(size_type)
            iterator operator-(size_type)
            difference_type operator-(iterator)
            difference_type operator-(const_iterator)
            bint operator==(iterator)
            bint operator==(const_iterator)
            bint operator!=(iterator)
            bint operator!=(const_iterator)
            bint operator<(iterator)
            bint operator<(const_iterator)
            bint operator>(iterator)
            bint operator>(const_iterator)
            bint operator<=(iterator)
            bint operator<=(const_iterator)
            bint operator>=(iterator)
            bint operator>=(const_iterator)
        cppclass const_iterator:
            const_iterator() except +
            const_iterator(iterator&) except +
            const_iterator(const_iterator&) except +
            operator=(iterator&) except +
            const RfEditOp& operator*()
            const_iterator operator++()
            const_iterator operator--()
            const_iterator operator++(int)
            const_iterator operator--(int)
            const_iterator operator+(size_type)
            const_iterator operator-(size_type)
            difference_type operator-(iterator)
            difference_type operator-(const_iterator)
            bint operator==(iterator)
            bint operator==(const_iterator)
            bint operator!=(iterator)
            bint operator!=(const_iterator)
            bint operator<(iterator)
            bint operator<(const_iterator)
            bint operator>(iterator)
            bint operator>(const_iterator)
            bint operator<=(iterator)
            bint operator<=(const_iterator)
            bint operator>=(iterator)
            bint operator>=(const_iterator)

        RfEditops() except +
        RfEditops(size_t) except +
        RfEditops(const RfEditops&) except +
        RfEditops(const RfOpcodes&) except +
        bool operator==(const RfEditops&)
        RfEditOp& operator[](size_t pos) except +
        size_t size()
        RfEditops inverse() except +
        RfEditops remove_subsequence(const RfEditops& subsequence) except +
        RfEditops slice(int, int, int) except +
        void remove_slice(int, int, int) except +
        size_t get_src_len()
        void set_src_len(size_t)
        size_t get_dest_len()
        void set_dest_len(size_t)
        RfEditops reverse()
        void emplace_back(...)
        void reserve(size_t) except +
        void shrink_to_fit() except +

        iterator begin()
        iterator end()
        const_iterator cbegin()
        const_iterator cend()
        iterator erase(iterator)

    ctypedef struct RfOpcode "rapidfuzz::Opcode":
        EditType type
        size_t src_begin
        size_t src_end
        size_t dest_begin
        size_t dest_end

    cdef cppclass RfOpcodes "rapidfuzz::Opcodes":
        ctypedef size_t size_type
        ctypedef ptrdiff_t difference_type

        cppclass iterator:
            iterator() except +
            iterator(iterator&) except +
            RfOpcode& operator*()
            iterator operator++()
            iterator operator--()
            iterator operator++(int)
            iterator operator--(int)
            iterator operator+(size_type)
            iterator operator-(size_type)
            difference_type operator-(iterator)
            difference_type operator-(const_iterator)
            bint operator==(iterator)
            bint operator==(const_iterator)
            bint operator!=(iterator)
            bint operator!=(const_iterator)
            bint operator<(iterator)
            bint operator<(const_iterator)
            bint operator>(iterator)
            bint operator>(const_iterator)
            bint operator<=(iterator)
            bint operator<=(const_iterator)
            bint operator>=(iterator)
            bint operator>=(const_iterator)
        cppclass const_iterator:
            const_iterator() except +
            const_iterator(iterator&) except +
            const_iterator(const_iterator&) except +
            operator=(iterator&) except +
            const RfOpcode& operator*()
            const_iterator operator++()
            const_iterator operator--()
            const_iterator operator++(int)
            const_iterator operator--(int)
            const_iterator operator+(size_type)
            const_iterator operator-(size_type)
            difference_type operator-(iterator)
            difference_type operator-(const_iterator)
            bint operator==(iterator)
            bint operator==(const_iterator)
            bint operator!=(iterator)
            bint operator!=(const_iterator)
            bint operator<(iterator)
            bint operator<(const_iterator)
            bint operator>(iterator)
            bint operator>(const_iterator)
            bint operator<=(iterator)
            bint operator<=(const_iterator)
            bint operator>=(iterator)
            bint operator>=(const_iterator)

        RfOpcodes() except +
        RfOpcodes(size_t) except +
        RfOpcodes(const RfOpcodes&) except +
        RfOpcodes(const RfEditops&) except +
        bool operator==(const RfOpcodes&)
        RfOpcode& operator[](size_t pos) except +
        size_t size()
        RfOpcodes inverse() except +
        size_t get_src_len()
        void set_src_len(size_t)
        size_t get_dest_len()
        void set_dest_len(size_t)
        RfOpcodes reverse()
        void emplace_back(...)
        void reserve(size_t) except +
        RfOpcode& back() except +
        void shrink_to_fit() except +
        bint empty()

        iterator begin()
        iterator end()
        const_iterator cbegin()
        const_iterator cend()
        iterator erase(iterator)

cdef extern from "cpp_common.hpp":
    cdef cppclass RF_StringWrapper:
        RF_String string
        PyObject* obj

        RF_StringWrapper()
        RF_StringWrapper(RF_String)
        RF_StringWrapper(RF_String, object)

    cdef cppclass RF_KwargsWrapper:
        RF_Kwargs kwargs

        RF_KwargsWrapper()
        RF_KwargsWrapper(RF_Kwargs)

    cdef cppclass PyObjectWrapper:
        PyObject* obj

        PyObjectWrapper()
        PyObjectWrapper(object)

    void default_string_deinit(RF_String* string) nogil

    int is_valid_string(object py_str) except +
    RF_String convert_string(object py_str)
    void validate_string(object py_str, const char* err) except +

cdef inline bool hash_array(arr, RF_String* s_proc) except False:
    # TODO on Cpython this does not require any copies
    cdef Py_UCS4 typecode = <Py_UCS4>arr.typecode
    s_proc.length = <int64_t>len(arr)

    s_proc.data = malloc(s_proc.length * sizeof(uint64_t))

    if s_proc.data == NULL:
        raise MemoryError

    try:
        # ignore signed/unsigned, since it is not relevant in any of the algorithms
        if typecode in {'f', 'd'}: # float/double are hashed
            s_proc.kind = RF_StringType.RF_UINT64
            for i in range(s_proc.length):
                (<uint64_t*>s_proc.data)[i] = <uint64_t>hash(arr[i])
        elif typecode in ('u', 'w'): # 'u' wchar_t
            s_proc.kind = RF_StringType.RF_UINT64
            for i in range(s_proc.length):
                (<uint64_t*>s_proc.data)[i] = <uint64_t><Py_UCS4>arr[i]
        else:
            s_proc.kind = RF_StringType.RF_UINT64
            for i in range(s_proc.length):
                (<uint64_t*>s_proc.data)[i] = <uint64_t>arr[i]
    except Exception as e:
        free(s_proc.data)
        s_proc.data = NULL
        raise

    s_proc.dtor = default_string_deinit
    return True


cdef inline bool hash_sequence(seq, RF_String* s_proc) except False:
    s_proc.length = <int64_t>len(seq)

    s_proc.data = malloc(s_proc.length * sizeof(uint64_t))

    if s_proc.data == NULL:
        raise MemoryError

    try:
        s_proc.kind = RF_StringType.RF_UINT64
        for i in range(s_proc.length):
            elem = seq[i]
            # this is required so e.g. a list of char can be compared to a string
            if isinstance(elem, str) and len(elem) == 1:
                (<uint64_t*>s_proc.data)[i] = <uint64_t><Py_UCS4>elem
            elif isinstance(elem, int) and elem == -1:
                (<uint64_t*>s_proc.data)[i] = <uint64_t>-1
            else:
                (<uint64_t*>s_proc.data)[i] = <uint64_t>hash(elem)
    except Exception as e:
        free(s_proc.data)
        s_proc.data = NULL
        raise

    s_proc.dtor = default_string_deinit
    return True

cdef inline bool is_none(s) noexcept:
    if s is None or s is pandas_NA:
        return True

    if isinstance(s, float) and isnan(<double>s):
        return True

    return False

# todo we will probably want to clean up the various methods of
# converting strings. This has to be done carefully, since especially with preprocessor functions
# the none check is often required before calling the preprocessing functions to keep the current behaviour
cdef inline bool conv_sequence_with_none(seq, RF_String* c_seq) except False:
    if is_valid_string(seq):
        c_seq[0] = move(convert_string(seq))
    elif is_none(seq):
        c_seq.length = 0
        c_seq.data = NULL
    elif isinstance(seq, array):
        hash_array(seq, c_seq)
    else:
        hash_sequence(seq, c_seq)

    return True

cdef inline RF_String conv_sequence(seq) except *:
    cdef RF_String c_seq
    if is_valid_string(seq):
        c_seq = move(convert_string(seq))
    elif isinstance(seq, array):
        hash_array(seq, &c_seq)
    else:
        hash_sequence(seq, &c_seq)

    return move(c_seq)

cdef inline double get_score_cutoff_f64(score_cutoff, float worst_score, float optimal_score) except *:
    cdef float c_score_cutoff = worst_score

    if score_cutoff is not None:
        c_score_cutoff = score_cutoff
        if optimal_score > worst_score:
            # e.g. 0.0 - 100.0
            if c_score_cutoff < worst_score or c_score_cutoff > optimal_score:
                raise TypeError("score_cutoff has to be in the range of %s - %s" % (worst_score, optimal_score))
        else:
            # e.g. DBL_MAX - 0
            if c_score_cutoff > worst_score or c_score_cutoff < optimal_score:
                raise TypeError(f"score_cutoff has to be in the range of {optimal_score} - {worst_score}")

    return c_score_cutoff

cdef inline int64_t get_score_cutoff_i64(score_cutoff, int64_t worst_score, int64_t optimal_score) except *:
    cdef int64_t c_score_cutoff = worst_score

    if score_cutoff is not None:
        if optimal_score > worst_score:
            # e.g. 0.0 - 100.0
            if c_score_cutoff < worst_score or c_score_cutoff > optimal_score:
                raise TypeError(f"score_cutoff has to be in the range of {worst_score} - {optimal_score}")
        else:
            # e.g. DBL_MAX - 0
            if c_score_cutoff > worst_score or c_score_cutoff < optimal_score:
                raise TypeError(f"score_cutoff has to be in the range of {optimal_score} - {worst_score}")

    return c_score_cutoff

cdef inline size_t get_score_cutoff_size_t(score_cutoff, size_t worst_score, size_t optimal_score) except *:
    cdef uint64_t c_score_cutoff = worst_score

    if score_cutoff is not None:
        c_score_cutoff = score_cutoff
        if c_score_cutoff > SIZE_MAX:
            c_score_cutoff = SIZE_MAX

        if optimal_score > worst_score:
            # e.g. 0.0 - 100.0
            if c_score_cutoff < worst_score or c_score_cutoff > optimal_score:
                raise TypeError(f"score_cutoff has to be in the range of {worst_score} - {optimal_score}")
        else:
            # e.g. DBL_MAX - 0
            if c_score_cutoff > worst_score or c_score_cutoff < optimal_score:
                raise TypeError(f"score_cutoff has to be in the range of {optimal_score} - {worst_score}")

    return <size_t>c_score_cutoff

cdef inline bool preprocess_strings(s1, s2, processor, RF_StringWrapper* s1_proc, RF_StringWrapper* s2_proc) except False:
    cdef RF_Preprocessor* preprocess_context = NULL

    if not processor:
        s1_proc[0] = RF_StringWrapper(conv_sequence(s1))
        s2_proc[0] = RF_StringWrapper(conv_sequence(s2))
    else:
        processor_capsule = getattr(processor, '_RF_Preprocess', processor)
        if PyCapsule_IsValid(processor_capsule, NULL):
            preprocess_context = <RF_Preprocessor*>PyCapsule_GetPointer(processor_capsule, NULL)

        if preprocess_context != NULL and preprocess_context.version == 1:
            preprocess_context.preprocess(s1, &(s1_proc[0].string))
            preprocess_context.preprocess(s2, &(s2_proc[0].string))
        else:
            s1 = processor(s1)
            s1_proc[0] = RF_StringWrapper(conv_sequence(s1), s1)
            s2 = processor(s2)
            s2_proc[0] = RF_StringWrapper(conv_sequence(s2), s2)

    return True

cdef inline bool NoKwargsInit(RF_Kwargs* self, dict kwargs) except False:
    if len(kwargs):
        raise TypeError("Got unexpected keyword arguments: ", ", ".join(kwargs.keys()))

    self.context = NULL
    self.dtor = NULL
    return True

cdef inline RF_Scorer CreateScorerContext(RF_KwargsInit kwargs_init, RF_GetScorerFlags get_scorer_flags, RF_ScorerFuncInit scorer_func_init, RF_UncachedScorerFunc uncached_scorer_func) noexcept:
    cdef RF_Scorer context
    context.version = SCORER_STRUCT_VERSION
    context.kwargs_init = kwargs_init
    context.get_scorer_flags = get_scorer_flags
    context.scorer_func_init = scorer_func_init
    context.uncached_scorer_func = uncached_scorer_func
    return context

cdef inline void SetFuncAttrs(cpp_func, py_func) except *:
    cpp_func.__name__ = py_func.__name__
    cpp_func.__qualname__ = py_func.__qualname__
    cpp_func.__doc__ = py_func.__doc__

cdef inline void SetScorerAttrs(cpp_func, py_func, RF_Scorer* context) except *:
    SetFuncAttrs(cpp_func, py_func)
    cpp_func._RF_Scorer = PyCapsule_New(context, NULL, NULL)
    cpp_func._RF_ScorerPy = py_func._RF_ScorerPy

    # used to detect the function hasn't been wrapped afterwards
    cpp_func._RF_OriginalScorer = cpp_func

cdef inline RF_Preprocessor CreateProcessorContext(RF_Preprocess preprocess) except *:
    cdef RF_Preprocessor context
    context.version = PREPROCESSOR_STRUCT_VERSION
    context.preprocess = preprocess
    return context

cdef inline void SetProcessorAttrs(cpp_func, py_func, RF_Preprocessor* context) except *:
    SetFuncAttrs(cpp_func, py_func)
    cpp_func._RF_Preprocess = PyCapsule_New(context, NULL, NULL)
