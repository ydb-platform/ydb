#cython: freethreading_compatible = True

from rapidfuzz.fuzz import WRatio, ratio

from . import process_py

cimport cython
from cpp_common cimport (
    PyObjectWrapper,
    RF_KwargsWrapper,
    RF_StringWrapper,
    SetFuncAttrs,
    conv_sequence_with_none,
    conv_sequence,
    get_score_cutoff_f64,
    get_score_cutoff_i64,
    get_score_cutoff_size_t,
    is_none
)
from cpython cimport Py_buffer
from cpython.buffer cimport PyBUF_F_CONTIGUOUS, PyBUF_ND, PyBUF_SIMPLE
from cpython.exc cimport PyErr_CheckSignals
from cpython.list cimport PyList_New, PyList_SET_ITEM
from cpython.object cimport PyObject
from cpython.ref cimport Py_INCREF
from libcpp.cmath cimport floor, isnan
from libc.stdint cimport int32_t, int64_t, uint8_t, uint64_t
from libcpp cimport algorithm, bool
from libcpp.utility cimport move
from libcpp.vector cimport vector

import heapq
from array import array
import sys

from cpython.pycapsule cimport PyCapsule_GetPointer, PyCapsule_IsValid

from rapidfuzz cimport (
    RF_SCORER_FLAG_RESULT_F64,
    RF_SCORER_FLAG_RESULT_I64,
    RF_SCORER_FLAG_RESULT_SIZE_T,
    RF_SCORER_FLAG_SYMMETRIC,
    RF_SCORER_NONE_IS_WORST_SCORE,
    SCORER_STRUCT_VERSION,
    PREPROCESSOR_STRUCT_VERSION,
    RF_Kwargs,
    RF_Preprocess,
    RF_Preprocessor,
    RF_Scorer,
    RF_ScorerFlags,
    RF_ScorerFunc,
    RF_String,
)

pandas_NA = None

cdef inline void setupPandas() noexcept:
    global pandas_NA
    if pandas_NA is None:
        pandas = sys.modules.get('pandas')
        if hasattr(pandas, 'NA'):
            pandas_NA = pandas.NA

setupPandas()

cdef extern from "process_cpp.hpp":
    cdef cppclass ExtractComp:
        ExtractComp()
        ExtractComp(const RF_ScorerFlags* scorer_flags)

    cdef cppclass ListMatchElem[T]:
        T score
        int64_t index
        PyObjectWrapper choice

    cdef cppclass DictMatchElem[T]:
        T score
        int64_t index
        PyObjectWrapper choice
        PyObjectWrapper key

    cdef cppclass DictStringElem:
        DictStringElem()
        DictStringElem(int64_t index, PyObjectWrapper key, PyObjectWrapper val, RF_StringWrapper proc_val)

        int64_t index
        PyObjectWrapper key
        PyObjectWrapper val
        RF_StringWrapper proc_val

    cdef cppclass ListStringElem:
        ListStringElem()
        ListStringElem(int64_t index, PyObjectWrapper val, RF_StringWrapper proc_val)

        int64_t index
        PyObjectWrapper val
        RF_StringWrapper proc_val

    cdef cppclass RF_ScorerWrapper:
        RF_ScorerFunc scorer_func

        RF_ScorerWrapper()
        RF_ScorerWrapper(RF_ScorerFunc)

        void call(const RF_String*, double, double, double*) except +
        void call(const RF_String*, int64_t, int64_t, int64_t*) except +
        void call(const RF_String*, size_t, size_t, size_t*) except +

    cdef vector[DictMatchElem[T]] extract_dict_impl[T](
        const RF_Kwargs*, const RF_ScorerFlags*, RF_Scorer*,
        const RF_StringWrapper&, const vector[DictStringElem]&, T, T) except +

    cdef vector[ListMatchElem[T]] extract_list_impl[T](
        const RF_Kwargs*, const RF_ScorerFlags*, RF_Scorer*,
        const RF_StringWrapper&, const vector[ListStringElem]&, T, T) except +

    cdef bool is_lowest_score_worst[T](const RF_ScorerFlags* scorer_flags)
    cdef T get_optimal_score[T](const RF_ScorerFlags* scorer_flags)

    cdef enum class MatrixType:
        UNDEFINED = 0
        FLOAT32 = 1
        FLOAT64 = 2
        INT8 = 3
        INT16 = 4
        INT32 = 5
        INT64 = 6
        UINT8 = 7
        UINT16 = 8
        UINT32 = 9
        UINT64 = 10

    cdef cppclass RfMatrix "Matrix":
        RfMatrix() except +
        RfMatrix(MatrixType, size_t, size_t) except +
        int get_dtype_size() except +
        const char* get_format() except +
        void set[T](size_t, size_t, T) except +

        MatrixType m_dtype
        size_t m_rows
        size_t m_cols
        void* m_matrix
        bool vector_output

    RfMatrix cdist_single_list_impl[T](  const RF_ScorerFlags* scorer_flags, const RF_Kwargs*, RF_Scorer*,
        const vector[RF_StringWrapper]&, MatrixType, int, T, T, T) except +
    RfMatrix cdist_two_lists_impl[T](    const RF_ScorerFlags* scorer_flags, const RF_Kwargs*, RF_Scorer*,
        const vector[RF_StringWrapper]&, const vector[RF_StringWrapper]&, MatrixType, int, T, T, T) except +
    RfMatrix cpdist_cpp_impl[T](    const RF_Kwargs*, RF_Scorer*,
        const vector[RF_StringWrapper]&, const vector[RF_StringWrapper]&, MatrixType, int, T, T, T) except +


cdef inline vector[DictStringElem] preprocess_dict(queries, processor) except *:
    cdef vector[DictStringElem] proc_queries
    cdef int64_t queries_len = <int64_t>len(queries)
    cdef RF_String proc_str
    cdef RF_Preprocessor* processor_context = NULL
    proc_queries.reserve(queries_len)
    cdef int64_t i

    # No processor
    if not processor:
        for i, (query_key, query) in enumerate(queries.items()):
            conv_sequence_with_none(query, &proc_str)
            if proc_str.data == NULL:
                continue

            proc_queries.emplace_back(
                i,
                move(PyObjectWrapper(query_key)),
                move(PyObjectWrapper(query)),
                move(RF_StringWrapper(proc_str))
            )
    else:
        processor_capsule = getattr(processor, '_RF_Preprocess', processor)
        if PyCapsule_IsValid(processor_capsule, NULL):
            processor_context = <RF_Preprocessor*>PyCapsule_GetPointer(processor_capsule, NULL)

        # use RapidFuzz C-Api
        if processor_context != NULL and processor_context.version == PREPROCESSOR_STRUCT_VERSION:
            for i, (query_key, query) in enumerate(queries.items()):
                if is_none(query):
                    continue
                processor_context.preprocess(query, &proc_str)
                proc_queries.emplace_back(
                    i,
                    move(PyObjectWrapper(query_key)),
                    move(PyObjectWrapper(query)),
                    move(RF_StringWrapper(proc_str))
                )

        # Call Processor through Python
        else:
            for i, (query_key, query) in enumerate(queries.items()):
                if is_none(query):
                    continue
                proc_query = processor(query)
                proc_queries.emplace_back(
                    i,
                    move(PyObjectWrapper(query_key)),
                    move(PyObjectWrapper(query)),
                    move(RF_StringWrapper(conv_sequence(proc_query), proc_query))
                )

    return move(proc_queries)


cdef inline vector[ListStringElem] preprocess_list(queries, processor) except *:
    cdef vector[ListStringElem] proc_queries
    cdef int64_t queries_len = <int64_t>len(queries)
    cdef RF_String proc_str
    cdef RF_Preprocessor* processor_context = NULL
    proc_queries.reserve(queries_len)
    cdef int64_t i

    # No processor
    if not processor:
        for i, query in enumerate(queries):
            conv_sequence_with_none(query, &proc_str)
            if proc_str.data == NULL:
                continue

            proc_queries.emplace_back(
                i,
                move(PyObjectWrapper(query)),
                move(RF_StringWrapper(proc_str))
            )
    else:
        processor_capsule = getattr(processor, '_RF_Preprocess', processor)
        if PyCapsule_IsValid(processor_capsule, NULL):
            processor_context = <RF_Preprocessor*>PyCapsule_GetPointer(processor_capsule, NULL)

        # use RapidFuzz C-Api
        if processor_context != NULL and processor_context.version == PREPROCESSOR_STRUCT_VERSION:
            for i, query in enumerate(queries):
                if is_none(query):
                    continue
                processor_context.preprocess(query, &proc_str)
                proc_queries.emplace_back(
                    i,
                    move(PyObjectWrapper(query)),
                    move(RF_StringWrapper(proc_str))
                )

        # Call Processor through Python
        else:
            for i, query in enumerate(queries):
                if is_none(query):
                    continue
                proc_query = processor(query)
                proc_queries.emplace_back(
                    i,
                    move(PyObjectWrapper(query)),
                    move(RF_StringWrapper(conv_sequence(proc_query), proc_query))
                )

    return move(proc_queries)


cdef inline extractOne_dict_f64(
    query, choices, RF_Scorer* scorer, const RF_ScorerFlags* scorer_flags,
    processor,
    score_cutoff,
    score_hint,
    const RF_Kwargs* scorer_kwargs
):
    cdef RF_String proc_str
    cdef double score
    cdef Py_ssize_t i = 0
    cdef RF_Preprocessor* processor_context = NULL
    if processor:
        processor_capsule = getattr(processor, '_RF_Preprocess', processor)
        if PyCapsule_IsValid(processor_capsule, NULL):
            processor_context = <RF_Preprocessor*>PyCapsule_GetPointer(processor_capsule, NULL)

    cdef RF_StringWrapper proc_query = move(RF_StringWrapper(conv_sequence(query)))
    cdef double c_score_cutoff = get_score_cutoff_f64(score_cutoff, scorer_flags.worst_score.f64, scorer_flags.optimal_score.f64)
    cdef double c_score_hint = get_score_cutoff_f64(score_hint, scorer_flags.worst_score.f64, scorer_flags.optimal_score.f64)

    cdef RF_ScorerFunc scorer_func
    scorer.scorer_func_init(&scorer_func, scorer_kwargs, 1, &proc_query.string)
    cdef RF_ScorerWrapper ScorerFunc = RF_ScorerWrapper(scorer_func)

    cdef bool lowest_score_worst = is_lowest_score_worst[double](scorer_flags)
    cdef double optimal_score = get_optimal_score[double](scorer_flags)

    cdef bool result_found = False
    cdef double result_score = 0
    result_key = None
    result_choice = None

    for choice_key, choice in choices.items():
        if i % 1000 == 0:
            PyErr_CheckSignals()
        i += 1
        if is_none(choice):
            continue

        if processor is None:
            proc_choice = move(RF_StringWrapper(conv_sequence(choice)))
        elif processor_context != NULL and processor_context.version == PREPROCESSOR_STRUCT_VERSION:
            processor_context.preprocess(choice, &proc_str)
            proc_choice = move(RF_StringWrapper(proc_str))
        else:
            py_proc_choice = processor(choice)
            proc_choice = move(RF_StringWrapper(conv_sequence(py_proc_choice)))

        ScorerFunc.call(&proc_choice.string, c_score_cutoff, c_score_hint, &score)

        if lowest_score_worst:
            if score >= c_score_cutoff and (not result_found or score > result_score):
                result_score = c_score_cutoff = score
                result_choice = choice
                result_key = choice_key
                result_found = True
        else:
            if score <= c_score_cutoff and (not result_found or score < result_score):
                result_score = c_score_cutoff = score
                result_choice = choice
                result_key = choice_key
                result_found = True

        if score == optimal_score:
            break

    return (result_choice, result_score, result_key) if result_found else None


cdef inline extractOne_dict_i64(
    query, choices,
    RF_Scorer* scorer,
    const RF_ScorerFlags* scorer_flags,
    processor,
    score_cutoff,
    score_hint,
    const RF_Kwargs* scorer_kwargs
):
    cdef RF_String proc_str
    cdef int64_t score
    cdef Py_ssize_t i = 0
    cdef RF_Preprocessor* processor_context = NULL
    if processor:
        processor_capsule = getattr(processor, '_RF_Preprocess', processor)
        if PyCapsule_IsValid(processor_capsule, NULL):
            processor_context = <RF_Preprocessor*>PyCapsule_GetPointer(processor_capsule, NULL)

    cdef RF_StringWrapper proc_query = move(RF_StringWrapper(conv_sequence(query)))
    cdef int64_t c_score_cutoff = get_score_cutoff_i64(score_cutoff, scorer_flags.worst_score.i64, scorer_flags.optimal_score.i64)
    cdef int64_t c_score_hint = get_score_cutoff_i64(score_hint, scorer_flags.worst_score.i64, scorer_flags.optimal_score.i64)

    cdef RF_ScorerFunc scorer_func
    scorer.scorer_func_init(&scorer_func, scorer_kwargs, 1, &proc_query.string)
    cdef RF_ScorerWrapper ScorerFunc = RF_ScorerWrapper(scorer_func)

    cdef bool lowest_score_worst = is_lowest_score_worst[int64_t](scorer_flags)
    cdef int64_t optimal_score = get_optimal_score[int64_t](scorer_flags)

    cdef bool result_found = False
    cdef int64_t result_score = 0
    result_key = None
    result_choice = None

    for choice_key, choice in choices.items():
        if i % 1000 == 0:
            PyErr_CheckSignals()
        i += 1
        if is_none(choice):
            continue

        if processor is None:
            proc_choice = move(RF_StringWrapper(conv_sequence(choice)))
        elif processor_context != NULL and processor_context.version == PREPROCESSOR_STRUCT_VERSION:
            processor_context.preprocess(choice, &proc_str)
            proc_choice = move(RF_StringWrapper(proc_str))
        else:
            py_proc_choice = processor(choice)
            proc_choice = move(RF_StringWrapper(conv_sequence(py_proc_choice)))

        ScorerFunc.call(&proc_choice.string, c_score_cutoff, c_score_hint, &score)

        if lowest_score_worst:
            if score >= c_score_cutoff and (not result_found or score > result_score):
                result_score = c_score_cutoff = score
                result_choice = choice
                result_key = choice_key
                result_found = True
        else:
            if score <= c_score_cutoff and (not result_found or score < result_score):
                result_score = c_score_cutoff = score
                result_choice = choice
                result_key = choice_key
                result_found = True

        if score == optimal_score:
            break

    return (result_choice, result_score, result_key) if result_found else None


cdef inline extractOne_dict_size_t(
    query, choices,
    RF_Scorer* scorer,
    const RF_ScorerFlags* scorer_flags,
    processor,
    score_cutoff,
    score_hint,
    const RF_Kwargs* scorer_kwargs
):
    cdef RF_String proc_str
    cdef size_t score
    cdef Py_ssize_t i = 0
    cdef RF_Preprocessor* processor_context = NULL
    if processor:
        processor_capsule = getattr(processor, '_RF_Preprocess', processor)
        if PyCapsule_IsValid(processor_capsule, NULL):
            processor_context = <RF_Preprocessor*>PyCapsule_GetPointer(processor_capsule, NULL)

    cdef RF_StringWrapper proc_query = move(RF_StringWrapper(conv_sequence(query)))
    cdef size_t c_score_cutoff = get_score_cutoff_size_t(score_cutoff, scorer_flags.worst_score.sizet, scorer_flags.optimal_score.sizet)
    cdef size_t c_score_hint = get_score_cutoff_size_t(score_hint, scorer_flags.worst_score.sizet, scorer_flags.optimal_score.sizet)

    cdef RF_ScorerFunc scorer_func
    scorer.scorer_func_init(&scorer_func, scorer_kwargs, 1, &proc_query.string)
    cdef RF_ScorerWrapper ScorerFunc = RF_ScorerWrapper(scorer_func)

    cdef bool lowest_score_worst = is_lowest_score_worst[size_t](scorer_flags)
    cdef size_t optimal_score = get_optimal_score[size_t](scorer_flags)

    cdef bool result_found = False
    cdef size_t result_score = 0
    result_key = None
    result_choice = None

    for choice_key, choice in choices.items():
        if i % 1000 == 0:
            PyErr_CheckSignals()
        i += 1
        if is_none(choice):
            continue

        if processor is None:
            proc_choice = move(RF_StringWrapper(conv_sequence(choice)))
        elif processor_context != NULL and processor_context.version == PREPROCESSOR_STRUCT_VERSION:
            processor_context.preprocess(choice, &proc_str)
            proc_choice = move(RF_StringWrapper(proc_str))
        else:
            py_proc_choice = processor(choice)
            proc_choice = move(RF_StringWrapper(conv_sequence(py_proc_choice)))

        ScorerFunc.call(&proc_choice.string, c_score_cutoff, c_score_hint, &score)

        if lowest_score_worst:
            if score >= c_score_cutoff and (not result_found or score > result_score):
                result_score = c_score_cutoff = score
                result_choice = choice
                result_key = choice_key
                result_found = True
        else:
            if score <= c_score_cutoff and (not result_found or score < result_score):
                result_score = c_score_cutoff = score
                result_choice = choice
                result_key = choice_key
                result_found = True

        if score == optimal_score:
            break

    return (result_choice, result_score, result_key) if result_found else None


cdef inline extractOne_dict(
    query, choices,
    RF_Scorer* scorer,
    const RF_ScorerFlags* scorer_flags,
    processor,
    score_cutoff,
    score_hint,
    const RF_Kwargs* scorer_kwargs
):
    flags = scorer_flags.flags

    if flags & RF_SCORER_FLAG_RESULT_F64:
        return extractOne_dict_f64(
            query, choices, scorer, scorer_flags, processor, score_cutoff, score_hint, scorer_kwargs
        )
    elif flags & RF_SCORER_FLAG_RESULT_SIZE_T:
        return extractOne_dict_size_t(
            query, choices, scorer, scorer_flags, processor, score_cutoff, score_hint, scorer_kwargs
        )
    elif flags & RF_SCORER_FLAG_RESULT_I64:
        return extractOne_dict_i64(
            query, choices, scorer, scorer_flags, processor, score_cutoff, score_hint, scorer_kwargs
        )

    raise ValueError("scorer does not properly use the C-API")

cdef inline extractOne_list_f64(
    query, choices,
    RF_Scorer* scorer,
    const RF_ScorerFlags* scorer_flags,
    processor,
    score_cutoff,
    score_hint,
    const RF_Kwargs* scorer_kwargs
):
    cdef RF_String proc_str
    cdef double score
    cdef Py_ssize_t i
    cdef RF_Preprocessor* processor_context = NULL
    if processor:
        processor_capsule = getattr(processor, '_RF_Preprocess', processor)
        if PyCapsule_IsValid(processor_capsule, NULL):
            processor_context = <RF_Preprocessor*>PyCapsule_GetPointer(processor_capsule, NULL)

    cdef RF_StringWrapper proc_query = move(RF_StringWrapper(conv_sequence(query)))
    cdef double c_score_cutoff = get_score_cutoff_f64(score_cutoff, scorer_flags.worst_score.f64, scorer_flags.optimal_score.f64)
    cdef double c_score_hint = get_score_cutoff_f64(score_hint, scorer_flags.worst_score.f64, scorer_flags.optimal_score.f64)

    cdef RF_ScorerFunc scorer_func
    scorer.scorer_func_init(&scorer_func, scorer_kwargs, 1, &proc_query.string)
    cdef RF_ScorerWrapper ScorerFunc = RF_ScorerWrapper(scorer_func)

    cdef bool lowest_score_worst = is_lowest_score_worst[double](scorer_flags)
    cdef double optimal_score = get_optimal_score[double](scorer_flags)

    cdef bool result_found = False
    cdef double result_score = 0
    cdef Py_ssize_t result_index = 0
    result_choice = None

    for i, choice in enumerate(choices):
        if i % 1000 == 0:
            PyErr_CheckSignals()
        if is_none(choice):
            continue

        if processor is None:
            proc_choice = move(RF_StringWrapper(conv_sequence(choice)))
        elif processor_context != NULL and processor_context.version == PREPROCESSOR_STRUCT_VERSION:
            processor_context.preprocess(choice, &proc_str)
            proc_choice = move(RF_StringWrapper(proc_str))
        else:
            py_proc_choice = processor(choice)
            proc_choice = move(RF_StringWrapper(conv_sequence(py_proc_choice)))

        ScorerFunc.call(&proc_choice.string, c_score_cutoff, c_score_hint, &score)

        if lowest_score_worst:
            if score >= c_score_cutoff and (not result_found or score > result_score):
                result_score = c_score_cutoff = score
                result_choice = choice
                result_index = i
                result_found = True
        else:
            if score <= c_score_cutoff and (not result_found or score < result_score):
                result_score = c_score_cutoff = score
                result_choice = choice
                result_index = i
                result_found = True

        if score == optimal_score:
            break

    return (result_choice, result_score, result_index) if result_found else None

cdef inline extractOne_list_i64(
    query, choices,
    RF_Scorer* scorer,
    const RF_ScorerFlags* scorer_flags,
    processor,
    score_cutoff,
    score_hint,
    const RF_Kwargs* scorer_kwargs
):
    cdef RF_String proc_str
    cdef int64_t score
    cdef Py_ssize_t i
    cdef RF_Preprocessor* processor_context = NULL
    if processor:
        processor_capsule = getattr(processor, '_RF_Preprocess', processor)
        if PyCapsule_IsValid(processor_capsule, NULL):
            processor_context = <RF_Preprocessor*>PyCapsule_GetPointer(processor_capsule, NULL)

    cdef RF_StringWrapper proc_query = move(RF_StringWrapper(conv_sequence(query)))
    cdef int64_t c_score_cutoff = get_score_cutoff_i64(score_cutoff, scorer_flags.worst_score.i64, scorer_flags.optimal_score.i64)
    cdef int64_t c_score_hint = get_score_cutoff_i64(score_hint, scorer_flags.worst_score.i64, scorer_flags.optimal_score.i64)

    cdef RF_ScorerFunc scorer_func
    scorer.scorer_func_init(&scorer_func, scorer_kwargs, 1, &proc_query.string)
    cdef RF_ScorerWrapper ScorerFunc = RF_ScorerWrapper(scorer_func)

    cdef bool lowest_score_worst = is_lowest_score_worst[int64_t](scorer_flags)
    cdef int64_t optimal_score = get_optimal_score[int64_t](scorer_flags)

    cdef bool result_found = False
    cdef int64_t result_score = 0
    cdef Py_ssize_t result_index = 0
    result_choice = None

    for i, choice in enumerate(choices):
        if i % 1000 == 0:
            PyErr_CheckSignals()
        if is_none(choice):
            continue

        if processor is None:
            proc_choice = move(RF_StringWrapper(conv_sequence(choice)))
        elif processor_context != NULL and processor_context.version == PREPROCESSOR_STRUCT_VERSION:
            processor_context.preprocess(choice, &proc_str)
            proc_choice = move(RF_StringWrapper(proc_str))
        else:
            py_proc_choice = processor(choice)
            proc_choice = move(RF_StringWrapper(conv_sequence(py_proc_choice)))

        ScorerFunc.call(&proc_choice.string, c_score_cutoff, c_score_hint, &score)

        if lowest_score_worst:
            if score >= c_score_cutoff and (not result_found or score > result_score):
                result_score = c_score_cutoff = score
                result_choice = choice
                result_index = i
                result_found = True
        else:
            if score <= c_score_cutoff and (not result_found or score < result_score):
                result_score = c_score_cutoff = score
                result_choice = choice
                result_index = i
                result_found = True

        if score == optimal_score:
            break

    return (result_choice, result_score, result_index) if result_found else None

cdef inline extractOne_list_size_t(
    query, choices,
    RF_Scorer* scorer,
    const RF_ScorerFlags* scorer_flags,
    processor,
    score_cutoff,
    score_hint,
    const RF_Kwargs* scorer_kwargs
):
    cdef RF_String proc_str
    cdef size_t score
    cdef Py_ssize_t i
    cdef RF_Preprocessor* processor_context = NULL
    if processor:
        processor_capsule = getattr(processor, '_RF_Preprocess', processor)
        if PyCapsule_IsValid(processor_capsule, NULL):
            processor_context = <RF_Preprocessor*>PyCapsule_GetPointer(processor_capsule, NULL)

    cdef RF_StringWrapper proc_query = move(RF_StringWrapper(conv_sequence(query)))
    cdef size_t c_score_cutoff = get_score_cutoff_size_t(score_cutoff, scorer_flags.worst_score.sizet, scorer_flags.optimal_score.sizet)
    cdef size_t c_score_hint = get_score_cutoff_size_t(score_hint, scorer_flags.worst_score.sizet, scorer_flags.optimal_score.sizet)

    cdef RF_ScorerFunc scorer_func
    scorer.scorer_func_init(&scorer_func, scorer_kwargs, 1, &proc_query.string)
    cdef RF_ScorerWrapper ScorerFunc = RF_ScorerWrapper(scorer_func)

    cdef bool lowest_score_worst = is_lowest_score_worst[size_t](scorer_flags)
    cdef size_t optimal_score = get_optimal_score[size_t](scorer_flags)

    cdef bool result_found = False
    cdef size_t result_score = 0
    cdef Py_ssize_t result_index = 0
    result_choice = None

    for i, choice in enumerate(choices):
        if i % 1000 == 0:
            PyErr_CheckSignals()
        if is_none(choice):
            continue

        if processor is None:
            proc_choice = move(RF_StringWrapper(conv_sequence(choice)))
        elif processor_context != NULL and processor_context.version == PREPROCESSOR_STRUCT_VERSION:
            processor_context.preprocess(choice, &proc_str)
            proc_choice = move(RF_StringWrapper(proc_str))
        else:
            py_proc_choice = processor(choice)
            proc_choice = move(RF_StringWrapper(conv_sequence(py_proc_choice)))

        ScorerFunc.call(&proc_choice.string, c_score_cutoff, c_score_hint, &score)

        if lowest_score_worst:
            if score >= c_score_cutoff and (not result_found or score > result_score):
                result_score = c_score_cutoff = score
                result_choice = choice
                result_index = i
                result_found = True
        else:
            if score <= c_score_cutoff and (not result_found or score < result_score):
                result_score = c_score_cutoff = score
                result_choice = choice
                result_index = i
                result_found = True

        if score == optimal_score:
            break

    return (result_choice, result_score, result_index) if result_found else None

cdef inline extractOne_list(
    query, choices,
    RF_Scorer* scorer,
    const RF_ScorerFlags* scorer_flags,
    processor,
    score_cutoff,
    score_hint,
    const RF_Kwargs* scorer_kwargs
):
    flags = scorer_flags.flags

    if flags & RF_SCORER_FLAG_RESULT_F64:
        return extractOne_list_f64(
            query, choices, scorer, scorer_flags, processor, score_cutoff, score_hint, scorer_kwargs
        )
    elif flags & RF_SCORER_FLAG_RESULT_SIZE_T:
        return extractOne_list_size_t(
            query, choices, scorer, scorer_flags, processor, score_cutoff, score_hint, scorer_kwargs
        )
    elif flags & RF_SCORER_FLAG_RESULT_I64:
        return extractOne_list_i64(
            query, choices, scorer, scorer_flags, processor, score_cutoff, score_hint, scorer_kwargs
        )

    raise ValueError("scorer does not properly use the C-API")


cdef inline get_scorer_flags_py(scorer, dict scorer_kwargs):
    params = getattr(scorer, '_RF_ScorerPy', None)
    if params is not None:
        flags = params["get_scorer_flags"](**scorer_kwargs)
        return (flags["worst_score"], flags["optimal_score"])
    return (0, 100)

cdef inline py_extractOne_dict(query, choices, scorer, processor, double score_cutoff, worst_score, optimal_score, dict scorer_kwargs):
    cdef bool lowest_score_worst = optimal_score > worst_score
    cdef bool result_found = False
    result_score = 0
    result_choice = None
    result_key = None

    for choice_key, choice in choices.items():
        if is_none(choice):
            continue

        if processor is not None:
            score = scorer(query, processor(choice), **scorer_kwargs)
        else:
            score = scorer(query, choice, **scorer_kwargs)

        if lowest_score_worst:
            if score >= score_cutoff and (not result_found or score > result_score):
                score_cutoff = score
                scorer_kwargs["score_cutoff"] = score
                result_score = score
                result_choice = choice
                result_key = choice_key
                result_found = True
        else:
            if score <= score_cutoff and (not result_found or score < result_score):
                score_cutoff = score
                scorer_kwargs["score_cutoff"] = score
                result_score = score
                result_choice = choice
                result_key = choice_key
                result_found = True

        if score == optimal_score:
            break

    return (result_choice, result_score, result_key) if result_choice is not None else None


cdef inline py_extractOne_list(query, choices, scorer, processor, double score_cutoff, worst_score, optimal_score, dict scorer_kwargs):
    cdef bool lowest_score_worst = optimal_score > worst_score
    cdef bool result_found = False
    cdef int64_t result_index = 0
    cdef int64_t i
    result_score = 0
    result_choice = None

    for i, choice in enumerate(choices):
        if is_none(choice):
            continue

        if processor is not None:
            score = scorer(query, processor(choice), **scorer_kwargs)
        else:
            score = scorer(query, choice, **scorer_kwargs)

        if lowest_score_worst:
            if score >= score_cutoff and (not result_found or score > result_score):
                score_cutoff = score
                scorer_kwargs["score_cutoff"] = score
                result_score = score
                result_choice = choice
                result_index = i
                result_found = True
        else:
            if score <= score_cutoff and (not result_found or score < result_score):
                score_cutoff = score
                scorer_kwargs["score_cutoff"] = score
                result_score = score
                result_choice = choice
                result_index = i
                result_found = True

        if score == optimal_score:
            break

    return (result_choice, result_score, result_index) if result_choice is not None else None


def extractOne(query, choices, *, scorer=WRatio, processor=None, score_cutoff=None, score_hint=None, scorer_kwargs=None):
    cdef RF_Scorer* scorer_context = NULL
    cdef RF_ScorerFlags scorer_flags

    scorer_kwargs = scorer_kwargs.copy() if scorer_kwargs else {}

    setupPandas()

    if is_none(query):
        return None

    # preprocess the query
    if callable(processor):
        query = processor(query)


    scorer_capsule = getattr(scorer, '_RF_Scorer', scorer)
    if PyCapsule_IsValid(scorer_capsule, NULL):
        scorer_context = <RF_Scorer*>PyCapsule_GetPointer(scorer_capsule, NULL)

    if scorer_context and scorer_context.version == SCORER_STRUCT_VERSION:
        kwargs_context = RF_KwargsWrapper()
        scorer_context.kwargs_init(&kwargs_context.kwargs, scorer_kwargs)
        scorer_context.get_scorer_flags(&kwargs_context.kwargs, &scorer_flags)

        if hasattr(choices, "items"):
            return extractOne_dict(query, choices, scorer_context, &scorer_flags,
                processor, score_cutoff, score_hint, &kwargs_context.kwargs)
        else:
            return extractOne_list(query, choices, scorer_context, &scorer_flags,
                processor, score_cutoff, score_hint, &kwargs_context.kwargs)


    worst_score, optimal_score = get_scorer_flags_py(scorer, scorer_kwargs)
    # the scorer has to be called through Python
    if score_cutoff is None:
        score_cutoff = worst_score

    scorer_kwargs["score_cutoff"] = score_cutoff

    if hasattr(choices, "items"):
        return py_extractOne_dict(query, choices, scorer, processor, score_cutoff, worst_score, optimal_score, scorer_kwargs)
    else:
        return py_extractOne_list(query, choices, scorer, processor, score_cutoff, worst_score, optimal_score, scorer_kwargs)

SetFuncAttrs(extractOne, process_py.extractOne)

cdef inline extract_dict_f64(
    query, choices,
    RF_Scorer* scorer,
    const RF_ScorerFlags* scorer_flags,
    processor,
    int64_t limit,
    score_cutoff,
    score_hint,
    const RF_Kwargs* scorer_kwargs
):
    proc_query = move(RF_StringWrapper(conv_sequence(query)))
    proc_choices = preprocess_dict(choices, processor)

    cdef vector[DictMatchElem[double]] results = extract_dict_impl[double](
        scorer_kwargs, scorer_flags, scorer, proc_query, proc_choices,
        get_score_cutoff_f64(score_cutoff, scorer_flags.worst_score.f64, scorer_flags.optimal_score.f64),
        get_score_cutoff_f64(score_hint, scorer_flags.worst_score.f64, scorer_flags.optimal_score.f64)
    )

    # due to score_cutoff not always completely filled
    if limit > <int64_t>results.size():
        limit = <int64_t>results.size()

    if limit >= <int64_t>results.size():
        algorithm.sort(results.begin(), results.end(), ExtractComp(scorer_flags))
    else:
        algorithm.partial_sort(results.begin(), results.begin() + <ptrdiff_t>limit, results.end(), ExtractComp(scorer_flags))
        results.resize(limit)

    # copy elements into Python List
    result_list = PyList_New(<Py_ssize_t>limit)
    for i in range(limit):
        result_item = (<object>results[i].choice.obj, results[i].score, <object>results[i].key.obj)
        Py_INCREF(result_item)
        PyList_SET_ITEM(result_list, <Py_ssize_t>i, result_item)

    return result_list


cdef inline extract_dict_i64(
    query, choices,
    RF_Scorer* scorer,
    const RF_ScorerFlags* scorer_flags,
    processor,
    int64_t limit,
    score_cutoff,
    score_hint,
    const RF_Kwargs* scorer_kwargs
):
    proc_query = move(RF_StringWrapper(conv_sequence(query)))
    proc_choices = preprocess_dict(choices, processor)

    cdef vector[DictMatchElem[int64_t]] results = extract_dict_impl[int64_t](
        scorer_kwargs, scorer_flags, scorer, proc_query, proc_choices,
        get_score_cutoff_i64(score_cutoff, scorer_flags.worst_score.i64, scorer_flags.optimal_score.i64),
        get_score_cutoff_i64(score_hint, scorer_flags.worst_score.i64, scorer_flags.optimal_score.i64)
    )

    # due to score_cutoff not always completely filled
    if limit > <int64_t>results.size():
        limit = <int64_t>results.size()

    if limit >= <int64_t>results.size():
        algorithm.sort(results.begin(), results.end(), ExtractComp(scorer_flags))
    else:
        algorithm.partial_sort(results.begin(), results.begin() + <ptrdiff_t>limit, results.end(), ExtractComp(scorer_flags))
        results.resize(limit)

    # copy elements into Python List
    result_list = PyList_New(<Py_ssize_t>limit)
    for i in range(limit):
        result_item = (<object>results[i].choice.obj, results[i].score, <object>results[i].key.obj)
        Py_INCREF(result_item)
        PyList_SET_ITEM(result_list, <Py_ssize_t>i, result_item)

    return result_list


cdef inline extract_dict_size_t(
    query, choices,
    RF_Scorer* scorer,
    const RF_ScorerFlags* scorer_flags,
    processor,
    int64_t limit,
    score_cutoff,
    score_hint,
    const RF_Kwargs* scorer_kwargs
):
    proc_query = move(RF_StringWrapper(conv_sequence(query)))
    proc_choices = preprocess_dict(choices, processor)

    cdef vector[DictMatchElem[size_t]] results = extract_dict_impl[size_t](
        scorer_kwargs, scorer_flags, scorer, proc_query, proc_choices,
        get_score_cutoff_size_t(score_cutoff, scorer_flags.worst_score.sizet, scorer_flags.optimal_score.sizet),
        get_score_cutoff_size_t(score_hint, scorer_flags.worst_score.sizet, scorer_flags.optimal_score.sizet)
    )

    # due to score_cutoff not always completely filled
    if limit > <int64_t>results.size():
        limit = <int64_t>results.size()

    if limit >= <int64_t>results.size():
        algorithm.sort(results.begin(), results.end(), ExtractComp(scorer_flags))
    else:
        algorithm.partial_sort(results.begin(), results.begin() + <ptrdiff_t>limit, results.end(), ExtractComp(scorer_flags))
        results.resize(limit)

    # copy elements into Python List
    result_list = PyList_New(<Py_ssize_t>limit)
    for i in range(limit):
        result_item = (<object>results[i].choice.obj, results[i].score, <object>results[i].key.obj)
        Py_INCREF(result_item)
        PyList_SET_ITEM(result_list, <Py_ssize_t>i, result_item)

    return result_list


cdef inline extract_dict(
    query, choices,
    RF_Scorer* scorer,
    const RF_ScorerFlags* scorer_flags,
    processor,
    int64_t limit,
    score_cutoff,
    score_hint,
    const RF_Kwargs* scorer_kwargs
):
    flags = scorer_flags.flags

    if flags & RF_SCORER_FLAG_RESULT_F64:
        return extract_dict_f64(
            query, choices, scorer, scorer_flags, processor, limit, score_cutoff, score_hint, scorer_kwargs
        )
    elif flags & RF_SCORER_FLAG_RESULT_SIZE_T:
        return extract_dict_size_t(
            query, choices, scorer, scorer_flags, processor, limit, score_cutoff, score_hint, scorer_kwargs
        )
    elif flags & RF_SCORER_FLAG_RESULT_I64:
        return extract_dict_i64(
            query, choices, scorer, scorer_flags, processor, limit, score_cutoff, score_hint, scorer_kwargs
        )

    raise ValueError("scorer does not properly use the C-API")


cdef inline extract_list_f64(
    query, choices,
    RF_Scorer* scorer,
    const RF_ScorerFlags* scorer_flags,
    processor,
    int64_t limit,
    score_cutoff,
    score_hint,
    const RF_Kwargs* scorer_kwargs
):
    proc_query = move(RF_StringWrapper(conv_sequence(query)))
    proc_choices = preprocess_list(choices, processor)

    cdef vector[ListMatchElem[double]] results = extract_list_impl[double](
        scorer_kwargs, scorer_flags, scorer, proc_query, proc_choices,
        get_score_cutoff_f64(score_cutoff, scorer_flags.worst_score.f64, scorer_flags.optimal_score.f64),
        get_score_cutoff_f64(score_hint, scorer_flags.worst_score.f64, scorer_flags.optimal_score.f64)
    )

    # due to score_cutoff not always completely filled
    if limit > <int64_t>results.size():
        limit = <int64_t>results.size()

    if limit >= <int64_t>results.size():
        algorithm.sort(results.begin(), results.end(), ExtractComp(scorer_flags))
    else:
        algorithm.partial_sort(results.begin(), results.begin() + <ptrdiff_t>limit, results.end(), ExtractComp(scorer_flags))
        results.resize(limit)

    # copy elements into Python List
    result_list = PyList_New(<Py_ssize_t>limit)
    for i in range(limit):
        result_item = (<object>results[i].choice.obj, results[i].score, results[i].index)
        Py_INCREF(result_item)
        PyList_SET_ITEM(result_list, <Py_ssize_t>i, result_item)

    return result_list


cdef inline extract_list_i64(
    query, choices,
    RF_Scorer* scorer,
    const RF_ScorerFlags* scorer_flags,
    processor,
    int64_t limit,
    score_cutoff,
    score_hint,
    const RF_Kwargs* scorer_kwargs
):
    proc_query = move(RF_StringWrapper(conv_sequence(query)))
    proc_choices = preprocess_list(choices, processor)

    cdef vector[ListMatchElem[int64_t]] results = extract_list_impl[int64_t](
        scorer_kwargs, scorer_flags, scorer, proc_query, proc_choices,
        get_score_cutoff_i64(score_cutoff, scorer_flags.worst_score.i64, scorer_flags.optimal_score.i64),
        get_score_cutoff_i64(score_hint, scorer_flags.worst_score.i64, scorer_flags.optimal_score.i64)
    )

    # due to score_cutoff not always completely filled
    if limit > <int64_t>results.size():
        limit = <int64_t>results.size()

    if limit >= <int64_t>results.size():
        algorithm.sort(results.begin(), results.end(), ExtractComp(scorer_flags))
    else:
        algorithm.partial_sort(results.begin(), results.begin() + <ptrdiff_t>limit, results.end(), ExtractComp(scorer_flags))
        results.resize(limit)

    # copy elements into Python List
    result_list = PyList_New(<Py_ssize_t>limit)
    for i in range(limit):
        result_item = (<object>results[i].choice.obj, results[i].score, results[i].index)
        Py_INCREF(result_item)
        PyList_SET_ITEM(result_list, <Py_ssize_t>i, result_item)

    return result_list


cdef inline extract_list_size_t(
    query, choices,
    RF_Scorer* scorer,
    const RF_ScorerFlags* scorer_flags,
    processor,
    int64_t limit,
    score_cutoff,
    score_hint,
    const RF_Kwargs* scorer_kwargs
):
    proc_query = move(RF_StringWrapper(conv_sequence(query)))
    proc_choices = preprocess_list(choices, processor)

    cdef vector[ListMatchElem[size_t]] results = extract_list_impl[size_t](
        scorer_kwargs, scorer_flags, scorer, proc_query, proc_choices,
        get_score_cutoff_size_t(score_cutoff, scorer_flags.worst_score.sizet, scorer_flags.optimal_score.sizet),
        get_score_cutoff_size_t(score_hint, scorer_flags.worst_score.sizet, scorer_flags.optimal_score.sizet)
    )

    # due to score_cutoff not always completely filled
    if limit > <int64_t>results.size():
        limit = <int64_t>results.size()

    if limit >= <int64_t>results.size():
        algorithm.sort(results.begin(), results.end(), ExtractComp(scorer_flags))
    else:
        algorithm.partial_sort(results.begin(), results.begin() + <ptrdiff_t>limit, results.end(), ExtractComp(scorer_flags))
        results.resize(limit)

    # copy elements into Python List
    result_list = PyList_New(<Py_ssize_t>limit)
    for i in range(limit):
        result_item = (<object>results[i].choice.obj, results[i].score, results[i].index)
        Py_INCREF(result_item)
        PyList_SET_ITEM(result_list, <Py_ssize_t>i, result_item)

    return result_list


cdef inline extract_list(
    query, choices,
    RF_Scorer* scorer,
    const RF_ScorerFlags* scorer_flags,
    processor,
    int64_t limit,
    score_cutoff,
    score_hint,
    const RF_Kwargs* scorer_kwargs
):
    flags = scorer_flags.flags

    if flags & RF_SCORER_FLAG_RESULT_F64:
        return extract_list_f64(
            query, choices, scorer, scorer_flags, processor, limit, score_cutoff, score_hint, scorer_kwargs
        )
    elif flags & RF_SCORER_FLAG_RESULT_SIZE_T:
        return extract_list_size_t(
            query, choices, scorer, scorer_flags, processor, limit, score_cutoff, score_hint, scorer_kwargs
        )
    elif flags & RF_SCORER_FLAG_RESULT_I64:
        return extract_list_i64(
            query, choices, scorer, scorer_flags, processor, limit, score_cutoff, score_hint, scorer_kwargs
        )

    raise ValueError("scorer does not properly use the C-API")


cdef inline py_extract_dict(query, choices, scorer, processor, int64_t limit, double score_cutoff, worst_score, optimal_score, dict scorer_kwargs):
    cdef bool lowest_score_worst = optimal_score > worst_score
    cdef object score = None
    cdef list result_list = []

    for choice_key, choice in choices.items():
        if is_none(choice):
            continue

        if processor is not None:
            score = scorer(query, processor(choice), **scorer_kwargs)
        else:
            score = scorer(query, choice, **scorer_kwargs)

        if lowest_score_worst:
            if score >= score_cutoff:
                result_list.append((choice, score, choice_key))
        else:
            if score <= score_cutoff:
                result_list.append((choice, score, choice_key))

    if lowest_score_worst:
        return heapq.nlargest(limit, result_list, key=lambda i: i[1])
    else:
        return heapq.nsmallest(limit, result_list, key=lambda i: i[1])


cdef inline py_extract_list(query, choices, scorer, processor, int64_t limit, double score_cutoff, worst_score, optimal_score, dict scorer_kwargs):
    cdef bool lowest_score_worst = optimal_score > worst_score
    cdef object score = None
    cdef list result_list = []
    cdef int64_t i

    for i, choice in enumerate(choices):
        if is_none(choice):
            continue

        if processor is not None:
            score = scorer(query, processor(choice), **scorer_kwargs)
        else:
            score = scorer(query, choice, **scorer_kwargs)

        if lowest_score_worst:
            if score >= score_cutoff:
                result_list.append((choice, score, i))
        else:
            if score <= score_cutoff:
                result_list.append((choice, score, i))

    if lowest_score_worst:
        return heapq.nlargest(limit, result_list, key=lambda i: i[1])
    else:
        return heapq.nsmallest(limit, result_list, key=lambda i: i[1])


def extract(query, choices, *, scorer=WRatio, processor=None, limit=5, score_cutoff=None, score_hint=None, scorer_kwargs=None):
    cdef RF_Scorer* scorer_context = NULL
    cdef RF_ScorerFlags scorer_flags
    cdef int64_t c_limit
    cdef int64_t choices_len
    scorer_kwargs = scorer_kwargs.copy() if scorer_kwargs else {}

    setupPandas()

    if is_none(query):
        return []

    try:
        choices_len = <int64_t>len(choices)
    except TypeError:
        # handle generators. In Theory we could retrieve the length later on while
        # preprocessing the choices, but this is good enough for now
        choices = list(choices)
        choices_len = <int64_t>len(choices)

    c_limit = choices_len
    if limit is not None:
        c_limit = min(c_limit, <int64_t>limit)

    if c_limit == 1:
        res = extractOne(
            query,
            choices,
            processor=processor,
            scorer=scorer,
            score_cutoff=score_cutoff,
            score_hint=score_hint,
            scorer_kwargs=scorer_kwargs,
        )
        if res is None:
            return []
        return [res]

    # preprocess the query
    if callable(processor):
        query = processor(query)

    scorer_capsule = getattr(scorer, '_RF_Scorer', scorer)
    if PyCapsule_IsValid(scorer_capsule, NULL):
        scorer_context = <RF_Scorer*>PyCapsule_GetPointer(scorer_capsule, NULL)

    if scorer_context and scorer_context.version == SCORER_STRUCT_VERSION:
        kwargs_context = RF_KwargsWrapper()
        scorer_context.kwargs_init(&kwargs_context.kwargs, scorer_kwargs)
        scorer_context.get_scorer_flags(&kwargs_context.kwargs, &scorer_flags)

        if hasattr(choices, "items"):
            return extract_dict(query, choices, scorer_context, &scorer_flags,
                processor, c_limit, score_cutoff, score_hint, &kwargs_context.kwargs)
        else:
            return extract_list(query, choices, scorer_context, &scorer_flags,
                processor, c_limit, score_cutoff, score_hint, &kwargs_context.kwargs)


    worst_score, optimal_score = get_scorer_flags_py(scorer, scorer_kwargs)
    # the scorer has to be called through Python
    if score_cutoff is None:
        score_cutoff = worst_score

    scorer_kwargs["score_cutoff"] = score_cutoff

    if hasattr(choices, "items"):
        return py_extract_dict(query, choices, scorer, processor, c_limit, score_cutoff, worst_score, optimal_score, scorer_kwargs)
    else:
        return py_extract_list(query, choices, scorer, processor, c_limit, score_cutoff, worst_score, optimal_score, scorer_kwargs)

SetFuncAttrs(extract, process_py.extract)

def extract_iter(query, choices, *, scorer=WRatio, processor=None, score_cutoff=None, score_hint=None, scorer_kwargs=None):
    cdef RF_Scorer* scorer_context = NULL
    cdef RF_ScorerFlags scorer_flags
    cdef RF_Preprocessor* processor_context = NULL
    cdef RF_KwargsWrapper kwargs_context

    def extract_iter_dict_f64():
        """
        implementation of extract_iter for dict, scorer using RapidFuzz C-API with the result type
        float64
        """
        cdef RF_String proc_str
        cdef double c_score_cutoff = get_score_cutoff_f64(score_cutoff, scorer_flags.worst_score.f64, scorer_flags.optimal_score.f64)
        cdef double c_score_hint = get_score_cutoff_f64(score_hint, scorer_flags.worst_score.f64, scorer_flags.optimal_score.f64)
        query_proc = RF_StringWrapper(conv_sequence(query))

        cdef RF_ScorerFunc scorer_func
        scorer_context.scorer_func_init(
            &scorer_func, &kwargs_context.kwargs, 1, &query_proc.string
        )
        cdef RF_ScorerWrapper ScorerFunc = RF_ScorerWrapper(scorer_func)
        cdef bool lowest_score_worst = is_lowest_score_worst[double](&scorer_flags)
        cdef double score

        for choice_key, choice in choices.items():
            if is_none(choice):
                continue

            # use RapidFuzz C-Api
            if processor_context != NULL and processor_context.version == PREPROCESSOR_STRUCT_VERSION:
                processor_context.preprocess(choice, &proc_str)
                choice_proc = RF_StringWrapper(proc_str)
            elif processor is not None:
                proc_choice = processor(choice)
                if is_none(proc_choice):
                    continue

                choice_proc = RF_StringWrapper(conv_sequence(proc_choice))
            else:
                choice_proc = RF_StringWrapper(conv_sequence(choice))

            ScorerFunc.call(&choice_proc.string, c_score_cutoff, c_score_hint, &score)
            if lowest_score_worst:
                if score >= c_score_cutoff:
                    yield (choice, score, choice_key)
            else:
                if score <= c_score_cutoff:
                    yield (choice, score, choice_key)

    def extract_iter_dict_i64():
        """
        implementation of extract_iter for dict, scorer using RapidFuzz C-API with the result type
        int64_t
        """
        cdef RF_String proc_str
        cdef int64_t c_score_cutoff = get_score_cutoff_i64(score_cutoff, scorer_flags.worst_score.i64, scorer_flags.optimal_score.i64)
        cdef int64_t c_score_hint = get_score_cutoff_i64(score_hint, scorer_flags.worst_score.i64, scorer_flags.optimal_score.i64)
        query_proc = RF_StringWrapper(conv_sequence(query))

        cdef RF_ScorerFunc scorer_func
        scorer_context.scorer_func_init(
            &scorer_func, &kwargs_context.kwargs, 1, &query_proc.string
        )
        cdef RF_ScorerWrapper ScorerFunc = RF_ScorerWrapper(scorer_func)
        cdef bool lowest_score_worst = is_lowest_score_worst[int64_t](&scorer_flags)
        cdef int64_t score

        for choice_key, choice in choices.items():
            if is_none(choice):
                continue

            # use RapidFuzz C-Api
            if processor_context != NULL and processor_context.version == PREPROCESSOR_STRUCT_VERSION:
                processor_context.preprocess(choice, &proc_str)
                choice_proc = RF_StringWrapper(proc_str)
            elif processor is not None:
                proc_choice = processor(choice)
                if is_none(proc_choice):
                    continue

                choice_proc = RF_StringWrapper(conv_sequence(proc_choice))
            else:
                choice_proc = RF_StringWrapper(conv_sequence(choice))

            ScorerFunc.call(&choice_proc.string, c_score_cutoff, c_score_hint, &score)
            if lowest_score_worst:
                if score >= c_score_cutoff:
                    yield (choice, score, choice_key)
            else:
                if score <= c_score_cutoff:
                    yield (choice, score, choice_key)

    def extract_iter_dict_size_t():
        """
        implementation of extract_iter for dict, scorer using RapidFuzz C-API with the result type
        size_t
        """
        cdef RF_String proc_str
        cdef size_t c_score_cutoff = get_score_cutoff_size_t(score_cutoff, scorer_flags.worst_score.sizet, scorer_flags.optimal_score.sizet)
        cdef size_t c_score_hint = get_score_cutoff_size_t(score_hint, scorer_flags.worst_score.sizet, scorer_flags.optimal_score.sizet)
        query_proc = RF_StringWrapper(conv_sequence(query))

        cdef RF_ScorerFunc scorer_func
        scorer_context.scorer_func_init(
            &scorer_func, &kwargs_context.kwargs, 1, &query_proc.string
        )
        cdef RF_ScorerWrapper ScorerFunc = RF_ScorerWrapper(scorer_func)
        cdef bool lowest_score_worst = is_lowest_score_worst[size_t](&scorer_flags)
        cdef size_t score

        for choice_key, choice in choices.items():
            if is_none(choice):
                continue

            # use RapidFuzz C-Api
            if processor_context != NULL and processor_context.version == PREPROCESSOR_STRUCT_VERSION:
                processor_context.preprocess(choice, &proc_str)
                choice_proc = RF_StringWrapper(proc_str)
            elif processor is not None:
                proc_choice = processor(choice)
                if is_none(proc_choice):
                    continue

                choice_proc = RF_StringWrapper(conv_sequence(proc_choice))
            else:
                choice_proc = RF_StringWrapper(conv_sequence(choice))

            ScorerFunc.call(&choice_proc.string, c_score_cutoff, c_score_hint, &score)
            if lowest_score_worst:
                if score >= c_score_cutoff:
                    yield (choice, score, choice_key)
            else:
                if score <= c_score_cutoff:
                    yield (choice, score, choice_key)

    def extract_iter_list_f64():
        """
        implementation of extract_iter for list, scorer using RapidFuzz C-API with the result type
        float64
        """
        cdef RF_String proc_str
        cdef double c_score_cutoff = get_score_cutoff_f64(score_cutoff, scorer_flags.worst_score.f64, scorer_flags.optimal_score.f64)
        cdef double c_score_hint = get_score_cutoff_f64(score_hint, scorer_flags.worst_score.f64, scorer_flags.optimal_score.f64)
        query_proc = RF_StringWrapper(conv_sequence(query))

        cdef RF_ScorerFunc scorer_func
        scorer_context.scorer_func_init(
            &scorer_func, &kwargs_context.kwargs, 1, &query_proc.string
        )
        cdef RF_ScorerWrapper ScorerFunc = RF_ScorerWrapper(scorer_func)
        cdef bool lowest_score_worst = is_lowest_score_worst[double](&scorer_flags)
        cdef double score

        for i, choice in enumerate(choices):
            if is_none(choice):
                continue

            # use RapidFuzz C-Api
            if processor_context != NULL and processor_context.version == PREPROCESSOR_STRUCT_VERSION:
                processor_context.preprocess(choice, &proc_str)
                choice_proc = RF_StringWrapper(proc_str)
            elif processor is not None:
                proc_choice = processor(choice)
                if is_none(proc_choice):
                    continue

                choice_proc = RF_StringWrapper(conv_sequence(proc_choice))
            else:
                choice_proc = RF_StringWrapper(conv_sequence(choice))

            ScorerFunc.call(&choice_proc.string, c_score_cutoff, c_score_hint, &score)
            if lowest_score_worst:
                if score >= c_score_cutoff:
                    yield (choice, score, i)
            else:
                if score <= c_score_cutoff:
                    yield (choice, score, i)

    def extract_iter_list_i64():
        """
        implementation of extract_iter for list, scorer using RapidFuzz C-API with the result type
        int64_t
        """
        cdef RF_String proc_str
        cdef int64_t c_score_cutoff = get_score_cutoff_i64(score_cutoff, scorer_flags.worst_score.i64, scorer_flags.optimal_score.i64)
        cdef int64_t c_score_hint = get_score_cutoff_i64(score_hint, scorer_flags.worst_score.i64, scorer_flags.optimal_score.i64)
        query_proc = RF_StringWrapper(conv_sequence(query))

        cdef RF_ScorerFunc scorer_func
        scorer_context.scorer_func_init(
            &scorer_func, &kwargs_context.kwargs, 1, &query_proc.string
        )
        cdef RF_ScorerWrapper ScorerFunc = RF_ScorerWrapper(scorer_func)
        cdef bool lowest_score_worst = is_lowest_score_worst[int64_t](&scorer_flags)
        cdef int64_t score

        for i, choice in enumerate(choices):
            if is_none(choice):
                continue

            # use RapidFuzz C-Api
            if processor_context != NULL and processor_context.version == PREPROCESSOR_STRUCT_VERSION:
                processor_context.preprocess(choice, &proc_str)
                choice_proc = RF_StringWrapper(proc_str)
            elif processor is not None:
                proc_choice = processor(choice)
                if is_none(proc_choice):
                    continue

                choice_proc = RF_StringWrapper(conv_sequence(proc_choice))
            else:
                choice_proc = RF_StringWrapper(conv_sequence(choice))

            ScorerFunc.call(&choice_proc.string, c_score_cutoff, c_score_hint, &score)
            if lowest_score_worst:
                if score >= c_score_cutoff:
                    yield (choice, score, i)
            else:
                if score <= c_score_cutoff:
                    yield (choice, score, i)

    def extract_iter_list_size_t():
        """
        implementation of extract_iter for list, scorer using RapidFuzz C-API with the result type
        size_t
        """
        cdef RF_String proc_str
        cdef size_t c_score_cutoff = get_score_cutoff_size_t(score_cutoff, scorer_flags.worst_score.sizet, scorer_flags.optimal_score.sizet)
        cdef size_t c_score_hint = get_score_cutoff_size_t(score_hint, scorer_flags.worst_score.sizet, scorer_flags.optimal_score.sizet)
        query_proc = RF_StringWrapper(conv_sequence(query))

        cdef RF_ScorerFunc scorer_func
        scorer_context.scorer_func_init(
            &scorer_func, &kwargs_context.kwargs, 1, &query_proc.string
        )
        cdef RF_ScorerWrapper ScorerFunc = RF_ScorerWrapper(scorer_func)
        cdef bool lowest_score_worst = is_lowest_score_worst[size_t](&scorer_flags)
        cdef size_t score

        for i, choice in enumerate(choices):
            if is_none(choice):
                continue

            # use RapidFuzz C-Api
            if processor_context != NULL and processor_context.version == PREPROCESSOR_STRUCT_VERSION:
                processor_context.preprocess(choice, &proc_str)
                choice_proc = RF_StringWrapper(proc_str)
            elif processor is not None:
                proc_choice = processor(choice)
                if is_none(proc_choice):
                    continue

                choice_proc = RF_StringWrapper(conv_sequence(proc_choice))
            else:
                choice_proc = RF_StringWrapper(conv_sequence(choice))

            ScorerFunc.call(&choice_proc.string, c_score_cutoff, c_score_hint, &score)
            if lowest_score_worst:
                if score >= c_score_cutoff:
                    yield (choice, score, i)
            else:
                if score <= c_score_cutoff:
                    yield (choice, score, i)

    def py_extract_iter_dict(worst_score, optimal_score):
        """
        implementation of extract_iter for:
          - type of choices = dict
          - scorer = python function
        """
        cdef bool lowest_score_worst = optimal_score > worst_score

        for choice_key, choice in choices.items():
            if is_none(choice):
                continue

            if processor is not None:
                score = scorer(query, processor(choice), **scorer_kwargs)
            else:
                score = scorer(query, choice, **scorer_kwargs)

            if lowest_score_worst:
                if score >= score_cutoff:
                    yield (choice, score, choice_key)
            else:
                if score <= score_cutoff:
                    yield (choice, score, choice_key)

    def py_extract_iter_list(worst_score, optimal_score):
        """
        implementation of extract_iter for:
          - type of choices = list
          - scorer = python function
        """
        cdef bool lowest_score_worst = optimal_score > worst_score
        cdef int64_t i

        for i, choice in enumerate(choices):
            if is_none(choice):
                continue

            if processor is not None:
                score = scorer(query, processor(choice), **scorer_kwargs)
            else:
                score = scorer(query, choice, **scorer_kwargs)

            if lowest_score_worst:
                if score >= score_cutoff:
                    yield (choice, score, i)
            else:
                if score <= score_cutoff:
                    yield (choice, score, i)

    scorer_kwargs = scorer_kwargs.copy() if scorer_kwargs else {}

    setupPandas()

    if is_none(query):
        # finish generator
        return

    # preprocess the query
    if callable(processor):
        query = processor(query)

    scorer_capsule = getattr(scorer, '_RF_Scorer', scorer)
    if PyCapsule_IsValid(scorer_capsule, NULL):
        scorer_context = <RF_Scorer*>PyCapsule_GetPointer(scorer_capsule, NULL)

    if scorer_context and scorer_context.version == SCORER_STRUCT_VERSION:
        kwargs_context = RF_KwargsWrapper()
        scorer_context.kwargs_init(&kwargs_context.kwargs, scorer_kwargs)
        scorer_context.get_scorer_flags(&kwargs_context.kwargs, &scorer_flags)

        processor_capsule = getattr(processor, '_RF_Preprocess', processor)
        if PyCapsule_IsValid(processor_capsule, NULL):
            processor_context = <RF_Preprocessor*>PyCapsule_GetPointer(processor_capsule, NULL)

        if hasattr(choices, "items"):
            if scorer_flags.flags & RF_SCORER_FLAG_RESULT_F64:
                yield from extract_iter_dict_f64()
                return
            elif scorer_flags.flags & RF_SCORER_FLAG_RESULT_SIZE_T:
                yield from extract_iter_dict_size_t()
                return
            elif scorer_flags.flags & RF_SCORER_FLAG_RESULT_I64:
                yield from extract_iter_dict_i64()
                return
        else:
            if scorer_flags.flags & RF_SCORER_FLAG_RESULT_F64:
                yield from extract_iter_list_f64()
                return
            elif scorer_flags.flags & RF_SCORER_FLAG_RESULT_SIZE_T:
                yield from extract_iter_list_size_t()
                return
            elif scorer_flags.flags & RF_SCORER_FLAG_RESULT_I64:
                yield from extract_iter_list_i64()
                return

        raise ValueError("scorer does not properly use the C-API")

    worst_score, optimal_score = get_scorer_flags_py(scorer, scorer_kwargs)
    # the scorer has to be called through Python
    if score_cutoff is None:
        score_cutoff = worst_score

    scorer_kwargs["score_cutoff"] = score_cutoff

    if hasattr(choices, "items"):
        yield from py_extract_iter_dict(worst_score, optimal_score)
    else:
        yield from py_extract_iter_list(worst_score, optimal_score)

SetFuncAttrs(extract_iter, process_py.extract_iter)

FLOAT32 = MatrixType.FLOAT32
FLOAT64 = MatrixType.FLOAT64
INT8 = MatrixType.INT8
INT16 = MatrixType.INT16
INT32 = MatrixType.INT32
INT64 = MatrixType.INT64
UINT8 = MatrixType.UINT8
UINT16 = MatrixType.UINT16
UINT32 = MatrixType.UINT32
UINT64 = MatrixType.UINT64

cdef inline vector[PyObjectWrapper] preprocess_py(queries, processor) except *:
    cdef vector[PyObjectWrapper] proc_queries
    cdef int64_t queries_len = <int64_t>len(queries)
    proc_queries.reserve(queries_len)

    # processor None/False
    if not processor:
        for query in queries:
            proc_queries.emplace_back(<PyObject*>query)
    # processor has to be called through python
    else:
        for query in queries:
            proc_query = query if is_none(query) else processor(query)
            proc_queries.emplace_back(<PyObject*>proc_query)

    return move(proc_queries)

cdef inline vector[RF_StringWrapper] preprocess(const RF_ScorerFlags* scorer_flags, queries, processor) except *:
    cdef vector[RF_StringWrapper] proc_queries
    cdef int64_t queries_len = <int64_t>len(queries)
    cdef RF_String proc_str
    cdef RF_Preprocessor* processor_context = NULL
    flags = scorer_flags.flags
    proc_queries.reserve(queries_len)

    # No processor
    if not processor:
        for query in queries:
            conv_sequence_with_none(query, &proc_str)
            if proc_str.data == NULL:
                if flags & RF_SCORER_NONE_IS_WORST_SCORE:
                    proc_queries.emplace_back()
                else:
                    raise ValueError(f"passed unsupported element {query}")
            else:
                proc_queries.emplace_back(proc_str, <PyObject*>query)
    else:
        processor_capsule = getattr(processor, '_RF_Preprocess', processor)
        if PyCapsule_IsValid(processor_capsule, NULL):
            processor_context = <RF_Preprocessor*>PyCapsule_GetPointer(processor_capsule, NULL)

        # use RapidFuzz C-Api
        if processor_context != NULL and processor_context.version == PREPROCESSOR_STRUCT_VERSION:
            for query in queries:
                if is_none(query) and flags & RF_SCORER_NONE_IS_WORST_SCORE:
                    proc_queries.emplace_back()
                else:
                    processor_context.preprocess(query, &proc_str)
                    proc_queries.emplace_back(proc_str, <PyObject*>query)

        # Call Processor through Python
        else:
            for query in queries:
                if is_none(query) and flags & RF_SCORER_NONE_IS_WORST_SCORE:
                    proc_queries.emplace_back()
                else:
                    proc_query = processor(query)
                    proc_queries.emplace_back(conv_sequence(proc_query), <PyObject*>proc_query)

    return move(proc_queries)

cdef inline MatrixType dtype_to_type_num_f64(dtype) except MatrixType.UNDEFINED:
    if dtype is None:
        return MatrixType.FLOAT32
    return <MatrixType>dtype

cdef inline MatrixType dtype_to_type_num_i64(dtype) except MatrixType.UNDEFINED:
    if dtype is None:
        return MatrixType.INT32
    return <MatrixType>dtype

cdef inline MatrixType dtype_to_type_num_size_t(dtype) except MatrixType.UNDEFINED:
    if dtype is None:
        return MatrixType.UINT32
    return <MatrixType>dtype

cdef inline MatrixType dtype_to_type_num_py(dtype, scorer, dict scorer_kwargs) except MatrixType.UNDEFINED:
    import numpy as np

    if dtype is not None:
        return <MatrixType>dtype

    params = getattr(scorer, "_RF_ScorerPy", None)
    if params is not None:
        flags = params["get_scorer_flags"](**scorer_kwargs)
        if <int>flags["flags"] & RF_SCORER_FLAG_RESULT_I64:
            return MatrixType.INT32
        if <int>flags["flags"] & RF_SCORER_FLAG_RESULT_SIZE_T:
            return MatrixType.UINT32
        return MatrixType.FLOAT32

    return MatrixType.FLOAT32


from cpython cimport Py_buffer
from libcpp.vector cimport vector


cdef class Matrix:

    cdef RfMatrix matrix
    cdef Py_ssize_t shape[2]
    cdef Py_ssize_t strides[2]
    cdef bint vector_output

    def __cinit__(self, bint vector_output=False):
        self.vector_output = vector_output

    def __getbuffer__(self, Py_buffer *buffer, int flags):

        if self.vector_output:
            self.shape[0] = self.matrix.m_rows
            self.strides[0] = self.matrix.get_dtype_size()
            buffer.ndim = 1
        else:
            self.shape[0] = self.matrix.m_rows
            self.shape[1] = self.matrix.m_cols
            self.strides[1] = self.matrix.get_dtype_size()
            self.strides[0] = self.matrix.m_cols * self.strides[1]
            buffer.ndim = 2

        buffer.buf = <char *>self.matrix.m_matrix
        buffer.format = <char *>self.matrix.get_format()
        buffer.internal = NULL
        buffer.itemsize = self.matrix.get_dtype_size()
        buffer.len = self.matrix.m_rows * self.matrix.m_cols * self.matrix.get_dtype_size()
        buffer.obj = self
        buffer.readonly = 0
        buffer.shape = self.shape
        buffer.strides = self.strides
        buffer.suboffsets = NULL

    def __releasebuffer__(self, Py_buffer *buffer):
        pass

cdef Matrix cpdist_cpp(
    queries, choices,
    RF_Scorer* scorer,
    const RF_ScorerFlags* scorer_flags,
    processor,
    score_cutoff,
    score_hint,
    score_multiplier,
    dtype,
    int c_workers,
    const RF_Kwargs* scorer_kwargs
):
    proc_queries = preprocess(scorer_flags, queries, processor)
    proc_choices = preprocess(scorer_flags, choices, processor)
    flags = scorer_flags.flags
    cdef Matrix matrix = Matrix(vector_output=True)

    if flags & RF_SCORER_FLAG_RESULT_F64:
        matrix.matrix = cpdist_cpp_impl[double](
            scorer_kwargs, scorer, proc_queries, proc_choices,
            dtype_to_type_num_f64(dtype),
            c_workers,
            get_score_cutoff_f64(score_cutoff, scorer_flags.worst_score.f64, scorer_flags.optimal_score.f64),
            get_score_cutoff_f64(score_hint, scorer_flags.worst_score.f64, scorer_flags.optimal_score.f64),
            <double>score_multiplier,
            scorer_flags.worst_score.f64
        )
    elif flags & RF_SCORER_FLAG_RESULT_SIZE_T:
        matrix.matrix = cpdist_cpp_impl[size_t](
            scorer_kwargs, scorer, proc_queries, proc_choices,
            dtype_to_type_num_size_t(dtype),
            c_workers,
            get_score_cutoff_size_t(score_cutoff, scorer_flags.worst_score.sizet, scorer_flags.optimal_score.sizet),
            get_score_cutoff_size_t(score_hint, scorer_flags.worst_score.sizet, scorer_flags.optimal_score.sizet),
            <size_t>score_multiplier,
            scorer_flags.worst_score.sizet
        )
    elif flags & RF_SCORER_FLAG_RESULT_I64:
        matrix.matrix = cpdist_cpp_impl[int64_t](
            scorer_kwargs, scorer, proc_queries, proc_choices,
            dtype_to_type_num_i64(dtype),
            c_workers,
            get_score_cutoff_i64(score_cutoff, scorer_flags.worst_score.i64, scorer_flags.optimal_score.i64),
            get_score_cutoff_i64(score_hint, scorer_flags.worst_score.i64, scorer_flags.optimal_score.i64),
            <int64_t>score_multiplier,
            scorer_flags.worst_score.i64
        )
    else:
        raise ValueError("scorer does not properly use the C-API")

    return matrix

cdef Matrix cdist_two_lists(
    queries, choices,
    RF_Scorer* scorer,
    const RF_ScorerFlags* scorer_flags,
    processor,
    score_cutoff,
    score_hint,
    score_multiplier,
    dtype,
    int c_workers,
    const RF_Kwargs* scorer_kwargs
):
    proc_queries = preprocess(scorer_flags, queries, processor)
    proc_choices = preprocess(scorer_flags, choices, processor)
    flags = scorer_flags.flags
    cdef Matrix matrix = Matrix()

    if flags & RF_SCORER_FLAG_RESULT_F64:
        matrix.matrix = cdist_two_lists_impl[double](
            scorer_flags,
            scorer_kwargs, scorer, proc_queries, proc_choices,
            dtype_to_type_num_f64(dtype),
            c_workers,
            get_score_cutoff_f64(score_cutoff, scorer_flags.worst_score.f64, scorer_flags.optimal_score.f64),
            get_score_cutoff_f64(score_hint, scorer_flags.worst_score.f64, scorer_flags.optimal_score.f64),
            <double>score_multiplier,
            scorer_flags.worst_score.f64,
        )
    elif flags & RF_SCORER_FLAG_RESULT_SIZE_T:
        matrix.matrix = cdist_two_lists_impl[size_t](
            scorer_flags,
            scorer_kwargs, scorer, proc_queries, proc_choices,
            dtype_to_type_num_size_t(dtype),
            c_workers,
            get_score_cutoff_size_t(score_cutoff, scorer_flags.worst_score.sizet, scorer_flags.optimal_score.sizet),
            get_score_cutoff_size_t(score_hint, scorer_flags.worst_score.sizet, scorer_flags.optimal_score.sizet),
            <size_t>score_multiplier,
            scorer_flags.worst_score.sizet
        )
    elif flags & RF_SCORER_FLAG_RESULT_I64:
        matrix.matrix = cdist_two_lists_impl[int64_t](
            scorer_flags,
            scorer_kwargs, scorer, proc_queries, proc_choices,
            dtype_to_type_num_i64(dtype),
            c_workers,
            get_score_cutoff_i64(score_cutoff, scorer_flags.worst_score.i64, scorer_flags.optimal_score.i64),
            get_score_cutoff_i64(score_hint, scorer_flags.worst_score.i64, scorer_flags.optimal_score.i64),
            <int64_t>score_multiplier,
            scorer_flags.worst_score.i64
        )
    else:
        raise ValueError("scorer does not properly use the C-API")

    return matrix

cdef Matrix cdist_single_list(
    queries,
    RF_Scorer* scorer,
    const RF_ScorerFlags* scorer_flags,
    processor,
    score_cutoff,
    score_hint,
    score_multiplier,
    dtype,
    int c_workers,
    const RF_Kwargs* scorer_kwargs
):
    proc_queries = preprocess(scorer_flags, queries, processor)
    flags = scorer_flags.flags
    cdef Matrix matrix = Matrix()

    if flags & RF_SCORER_FLAG_RESULT_F64:
        matrix.matrix = cdist_single_list_impl[double](
            scorer_flags,
            scorer_kwargs, scorer, proc_queries,
            dtype_to_type_num_f64(dtype),
            c_workers,
            get_score_cutoff_f64(score_cutoff, scorer_flags.worst_score.f64, scorer_flags.optimal_score.f64),
            get_score_cutoff_f64(score_hint, scorer_flags.worst_score.f64, scorer_flags.optimal_score.f64),
            <double>score_multiplier,
            scorer_flags.worst_score.f64
        )
    elif flags & RF_SCORER_FLAG_RESULT_SIZE_T:
        matrix.matrix = cdist_single_list_impl[size_t](
            scorer_flags,
            scorer_kwargs, scorer, proc_queries,
            dtype_to_type_num_size_t(dtype),
            c_workers,
            get_score_cutoff_size_t(score_cutoff, scorer_flags.worst_score.sizet, scorer_flags.optimal_score.sizet),
            get_score_cutoff_size_t(score_hint, scorer_flags.worst_score.sizet, scorer_flags.optimal_score.sizet),
            <size_t>score_multiplier,
            scorer_flags.worst_score.sizet
        )
    elif flags & RF_SCORER_FLAG_RESULT_I64:
        matrix.matrix = cdist_single_list_impl[int64_t](
            scorer_flags,
            scorer_kwargs, scorer, proc_queries,
            dtype_to_type_num_i64(dtype),
            c_workers,
            get_score_cutoff_i64(score_cutoff, scorer_flags.worst_score.i64, scorer_flags.optimal_score.i64),
            get_score_cutoff_i64(score_hint, scorer_flags.worst_score.i64, scorer_flags.optimal_score.i64),
            <int64_t>score_multiplier,
            scorer_flags.worst_score.i64
        )
    else:
        raise ValueError("scorer does not properly use the C-API")

    return matrix


@cython.boundscheck(False)
@cython.wraparound(False)
cdef cdist_py(queries, choices, scorer, processor, score_cutoff, score_multiplier, dtype, workers, dict scorer_kwargs):
    # todo this should handle two similar sequences more efficiently

    proc_queries = preprocess_py(queries, processor)
    proc_choices = preprocess_py(choices, processor)
    cdef double score
    cdef Matrix matrix = Matrix()
    c_dtype = dtype_to_type_num_py(dtype, scorer, scorer_kwargs)
    matrix.matrix = RfMatrix(c_dtype, proc_queries.size(), proc_choices.size())

    scorer_kwargs["score_cutoff"] = score_cutoff

    for i in range(proc_queries.size()):
        for j in range(proc_choices.size()):
            score = scorer(<object>proc_queries[i].obj, <object>proc_choices[j].obj, **scorer_kwargs)
            matrix.matrix.set(i, j, score * <double>score_multiplier)

    return matrix

def cdist(queries, choices, *, scorer=ratio, processor=None, score_cutoff=None, score_hint=None, score_multiplier=1, dtype=None, workers=1, scorer_kwargs=None):
    cdef RF_Scorer* scorer_context = NULL
    cdef RF_ScorerFlags scorer_flags
    cdef bool is_orig_scorer

    setupPandas()

    scorer_kwargs = scorer_kwargs.copy() if scorer_kwargs else {}

    scorer_capsule = getattr(scorer, '_RF_Scorer', scorer)
    if PyCapsule_IsValid(scorer_capsule, NULL):
        scorer_context = <RF_Scorer*>PyCapsule_GetPointer(scorer_capsule, NULL)

    is_orig_scorer = getattr(scorer, '_RF_OriginalScorer', None) is scorer

    if is_orig_scorer and scorer_context and scorer_context.version == SCORER_STRUCT_VERSION:
        kwargs_context = RF_KwargsWrapper()
        scorer_context.kwargs_init(&kwargs_context.kwargs, scorer_kwargs)
        scorer_context.get_scorer_flags(&kwargs_context.kwargs, &scorer_flags)

        # scorer(queries[i], choices[j]) == scorer(queries[j], choices[i])
        if scorer_flags.flags & RF_SCORER_FLAG_SYMMETRIC and queries is choices:
            return cdist_single_list(
                queries, scorer_context, &scorer_flags, processor,
                score_cutoff, score_hint, score_multiplier,
                dtype, workers, &kwargs_context.kwargs)
        else:
            return cdist_two_lists(
                queries, choices, scorer_context, &scorer_flags, processor,
                score_cutoff, score_hint, score_multiplier,
                dtype, workers, &kwargs_context.kwargs)

    return cdist_py(queries, choices, scorer, processor, score_cutoff, score_multiplier, dtype, workers, scorer_kwargs)

SetFuncAttrs(cdist, process_py.cdist)

@cython.boundscheck(False)
@cython.wraparound(False)
cdef cpdist_py(queries, choices, scorer, processor, score_cutoff, score_multiplier, dtype, workers, dict scorer_kwargs):

    proc_queries = preprocess_py(queries, processor)
    proc_choices = preprocess_py(choices, processor)
    cdef double score
    cdef Matrix matrix = Matrix(vector_output=True)
    c_dtype = dtype_to_type_num_py(dtype, scorer, scorer_kwargs)
    matrix.matrix = RfMatrix(c_dtype, proc_queries.size(), 1)

    scorer_kwargs["score_cutoff"] = score_cutoff

    for i in range(proc_queries.size()):
        score = scorer(<object>proc_queries[i].obj, <object>proc_choices[i].obj, **scorer_kwargs)
        matrix.matrix.set(i, 0, score * <double>score_multiplier)

    return matrix

def cpdist(queries, choices, *, scorer=ratio, processor=None, score_cutoff=None, score_hint=None, score_multiplier=1, dtype=None, workers=1, scorer_kwargs=None):

    if len(queries) != len(choices):
        error_message = "Length of queries and choices must be the same!"
        raise ValueError(error_message)

    cdef RF_Scorer* scorer_context = NULL
    cdef RF_ScorerFlags scorer_flags
    cdef bool is_orig_scorer

    setupPandas()

    scorer_kwargs = scorer_kwargs.copy() if scorer_kwargs else {}

    scorer_capsule = getattr(scorer, '_RF_Scorer', scorer)
    if PyCapsule_IsValid(scorer_capsule, NULL):
        scorer_context = <RF_Scorer*>PyCapsule_GetPointer(scorer_capsule, NULL)

    is_orig_scorer = getattr(scorer, '_RF_OriginalScorer', None) is scorer

    if is_orig_scorer and scorer_context and scorer_context.version == SCORER_STRUCT_VERSION:
        kwargs_context = RF_KwargsWrapper()
        scorer_context.kwargs_init(&kwargs_context.kwargs, scorer_kwargs)
        scorer_context.get_scorer_flags(&kwargs_context.kwargs, &scorer_flags)

        return cpdist_cpp(
            queries, choices, scorer_context, &scorer_flags, processor,
            score_cutoff, score_hint, score_multiplier,
            dtype, workers, &kwargs_context.kwargs)

    return cpdist_py(queries, choices, scorer, processor, score_cutoff, score_multiplier, dtype, workers, scorer_kwargs)

SetFuncAttrs(cpdist, process_py.cpdist)
