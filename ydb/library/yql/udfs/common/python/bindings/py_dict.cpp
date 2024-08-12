#include "py_dict.h"
#include "py_iterator.h"
#include "py_cast.h"
#include "py_errors.h"
#include "py_utils.h"

#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>
#include <ydb/library/yql/public/udf/udf_type_inspection.h>


using namespace NKikimr;

namespace NPython {

//////////////////////////////////////////////////////////////////////////////
// TPyLazyDict interface
//////////////////////////////////////////////////////////////////////////////
struct TPyLazyDict
{
    using TPtr = NUdf::TRefCountedPtr<TPyLazyDict, TPyPtrOps<TPyLazyDict>>;

    PyObject_HEAD;
    TPyCastContext::TPtr CastCtx;
    const NUdf::TType* KeyType;
    const NUdf::TType* PayloadType;
    TPyCleanupListItem<NUdf::IBoxedValuePtr> Value;

    inline static TPyLazyDict* Cast(PyObject* o) {
        return reinterpret_cast<TPyLazyDict*>(o);
    }

    inline static void Dealloc(PyObject* self) {
        delete Cast(self);
    }

    static PyObject* New(
            const TPyCastContext::TPtr& castCtx,
            const NUdf::TType* keyType,
            const NUdf::TType* payloadType,
            NUdf::IBoxedValuePtr&& value);

    static int Bool(PyObject* self);
    static PyObject* Repr(PyObject* self);
    static Py_ssize_t Len(PyObject* self);
    static PyObject* Subscript(PyObject* self, PyObject* key);
    static int Contains(PyObject* self, PyObject* key);
    static PyObject* Get(PyObject* self, PyObject* args);

    static PyObject* Iter(PyObject* self) { return Keys(self, nullptr); }
    static PyObject* Keys(PyObject* self, PyObject* /* args */);
    static PyObject* Items(PyObject* self, PyObject* /* args */);
    static PyObject* Values(PyObject* self, PyObject* /* args */);
};

PyMappingMethods LazyDictMapping = {
    INIT_MEMBER(mp_length, TPyLazyDict::Len),
    INIT_MEMBER(mp_subscript, TPyLazyDict::Subscript),
    INIT_MEMBER(mp_ass_subscript, nullptr),
};

PySequenceMethods LazyDictSequence = {
    INIT_MEMBER(sq_length         , TPyLazyDict::Len),
    INIT_MEMBER(sq_concat         , nullptr),
    INIT_MEMBER(sq_repeat         , nullptr),
    INIT_MEMBER(sq_item           , nullptr),
#if PY_MAJOR_VERSION >= 3
    INIT_MEMBER(was_sq_slice      , nullptr),
#else
    INIT_MEMBER(sq_slice          , nullptr),
#endif
    INIT_MEMBER(sq_ass_item       , nullptr),
#if PY_MAJOR_VERSION >= 3
    INIT_MEMBER(was_sq_ass_slice  , nullptr),
#else
    INIT_MEMBER(sq_ass_slice      , nullptr),
#endif
    INIT_MEMBER(sq_contains       , TPyLazyDict::Contains),
    INIT_MEMBER(sq_inplace_concat , nullptr),
    INIT_MEMBER(sq_inplace_repeat , nullptr),
};

PyNumberMethods LazyDictNumbering = {
     INIT_MEMBER(nb_add, nullptr),
     INIT_MEMBER(nb_subtract, nullptr),
     INIT_MEMBER(nb_multiply, nullptr),
#if PY_MAJOR_VERSION < 3
     INIT_MEMBER(nb_divide, nullptr),
#endif
     INIT_MEMBER(nb_remainder, nullptr),
     INIT_MEMBER(nb_divmod, nullptr),
     INIT_MEMBER(nb_power, nullptr),
     INIT_MEMBER(nb_negative, nullptr),
     INIT_MEMBER(nb_positive, nullptr),
     INIT_MEMBER(nb_absolute, nullptr),
#if PY_MAJOR_VERSION >= 3
     INIT_MEMBER(nb_bool, TPyLazyDict::Bool),
#else
     INIT_MEMBER(nb_nonzero, TPyLazyDict::Bool),
#endif
     INIT_MEMBER(nb_invert, nullptr),
     INIT_MEMBER(nb_lshift, nullptr),
     INIT_MEMBER(nb_rshift, nullptr),
     INIT_MEMBER(nb_and, nullptr),
     INIT_MEMBER(nb_xor, nullptr),
     INIT_MEMBER(nb_or, nullptr),
#if PY_MAJOR_VERSION < 3
     INIT_MEMBER(nb_coerce, nullptr),
#endif
     INIT_MEMBER(nb_int, nullptr),
#if PY_MAJOR_VERSION >= 3
     INIT_MEMBER(nb_reserved, nullptr),
#else
     INIT_MEMBER(nb_long, nullptr),
#endif
     INIT_MEMBER(nb_float, nullptr),
#if PY_MAJOR_VERSION < 3
     INIT_MEMBER(nb_oct, nullptr),
     INIT_MEMBER(nb_hex, nullptr),
#endif

     INIT_MEMBER(nb_inplace_add, nullptr),
     INIT_MEMBER(nb_inplace_subtract, nullptr),
     INIT_MEMBER(nb_inplace_multiply, nullptr),
     INIT_MEMBER(nb_inplace_remainder, nullptr),
     INIT_MEMBER(nb_inplace_power, nullptr),
     INIT_MEMBER(nb_inplace_lshift, nullptr),
     INIT_MEMBER(nb_inplace_rshift, nullptr),
     INIT_MEMBER(nb_inplace_and, nullptr),
     INIT_MEMBER(nb_inplace_xor, nullptr),
     INIT_MEMBER(nb_inplace_or, nullptr),

     INIT_MEMBER(nb_floor_divide, nullptr),
     INIT_MEMBER(nb_true_divide, nullptr),
     INIT_MEMBER(nb_inplace_floor_divide, nullptr),
     INIT_MEMBER(nb_inplace_true_divide, nullptr),

     INIT_MEMBER(nb_index, nullptr),
#if PY_MAJOR_VERSION >= 3
     INIT_MEMBER(nb_matrix_multiply, nullptr),
     INIT_MEMBER(nb_inplace_matrix_multiply, nullptr),
#endif
};


#if PY_MAJOR_VERSION >= 3
#define Py_TPFLAGS_HAVE_ITER 0
#define Py_TPFLAGS_HAVE_SEQUENCE_IN 0
#endif

PyDoc_STRVAR(get__doc__,
    "D.get(k[,d]) -> D[k] if k in D, else d.  d defaults to None.");
PyDoc_STRVAR(keys__doc__,
    "D.keys() -> an iterator over the keys of D");
PyDoc_STRVAR(values__doc__,
    "D.values() -> an iterator over the values of D");
PyDoc_STRVAR(items__doc__,
    "D.items() -> an iterator over the (key, value) items of D");
#if PY_MAJOR_VERSION < 3
PyDoc_STRVAR(iterkeys__doc__,
    "D.iterkeys() -> an iterator over the keys of D");
PyDoc_STRVAR(itervalues__doc__,
    "D.itervalues() -> an iterator over the values of D");
PyDoc_STRVAR(iteritems__doc__,
    "D.iteritems() -> an iterator over the (key, value) items of D");
#endif

static PyMethodDef LazyDictMethods[] = {
    { "get", TPyLazyDict::Get, METH_VARARGS, get__doc__ },
    { "keys", TPyLazyDict::Keys, METH_NOARGS, keys__doc__ },
    { "items", TPyLazyDict::Items, METH_NOARGS, items__doc__ },
    { "values", TPyLazyDict::Values, METH_NOARGS, values__doc__ },
#if PY_MAJOR_VERSION < 3
    { "iterkeys", TPyLazyDict::Keys, METH_NOARGS, iterkeys__doc__ },
    { "iteritems", TPyLazyDict::Items, METH_NOARGS, iteritems__doc__ },
    { "itervalues", TPyLazyDict::Values, METH_NOARGS, itervalues__doc__ },
#endif
    { nullptr, nullptr, 0, nullptr }   /* sentinel */
};

PyTypeObject PyLazyDictType = {
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    INIT_MEMBER(tp_name           , "yql.TDict"),
    INIT_MEMBER(tp_basicsize      , sizeof(TPyLazyDict)),
    INIT_MEMBER(tp_itemsize       , 0),
    INIT_MEMBER(tp_dealloc        , TPyLazyDict::Dealloc),
#if PY_VERSION_HEX < 0x030800b4
    INIT_MEMBER(tp_print          , nullptr),
#else
    INIT_MEMBER(tp_vectorcall_offset, 0),
#endif
    INIT_MEMBER(tp_getattr        , nullptr),
    INIT_MEMBER(tp_setattr        , nullptr),
#if PY_MAJOR_VERSION >= 3
    INIT_MEMBER(tp_as_async       , nullptr),
#else
    INIT_MEMBER(tp_compare        , nullptr),
#endif
    INIT_MEMBER(tp_repr           , TPyLazyDict::Repr),
    INIT_MEMBER(tp_as_number      , &LazyDictNumbering),
    INIT_MEMBER(tp_as_sequence    , &LazyDictSequence),
    INIT_MEMBER(tp_as_mapping     , &LazyDictMapping),
    INIT_MEMBER(tp_hash           , nullptr),
    INIT_MEMBER(tp_call           , nullptr),
    INIT_MEMBER(tp_str            , nullptr),
    INIT_MEMBER(tp_getattro       , nullptr),
    INIT_MEMBER(tp_setattro       , nullptr),
    INIT_MEMBER(tp_as_buffer      , nullptr),
    INIT_MEMBER(tp_flags          , Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_ITER | Py_TPFLAGS_HAVE_SEQUENCE_IN),
    INIT_MEMBER(tp_doc            , "yql.TDict object"),
    INIT_MEMBER(tp_traverse       , nullptr),
    INIT_MEMBER(tp_clear          , nullptr),
    INIT_MEMBER(tp_richcompare    , nullptr),
    INIT_MEMBER(tp_weaklistoffset , 0),
    INIT_MEMBER(tp_iter           , &TPyLazyDict::Iter),
    INIT_MEMBER(tp_iternext       , nullptr),
    INIT_MEMBER(tp_methods        , LazyDictMethods),
    INIT_MEMBER(tp_members        , nullptr),
    INIT_MEMBER(tp_getset         , nullptr),
    INIT_MEMBER(tp_base           , nullptr),
    INIT_MEMBER(tp_dict           , nullptr),
    INIT_MEMBER(tp_descr_get      , nullptr),
    INIT_MEMBER(tp_descr_set      , nullptr),
    INIT_MEMBER(tp_dictoffset     , 0),
    INIT_MEMBER(tp_init           , nullptr),
    INIT_MEMBER(tp_alloc          , nullptr),
    INIT_MEMBER(tp_new            , nullptr),
    INIT_MEMBER(tp_free           , nullptr),
    INIT_MEMBER(tp_is_gc          , nullptr),
    INIT_MEMBER(tp_bases          , nullptr),
    INIT_MEMBER(tp_mro            , nullptr),
    INIT_MEMBER(tp_cache          , nullptr),
    INIT_MEMBER(tp_subclasses     , nullptr),
    INIT_MEMBER(tp_weaklist       , nullptr),
    INIT_MEMBER(tp_del            , nullptr),
    INIT_MEMBER(tp_version_tag    , 0),
#if PY_MAJOR_VERSION >= 3
    INIT_MEMBER(tp_finalize       , nullptr),
#endif
#if PY_VERSION_HEX >= 0x030800b1
    INIT_MEMBER(tp_vectorcall     , nullptr),
#endif
#if PY_VERSION_HEX >= 0x030800b4 && PY_VERSION_HEX < 0x03090000
    INIT_MEMBER(tp_print          , nullptr),
#endif
};

//////////////////////////////////////////////////////////////////////////////
// TPyLazySet interface
//////////////////////////////////////////////////////////////////////////////
struct TPyLazySet
{
    using TPtr = NUdf::TRefCountedPtr<TPyLazySet, TPyPtrOps<TPyLazySet>>;

    PyObject_HEAD;
    TPyCastContext::TPtr CastCtx;
    const NUdf::TType* ItemType;
    TPyCleanupListItem<NUdf::IBoxedValuePtr> Value;

    inline static TPyLazySet* Cast(PyObject* o) {
        return reinterpret_cast<TPyLazySet*>(o);
    }

    inline static void Dealloc(PyObject* self) {
        delete Cast(self);
    }

    static PyObject* New(
            const TPyCastContext::TPtr& castCtx,
            const NUdf::TType* itemType,
            NUdf::IBoxedValuePtr&& value);

    static int Bool(PyObject* self);
    static PyObject* Repr(PyObject* self);
    static Py_ssize_t Len(PyObject* self);
    static int Contains(PyObject* self, PyObject* key);
    static PyObject* Get(PyObject* self, PyObject* args);

    static PyObject* Iter(PyObject* self);
};

PySequenceMethods LazySetSequence = {
    INIT_MEMBER(sq_length         , TPyLazySet::Len),
    INIT_MEMBER(sq_concat         , nullptr),
    INIT_MEMBER(sq_repeat         , nullptr),
    INIT_MEMBER(sq_item           , nullptr),
#if PY_MAJOR_VERSION >= 3
    INIT_MEMBER(was_sq_slice      , nullptr),
#else
    INIT_MEMBER(sq_slice          , nullptr),
#endif
    INIT_MEMBER(sq_ass_item       , nullptr),
#if PY_MAJOR_VERSION >= 3
    INIT_MEMBER(was_sq_ass_slice  , nullptr),
#else
    INIT_MEMBER(sq_ass_slice      , nullptr),
#endif
    INIT_MEMBER(sq_contains       , TPyLazySet::Contains),
    INIT_MEMBER(sq_inplace_concat , nullptr),
    INIT_MEMBER(sq_inplace_repeat , nullptr),
};

PyNumberMethods LazySetNumbering = {
     INIT_MEMBER(nb_add, nullptr),
     INIT_MEMBER(nb_subtract, nullptr),
     INIT_MEMBER(nb_multiply, nullptr),
#if PY_MAJOR_VERSION < 3
     INIT_MEMBER(nb_divide, nullptr),
#endif
     INIT_MEMBER(nb_remainder, nullptr),
     INIT_MEMBER(nb_divmod, nullptr),
     INIT_MEMBER(nb_power, nullptr),
     INIT_MEMBER(nb_negative, nullptr),
     INIT_MEMBER(nb_positive, nullptr),
     INIT_MEMBER(nb_absolute, nullptr),
#if PY_MAJOR_VERSION >= 3
     INIT_MEMBER(nb_bool, TPyLazySet::Bool),
#else
     INIT_MEMBER(nb_nonzero, TPyLazySet::Bool),
#endif
     INIT_MEMBER(nb_invert, nullptr),
     INIT_MEMBER(nb_lshift, nullptr),
     INIT_MEMBER(nb_rshift, nullptr),
     INIT_MEMBER(nb_and, nullptr),
     INIT_MEMBER(nb_xor, nullptr),
     INIT_MEMBER(nb_or, nullptr),
#if PY_MAJOR_VERSION < 3
     INIT_MEMBER(nb_coerce, nullptr),
#endif
     INIT_MEMBER(nb_int, nullptr),
#if PY_MAJOR_VERSION >= 3
     INIT_MEMBER(nb_reserved, nullptr),
#else
     INIT_MEMBER(nb_long, nullptr),
#endif
     INIT_MEMBER(nb_float, nullptr),
#if PY_MAJOR_VERSION < 3
     INIT_MEMBER(nb_oct, nullptr),
     INIT_MEMBER(nb_hex, nullptr),
#endif

     INIT_MEMBER(nb_inplace_add, nullptr),
     INIT_MEMBER(nb_inplace_subtract, nullptr),
     INIT_MEMBER(nb_inplace_multiply, nullptr),
     INIT_MEMBER(nb_inplace_remainder, nullptr),
     INIT_MEMBER(nb_inplace_power, nullptr),
     INIT_MEMBER(nb_inplace_lshift, nullptr),
     INIT_MEMBER(nb_inplace_rshift, nullptr),
     INIT_MEMBER(nb_inplace_and, nullptr),
     INIT_MEMBER(nb_inplace_xor, nullptr),
     INIT_MEMBER(nb_inplace_or, nullptr),

     INIT_MEMBER(nb_floor_divide, nullptr),
     INIT_MEMBER(nb_true_divide, nullptr),
     INIT_MEMBER(nb_inplace_floor_divide, nullptr),
     INIT_MEMBER(nb_inplace_true_divide, nullptr),

     INIT_MEMBER(nb_index, nullptr),
#if PY_MAJOR_VERSION >= 3
     INIT_MEMBER(nb_matrix_multiply, nullptr),
     INIT_MEMBER(nb_inplace_matrix_multiply, nullptr),
#endif
};

PyTypeObject PyLazySetType = {
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    INIT_MEMBER(tp_name           , "yql.TSet"),
    INIT_MEMBER(tp_basicsize      , sizeof(TPyLazySet)),
    INIT_MEMBER(tp_itemsize       , 0),
    INIT_MEMBER(tp_dealloc        , TPyLazySet::Dealloc),
#if PY_VERSION_HEX < 0x030800b4
    INIT_MEMBER(tp_print          , nullptr),
#else
    INIT_MEMBER(tp_vectorcall_offset, 0),
#endif
    INIT_MEMBER(tp_getattr        , nullptr),
    INIT_MEMBER(tp_setattr        , nullptr),
#if PY_MAJOR_VERSION >= 3
    INIT_MEMBER(tp_as_async       , nullptr),
#else
    INIT_MEMBER(tp_compare        , nullptr),
#endif
    INIT_MEMBER(tp_repr           , TPyLazySet::Repr),
    INIT_MEMBER(tp_as_number      , &LazySetNumbering),
    INIT_MEMBER(tp_as_sequence    , &LazySetSequence),
    INIT_MEMBER(tp_as_mapping     , nullptr),
    INIT_MEMBER(tp_hash           , nullptr),
    INIT_MEMBER(tp_call           , nullptr),
    INIT_MEMBER(tp_str            , nullptr),
    INIT_MEMBER(tp_getattro       , nullptr),
    INIT_MEMBER(tp_setattro       , nullptr),
    INIT_MEMBER(tp_as_buffer      , nullptr),
    INIT_MEMBER(tp_flags          , Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_ITER | Py_TPFLAGS_HAVE_SEQUENCE_IN),
    INIT_MEMBER(tp_doc            , "yql.TSet object"),
    INIT_MEMBER(tp_traverse       , nullptr),
    INIT_MEMBER(tp_clear          , nullptr),
    INIT_MEMBER(tp_richcompare    , nullptr),
    INIT_MEMBER(tp_weaklistoffset , 0),
    INIT_MEMBER(tp_iter           , &TPyLazySet::Iter),
    INIT_MEMBER(tp_iternext       , nullptr),
    INIT_MEMBER(tp_methods        , nullptr),
    INIT_MEMBER(tp_members        , nullptr),
    INIT_MEMBER(tp_getset         , nullptr),
    INIT_MEMBER(tp_base           , nullptr),
    INIT_MEMBER(tp_dict           , nullptr),
    INIT_MEMBER(tp_descr_get      , nullptr),
    INIT_MEMBER(tp_descr_set      , nullptr),
    INIT_MEMBER(tp_dictoffset     , 0),
    INIT_MEMBER(tp_init           , nullptr),
    INIT_MEMBER(tp_alloc          , nullptr),
    INIT_MEMBER(tp_new            , nullptr),
    INIT_MEMBER(tp_free           , nullptr),
    INIT_MEMBER(tp_is_gc          , nullptr),
    INIT_MEMBER(tp_bases          , nullptr),
    INIT_MEMBER(tp_mro            , nullptr),
    INIT_MEMBER(tp_cache          , nullptr),
    INIT_MEMBER(tp_subclasses     , nullptr),
    INIT_MEMBER(tp_weaklist       , nullptr),
    INIT_MEMBER(tp_del            , nullptr),
    INIT_MEMBER(tp_version_tag    , 0),
#if PY_MAJOR_VERSION >= 3
    INIT_MEMBER(tp_finalize       , nullptr),
#endif
#if PY_VERSION_HEX >= 0x030800b1
    INIT_MEMBER(tp_vectorcall     , nullptr),
#endif
#if PY_VERSION_HEX >= 0x030800b4 && PY_VERSION_HEX < 0x03090000
    INIT_MEMBER(tp_print          , nullptr),
#endif
};

//////////////////////////////////////////////////////////////////////////////
// TPyLazyDict implementation
//////////////////////////////////////////////////////////////////////////////
int TPyLazyDict::Bool(PyObject* self)
{
    PY_TRY {
        return NUdf::TBoxedValueAccessor::HasDictItems(*Cast(self)->Value.Get()) ? 1 : 0;
    } PY_CATCH(-1)
}

PyObject* TPyLazyDict::Repr(PyObject*)
{
    return PyRepr("<yql.TDict>").Release();
}

Py_ssize_t TPyLazyDict::Len(PyObject* self)
{
    PY_TRY {
        return static_cast<Py_ssize_t>(NUdf::TBoxedValueAccessor::GetDictLength(*Cast(self)->Value.Get()));
    } PY_CATCH(-1)
}

PyObject* TPyLazyDict::Subscript(PyObject* self, PyObject* key)
{
    PY_TRY {
        TPyLazyDict* dict = Cast(self);

        if (dict->KeyType) {
            const auto mkqlKey = FromPyObject(dict->CastCtx, dict->KeyType, key);
            if (auto value = NUdf::TBoxedValueAccessor::Lookup(*dict->Value.Get(), mkqlKey)) {
                return ToPyObject(dict->CastCtx, dict->PayloadType, value.Release().GetOptionalValue()).Release();
            }

            const TPyObjectPtr repr = PyObject_Repr(key);
            PyErr_SetObject(PyExc_KeyError, repr.Get());
            return nullptr;
        } else {
            if (!PyIndex_Check(key)) {
                const TPyObjectPtr type = PyObject_Type(key);
                const TPyObjectPtr repr = PyObject_Repr(type.Get());
                const TPyObjectPtr error = PyUnicode_FromFormat("Unsupported index object type: %R", repr.Get());
                PyErr_SetObject(PyExc_TypeError, error.Get());
                return nullptr;
            }

            const Py_ssize_t index = PyNumber_AsSsize_t(key, PyExc_IndexError);
            if (index < 0) {
                return nullptr;
            }

            if (auto value = NUdf::TBoxedValueAccessor::Lookup(*dict->Value.Get(), NUdf::TUnboxedValuePod(ui64(index)))) {
                return ToPyObject(dict->CastCtx, dict->PayloadType, value.Release().GetOptionalValue()).Release();
            }

            const TPyObjectPtr repr = PyObject_Repr(key);
            PyErr_SetObject(PyExc_IndexError, repr.Get());
            return nullptr;
        }

    } PY_CATCH(nullptr)
}

// -1  error
// 0   not found
// 1   found
int TPyLazyDict::Contains(PyObject* self, PyObject* key)
{
    PY_TRY {
        TPyLazyDict* dict = Cast(self);
        NUdf::TUnboxedValue mkqlKey;

        if (dict->KeyType) {
            mkqlKey = FromPyObject(dict->CastCtx, dict->KeyType, key);
        } else {
            if (!PyIndex_Check(key)) {
                const TPyObjectPtr type = PyObject_Type(key);
                const TPyObjectPtr repr = PyObject_Repr(type.Get());
                const TPyObjectPtr error = PyUnicode_FromFormat("Unsupported index object type: %R", repr.Get());
                PyErr_SetObject(PyExc_TypeError, error.Get());
                return -1;
            }

            const Py_ssize_t index = PyNumber_AsSsize_t(key, PyExc_IndexError);
            if (index < 0) {
                return 0;
            }
            mkqlKey = NUdf::TUnboxedValuePod(ui64(index));
        }

        return NUdf::TBoxedValueAccessor::Contains(*dict->Value.Get(), mkqlKey) ? 1 : 0;
    } PY_CATCH(-1)
}

PyObject* TPyLazyDict::Get(PyObject* self, PyObject* args)
{
    PY_TRY {
        PyObject* key = nullptr;
        PyObject* failobj = Py_None;

        if (!PyArg_UnpackTuple(args, "get", 1, 2, &key, &failobj))
            return nullptr;

        TPyLazyDict* dict = Cast(self);
        if (dict->KeyType) {
            const auto mkqlKey = FromPyObject(dict->CastCtx, dict->KeyType, key);
            if (auto value = NUdf::TBoxedValueAccessor::Lookup(*dict->Value.Get(), mkqlKey)) {
                return ToPyObject(dict->CastCtx, dict->PayloadType, value.Release().GetOptionalValue()).Release();
            }
        } else {
            if (!PyIndex_Check(key)) {
                const TPyObjectPtr type = PyObject_Type(key);
                const TPyObjectPtr repr = PyObject_Repr(type.Get());
                const TPyObjectPtr error = PyUnicode_FromFormat("Unsupported index object type: %R", repr.Get());
                PyErr_SetObject(PyExc_TypeError, error.Get());
                return nullptr;
            }

            const Py_ssize_t index = PyNumber_AsSsize_t(key, PyExc_IndexError);
            if (index < 0) {
                return nullptr;
            }

            if (auto value = NUdf::TBoxedValueAccessor::Lookup(*dict->Value.Get(), NUdf::TUnboxedValuePod(ui64(index)))) {
                return ToPyObject(dict->CastCtx, dict->PayloadType, value.Release().GetOptionalValue()).Release();
            }
        }

        Py_INCREF(failobj);
        return failobj;
    } PY_CATCH(nullptr)
}

PyObject* TPyLazyDict::Keys(PyObject* self, PyObject* /* args */)
{
    PY_TRY {
        const auto dict = Cast(self);
        return ToPyIterator(dict->CastCtx, dict->KeyType,
            NUdf::TBoxedValueAccessor::GetKeysIterator(*dict->Value.Get())).Release();
    } PY_CATCH(nullptr)
}

PyObject* TPyLazyDict::Items(PyObject* self, PyObject* /* args */)
{
    PY_TRY {
        const auto dict = Cast(self);
        return ToPyIterator(dict->CastCtx, dict->KeyType, dict->PayloadType,
            NUdf::TBoxedValueAccessor::GetDictIterator(*dict->Value.Get())).Release();
    } PY_CATCH(nullptr)
}

PyObject* TPyLazyDict::Values(PyObject* self, PyObject* /* args */)
{
    PY_TRY {
        const auto dict = Cast(self);
        return ToPyIterator(dict->CastCtx, dict->PayloadType,
            NUdf::TBoxedValueAccessor::GetPayloadsIterator(*dict->Value.Get())).Release();
    } PY_CATCH(nullptr)
}

PyObject* TPyLazyDict::New(
        const TPyCastContext::TPtr& castCtx,
        const NUdf::TType* keyType,
        const NUdf::TType* payloadType,
        NUdf::IBoxedValuePtr&& value)
{
    TPyLazyDict* dict = new TPyLazyDict;
    PyObject_INIT(dict, &PyLazyDictType);

    dict->CastCtx = castCtx;
    dict->KeyType = keyType;
    dict->PayloadType = payloadType;
    dict->Value.Set(castCtx->PyCtx, value);
    return reinterpret_cast<PyObject*>(dict);
}

//////////////////////////////////////////////////////////////////////////////
// TPyLazySet implementation
//////////////////////////////////////////////////////////////////////////////
int TPyLazySet::Bool(PyObject* self)
{
    PY_TRY {
        return NUdf::TBoxedValueAccessor::HasDictItems(*Cast(self)->Value.Get()) ? 1 : 0;
    } PY_CATCH(-1)
}

PyObject* TPyLazySet::Repr(PyObject*)
{
    return PyRepr("<yql.TSet>").Release();
}

Py_ssize_t TPyLazySet::Len(PyObject* self)
{
    PY_TRY {
        return static_cast<Py_ssize_t>(NUdf::TBoxedValueAccessor::GetDictLength(*Cast(self)->Value.Get()));
    } PY_CATCH(-1)
}

// -1  error
// 0   not found
// 1   found
int TPyLazySet::Contains(PyObject* self, PyObject* key)
{
    PY_TRY {
        const auto set = Cast(self);
        const auto mkqlKey = FromPyObject(set->CastCtx, set->ItemType, key);
        return NUdf::TBoxedValueAccessor::Contains(*set->Value.Get(), mkqlKey) ? 1 : 0;
    } PY_CATCH(-1)
}

PyObject* TPyLazySet::Iter(PyObject* self)
{
    PY_TRY {
        const auto set = Cast(self);
        return ToPyIterator(set->CastCtx, set->ItemType,
            NUdf::TBoxedValueAccessor::GetKeysIterator(*set->Value.Get())).Release();
    } PY_CATCH(nullptr)
}

PyObject* TPyLazySet::New(
        const TPyCastContext::TPtr& castCtx,
        const NUdf::TType* itemType,
        NUdf::IBoxedValuePtr&& value)
{
    TPyLazySet* dict = new TPyLazySet;
    PyObject_INIT(dict, &PyLazySetType);

    dict->CastCtx = castCtx;
    dict->ItemType = itemType;
    dict->Value.Set(castCtx->PyCtx, value);
    return reinterpret_cast<PyObject*>(dict);
}

//////////////////////////////////////////////////////////////////////////////

TPyObjectPtr ToPyLazyDict(
        const TPyCastContext::TPtr& castCtx,
        const NUdf::TType* keyType,
        const NUdf::TType* payloadType,
        const NUdf::TUnboxedValuePod& value)
{
    return TPyLazyDict::New(castCtx, keyType, payloadType, value.AsBoxed());
}

TPyObjectPtr ToPyLazySet(
        const TPyCastContext::TPtr& castCtx,
        const NUdf::TType* itemType,
        const NUdf::TUnboxedValuePod& value)
{
    return TPyLazySet::New(castCtx, itemType, value.AsBoxed());
}

} // namspace NPython
