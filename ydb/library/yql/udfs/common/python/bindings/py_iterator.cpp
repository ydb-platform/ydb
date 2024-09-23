#include "py_iterator.h"
#include "py_cast.h"
#include "py_errors.h"
#include "py_utils.h"

#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>

using namespace NKikimr;

namespace NPython {

//////////////////////////////////////////////////////////////////////////////
// TPyIterator interface
//////////////////////////////////////////////////////////////////////////////
struct TPyIterator
{
    PyObject_HEAD;
    TPyCastContext::TPtr CastCtx;
    const NUdf::TType* ItemType;
    TPyCleanupListItem<NUdf::IBoxedValuePtr> Iterator;

    inline static TPyIterator* Cast(PyObject* o) {
        return reinterpret_cast<TPyIterator*>(o);
    }

    inline static void Dealloc(PyObject* self) {
        delete Cast(self);
    }

    inline static PyObject* Repr(PyObject* self) {
        Y_UNUSED(self);
        return PyRepr("<yql.TDictKeysIterator>").Release();
    }

    static PyObject* New(const TPyCastContext::TPtr& ctx, const NUdf::TType* itemType, NUdf::IBoxedValuePtr&& iterator);
    static PyObject* Next(PyObject* self);
};

#if PY_MAJOR_VERSION >= 3
#define Py_TPFLAGS_HAVE_ITER 0
#endif

PyTypeObject PyIteratorType = {
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    INIT_MEMBER(tp_name           , "yql.TIterator"),
    INIT_MEMBER(tp_basicsize      , sizeof(TPyIterator)),
    INIT_MEMBER(tp_itemsize       , 0),
    INIT_MEMBER(tp_dealloc        , TPyIterator::Dealloc),
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
    INIT_MEMBER(tp_repr           , TPyIterator::Repr),
    INIT_MEMBER(tp_as_number      , nullptr),
    INIT_MEMBER(tp_as_sequence    , nullptr),
    INIT_MEMBER(tp_as_mapping     , nullptr),
    INIT_MEMBER(tp_hash           , nullptr),
    INIT_MEMBER(tp_call           , nullptr),
    INIT_MEMBER(tp_str            , nullptr),
    INIT_MEMBER(tp_getattro       , nullptr),
    INIT_MEMBER(tp_setattro       , nullptr),
    INIT_MEMBER(tp_as_buffer      , nullptr),
    INIT_MEMBER(tp_flags          , Py_TPFLAGS_HAVE_ITER),
    INIT_MEMBER(tp_doc            , "yql.TDictKeysIterator object"),
    INIT_MEMBER(tp_traverse       , nullptr),
    INIT_MEMBER(tp_clear          , nullptr),
    INIT_MEMBER(tp_richcompare    , nullptr),
    INIT_MEMBER(tp_weaklistoffset , 0),
    INIT_MEMBER(tp_iter           , PyObject_SelfIter),
    INIT_MEMBER(tp_iternext       , TPyIterator::Next),
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
// TPyPairIterator interface
//////////////////////////////////////////////////////////////////////////////
struct TPyPairIterator
{
    PyObject_HEAD;
    TPyCastContext::TPtr CastCtx;
    const NUdf::TType* KeyType;
    const NUdf::TType* PayloadType;
    TPyCleanupListItem<NUdf::IBoxedValuePtr> Iterator;

    inline static TPyPairIterator* Cast(PyObject* o) {
        return reinterpret_cast<TPyPairIterator*>(o);
    }

    inline static void Dealloc(PyObject* self) {
        delete Cast(self);
    }

    inline static PyObject* Repr(PyObject* self) {
        Y_UNUSED(self);
        return PyRepr("<yql.TDictIterator>").Release();
    }

    static PyObject* New(const TPyCastContext::TPtr& ctx, const NUdf::TType* keyType, const NUdf::TType* payloadType, NUdf::IBoxedValuePtr&& iterator);
    static PyObject* Next(PyObject* self);
};

PyTypeObject PyPairIteratorType = {
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    INIT_MEMBER(tp_name           , "yql.TDictIterator"),
    INIT_MEMBER(tp_basicsize      , sizeof(TPyPairIterator)),
    INIT_MEMBER(tp_itemsize       , 0),
    INIT_MEMBER(tp_dealloc        , TPyPairIterator::Dealloc),
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
    INIT_MEMBER(tp_repr           , TPyPairIterator::Repr),
    INIT_MEMBER(tp_as_number      , nullptr),
    INIT_MEMBER(tp_as_sequence    , nullptr),
    INIT_MEMBER(tp_as_mapping     , nullptr),
    INIT_MEMBER(tp_hash           , nullptr),
    INIT_MEMBER(tp_call           , nullptr),
    INIT_MEMBER(tp_str            , nullptr),
    INIT_MEMBER(tp_getattro       , nullptr),
    INIT_MEMBER(tp_setattro       , nullptr),
    INIT_MEMBER(tp_as_buffer      , nullptr),
    INIT_MEMBER(tp_flags          , Py_TPFLAGS_HAVE_ITER),
    INIT_MEMBER(tp_doc            , "yql.TPairIterator object"),
    INIT_MEMBER(tp_traverse       , nullptr),
    INIT_MEMBER(tp_clear          , nullptr),
    INIT_MEMBER(tp_richcompare    , nullptr),
    INIT_MEMBER(tp_weaklistoffset , 0),
    INIT_MEMBER(tp_iter           , PyObject_SelfIter),
    INIT_MEMBER(tp_iternext       , TPyPairIterator::Next),
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
// TPyIterator implementation
//////////////////////////////////////////////////////////////////////////////
PyObject* TPyIterator::New(const TPyCastContext::TPtr& ctx, const NUdf::TType* itemType, NUdf::IBoxedValuePtr&& iterator)
{
    TPyIterator* dictIter = new TPyIterator;
    PyObject_INIT(dictIter, &PyIteratorType);
    dictIter->CastCtx = ctx;
    dictIter->ItemType = itemType;
    dictIter->Iterator.Set(ctx->PyCtx, iterator);
    return reinterpret_cast<PyObject*>(dictIter);
}

PyObject* TPyIterator::Next(PyObject* self)
{
    PY_TRY {
        const auto iter = Cast(self);
        NUdf::TUnboxedValue item;
        if (NUdf::TBoxedValueAccessor::Next(*iter->Iterator.Get(), item)) {
            return (iter->ItemType ? ToPyObject(iter->CastCtx, iter->ItemType, item) : PyCast<ui64>(item.Get<ui64>())).Release();
        }
        return nullptr;
    } PY_CATCH(nullptr)
}

//////////////////////////////////////////////////////////////////////////////
// TPyPairIterator implementation
//////////////////////////////////////////////////////////////////////////////
PyObject* TPyPairIterator::New(const TPyCastContext::TPtr& ctx, const NUdf::TType* keyType, const NUdf::TType* payloadType, NUdf::IBoxedValuePtr&& iterator)
{
    TPyPairIterator* dictIter = new TPyPairIterator;
    PyObject_INIT(dictIter, &PyPairIteratorType);
    dictIter->CastCtx = ctx;
    dictIter->KeyType = keyType;
    dictIter->PayloadType = payloadType;
    dictIter->Iterator.Set(ctx->PyCtx, iterator);
    return reinterpret_cast<PyObject*>(dictIter);
}

PyObject* TPyPairIterator::Next(PyObject* self)
{
    PY_TRY {
        const auto iter = Cast(self);
        NUdf::TUnboxedValue k, v;
        if (NUdf::TBoxedValueAccessor::NextPair(*iter->Iterator.Get(), k, v)) {
            const TPyObjectPtr key = iter->KeyType ?
                ToPyObject(iter->CastCtx, iter->KeyType, k):
                PyCast<ui64>(k.Get<ui64>());
            const TPyObjectPtr value = ToPyObject(iter->CastCtx, iter->PayloadType, v);
            return PyTuple_Pack(2, key.Get(), value.Get());
        }
        return nullptr;
    } PY_CATCH(nullptr)
}

//////////////////////////////////////////////////////////////////////////////

TPyObjectPtr ToPyIterator(
        const TPyCastContext::TPtr& castCtx,
        const NUdf::TType* itemType,
        const NUdf::TUnboxedValuePod& value)
{
    return TPyIterator::New(castCtx, itemType, value.AsBoxed());
}

TPyObjectPtr ToPyIterator(
        const TPyCastContext::TPtr& castCtx,
        const NUdf::TType* keyType,
        const NUdf::TType* payloadType,
        const NUdf::TUnboxedValuePod& value)
{
    return TPyPairIterator::New(castCtx, keyType, payloadType, value.AsBoxed());
}

} // namspace NPython
