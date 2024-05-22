#include "py_stream.h"
#include "py_cast.h"
#include "py_errors.h"
#include "py_gil.h"
#include "py_utils.h"

#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>
#include <ydb/library/yql/public/udf/udf_type_inspection.h>
#include <ydb/library/yql/public/udf/udf_terminator.h>

#include <util/string/builder.h>

using namespace NKikimr;

namespace NPython {

// will be initialized in InitYqlModule()
PyObject* PyYieldIterationException = nullptr;

//////////////////////////////////////////////////////////////////////////////
// TPyStream
//////////////////////////////////////////////////////////////////////////////
struct TPyStream {
    PyObject_HEAD;
    TPyCastContext::TPtr CastCtx;
    TPyCleanupListItem<NUdf::IBoxedValuePtr> Value;
    const NUdf::TType* ItemType;

    inline static TPyStream* Cast(PyObject* o) {
        return reinterpret_cast<TPyStream*>(o);
    }

    inline static void Dealloc(PyObject* self) {
        delete Cast(self);
    }

    inline static PyObject* Repr(PyObject* self) {
        Y_UNUSED(self);
        return PyRepr("<yql.TStream>").Release();
    }

    static PyObject* New(
            const TPyCastContext::TPtr& castCtx,
            const NUdf::TType* type,
            NUdf::IBoxedValuePtr value);

    static PyObject* Next(PyObject* self);
};

#if PY_MAJOR_VERSION >= 3
#define Py_TPFLAGS_HAVE_ITER 0
#endif

PyTypeObject PyStreamType = {
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    INIT_MEMBER(tp_name           , "yql.TStream"),
    INIT_MEMBER(tp_basicsize      , sizeof(TPyStream)),
    INIT_MEMBER(tp_itemsize       , 0),
    INIT_MEMBER(tp_dealloc        , TPyStream::Dealloc),
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
    INIT_MEMBER(tp_repr           , TPyStream::Repr),
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
    INIT_MEMBER(tp_doc            , "yql.TStream object"),
    INIT_MEMBER(tp_traverse       , nullptr),
    INIT_MEMBER(tp_clear          , nullptr),
    INIT_MEMBER(tp_richcompare    , nullptr),
    INIT_MEMBER(tp_weaklistoffset , 0),
    INIT_MEMBER(tp_iter           , PyObject_SelfIter),
    INIT_MEMBER(tp_iternext       , TPyStream::Next),
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

PyObject* TPyStream::New(
        const TPyCastContext::TPtr& castCtx,
        const NUdf::TType* type,
        NUdf::IBoxedValuePtr value)
{
    TPyStream* stream = new TPyStream;
    PyObject_INIT(stream, &PyStreamType);
    stream->CastCtx = castCtx;
    stream->Value.Set(castCtx->PyCtx, value);

    const NUdf::TStreamTypeInspector inspector(*castCtx->PyCtx->TypeInfoHelper, type);
    stream->ItemType = inspector.GetItemType();

    return reinterpret_cast<PyObject*>(stream);
}

PyObject* TPyStream::Next(PyObject* self) {
    PY_TRY {
        TPyStream* stream = Cast(self);

        NUdf::TUnboxedValue item;
        auto status = NUdf::TBoxedValueAccessor::Fetch(*stream->Value.Get(), item);

        switch (status) {
        case NUdf::EFetchStatus::Ok:
            return ToPyObject(stream->CastCtx, stream->ItemType, item)
                    .Release();
        case NUdf::EFetchStatus::Finish:
            return nullptr;
        case NUdf::EFetchStatus::Yield:
            PyErr_SetNone(PyYieldIterationException);
            return nullptr;
        default:
            Y_ABORT("Unknown stream status");
        }
    } PY_CATCH(nullptr)
}

//////////////////////////////////////////////////////////////////////////////
// TStreamOverPyIter
//////////////////////////////////////////////////////////////////////////////
class TStreamOverPyIter final: public NUdf::TBoxedValue {
public:
    TStreamOverPyIter(
            TPyCastContext::TPtr castCtx,
            const NUdf::TType* itemType,
            TPyObjectPtr pyIter,
            TPyObjectPtr pyIterable,
            TPyObjectPtr pyGeneratorCallable,
            TPyObjectPtr pyGeneratorCallableClosure,
            TPyObjectPtr pyGeneratorCallableArgs)
        : CastCtx_(std::move(castCtx))
        , ItemType_(itemType)
        , PyIter_(std::move(pyIter))
        , PyIterable_(std::move(pyIterable))
        , PyGeneratorCallable_(std::move(pyGeneratorCallable))
        , PyGeneratorCallableClosure_(std::move(pyGeneratorCallableClosure))
        , PyGeneratorCallableArgs_(std::move(pyGeneratorCallableArgs))
    {
    }

    ~TStreamOverPyIter() {
        TPyGilLocker lock;
        PyIter_.Reset();
        PyIterable_.Reset();
        PyGeneratorCallableArgs_.Reset();
        PyGeneratorCallableClosure_.Reset();
        PyGeneratorCallable_.Reset();
    }

private:
    NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
        try {
            TPyGilLocker lock;
            TPyObjectPtr next(PyIter_Next(PyIter_.Get()));
            if (next) {
                if (PyErr_GivenExceptionMatches(next.Get(), PyYieldIterationException)) {
                    return NUdf::EFetchStatus::Yield;
                }

                result = FromPyObject(CastCtx_, ItemType_, next.Get());
                return NUdf::EFetchStatus::Ok;
            }

            if (PyObject* ex = PyErr_Occurred()) {
                if (PyErr_GivenExceptionMatches(ex, PyYieldIterationException)) {
                    PyErr_Clear();
                    TPyObjectPtr iterable;
                    TPyObjectPtr iter;
                    if (PyIterable_) {
                        PyIter_.Reset();
                        iterable = PyIterable_;
                    } else if (PyGeneratorCallable_) {
                        PyIter_.Reset();
                        TPyObjectPtr result(PyObject_CallObject(PyGeneratorCallable_.Get(), PyGeneratorCallableArgs_.Get()));
                        if (!result) {
                            UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << "Failed to execute:\n" << GetLastErrorAsString()).data());
                        }

                        if (PyGen_Check(result.Get())) {
                            iterable = std::move(result);
                        } else if (PyIter_Check(result.Get())) {
                            iter = std::move(result);
                        } else {
                            UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << "Expected iterator or generator, but got " << PyObjectRepr(result.Get())).data());
                        }
                    } else {
                        return NUdf::EFetchStatus::Yield;
                    }

                    if (!iter) {
                        iter.ResetSteal(PyObject_GetIter(iterable.Get()));
                        if (!iter) {
                            UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << GetLastErrorAsString()).data());
                        }
                    }

                    PyIter_.ResetAddRef(iter.Get());
                    return NUdf::EFetchStatus::Yield;
                }

                UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << GetLastErrorAsString()).data());
            }

            return NUdf::EFetchStatus::Finish;
        }
        catch (const yexception& e) {
            UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << e.what()).data());
        }
    }

private:
    TPyCastContext::TPtr CastCtx_;
    const NUdf::TType* ItemType_;
    TPyObjectPtr PyIter_;
    TPyObjectPtr PyIterable_;
    TPyObjectPtr PyGeneratorCallable_;
    TPyObjectPtr PyGeneratorCallableClosure_;
    TPyObjectPtr PyGeneratorCallableArgs_;
};


//////////////////////////////////////////////////////////////////////////////
// public functions
//////////////////////////////////////////////////////////////////////////////
TPyObjectPtr ToPyStream(
        const TPyCastContext::TPtr& castCtx,
        const NKikimr::NUdf::TType* type,
        const NKikimr::NUdf::TUnboxedValuePod& value)
{
    return TPyStream::New(castCtx, type, value.AsBoxed());
}

NKikimr::NUdf::TUnboxedValue FromPyStream(
    const TPyCastContext::TPtr& castCtx,
    const NKikimr::NUdf::TType* type,
    const TPyObjectPtr& value,
    const TPyObjectPtr& originalCallable,
    const TPyObjectPtr& originalCallableClosure,
    const TPyObjectPtr& originalCallableArgs
)
{
    const NUdf::TStreamTypeInspector inspector(*castCtx->PyCtx->TypeInfoHelper, type);
    const NUdf::TType* itemType = inspector.GetItemType();

    if (PyGen_Check(value.Get())) {
        TPyObjectPtr iter(PyObject_GetIter(value.Get()));
        if (!iter) {
            UdfTerminate((TStringBuilder() << castCtx->PyCtx->Pos << GetLastErrorAsString()).data());
        }
        return NUdf::TUnboxedValuePod(new TStreamOverPyIter(castCtx, itemType, std::move(iter), nullptr,
            originalCallable, originalCallableClosure, originalCallableArgs));
    }

    if (PyIter_Check(value.Get())
#if PY_MAJOR_VERSION < 3
        // python 2 iterators must also implement "next" method
        && 1 == PyObject_HasAttrString(value.Get(), "next")
#endif
    ) {
        TPyObjectPtr iter(value.Get(), TPyObjectPtr::ADD_REF);
        return NUdf::TUnboxedValuePod(new TStreamOverPyIter(castCtx, itemType, std::move(iter), nullptr,
            originalCallable, originalCallableClosure, originalCallableArgs));
    }

    // assume that this function will returns generator
    if (PyCallable_Check(value.Get())) {
        TPyObjectPtr generator(PyObject_CallObject(value.Get(), nullptr));
        if (!generator || !PyGen_Check(generator.Get())) {
            UdfTerminate((TStringBuilder() << castCtx->PyCtx->Pos << "Expected generator as a result of function call").data());
        }
        TPyObjectPtr iter(PyObject_GetIter(generator.Get()));
        if (!iter) {
            UdfTerminate((TStringBuilder() << castCtx->PyCtx->Pos << GetLastErrorAsString()).data());
        }

        TPyObjectPtr callableClosure;
        if (PyFunction_Check(value.Get())) {
            PyObject* closure = PyFunction_GetClosure(value.Get());
            if (closure) {
                callableClosure = TPyObjectPtr(closure, TPyObjectPtr::ADD_REF);
            }
        }

        return NUdf::TUnboxedValuePod(new TStreamOverPyIter(castCtx, itemType, std::move(iter), nullptr,
            originalCallable ? value : nullptr, originalCallable ? callableClosure : nullptr, nullptr));
    }

    // must be after checking for callable
    if (PySequence_Check(value.Get()) || PyObject_HasAttrString(value.Get(), "__iter__")) {
        TPyObjectPtr iter(PyObject_GetIter(value.Get()));
        if (!iter) {
            UdfTerminate((TStringBuilder() << castCtx->PyCtx->Pos << GetLastErrorAsString()).data());
        }
        return NUdf::TUnboxedValuePod(new TStreamOverPyIter(castCtx, itemType, std::move(iter), originalCallable ? value : nullptr, nullptr, nullptr, nullptr));
    }

    UdfTerminate((TStringBuilder() << castCtx->PyCtx->Pos << "Expected iterator, generator, generator factory, "
                          "or iterable object, but got " << PyObjectRepr(value.Get())).data());
}

} // namespace NPython
