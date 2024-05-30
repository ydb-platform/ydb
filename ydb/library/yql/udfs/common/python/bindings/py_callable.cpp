#include "py_callable.h"
#include "py_cast.h"
#include "py_errors.h"
#include "py_gil.h"
#include "py_stream.h"
#include "py_utils.h"

#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>
#include <ydb/library/yql/public/udf/udf_type_inspection.h>
#include <ydb/library/yql/public/udf/udf_terminator.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/string/builder.h>

using namespace NKikimr;

namespace NPython {
namespace {

//////////////////////////////////////////////////////////////////////////////
// TPyCallableObject
//////////////////////////////////////////////////////////////////////////////
struct TPyCallableObject
{
    PyObject_HEAD;
    TPyCastContext::TPtr CastCtx;
    const NUdf::TType* Type;
    TPyCleanupListItem<NUdf::IBoxedValuePtr> Value;
    NUdf::TCallableTypeInspector Inspector;

    TPyCallableObject(const TPyCastContext::TPtr& castCtx, const NUdf::TType* type)
        : CastCtx(castCtx)
        , Type(type)
        , Inspector(*castCtx->PyCtx->TypeInfoHelper, type)
    {}
};

inline TPyCallableObject* CastToCallable(PyObject* o)
{
    return reinterpret_cast<TPyCallableObject*>(o);
}

void CallableDealloc(PyObject* self)
{
    delete CastToCallable(self);
}

PyObject* CallableRepr(PyObject*)
{
    // TODO: print callable signature
    return PyRepr("<yql.TCallable>").Release();
}

PyObject* CallableCall(PyObject *self, PyObject *args, PyObject *kwargs)
{
    Y_UNUSED(kwargs);

    PY_TRY {
        TPyCallableObject* callable = CastToCallable(self);
        auto callableType = callable->Type;
        auto valueBuilder = callable->CastCtx->ValueBuilder;
        const auto& inspector = callable->Inspector;

        TSmallVec<NUdf::TUnboxedValue> cArgs;
        cArgs.resize(inspector.GetArgsCount());
        FromPyArgs(callable->CastCtx, callableType, args, cArgs.data(), inspector);

        NUdf::TUnboxedValue result;
        {
            TPyGilUnlocker unlock;
            result = NUdf::TBoxedValueAccessor::Run(*callable->Value.Get(), valueBuilder, cArgs.data());
        }

        return ToPyObject(callable->CastCtx, inspector.GetReturnType(), result).Release();
    } PY_CATCH(nullptr)
}

}

PyTypeObject PyCallableType = {
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    INIT_MEMBER(tp_name           , "yql.TCallable"),
    INIT_MEMBER(tp_basicsize      , sizeof(TPyCallableObject)),
    INIT_MEMBER(tp_itemsize       , 0),
    INIT_MEMBER(tp_dealloc        , CallableDealloc),
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
    INIT_MEMBER(tp_repr           , CallableRepr),
    INIT_MEMBER(tp_as_number      , nullptr),
    INIT_MEMBER(tp_as_sequence    , nullptr),
    INIT_MEMBER(tp_as_mapping     , nullptr),
    INIT_MEMBER(tp_hash           , nullptr),
    INIT_MEMBER(tp_call           , CallableCall),
    INIT_MEMBER(tp_str            , nullptr),
    INIT_MEMBER(tp_getattro       , nullptr),
    INIT_MEMBER(tp_setattro       , nullptr),
    INIT_MEMBER(tp_as_buffer      , nullptr),
    INIT_MEMBER(tp_flags          , 0),
    INIT_MEMBER(tp_doc            , "yql.TCallable object"),
    INIT_MEMBER(tp_traverse       , nullptr),
    INIT_MEMBER(tp_clear          , nullptr),
    INIT_MEMBER(tp_richcompare    , nullptr),
    INIT_MEMBER(tp_weaklistoffset , 0),
    INIT_MEMBER(tp_iter           , nullptr),
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
// TPyCallable
//////////////////////////////////////////////////////////////////////////////
class TPyCallable: public NUdf::TBoxedValue
{
public:
    TPyCallable(
            PyObject* function,
            const NUdf::TType* functionType,
            const TPyCastContext::TPtr& castCtx)
        : Function_(function, TPyObjectPtr::ADD_REF)
        , FunctionType_(functionType)
        , CastCtx_(castCtx)
        , Inspector_(*castCtx->PyCtx->TypeInfoHelper, functionType)
    {
        // keep ownership of function closure if any
        if (PyFunction_Check(function)) {
            PyObject* closure = PyFunction_GetClosure(function);
            if (closure) {
                Closure_ = TPyObjectPtr(closure, TPyObjectPtr::ADD_REF);
            }
        }
    }

    ~TPyCallable() {
        TPyGilLocker lock;
        Closure_.Reset();
        Function_.Reset();
        CastCtx_.Reset();
    }

private:
    NUdf::TUnboxedValue Run(
            const NUdf::IValueBuilder*,
            const NUdf::TUnboxedValuePod* args) const final
    {
        TPyGilLocker lock;
        try {
            TPyObjectPtr pyArgs = ToPyArgs(CastCtx_, FunctionType_, args, Inspector_);
            TPyObjectPtr resultObj =
                    PyObject_CallObject(Function_.Get(), pyArgs.Get());
            if (!resultObj) {
                UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << "Failed to execute:\n" << GetLastErrorAsString()).data());
            }

            auto returnType = Inspector_.GetReturnType();
            if (CastCtx_->PyCtx->TypeInfoHelper->GetTypeKind(returnType) == NUdf::ETypeKind::Stream) {
                return FromPyStream(CastCtx_, returnType, resultObj, Function_, Closure_, pyArgs);
            }

            return FromPyObject(CastCtx_, returnType, resultObj.Get());
        } catch (const yexception& e) {
            UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << "Failed to cast arguments or result\n" << e.what()).data());
        }
    }

    TPyObjectPtr Function_;
    TPyObjectPtr Closure_;
    const NUdf::TType* FunctionType_;
    TPyCastContext::TPtr CastCtx_;
    NUdf::TCallableTypeInspector Inspector_;
};


TPyObjectPtr ToPyCallable(
        const TPyCastContext::TPtr& castCtx,
        const NUdf::TType* type,
        const NUdf::TUnboxedValuePod& value)
{
    TPyCallableObject* callable = new TPyCallableObject(castCtx, type);
    PyObject_INIT(callable, &PyCallableType);

    callable->Value.Set(castCtx->PyCtx, value.AsBoxed());

    return reinterpret_cast<PyObject*>(callable);
}

NUdf::TUnboxedValue FromPyCallable(
        const TPyCastContext::TPtr& castCtx,
        const NUdf::TType* type,
        PyObject* value)
{
    return NUdf::TUnboxedValuePod(new TPyCallable(value, type, castCtx));
}

TMaybe<TPyObjectPtr> GetOptionalAttribute(PyObject* value, const char* attrName) {
    if (TPyObjectPtr attr = PyObject_GetAttrString(value, attrName)) {
        return attr;
    } else {
        if (PyErr_ExceptionMatches(PyExc_AttributeError)) {
            PyErr_Clear();
            return Nothing();
        } else {
            throw yexception() << "Cannot get attribute '" << attrName << "', error: " << GetLastErrorAsString();
        }
    }
}


struct TPySecureParam
{
    PyObject_HEAD;
    TPyCastContext::TPtr CastCtx;

    TPySecureParam(const TPyCastContext::TPtr& castCtx) : CastCtx(castCtx) {}
};

inline TPySecureParam* CastToSecureParam(PyObject* o)
{
    return reinterpret_cast<TPySecureParam*>(o);
}

void SecureParamDealloc(PyObject* self)
{
    delete CastToSecureParam(self);
}

PyObject* SecureParamRepr(PyObject*)
{
    return PyRepr("<yql.TSecureParam>").Release();
}

PyObject* SecureParamCall(PyObject* self, PyObject* args, PyObject* kwargs)
{
    Y_UNUSED(kwargs);

    struct PyBufDeleter {
        void operator() (Py_buffer* view) { PyBuffer_Release(view); }
    };
    Py_buffer input;
    if (!PyArg_ParseTuple(args, "s*", &input)) {
        return nullptr;
    }
    std::unique_ptr<Py_buffer, PyBufDeleter> bufPtr(&input);
    auto valueBuilder = CastToSecureParam(self)->CastCtx->ValueBuilder;
    NUdf::TStringRef key(static_cast<const char*>(input.buf), input.len);
    PY_TRY {
        if (!valueBuilder->GetSecureParam(key, key)) {
            throw yexception() << "Cannot get secure parameter for key: " << key;
        }
        return PyRepr(TStringBuf(key.Data(), key.Size())).Release();
    } PY_CATCH(nullptr)
}

static PyTypeObject PySecureParamType = {
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    INIT_MEMBER(tp_name           , "yql.TSecureParam"),
    INIT_MEMBER(tp_basicsize      , sizeof(TPySecureParam)),
    INIT_MEMBER(tp_itemsize       , 0),
    INIT_MEMBER(tp_dealloc        , SecureParamDealloc),
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
    INIT_MEMBER(tp_repr           , SecureParamRepr),
    INIT_MEMBER(tp_as_number      , nullptr),
    INIT_MEMBER(tp_as_sequence    , nullptr),
    INIT_MEMBER(tp_as_mapping     , nullptr),
    INIT_MEMBER(tp_hash           , nullptr),
    INIT_MEMBER(tp_call           , SecureParamCall),
    INIT_MEMBER(tp_str            , nullptr),
    INIT_MEMBER(tp_getattro       , nullptr),
    INIT_MEMBER(tp_setattro       , nullptr),
    INIT_MEMBER(tp_as_buffer      , nullptr),
    INIT_MEMBER(tp_flags          , 0),
    INIT_MEMBER(tp_doc            , "yql.TSecureParam object"),
    INIT_MEMBER(tp_traverse       , nullptr),
    INIT_MEMBER(tp_clear          , nullptr),
    INIT_MEMBER(tp_richcompare    , nullptr),
    INIT_MEMBER(tp_weaklistoffset , 0),
    INIT_MEMBER(tp_iter           , nullptr),
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

TPyObjectPtr ToPySecureParam(const TPyCastContext::TPtr& castCtx)
{
    TPySecureParam* ret = new TPySecureParam(castCtx);
    PyObject_INIT(ret, &PySecureParamType);
    return reinterpret_cast<PyObject*>(ret);
}


void SetupCallableSettings(const TPyCastContext::TPtr& castCtx, PyObject* value) {
    if (const auto lazyInput = GetOptionalAttribute(value, "_yql_lazy_input")) try {
        castCtx->LazyInputObjects = PyCast<bool>(lazyInput->Get());
    } catch (const yexception& e) {
        throw yexception() << "Cannot parse attribute '_yql_lazy_input', error: " << e.what();
    }

    if (const auto convertYson = GetOptionalAttribute(value, "_yql_convert_yson")) try {
        Py_ssize_t itemsCount = PyTuple_GET_SIZE(convertYson->Get());
        if (itemsCount != 2) {
            throw yexception() << "Expected tuple of 2 callables";
        }

        castCtx->YsonConverterIn.ResetAddRef(PyTuple_GET_ITEM(convertYson->Get(), 0));
        castCtx->YsonConverterOut.ResetAddRef(PyTuple_GET_ITEM(convertYson->Get(), 1));
        if (!PyCallable_Check(castCtx->YsonConverterIn.Get()) || !PyCallable_Check(castCtx->YsonConverterOut.Get())) {
            throw yexception() << "Expected tuple of 2 callables";
        }
    } catch (const yexception& e) {
        throw yexception() << "Cannot parse attribute '_yql_convert_yson', error: " << e.what();
    }

    if (const auto bytesDecodeMode = GetOptionalAttribute(value, "_yql_bytes_decode_mode")) try {
        PyObject* bytesValue = nullptr;
        if (PyBytes_Check(bytesDecodeMode->Get())) {
            bytesValue = PyObject_Bytes(bytesDecodeMode->Get());
        } else if (PyUnicode_Check(bytesDecodeMode->Get())) {
            bytesValue = PyUnicode_AsUTF8String(bytesDecodeMode->Get());
        } else {
            throw yexception() << "Expected bytes or unicode";
        }
        if (!bytesValue) {
            PyErr_Clear();
            throw yexception() << "Failed to convert to bytes";
        }

        TStringBuf view(PyBytes_AS_STRING(bytesValue));
        if (view == "never") {
            castCtx->BytesDecodeMode = EBytesDecodeMode::Never;
        } else if (view == "strict") {
            castCtx->BytesDecodeMode = EBytesDecodeMode::Strict;
        } else {
            Py_DECREF(bytesValue);
            throw yexception() << "Expected values 'never' or 'strict'";
        }
        Py_DECREF(bytesValue);
    } catch (const yexception& e) {
        throw yexception() << "Cannot parse attribute '_yql_bytes_decode_mode', error: " << e.what();
    }

    if (PyObject_SetAttrString(value, "_yql_secure_param", ToPySecureParam(castCtx).Get()) != 0) {
        throw yexception() << "Cannot set attribute '_yql_secure_param'";
    }
}

} // namespace NPython
