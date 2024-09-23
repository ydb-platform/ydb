#include "py_cast.h"
#include "py_ptr.h"
#include "py_errors.h"
#include "py_callable.h"
#include "py_dict.h"
#include "py_list.h"
#include "py_gil.h"
#include "py_utils.h"
#include "py_void.h"
#include "py_resource.h"
#include "py_stream.h"
#include "py_struct.h"
#include "py_tuple.h"
#include "py_variant.h"
#include "py_decimal.h"

#include <ydb/library/yql/public/udf/udf_value_builder.h>
#include <ydb/library/yql/public/udf/udf_type_inspection.h>
#include <ydb/library/yql/public/udf/udf_type_printer.h>
#include <ydb/library/yql/public/udf/udf_terminator.h>
#include <ydb/library/yql/utils/utf8.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/string/join.h>
#include <util/string/builder.h>

#ifdef HAVE_LONG_LONG
#   define YQL_PyLong_AsUnsignedMask PyLong_AsUnsignedLongLongMask
#   define YQL_PyLong_Asi64 PyLong_AsLongLong
#   define YQL_PyLong_Asui64 PyLong_AsUnsignedLongLong
#else
#   define YQL_PyLong_AsUnsignedMask PyLong_AsUnsignedLongMask
#   define YQL_PyLong_Asi64 PyLong_AsLong
#   define YQL_PyLong_Asui64 PyLong_AsUnsignedLong
#endif

#define TO_PYTHON(Format, Type) \
    template <> \
    ::NPython::TPyObjectPtr PyCast<Type>(Type value) { \
        return Py_BuildValue(Format, value); \
    }

#define TO_PYTHON_BYTES(Type) \
    template <> \
    ::NPython::TPyObjectPtr PyCast<Type>(const Type& val) { \
        TStringBuf value = val; \
        if (value.data() == nullptr) \
            Py_RETURN_NONE; \
        const Py_ssize_t size = static_cast<Py_ssize_t>(value.size()); \
        return PyBytes_FromStringAndSize(value.data(), size); \
    }

#define TO_PYTHON_UNICODE(Type) \
    template <> \
    ::NPython::TPyObjectPtr ToPyUnicode<Type>(const Type& val) { \
        TStringBuf value = val; \
        if (value.data() == nullptr) \
            Py_RETURN_NONE; \
        Py_ssize_t size = static_cast<Py_ssize_t>(value.size()); \
        return PyUnicode_FromStringAndSize(value.data(), size); \
    }

#define PY_ENSURE_TYPE(Type, Value, Message) \
    do { \
        if (!Py##Type##_Check(Value)) { \
            throw yexception() << Message << " " #Type "; Object repr: " \
                               << PyObjectRepr(Value); \
        } \
    } while (0)

#define FROM_PYTHON_FLOAT(Type) \
    template <> \
    Type PyCast<Type>(PyObject* value) { \
        double result = PyFloat_AsDouble(value); \
        if (result == -1.0 && PyErr_Occurred()) { \
            PyErr_Clear(); \
            ThrowCastException(value, "Float"); \
        } \
        return static_cast<Type>(result); \
    }

#define FROM_PYTHON_LONG(Type, BigType) \
    template <> \
    Type PyCast<Type>(PyObject* value) { \
        if (PyLong_Check(value)) { \
            auto result = YQL_PyLong_As##BigType(value); \
            if (result == static_cast<Type>(-1L) && PyErr_Occurred()) { \
                PyErr_Clear(); \
                ThrowCastException(value, "Long"); \
            } \
            if (result < Min<Type>() || result > Max<Type>()) { \
                throw yexception() << "Python object " << PyObjectRepr(value) \
                    << " is out of range for " << #Type; \
            } \
            return static_cast<Type>(result); \
        } \
        ThrowCastTypeException(value, "Long"); \
    }

#define FROM_PYTHON_INT_OR_LONG(Type, BigType) \
    template <> \
    Type PyCast<Type>(PyObject* value) { \
        if (PyInt_Check(value)) { \
            long result = PyInt_AsLong(value); \
            if (result == -1L && PyErr_Occurred()) { \
                PyErr_Clear(); \
                ThrowCastException(value, "Long"); \
            } \
            if ( \
                static_cast<i64>(Min<long>()) < static_cast<i64>(Min<Type>()) && result < static_cast<long>(Min<Type>()) || \
                static_cast<ui64>(Max<long>()) > static_cast<ui64>(Max<Type>()) && result > static_cast<long>(Max<Type>()) \
            ) { \
                throw yexception() << "Python object " << PyObjectRepr(value) \
                    << " is out of range for " << #Type; \
            } \
            return static_cast<Type>(result); \
        } else if (PyLong_Check(value)) { \
            auto result = YQL_PyLong_As##BigType(value); \
            if (result == static_cast<Type>(-1L) && PyErr_Occurred()) { \
                PyErr_Clear(); \
                ThrowCastException(value, "Long"); \
            } \
            if (result < Min<Type>() || result > Max<Type>()) { \
                throw yexception() << "Python object " << PyObjectRepr(value) \
                    << " is out of range for " << #Type; \
            } \
            return static_cast<Type>(result); \
        } \
        ThrowCastTypeException(value, "Long"); \
    }

#define FROM_PYTHON_BYTES_OR_UTF(Type) \
    template <> \
    Type PyCast<Type>(PyObject* value) { \
        if (PyUnicode_Check(value)) { \
            Py_ssize_t size = 0U; \
            const auto str = PyUnicode_AsUTF8AndSize(value, &size); \
            if (!str || size < 0) { \
                ThrowCastTypeException(value, "String"); \
            } \
            return Type(str, size_t(size)); \
        } else if (PyBytes_Check(value)) { \
            Py_ssize_t size = 0U; \
            char *str = nullptr; \
            const auto rc = PyBytes_AsStringAndSize(value, &str, &size); \
            if (rc == -1 || size < 0) { \
                ThrowCastTypeException(value, "String"); \
            } \
            return Type(str, size_t(size)); \
        } \
        ThrowCastTypeException(value, "String"); \
    }

#define FROM_PYTHON_BYTES(Type) \
    template <> \
    Type PyCast<Type>(PyObject* value) { \
        PY_ENSURE_TYPE(Bytes, value, "Expected"); \
        char* str = nullptr; \
        Py_ssize_t size = 0; \
        const auto rc = PyBytes_AsStringAndSize(value, &str, &size); \
        if (rc == -1 || size < 0) { \
            ThrowCastTypeException(value, "String"); \
        } \
        return Type(str, size_t(size)); \
    }

#define TRY_FROM_PYTHON_FLOAT(Type) \
    template <> \
    bool TryPyCast<Type>(PyObject* value, Type& result) { \
        double v = PyFloat_AsDouble(value); \
        if (v == -1.0 && PyErr_Occurred()) { \
            PyErr_Clear(); \
            return false; \
        } \
        result = static_cast<Type>(v); \
        return true; \
    }

#define TRY_FROM_PYTHON_LONG(Type, BigType) \
    template <> \
    bool TryPyCast<Type>(PyObject* value, Type& res) { \
        if (PyLong_Check(value)) { \
            auto result = YQL_PyLong_As##BigType(value); \
            if (result == static_cast<Type>(-1L) && PyErr_Occurred()) { \
                PyErr_Clear(); \
                return false; \
            } \
            if (result < Min<Type>() || result > Max<Type>()) { \
                return false; \
            } \
            res = static_cast<Type>(result); \
            return true; \
        } \
        return false; \
    }

#define TRY_FROM_PYTHON_INT_OR_LONG(Type, BigType) \
    template <> \
    bool TryPyCast<Type>(PyObject* value, Type& res) { \
        if (PyInt_Check(value)) { \
            long result = PyInt_AsLong(value); \
            if (result == -1L && PyErr_Occurred()) { \
                PyErr_Clear(); \
                return false; \
            } \
            res = static_cast<Type>(result); \
            if (result < static_cast<long>(Min<Type>()) || (static_cast<ui64>(Max<long>()) > static_cast<ui64>(Max<Type>()) && result > static_cast<long>(Max<Type>()))) { \
                return false; \
            } \
            return true; \
        } else if (PyLong_Check(value)) { \
            auto result = YQL_PyLong_As##BigType(value); \
            if (result == static_cast<Type>(-1L) && PyErr_Occurred()) { \
                PyErr_Clear(); \
                return false; \
            } \
            if (result < Min<Type>() || result > Max<Type>()) { \
                return false; \
            } \
            res = static_cast<Type>(result); \
            return true; \
        } \
        return false; \
    }

#define TRY_FROM_PYTHON_BYTES_OR_UTF(Type) \
    template <> \
    bool TryPyCast(PyObject* value, Type& result) { \
        if (PyUnicode_Check(value)) { \
            Py_ssize_t size = 0U; \
            const auto str = PyUnicode_AsUTF8AndSize(value, &size); \
            if (!str || size < 0) { \
                return false; \
            } \
            result = Type(str, size_t(size)); \
            return true; \
        } else if (PyBytes_Check(value)) { \
            Py_ssize_t size = 0U; \
            char *str = nullptr; \
            const auto rc = PyBytes_AsStringAndSize(value, &str, &size); \
            if (rc == -1 || size < 0) { \
                ThrowCastTypeException(value, "String"); \
            } \
            result = Type(str, size_t(size)); \
            return true; \
        } \
        return false; \
    }

#define TRY_FROM_PYTHON_STR_OR_UTF(Type) \
    template <> \
    bool TryPyCast(PyObject* value, Type& result) { \
        if (PyUnicode_Check(value)) { \
            const TPyObjectPtr utf8(PyUnicode_AsUTF8String(value)); \
            char* str = nullptr; \
            Py_ssize_t size = 0; \
            int rc = PyBytes_AsStringAndSize(utf8.Get(), &str, &size); \
            if (rc == -1 || size < 0) { \
                return false; \
            } \
            result = Type(str, size_t(size)); \
            return true; \
        } else if (PyBytes_Check(value)) { \
            char* str = nullptr; \
            Py_ssize_t size = 0; \
            int rc = PyBytes_AsStringAndSize(value, &str, &size); \
            if (rc == -1 || size < 0) { \
                return false; \
            } \
            result = Type(str, size_t(size)); \
            return true; \
        } else { \
            return false; \
        } \
    }

namespace NPython {

using namespace NKikimr;

inline void ThrowCastTypeException(PyObject* value, TStringBuf toType) {
    throw yexception() << "Can't cast object '" << Py_TYPE(value)->tp_name << "' to " << toType
                       << "; Object repr: " << PyObjectRepr(value);
}

inline void ThrowCastException(PyObject* value, TStringBuf toType) {
    throw yexception() << "Cast error object " << PyObjectRepr(value) << " to " << toType << ": "
                       << GetLastErrorAsString();
}


template <>
bool TryPyCast<bool>(PyObject* value, bool& result)
{
    int isTrue = PyObject_IsTrue(value);
    if (isTrue == -1) {
        return false;
    }
    result = (isTrue == 1);
    return true;
}

#if PY_MAJOR_VERSION >= 3
TRY_FROM_PYTHON_LONG(i8, i64)
TRY_FROM_PYTHON_LONG(ui8, ui64)
TRY_FROM_PYTHON_LONG(i16, i64)
TRY_FROM_PYTHON_LONG(ui16, ui64)
TRY_FROM_PYTHON_LONG(i32, i64)
TRY_FROM_PYTHON_LONG(ui32, ui64)
TRY_FROM_PYTHON_LONG(i64, i64)
TRY_FROM_PYTHON_LONG(ui64, ui64)
TRY_FROM_PYTHON_BYTES_OR_UTF(TString)
TRY_FROM_PYTHON_BYTES_OR_UTF(NUdf::TStringRef)
#else
TRY_FROM_PYTHON_INT_OR_LONG(i8, i64)
TRY_FROM_PYTHON_INT_OR_LONG(ui8, ui64)
TRY_FROM_PYTHON_INT_OR_LONG(i16, i64)
TRY_FROM_PYTHON_INT_OR_LONG(ui16, ui64)
TRY_FROM_PYTHON_INT_OR_LONG(i32, i64)
TRY_FROM_PYTHON_INT_OR_LONG(ui32, ui64)
TRY_FROM_PYTHON_INT_OR_LONG(i64, i64)
TRY_FROM_PYTHON_INT_OR_LONG(ui64, ui64)
TRY_FROM_PYTHON_STR_OR_UTF(TString)
TRY_FROM_PYTHON_STR_OR_UTF(NUdf::TStringRef)
#endif

TRY_FROM_PYTHON_FLOAT(float)
TRY_FROM_PYTHON_FLOAT(double)

template <>
bool PyCast<bool>(PyObject* value)
{
    int res = PyObject_IsTrue(value);
    if (res == -1) {
        throw yexception() << "Can't cast object '" << Py_TYPE(value)->tp_name << "' to bool. "
                           << GetLastErrorAsString();
    }
    return res == 1;
}

#if PY_MAJOR_VERSION >= 3
FROM_PYTHON_LONG(i8, i64)
FROM_PYTHON_LONG(ui8, ui64)
FROM_PYTHON_LONG(i16, i64)
FROM_PYTHON_LONG(ui16, ui64)
FROM_PYTHON_LONG(i32, i64)
FROM_PYTHON_LONG(ui32, ui64)
FROM_PYTHON_LONG(i64, i64)
FROM_PYTHON_LONG(ui64, ui64)
FROM_PYTHON_BYTES_OR_UTF(TString)
FROM_PYTHON_BYTES_OR_UTF(TStringBuf)
FROM_PYTHON_BYTES_OR_UTF(NUdf::TStringRef)
#else
FROM_PYTHON_INT_OR_LONG(i8, i64)
FROM_PYTHON_INT_OR_LONG(ui8, ui64)
FROM_PYTHON_INT_OR_LONG(i16, i64)
FROM_PYTHON_INT_OR_LONG(ui16, ui64)
FROM_PYTHON_INT_OR_LONG(i32, i64)
FROM_PYTHON_INT_OR_LONG(ui32, ui64)
FROM_PYTHON_INT_OR_LONG(i64, i64)
FROM_PYTHON_INT_OR_LONG(ui64, ui64)
FROM_PYTHON_BYTES(TString)
FROM_PYTHON_BYTES(TStringBuf)
FROM_PYTHON_BYTES(NUdf::TStringRef)
#endif

FROM_PYTHON_FLOAT(float)
FROM_PYTHON_FLOAT(double)

template <>
TPyObjectPtr PyCast<bool>(bool value)
{
    PyObject* res = value ? Py_True : Py_False;
    return TPyObjectPtr(res, TPyObjectPtr::ADD_REF);
}

TO_PYTHON("b", i8)
TO_PYTHON("B", ui8)
TO_PYTHON("h", i16)
TO_PYTHON("H", ui16)
TO_PYTHON("i", i32)
TO_PYTHON("I", ui32)
#ifdef HAVE_LONG_LONG
TO_PYTHON("L", i64)
TO_PYTHON("K", ui64)
#else
TO_PYTHON("l", i64)
TO_PYTHON("k", ui64)
#endif

TO_PYTHON_BYTES(TString)
TO_PYTHON_BYTES(TStringBuf)
TO_PYTHON_BYTES(NUdf::TStringRef)
TO_PYTHON_UNICODE(TString)
TO_PYTHON_UNICODE(TStringBuf)
TO_PYTHON_UNICODE(NUdf::TStringRef)

template <typename T>
NUdf::TUnboxedValuePod FromPyTz(PyObject* value, T limit, TStringBuf typeName, const TPyCastContext::TPtr& ctx) {
    PY_ENSURE(PyTuple_Check(value),
        "Expected to get Tuple, but got " << Py_TYPE(value)->tp_name);

    Py_ssize_t tupleSize = PyTuple_GET_SIZE(value);
    PY_ENSURE(tupleSize == 2,
        "Expected to get Tuple with 2 elements, but got "
        << tupleSize << " elements");

    PyObject* el0 = PyTuple_GET_ITEM(value, 0);
    PyObject* el1 = PyTuple_GET_ITEM(value, 1);
    auto num = PyCast<T>(el0);
    if (num >= limit) {
        throw yexception() << "Python object " << PyObjectRepr(el0) \
            << " is out of range for " << typeName;
    }

    auto name = PyCast<NUdf::TStringRef>(el1);
    auto ret = NUdf::TUnboxedValuePod(num);
    ui32 tzId;
    if (!ctx->ValueBuilder->GetDateBuilder().FindTimezoneId(name, tzId)) {
        throw yexception() << "Unknown timezone: " << TStringBuf(name);
    }

    ret.SetTimezoneId(tzId);
    return ret;
}

TO_PYTHON("f", float)
TO_PYTHON("d", double)

namespace {

TPyObjectPtr ToPyData(const TPyCastContext::TPtr& ctx,
        const NUdf::TType* type, const NUdf::TUnboxedValuePod& value)
{
    const NUdf::TDataAndDecimalTypeInspector inspector(*ctx->PyCtx->TypeInfoHelper, type);
    const auto typeId = inspector.GetTypeId();

    switch (typeId) {
    case NUdf::TDataType<i8>::Id: return PyCast<i8>(value.Get<i8>());
    case NUdf::TDataType<ui8>::Id: return PyCast<ui8>(value.Get<ui8>());
    case NUdf::TDataType<i16>::Id: return PyCast<i16>(value.Get<i16>());
    case NUdf::TDataType<ui16>::Id: return PyCast<ui16>(value.Get<ui16>());
    case NUdf::TDataType<i32>::Id: return PyCast<i32>(value.Get<i32>());
    case NUdf::TDataType<ui32>::Id: return PyCast<ui32>(value.Get<ui32>());
    case NUdf::TDataType<i64>::Id: return PyCast<i64>(value.Get<i64>());
    case NUdf::TDataType<ui64>::Id: return PyCast<ui64>(value.Get<ui64>());
    case NUdf::TDataType<bool>::Id: return PyCast<bool>(value.Get<bool>());
    case NUdf::TDataType<float>::Id: return PyCast<float>(value.Get<float>());
    case NUdf::TDataType<double>::Id: return PyCast<double>(value.Get<double>());
    case NUdf::TDataType<NUdf::TDecimal>::Id: return ToPyDecimal(ctx, value, inspector.GetPrecision(), inspector.GetScale());
    case NUdf::TDataType<const char*>::Id: {
        if (ctx->BytesDecodeMode == EBytesDecodeMode::Never) {
            return PyCast<NUdf::TStringRef>(value.AsStringRef());
        } else {
            auto pyObj = ToPyUnicode<NUdf::TStringRef>(value.AsStringRef());
            if (!pyObj) {
                UdfTerminate((TStringBuilder() << ctx->PyCtx->Pos <<
                    "Failed to convert to unicode with _yql_bytes_decode_mode='strict':\n" <<
                    GetLastErrorAsString()).data()
                );
            }
            return pyObj;
        }
    }
    case NUdf::TDataType<NUdf::TYson>::Id: {
        auto pyObj = PyCast<NUdf::TStringRef>(value.AsStringRef());
        if (ctx->YsonConverterIn) {
            TPyObjectPtr pyArgs(PyTuple_New(1));
            PyTuple_SET_ITEM(pyArgs.Get(), 0, pyObj.Release());
            pyObj = PyObject_CallObject(ctx->YsonConverterIn.Get(), pyArgs.Get());
            if (!pyObj) {
                UdfTerminate((TStringBuilder() << ctx->PyCtx->Pos << "Failed to execute:\n" << GetLastErrorAsString()).data());
            }
        }

        return pyObj;
    }
    case NUdf::TDataType<NUdf::TUuid>::Id:
        return PyCast<NUdf::TStringRef>(value.AsStringRef());
    case NUdf::TDataType<NUdf::TJson>::Id:
    case NUdf::TDataType<NUdf::TUtf8>::Id:
        return ToPyUnicode<NUdf::TStringRef>(value.AsStringRef());
    case NUdf::TDataType<NUdf::TDate>::Id: return PyCast<ui16>(value.Get<ui16>());
    case NUdf::TDataType<NUdf::TDatetime>::Id: return PyCast<ui32>(value.Get<ui32>());
    case NUdf::TDataType<NUdf::TTimestamp>::Id: return PyCast<ui64>(value.Get<ui64>());
    case NUdf::TDataType<NUdf::TInterval>::Id: return PyCast<i64>(value.Get<i64>());
    case NUdf::TDataType<NUdf::TTzDate>::Id: {
        TPyObjectPtr pyValue = PyCast<ui16>(value.Get<ui16>());
        auto tzId = value.GetTimezoneId();
        auto tzName = ctx->GetTimezoneName(tzId);
        return PyTuple_Pack(2, pyValue.Get(), tzName.Get());
    }
    case NUdf::TDataType<NUdf::TTzDatetime>::Id: {
        TPyObjectPtr pyValue = PyCast<ui32>(value.Get<ui32>());
        auto tzId = value.GetTimezoneId();
        auto tzName = ctx->GetTimezoneName(tzId);
        return PyTuple_Pack(2, pyValue.Get(), tzName.Get());
    }
    case NUdf::TDataType<NUdf::TTzTimestamp>::Id: {
        TPyObjectPtr pyValue = PyCast<ui64>(value.Get<ui64>());
        auto tzId = value.GetTimezoneId();
        auto tzName = ctx->GetTimezoneName(tzId);
        return PyTuple_Pack(2, pyValue.Get(), tzName.Get());
    }
    }

    throw yexception()
            << "Unsupported type " << typeId;
}

NUdf::TUnboxedValue FromPyData(
        const TPyCastContext::TPtr& ctx,
        const NUdf::TType* type, PyObject* value)
{
    const NUdf::TDataAndDecimalTypeInspector inspector(*ctx->PyCtx->TypeInfoHelper, type);
    const auto typeId = inspector.GetTypeId();

    switch (typeId) {
    case NUdf::TDataType<i8>::Id: return NUdf::TUnboxedValuePod(PyCast<i8>(value));
    case NUdf::TDataType<ui8>::Id: return NUdf::TUnboxedValuePod(PyCast<ui8>(value));
    case NUdf::TDataType<i16>::Id: return NUdf::TUnboxedValuePod(PyCast<i16>(value));
    case NUdf::TDataType<ui16>::Id: return NUdf::TUnboxedValuePod(PyCast<ui16>(value));
    case NUdf::TDataType<i32>::Id: return NUdf::TUnboxedValuePod(PyCast<i32>(value));
    case NUdf::TDataType<ui32>::Id: return NUdf::TUnboxedValuePod(PyCast<ui32>(value));
    case NUdf::TDataType<i64>::Id: return NUdf::TUnboxedValuePod(PyCast<i64>(value));
    case NUdf::TDataType<ui64>::Id: return NUdf::TUnboxedValuePod(PyCast<ui64>(value));
    case NUdf::TDataType<bool>::Id: return NUdf::TUnboxedValuePod(PyCast<bool>(value));
    case NUdf::TDataType<float>::Id: return NUdf::TUnboxedValuePod(PyCast<float>(value));
    case NUdf::TDataType<double>::Id: return NUdf::TUnboxedValuePod(PyCast<double>(value));
    case NUdf::TDataType<NUdf::TDecimal>::Id: return FromPyDecimal(ctx, value, inspector.GetPrecision(), inspector.GetScale());
    case NUdf::TDataType<NUdf::TYson>::Id: {
        if (ctx->YsonConverterOut) {
            TPyObjectPtr input(value, TPyObjectPtr::ADD_REF);
            TPyObjectPtr pyArgs(PyTuple_New(1));
            // PyTuple_SET_ITEM steals reference, so pass ownership to it
            PyTuple_SET_ITEM(pyArgs.Get(), 0, input.Release());
            input.ResetSteal(PyObject_CallObject(ctx->YsonConverterOut.Get(), pyArgs.Get()));
            if (!input) {
                UdfTerminate((TStringBuilder() << ctx->PyCtx->Pos << "Failed to execute:\n" << GetLastErrorAsString()).data());
            }
            return ctx->ValueBuilder->NewString(PyCast<NUdf::TStringRef>(input.Get()));
        }
    }
#if PY_MAJOR_VERSION >= 3
    case NUdf::TDataType<const char*>::Id:
        return ctx->ValueBuilder->NewString(PyCast<NUdf::TStringRef>(value));
    case NUdf::TDataType<NUdf::TUtf8>::Id:
    case NUdf::TDataType<NUdf::TJson>::Id:
        if (PyUnicode_Check(value)) {
            const TPyObjectPtr uif8(PyUnicode_AsUTF8String(value));
            return ctx->ValueBuilder->NewString(PyCast<NUdf::TStringRef>(uif8.Get()));
        }
        throw yexception() << "Python object " << PyObjectRepr(value) << " has invalid value for unicode";
#else
    case NUdf::TDataType<const char*>::Id:
    case NUdf::TDataType<NUdf::TJson>::Id:
    case NUdf::TDataType<NUdf::TUtf8>::Id: {
        if (PyUnicode_Check(value)) {
            const TPyObjectPtr utf8(PyUnicode_AsUTF8String(value));
            return ctx->ValueBuilder->NewString(PyCast<NUdf::TStringRef>(utf8.Get()));
        }

        if ((typeId == NUdf::TDataType<NUdf::TUtf8>::Id || typeId == NUdf::TDataType<NUdf::TJson>::Id) &&
            PyBytes_Check(value) && !NYql::IsUtf8(std::string_view(PyBytes_AS_STRING(value), static_cast<size_t>(PyBytes_GET_SIZE(value))))) {
            throw yexception() << "Python string " << PyObjectRepr(value) << " is invalid for Utf8/Json";
        }

        return ctx->ValueBuilder->NewString(PyCast<NUdf::TStringRef>(value));
    }
#endif
    case NUdf::TDataType<NUdf::TUuid>::Id: {
        const auto& ret = ctx->ValueBuilder->NewString(PyCast<NUdf::TStringRef>(value));
        if (ret.AsStringRef().Size() != 16) {
            throw yexception() << "Python object " << PyObjectRepr(value) \
                << " has invalid value for Uuid";
        }

        return ret;
    }
    case NUdf::TDataType<NUdf::TDate>::Id: {
        auto num = PyCast<ui16>(value);
        if (num >= NUdf::MAX_DATE) {
            throw yexception() << "Python object " << PyObjectRepr(value) \
                << " is out of range for Date";
        }

        return NUdf::TUnboxedValuePod(num);
    }

    case NUdf::TDataType<NUdf::TDatetime>::Id: {
        auto num = PyCast<ui32>(value);
        if (num >= NUdf::MAX_DATETIME) {
            throw yexception() << "Python object " << PyObjectRepr(value) \
                << " is out of range for Datetime";
        }

        return NUdf::TUnboxedValuePod(num);
    }

    case NUdf::TDataType<NUdf::TTimestamp>::Id: {
        auto num = PyCast<ui64>(value);
        if (num >= NUdf::MAX_TIMESTAMP) {
             throw yexception() << "Python object " << PyObjectRepr(value) \
                 << " is out of range for Timestamp";
        }

        return NUdf::TUnboxedValuePod(num);
    }

    case NUdf::TDataType<NUdf::TInterval>::Id: {
        auto num = PyCast<i64>(value);
        if (num <= -(i64)NUdf::MAX_TIMESTAMP || num >= (i64)NUdf::MAX_TIMESTAMP) {
            throw yexception() << "Python object " << PyObjectRepr(value) \
                << " is out of range for Interval";
        }

        return NUdf::TUnboxedValuePod(num);
    }

    case NUdf::TDataType<NUdf::TTzDate>::Id:
        return FromPyTz<ui16>(value, NUdf::MAX_DATE, TStringBuf("TzDate"), ctx);
    case NUdf::TDataType<NUdf::TTzDatetime>::Id:
        return FromPyTz<ui32>(value, NUdf::MAX_DATETIME, TStringBuf("TzDatetime"), ctx);
    case NUdf::TDataType<NUdf::TTzTimestamp>::Id:
        return FromPyTz<ui64>(value, NUdf::MAX_TIMESTAMP, TStringBuf("TzTimestamp"), ctx);
    }

    throw yexception()
            << "Unsupported type " << typeId;
}

TPyObjectPtr ToPyList(
    const TPyCastContext::TPtr& ctx,
    const NUdf::TType* type,
    const NUdf::TUnboxedValuePod& value)
{
    const NUdf::TListTypeInspector inspector(*ctx->PyCtx->TypeInfoHelper, type);
    const auto itemType = inspector.GetItemType();

    if (ctx->LazyInputObjects) {
        return ToPyLazyList(ctx, itemType, value);
    }

    TPyObjectPtr list(PyList_New(0));
    const auto iterator = value.GetListIterator();
    for (NUdf::TUnboxedValue item; iterator.Next(item);) {
        auto pyItem = ToPyObject(ctx, itemType, item);
        if (PyList_Append(list.Get(), pyItem.Get()) < 0) {
            throw yexception() << "Can't append item to list"
               << GetLastErrorAsString();
        }
    }

    return list;
}

NUdf::TUnboxedValue FromPyList(
        const TPyCastContext::TPtr& ctx,
        const NUdf::TType* type, PyObject* value)
{
    const NUdf::TListTypeInspector inspector(*ctx->PyCtx->TypeInfoHelper, type);

    if (PyList_Check(value)) {
        // eager list to list conversion
        auto itemType = inspector.GetItemType();
        Py_ssize_t cnt = PyList_GET_SIZE(value);
        NUdf::TUnboxedValue *items = nullptr;
        const auto list = ctx->ValueBuilder->NewArray(cnt, items);
        for (Py_ssize_t i = 0; i < cnt; ++i) {
            PyObject *item = PyList_GET_ITEM(value, i);
            *items++ = FromPyObject(ctx, itemType, item);
        }
        return list;
    }

    if (PyTuple_Check(value)) {
        // eager tuple to list conversion
        auto itemType = inspector.GetItemType();
        Py_ssize_t cnt = PyTuple_GET_SIZE(value);
        NUdf::TUnboxedValue *items = nullptr;
        const auto list = ctx->ValueBuilder->NewArray(cnt, items);
        for (Py_ssize_t i = 0; i < cnt; ++i) {
            PyObject *item = PyTuple_GET_ITEM(value, i);
            *items++ = FromPyObject(ctx, itemType, item);
        }
        return list;
    }

    if (PyGen_Check(value)) {
        TPyObjectPtr valuePtr(PyObject_GetIter(value));
        return FromPyLazyIterator(ctx, type, std::move(valuePtr));
    }

    if (PyIter_Check(value)
#if PY_MAJOR_VERSION < 3
        // python 2 iterators must also implement "next" method
        && 1 == PyObject_HasAttrString(value, "next")
#endif
    ) {
        TPyObjectPtr valuePtr(value, TPyObjectPtr::ADD_REF);
        return FromPyLazyIterator(ctx, type, std::move(valuePtr));
    }

    // assume that this function will returns generator
    if (PyCallable_Check(value)) {
        TPyObjectPtr valuePtr(value, TPyObjectPtr::ADD_REF);
        return FromPyLazyGenerator(ctx, type, std::move(valuePtr));
    }

    if (PySequence_Check(value) || PyObject_HasAttrString(value, "__iter__")) {
        TPyObjectPtr valuePtr(value, TPyObjectPtr::ADD_REF);
        return FromPyLazyIterable(ctx, type, std::move(valuePtr));
    }

    throw yexception() << "Expected list, tuple, generator, generator factory, "
                          "iterator or iterable object, but got: " << PyObjectRepr(value);
}

TPyObjectPtr ToPyOptional(
        const TPyCastContext::TPtr& ctx,
        const NUdf::TType* type,
        const NUdf::TUnboxedValuePod& value)
{
    if (!value) {
        return TPyObjectPtr(Py_None, TPyObjectPtr::ADD_REF);
    }

    const NUdf::TOptionalTypeInspector inspector(*ctx->PyCtx->TypeInfoHelper, type);
    return ToPyObject(ctx, inspector.GetItemType(), value);
}

NUdf::TUnboxedValue FromPyOptional(
        const TPyCastContext::TPtr& ctx,
        const NUdf::TType* type, PyObject* value)
{
    if (value == Py_None) {
        return NUdf::TUnboxedValue();
    }

    const NUdf::TOptionalTypeInspector inspector(*ctx->PyCtx->TypeInfoHelper, type);
    return FromPyObject(ctx, inspector.GetItemType(), value).Release().MakeOptional();
}

TPyObjectPtr ToPyDict(
    const TPyCastContext::TPtr& ctx,
    const NUdf::TType* type,
    const NUdf::TUnboxedValuePod& value)
{
    const NUdf::TDictTypeInspector inspector(*ctx->PyCtx->TypeInfoHelper, type);
    const auto keyType = inspector.GetKeyType();
    const auto valueType = inspector.GetValueType();

    if (NUdf::ETypeKind::Void == ctx->PyCtx->TypeInfoHelper->GetTypeKind(valueType)) {
       if (ctx->LazyInputObjects) { // TODO
            return ToPyLazySet(ctx, keyType, value);
        }

        const TPyObjectPtr set(PyFrozenSet_New(nullptr));
        const auto iterator = value.GetKeysIterator();
        for (NUdf::TUnboxedValue key; iterator.Next(key);) {
            auto pyKey = ToPyObject(ctx, keyType, key);
            if (PySet_Add(set.Get(), pyKey.Get()) < 0) {
                throw yexception() << "Can't add item to set" << GetLastErrorAsString();
            }
        }

        return set;
    } else {
        if (ctx->LazyInputObjects) {
            return ToPyLazyDict(ctx, keyType, valueType, value);
        }

        const TPyObjectPtr dict(PyDict_New());
        const auto iterator = value.GetDictIterator();
        for (NUdf::TUnboxedValue key, valueObj; iterator.NextPair(key, valueObj);) {
            auto pyKey = ToPyObject(ctx, keyType, key);
            auto pyValue = ToPyObject(ctx, valueType, valueObj);
            if (PyDict_SetItem(dict.Get(), pyKey.Get(), pyValue.Get()) < 0) {
                throw yexception() << "Can't add item to dict" << GetLastErrorAsString();
            }
        }

        return dict;
    }
}

NUdf::TUnboxedValue FromPyDict(
        const TPyCastContext::TPtr& ctx,
        const NUdf::TType* type, PyObject* value)
{
    const NUdf::TDictTypeInspector inspector(*ctx->PyCtx->TypeInfoHelper, type);
    const auto keyType = inspector.GetKeyType();
    const auto valueType = inspector.GetValueType();

    if ((PyList_Check(value) || PyTuple_Check(value) || value->ob_type == &PyThinListType || value->ob_type == &PyLazyListType)
        && ctx->PyCtx->TypeInfoHelper->GetTypeKind(keyType) == NUdf::ETypeKind::Data) {
        const NUdf::TDataTypeInspector keiIns(*ctx->PyCtx->TypeInfoHelper, keyType);
        if (NUdf::GetDataTypeInfo(NUdf::GetDataSlot(keiIns.GetTypeId())).Features & NUdf::EDataTypeFeatures::IntegralType) {
            return FromPySequence(ctx, valueType, keiIns.GetTypeId(), value);
        }
    } else if (NUdf::ETypeKind::Void == ctx->PyCtx->TypeInfoHelper->GetTypeKind(valueType)) {
        if (PyAnySet_Check(value)) {
            return FromPySet(ctx, keyType, value);
        } else if (value->ob_type->tp_as_sequence && value->ob_type->tp_as_sequence->sq_contains) {
            return FromPySequence(ctx, keyType, value);
        }
    } else if (PyDict_Check(value)) {
        return FromPyDict(ctx, keyType, valueType, value);
    } else if (PyMapping_Check(value)) {
        return FromPyMapping(ctx, keyType, valueType, value);
    }

    throw yexception() << "Can't cast "<< PyObjectRepr(value) << " to dict.";
}

} // namespace

TPyObjectPtr ToPyObject(
        const TPyCastContext::TPtr& ctx,
        const NUdf::TType* type, const NUdf::TUnboxedValuePod& value)
{
    switch (ctx->PyCtx->TypeInfoHelper->GetTypeKind(type)) {
        case NUdf::ETypeKind::Data: return ToPyData(ctx, type, value);
        case NUdf::ETypeKind::Tuple: return ToPyTuple(ctx, type, value);
        case NUdf::ETypeKind::Struct: return ToPyStruct(ctx, type, value);
        case NUdf::ETypeKind::List: return ToPyList(ctx, type, value);
        case NUdf::ETypeKind::Optional: return ToPyOptional(ctx, type, value);
        case NUdf::ETypeKind::Dict: return ToPyDict(ctx, type, value);
        case NUdf::ETypeKind::Callable: return ToPyCallable(ctx, type, value);
        case NUdf::ETypeKind::Resource: return ToPyResource(ctx, type, value);
        case NUdf::ETypeKind::Void: return ToPyVoid(ctx, type, value);
        case NUdf::ETypeKind::Stream: return ToPyStream(ctx, type, value);
        case NUdf::ETypeKind::Variant: return ToPyVariant(ctx, type, value);
        default: {
            ::TStringBuilder sb;
            sb << "Failed to export: ";
            NUdf::TTypePrinter(*ctx->PyCtx->TypeInfoHelper, type).Out(sb.Out);
            throw yexception() << sb;
        }
    }
}

NUdf::TUnboxedValue FromPyObject(
        const TPyCastContext::TPtr& ctx,
        const NUdf::TType* type, PyObject* value)
{
    switch (ctx->PyCtx->TypeInfoHelper->GetTypeKind(type)) {
        case NUdf::ETypeKind::Data: return FromPyData(ctx, type, value);
        case NUdf::ETypeKind::Tuple: return FromPyTuple(ctx, type, value);
        case NUdf::ETypeKind::Struct: return FromPyStruct(ctx, type, value);
        case NUdf::ETypeKind::List: return FromPyList(ctx, type, value);
        case NUdf::ETypeKind::Optional: return FromPyOptional(ctx, type, value);
        case NUdf::ETypeKind::Dict: return FromPyDict(ctx, type, value);
        case NUdf::ETypeKind::Callable: return FromPyCallable(ctx, type, value);
        case NUdf::ETypeKind::Resource: return FromPyResource(ctx, type, value);
        case NUdf::ETypeKind::Void: return FromPyVoid(ctx, type, value);
        case NUdf::ETypeKind::Stream: return FromPyStream(ctx, type, TPyObjectPtr(value, TPyObjectPtr::ADD_REF), nullptr, nullptr, nullptr);
        case NUdf::ETypeKind::Variant: return FromPyVariant(ctx, type, value);
        default: {
            ::TStringBuilder sb;
            sb << "Failed to import: ";
            NUdf::TTypePrinter(*ctx->PyCtx->TypeInfoHelper, type).Out(sb.Out);
            throw yexception() << sb;
        }
    }
}

TPyObjectPtr ToPyArgs(
        const TPyCastContext::TPtr& ctx,
        const NUdf::TType* type,
        const NUdf::TUnboxedValuePod* args,
        const NUdf::TCallableTypeInspector& inspector)
{
    const auto argsCount = inspector.GetArgsCount();
    TPyObjectPtr tuple(PyTuple_New(argsCount));

    for (ui32 i = 0; i < argsCount; i++) {
        auto arg = ToPyObject(ctx, inspector.GetArgType(i), args[i]);
        PyTuple_SET_ITEM(tuple.Get(), i, arg.Release());
    }

    return tuple;
}

void FromPyArgs(
        const TPyCastContext::TPtr& ctx,
        const NUdf::TType* type,
        PyObject* pyArgs,
        NUdf::TUnboxedValue* cArgs,
        const NUdf::TCallableTypeInspector& inspector)
{
    PY_ENSURE_TYPE(Tuple, pyArgs, "Expected");

    const auto argsCount = inspector.GetArgsCount();
    const auto optArgsCount = inspector.GetOptionalArgsCount();

    ui32 pyArgsCount = static_cast<ui32>(PyTuple_GET_SIZE(pyArgs));
    PY_ENSURE(argsCount - optArgsCount <= pyArgsCount && pyArgsCount <= argsCount,
           "arguments count missmatch: "
           "min " << (argsCount - optArgsCount) << ", max " << argsCount
           << ", got " << pyArgsCount);

    for (ui32 i = 0; i < pyArgsCount; i++) {
        PyObject* item = PyTuple_GET_ITEM(pyArgs, i);
        cArgs[i] = FromPyObject(ctx, inspector.GetArgType(i), item);
    }

    for (ui32 i = pyArgsCount; i < argsCount; i++) {
        cArgs[i] = NUdf::TUnboxedValuePod();
    }
}

class TDummyMemoryLock : public IMemoryLock {
public:
    void Acquire() override {}
    void Release() override {}
};

TPyCastContext::TPyCastContext(
    const NKikimr::NUdf::IValueBuilder* builder,
    TPyContext::TPtr pyCtx,
    THolder<IMemoryLock> memoryLock)
    : ValueBuilder(builder)
    , PyCtx(std::move(pyCtx))
    , MemoryLock(std::move(memoryLock))
{
    if (!MemoryLock) {
        MemoryLock = MakeHolder<TDummyMemoryLock>();
    }
}

TPyCastContext::~TPyCastContext() {
    TPyGilLocker locker;
    StructTypes.clear();
    YsonConverterIn.Reset();
    YsonConverterOut.Reset();
    TimezoneNames.clear();
}

const TPyObjectPtr& TPyCastContext::GetTimezoneName(ui32 id) {
    auto& x = TimezoneNames[id];
    if (!x) {
        NKikimr::NUdf::TStringRef ref;
        if (!ValueBuilder->GetDateBuilder().FindTimezoneName(id, ref)) {
            throw yexception() << "Unknown timezone id: " << id;
        }

        x = PyRepr(ref);
    }

    return x;
}

} // namspace NPython
