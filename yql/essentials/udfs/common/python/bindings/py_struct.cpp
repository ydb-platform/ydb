#include "py_struct.h"
#include "py_cast.h"
#include "py_errors.h"
#include "py_gil.h"
#include "py_utils.h"

#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/public/udf/udf_value_builder.h>
#include <yql/essentials/public/udf/udf_type_inspection.h>
#include <yql/essentials/public/udf/udf_terminator.h>

#include <util/string/cast.h>
#include <util/string/join.h>
#include <util/string/builder.h>

using namespace NKikimr;

namespace NPython {

namespace {

TPyObjectPtr CreateNewStrucInstance(const TPyCastContext::TPtr& ctx, const NKikimr::NUdf::TType* type, const NUdf::TStructTypeInspector& inspector)
{
    const auto it = ctx->StructTypes.emplace(type, TPyObjectPtr());
    if (it.second) {
#if PY_MAJOR_VERSION >= 3
        std::vector<PyStructSequence_Field> fields(inspector.GetMembersCount() + 1U);
        for (ui32 i = 0U; i < inspector.GetMembersCount(); ++i) {
            fields[i] = {const_cast<char*>(inspector.GetMemberName(i).Data()), nullptr};
        }
        fields.back() = {nullptr, nullptr};

        PyStructSequence_Desc desc = {
            INIT_MEMBER(name, "yql.Struct"),
            INIT_MEMBER(doc, nullptr),
            INIT_MEMBER(fields, fields.data()),
            INIT_MEMBER(n_in_sequence, int(inspector.GetMembersCount()))
        };

        const auto typeObject = new PyTypeObject();
        if (0 > PyStructSequence_InitType2(typeObject, &desc)) {
            throw yexception() << "can't create struct type: " << GetLastErrorAsString();
        }

        it.first->second.ResetSteal(reinterpret_cast<PyObject*>(typeObject));
    }

    const TPyObjectPtr object = PyStructSequence_New(it.first->second.GetAs<PyTypeObject>());
#else
        const auto className = TString("yql.Struct_") += ToString(ctx->StructTypes.size());
        PyObject* metaclass = (PyObject *) &PyClass_Type;
        const TPyObjectPtr name = PyRepr(TStringBuf(className));
        const TPyObjectPtr bases = PyTuple_New(0);
        const TPyObjectPtr dict = PyDict_New();

        TPyObjectPtr newClass = PyObject_CallFunctionObjArgs(
                metaclass, name.Get(), bases.Get(), dict.Get(),
                nullptr);
        if (!newClass) {
            throw yexception() << "can't create new type: " << GetLastErrorAsString();
        }

        it.first->second = std::move(newClass);
    }

    Y_UNUSED(inspector);
    const TPyObjectPtr object = PyInstance_New(it.first->second.Get(), nullptr, nullptr);
#endif
    if (!object) {
        throw yexception() << "can't struct instance: " << GetLastErrorAsString();
    }
    return object;
}

TPyObjectPtr ConvertStringToPyObject(TStringBuf str)
{
#if PY_MAJOR_VERSION >= 3
    return PyUnicode_FromStringAndSize(str.data(), str.size());
#else
    return PyString_FromStringAndSize(str.data(), str.size());
#endif
}

// Sized counterpart of PyDict_GetItemString from C API. Returns borrowed reference.
PyObject* GetItemFromPyDictUsingString(PyObject* v, TStringBuf key)
{
    TPyObjectPtr kv = ConvertStringToPyObject(key);
    if (!kv) {
        PyErr_Clear();
        return nullptr;
    }
    return PyDict_GetItem(v, kv.Get());
}

PyObject* GetItemFromPyDictUsingBytes(PyObject* v, TStringBuf key)
{
    TPyObjectPtr bytesMemberName = PyBytes_FromStringAndSize(key.data(), key.size());
    return PyDict_GetItem(v, bytesMemberName.Get());
}

// Helper function for double-probing UTF-8 str() and bytes() in the case of Python 3.
PyObject* GetItemFromPyDict(PyObject* v, TStringBuf key)
{
    PyObject* ret = GetItemFromPyDictUsingString(v, key);
#if PY_MAJOR_VERSION >= 3
    if (!ret) {
        ret = GetItemFromPyDictUsingBytes(v, key);
    }
#endif
    return ret;
}

// Sized counterpart of PyObject_GetAttrString from C API. Returns strong reference.
TPyObjectPtr GetAttrFromPyObject(PyObject* v, TStringBuf name)
{
    TPyObjectPtr w = ConvertStringToPyObject(name);
    if (!w) {
        return nullptr;
    }
    return PyObject_GetAttr(v, w.Get());
}

}

TPyObjectPtr ToPyStruct(const TPyCastContext::TPtr& ctx, const NUdf::TType* type, const NUdf::TUnboxedValuePod& value)
{
    const NUdf::TStructTypeInspector inspector(*ctx->PyCtx->TypeInfoHelper, type);
    const TPyObjectPtr object = CreateNewStrucInstance(ctx, type, inspector);
    const auto membersCount = inspector.GetMembersCount();

    if (auto ptr = value.GetElements()) {
        for (Py_ssize_t i = 0; i < membersCount; ++i) {
#if PY_MAJOR_VERSION >= 3
            auto item = ToPyObject(ctx, inspector.GetMemberType(i), *ptr++);
            PyStructSequence_SetItem(object.Get(), i, item.Release());
#else
            const TStringBuf name = inspector.GetMemberName(i);
            const auto item = ToPyObject(ctx, inspector.GetMemberType(i), *ptr++);
            if (0 > PyObject_SetAttrString(object.Get(), name.data(), item.Get())) {
                throw yexception()
                        << "Can't set attr '" << name << "' to python object: "
                        << GetLastErrorAsString();
            }
#endif
        }
    } else {
        for (Py_ssize_t i = 0; i < membersCount; ++i) {
#if PY_MAJOR_VERSION >= 3
            auto item = ToPyObject(ctx, inspector.GetMemberType(i), value.GetElement(i));
            PyStructSequence_SetItem(object.Get(), i, item.Release());
#else
            const TStringBuf name = inspector.GetMemberName(i);
            const auto item = ToPyObject(ctx, inspector.GetMemberType(i), value.GetElement(i));
            if (0 > PyObject_SetAttrString(object.Get(), name.data(), item.Get())) {
                throw yexception()
                        << "Can't set attr '" << name << "' to python object: "
                        << GetLastErrorAsString();
            }
#endif
        }
    }

    return object;
}

NUdf::TUnboxedValue FromPyStruct(const TPyCastContext::TPtr& ctx, const NUdf::TType* type, PyObject* value)
{
    NUdf::TUnboxedValue* items = nullptr;
    const NUdf::TStructTypeInspector inspector(*ctx->PyCtx->TypeInfoHelper, type);
    const auto membersCount = inspector.GetMembersCount();
    auto mkqlStruct = ctx->ValueBuilder->NewArray(membersCount, items);

    TVector<TString> errors;
    if (PyDict_Check(value)) {
        for (ui32 i = 0; i < membersCount; i++) {
            TStringBuf memberName = inspector.GetMemberName(i);
            auto memberType = inspector.GetMemberType(i);
            // borrowed reference - no need to manage ownership
            PyObject* item = GetItemFromPyDict(value, memberName);
            if (!item) {
                if (ctx->PyCtx->TypeInfoHelper->GetTypeKind(memberType) == NUdf::ETypeKind::Optional) {
                    items[i] = NUdf::TUnboxedValue();
                    continue;
                }

                errors.push_back(TStringBuilder() << "Dict has no item '" << memberName << "'");
                continue;
            }

            try {
                items[i] = FromPyObject(ctx, memberType, item);
            } catch (const yexception& e) {
                errors.push_back(TStringBuilder() << "Failed to convert dict item '" << memberName << "' - " << e.what());
            }
        }

        if (!errors.empty()) {
            throw yexception() << "Failed to convert dict to struct\n" << JoinSeq("\n", errors) << "\nDict repr: " << PyObjectRepr(value);
        }
    } else {
        for (ui32 i = 0; i < membersCount; i++) {
            TStringBuf memberName = inspector.GetMemberName(i);
            auto memberType = inspector.GetMemberType(i);
            TPyObjectPtr attr = GetAttrFromPyObject(value, memberName);
            if (!attr) {
                if (ctx->PyCtx->TypeInfoHelper->GetTypeKind(memberType) == NUdf::ETypeKind::Optional &&
                    PyErr_ExceptionMatches(PyExc_AttributeError)) {
                    PyErr_Clear();
                    items[i] = NUdf::TUnboxedValue();
                    continue;
                }

                errors.push_back(TStringBuilder() << "Object has no attr '" << memberName << "' , error: " << GetLastErrorAsString());
                continue;
            }

            try {
                items[i] = FromPyObject(ctx, memberType, attr.Get());
            } catch (const yexception& e) {
                errors.push_back(TStringBuilder() << "Failed to convert object attr '" << memberName << "' - " << e.what());
            }
        }

        if (!errors.empty()) {
            throw yexception() << "Failed to convert object to struct\n" << JoinSeq("\n", errors) << "\nObject repr: " << PyObjectRepr(value);
        }
    }

    return mkqlStruct;
}

}
