#include "py_variant.h"
#include "py_cast.h"
#include "py_errors.h"
#include "py_utils.h"

#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>
#include <ydb/library/yql/public/udf/udf_type_inspection.h>


using namespace NKikimr;

namespace NPython {

//////////////////////////////////////////////////////////////////////////////
// public functions
//////////////////////////////////////////////////////////////////////////////
TPyObjectPtr ToPyVariant(
        const TPyCastContext::TPtr& castCtx,
        const NUdf::TType* type,
        const NUdf::TUnboxedValuePod& value)
{
    auto& th = *castCtx->PyCtx->TypeInfoHelper;
    NUdf::TVariantTypeInspector varInsp(th, type);
    const NUdf::TType* subType = varInsp.GetUnderlyingType();
    ui32 index = value.GetVariantIndex();
    auto item = value.GetVariantItem();

    const NUdf::TType* itemType = nullptr;
    if (auto tupleInsp = NUdf::TTupleTypeInspector(th, subType)) {
        itemType = tupleInsp.GetElementType(index);
        TPyObjectPtr pyIndex = PyCast<ui32>(index);
        TPyObjectPtr pyItem = ToPyObject(castCtx, itemType, item);
        return PyTuple_Pack(2, pyIndex.Get(), pyItem.Get());
    } else if (auto structInsp = NUdf::TStructTypeInspector(th, subType)) {
        itemType = structInsp.GetMemberType(index);
        TPyObjectPtr pyName = ToPyUnicode<NUdf::TStringRef>(
                    structInsp.GetMemberName(index));
        TPyObjectPtr pyItem = ToPyObject(castCtx, itemType, item);
        return PyTuple_Pack(2, pyName.Get(), pyItem.Get());
    }

    throw yexception() << "Cannot get Variant item type";
}

NUdf::TUnboxedValue FromPyVariant(
        const TPyCastContext::TPtr& castCtx,
        const NUdf::TType* type,
        PyObject* value)
{
    PY_ENSURE(PyTuple_Check(value),
              "Expected to get Tuple, but got " << Py_TYPE(value)->tp_name);

    Py_ssize_t tupleSize = PyTuple_GET_SIZE(value);
    PY_ENSURE(tupleSize == 2,
              "Expected to get Tuple with 2 elements, but got "
              << tupleSize << " elements");

    auto& th = *castCtx->PyCtx->TypeInfoHelper;
    NUdf::TVariantTypeInspector varInsp(th, type);
    const NUdf::TType* subType = varInsp.GetUnderlyingType();

    PyObject* el0 = PyTuple_GET_ITEM(value, 0);
    PyObject* el1 = PyTuple_GET_ITEM(value, 1);

    ui32 index;
    NUdf::TStringRef name;
    if (TryPyCast(el0, index)) {
        if (auto tupleInsp = NUdf::TTupleTypeInspector(th, subType)) {
            PY_ENSURE(index < tupleInsp.GetElementsCount(),
                      "Index must be < " << tupleInsp.GetElementsCount()
                      << ", but got " << index);
            auto* itemType = tupleInsp.GetElementType(index);
            return castCtx->ValueBuilder->NewVariant(index, FromPyObject(castCtx, itemType, el1));
        } else {
            throw yexception() << "Cannot convert " << PyObjectRepr(value)
                    << " underlying Variant type is not a Tuple";
        }
    } else if (TryPyCast(el0, name)) {
        if (auto structInsp = NUdf::TStructTypeInspector(th, subType)) {
            ui32 index = structInsp.GetMemberIndex(name);
            PY_ENSURE(index < structInsp.GetMembersCount(),
                      "Unknown member name: " << TStringBuf(name));
            auto* itemType = structInsp.GetMemberType(index);
            return castCtx->ValueBuilder->NewVariant(index, FromPyObject(castCtx, itemType, el1));
        } else {
            throw yexception() << "Cannot convert " << PyObjectRepr(value)
                    << " underlying Variant type is not a Struct";
        }
    } else {
        throw yexception()
                << "Expected first Tuple element to either be an int "
                   "or a str, but got " << Py_TYPE(el0)->tp_name;
    }
}

} // namspace NPython
