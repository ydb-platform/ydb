#include "py_tuple.h"
#include "py_cast.h"
#include "py_errors.h"
#include "py_gil.h"
#include "py_utils.h"

#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>
#include <ydb/library/yql/public/udf/udf_type_inspection.h>
#include <ydb/library/yql/public/udf/udf_terminator.h>

using namespace NKikimr;

namespace NPython {

TPyObjectPtr ToPyTuple(const TPyCastContext::TPtr& ctx, const NUdf::TType* type, const NUdf::TUnboxedValuePod& value)
{
    const NUdf::TTupleTypeInspector inspector(*ctx->PyCtx->TypeInfoHelper, type);
    const auto elementsCount = inspector.GetElementsCount();

    const TPyObjectPtr tuple(PyTuple_New(elementsCount));

    if (auto ptr = value.GetElements()) {
        for (ui32 i = 0U; i < elementsCount; ++i) {
            auto item = ToPyObject(ctx, inspector.GetElementType(i), *ptr++);
            PyTuple_SET_ITEM(tuple.Get(), i, item.Release());
        }
    } else {
        for (ui32 i = 0U; i < elementsCount; ++i) {
            auto item = ToPyObject(ctx, inspector.GetElementType(i), value.GetElement(i));
            PyTuple_SET_ITEM(tuple.Get(), i, item.Release());
        }
    }

    return tuple;
}

NUdf::TUnboxedValue FromPyTuple(const TPyCastContext::TPtr& ctx, const NUdf::TType* type, PyObject* value)
{
    const NUdf::TTupleTypeInspector inspector(*ctx->PyCtx->TypeInfoHelper, type);
    if (const TPyObjectPtr fast = PySequence_Fast(value, "Expected tuple or list.")) {
        const Py_ssize_t itemsCount = PySequence_Fast_GET_SIZE(fast.Get());

        if (itemsCount < 0 || inspector.GetElementsCount() != itemsCount) {
            throw yexception() << "Tuple elements count mismatch.";
        }

        NUdf::TUnboxedValue* tuple_items = nullptr;
        const auto tuple = ctx->ValueBuilder->NewArray(inspector.GetElementsCount(), tuple_items);
        for (Py_ssize_t i = 0; i < itemsCount; i++) {
            const auto item = PySequence_Fast_GET_ITEM(fast.Get(), i);
            *tuple_items++ = FromPyObject(ctx, inspector.GetElementType(i), item);
        }

        return tuple;
    }

    throw yexception() << "Expected Tuple or Sequence but got: " << PyObjectRepr(value);
}

}
