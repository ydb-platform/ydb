#include "py_resource.h"
#include "py_cast.h"
#include "py_errors.h"
#include "py_gil.h"
#include "py_utils.h"

#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_type_inspection.h>

using namespace NKikimr;

namespace NPython {
namespace {

void DestroyResourceCapsule(PyObject* obj) {
    if (auto* ptr = PyCapsule_GetPointer(obj, ResourceCapsuleName)) {
        delete reinterpret_cast<NUdf::TUnboxedValue*>(ptr);
    }
}

/////////////////////////////////////////////////////////////////////////////
// TResource
/////////////////////////////////////////////////////////////////////////////
class TResource final: public NUdf::TBoxedValue
{
public:
    TResource(PyObject* value, const NUdf::TStringRef& tag)
        : Value_(value, TPyObjectPtr::ADD_REF), Tag_(tag)
    {
    }

    ~TResource() {
        TPyGilLocker lock;
        Value_.Reset();
    }

private:
    NUdf::TStringRef GetResourceTag() const override {
        return Tag_;
    }

    void* GetResource() final {
        return Value_.Get();
    }

    TPyObjectPtr Value_;
    const NUdf::TStringRef Tag_;
};

} // namespace

const char ResourceCapsuleName[] = "YqlResourceCapsule";

TPyObjectPtr ToPyResource(
        const TPyCastContext::TPtr& ctx,
        const NUdf::TType* type,
        const NUdf::TUnboxedValuePod& value)
{
// TODO NILE-43
#if false && UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 15)
    NUdf::TResourceTypeInspector inpector(*ctx->PyCtx->TypeInfoHelper, type);
    auto tag = inpector.GetTag();
    if (tag == ctx->PyCtx->ResourceTag) {
        PyObject* p = reinterpret_cast<PyObject*>(value.GetResource());
        return TPyObjectPtr(p, TPyObjectPtr::ADD_REF);
    }
#else
    Y_UNUSED(type);
    if (value.GetResourceTag() == ctx->PyCtx->ResourceTag) {
        PyObject* p = reinterpret_cast<PyObject*>(value.GetResource());
        return TPyObjectPtr(p, TPyObjectPtr::ADD_REF);
    }
#endif
    auto resource = MakeHolder<NUdf::TUnboxedValue>(value);

    return PyCapsule_New(resource.Release(), ResourceCapsuleName, &DestroyResourceCapsule);
}

NUdf::TUnboxedValue FromPyResource(
        const TPyCastContext::TPtr& ctx,
        const NUdf::TType* type, PyObject* value)
{
// TODO NILE-43
#if false && UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 15)
    NUdf::TResourceTypeInspector inpector(*ctx->PyCtx->TypeInfoHelper, type);
    auto tag = inpector.GetTag();
    if (tag == ctx->PyCtx->ResourceTag) {
        return NUdf::TUnboxedValuePod(new TResource(value, ctx->PyCtx->ResourceTag));
    }

    if (PyCapsule_IsValid(value, ResourceCapsuleName)) {
        auto* resource = reinterpret_cast<NUdf::TUnboxedValue*>(PyCapsule_GetPointer(value, ResourceCapsuleName));
        auto valueTag = resource->GetResourceTag();
        if (valueTag != tag) {
            throw yexception() << "Mismatch of resource tag, expected: "
                << tag << ", got: " << valueTag;
        }

        return *resource;
    }

    throw yexception() << "Python object " << PyObjectRepr(value) \
        << " is not a valid resource with tag " << tag;
#else
    Y_UNUSED(type);
    if (PyCapsule_CheckExact(value)) {
        if (!PyCapsule_IsValid(value, ResourceCapsuleName)) {
            throw yexception() << "Python object " << PyObjectRepr(value) << " is not a valid resource capsule";
        }
        return *reinterpret_cast<NUdf::TUnboxedValue*>(PyCapsule_GetPointer(value, ResourceCapsuleName));
    }
    return NUdf::TUnboxedValuePod(new TResource(value, ctx->PyCtx->ResourceTag));
#endif
}

} // namspace NPython
