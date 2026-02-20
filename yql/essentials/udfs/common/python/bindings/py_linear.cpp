#include "py_linear.h"
#include "py_cast.h"
#include "py_errors.h"
#include "py_utils.h"
#include "py_gil.h"

#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/public/udf/udf_value_builder.h>
#include <yql/essentials/public/udf/udf_type_inspection.h>

#include <util/string/builder.h>

using namespace NKikimr;

namespace NPython {

//////////////////////////////////////////////////////////////////////////////
// TPyDynamicLinear interface
//////////////////////////////////////////////////////////////////////////////
struct TPyDynamicLinear {
    using TPtr = NUdf::TRefCountedPtr<TPyDynamicLinear, TPyPtrOps<TPyDynamicLinear>>;

    PyObject_HEAD;
    TPyCastContext::TPtr CastCtx;
    const NUdf::TType* ItemType;
    TPyCleanupListItem<NUdf::IBoxedValuePtr> Value;

    inline static TPyDynamicLinear* Cast(PyObject* o) {
        return reinterpret_cast<TPyDynamicLinear*>(o);
    }

    inline static void Dealloc(PyObject* self) {
        delete Cast(self);
    }

    static PyObject* New(
        const TPyCastContext::TPtr& castCtx,
        const NUdf::TType* itemType,
        NUdf::IBoxedValuePtr value);

    static PyObject* Repr(PyObject* self);
    static PyObject* Extract(PyObject* self, PyObject* /* arg */);
};

namespace {

// NOLINTNEXTLINE(modernize-avoid-c-arrays)
PyMethodDef TPyDynamicLinearMethods[] = {
    {"extract", TPyDynamicLinear::Extract, METH_NOARGS, nullptr},
    {nullptr, nullptr, 0, nullptr} /* sentinel */
};

} // namespace

#if PY_MAJOR_VERSION >= 3
    #define Py_TPFLAGS_HAVE_ITER 0 // NOLINT(readability-identifier-naming)
#endif

PyTypeObject PyDynamicLinearType = {
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    // clang-format off
    INIT_MEMBER(tp_name, "yql.TDynamicLinear"),
    // clang-format on
    INIT_MEMBER(tp_basicsize, sizeof(TPyDynamicLinear)),
    INIT_MEMBER(tp_itemsize, 0),
    INIT_MEMBER(tp_dealloc, TPyDynamicLinear::Dealloc),
#if PY_VERSION_HEX < 0x030800b4
    INIT_MEMBER(tp_print, nullptr),
#else
    INIT_MEMBER(tp_vectorcall_offset, 0),
#endif
    INIT_MEMBER(tp_getattr, nullptr),
    INIT_MEMBER(tp_setattr, nullptr),
#if PY_MAJOR_VERSION >= 3
    INIT_MEMBER(tp_as_async, nullptr),
#else
    INIT_MEMBER(tp_compare, nullptr),
#endif
    INIT_MEMBER(tp_repr, TPyDynamicLinear::Repr),
    INIT_MEMBER(tp_as_number, nullptr),
    INIT_MEMBER(tp_as_sequence, nullptr),
    INIT_MEMBER(tp_as_mapping, nullptr),
    INIT_MEMBER(tp_hash, nullptr),
    INIT_MEMBER(tp_call, nullptr),
    INIT_MEMBER(tp_str, nullptr),
    INIT_MEMBER(tp_getattro, nullptr),
    INIT_MEMBER(tp_setattro, nullptr),
    INIT_MEMBER(tp_as_buffer, nullptr),
    INIT_MEMBER(tp_flags, 0),
    INIT_MEMBER(tp_doc, "yql.TDynamicLinear object"),
    INIT_MEMBER(tp_traverse, nullptr),
    INIT_MEMBER(tp_clear, nullptr),
    INIT_MEMBER(tp_richcompare, nullptr),
    INIT_MEMBER(tp_weaklistoffset, 0),
    INIT_MEMBER(tp_iter, nullptr),
    INIT_MEMBER(tp_iternext, nullptr),
    INIT_MEMBER(tp_methods, TPyDynamicLinearMethods),
    INIT_MEMBER(tp_members, nullptr),
    INIT_MEMBER(tp_getset, nullptr),
    INIT_MEMBER(tp_base, nullptr),
    INIT_MEMBER(tp_dict, nullptr),
    INIT_MEMBER(tp_descr_get, nullptr),
    INIT_MEMBER(tp_descr_set, nullptr),
    INIT_MEMBER(tp_dictoffset, 0),
    INIT_MEMBER(tp_init, nullptr),
    INIT_MEMBER(tp_alloc, nullptr),
    INIT_MEMBER(tp_new, nullptr),
    INIT_MEMBER(tp_free, nullptr),
    INIT_MEMBER(tp_is_gc, nullptr),
    INIT_MEMBER(tp_bases, nullptr),
    INIT_MEMBER(tp_mro, nullptr),
    INIT_MEMBER(tp_cache, nullptr),
    INIT_MEMBER(tp_subclasses, nullptr),
    INIT_MEMBER(tp_weaklist, nullptr),
    INIT_MEMBER(tp_del, nullptr),
    INIT_MEMBER(tp_version_tag, 0),
#if PY_MAJOR_VERSION >= 3
    INIT_MEMBER(tp_finalize, nullptr),
#endif
#if PY_VERSION_HEX >= 0x030800b1
    INIT_MEMBER(tp_vectorcall, nullptr),
#endif
#if PY_VERSION_HEX >= 0x030800b4 && PY_VERSION_HEX < 0x03090000
    INIT_MEMBER(tp_print, nullptr),
#endif
};

PyObject* TPyDynamicLinear::New(
    const TPyCastContext::TPtr& castCtx,
    const NUdf::TType* itemType,
    NUdf::IBoxedValuePtr value)
{
    TPyDynamicLinear* linear = new TPyDynamicLinear;
    PyObject_INIT(linear, &PyDynamicLinearType);

    linear->CastCtx = castCtx;
    linear->ItemType = itemType;
    linear->Value.Set(castCtx->PyCtx, value);

    return reinterpret_cast<PyObject*>(linear);
}

PyObject* TPyDynamicLinear::Repr(PyObject*)
{
    return PyRepr("<yql.TDynamicLinear>").Release();
}

PyObject* TPyDynamicLinear::Extract(PyObject* self, PyObject* /* arg */)
{
    PY_TRY {
        TPyDynamicLinear* linear = Cast(self);
        NUdf::TUnboxedValue res;
        if (NUdf::TBoxedValueAccessor::Next(*linear->Value.Get(), res)) {
            return ToPyObject(linear->CastCtx, linear->ItemType, res).Release();
        }

        PyErr_SetString(PyExc_ValueError, "The linear value has already been used");
        return nullptr;
    }
    PY_CATCH(nullptr)
}

class TDynamicLinearProxy: public NUdf::TBoxedValue {
public:
    TDynamicLinearProxy(const TPyCastContext::TPtr& castCtx, const NUdf::TType* itemType, TPyObjectPtr&& pyObject)
        : CastCtx_(castCtx)
        , ItemType_(itemType)
        , PyObject_(std::move(pyObject))
    {
    }

    ~TDynamicLinearProxy() override {
        const TPyGilLocker lock;
        PyObject_.Reset();
    }

    bool Next(NUdf::TUnboxedValue& value) override try {
        const TPyGilLocker lock;
        if (Consumed_) {
            return false;
        }

        TPyObjectPtr function(PyObject_GetAttrString(PyObject_.Get(), "extract"));
        if (!function) {
            throw yexception() << "Missing 'extract' attribute";
        }

        if (!PyCallable_Check(function.Get())) {
            throw yexception() << "'extract' attribute should be a callable";
        }

        TPyObjectPtr resultObj = PyObject_CallObject(function.Get(), nullptr);
        if (!resultObj) {
            throw yexception() << "Failed to execute:\n"
                               << GetLastErrorAsString();
        }

        Consumed_ = true;
        value = FromPyObject(CastCtx_, ItemType_, resultObj.Get());
        return true;
    } catch (const yexception& e) {
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << e.what()).c_str());
    }

private:
    const TPyCastContext::TPtr CastCtx_;
    const NUdf::TType* ItemType_;
    TPyObjectPtr PyObject_;
    bool Consumed_ = false;
};

//////////////////////////////////////////////////////////////////////////////
// public functions
//////////////////////////////////////////////////////////////////////////////
TPyObjectPtr ToPyDynamicLinear(
    const TPyCastContext::TPtr& castCtx,
    const NUdf::TType* itemType,
    const NUdf::TUnboxedValuePod& value)
{
    return TPyDynamicLinear::New(castCtx, itemType, value.AsBoxed());
}

NUdf::TUnboxedValue FromPyDynamicLinear(
    const TPyCastContext::TPtr& castCtx,
    const NUdf::TType* itemType,
    TPyObjectPtr value)
{
    return NUdf::TUnboxedValuePod(new TDynamicLinearProxy(castCtx, itemType, std::move(value)));
}

} // namespace NPython
