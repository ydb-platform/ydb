#include "py_void.h"
#include "py_errors.h"
#include "py_utils.h"

#include <ydb/library/yql/public/udf/udf_value.h>

using namespace NKikimr;

namespace NPython {
namespace {

static PyObject* VoidRepr(PyObject*) {
    return PyRepr("yql.Void").Release();
}

static void VoidDealloc(PyObject*) {
    Py_FatalError("Deallocating yql.Void");
}

} // namespace

PyTypeObject PyVoidType = {
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    INIT_MEMBER(tp_name           , "yql.Void"),
    INIT_MEMBER(tp_basicsize      , 0),
    INIT_MEMBER(tp_itemsize       , 0),
    INIT_MEMBER(tp_dealloc        , VoidDealloc),
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
    INIT_MEMBER(tp_repr           , VoidRepr),
    INIT_MEMBER(tp_as_number      , nullptr),
    INIT_MEMBER(tp_as_sequence    , nullptr),
    INIT_MEMBER(tp_as_mapping     , nullptr),
    INIT_MEMBER(tp_hash           , nullptr),
    INIT_MEMBER(tp_call           , nullptr),
    INIT_MEMBER(tp_str            , nullptr),
    INIT_MEMBER(tp_getattro       , nullptr),
    INIT_MEMBER(tp_setattro       , nullptr),
    INIT_MEMBER(tp_as_buffer      , nullptr),
    INIT_MEMBER(tp_flags          , 0),
    INIT_MEMBER(tp_doc            , "yql.Void object"),
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

PyObject PyVoidObject = {
    _PyObject_EXTRA_INIT
    1, &PyVoidType
};

TPyObjectPtr ToPyVoid(
        const TPyCastContext::TPtr& ctx,
        const NUdf::TType* type,
        const NUdf::TUnboxedValuePod& value)
{
    Y_UNUSED(ctx);
    Y_UNUSED(type);
    Y_UNUSED(value);
    return TPyObjectPtr(&PyVoidObject, TPyObjectPtr::ADD_REF);
}

NUdf::TUnboxedValue FromPyVoid(
        const TPyCastContext::TPtr& ctx,
        const NUdf::TType* type,
        PyObject* value)
{
    Y_UNUSED(ctx);
    Y_UNUSED(type);
    Y_UNUSED(value);
    PY_ENSURE(value == &PyVoidObject, "Expected object of yql.Void type");
    return NUdf::TUnboxedValue::Void();
}

} // namspace NPython
