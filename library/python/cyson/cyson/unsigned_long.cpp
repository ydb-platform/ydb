#include "unsigned_long.h"

#include "helpers.h"

#include <util/generic/va_args.h>
#include <util/system/compiler.h>

#if (PY_MAJOR_VERSION == 2)
#include <longintrepr.h>
#endif

#if (PY_MAJOR_VERSION >= 3) && defined(Py_LIMITED_API)
#error "limited API for Python3 not supported yet"
#endif

#if PY_VERSION_HEX >= 0x030800b4 && PY_VERSION_HEX < 0x03090000
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif

#ifndef Py_TPFLAGS_CHECKTYPES
#define Py_TPFLAGS_CHECKTYPES 0
#endif

#if PY_VERSION_HEX < 0x030c0000
#define GetSize(obj) Py_SIZE(obj)
#define IsNegative(obj) GetSize(obj) < 0
#define IsPositive(obj) GetSize(obj) >= 0
#else
#define GetSize(obj) (((PyLongObject*)obj)->long_value.lv_tag >> _PyLong_NON_SIZE_BITS)
#define IsNegative(obj) ((((PyLongObject*)obj)->long_value.lv_tag & _PyLong_SIGN_MASK) & 2)
#define IsPositive(obj) !IsNegative(obj)
#endif

namespace NCYson {
    static NPrivate::TPyObjectPtr PyUnsignedLong_ReprPtr;

    static void SetNegativeValueError(PyObject* obj, PyTypeObject* type) {
        assert(PyLong_Check(obj));
        assert(IsNegative(obj));

        PyObject* repr = ConvertPyLongToPyBytes(obj);

        PyErr_Format(
            PyExc_OverflowError,
            "cannot convert negative value (%s) to %s",
            Y_LIKELY(repr) ? PyBytes_AS_STRING(repr) : "???",
            type->tp_name);

        Py_XDECREF(repr);
    }

    PyObject* PreparePyUIntType(PyObject* reprfunc) {
        if (Y_UNLIKELY(PyType_Ready(&PyUnsignedLong_Type) < 0)) {
            return nullptr;
        }

        PyUnsignedLong_ReprPtr.Reset(reprfunc);

        Py_INCREF((PyObject*)&PyUnsignedLong_Type);

        return (PyObject*)&PyUnsignedLong_Type;
    }

    PyObject* ConstructPyUIntFromPyLong(PyLongObject* obj) {
        PyObject *result;
        Py_ssize_t index;
        Py_ssize_t size;

        assert(PyLong_Check(obj));

        if (IsNegative(obj)) {
            SetNegativeValueError((PyObject*)obj, &PyUnsignedLong_Type);
            return nullptr;
        }

        size = GetSize(obj);
        result = PyLong_Type.tp_alloc(&PyUnsignedLong_Type, size);
        if (Y_UNLIKELY(!result)) {
            return nullptr;
        }

        assert(IsExactPyUInt(result));

        Py_SET_SIZE(result, size);

#if PY_VERSION_HEX >= 0x030c0000
        ((PyLongObject*)result)->long_value.lv_tag = obj->long_value.lv_tag;
#endif
        for (index = 0; index < size; ++index) {
#if PY_VERSION_HEX < 0x030c0000
            ((PyLongObject*)result)->ob_digit[index] = obj->ob_digit[index];
#else
            ((PyLongObject*)result)->long_value.ob_digit[index] = obj->long_value.ob_digit[index];
#endif
        }

        return result;
    }

    PyObject* ConstructPyUIntFromUint(uint64_t n) {
        PyObject* result;
        PyObject* tmp;

        tmp = PyLong_FromUnsignedLong(n);
        if (Y_UNLIKELY(!tmp)) {
            return nullptr;
        }

        result = ConstructPyUIntFromPyLong((PyLongObject*)tmp);

        Py_DECREF(tmp);

        return result;
    }

    static PyObject* unsigned_long_new(PyTypeObject *type, PyObject *args, PyObject* kws) {
        PyObject* result;

        result = PyLong_Type.tp_new(type, args, kws);
        if (Y_UNLIKELY(!result)) {
            return nullptr;
        }

        assert(IsExactPyUInt(result));

        if (IsNegative(result)) {
            SetNegativeValueError(result, type);
            Py_DECREF(result);
            return nullptr;
        }

        return result;
    }

    static PyObject* unsigned_long_repr(PyObject* self) {
        PyObject* result;

        PyObject* callable = PyUnsignedLong_ReprPtr.GetBorrowed();

        if (callable) {
            result = PyObject_CallFunctionObjArgs(callable, self, nullptr);
        } else {
            result = PyObject_Repr(self);
        }

        return result;
    }

#define PYOBJECT_ARG(o) PyObject* o,
#define PYOBJECT_ARG_LAST(o) PyObject* o

#define UNSIGNED_LONG_OPERATION(SLOT, ...)                                                                       \
    static PyObject* unsigned_long_##SLOT(Y_MAP_ARGS_WITH_LAST(PYOBJECT_ARG, PYOBJECT_ARG_LAST, __VA_ARGS__)) {  \
        PyObject* result = PyLong_Type.tp_as_number->nb_##SLOT(__VA_ARGS__);                                     \
        if (result && PyLong_CheckExact(result) && (IsPositive(result))) {                                       \
            PyObject* tmp = result;                                                                              \
            result = ConstructPyUIntFromPyLong((PyLongObject*)tmp);                                              \
            Py_DECREF(tmp);                                                                                      \
        }                                                                                                        \
        return result;                                                                                           \
    }

    UNSIGNED_LONG_OPERATION(add, x, y);
    UNSIGNED_LONG_OPERATION(subtract, x, y);
    UNSIGNED_LONG_OPERATION(multiply, x, y);
#if PY_MAJOR_VERSION < 3
    UNSIGNED_LONG_OPERATION(divide, x, y);
#endif
    UNSIGNED_LONG_OPERATION(remainder, x, y);
    UNSIGNED_LONG_OPERATION(power, x, y, z);
    UNSIGNED_LONG_OPERATION(lshift, x, y);
    UNSIGNED_LONG_OPERATION(rshift, x, y);
    UNSIGNED_LONG_OPERATION(and, x, y);
    UNSIGNED_LONG_OPERATION(xor, x, y);
    UNSIGNED_LONG_OPERATION(or, x, y);
    UNSIGNED_LONG_OPERATION(floor_divide, x, y);
    UNSIGNED_LONG_OPERATION(true_divide, x, y);

#undef UNSIGNED_LONG_OPERATION
#undef PYOBJECT_ARG_LAST
#undef PYOBJECT_ARG

    static PyNumberMethods unsigned_long_as_number = {
        unsigned_long_add, /*nb_add*/
        unsigned_long_subtract, /*nb_subtract*/
        unsigned_long_multiply, /*nb_multiply*/
#if PY_MAJOR_VERSION < 3
        unsigned_long_divide, /*nb_divide*/
#endif
        unsigned_long_remainder, /*nb_remainder*/
        0, /*nb_divmod*/
        unsigned_long_power, /*nb_power*/
        0, /*nb_negative*/
        GetSelf, /*nb_positive*/
        GetSelf, /*nb_absolute*/
        0, /*nb_nonzero*/
        0, /*nb_invert*/
        unsigned_long_lshift, /*nb_lshift*/
        unsigned_long_rshift, /*nb_rshift*/
        unsigned_long_and, /*nb_and*/
        unsigned_long_xor, /*nb_xor*/
        unsigned_long_or, /*nb_or*/
#if PY_MAJOR_VERSION < 3
        0, /*nb_coerce*/
#endif
        0, /*nb_int*/
#if PY_MAJOR_VERSION < 3
        0, /*nb_long*/
#else
        0, /*reserved*/
#endif
        0, /*nb_float*/
#if PY_MAJOR_VERSION < 3
        0, /*nb_oct*/
        0, /*nb_hex*/
#endif
        0, /*nb_inplace_add*/
        0, /*nb_inplace_subtract*/
        0, /*nb_inplace_multiply*/
#if PY_MAJOR_VERSION < 3
        0, /*nb_inplace_divide*/
#endif
        0, /*nb_inplace_remainder*/
        0, /*nb_inplace_power*/
        0, /*nb_inplace_lshift*/
        0, /*nb_inplace_rshift*/
        0, /*nb_inplace_and*/
        0, /*nb_inplace_xor*/
        0, /*nb_inplace_or*/
        unsigned_long_floor_divide, /*nb_floor_divide*/
        unsigned_long_true_divide, /*nb_true_divide*/
        0, /*nb_inplace_floor_divide*/
        0, /*nb_inplace_true_divide*/
        0, /*nb_index*/
#if PY_VERSION_HEX >= 0x03050000
        0, /*nb_matrix_multiply*/
        0, /*nb_inplace_matrix_multiply*/
#endif
    };

    PyTypeObject PyUnsignedLong_Type = {
        PyVarObject_HEAD_INIT(0, 0)
        "cyson._cyson.UInt", /*tp_name*/
        PyLong_Type.tp_basicsize, /*tp_basicsize*/
        PyLong_Type.tp_itemsize, /*tp_itemsize*/
        PyLong_Type.tp_dealloc, /*tp_dealloc*/
#if PY_VERSION_HEX < 0x030800b4
        0, /*tp_print*/
#endif
#if PY_VERSION_HEX >= 0x030800b4
        0, /*tp_vectorcall_offset*/
#endif
        0, /*tp_getattr*/
        0, /*tp_setattr*/
#if PY_MAJOR_VERSION < 3
        0, /*tp_compare*/
#endif
#if PY_MAJOR_VERSION >= 3
        0, /*tp_as_async*/
#endif
        unsigned_long_repr, /*tp_repr*/
        &unsigned_long_as_number, /*tp_as_number*/
        0, /*tp_as_sequence*/
        0, /*tp_as_mapping*/
        0, /*tp_hash*/
        0, /*tp_call*/
        0, /*tp_str*/
        0, /*tp_getattro*/
        0, /*tp_setattro*/
        0, /*tp_as_buffer*/
        Py_TPFLAGS_DEFAULT|Py_TPFLAGS_CHECKTYPES|Py_TPFLAGS_LONG_SUBCLASS, /*tp_flags*/
        "UInt(0) -> UInt\nUInt(x, base=10) -> UInt", /*tp_doc*/
        0, /*tp_traverse*/
        0, /*tp_clear*/
        0, /*tp_richcompare*/
        0, /*tp_weaklistoffset*/
        0, /*tp_iter*/
        0, /*tp_iternext*/
        0, /*tp_methods*/
        0, /*tp_members*/
        0, /*tp_getset*/
        &PyLong_Type, /*tp_base*/
        0, /*tp_dict*/
        0, /*tp_descr_get*/
        0, /*tp_descr_set*/
        0, /*tp_dictoffset*/
        0, /*tp_init*/
        0, /*tp_alloc*/
        unsigned_long_new, /*tp_new*/
        0, /*tp_free*/
        0, /*tp_is_gc*/
        0, /*tp_bases*/
        0, /*tp_mro*/
        0, /*tp_cache*/
        0, /*tp_subclasses*/
        0, /*tp_weaklist*/
        0, /*tp_del*/
        0, /*tp_version_tag*/
#if PY_VERSION_HEX >= 0x030400a1
        0, /*tp_finalize*/
#endif
#if PY_VERSION_HEX >= 0x030800b1
        0, /*tp_vectorcall*/
#endif
#if PY_VERSION_HEX >= 0x030800b4 && PY_VERSION_HEX < 0x03090000
        0, /*tp_print*/
#endif
#if PY_VERSION_HEX >= 0x030c0000
        0, /*tp_watched*/
#endif
#if PY_VERSION_HEX >= 0x030D00A4
        0, /*tp_versions_used*/
#endif
    };
}
