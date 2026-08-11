// Always enable assertions
#undef NDEBUG

#include "pythoncapi_compat.h"

#ifdef NDEBUG
#  error "assertions must be enabled"
#endif

#ifdef Py_LIMITED_API
#  error "Py_LIMITED_API is not supported"
#endif

#if PY_VERSION_HEX >= 0x03000000
#  define PYTHON3 1
#endif

#if defined(_MSC_VER) && defined(__cplusplus)
#  define MODULE_NAME test_pythoncapi_compat_cppext
#elif defined(__cplusplus) && __cplusplus >= 201103
#  define MODULE_NAME test_pythoncapi_compat_cpp11ext
#elif defined(__cplusplus)
#  define MODULE_NAME test_pythoncapi_compat_cpp03ext
#else
#  define MODULE_NAME test_pythoncapi_compat_cext
#endif

#define _STR(NAME) #NAME
#define STR(NAME) _STR(NAME)
#define _CONCAT(a, b) a ## b
#define CONCAT(a, b) _CONCAT(a, b)

#define MODULE_NAME_STR STR(MODULE_NAME)

// Ignore reference count checks on PyPy
#ifndef PYPY_VERSION
#  define CHECK_REFCNT
#endif

// CPython 3.12 beta 1 implements immortal objects (PEP 683)
#if 0x030C00B1 <= PY_VERSION_HEX && !defined(PYPY_VERSION)
   // Don't check reference count of Python 3.12 immortal objects (ex: bool
   // and str types)
#  define IMMORTAL_OBJS
#endif

#ifdef CHECK_REFCNT
#  define ASSERT_REFCNT(expr) assert(expr)
#else
#  define ASSERT_REFCNT(expr)
#endif

// Marker to check that pointer value was set
static const char uninitialized[] = "uninitialized";
#define UNINITIALIZED_OBJ ((PyObject *)uninitialized)
#define UNINITIALIZED_INT 0x83ff979


static PyObject*
create_string(const char *str)
{
    PyObject *obj;
#ifdef PYTHON3
    obj = PyUnicode_FromString(str);
#else
    obj = PyString_FromString(str);
#endif
    assert(obj != _Py_NULL);
    return obj;
}


static PyObject *
test_object(PyObject *Py_UNUSED(module), PyObject* Py_UNUSED(ignored))
{
    PyObject *obj = PyList_New(0);
    if (obj == _Py_NULL) {
        return _Py_NULL;
    }
    Py_ssize_t refcnt = Py_REFCNT(obj);

    // Py_NewRef()
    PyObject *ref = Py_NewRef(obj);
    assert(ref == obj);
    assert(Py_REFCNT(obj) == (refcnt + 1));
    Py_DECREF(ref);

    // Py_XNewRef()
    PyObject *xref = Py_XNewRef(obj);
    assert(xref == obj);
    assert(Py_REFCNT(obj) == (refcnt + 1));
    Py_DECREF(xref);

    assert(Py_XNewRef(_Py_NULL) == _Py_NULL);

    // Py_SETREF()
    PyObject *setref = Py_NewRef(obj);
    PyObject *none = Py_None;
    assert(Py_REFCNT(obj) == (refcnt + 1));

    Py_SETREF(setref, none);
    assert(setref == none);
    assert(Py_REFCNT(obj) == refcnt);
    Py_INCREF(setref);

    Py_SETREF(setref, _Py_NULL);
    assert(setref == _Py_NULL);

    // Py_XSETREF()
    PyObject *xsetref = _Py_NULL;

    Py_INCREF(obj);
    assert(Py_REFCNT(obj) == (refcnt + 1));
    Py_XSETREF(xsetref, obj);
    assert(xsetref == obj);

    Py_XSETREF(xsetref, _Py_NULL);
    assert(Py_REFCNT(obj) == refcnt);
    assert(xsetref == _Py_NULL);

    // Py_SET_REFCNT
    Py_SET_REFCNT(obj, Py_REFCNT(obj));
    // Py_SET_TYPE
    Py_SET_TYPE(obj, Py_TYPE(obj));
    // Py_SET_SIZE
    Py_SET_SIZE(obj, Py_SIZE(obj));
    // Py_IS_TYPE()
    int is_type = Py_IS_TYPE(obj, Py_TYPE(obj));
    assert(is_type);

    assert(Py_REFCNT(obj) == refcnt);
    Py_DECREF(obj);
    Py_RETURN_NONE;
}


static PyObject *
test_py_is(PyObject *Py_UNUSED(module), PyObject* Py_UNUSED(ignored))
{
    PyObject *o_none = Py_None;
    PyObject *o_true = Py_True;
    PyObject *o_false = Py_False;
    PyObject *obj = PyList_New(0);
    if (obj == _Py_NULL) {
        return _Py_NULL;
    }

    /* test Py_Is() */
    assert(Py_Is(obj, obj));
    assert(!Py_Is(obj, o_none));

    /* test Py_IsNone() */
    assert(Py_IsNone(o_none));
    assert(!Py_IsNone(obj));

    /* test Py_IsTrue() */
    assert(Py_IsTrue(o_true));
    assert(!Py_IsTrue(o_false));
    assert(!Py_IsTrue(obj));

    /* testPy_IsFalse() */
    assert(Py_IsFalse(o_false));
    assert(!Py_IsFalse(o_true));
    assert(!Py_IsFalse(obj));

    Py_DECREF(obj);
    Py_RETURN_NONE;
}


#ifndef PYPY_VERSION
static void
test_frame_getvar(PyFrameObject *frame)
{
    // Make the assumption that test_frame_getvar() is only called by
    // _run_tests() of test_pythoncapi_compat.py and so that the "name"
    // variable exists.

    // test PyFrame_GetVar() and PyFrame_GetVarString()
    PyObject *attr = PyUnicode_FromString("name");
    assert(attr != _Py_NULL);
    PyObject *name1 = PyFrame_GetVar(frame, attr);
    Py_DECREF(attr);
    assert(name1 != _Py_NULL);
    Py_DECREF(name1);

    PyObject *name2 = PyFrame_GetVarString(frame, "name");
    assert(name2 != _Py_NULL);
    Py_DECREF(name2);

    // test PyFrame_GetVar() and PyFrame_GetVarString() NameError
    PyObject *attr3 = PyUnicode_FromString("dontexist");
    assert(attr3 != _Py_NULL);
    PyObject *name3 = PyFrame_GetVar(frame, attr3);
    Py_DECREF(attr3);
    assert(name3 == _Py_NULL);
    assert(PyErr_ExceptionMatches(PyExc_NameError));
    PyErr_Clear();

    PyObject *name4 = PyFrame_GetVarString(frame, "dontexist");
    assert(name4 == _Py_NULL);
    assert(PyErr_ExceptionMatches(PyExc_NameError));
    PyErr_Clear();
}


static PyObject *
test_frame(PyObject *Py_UNUSED(module), PyObject* Py_UNUSED(ignored))
{
    PyThreadState *tstate = PyThreadState_Get();

    // test PyThreadState_GetFrame()
    PyFrameObject *frame = PyThreadState_GetFrame(tstate);
    if (frame == _Py_NULL) {
        PyErr_SetString(PyExc_AssertionError, "PyThreadState_GetFrame failed");
        return _Py_NULL;
    }

    // test _PyThreadState_GetFrameBorrow()
    Py_ssize_t frame_refcnt = Py_REFCNT(frame);
    PyFrameObject *frame2 = _PyThreadState_GetFrameBorrow(tstate);
    assert(frame2 == frame);
    assert(Py_REFCNT(frame) == frame_refcnt);

    // test PyFrame_GetCode()
    PyCodeObject *code = PyFrame_GetCode(frame);
    assert(code != _Py_NULL);
    assert(PyCode_Check(code));

    // test _PyFrame_GetCodeBorrow()
    Py_ssize_t code_refcnt = Py_REFCNT(code);
    PyCodeObject *code2 = _PyFrame_GetCodeBorrow(frame);
    assert(code2 == code);
    assert(Py_REFCNT(code) == code_refcnt);
    Py_DECREF(code);

    // PyFrame_GetBack()
    PyFrameObject* back = PyFrame_GetBack(frame);
    if (back != _Py_NULL) {
        assert(PyFrame_Check(back));
    }

    // test _PyFrame_GetBackBorrow()
    if (back != _Py_NULL) {
        Py_ssize_t back_refcnt = Py_REFCNT(back);
        PyFrameObject *back2 = _PyFrame_GetBackBorrow(frame);
        assert(back2 == back);
        assert(Py_REFCNT(back) == back_refcnt);
    }
    else {
        PyFrameObject *back2 = _PyFrame_GetBackBorrow(frame);
        assert(back2 == back);
    }
    Py_XDECREF(back);

    // test PyFrame_GetLocals()
    PyObject *locals = PyFrame_GetLocals(frame);
    assert(locals != _Py_NULL);
    // Python 3.13 creates a local proxy
#if PY_VERSION_HEX < 0x030D0000
    assert(PyDict_Check(locals));
#endif

    // test PyFrame_GetGlobals()
    PyObject *globals = PyFrame_GetGlobals(frame);
    assert(globals != _Py_NULL);
    assert(PyDict_Check(globals));

    // test PyFrame_GetBuiltins()
    PyObject *builtins = PyFrame_GetBuiltins(frame);
    assert(builtins != _Py_NULL);
    assert(PyDict_Check(builtins));

    assert(locals != globals);
    assert(globals != builtins);
    assert(builtins != locals);

    Py_DECREF(locals);
    Py_DECREF(globals);
    Py_DECREF(builtins);

    // test PyFrame_GetLasti()
    int lasti = PyFrame_GetLasti(frame);
    assert(lasti >= 0);

    // test PyFrame_GetVar() and PyFrame_GetVarString()
    test_frame_getvar(frame);

    // done
    Py_DECREF(frame);
    Py_RETURN_NONE;
}
#endif


static PyObject *
test_thread_state(PyObject *Py_UNUSED(module), PyObject* Py_UNUSED(ignored))
{
    PyThreadState *tstate = PyThreadState_Get();

    // test PyThreadState_GetInterpreter()
    PyInterpreterState *interp = PyThreadState_GetInterpreter(tstate);
    assert(interp != _Py_NULL);

#ifndef PYPY_VERSION
    // test PyThreadState_GetFrame()
    PyFrameObject *frame = PyThreadState_GetFrame(tstate);
    if (frame != _Py_NULL) {
        assert(PyFrame_Check(frame));
    }
    Py_XDECREF(frame);
#endif

#if 0x030700A1 <= PY_VERSION_HEX && !defined(PYPY_VERSION)
    uint64_t id = PyThreadState_GetID(tstate);
    assert(id > 0);
#endif

#ifndef PYPY_VERSION
    // PyThreadState_EnterTracing(), PyThreadState_LeaveTracing()
    PyThreadState_EnterTracing(tstate);
    PyThreadState_LeaveTracing(tstate);
#endif

#if PY_VERSION_HEX >= 0x03050200
    // PyThreadState_GetUnchecked()
    assert(PyThreadState_GetUnchecked() == tstate);
#endif

    Py_RETURN_NONE;
}


static PyObject *
test_interpreter(PyObject *Py_UNUSED(module), PyObject* Py_UNUSED(ignored))
{
    // test PyInterpreterState_Get()
    PyInterpreterState *interp = PyInterpreterState_Get();
    assert(interp != _Py_NULL);
    PyThreadState *tstate = PyThreadState_Get();
    PyInterpreterState *interp2 = PyThreadState_GetInterpreter(tstate);
    assert(interp == interp2);

#if 0x030300A1 <= PY_VERSION_HEX && (!defined(PYPY_VERSION_NUM) || PYPY_VERSION_NUM >= 0x7030000)
    // test Py_IsFinalizing()
    assert(Py_IsFinalizing() == 0);
#endif

    Py_RETURN_NONE;
}


static PyObject *
test_calls(PyObject *Py_UNUSED(module), PyObject* Py_UNUSED(ignored))
{
    PyObject *func = _Py_CAST(PyObject*, &PyUnicode_Type);

    // test PyObject_CallNoArgs(): str() returns ''
    PyObject *res = PyObject_CallNoArgs(func);
    if (res == _Py_NULL) {
        return _Py_NULL;
    }
    assert(PyUnicode_Check(res));
    Py_DECREF(res);

    // test PyObject_CallOneArg(): str(1) returns '1'
    PyObject *arg = PyLong_FromLong(1);
    if (arg == _Py_NULL) {
        return _Py_NULL;
    }
    res = PyObject_CallOneArg(func, arg);
    Py_DECREF(arg);
    if (res == _Py_NULL) {
        return _Py_NULL;
    }
    assert(PyUnicode_Check(res));
    Py_DECREF(res);

    Py_RETURN_NONE;
}


static PyObject *
test_gc(PyObject *Py_UNUSED(module), PyObject* Py_UNUSED(ignored))
{
    PyObject *tuple = PyTuple_New(1);
    Py_INCREF(Py_None);
    PyTuple_SET_ITEM(tuple, 0, Py_None);

#if !defined(PYPY_VERSION)
    // test PyObject_GC_IsTracked()
    int tracked = PyObject_GC_IsTracked(tuple);
    assert(tracked);
#endif

#if PY_VERSION_HEX >= 0x030400F0 && !defined(PYPY_VERSION)
    // test PyObject_GC_IsFinalized()
    int finalized = PyObject_GC_IsFinalized(tuple);
    assert(!finalized);
#endif

    Py_DECREF(tuple);
    Py_RETURN_NONE;
}


static int
check_module_attr(PyObject *module, const char *name, PyObject *expected)
{
    PyObject *attr = PyObject_GetAttrString(module, name);
    if (attr == _Py_NULL) {
        return -1;
    }
    assert(attr == expected);
    Py_DECREF(attr);

    if (PyObject_DelAttrString(module, name) < 0) {
        return -1;
    }
    return 0;
}


// test PyModule_AddType()
static int
test_module_add_type(PyObject *module)
{
    PyTypeObject *type = &PyUnicode_Type;
#ifdef PYTHON3
    const char *type_name = "str";
#else
    const char *type_name = "unicode";
#endif
#ifdef CHECK_REFCNT
    Py_ssize_t refcnt = Py_REFCNT(type);
#endif

    if (PyModule_AddType(module, type) < 0) {
        return -1;
    }
#ifndef IMMORTAL_OBJS
    ASSERT_REFCNT(Py_REFCNT(type) == refcnt + 1);
#endif

    if (check_module_attr(module, type_name, _Py_CAST(PyObject*, type)) < 0) {
        return -1;
    }
    ASSERT_REFCNT(Py_REFCNT(type) == refcnt);
    return 0;
}


// test PyModule_AddObjectRef()
static int
test_module_addobjectref(PyObject *module)
{
    const char *name = "test_module_addobjectref";
    PyObject *obj = PyUnicode_FromString(name);
    assert(obj != _Py_NULL);
#ifdef CHECK_REFCNT
    Py_ssize_t refcnt = Py_REFCNT(obj);
#endif

    if (PyModule_AddObjectRef(module, name, obj) < 0) {
        ASSERT_REFCNT(Py_REFCNT(obj) == refcnt);
        Py_DECREF(obj);
        return -1;
    }
    ASSERT_REFCNT(Py_REFCNT(obj) == refcnt + 1);

    if (check_module_attr(module, name, obj) < 0) {
        Py_DECREF(obj);
        return -1;
    }
    ASSERT_REFCNT(Py_REFCNT(obj) == refcnt);

    // PyModule_AddObjectRef() with value=NULL must not crash
    assert(!PyErr_Occurred());
    int res = PyModule_AddObjectRef(module, name, _Py_NULL);
    assert(res < 0);
    assert(PyErr_ExceptionMatches(PyExc_SystemError));
    PyErr_Clear();

    Py_DECREF(obj);
    return 0;
}


// test PyModule_Add()
static int
test_module_add(PyObject *module)
{
    const char *name = "test_module_add";
    PyObject *obj = PyUnicode_FromString(name);
    assert(obj != _Py_NULL);
#ifdef CHECK_REFCNT
    Py_ssize_t refcnt = Py_REFCNT(obj);
#endif

    if (PyModule_Add(module, name, Py_NewRef(obj)) < 0) {
        ASSERT_REFCNT(Py_REFCNT(obj) == refcnt);
        Py_DECREF(obj);
        return -1;
    }
    ASSERT_REFCNT(Py_REFCNT(obj) == refcnt + 1);

    if (check_module_attr(module, name, obj) < 0) {
        Py_DECREF(obj);
        return -1;
    }
    ASSERT_REFCNT(Py_REFCNT(obj) == refcnt);

    // PyModule_Add() with value=NULL must not crash
    assert(!PyErr_Occurred());
    int res = PyModule_Add(module, name, _Py_NULL);
    assert(res < 0);
    assert(PyErr_ExceptionMatches(PyExc_SystemError));
    PyErr_Clear();

    Py_DECREF(obj);
    return 0;
}


static PyObject *
test_module(PyObject *Py_UNUSED(module), PyObject* Py_UNUSED(ignored))
{
    PyObject *module = PyImport_ImportModule("sys");
    if (module == _Py_NULL) {
        return _Py_NULL;
    }
    assert(PyModule_Check(module));

    if (test_module_add_type(module) < 0) {
        goto error;
    }
    if (test_module_addobjectref(module) < 0) {
        goto error;
    }
    if (test_module_add(module) < 0) {
        goto error;
    }

    Py_DECREF(module);
    Py_RETURN_NONE;

error:
    Py_DECREF(module);
    return _Py_NULL;
}


#if (PY_VERSION_HEX <= 0x030B00A1 || 0x030B00A7 <= PY_VERSION_HEX) && !defined(PYPY_VERSION)
static PyObject *
test_float_pack(PyObject *Py_UNUSED(module), PyObject* Py_UNUSED(ignored))
{
    const int big_endian = 0;
    const int little_endian = 1;
    char data[8];
    const double d = 1.5;

#if PY_VERSION_HEX >= 0x030600B1
#  define HAVE_FLOAT_PACK2
#endif

    // Test Pack (big endian)
#ifdef HAVE_FLOAT_PACK2
    assert(PyFloat_Pack2(d, data, big_endian) == 0);
    assert(memcmp(data, ">\x00", 2) == 0);
#endif

    assert(PyFloat_Pack4(d, data, big_endian) == 0);
    assert(memcmp(data, "?\xc0\x00\x00", 2) == 0);

    assert(PyFloat_Pack8(d, data, big_endian) == 0);
    assert(memcmp(data, "?\xf8\x00\x00\x00\x00\x00\x00", 2) == 0);

    // Test Pack (little endian)
#ifdef HAVE_FLOAT_PACK2
    assert(PyFloat_Pack2(d, data, little_endian) == 0);
    assert(memcmp(data, "\x00>", 2) == 0);
#endif

    assert(PyFloat_Pack4(d, data, little_endian) == 0);
    assert(memcmp(data, "\x00\x00\xc0?", 2) == 0);

    assert(PyFloat_Pack8(d, data, little_endian) == 0);
    assert(memcmp(data, "\x00\x00\x00\x00\x00\x00\xf8?", 2) == 0);

    // Test Unpack (big endian)
#ifdef HAVE_FLOAT_PACK2
    assert(PyFloat_Unpack2(">\x00", big_endian) == d);
#endif
    assert(PyFloat_Unpack4("?\xc0\x00\x00", big_endian) == d);
    assert(PyFloat_Unpack8("?\xf8\x00\x00\x00\x00\x00\x00", big_endian) == d);

    // Test Unpack (little endian)
#ifdef HAVE_FLOAT_PACK2
    assert(PyFloat_Unpack2("\x00>", little_endian) == d);
#endif
    assert(PyFloat_Unpack4("\x00\x00\xc0?", little_endian) == d);
    assert(PyFloat_Unpack8("\x00\x00\x00\x00\x00\x00\xf8?", little_endian) == d);

    Py_RETURN_NONE;
}
#endif


#if !defined(PYPY_VERSION)
static PyObject *
test_code(PyObject *Py_UNUSED(module), PyObject* Py_UNUSED(ignored))
{
    PyThreadState *tstate = PyThreadState_Get();
    PyFrameObject *frame = PyThreadState_GetFrame(tstate);
    if (frame == _Py_NULL) {
        PyErr_SetString(PyExc_AssertionError, "PyThreadState_GetFrame failed");
        return _Py_NULL;
    }
    PyCodeObject *code = PyFrame_GetCode(frame);

    // PyCode_GetCode()
    {
        PyObject *co_code = PyCode_GetCode(code);
        assert(co_code != _Py_NULL);
        assert(PyBytes_Check(co_code));
        Py_DECREF(co_code);
    }

    // PyCode_GetVarnames
    {
        PyObject *co_varnames = PyCode_GetVarnames(code);
        assert(co_varnames != _Py_NULL);
        assert(PyTuple_CheckExact(co_varnames));
        assert(PyTuple_GET_SIZE(co_varnames) != 0);
        Py_DECREF(co_varnames);
    }

    // PyCode_GetCellvars
    {
        PyObject *co_cellvars = PyCode_GetCellvars(code);
        assert(co_cellvars != _Py_NULL);
        assert(PyTuple_CheckExact(co_cellvars));
        assert(PyTuple_GET_SIZE(co_cellvars) == 0);
        Py_DECREF(co_cellvars);
    }

    // PyCode_GetFreevars
    {
        PyObject *co_freevars = PyCode_GetFreevars(code);
        assert(co_freevars != _Py_NULL);
        assert(PyTuple_CheckExact(co_freevars));
        assert(PyTuple_GET_SIZE(co_freevars) == 0);
        Py_DECREF(co_freevars);
    }

    Py_DECREF(code);
    Py_DECREF(frame);
    Py_RETURN_NONE;
}
#endif


#ifdef __cplusplus
// Class to test operator casting an object to PyObject*
class StrongRef
{
public:
    StrongRef(PyObject *obj) : m_obj(obj) {
        Py_INCREF(this->m_obj);
    }

    ~StrongRef() {
        Py_DECREF(this->m_obj);
    }

    // Cast to PyObject*: get a borrowed reference
    inline operator PyObject*() const { return this->m_obj; }

private:
    PyObject *m_obj;  // Strong reference
};
#endif


static PyObject *
test_api_casts(PyObject *Py_UNUSED(module), PyObject *Py_UNUSED(args))
{
    PyObject *obj = Py_BuildValue("(ii)", 1, 2);
    if (obj == _Py_NULL) {
        return _Py_NULL;
    }
    Py_ssize_t refcnt = Py_REFCNT(obj);
    assert(refcnt >= 1);

    // gh-92138: For backward compatibility, functions of Python C API accepts
    // "const PyObject*". Check that using it does not emit C++ compiler
    // warnings.
    const PyObject *const_obj = obj;
    Py_INCREF(const_obj);
    Py_DECREF(const_obj);
    PyTypeObject *type = Py_TYPE(const_obj);
    assert(Py_REFCNT(const_obj) == refcnt);
    assert(type == &PyTuple_Type);
    assert(PyTuple_GET_SIZE(const_obj) == 2);
    PyObject *one = PyTuple_GET_ITEM(const_obj, 0);
    assert(PyLong_AsLong(one) == 1);

#ifdef __cplusplus
    // gh-92898: StrongRef doesn't inherit from PyObject but has an operator to
    // cast to PyObject*.
    StrongRef strong_ref(obj);
    assert(Py_TYPE(strong_ref) == &PyTuple_Type);
    assert(Py_REFCNT(strong_ref) == (refcnt + 1));
    Py_INCREF(strong_ref);
    Py_DECREF(strong_ref);
#endif

    // gh-93442: Pass 0 as NULL for PyObject*
    Py_XINCREF(0);
    Py_XDECREF(0);
#if _cplusplus >= 201103
    // Test nullptr passed as PyObject*
    Py_XINCREF(nullptr);
    Py_XDECREF(nullptr);
#endif

    Py_DECREF(obj);
    Py_RETURN_NONE;
}


static PyObject *
test_import(PyObject *Py_UNUSED(module), PyObject *Py_UNUSED(args))
{
    PyObject *mod = PyImport_ImportModule("sys");
    if (mod == _Py_NULL) {
        return _Py_NULL;
    }
    Py_ssize_t refcnt = Py_REFCNT(mod);

    // test PyImport_AddModuleRef()
    PyObject *mod2 = PyImport_AddModuleRef("sys");
    if (mod2 == _Py_NULL) {
        return _Py_NULL;
    }
    assert(PyModule_Check(mod2));
    assert(Py_REFCNT(mod) == (refcnt + 1));

    Py_DECREF(mod2);
    Py_DECREF(mod);

    Py_RETURN_NONE;
}


static void
gc_collect(void)
{
#if defined(PYPY_VERSION)
    PyObject *mod = PyImport_ImportModule("gc");
    assert(mod != _Py_NULL);

    PyObject *res = PyObject_CallMethod(mod, "collect", _Py_NULL);
    Py_DECREF(mod);
    assert(res != _Py_NULL);
    Py_DECREF(res);
#else
    PyGC_Collect();
#endif
}


static PyObject *
func_varargs(PyObject *Py_UNUSED(module), PyObject *args, PyObject *kwargs)
{
    if (kwargs != _Py_NULL) {
        return PyTuple_Pack(2, args, kwargs);
    }
    else {
        return PyTuple_Pack(1, args);
    }
}


static void
check_int(PyObject *obj, int value)
{
#ifdef PYTHON3
    assert(PyLong_Check(obj));
    assert(PyLong_AsLong(obj) == value);
#else
    assert(PyInt_Check(obj));
    assert(PyInt_AsLong(obj) == value);
#endif
}


static PyObject *
test_weakref(PyObject *Py_UNUSED(module), PyObject *Py_UNUSED(args))
{
    // Create a new heap type, create an instance of this type, and delete the
    // type. This object supports weak references.
    PyObject *new_type = PyObject_CallFunction((PyObject*)&PyType_Type,
                                               "s(){}", "TypeName");
    if (new_type == _Py_NULL) {
        return _Py_NULL;
    }
    PyObject *obj = PyObject_CallNoArgs(new_type);
    Py_DECREF(new_type);
    if (obj == _Py_NULL) {
        return _Py_NULL;
    }
    Py_ssize_t refcnt = Py_REFCNT(obj);

    // create a weak reference
    PyObject *weakref = PyWeakref_NewRef(obj, _Py_NULL);
    if (weakref == _Py_NULL) {
        return _Py_NULL;
    }

    // test PyWeakref_GetRef(), reference is alive
    PyObject *ref = UNINITIALIZED_OBJ;
    assert(PyWeakref_GetRef(weakref, &ref) == 1);
    assert(ref == obj);
    assert(Py_REFCNT(obj) == (refcnt + 1));
    Py_DECREF(ref);

    // delete the referenced object: clear the weakref
    Py_DECREF(obj);
    gc_collect();

    // test PyWeakref_GetRef(), reference is dead
    ref = Py_True;
    assert(PyWeakref_GetRef(weakref, &ref) == 0);
    assert(ref == _Py_NULL);

    // test PyWeakref_GetRef(), invalid type
    PyObject *invalid_weakref = Py_None;
    assert(!PyErr_Occurred());
    ref = Py_True;
    assert(PyWeakref_GetRef(invalid_weakref, &ref) == -1);
    assert(PyErr_ExceptionMatches(PyExc_TypeError));
    assert(ref == _Py_NULL);
    PyErr_Clear();

#ifndef PYPY_VERSION
    // test PyWeakref_GetRef(NULL)
    ref = Py_True;
    assert(PyWeakref_GetRef(_Py_NULL, &ref) == -1);
    assert(PyErr_ExceptionMatches(PyExc_SystemError));
    assert(ref == _Py_NULL);
    PyErr_Clear();
#endif

    Py_DECREF(weakref);
    Py_RETURN_NONE;
}


static void
test_vectorcall_noargs(PyObject *func_varargs)
{
    PyObject *res = PyObject_Vectorcall(func_varargs, _Py_NULL, 0, _Py_NULL);
    assert(res != _Py_NULL);

    assert(PyTuple_Check(res));
    assert(PyTuple_GET_SIZE(res) == 1);
    PyObject *posargs = PyTuple_GET_ITEM(res, 0);

    assert(PyTuple_Check(posargs));
    assert(PyTuple_GET_SIZE(posargs) == 0);

    Py_DECREF(res);
}


static void
test_vectorcall_args(PyObject *func_varargs)
{
    PyObject *args_tuple = Py_BuildValue("ii", 1, 2);
    assert(args_tuple != _Py_NULL);
    size_t nargs = (size_t)PyTuple_GET_SIZE(args_tuple);
    PyObject **args = &PyTuple_GET_ITEM(args_tuple, 0);

    PyObject *res = PyObject_Vectorcall(func_varargs, args, nargs, _Py_NULL);
    Py_DECREF(args_tuple);
    assert(res != _Py_NULL);

    assert(PyTuple_Check(res));
    assert(PyTuple_GET_SIZE(res) == 1);
    PyObject *posargs = PyTuple_GET_ITEM(res, 0);

    assert(PyTuple_Check(posargs));
    assert(PyTuple_GET_SIZE(posargs) == 2);
    check_int(PyTuple_GET_ITEM(posargs, 0), 1);
    check_int(PyTuple_GET_ITEM(posargs, 1), 2);

    Py_DECREF(res);
}


static void
test_vectorcall_args_offset(PyObject *func_varargs)
{
    // args contains 3 values, but only pass 2 last values
    PyObject *args_tuple = Py_BuildValue("iii", 1, 2, 3);
    assert(args_tuple != _Py_NULL);
    size_t nargs = 2 | PY_VECTORCALL_ARGUMENTS_OFFSET;
    PyObject **args = &PyTuple_GET_ITEM(args_tuple, 1);
    PyObject *arg0 = PyTuple_GET_ITEM(args_tuple, 0);

    PyObject *res = PyObject_Vectorcall(func_varargs, args, nargs, _Py_NULL);
    assert(PyTuple_GET_ITEM(args_tuple, 0) == arg0);
    Py_DECREF(args_tuple);
    assert(res != _Py_NULL);

    assert(PyTuple_Check(res));
    assert(PyTuple_GET_SIZE(res) == 1);
    PyObject *posargs = PyTuple_GET_ITEM(res, 0);

    assert(PyTuple_Check(posargs));
    assert(PyTuple_GET_SIZE(posargs) == 2);
    check_int(PyTuple_GET_ITEM(posargs, 0), 2);
    check_int(PyTuple_GET_ITEM(posargs, 1), 3);

    Py_DECREF(res);
}


static void
test_vectorcall_args_kwnames(PyObject *func_varargs)
{
    PyObject *args_tuple = Py_BuildValue("iiiii", 1, 2, 3, 4, 5);
    assert(args_tuple != _Py_NULL);
    PyObject **args = &PyTuple_GET_ITEM(args_tuple, 0);

#ifdef PYTHON3
    PyObject *key1 = PyUnicode_FromString("key1");
    PyObject *key2 = PyUnicode_FromString("key2");
#else
    PyObject *key1 = PyString_FromString("key1");
    PyObject *key2 = PyString_FromString("key2");
#endif
    assert(key1 != _Py_NULL);
    assert(key2 != _Py_NULL);
    PyObject *kwnames = PyTuple_Pack(2, key1, key2);
    assert(kwnames != _Py_NULL);
    size_t nargs = (size_t)(PyTuple_GET_SIZE(args_tuple) - PyTuple_GET_SIZE(kwnames));

    PyObject *res = PyObject_Vectorcall(func_varargs, args, nargs, kwnames);
    Py_DECREF(args_tuple);
    Py_DECREF(kwnames);
    assert(res != _Py_NULL);

    assert(PyTuple_Check(res));
    assert(PyTuple_GET_SIZE(res) == 2);
    PyObject *posargs = PyTuple_GET_ITEM(res, 0);
    PyObject *kwargs = PyTuple_GET_ITEM(res, 1);

    assert(PyTuple_Check(posargs));
    assert(PyTuple_GET_SIZE(posargs) == 3);
    check_int(PyTuple_GET_ITEM(posargs, 0), 1);
    check_int(PyTuple_GET_ITEM(posargs, 1), 2);
    check_int(PyTuple_GET_ITEM(posargs, 2), 3);

    assert(PyDict_Check(kwargs));
    assert(PyDict_Size(kwargs) == 2);

    Py_ssize_t pos = 0;
    PyObject *key, *value;
    while (PyDict_Next(kwargs, &pos, &key, &value)) {
#ifdef PYTHON3
        assert(PyUnicode_Check(key));
#else
        assert(PyString_Check(key));
#endif
        if (PyObject_RichCompareBool(key, key1, Py_EQ)) {
            check_int(value, 4);
        }
        else {
            assert(PyObject_RichCompareBool(key, key2, Py_EQ));
            check_int(value, 5);
        }
    }

    Py_DECREF(res);
    Py_DECREF(key1);
    Py_DECREF(key2);
}


static PyObject *
test_vectorcall(PyObject *module, PyObject *Py_UNUSED(args))
{
#ifndef PYTHON3
    module = PyImport_ImportModule(MODULE_NAME_STR);
    assert(module != _Py_NULL);
#endif
    PyObject *func_varargs = PyObject_GetAttrString(module, "func_varargs");
#ifndef PYTHON3
    Py_DECREF(module);
#endif
    if (func_varargs == _Py_NULL) {
        return _Py_NULL;
    }

    // test PyObject_Vectorcall()
    test_vectorcall_noargs(func_varargs);
    test_vectorcall_args(func_varargs);
    test_vectorcall_args_offset(func_varargs);
    test_vectorcall_args_kwnames(func_varargs);

    Py_DECREF(func_varargs);
    Py_RETURN_NONE;
}


static PyObject *
test_getattr(PyObject *Py_UNUSED(module), PyObject *Py_UNUSED(args))
{
    assert(!PyErr_Occurred());

    PyObject *obj = PyImport_ImportModule("sys");
    if (obj == _Py_NULL) {
        return _Py_NULL;
    }

    PyObject *attr_name = create_string("version");
    PyObject *missing_attr = create_string("nonexistant_attr_name");

    // test PyObject_GetOptionalAttr(): attribute exists
    PyObject *value;
    value = UNINITIALIZED_OBJ;
    assert(PyObject_GetOptionalAttr(obj, attr_name, &value) == 1);
    assert(value != _Py_NULL);
    Py_DECREF(value);

    // test PyObject_HasAttrWithError(): attribute exists
    assert(PyObject_HasAttrWithError(obj, attr_name) == 1);

    // test PyObject_GetOptionalAttrString(): attribute exists
    value = UNINITIALIZED_OBJ;
    assert(PyObject_GetOptionalAttrString(obj, "version", &value) == 1);
    assert(!PyErr_Occurred());
    assert(value != _Py_NULL);
    Py_DECREF(value);

    // test PyObject_HasAttrStringWithError(): attribute exists
    assert(PyObject_HasAttrStringWithError(obj, "version") == 1);
    assert(!PyErr_Occurred());

    // test PyObject_GetOptionalAttr(): attribute doesn't exist
    value = UNINITIALIZED_OBJ;
    assert(PyObject_GetOptionalAttr(obj, missing_attr, &value) == 0);
    assert(!PyErr_Occurred());
    assert(value == _Py_NULL);

    // test PyObject_HasAttrWithError(): attribute doesn't exist
    assert(PyObject_HasAttrWithError(obj, missing_attr) == 0);
    assert(!PyErr_Occurred());

    // test PyObject_GetOptionalAttrString(): attribute doesn't exist
    value = UNINITIALIZED_OBJ;
    assert(PyObject_GetOptionalAttrString(obj, "nonexistant_attr_name", &value) == 0);
    assert(!PyErr_Occurred());
    assert(value == _Py_NULL);

    // test PyObject_HasAttrStringWithError(): attribute doesn't exist
    assert(PyObject_HasAttrStringWithError(obj, "nonexistant_attr_name") == 0);
    assert(!PyErr_Occurred());

    Py_DECREF(attr_name);
    Py_DECREF(missing_attr);
    Py_DECREF(obj);
    Py_RETURN_NONE;
}


static PyObject *
test_getitem(PyObject *Py_UNUSED(module), PyObject *Py_UNUSED(args))
{
    assert(!PyErr_Occurred());

    PyObject *value = Py_BuildValue("s", "value");
    assert(value != _Py_NULL);
    PyObject *obj = Py_BuildValue("{sO}", "key", value);
    assert(obj != _Py_NULL);
    PyObject *present_key, *missing_key;
    PyObject *item;

    present_key = create_string("key");
    missing_key = create_string("dontexist");

    // test PyMapping_GetOptionalItem(): key is present
    item = UNINITIALIZED_OBJ;
    assert(PyMapping_GetOptionalItem(obj, present_key, &item) == 1);
    assert(item == value);
    Py_DECREF(item);
    assert(!PyErr_Occurred());

    // test PyMapping_HasKeyWithError(): key is present
    assert(PyMapping_HasKeyWithError(obj, present_key) == 1);
    assert(!PyErr_Occurred());

    // test PyMapping_GetOptionalItemString(): key is present
    item = UNINITIALIZED_OBJ;
    assert(PyMapping_GetOptionalItemString(obj, "key", &item) == 1);
    assert(item == value);
    Py_DECREF(item);

    // test PyMapping_HasKeyStringWithError(): key is present
    assert(PyMapping_HasKeyStringWithError(obj, "key") == 1);
    assert(!PyErr_Occurred());

    // test PyMapping_GetOptionalItem(): missing key
    item = UNINITIALIZED_OBJ;
    assert(PyMapping_GetOptionalItem(obj, missing_key, &item) == 0);
    assert(item == _Py_NULL);
    assert(!PyErr_Occurred());

    // test PyMapping_HasKeyWithError(): missing key
    assert(PyMapping_HasKeyWithError(obj, missing_key) == 0);
    assert(!PyErr_Occurred());

    // test PyMapping_GetOptionalItemString(): missing key
    item = UNINITIALIZED_OBJ;
    assert(PyMapping_GetOptionalItemString(obj, "dontexist", &item) == 0);
    assert(item == _Py_NULL);

    // test PyMapping_HasKeyStringWithError(): missing key
    assert(PyMapping_HasKeyStringWithError(obj, "dontexist") == 0);
    assert(!PyErr_Occurred());

    Py_DECREF(obj);
    Py_DECREF(value);
    Py_DECREF(present_key);
    Py_DECREF(missing_key);
    Py_RETURN_NONE;
}


static PyObject *
test_dict_api(PyObject *Py_UNUSED(module), PyObject *Py_UNUSED(args))
{
    assert(!PyErr_Occurred());

    PyObject *dict = NULL, *key = NULL, *missing_key = NULL, *value = NULL;
    PyObject *invalid_key = NULL;
    PyObject *invalid_dict = NULL;
    PyObject *get_value = NULL;
    int res;

    // test PyDict_New()
    dict = PyDict_New();
    if (dict == NULL) {
        goto error;
    }

    key = PyUnicode_FromString("key");
    if (key == NULL) {
        goto error;
    }
    invalid_dict = key;  // borrowed reference

    missing_key = PyUnicode_FromString("missing_key");
    if (missing_key == NULL) {
        goto error;
    }

    invalid_key = PyList_New(0);  // not hashable key
    if (invalid_key == NULL) {
        goto error;
    }

    value = PyUnicode_FromString("value");
    if (value == NULL) {
        goto error;
    }

    res = PyDict_SetItemString(dict, "key", value);
    if (res < 0) {
        goto error;
    }
    assert(res == 0);

    // test PyDict_Contains()
    assert(PyDict_Contains(dict, key) == 1);
    assert(PyDict_Contains(dict, missing_key) == 0);

    // test PyDict_ContainsString()
    assert(PyDict_ContainsString(dict, "key") == 1);
    assert(PyDict_ContainsString(dict, "missing_key") == 0);
    assert(PyDict_ContainsString(dict, "\xff") == -1);
    assert(PyErr_ExceptionMatches(PyExc_UnicodeDecodeError));
    PyErr_Clear();

    // test PyDict_GetItemRef(), key is present
    get_value = UNINITIALIZED_OBJ;
    assert(PyDict_GetItemRef(dict, key, &get_value) == 1);
    assert(get_value == value);
    Py_DECREF(get_value);

    // test PyDict_GetItemStringRef(), key is present
    get_value = UNINITIALIZED_OBJ;
    assert(PyDict_GetItemStringRef(dict, "key", &get_value) == 1);
    assert(get_value == value);
    Py_DECREF(get_value);

    // test PyDict_GetItemRef(), missing key
    get_value = UNINITIALIZED_OBJ;
    assert(PyDict_GetItemRef(dict, missing_key, &get_value) == 0);
    assert(!PyErr_Occurred());
    assert(get_value == NULL);

    // test PyDict_GetItemStringRef(), missing key
    get_value = UNINITIALIZED_OBJ;
    assert(PyDict_GetItemStringRef(dict, "missing_key", &get_value) == 0);
    assert(!PyErr_Occurred());
    assert(get_value == NULL);

    // test PyDict_GetItemRef(), invalid dict
    get_value = UNINITIALIZED_OBJ;
    assert(PyDict_GetItemRef(invalid_dict, key, &get_value) == -1);
    assert(PyErr_ExceptionMatches(PyExc_SystemError));
    PyErr_Clear();
    assert(get_value == NULL);

    // test PyDict_GetItemStringRef(), invalid dict
    get_value = UNINITIALIZED_OBJ;
    assert(PyDict_GetItemStringRef(invalid_dict, "key", &get_value) == -1);
    assert(PyErr_ExceptionMatches(PyExc_SystemError));
    PyErr_Clear();
    assert(get_value == NULL);

    // test PyDict_GetItemRef(), invalid key
    get_value = UNINITIALIZED_OBJ;
    assert(PyDict_GetItemRef(dict, invalid_key, &get_value) == -1);
    assert(PyErr_ExceptionMatches(PyExc_TypeError));
    PyErr_Clear();
    assert(get_value == NULL);

    Py_DECREF(dict);
    Py_DECREF(key);
    Py_DECREF(missing_key);
    Py_DECREF(value);
    Py_DECREF(invalid_key);

    Py_RETURN_NONE;

error:
    Py_XDECREF(dict);
    Py_XDECREF(key);
    Py_XDECREF(missing_key);
    Py_XDECREF(value);
    Py_XDECREF(invalid_key);
    return NULL;
}


static PyObject *
test_dict_pop(PyObject *Py_UNUSED(module), PyObject *Py_UNUSED(args))
{
    PyObject *dict = PyDict_New();
    if (dict == NULL) {
        return NULL;
    }

    PyObject *key = PyUnicode_FromString("key");
    assert(key != NULL);
    PyObject *value = PyUnicode_FromString("abc");
    assert(value != NULL);

    // test PyDict_Pop(), get the removed value, key is present
    assert(PyDict_SetItem(dict, key, value) == 0);
    PyObject *removed = UNINITIALIZED_OBJ;
    assert(PyDict_Pop(dict, key, &removed) == 1);
    assert(removed == value);
    Py_DECREF(removed);

    // test PyDict_Pop(), ignore the removed value, key is present
    assert(PyDict_SetItem(dict, key, value) == 0);
    assert(PyDict_Pop(dict, key, NULL) == 1);

    // test PyDict_Pop(), key is missing
    removed = UNINITIALIZED_OBJ;
    assert(PyDict_Pop(dict, key, &removed) == 0);
    assert(removed == NULL);
    assert(PyDict_Pop(dict, key, NULL) == 0);

    // test PyDict_PopString(), get the removed value, key is present
    assert(PyDict_SetItem(dict, key, value) == 0);
    removed = UNINITIALIZED_OBJ;
    assert(PyDict_PopString(dict, "key", &removed) == 1);
    assert(removed == value);
    Py_DECREF(removed);

    // test PyDict_PopString(), ignore the removed value, key is present
    assert(PyDict_SetItem(dict, key, value) == 0);
    assert(PyDict_PopString(dict, "key", NULL) == 1);

    // test PyDict_PopString(), key is missing
    removed = UNINITIALIZED_OBJ;
    assert(PyDict_PopString(dict, "key", &removed) == 0);
    assert(removed == NULL);
    assert(PyDict_PopString(dict, "key", NULL) == 0);

    // dict error
    removed = UNINITIALIZED_OBJ;
    assert(PyDict_Pop(key, key, &removed) == -1);
    assert(removed == NULL);
    assert(PyErr_ExceptionMatches(PyExc_SystemError));
    PyErr_Clear();

    assert(PyDict_Pop(key, key, NULL) == -1);
    assert(PyErr_ExceptionMatches(PyExc_SystemError));
    PyErr_Clear();

    removed = UNINITIALIZED_OBJ;
    assert(PyDict_PopString(key, "key", &removed) == -1);
    assert(removed == NULL);
    assert(PyErr_ExceptionMatches(PyExc_SystemError));
    PyErr_Clear();

    assert(PyDict_PopString(key, "key", NULL) == -1);
    assert(PyErr_ExceptionMatches(PyExc_SystemError));
    PyErr_Clear();

    // exit
    Py_DECREF(dict);
    Py_DECREF(key);
    Py_DECREF(value);
    Py_RETURN_NONE;
}


static PyObject *
test_dict_setdefault(PyObject *Py_UNUSED(module), PyObject *Py_UNUSED(args))
{
    PyObject *dict = PyDict_New();
    if (dict == NULL) {
        return NULL;
    }
    PyObject *key = PyUnicode_FromString("key");
    assert(key != NULL);
    PyObject *value = PyUnicode_FromString("abc");
    assert(value != NULL);
    PyObject *invalid_key = PyList_New(0);  // not hashable key
    assert(invalid_key != NULL);

    // insert item
    PyObject *result = UNINITIALIZED_OBJ;
    assert(PyDict_SetDefaultRef(dict, key, value, &result) == 0);
    assert(result == value);
    Py_DECREF(result);

    // item already present
    result = UNINITIALIZED_OBJ;
    assert(PyDict_SetDefaultRef(dict, key, value, &result) == 1);
    assert(result == value);
    Py_DECREF(result);

    // error: invalid key
    assert(!PyErr_Occurred());
    result = UNINITIALIZED_OBJ;
    assert(PyDict_SetDefaultRef(dict, invalid_key, value, &result) == -1);
    assert(result == NULL);
    assert(PyErr_Occurred());
    PyErr_Clear();

    // insert item with NULL result
    assert(PyDict_Pop(dict, key, NULL) == 1);
    assert(PyDict_SetDefaultRef(dict, key, value, NULL) == 0);

    // item already present with NULL result
    assert(PyDict_SetDefaultRef(dict, key, value, NULL) == 1);

    // error: invalid key with NULL result
    assert(!PyErr_Occurred());
    assert(PyDict_SetDefaultRef(dict, invalid_key, value, NULL) == -1);
    assert(PyErr_Occurred());
    PyErr_Clear();

    // exit
    Py_DECREF(dict);
    Py_DECREF(key);
    Py_DECREF(value);
    Py_DECREF(invalid_key);
    Py_RETURN_NONE;
}


static PyObject *
test_long_api(PyObject *Py_UNUSED(module), PyObject *Py_UNUSED(args))
{
    // test PyLong_AsInt()
    assert(!PyErr_Occurred());
    PyObject *obj = PyLong_FromLong(123);
    if (obj == NULL) {
        return NULL;
    }
    int value = PyLong_AsInt(obj);
    assert(value == 123);
    assert(!PyErr_Occurred());
    Py_DECREF(obj);

    // test PyLong_AsInt() with overflow
    PyObject *obj2 = PyLong_FromLongLong((long long)INT_MAX + 1);
    if (obj2 == NULL) {
        return NULL;
    }
    value = PyLong_AsInt(obj2);
    assert(value == -1);
    assert(PyErr_ExceptionMatches(PyExc_OverflowError));
    PyErr_Clear();
    Py_DECREF(obj2);

    // test PyLong_GetSign()
    int sign = UNINITIALIZED_INT;
    assert(PyLong_GetSign(obj, &sign) == 0);
    assert(sign == 1);

    Py_RETURN_NONE;
}


// --- HeapCTypeWithManagedDict --------------------------------------------

// Py_TPFLAGS_MANAGED_DICT was added to Python 3.11.0a3
#if PY_VERSION_HEX >= 0x030B00A3
#  define TEST_MANAGED_DICT

typedef struct {
    PyObject_HEAD
} HeapCTypeObject;

static int
heapmanaged_traverse(PyObject *self, visitproc visit, void *arg)
{
    Py_VISIT(Py_TYPE(self));
    return PyObject_VisitManagedDict(self, visit, arg);
}

static int
heapmanaged_clear(PyObject *self)
{
    PyObject_ClearManagedDict(self);
    return 0;
}

static void
heapmanaged_dealloc(HeapCTypeObject *self)
{
    PyTypeObject *tp = Py_TYPE(self);
    PyObject_ClearManagedDict((PyObject *)self);
    PyObject_GC_UnTrack(self);
    PyObject_GC_Del(self);
    Py_DECREF(tp);
}

static PyType_Slot HeapCTypeWithManagedDict_slots[] = {
    {Py_tp_traverse, _Py_CAST(void*, heapmanaged_traverse)},
    {Py_tp_clear, _Py_CAST(void*, heapmanaged_clear)},
    {Py_tp_dealloc, _Py_CAST(void*, heapmanaged_dealloc)},
    {0, 0},
};

static PyType_Spec HeapCTypeWithManagedDict_spec = {
    "test_pythoncapi_compat.HeapCTypeWithManagedDict",
    sizeof(PyObject),
    0,
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_GC | Py_TPFLAGS_MANAGED_DICT,
    HeapCTypeWithManagedDict_slots
};

static PyObject *
test_managed_dict(PyObject *Py_UNUSED(module), PyObject *Py_UNUSED(args))
{
    PyObject *type = PyType_FromSpec(&HeapCTypeWithManagedDict_spec);
    if (type == NULL) {
        return NULL;
    }

    PyObject *obj = PyObject_CallNoArgs(type);
    if (obj == NULL) {
        Py_DECREF(type);
        return NULL;
    }

    // call heapmanaged_traverse()
    PyGC_Collect();

    // call heapmanaged_clear()
    Py_DECREF(obj);
    PyGC_Collect();

    Py_DECREF(type);
    // Just in case!
    PyGC_Collect();

    Py_RETURN_NONE;
}
#endif  // PY_VERSION_HEX >= 0x030B00A3


static PyObject *
test_unicode(PyObject *Py_UNUSED(module), PyObject *Py_UNUSED(args))
{
    PyObject *abc = PyUnicode_FromString("abc");
    if (abc == NULL) {
        return NULL;
    }

    PyObject *abc0def = PyUnicode_FromStringAndSize("abc\0def", 7);
    if (abc0def == NULL) {
        Py_DECREF(abc);
        return NULL;
    }

    // PyUnicode_EqualToUTF8() and PyUnicode_EqualToUTF8AndSize() can be called
    // with an exception raised and they must not clear the current exception.
    PyErr_NoMemory();

    assert(PyUnicode_EqualToUTF8AndSize(abc, "abc", 3) == 1);
    assert(PyUnicode_EqualToUTF8AndSize(abc, "Python", 6) == 0);
    assert(PyUnicode_EqualToUTF8AndSize(abc0def, "abc\0def", 7) == 1);

    assert(PyUnicode_EqualToUTF8(abc, "abc") == 1);
    assert(PyUnicode_EqualToUTF8(abc, "Python") == 0);
    assert(PyUnicode_EqualToUTF8(abc0def, "abc\0def") == 0);

    assert(PyErr_ExceptionMatches(PyExc_MemoryError));
    PyErr_Clear();

    Py_DECREF(abc);
    Py_DECREF(abc0def);
    Py_RETURN_NONE;
}


static PyObject *
test_list(PyObject *Py_UNUSED(module), PyObject *Py_UNUSED(args))
{
    PyObject *list = PyList_New(0);
    if (list == NULL) {
        return NULL;
    }

    // test PyList_Extend()
    {
        PyObject *abc = PyUnicode_FromString("abc");
        if (abc == NULL) {
            Py_DECREF(list);
            return NULL;
        }

        assert(PyList_Extend(list, abc) == 0);
        Py_DECREF(abc);
        assert(PyList_GET_SIZE(list) == 3);
    }

    // test PyList_GetItemRef()
    PyObject *item = PyList_GetItemRef(list, 1);
    assert(item != NULL);
    assert(item == PyList_GetItem(list, 1));
    Py_DECREF(item);

    // test PyList_Clear()
    assert(PyList_Clear(list) == 0);
    assert(PyList_GET_SIZE(list) == 0);

    Py_DECREF(list);
    Py_RETURN_NONE;
}


static PyObject *
test_hash(PyObject *Py_UNUSED(module), PyObject *Py_UNUSED(args))
{
    void *ptr0 = NULL;
    assert(Py_HashPointer(ptr0) == 0);

#ifndef PYPY_VERSION
#if SIZEOF_VOID_P == 8
    void *ptr1 = (void*)(uintptr_t)0xABCDEF1234567890;
    assert(Py_HashPointer(ptr1) == (uintptr_t)0x0ABCDEF123456789);
#else
    void *ptr1 = (void*)(uintptr_t)0xDEADCAFE;
    assert(Py_HashPointer(ptr1) == (uintptr_t)0xEDEADCAF);
#endif
#else
    // PyPy
#if SIZEOF_VOID_P == 8
    void *ptr1 = (void*)(uintptr_t)0xABCDEF1234567890;
#else
    void *ptr1 = (void*)(uintptr_t)0xDEADCAFE;
#endif
    assert(Py_HashPointer(ptr1) == (Py_hash_t)ptr1);
#endif

#if ((!defined(PYPY_VERSION) && PY_VERSION_HEX >= 0x030400B1) \
     || (defined(PYPY_VERSION) && PY_VERSION_HEX >= 0x03070000 \
         && PYPY_VERSION_NUM >= 0x07090000))
    // Just check that constants are available
    size_t bits = PyHASH_BITS;
    assert(bits >= 8);
    size_t mod = PyHASH_MODULUS;
    assert(mod >= 7);
    size_t inf = PyHASH_INF;
    assert(inf != 0);
    size_t imag = PyHASH_IMAG;
    assert(imag != 0);
#endif

    Py_RETURN_NONE;
}


#if PY_VERSION_HEX  >= 0x03050000
#define TEST_PYTIME

static PyObject *
test_time(PyObject *Py_UNUSED(module), PyObject *Py_UNUSED(args))
{
    PyTime_t t;
#define UNINITIALIZED_TIME ((PyTime_t)-483884113929936179)

    t = UNINITIALIZED_TIME;
    assert(PyTime_Time(&t) == 0);
    assert(t != UNINITIALIZED_TIME);

    t = UNINITIALIZED_TIME;
    assert(PyTime_Monotonic(&t) == 0);
    assert(t != UNINITIALIZED_TIME);

    // Test multiple times since an implementation uses a cache
    for (int i=0; i < 5; i++) {
        t = UNINITIALIZED_TIME;
        assert(PyTime_PerfCounter(&t) == 0);
        assert(t != UNINITIALIZED_TIME);
    }

    assert(PyTime_AsSecondsDouble(1) == 1e-9);
    assert(PyTime_AsSecondsDouble(1500 * 1000 * 1000) == 1.5);
    assert(PyTime_AsSecondsDouble(-500 * 1000 * 1000) == -0.5);

    Py_RETURN_NONE;
}
#endif


static void
check_get_constant(PyObject* (*get_constant)(unsigned int), int borrowed)
{
#define CLEAR(var) if (!borrowed) { Py_DECREF(var); }

    PyObject *obj, *expected;

    // Py_CONSTANT_NONE
    obj = get_constant(Py_CONSTANT_NONE);
    assert(obj == Py_None);
    CLEAR(obj);

    // Py_CONSTANT_FALSE
    obj = get_constant(Py_CONSTANT_FALSE);
    assert(obj = Py_False);
    CLEAR(obj);

    // Py_CONSTANT_TRUE
    obj = get_constant(Py_CONSTANT_TRUE);
    assert(obj == Py_True);
    CLEAR(obj);

    // Py_CONSTANT_ELLIPSIS
    obj = get_constant(Py_CONSTANT_ELLIPSIS);
    assert(obj == Py_Ellipsis);
    CLEAR(obj);

    // Py_CONSTANT_NOT_IMPLEMENTED
    obj = get_constant(Py_CONSTANT_NOT_IMPLEMENTED);
    assert(obj == Py_NotImplemented);
    CLEAR(obj);

    // Py_CONSTANT_ZERO
    obj = get_constant(Py_CONSTANT_ZERO);
    expected = PyLong_FromLong(0);
    assert(expected != NULL);
    assert(Py_TYPE(obj) == &PyLong_Type);
    assert(PyObject_RichCompareBool(obj, expected, Py_EQ) == 1);
    CLEAR(obj);
    Py_DECREF(expected);

    // Py_CONSTANT_ONE
    obj = get_constant(Py_CONSTANT_ONE);
    expected = PyLong_FromLong(1);
    assert(expected != NULL);
    assert(Py_TYPE(obj) == &PyLong_Type);
    assert(PyObject_RichCompareBool(obj, expected, Py_EQ) == 1);
    CLEAR(obj);
    Py_DECREF(expected);

    // Py_CONSTANT_EMPTY_STR
    obj = get_constant(Py_CONSTANT_EMPTY_STR);
    assert(Py_TYPE(obj) == &PyUnicode_Type);
#if PY_VERSION_HEX >= 0x03030000
    assert(PyUnicode_GetLength(obj) == 0);
#else
    assert(PyUnicode_GetSize(obj) == 0);
#endif
    CLEAR(obj);

    // Py_CONSTANT_EMPTY_BYTES
    obj = get_constant(Py_CONSTANT_EMPTY_BYTES);
    assert(Py_TYPE(obj) == &PyBytes_Type);
    assert(PyBytes_Size(obj) == 0);
    CLEAR(obj);

    // Py_CONSTANT_EMPTY_TUPLE
    obj = get_constant(Py_CONSTANT_EMPTY_TUPLE);
    assert(Py_TYPE(obj) == &PyTuple_Type);
    assert(PyTuple_Size(obj) == 0);
    CLEAR(obj);

#undef CLEAR
}


static PyObject *
test_get_constant(PyObject *Py_UNUSED(module), PyObject *Py_UNUSED(args))
{
    check_get_constant(Py_GetConstant, 0);
    check_get_constant(Py_GetConstantBorrowed, 1);
    Py_RETURN_NONE;
}


#if PY_VERSION_HEX < 0x030E0000 && PY_VERSION_HEX >= 0x03060000 && !defined(PYPY_VERSION)
#define TEST_UNICODEWRITER 1

static PyObject *
test_unicodewriter(PyObject *Py_UNUSED(self), PyObject *Py_UNUSED(args))
{
    PyUnicodeWriter *writer = PyUnicodeWriter_Create(0);
    if (writer == NULL) {
        return NULL;
    }
    int ret;

    // test PyUnicodeWriter_WriteStr()
    PyObject *str = PyUnicode_FromString("var");
    if (str == NULL) {
        goto error;
    }
    ret = PyUnicodeWriter_WriteStr(writer, str);
    Py_CLEAR(str);
    if (ret < 0) {
        goto error;
    }

    // test PyUnicodeWriter_WriteChar()
    if (PyUnicodeWriter_WriteChar(writer, '=') < 0) {
        goto error;
    }

    // test PyUnicodeWriter_WriteSubstring()
    str = PyUnicode_FromString("[long]");
    if (str == NULL) {
        goto error;
    }
    ret = PyUnicodeWriter_WriteSubstring(writer, str, 1, 5);
    Py_CLEAR(str);
    if (ret < 0) {
        goto error;
    }

    // test PyUnicodeWriter_WriteUTF8()
    if (PyUnicodeWriter_WriteUTF8(writer, " valu\xC3\xA9", -1) < 0) {
        goto error;
    }
    if (PyUnicodeWriter_WriteChar(writer, ' ') < 0) {
        goto error;
    }

    // test PyUnicodeWriter_WriteRepr()
    str = PyUnicode_FromString("repr");
    if (str == NULL) {
        goto error;
    }
    if (PyUnicodeWriter_WriteRepr(writer, str) < 0) {
        goto error;
    }
    Py_CLEAR(str);

    {
        PyObject *result = PyUnicodeWriter_Finish(writer);
        if (result == NULL) {
            return NULL;
        }
        assert(PyUnicode_EqualToUTF8(result, "var=long valu\xC3\xA9 'repr'"));
        Py_DECREF(result);
    }

    Py_RETURN_NONE;

error:
    PyUnicodeWriter_Discard(writer);
    return NULL;
}


static PyObject *
test_unicodewriter_widechar(PyObject *Py_UNUSED(self), PyObject *Py_UNUSED(args))
{
    PyUnicodeWriter *writer = PyUnicodeWriter_Create(0);
    if (writer == NULL) {
        return NULL;
    }

    // test PyUnicodeWriter_WriteWideChar()
    int ret = PyUnicodeWriter_WriteWideChar(writer, L"euro=\u20AC", -1);
    if (ret < 0) {
        goto error;
    }

    {
        PyObject *result = PyUnicodeWriter_Finish(writer);
        if (result == NULL) {
            return NULL;
        }
        assert(PyUnicode_EqualToUTF8(result, "euro=\xe2\x82\xac"));
        Py_DECREF(result);
    }

    Py_RETURN_NONE;

error:
    PyUnicodeWriter_Discard(writer);
    return NULL;
}


static PyObject *
test_unicodewriter_format(PyObject *Py_UNUSED(self), PyObject *Py_UNUSED(args))
{
    PyUnicodeWriter *writer = PyUnicodeWriter_Create(0);
    if (writer == NULL) {
        return NULL;
    }

    // test PyUnicodeWriter_Format()
    if (PyUnicodeWriter_Format(writer, "%s %i", "Hello", 123) < 0) {
        goto error;
    }

    // test PyUnicodeWriter_WriteChar()
    if (PyUnicodeWriter_WriteChar(writer, '.') < 0) {
        goto error;
    }

    {
        PyObject *result = PyUnicodeWriter_Finish(writer);
        if (result == NULL) {
            return NULL;
        }
        assert(PyUnicode_EqualToUTF8(result, "Hello 123."));
        Py_DECREF(result);
    }

    Py_RETURN_NONE;

error:
    PyUnicodeWriter_Discard(writer);
    return NULL;
}
#endif


static struct PyMethodDef methods[] = {
    {"test_object", test_object, METH_NOARGS, _Py_NULL},
    {"test_py_is", test_py_is, METH_NOARGS, _Py_NULL},
#ifndef PYPY_VERSION
    {"test_frame", test_frame, METH_NOARGS, _Py_NULL},
#endif
    {"test_thread_state", test_thread_state, METH_NOARGS, _Py_NULL},
    {"test_interpreter", test_interpreter, METH_NOARGS, _Py_NULL},
    {"test_calls", test_calls, METH_NOARGS, _Py_NULL},
    {"test_gc", test_gc, METH_NOARGS, _Py_NULL},
    {"test_module", test_module, METH_NOARGS, _Py_NULL},
#if (PY_VERSION_HEX <= 0x030B00A1 || 0x030B00A7 <= PY_VERSION_HEX) && !defined(PYPY_VERSION)
    {"test_float_pack", test_float_pack, METH_NOARGS, _Py_NULL},
#endif
#ifndef PYPY_VERSION
    {"test_code", test_code, METH_NOARGS, _Py_NULL},
#endif
    {"test_api_casts", test_api_casts, METH_NOARGS, _Py_NULL},
    {"test_import", test_import, METH_NOARGS, _Py_NULL},
    {"test_weakref", test_weakref, METH_NOARGS, _Py_NULL},
    {"func_varargs", (PyCFunction)(void*)func_varargs, METH_VARARGS | METH_KEYWORDS, _Py_NULL},
    {"test_vectorcall", test_vectorcall, METH_NOARGS, _Py_NULL},
    {"test_getattr", test_getattr, METH_NOARGS, _Py_NULL},
    {"test_getitem", test_getitem, METH_NOARGS, _Py_NULL},
    {"test_dict_api", test_dict_api, METH_NOARGS, _Py_NULL},
    {"test_dict_pop", test_dict_pop, METH_NOARGS, _Py_NULL},
    {"test_dict_setdefault", test_dict_setdefault, METH_NOARGS, _Py_NULL},
    {"test_long_api", test_long_api, METH_NOARGS, _Py_NULL},
#ifdef TEST_MANAGED_DICT
    {"test_managed_dict", test_managed_dict, METH_NOARGS, _Py_NULL},
#endif
    {"test_unicode", test_unicode, METH_NOARGS, _Py_NULL},
    {"test_list", test_list, METH_NOARGS, _Py_NULL},
    {"test_hash", test_hash, METH_NOARGS, _Py_NULL},
#ifdef TEST_PYTIME
    {"test_time", test_time, METH_NOARGS, _Py_NULL},
#endif
    {"test_get_constant", test_get_constant, METH_NOARGS, _Py_NULL},
#ifdef TEST_UNICODEWRITER
    {"test_unicodewriter", test_unicodewriter, METH_NOARGS, _Py_NULL},
    {"test_unicodewriter_widechar", test_unicodewriter_widechar, METH_NOARGS, _Py_NULL},
    {"test_unicodewriter_format", test_unicodewriter_format, METH_NOARGS, _Py_NULL},
#endif
    {_Py_NULL, _Py_NULL, 0, _Py_NULL}
};


static int
module_exec(PyObject *module)
{
#ifdef __cplusplus
    if (PyModule_AddIntMacro(module, __cplusplus)) {
        return -1;
    }
#endif
    if (PyModule_AddStringMacro(module, PY_VERSION)) {
        return -1;
    }
    if (PyModule_AddIntMacro(module, PY_VERSION_HEX)) {
        return -1;
    }
#ifdef PYPY_VERSION
    if (PyModule_AddStringMacro(module, PYPY_VERSION)) {
        return -1;
    }
#endif
#ifdef PYPY_VERSION_NUM
    if (PyModule_AddIntMacro(module, PYPY_VERSION_NUM)) {
        return -1;
    }
#endif
    return 0;
}


#if PY_VERSION_HEX >= 0x03050000
static PyModuleDef_Slot module_slots[] = {
    {Py_mod_exec, _Py_CAST(void*, module_exec)},
    {0, _Py_NULL}
};
#endif


#ifdef PYTHON3
static struct PyModuleDef module_def = {
    PyModuleDef_HEAD_INIT,
    MODULE_NAME_STR,     // m_name
    _Py_NULL,            // m_doc
    0,                   // m_size
    methods,             // m_methods
#if PY_VERSION_HEX >= 0x03050000
    module_slots,        // m_slots
#else
    _Py_NULL,            // m_reload
#endif
    _Py_NULL,            // m_traverse
    _Py_NULL,            // m_clear
    _Py_NULL,            // m_free
};


#define INIT_FUNC CONCAT(PyInit_, MODULE_NAME)

#if PY_VERSION_HEX >= 0x03050000
PyMODINIT_FUNC
INIT_FUNC(void)
{
    return PyModuleDef_Init(&module_def);
}
#else
// Python 3.4
PyMODINIT_FUNC
INIT_FUNC(void)
{
    PyObject *module = PyModule_Create(&module_def);
    if (module == _Py_NULL) {
        return _Py_NULL;
    }
    if (module_exec(module) < 0) {
        Py_DECREF(module);
        return _Py_NULL;
    }
    return module;
}
#endif

#else
// Python 2

#define INIT_FUNC CONCAT(init, MODULE_NAME)

PyMODINIT_FUNC
INIT_FUNC(void)
{
    PyObject *module;
    module = Py_InitModule4(MODULE_NAME_STR,
                            methods,
                            _Py_NULL,
                            _Py_NULL,
                            PYTHON_API_VERSION);
    if (module == _Py_NULL) {
        return;
    }

    if (module_exec(module) < 0) {
        return;
    }
}
#endif
