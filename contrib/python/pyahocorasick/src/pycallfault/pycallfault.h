#ifndef PYCALLFAULT_H_
#define PYCALLFAULT_H_

#define F(name) name##_custom

void initialize_pycallfault(void);

// --- python function wrappers -----------------------------------------

int check(void);
int check_and_set_error(void);

#define PyObject_New_custom(...) (check_and_set_error() ? NULL : PyObject_New(__VA_ARGS__))

#define PyArg_ParseTuple_custom(...) (check() ? 0 : PyArg_ParseTuple(__VA_ARGS__))

#define PyTuple_GetItem_custom(...) (check_and_set_error() ? NULL : PyTuple_GetItem(__VA_ARGS__))

#define PyList_New_custom(arg) (check_and_set_error() ? NULL : PyList_New(arg))

#define PyList_GetItem_custom(...) (check_and_set_error() ? NULL : PyList_GetItem(__VA_ARGS__))

#define PyList_SetItem_custom(...) (check_and_set_error() ? -1 : PyList_SetItem(__VA_ARGS__))

#define PyList_Append_custom(...) (check_and_set_error() ? -1 : PyList_Append(__VA_ARGS__))

#define PyNumber_AsSsize_t_custom(...) (check_and_set_error() ? -1 : PyNumber_AsSsize_t(__VA_ARGS__))

#define Py_BuildValue_custom(...) (check_and_set_error() ? NULL : Py_BuildValue(__VA_ARGS__))

#define PyCallable_Check_custom(arg) (check() ? 0 : PyCallable_Check(arg))

#define PyString_Check_custom(arg) (check() ? 0 : PyString_Check(arg))

#define PyUnicode_Check_custom(arg) (check() ? 0 : PyUnicode_Check(arg))

#define PyBytes_Check_custom(arg) (check() ? 0 : PyBytes_Check(arg))

#define PyBytes_CheckExact_custom(arg) (check() ? 0 : PyBytes_CheckExact(arg))

#define PyNumber_Check_custom(arg) (check() ? 0 : PyNumber_Check(arg))

#define PyTuple_Check_custom(arg) (check() ? 0 : PyTuple_Check(arg))

#define PyObject_CallFunction_custom(...) (check_and_set_error() ? NULL : PyObject_CallFunction(__VA_ARGS__))

#define PyObject_CallFunctionObjArgs_custom(...) (check_and_set_error() ? NULL : PyObject_CallFunctionObjArgs(__VA_ARGS__))

#define PyArg_ParseTupleAndKeywords_custom(...) (check_and_set_error() ? 0 : PyArg_ParseTupleAndKeywords(__VA_ARGS__))

#define PyNumber_Index_custom(arg) (check_and_set_error() ? NULL : PyNumber_Index(arg))

#define PyUnicode_FromKindAndData_custom(...) (check_and_set_error() ? NULL : PyUnicode_FromKindAndData(__VA_ARGS__))

#define PyUnicode_AsUTF8String_custom(...) (check_and_set_error() ? NULL : PyUnicode_AsUTF8String(__VA_ARGS__))

#define PyBytes_FromStringAndSize_custom(...) (check_and_set_error() ? NULL : PyBytes_FromStringAndSize(__VA_ARGS__))

#endif // PYCALLFAULT_H_
