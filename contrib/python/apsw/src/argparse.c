typedef struct
{
    PyObject **result;
    const char *message;
} argcheck_Optional_Callable_param;

static int
argcheck_Optional_Callable(PyObject *object, void *vparam)
{
    argcheck_Optional_Callable_param *param = (argcheck_Optional_Callable_param *)vparam;
    if (object == Py_None)
        *param->result = NULL;
    else if (PyCallable_Check(object))
        *param->result = object;
    else
    {
        PyErr_Format(PyExc_TypeError, "Function argument expected a Callable or None: %s", param->message);
        return 0;
    }
    return 1;
}

/* Standard PyArg_Parse considers anything truthy to be True such as
   non-empty strings, tuples etc.  This is a footgun for args eg:

      method("False")  # considered to be method(True)

   This converter only accepts bool / int (or subclasses)
*/
typedef struct
{
    int *result;
    const char *message;
} argcheck_bool_param;

static int
argcheck_bool(PyObject *object, void *vparam)
{
    argcheck_bool_param *param = (argcheck_bool_param *)vparam;

    int val;

    if (!PyBool_Check(object) && !PyLong_Check(object))
    {
        PyErr_Format(PyExc_TypeError, "Function argument expected a bool: %s", param->message);
        return 0;
    }

    val = PyObject_IsTrue(object);
    switch (val)
    {
    case 0:
    case 1:
        *param->result = val;
        return 1;
    default:
        return 0;
    }
}

typedef struct
{
    PyObject **result;
    const char *message;
} argcheck_List_int_int_param;

/* Doing this here avoids cleanup in the calling function */
static int
argcheck_List_int_int(PyObject *object, void *vparam)
{
    int i;
    argcheck_List_int_int_param *param = (argcheck_List_int_int_param *)vparam;

    if (!PyList_Check(object))
    {
        PyErr_Format(PyExc_TypeError, "Function argument expected a list: %s", param->message);
        return 0;
    }

    if (PySequence_Length(object) != 2)
    {
        PyErr_Format(PyExc_ValueError, "Function argument expected a two item list: %s", param->message);
        return 0;
    }

    for (i = 0; i < 2; i++)
    {
        int check;
        PyObject *list_item = PySequence_GetItem(object, i);
        if (!list_item)
            return 0;
        check = PyLong_Check(list_item);
        Py_DECREF(list_item);
        if (!check)
        {
            PyErr_Format(PyExc_TypeError, "Function argument list[int,int] expected int for item %d: %s", i, param->message);
            return 0;
        }
    }
    *param->result = object;
    return 1;
}

static PyTypeObject APSWURIFilenameType;

typedef struct
{
    PyObject **result;
    const char *message;
} argcheck_Optional_str_URIFilename_param;

static int
argcheck_Optional_str_URIFilename(PyObject *object, void *vparam)
{
    argcheck_Optional_str_URIFilename_param *param = (argcheck_Optional_str_URIFilename_param *)vparam;

    if (object == Py_None || PyUnicode_Check(object) || PyObject_IsInstance(object, (PyObject *)&APSWURIFilenameType))
    {
        *param->result = object;
        return 1;
    }
    PyErr_Format(PyExc_TypeError, "Function argument expect None | str | apsw.URIFilename: %s", param->message);
    return 0;
}

typedef struct
{
    void **result;
    const char *message;
} argcheck_pointer_param;

static int
argcheck_pointer(PyObject *object, void *vparam)
{
    argcheck_pointer_param *param = (argcheck_pointer_param *)vparam;
    if (!PyLong_Check(object))
    {
        PyErr_Format(PyExc_TypeError, "Function argument expected int (to be used as a pointer): %s", param->message);
        return 0;
    }
    *param->result = PyLong_AsVoidPtr(object);
    return PyErr_Occurred() ? 0 : 1;
}

typedef struct
{
    PyObject **result;
    const char *message;
} argcheck_Optional_Bindings_param;

static int
argcheck_Optional_Bindings(PyObject *object, void *vparam)
{
    argcheck_Optional_Bindings_param *param = (argcheck_Optional_Bindings_param *)vparam;
    if (object == Py_None)
    {
        *param->result = NULL;
        return 1;
    }
    /* PySequence_Check is too strict and rejects things that are
        accepted by PySequence_Fast like sets and generators,
        so everything is accepted */
    *param->result = object;
    return 1;
}