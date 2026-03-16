
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
// documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
// OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#include "pyodbc.h"
#include "pyodbcmodule.h"
#include "row.h"
#include "wrapper.h"

struct Row
{
    // A Row must act like a sequence (a tuple of results) to meet the DB API specification, but we also allow values
    // to be accessed via lowercased column names.  We also supply a `columns` attribute which returns the list of
    // column names.

    PyObject_HEAD

    // cursor.description, accessed as _description
    PyObject* description;

    // A Python dictionary mapping from column name to a PyInteger, used to access columns by name.
    PyObject* map_name_to_index;

    // The number of values in apValues.
    Py_ssize_t cValues;
    // The column values, stored as an array.
    PyObject** apValues;
};

#define Row_Check(op) PyObject_TypeCheck(op, &RowType)
#define Row_CheckExact(op) (Py_TYPE(op) == &RowType)

void FreeRowValues(Py_ssize_t cValues, PyObject** apValues)
{
    // Frees each pointer in the apValues buffer *and* the buffer itself.

    if (apValues)
    {
        for (Py_ssize_t i = 0; i < cValues; i++)
            Py_XDECREF(apValues[i]);
        PyMem_Free(apValues);
    }
}

static void Row_dealloc(PyObject* o)
{
    // Note: Now that __newobj__ is available, our variables could be zero...

    Row* self = (Row*)o;

    Py_XDECREF(self->description);
    Py_XDECREF(self->map_name_to_index);
    FreeRowValues(self->cValues, self->apValues);
    PyObject_Del(self);
}

static PyObject* Row_getstate(PyObject* self)
{
    // Returns a tuple containing the saved state.  We don't really support empty rows, but unfortunately they can be
    // created now by the new constructor which was necessary for implementing pickling.  In that case (everything is
    // zero), an empty tuple is returned.

    // Not exposed.

    Row* row = (Row*)self;

    if (row->description == 0)
        return PyTuple_New(0);

    Object state(PyTuple_New(2 + row->cValues));
    if (!state.IsValid())
        return 0;

    PyTuple_SET_ITEM(state, 0, row->description);
    PyTuple_SET_ITEM(state, 1, row->map_name_to_index);
    for (int i = 0; i < row->cValues; i++)
      PyTuple_SET_ITEM(state, i+2, row->apValues[i]);

    for (int i = 0; i < PyTuple_GET_SIZE(state); i++)
      Py_XINCREF(PyTuple_GET_ITEM(state, i));

    return state.Detach();
}


static PyObject* new_check(PyObject* args)
{
    // We don't support a normal constructor, so only allow this for unpickling.  There should be a single arg that was
    // returned by Row_reduce.  Make sure the sizes match.  The desc and map should have one entry per column, which
    // should equal the number of remaining items.

    if (PyTuple_GET_SIZE(args) < 3)
        return 0;

    PyObject* desc = PyTuple_GET_ITEM(args, 0);
    PyObject* map  = PyTuple_GET_ITEM(args, 1);

    if (!PyTuple_CheckExact(desc) || !PyDict_CheckExact(map))
        return 0;

    Py_ssize_t cols = PyTuple_GET_SIZE(desc);

    if (PyDict_Size(map) != cols || PyTuple_GET_SIZE(args) - 2 != cols)
        return 0;

    PyObject** apValues = (PyObject**)PyMem_Malloc(sizeof(PyObject*) * cols);
    if (!apValues)
        return 0;

    for (int i = 0; i < cols; i++)
    {
        apValues[i] = PyTuple_GET_ITEM(args, i+2);
        Py_INCREF(apValues[i]);
    }

    // Row_Internal will incref desc and map.  If something goes wrong, it will free apValues.

    return (PyObject*)Row_InternalNew(desc, map, cols, apValues);
}

static PyObject* Row_new(PyTypeObject* type, PyObject* args, PyObject* kwargs)
{
    UNUSED(kwargs);

    PyObject* row = new_check(args);
    if (row == 0)
        PyErr_SetString(PyExc_TypeError, "cannot create 'pyodbc.Row' instances");
    return row;

}

Row* Row_InternalNew(PyObject* description, PyObject* map_name_to_index, Py_ssize_t cValues, PyObject** apValues)
{
    // Called by other modules to create rows.  Takes ownership of apValues.

#ifdef _MSC_VER
#pragma warning(disable : 4365)
#endif
    Row* row = PyObject_NEW(Row, &RowType);
#ifdef _MSC_VER
#pragma warning(default : 4365)
#endif

    if (row)
    {
        Py_INCREF(description);
        row->description = description;
        Py_INCREF(map_name_to_index);
        row->map_name_to_index = map_name_to_index;
        row->apValues          = apValues;
        row->cValues           = cValues;
    }
    else
    {
        FreeRowValues(cValues, apValues);
    }

    return row;
}


static PyObject* Row_getattro(PyObject* o, PyObject* name)
{
    // Called to handle 'row.colname'.

    Row* self = (Row*)o;

    PyObject* index = PyDict_GetItem(self->map_name_to_index, name);

    if (index)
    {
        Py_ssize_t i = PyNumber_AsSsize_t(index, 0);
        Py_INCREF(self->apValues[i]);
        return self->apValues[i];
    }

    return PyObject_GenericGetAttr(o, name);
}


static Py_ssize_t Row_length(PyObject* self)
{
    return ((Row*)self)->cValues;
}


static int Row_contains(PyObject* o, PyObject* el)
{
    // Implementation of contains.  The documentation is not good (non-existent?), so I copied the following from the
    // PySequence_Contains documentation: Return -1 if error; 1 if ob in seq; 0 if ob not in seq.

    Row* self = (Row*)o;

    int cmp = 0;

    for (Py_ssize_t i = 0, c = self->cValues ; cmp == 0 && i < c; ++i)
        cmp = PyObject_RichCompareBool(el, self->apValues[i], Py_EQ);

    return cmp;
}

PyObject* Row_item(PyObject* o, Py_ssize_t i)
{
    // Apparently, negative indexes are handled by magic ;) -- they never make it here.

    Row* self = (Row*)o;

    if (i < 0 || i >= self->cValues)
    {
        PyErr_SetString(PyExc_IndexError, "tuple index out of range");
        return NULL;
    }

    Py_INCREF(self->apValues[i]);
    return self->apValues[i];
}


static int Row_ass_item(PyObject* o, Py_ssize_t i, PyObject* v)
{
    // Implements row[i] = value.

    Row* self = (Row*)o;

    if (i < 0 || i >= self->cValues)
    {
        PyErr_SetString(PyExc_IndexError, "Row assignment index out of range");
        return -1;
    }

    Py_XDECREF(self->apValues[i]);
    Py_INCREF(v);
    self->apValues[i] = v;

    return 0;
}


static int Row_setattro(PyObject* o, PyObject *name, PyObject* v)
{
    Row* self = (Row*)o;

    PyObject* index = PyDict_GetItem(self->map_name_to_index, name);

    if (index)
        return Row_ass_item(o, PyNumber_AsSsize_t(index, 0), v);

    return PyObject_GenericSetAttr(o, name, v);
}


static PyObject* Row_repr(PyObject* o)
{
    // We want to return the same representation as a tuple.  The easiest way is to create a
    // temporary tuple.  I do not consider this something normally used in high performance
    // areas.

    Row* self = (Row*)o;

    Object t(PyTuple_New(self->cValues));
    if (!t)
      return 0;

    for (Py_ssize_t i = 0; i < self->cValues; i++) {
        Py_INCREF(self->apValues[i]);
        PyTuple_SET_ITEM(t.Get(), i, self->apValues[i]);
    }

    return PyObject_Repr(t);
}

static PyObject* Row_richcompare(PyObject* olhs, PyObject* orhs, int op)
{
    if (!Row_Check(olhs) || !Row_Check(orhs))
    {
        Py_INCREF(Py_NotImplemented);
        return Py_NotImplemented;
    }

    Row* lhs = (Row*)olhs;
    Row* rhs = (Row*)orhs;

    if (lhs->cValues != rhs->cValues)
    {
        // Different sizes, so use the same rules as the tuple class.
        bool result;
        switch (op)
        {
        case Py_EQ: result = (lhs->cValues == rhs->cValues); break;
        case Py_GE: result = (lhs->cValues >= rhs->cValues); break;
        case Py_GT: result = (lhs->cValues >  rhs->cValues); break;
        case Py_LE: result = (lhs->cValues <= rhs->cValues); break;
        case Py_LT: result = (lhs->cValues <  rhs->cValues); break;
        case Py_NE: result = (lhs->cValues != rhs->cValues); break;
        default:
            // Can't get here, but don't have a cross-compiler way to silence this.
            result = false;
        }
        PyObject* p = result ? Py_True : Py_False;
        Py_INCREF(p);
        return p;
    }

    for (Py_ssize_t i = 0, c = lhs->cValues; i < c; i++)
        if (!PyObject_RichCompareBool(lhs->apValues[i], rhs->apValues[i], Py_EQ))
            return PyObject_RichCompare(lhs->apValues[i], rhs->apValues[i], op);

    // All items are equal.
    switch (op)
    {
    case Py_EQ:
    case Py_GE:
    case Py_LE:
        Py_RETURN_TRUE;

    case Py_GT:
    case Py_LT:
    case Py_NE:
        break;
    }

    Py_RETURN_FALSE;
}


static PyObject* Row_subscript(PyObject* o, PyObject* key)
{
    Row* row = (Row*)o;

    if (PyIndex_Check(key))
    {
        Py_ssize_t i = PyNumber_AsSsize_t(key, PyExc_IndexError);
        if (i == -1 && PyErr_Occurred())
            return 0;
        if (i < 0)
            i += row->cValues;

        if (i < 0 || i >= row->cValues)
            return PyErr_Format(PyExc_IndexError, "row index out of range index=%d len=%d", (int)i, (int)row->cValues);

        Py_INCREF(row->apValues[i]);
        return row->apValues[i];
    }

    if (PySlice_Check(key))
    {
        Py_ssize_t start, stop, step, slicelength;
        if (PySlice_GetIndicesEx(key, row->cValues, &start, &stop, &step, &slicelength) < 0)
            return 0;

        if (slicelength <= 0)
            return PyTuple_New(0);

        if (start == 0 && step == 1 && slicelength == row->cValues)
        {
            Py_INCREF(o);
            return o;
        }

        Object result(PyTuple_New(slicelength));
        if (!result)
            return 0;
        for (Py_ssize_t i = 0, index = start; i < slicelength; i++, index += step)
        {
            PyTuple_SET_ITEM(result.Get(), i, row->apValues[index]);
            Py_INCREF(row->apValues[index]);
        }
        return result.Detach();
    }

    return PyErr_Format(PyExc_TypeError, "row indices must be integers, not %.200s", Py_TYPE(key)->tp_name);
}


static PySequenceMethods row_as_sequence =
{
    Row_length,                 // sq_length
    0,                          // sq_concat
    0,                          // sq_repeat
    Row_item,                   // sq_item
    0,                          // was_sq_slice
    Row_ass_item,               // sq_ass_item
    0,                          // sq_ass_slice
    Row_contains,               // sq_contains
};


static PyMappingMethods row_as_mapping =
{
    Row_length,                 // mp_length
    Row_subscript,              // mp_subscript
    0,                          // mp_ass_subscript
};


static char description_doc[] = "The Cursor.description sequence from the Cursor that created this row.";

static PyMemberDef Row_members[] =
{
    { "cursor_description", T_OBJECT_EX, offsetof(Row, description), READONLY, description_doc },
    { 0 }
};

static PyObject* Row_reduce(PyObject* self, PyObject* args)
{
    PyObject* state = Row_getstate(self);
    if (!state)
        return 0;

    return Py_BuildValue("ON", Py_TYPE(self), state);
}


static PyMethodDef Row_methods[] =
{
    { "__reduce__", (PyCFunction)Row_reduce, METH_NOARGS, 0 },
    { 0, 0, 0, 0 }
};


static char row_doc[] =
    "Row objects are sequence objects that hold query results.\n"
    "\n"
    "They are similar to tuples in that they cannot be resized and new attributes\n"
    "cannot be added, but individual elements can be replaced.  This allows data to\n"
    "be \"fixed up\" after being fetched.  (For example, datetimes may be replaced by\n"
    "those with time zones attached.)\n"
    "\n"
    "  row[0] = row[0].replace(tzinfo=timezone)\n"
    "  print row[0]\n"
    "\n"
    "Additionally, individual values can be optionally be accessed or replaced by\n"
    "name.  Non-alphanumeric characters are replaced with an underscore.\n"
    "\n"
    "  cursor.execute(\"select customer_id, [Name With Spaces] from tmp\")\n"
    "  row = cursor.fetchone()\n"
    "  print row.customer_id, row.Name_With_Spaces\n"
    "\n"
    "If using this non-standard feature, it is often convenient to specify the name\n"
    "using the SQL 'as' keyword:\n"
    "\n"
    "  cursor.execute(\"select count(*) as total from tmp\")\n"
    "  row = cursor.fetchone()\n"
    "  print row.total";

PyTypeObject RowType =
{
    PyVarObject_HEAD_INIT(NULL, 0)
    "pyodbc.Row",                                           // tp_name
    sizeof(Row),                                            // tp_basicsize
    0,                                                      // tp_itemsize
    Row_dealloc,                                            // tp_dealloc
    0,                                                      // tp_print
    0,                                                      // tp_getattr
    0,                                                      // tp_setattr
    0,                                                      // tp_compare
    Row_repr,                                               // tp_repr
    0,                                                      // tp_as_number
    &row_as_sequence,                                       // tp_as_sequence
    &row_as_mapping,                                        // tp_as_mapping
    0,                                                      // tp_hash
    0,                                                      // tp_call
    0,                                                      // tp_str
    Row_getattro,                                           // tp_getattro
    Row_setattro,                                           // tp_setattro
    0,                                                      // tp_as_buffer
    Py_TPFLAGS_DEFAULT,                                     // tp_flags
    row_doc,                                                // tp_doc
    0,                                                      // tp_traverse
    0,                                                      // tp_clear
    Row_richcompare,                                        // tp_richcompare
    0,                                                      // tp_weaklistoffset
    0,                                                      // tp_iter
    0,                                                      // tp_iternext
    Row_methods,                                            // tp_methods
    Row_members,                                            // tp_members
    0,                                                      // tp_getset
    0,                                                      // tp_base
    0,                                                      // tp_dict
    0,                                                      // tp_descr_get
    0,                                                      // tp_descr_set
    0,                                                      // tp_dictoffset
    0,                                                      // tp_init
    0,                                                      // tp_alloc
    Row_new,                                                // tp_new
    0,                                                      // tp_free
    0,                                                      // tp_is_gc
    0,                                                      // tp_bases
    0,                                                      // tp_mro
    0,                                                      // tp_cache
    0,                                                      // tp_subclasses
    0,                                                      // tp_weaklist
};
