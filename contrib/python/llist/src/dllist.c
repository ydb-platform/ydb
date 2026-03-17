/* Copyright (c) 2011-2018 Adam Jakubek, Rafał Gałczyński
 * Released under the MIT license (see attached LICENSE file).
 */

#include <Python.h>
#include <structmember.h>

#include "config.h"
#include "flags.h"
#include "py23macros.h"
#include "utils.h"

#ifndef PyVarObject_HEAD_INIT
    #define PyVarObject_HEAD_INIT(type, size) \
        PyObject_HEAD_INIT(type) size,
#endif


static PyTypeObject DLListType;
static PyTypeObject DLListNodeType;
static PyTypeObject DLListIteratorType;
static PyTypeObject DLListNodeIteratorType;


/* DLListNode */

typedef struct
{
    PyObject_HEAD
    PyObject* value;
    PyObject* prev;
    PyObject* next;
    PyObject* list_weakref;
    unsigned char flags;
} DLListNodeObject;

/* Convenience function for linking list nodes.
 * Automatically updates pointers in inserted node and its neighbours.
 */
static void dllistnode_link(PyObject* prev,
                            PyObject* next,
                            DLListNodeObject* inserted,
                            PyObject* owner_list)
{
    assert(inserted != NULL);
    assert(inserted->prev == Py_None);
    assert(inserted->next == Py_None);
    assert(owner_list != NULL);
    assert(owner_list != Py_None);

    /* prev is initialized to Py_None by default (by dllistnode_new) */
    if (prev != NULL && prev != Py_None)
    {
        inserted->prev = prev;
        ((DLListNodeObject*)prev)->next = (PyObject*)inserted;
    }

    /* next is initialized to Py_None by default (by dllistnode_new) */
    if (next != NULL && next != Py_None)
    {
        inserted->next = next;
        ((DLListNodeObject*)next)->prev = (PyObject*)inserted;
    }

    Py_DECREF(inserted->list_weakref);
    inserted->list_weakref = PyWeakref_NewRef(owner_list, NULL);
}

/* Convenience function for creating list nodes.
 * Automatically update pointers in neighbours.
 */
static DLListNodeObject* dllistnode_create(PyObject* prev,
                                           PyObject* next,
                                           PyObject* value,
                                           PyObject* owner_list)
{
    DLListNodeObject *node;
    PyObject* args = NULL;

    assert(value != NULL);

    if (value != Py_None)
    {
        args = PyTuple_New(1);
        if (args == NULL)
        {
            PyErr_SetString(PyExc_RuntimeError,
                "Failed to create argument tuple");
            return NULL;
        }

        Py_INCREF(value);
        if (PyTuple_SetItem(args, 0, value) != 0)
        {
            PyErr_SetString(PyExc_RuntimeError,
                "Failed to initialize argument tuple");
            return NULL;
        }
    }

    node = (DLListNodeObject*)PyObject_CallObject(
        (PyObject*)&DLListNodeType, args);

    Py_XDECREF(args);

    dllistnode_link(prev, next, node, owner_list);

    return node;
}

/* Convenience function for deleting list nodes.
 * Clears neighbour and owner references and decrefs the node.
 */
static void dllistnode_delete(DLListNodeObject* node)
{
    if (node->prev != Py_None)
    {
        DLListNodeObject* prev = (DLListNodeObject*)node->prev;
        prev->next = node->next;
    }

    if (node->next != Py_None)
    {
        DLListNodeObject* next = (DLListNodeObject*)node->next;
        next->prev = node->prev;
    }

    node->prev = Py_None;
    node->next = Py_None;

    Py_XDECREF(node->list_weakref);
    Py_INCREF(Py_None);
    node->list_weakref = Py_None;

    Py_DECREF((PyObject*)node);
}

/* Convenience function for formatting list node to a string.
 * Pass PyObject_Repr or PyObject_Str in the fmt_func argument. */
static PyObject* dllistnode_to_string(DLListNodeObject* self,
                                      reprfunc fmt_func,
                                      const char* prefix,
                                      const char* suffix)
{
    PyObject* str = NULL;
    PyObject* tmp_str;

    assert(fmt_func != NULL);

    if (Py_ReprEnter((PyObject*)self) > 0)
        return Py23String_FromString("dllistnode(<...>)");

    str = Py23String_FromString(prefix);
    if (str == NULL)
        goto str_alloc_error;

    tmp_str = fmt_func(self->value);
    if (tmp_str == NULL)
        goto str_alloc_error;
    Py23String_ConcatAndDel(&str, tmp_str);

    tmp_str = Py23String_FromString(suffix);
    if (tmp_str == NULL)
        goto str_alloc_error;
    Py23String_ConcatAndDel(&str, tmp_str);

    Py_ReprLeave((PyObject*)self);

    return str;

str_alloc_error:
    Py_XDECREF(str);
    PyErr_SetString(PyExc_RuntimeError, "Failed to create string");

    Py_ReprLeave((PyObject*)self);

    return NULL;
}

static int dllistnode_traverse(DLListNodeObject* self,
                               visitproc visit,
                               void* arg)
{
    Py_VISIT(self->value);
    Py_VISIT(self->list_weakref);

    return 0;
}

static int dllistnode_clear_refs(DLListNodeObject* self)
{
    Py_CLEAR(self->value);
    Py_CLEAR(self->list_weakref);

    if ((self->flags & LLIST_HAS_PY_NONE_REF) != 0)
    {
        self->flags &= ~LLIST_HAS_PY_NONE_REF;
        Py_DECREF(Py_None);
    }

    return 0;
}

static void dllistnode_dealloc(DLListNodeObject* self)
{
    PyObject_GC_UnTrack(self);
    PyObject* obj_self = (PyObject*)self;

    dllistnode_clear_refs(self);

    obj_self->ob_type->tp_free(obj_self);
}

static PyObject* dllistnode_new(PyTypeObject* type,
                                PyObject* args,
                                PyObject* kwds)
{
    DLListNodeObject* self;

    self = (DLListNodeObject*)type->tp_alloc(type, 0);
    if (self == NULL)
        return NULL;

    /* A single reference to Py_None is held for the whole
     * lifetime of a node. */
    Py_INCREF(Py_None);

    self->value = Py_None;
    self->prev = Py_None;
    self->next = Py_None;
    self->list_weakref = Py_None;
    self->flags = LLIST_HAS_PY_NONE_REF;

    Py_INCREF(self->value);
    Py_INCREF(self->list_weakref);

    return (PyObject*)self;
}

static int dllistnode_init(DLListNodeObject* self,
                           PyObject* args,
                           PyObject* kwds)
{
    PyObject* value = NULL;

    if (!PyArg_UnpackTuple(args, "__init__", 0, 1, &value))
        return -1;

    if (value == NULL)
        return 0;

    /* initialize node using passed value */
    Py_DECREF(self->value);
    Py_INCREF(value);
    self->value = value;

    return 0;
}

static PyObject* dllistnode_call(DLListNodeObject* self,
                                 PyObject* args,
                                 PyObject* kw)
{
    Py_INCREF(self->value);
    return self->value;
}

static PyObject* dllistnode_repr(DLListNodeObject* self)
{
    return dllistnode_to_string(self, PyObject_Repr, "<dllistnode(", ")>");
}

static PyObject* dllistnode_str(DLListNodeObject* self)
{
    return dllistnode_to_string(self, PyObject_Str, "dllistnode(", ")");
}

static PyMemberDef DLListNodeMembers[] =
{
    { "value", T_OBJECT_EX, offsetof(DLListNodeObject, value), 0,
      "Value stored in node" },
    { "prev", T_OBJECT_EX, offsetof(DLListNodeObject, prev), READONLY,
      "Previous node" },
    { "next", T_OBJECT_EX, offsetof(DLListNodeObject, next), READONLY,
      "Next node" },
    { "owner", T_OBJECT_EX, offsetof(DLListNodeObject, list_weakref), READONLY,
      "List that this node belongs to" },
    { NULL },   /* sentinel */
};

static PyTypeObject DLListNodeType =
{
    PyVarObject_HEAD_INIT(NULL, 0)
    "llist.dllistnode",             /* tp_name */
    sizeof(DLListNodeObject),       /* tp_basicsize */
    0,                              /* tp_itemsize */
    (destructor)dllistnode_dealloc, /* tp_dealloc */
    0,                              /* tp_print */
    0,                              /* tp_getattr */
    0,                              /* tp_setattr */
    0,                              /* tp_compare */
    (reprfunc)dllistnode_repr,      /* tp_repr */
    0,                              /* tp_as_number */
    0,                              /* tp_as_sequence */
    0,                              /* tp_as_mapping */
    0,                              /* tp_hash */
    (ternaryfunc)dllistnode_call,   /* tp_call */
    (reprfunc)dllistnode_str,       /* tp_str */
    0,                              /* tp_getattro */
    0,                              /* tp_setattro */
    0,                              /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_GC,
                                    /* tp_flags */
    "Doubly linked list node",      /* tp_doc */
    (traverseproc)dllistnode_traverse,
                                    /* tp_traverse */
    (inquiry)dllistnode_clear_refs, /* tp_clear */
    0,                              /* tp_richcompare */
    0,                              /* tp_weaklistoffset */
    0,                              /* tp_iter */
    0,                              /* tp_iternext */
    0,                              /* tp_methods */
    DLListNodeMembers,              /* tp_members */
    0,                              /* tp_getset */
    0,                              /* tp_base */
    0,                              /* tp_dict */
    0,                              /* tp_descr_get */
    0,                              /* tp_descr_set */
    0,                              /* tp_dictoffset */
    (initproc)dllistnode_init,      /* tp_init */
    0,                              /* tp_alloc */
    dllistnode_new,                 /* tp_new */
};


/* DLList */

typedef struct
{
    PyObject_HEAD
    PyObject* first;
    PyObject* last;
    PyObject* last_accessed_node;
    Py_ssize_t last_accessed_idx;
    Py_ssize_t size;
    PyObject* weakref_list;
    unsigned char flags;
} DLListObject;

static Py_ssize_t py_ssize_t_abs(Py_ssize_t x)
{
    return (x >= 0) ? x : -x;
}

/* Convenience function for locating list nodes using index. */
static DLListNodeObject* dllist_get_node_internal(DLListObject* self,
                                                  Py_ssize_t index)
{
    Py_ssize_t i;
    Py_ssize_t middle = self->size / 2;
    DLListNodeObject* node;
    Py_ssize_t start_pos;
    int reverse_dir;

    if (index >= self->size || index < 0)
    {
        PyErr_SetString(PyExc_IndexError, "Index out of range");
        return NULL;
    }

    /* pick the closest base node */
    if (index <= middle)
    {
        node = (DLListNodeObject*)self->first;
        start_pos = 0;
        reverse_dir = 0;
    }
    else
    {
        node = (DLListNodeObject*)self->last;
        start_pos = self->size - 1;
        reverse_dir = 1;
    }

    /* check if last accessed index is closer */
    if (self->last_accessed_node != Py_None &&
        self->last_accessed_idx >= 0 &&
        py_ssize_t_abs(index - self->last_accessed_idx) < middle)
    {
        node = (DLListNodeObject*)self->last_accessed_node;
        start_pos = self->last_accessed_idx;
        reverse_dir = (index < self->last_accessed_idx) ? 1 : 0;
    }

    assert((PyObject*)node != Py_None);

    if (!reverse_dir)
    {
        /* forward scan */
        for (i = start_pos; i < index; ++i)
            node = (DLListNodeObject*)node->next;
    }
    else
    {
        /* backward scan */
        for (i = start_pos; i > index; --i)
            node = (DLListNodeObject*)node->prev;
    }

    return node;
}

/* Convenience function for extending (concatenating in-place)
 * the list with elements from a sequence. */
static int dllist_extend_internal(DLListObject* self, PyObject* sequence)
{
    Py_ssize_t i;
    Py_ssize_t sequence_len;

    if (PyObject_TypeCheck(sequence, &DLListType))
    {
        /* Special path for extending with a DLList.
         * It's not strictly required but it will maintain
         * the last accessed item. */
        PyObject* iter_node_obj = ((DLListObject*)sequence)->first;
        PyObject* last_node_obj = self->last;

        while (iter_node_obj != Py_None)
        {
            DLListNodeObject* iter_node = (DLListNodeObject*)iter_node_obj;
            PyObject* new_node;

            new_node = (PyObject*)dllistnode_create(
                self->last, NULL, iter_node->value, (PyObject*)self);

            if (self->first == Py_None)
                self->first = new_node;
            self->last = new_node;

            if (iter_node_obj == last_node_obj)
            {
                /* This is needed to terminate loop if self == sequence. */
                break;
            }

            iter_node_obj = iter_node->next;
        }

        self->size += ((DLListObject*)sequence)->size;

        return 1;
    }

    sequence_len = PySequence_Length(sequence);
    if (sequence_len == -1)
    {
        PyErr_SetString(PyExc_ValueError, "Invalid sequence");
        return 0;
    }

    for (i = 0; i < sequence_len; ++i)
    {
        PyObject* item;
        PyObject* new_node;

        item = PySequence_GetItem(sequence, i);
        if (item == NULL)
        {
            PyErr_SetString(PyExc_ValueError,
                "Failed to get element from sequence");
            return 0;
        }

        new_node = (PyObject*)dllistnode_create(
            self->last, NULL, item, (PyObject*)self);

        if (self->first == Py_None)
            self->first = new_node;
        self->last = new_node;

        ++self->size;

        Py_DECREF(item);
    }

    return 1;
}

/* Convenience function for formatting list to a string.
 * Pass PyObject_Repr or PyObject_Str in the fmt_func argument. */
static PyObject* dllist_to_string(DLListObject* self,
                                  reprfunc fmt_func)
{
    PyObject* str = NULL;
    PyObject* comma_str = NULL;
    PyObject* tmp_str;
    DLListNodeObject* node = (DLListNodeObject*)self->first;

    assert(fmt_func != NULL);

    if (Py_ReprEnter((PyObject*)self) > 0)
        return Py23String_FromString("dllist(<...>)");

    if (self->first == Py_None)
    {
        str = Py23String_FromString("dllist()");
        if (str == NULL)
            goto str_alloc_error;
        return str;
    }

    str = Py23String_FromString("dllist([");
    if (str == NULL)
        goto str_alloc_error;

    comma_str = Py23String_FromString(", ");
    if (comma_str == NULL)
        goto str_alloc_error;

    while ((PyObject*)node != Py_None)
    {
        if (node != (DLListNodeObject*)self->first)
            Py23String_Concat(&str, comma_str);

        tmp_str = fmt_func(node->value);
        if (tmp_str == NULL)
            goto str_alloc_error;
        Py23String_ConcatAndDel(&str, tmp_str);

        node = (DLListNodeObject*)node->next;
    }

    Py_DECREF(comma_str);
    comma_str = NULL;

    tmp_str = Py23String_FromString("])");
    if (tmp_str == NULL)
        goto str_alloc_error;
    Py23String_ConcatAndDel(&str, tmp_str);

    Py_ReprLeave((PyObject*)self);

    return str;

str_alloc_error:
    Py_XDECREF(str);
    Py_XDECREF(comma_str);
    PyErr_SetString(PyExc_RuntimeError, "Failed to create string");

    Py_ReprLeave((PyObject*)self);

    return NULL;
}

static int dllist_traverse(DLListObject* self, visitproc visit, void* arg)
{
    PyObject* node = self->first;

    if (node != NULL)
    {
        while (node != Py_None)
        {
            PyObject* next_node = ((DLListNodeObject*)node)->next;
            Py_VISIT(node);
            node = next_node;
        }
    }

    return 0;
}

static void dllist_invalidate_last_access_cache(DLListObject* self)
{
    self->last_accessed_node = Py_None;
    self->last_accessed_idx = -1;
}

static int dllist_clear_refs(DLListObject* self)
{
    PyObject* node = self->first;

    if (self->weakref_list != NULL)
        PyObject_ClearWeakRefs((PyObject*)self);

    self->first = NULL;
    self->last = NULL;
    self->weakref_list = NULL;

    if (node != NULL)
    {
        while (node != Py_None)
        {
            PyObject* next_node = ((DLListNodeObject*)node)->next;
            dllistnode_delete((DLListNodeObject*)node);
            node = next_node;
        }
    }

    if ((self->flags & LLIST_HAS_PY_NONE_REF) != 0)
    {
        self->flags &= ~LLIST_HAS_PY_NONE_REF;
        Py_DECREF(Py_None);
    }

    return 0;
}

static void dllist_dealloc(DLListObject* self)
{
    PyObject_GC_UnTrack(self);
    PyObject* obj_self = (PyObject*)self;

    dllist_clear_refs(self);

    obj_self->ob_type->tp_free(obj_self);
}

static PyObject* dllist_new(PyTypeObject* type,
                            PyObject* args,
                            PyObject* kwds)
{
    DLListObject* self;

    self = (DLListObject*)type->tp_alloc(type, 0);
    if (self == NULL)
        return NULL;

    /* A single reference to Py_None is held for the whole
     * lifetime of a list. */
    Py_INCREF(Py_None);

    self->first = Py_None;
    self->last = Py_None;
    self->last_accessed_node = Py_None;
    self->last_accessed_idx = -1;
    self->size = 0;
    self->weakref_list = NULL;
    self->flags = LLIST_HAS_PY_NONE_REF;

    return (PyObject*)self;
}

static int dllist_init(DLListObject* self, PyObject* args, PyObject* kwds)
{
    PyObject* sequence = NULL;

    if (!PyArg_UnpackTuple(args, "__init__", 0, 1, &sequence))
        return -1;

    if (sequence == NULL)
        return 0;

    /* initialize list using passed sequence */
    if (!PySequence_Check(sequence))
    {
        PyErr_SetString(PyExc_TypeError, "Argument must be a sequence");
        return -1;
    }

    return dllist_extend_internal(self, sequence) ? 0 : -1;
}

static PyObject* dllist_node_at(PyObject* self, PyObject* indexObject)
{
    DLListNodeObject* node;
    Py_ssize_t index;

    if (!Py23Int_Check(indexObject))
    {
        PyErr_SetString(PyExc_TypeError, "Index must be an integer");
        return NULL;
    }

    index = Py23Int_AsSsize_t(indexObject);

    if (index < 0)
        index = ((DLListObject*)self)->size + index;

    node = dllist_get_node_internal((DLListObject*)self, index);
    if (node != NULL)
    {
        /* update last accessed node */
        ((DLListObject*)self)->last_accessed_node = (PyObject*)node;
        ((DLListObject*)self)->last_accessed_idx = index;

        Py_INCREF(node);
    }

    return (PyObject*)node;
}

static PyObject* dllist_repr(DLListObject* self)
{
    return dllist_to_string(self, PyObject_Repr);
}

static PyObject* dllist_str(DLListObject* self)
{
    return dllist_to_string(self, PyObject_Str);
}

static long dllist_hash(DLListObject* self)
{
    long hash = 0;
    PyObject* iter_node_obj = self->first;

    while (iter_node_obj != Py_None)
    {
        long obj_hash;
        DLListNodeObject* iter_node = (DLListNodeObject*)iter_node_obj;

        obj_hash = PyObject_Hash(iter_node->value);
        if (obj_hash == -1)
            return -1;

        hash = hash_combine(hash, obj_hash);
        iter_node_obj = iter_node->next;
    }

    return hash;
}

static PyObject* dllist_richcompare(DLListObject* self,
                                    DLListObject* other,
                                    int op)
{
    DLListNodeObject* self_node;
    DLListNodeObject* other_node;
    int satisfied = 1;

    if (!PyObject_TypeCheck(other, &DLListType))
    {
        Py_INCREF(Py_NotImplemented);
        return Py_NotImplemented;
    }

    if (self == other &&
        (op == Py_EQ || op == Py_LE || op == Py_GE))
        Py_RETURN_TRUE;

    if (self->size != other->size)
    {
        if (op == Py_EQ)
            Py_RETURN_FALSE;
        else if (op == Py_NE)
            Py_RETURN_TRUE;
    }

    /* Scan through sequences' items as long as they are equal. */
    self_node = (DLListNodeObject*)self->first;
    other_node = (DLListNodeObject*)other->first;

    while ((PyObject*)self_node != Py_None &&
            (PyObject*)other_node != Py_None)
    {
        satisfied = PyObject_RichCompareBool(
            self_node->value, other_node->value, Py_EQ);

        if (satisfied == 0)
            break;

        if (satisfied == -1)
            return NULL;

        self_node = (DLListNodeObject*)self_node->next;
        other_node = (DLListNodeObject*)other_node->next;
    }

    /* Compare last item */
    if (satisfied)
    {
        /* At least one of operands has been fully traversed.
         * Either self_node or other_node is equal to Py_None. */
        switch (op)
        {
        case Py_EQ:
            satisfied = (self_node == other_node);
            break;
        case Py_NE:
            satisfied = (self_node != other_node);
            break;
        case Py_LT:
            satisfied = ((PyObject*)other_node != Py_None);
            break;
        case Py_GT:
            satisfied = ((PyObject*)self_node != Py_None);
            break;
        case Py_LE:
            satisfied = ((PyObject*)self_node == Py_None);
            break;
        case Py_GE:
            satisfied = ((PyObject*)other_node == Py_None);
            break;
        default:
            assert(0 && "Invalid rich compare operator");
            PyErr_SetString(PyExc_ValueError, "Invalid rich compare operator");
            return NULL;
        }
    }
    else if (op != Py_EQ)
    {
        /* Both nodes are valid, but not equal */
        satisfied = PyObject_RichCompareBool(
            self_node->value, other_node->value, op);
    }

    if (satisfied)
        Py_RETURN_TRUE;
    else
        Py_RETURN_FALSE;
}

static PyObject* dllist_appendleft(DLListObject* self, PyObject* arg)
{
    DLListNodeObject* new_node;

    if (PyObject_TypeCheck(arg, &DLListNodeType))
        arg = ((DLListNodeObject*)arg)->value;

    new_node = dllistnode_create(NULL, self->first, arg, (PyObject*)self);

    self->first = (PyObject*)new_node;

    if (self->last == Py_None)
        self->last = (PyObject*)new_node;

    if (self->last_accessed_idx >= 0)
        ++self->last_accessed_idx;

    ++self->size;

    Py_INCREF((PyObject*)new_node);
    return (PyObject*)new_node;
}

static PyObject* dllist_appendright(DLListObject* self, PyObject* arg)
{
    DLListNodeObject* new_node;

    if (PyObject_TypeCheck(arg, &DLListNodeType))
        arg = ((DLListNodeObject*)arg)->value;

    new_node = dllistnode_create(self->last, NULL, arg, (PyObject*)self);

    self->last = (PyObject*)new_node;

    if (self->first == Py_None)
        self->first = (PyObject*)new_node;

    ++self->size;

    Py_INCREF((PyObject*)new_node);
    return (PyObject*)new_node;
}

static PyObject* dllist_appendnode(DLListObject* self, PyObject* arg)
{
    if (!PyObject_TypeCheck(arg, &DLListNodeType))
    {
        PyErr_SetString(PyExc_TypeError, "Argument must be a dllistnode");
        return NULL;
    }

    DLListNodeObject* node = (DLListNodeObject*) arg;

    if (node->list_weakref != Py_None
        || node->prev != Py_None
        || node->next != Py_None)
    {
        PyErr_SetString(PyExc_ValueError,
            "Argument node must not belong to a list");
        return NULL;
    }

    /* appending to empty list */
    if(self->first == Py_None)
        self->first = (PyObject*)node;
    /* setting next of last element as new node */
    else
    {
        ((DLListNodeObject*)self->last)->next = (PyObject*)node;
        node->prev = self->last;
    }

    /* allways set last node to new node */
    self->last = (PyObject*)node;

    Py_DECREF(node->list_weakref);
    node->list_weakref = PyWeakref_NewRef((PyObject*) self, NULL);

    Py_INCREF((PyObject*)node);
    ++self->size;

    Py_INCREF((PyObject*)node);
    return (PyObject*)node;
}

static int dllist_validate_ref_node(DLListObject* self, PyObject* ref_node)
{
    if (!PyObject_TypeCheck(ref_node, &DLListNodeType))
    {
        PyErr_SetString(PyExc_TypeError,
            "ref_node argument must be a dllistnode");
        return 0;
    }

    PyObject* list_weakref = ((DLListNodeObject*)ref_node)->list_weakref;

    if (list_weakref == Py_None)
    {
        PyErr_SetString(PyExc_ValueError,
            "dllistnode does not belong to a list");
        return 0;
    }

    PyObject* list_ref = PyWeakref_GetObject(list_weakref);

    if (list_ref != (PyObject*)self)
    {
        PyErr_SetString(PyExc_ValueError,
            "dllistnode belongs to another list");
        return 0;
    }

    return 1;
}

static PyObject* dllist_insert(DLListObject* self, PyObject* args)
{
    PyObject* val = NULL;
    PyObject* ref_node = NULL;
    DLListNodeObject* new_node;

    if (!PyArg_UnpackTuple(args, "insert", 1, 2, &val, &ref_node))
        return NULL;

    if (PyObject_TypeCheck(val, &DLListNodeType))
        val = ((DLListNodeObject*)val)->value;

    if (ref_node == NULL || ref_node == Py_None)
    {
        /* append item at the end of the list */
        new_node = dllistnode_create(self->last, NULL, val, (PyObject*)self);

        self->last = (PyObject*)new_node;

        if (self->first == Py_None)
            self->first = (PyObject*)new_node;
    }
    else
    {
        if (!dllist_validate_ref_node(self, ref_node))
            return NULL;

        /* insert item before ref_node */
        new_node = dllistnode_create(
            ((DLListNodeObject*)ref_node)->prev,
            ref_node, val, (PyObject*)self);

        if (ref_node == self->first)
            self->first = (PyObject*)new_node;

        if (self->last == Py_None)
            self->last = (PyObject*)new_node;

        dllist_invalidate_last_access_cache(self);
    }

    ++self->size;

    Py_INCREF((PyObject*)new_node);
    return (PyObject*)new_node;
}

static PyObject* dllist_insertbefore(DLListObject* self, PyObject* args)
{
    PyObject* value = NULL;
    PyObject* ref = NULL;

    if (!PyArg_UnpackTuple(args, "insertbefore", 2, 2, &value, &ref))
        return NULL;

    if (!dllist_validate_ref_node(self, ref))
        return NULL;

    /* if inserted item is a node, extract and insert its value instead */
    if (PyObject_TypeCheck(value, &DLListNodeType))
        value = ((DLListNodeObject*)value)->value;

    DLListNodeObject* ref_node = (DLListNodeObject*)ref;

    DLListNodeObject* new_node = dllistnode_create(
        ref_node->prev, ref, value, (PyObject*)self);

    if (ref == self->first)
        self->first = (PyObject*)new_node;

    dllist_invalidate_last_access_cache(self);

    ++self->size;

    Py_INCREF((PyObject*)new_node);
    return (PyObject*)new_node;
}

static PyObject* dllist_insertafter(DLListObject* self, PyObject* args)
{
    PyObject* value = NULL;
    PyObject* ref = NULL;

    if (!PyArg_UnpackTuple(args, "insertafter", 2, 2, &value, &ref))
        return NULL;

    if (!dllist_validate_ref_node(self, ref))
        return NULL;

    /* if inserted item is a node, extract and insert its value instead */
    if (PyObject_TypeCheck(value, &DLListNodeType))
        value = ((DLListNodeObject*)value)->value;

    DLListNodeObject* ref_node = (DLListNodeObject*)ref;

    DLListNodeObject* new_node = dllistnode_create(
        ref, ref_node->next, value, (PyObject*)self);

    if (ref == self->last)
        self->last = (PyObject*)new_node;

    dllist_invalidate_last_access_cache(self);

    ++self->size;

    Py_INCREF((PyObject*)new_node);
    return (PyObject*)new_node;
}

static int dllist_validate_inserted_node(DLListObject* self, PyObject* inserted)
{
    if (!PyObject_TypeCheck(inserted, &DLListNodeType))
    {
        PyErr_SetString(PyExc_TypeError,
            "Inserted object must be a dllistnode");
        return 0;
    }

    DLListNodeObject* inserted_node = (DLListNodeObject*)inserted;

    if (inserted_node->list_weakref != Py_None
        || inserted_node->prev != Py_None
        || inserted_node->next != Py_None)
    {
        PyErr_SetString(PyExc_ValueError,
            "Inserted node must not belong to a list");
        return 0;
    }

    return 1;
}

static PyObject* dllist_insertnode(DLListObject* self, PyObject* args)
{
    PyObject* inserted = NULL;
    PyObject* ref = NULL;

    if (!PyArg_UnpackTuple(args, "insertnode", 1, 2, &inserted, &ref))
        return NULL;

    if (!dllist_validate_inserted_node(self, inserted))
        return NULL;

    DLListNodeObject* inserted_node = (DLListNodeObject*)inserted;

    if (ref == NULL || ref == Py_None)
    {
        /* append item at the end of the list */
        dllistnode_link(self->last, NULL, inserted_node, (PyObject*)self);

        self->last = inserted;

        if (self->first == Py_None)
            self->first = inserted;
    }
    else
    {
        if (!dllist_validate_ref_node(self, ref))
            return NULL;

        DLListNodeObject* ref_node = (DLListNodeObject*)ref;

        /* insert item before ref_node */
        dllistnode_link(ref_node->prev, ref, inserted_node, (PyObject*)self);

        if (ref == self->first)
            self->first = inserted;

        if (self->last == Py_None)
            self->last = inserted;

        dllist_invalidate_last_access_cache(self);
    }

    Py_INCREF(inserted);
    ++self->size;

    Py_INCREF(inserted);
    return inserted;
}

static PyObject* dllist_insertnodebefore(DLListObject* self, PyObject* args)
{
    PyObject* inserted = NULL;
    PyObject* ref = NULL;

    if (!PyArg_UnpackTuple(args, "insertnodebefore", 2, 2, &inserted, &ref))
        return NULL;

    if (!dllist_validate_inserted_node(self, inserted))
        return NULL;

    if (!dllist_validate_ref_node(self, ref))
        return NULL;

    DLListNodeObject* inserted_node = (DLListNodeObject*)inserted;
    DLListNodeObject* ref_node = (DLListNodeObject*)ref;

    dllistnode_link(ref_node->prev, ref, inserted_node, (PyObject*)self);

    if (ref == self->first)
        self->first = inserted;

    dllist_invalidate_last_access_cache(self);

    Py_INCREF(inserted);
    ++self->size;

    Py_INCREF(inserted);
    return inserted;
}

static PyObject* dllist_insertnodeafter(DLListObject* self, PyObject* args)
{
    PyObject* inserted = NULL;
    PyObject* ref = NULL;

    if (!PyArg_UnpackTuple(args, "insertnodeafter", 2, 2, &inserted, &ref))
        return NULL;

    if (!dllist_validate_inserted_node(self, inserted))
        return NULL;

    if (!dllist_validate_ref_node(self, ref))
        return NULL;

    DLListNodeObject* inserted_node = (DLListNodeObject*)inserted;
    DLListNodeObject* ref_node = (DLListNodeObject*)ref;

    dllistnode_link(ref, ref_node->next, inserted_node, (PyObject*)self);

    if (ref == self->last)
        self->last = inserted;

    dllist_invalidate_last_access_cache(self);

    Py_INCREF(inserted);
    ++self->size;

    Py_INCREF(inserted);
    return inserted;
}

static PyObject* dllist_extendleft(DLListObject* self, PyObject* sequence)
{
    Py_ssize_t i;
    Py_ssize_t sequence_len;

    if (PyObject_TypeCheck(sequence, &DLListType))
    {
        /* Special path for extending with a DLList.
         * It's not strictly required but it will maintain
         * the last accessed item. */
        PyObject* iter_node_obj = ((DLListObject*)sequence)->first;
        PyObject* last_node_obj = ((DLListObject*)sequence)->last;

        while (iter_node_obj != Py_None)
        {
            DLListNodeObject* iter_node = (DLListNodeObject*)iter_node_obj;
            PyObject* new_node;

            new_node = (PyObject*)dllistnode_create(
                NULL, self->first, iter_node->value, (PyObject*)self);

            self->first = new_node;
            if (self->last == Py_None)
                self->last = new_node;

            if (iter_node_obj == last_node_obj)
            {
                /* This is needed to terminate loop if self == sequence. */
                break;
            }

            iter_node_obj = iter_node->next;
        }

        self->size += ((DLListObject*)sequence)->size;

        /* update index of last accessed item */
        if (self->last_accessed_idx >= 0)
            self->last_accessed_idx += ((DLListObject*)sequence)->size;

        Py_RETURN_NONE;
    }

    sequence_len = PySequence_Length(sequence);
    if (sequence_len == -1)
    {
        PyErr_SetString(PyExc_ValueError, "Invalid sequence");
        return NULL;
    }

    for (i = 0; i < sequence_len; ++i)
    {
        PyObject* item;
        PyObject* new_node;

        item = PySequence_GetItem(sequence, i);
        if (item == NULL)
        {
            PyErr_SetString(PyExc_ValueError,
                "Failed to get element from sequence");
            return NULL;
        }

        new_node = (PyObject*)dllistnode_create(
            NULL, self->first, item, (PyObject*)self);

        self->first = new_node;
        if (self->last == Py_None)
            self->last = new_node;

        ++self->size;

        /* update index of last accessed item */
        if (self->last_accessed_idx >= 0)
            ++self->last_accessed_idx;

        Py_DECREF(item);
    }

    Py_RETURN_NONE;
}

static PyObject* dllist_extendright(DLListObject* self, PyObject* arg)
{
    if (!dllist_extend_internal(self, arg))
        return NULL;

    Py_RETURN_NONE;
}

static PyObject* dllist_clear(DLListObject* self)
{
    PyObject* iter_node_obj = self->first;

    while (iter_node_obj != Py_None)
    {
        DLListNodeObject* iter_node = (DLListNodeObject*)iter_node_obj;

        iter_node_obj = iter_node->next;
        dllistnode_delete(iter_node);
    }

    dllist_invalidate_last_access_cache(self);

    self->first = Py_None;
    self->last = Py_None;
    self->size = 0;

    Py_RETURN_NONE;
}

static PyObject* dllist_popleft(DLListObject* self)
{
    DLListNodeObject* del_node;
    PyObject* value;

    if (self->first == Py_None)
    {
        PyErr_SetString(PyExc_ValueError, "List is empty");
        return NULL;
    }

    del_node = (DLListNodeObject*)self->first;

    self->first = del_node->next;
    if (self->last == (PyObject*)del_node)
        self->last = Py_None;

    if (self->last_accessed_node != (PyObject*)del_node)
    {
        if (self->last_accessed_idx >= 0)
            --self->last_accessed_idx;
    }
    else
      dllist_invalidate_last_access_cache(self);

    --self->size;

    Py_INCREF(del_node->value);
    value = del_node->value;

    dllistnode_delete(del_node);

    return value;
}

static PyObject* dllist_popright(DLListObject* self)
{
    DLListNodeObject* del_node;
    PyObject* value;

    if (self->last == Py_None)
    {
        PyErr_SetString(PyExc_ValueError, "List is empty");
        return NULL;
    }

    del_node = (DLListNodeObject*)self->last;

    self->last = del_node->prev;
    if (self->first == (PyObject*)del_node)
        self->first = Py_None;

    if (self->last_accessed_node == (PyObject*)del_node)
      dllist_invalidate_last_access_cache(self);

    --self->size;

    Py_INCREF(del_node->value);
    value = del_node->value;

    dllistnode_delete(del_node);

    return value;
}

static PyObject* dllist_remove(DLListObject* self, PyObject* arg)
{
    DLListNodeObject* del_node;
    PyObject* list_ref;
    PyObject* value;

    if (!PyObject_TypeCheck(arg, &DLListNodeType))
    {
        PyErr_SetString(PyExc_TypeError, "Argument must be a dllistnode");
        return NULL;
    }

    if (self->first == Py_None)
    {
        PyErr_SetString(PyExc_ValueError, "List is empty");
        return NULL;
    }

    del_node = (DLListNodeObject*)arg;

    if (del_node->list_weakref == Py_None)
    {
        PyErr_SetString(PyExc_ValueError,
            "dllistnode does not belong to a list");
        return NULL;
    }

    list_ref = PyWeakref_GetObject(del_node->list_weakref);
    if (list_ref != (PyObject*)self)
    {
        PyErr_SetString(PyExc_ValueError,
            "dllistnode belongs to another list");
        return NULL;
    }

    if (self->first == arg)
        self->first = del_node->next;
    if (self->last == arg)
        self->last = del_node->prev;
    if (self->last_accessed_node == arg)
        self->last_accessed_node = del_node->prev;

    dllist_invalidate_last_access_cache(self);

    --self->size;

    Py_INCREF(del_node->value);
    value = del_node->value;

    dllistnode_delete(del_node);

    return value;
}

static PyObject* dllist_rotate(DLListObject* self, PyObject* nObject)
{
    Py_ssize_t n;
    Py_ssize_t split_idx;
    Py_ssize_t n_mod;
    DLListNodeObject* new_first;
    DLListNodeObject* new_last;

    if (self->size <= 1)
        Py_RETURN_NONE;

    if (!Py23Int_Check(nObject))
    {
        PyErr_SetString(PyExc_TypeError, "n must be an integer");
        return NULL;
    }

    n = Py23Int_AsSsize_t(nObject);
    n_mod = py_ssize_t_abs(n) % self->size;

    if (n_mod == 0)
        Py_RETURN_NONE; /* no-op */

    if (n > 0)
        split_idx = self->size - n_mod; /* rotate right */
    else
        split_idx = n_mod;  /* rotate left */

    new_last = dllist_get_node_internal(self, split_idx - 1);
    assert(new_last != NULL);
    new_first = (DLListNodeObject*)new_last->next;

    ((DLListNodeObject*)self->first)->prev = self->last;
    ((DLListNodeObject*)self->last)->next = self->first;

    new_first->prev = Py_None;
    new_last->next = Py_None;

    self->first = (PyObject*)new_first;
    self->last = (PyObject*)new_last;

    if (self->last_accessed_idx >= 0)
    {
        self->last_accessed_idx =
            (self->last_accessed_idx + self->size - split_idx) % self->size;
    }

    Py_RETURN_NONE;
}

static PyObject* dllist_create_iterator(PyObject* self, PyObject* iterator_type)
{
    PyObject* args;
    PyObject* result;

    args = PyTuple_New(1);
    if (args == NULL)
    {
        PyErr_SetString(PyExc_RuntimeError,
            "Failed to create argument tuple");
        return NULL;
    }

    Py_INCREF(self);
    if (PyTuple_SetItem(args, 0, self) != 0)
    {
        PyErr_SetString(PyExc_RuntimeError,
            "Failed to initialize argument tuple");
        return NULL;
    }

    result = PyObject_CallObject(iterator_type, args);

    Py_DECREF(args);

    return result;
}

static PyObject* dllist_iternodes(PyObject* self)
{
    return dllist_create_iterator(self, (PyObject*)&DLListNodeIteratorType);
}

static PyObject* dllist_itervalues(PyObject* self)
{
    return dllist_create_iterator(self, (PyObject*)&DLListIteratorType);
}

static Py_ssize_t dllist_len(PyObject* self)
{
    DLListObject* list = (DLListObject*)self;
    return list->size;
}

static PyObject* dllist_concat(PyObject* self, PyObject* other)
{
    DLListObject* new_list;

    new_list = (DLListObject*)PyObject_CallObject(
        (PyObject*)&DLListType, NULL);

    if (!dllist_extend_internal(new_list, self) ||
        !dllist_extend_internal(new_list, other))
    {
        Py_DECREF(new_list);
        return NULL;
    }

    return (PyObject*)new_list;
}

static PyObject* dllist_inplace_concat(PyObject* self, PyObject* other)
{
    if (!dllist_extend_internal((DLListObject*)self, other))
        return NULL;

    Py_INCREF(self);
    return self;
}

static PyObject* dllist_repeat(PyObject* self, Py_ssize_t count)
{
    DLListObject* new_list;
    Py_ssize_t i;

    new_list = (DLListObject*)PyObject_CallObject(
        (PyObject*)&DLListType, NULL);

    for (i = 0; i < count; ++i)
    {
        if (!dllist_extend_internal(new_list, self))
        {
            Py_DECREF(new_list);
            return NULL;
        }
    }

    return (PyObject*)new_list;
}

static PyObject* dllist_get_item(PyObject* self, Py_ssize_t index)
{
    DLListNodeObject* node;

    node = dllist_get_node_internal((DLListObject*)self, index);
    if (node != NULL)
    {
        PyObject* value = node->value;

        Py_XINCREF(value);

        /* update last accessed node */
        ((DLListObject*)self)->last_accessed_node = (PyObject*)node;
        ((DLListObject*)self)->last_accessed_idx = index;

        return value;
    }

    return NULL;
}

static int dllist_set_item(PyObject* self, Py_ssize_t index, PyObject* val)
{
    DLListObject* list = (DLListObject*)self;
    DLListNodeObject* node;
    PyObject* oldval;

    node = dllist_get_node_internal(list, index);
    if (node == NULL)
        return -1;

    /* Here is a tricky (and undocumented) part of sequence protocol.
     * Python will pass NULL as item value when item is deleted with:
     * del list[index] */
    if (val == NULL)
    {
        PyObject* prev = node->prev;
        PyObject* result;

        result = dllist_remove(list, (PyObject*)node);

        if (prev != Py_None && index > 0)
        {
            /* Last accessed item was invalidated by dllist_remove.
             * We restore it here as the preceding node. */
            list->last_accessed_node = prev;
            list->last_accessed_idx = index - 1;
        }

        Py_XDECREF(result);

        return (result != NULL) ? 0 : -1;
    }

    /* The rest of this function handles normal assignment:
     * list[index] = item */
    if (PyObject_TypeCheck(val, &DLListNodeType))
        val = ((DLListNodeObject*)val)->value;

    oldval = node->value;

    Py_INCREF(val);
    node->value = val;
    Py_DECREF(oldval);

    /* update last accessed node */
    list->last_accessed_node = (PyObject*)node;
    list->last_accessed_idx = index;

    return 0;
}

static PyMethodDef DLListMethods[] =
{
    { "appendleft", (PyCFunction)dllist_appendleft, METH_O,
      "Append element at the beginning of the list" },
    { "append", (PyCFunction)dllist_appendright, METH_O,
      "Append element at the end of the list" },
    { "appendright", (PyCFunction)dllist_appendright, METH_O,
      "Append element at the end of the list" },
    { "appendnode", (PyCFunction)dllist_appendnode, METH_O,
      "Append raw dllistnode at the end of the list" },
    { "clear", (PyCFunction)dllist_clear, METH_NOARGS,
      "Remove all elements from the list" },
    { "extend", (PyCFunction)dllist_extendright, METH_O,
      "Append elements from iterable at the right side of the list" },
    { "extendleft", (PyCFunction)dllist_extendleft, METH_O,
      "Append elements from iterable at the left side of the list" },
    { "extendright", (PyCFunction)dllist_extendright, METH_O,
      "Append elements from iterable at the right side of the list" },
    { "insert", (PyCFunction)dllist_insert, METH_VARARGS,
      "Inserts element before node" },
    { "insertbefore", (PyCFunction)dllist_insertbefore, METH_VARARGS,
      "Inserts element before node" },
    { "insertafter", (PyCFunction)dllist_insertafter, METH_VARARGS,
      "Inserts element after node" },
    { "insertnode", (PyCFunction)dllist_insertnode, METH_VARARGS,
      "Inserts element before node" },
    { "insertnodebefore", (PyCFunction)dllist_insertnodebefore, METH_VARARGS,
      "Inserts element before node" },
    { "insertnodeafter", (PyCFunction)dllist_insertnodeafter, METH_VARARGS,
      "Inserts element before node" },
    { "iternodes", (PyCFunction)dllist_iternodes, METH_NOARGS,
      "Return iterator over list nodes" },
    { "itervalues", (PyCFunction)dllist_itervalues, METH_NOARGS,
      "Return iterator over list values" },
    { "nodeat", (PyCFunction)dllist_node_at, METH_O,
      "Return node at index" },
    { "popleft", (PyCFunction)dllist_popleft, METH_NOARGS,
      "Remove first element from the list and return it" },
    { "pop", (PyCFunction)dllist_popright, METH_NOARGS,
      "Remove last element from the list and return it" },
    { "popright", (PyCFunction)dllist_popright, METH_NOARGS,
      "Remove last element from the list and return it" },
    { "remove", (PyCFunction)dllist_remove, METH_O,
      "Remove element from the list" },
    { "rotate", (PyCFunction)dllist_rotate, METH_O,
      "Rotate the list n steps to the right" },
    { NULL },   /* sentinel */
};

static PyMemberDef DLListMembers[] =
{
    { "first", T_OBJECT_EX, offsetof(DLListObject, first), READONLY,
      "First node" },
    { "last", T_OBJECT_EX, offsetof(DLListObject, last), READONLY,
      "Next node" },
    { "size", T_INT, offsetof(DLListObject, size), READONLY,
      "Number of elements in the list" },
    { NULL },   /* sentinel */
};

static PySequenceMethods DLListSequenceMethods =
{
    dllist_len,                 /* sq_length */
    dllist_concat,              /* sq_concat */
    dllist_repeat,              /* sq_repeat */
    dllist_get_item,            /* sq_item */
    0,                          /* sq_slice */
    dllist_set_item,            /* sq_ass_item */
    0,                          /* sq_ass_slice */
    0,                          /* sq_contains */
    dllist_inplace_concat,      /* sq_inplace_concat */
    0,                          /* sq_inplace_repeat */
};

static PyTypeObject DLListType =
{
    PyVarObject_HEAD_INIT(NULL, 0)
    "llist.dllist",             /* tp_name */
    sizeof(DLListObject),       /* tp_basicsize */
    0,                          /* tp_itemsize */
    (destructor)dllist_dealloc, /* tp_dealloc */
    0,                          /* tp_print */
    0,                          /* tp_getattr */
    0,                          /* tp_setattr */
    0,                          /* tp_compare */
    (reprfunc)dllist_repr,      /* tp_repr */
    0,                          /* tp_as_number */
    &DLListSequenceMethods,     /* tp_as_sequence */
    0,                          /* tp_as_mapping */
    (hashfunc)dllist_hash,      /* tp_hash */
    0,                          /* tp_call */
    (reprfunc)dllist_str,       /* tp_str */
    0,                          /* tp_getattro */
    0,                          /* tp_setattro */
    0,                          /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_GC,
                                /* tp_flags */
    "Doubly linked list",       /* tp_doc */
    (traverseproc)dllist_traverse,
                                /* tp_traverse */
    (inquiry)dllist_clear_refs, /* tp_clear */
    (richcmpfunc)dllist_richcompare,
                                /* tp_richcompare */
    offsetof(DLListObject, weakref_list),
                                /* tp_weaklistoffset */
    dllist_itervalues,          /* tp_iter */
    0,                          /* tp_iternext */
    DLListMethods,              /* tp_methods */
    DLListMembers,              /* tp_members */
    0,                          /* tp_getset */
    0,                          /* tp_base */
    0,                          /* tp_dict */
    0,                          /* tp_descr_get */
    0,                          /* tp_descr_set */
    0,                          /* tp_dictoffset */
    (initproc)dllist_init,      /* tp_init */
    0,                          /* tp_alloc */
    dllist_new,                 /* tp_new */
};


/* DLListIterator */

typedef struct
{
    PyObject_HEAD
    DLListObject* list;
    PyObject* current_node;
} DLListIteratorObject;

static int dllistiterator_traverse(DLListIteratorObject* self,
                                    visitproc visit,
                                    void* arg)
{
    Py_VISIT(self->list);
    Py_VISIT(self->current_node);

    return 0;
}

static int dllistiterator_clear_refs(DLListIteratorObject* self)
{
    Py_CLEAR(self->list);
    Py_CLEAR(self->current_node);

    return 0;
}

static void dllistiterator_dealloc(DLListIteratorObject* self)
{
    PyObject_GC_UnTrack(self);
    PyObject* obj_self = (PyObject*)self;

    dllistiterator_clear_refs(self);

    obj_self->ob_type->tp_free(obj_self);
}

static PyObject* dllistiterator_new(PyTypeObject* type,
                                    PyObject* args,
                                    PyObject* kwds)
{
    DLListIteratorObject* self;
    PyObject* owner_list = NULL;

    if (!PyArg_UnpackTuple(args, "__new__", 1, 1, &owner_list))
        return NULL;

    if (!PyObject_TypeCheck(owner_list, &DLListType))
    {
        PyErr_SetString(PyExc_TypeError, "dllist argument expected");
        return NULL;
    }

    self = (DLListIteratorObject*)type->tp_alloc(type, 0);
    if (self == NULL)
        return NULL;

    self->list = (DLListObject*)owner_list;
    self->current_node = NULL;

    Py_INCREF(self->list);

    return (PyObject*)self;
}

static DLListNodeObject* dllistiterator_advance(PyObject* self)
{
    DLListIteratorObject* iter_self = (DLListIteratorObject*)self;
    PyObject* value;

    if (iter_self->current_node == NULL)
    {
        // first iteration
        iter_self->current_node = iter_self->list->first;
        Py_INCREF(iter_self->current_node);
    }
    else if (iter_self->current_node != Py_None)
    {
        PyObject* next_node;

        next_node = ((DLListNodeObject*)iter_self->current_node)->next;
        Py_INCREF(next_node);
        Py_DECREF(iter_self->current_node);
        iter_self->current_node = next_node;
    }

    if (iter_self->current_node == Py_None)
    {
        PyErr_SetNone(PyExc_StopIteration);
        return NULL;
    }

    return (DLListNodeObject*)iter_self->current_node;
}

static PyObject* dllistvalueiterator_iternext(PyObject* self)
{
    DLListNodeObject* current_node = dllistiterator_advance(self);

    if (current_node == NULL)
        return NULL;

    PyObject* value = current_node->value;
    Py_INCREF(value);

    return value;
}

static PyObject* dllistnodeiterator_iternext(PyObject* self)
{
    DLListNodeObject* current_node = dllistiterator_advance(self);
    Py_XINCREF(current_node);
    return (PyObject*)current_node;
}

static PyTypeObject DLListIteratorType =
{
    PyVarObject_HEAD_INIT(NULL, 0)
    "llist.dllistiterator",             /* tp_name */
    sizeof(DLListIteratorObject),       /* tp_basicsize */
    0,                                  /* tp_itemsize */
    (destructor)dllistiterator_dealloc, /* tp_dealloc */
    0,                                  /* tp_print */
    0,                                  /* tp_getattr */
    0,                                  /* tp_setattr */
    0,                                  /* tp_compare */
    0,                                  /* tp_repr */
    0,                                  /* tp_as_number */
    0,                                  /* tp_as_sequence */
    0,                                  /* tp_as_mapping */
    0,                                  /* tp_hash */
    0,                                  /* tp_call */
    0,                                  /* tp_str */
    0,                                  /* tp_getattro */
    0,                                  /* tp_setattro */
    0,                                  /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_GC,
                                        /* tp_flags */
    "Doubly linked list iterator",      /* tp_doc */
    (traverseproc)dllistiterator_traverse,
                                        /* tp_traverse */
    (inquiry)dllistiterator_clear_refs, /* tp_clear */
    0,                                  /* tp_richcompare */
    0,                                  /* tp_weaklistoffset */
    PyObject_SelfIter,                  /* tp_iter */
    dllistvalueiterator_iternext,       /* tp_iternext */
    0,                                  /* tp_methods */
    0,                                  /* tp_members */
    0,                                  /* tp_getset */
    0,                                  /* tp_base */
    0,                                  /* tp_dict */
    0,                                  /* tp_descr_get */
    0,                                  /* tp_descr_set */
    0,                                  /* tp_dictoffset */
    0,                                  /* tp_init */
    0,                                  /* tp_alloc */
    dllistiterator_new,                 /* tp_new */
};

static PyTypeObject DLListNodeIteratorType =
{
    PyVarObject_HEAD_INIT(NULL, 0)
    "llist.dllistnodeiterator",         /* tp_name */
    sizeof(DLListIteratorObject),       /* tp_basicsize */
    0,                                  /* tp_itemsize */
    (destructor)dllistiterator_dealloc, /* tp_dealloc */
    0,                                  /* tp_print */
    0,                                  /* tp_getattr */
    0,                                  /* tp_setattr */
    0,                                  /* tp_compare */
    0,                                  /* tp_repr */
    0,                                  /* tp_as_number */
    0,                                  /* tp_as_sequence */
    0,                                  /* tp_as_mapping */
    0,                                  /* tp_hash */
    0,                                  /* tp_call */
    0,                                  /* tp_str */
    0,                                  /* tp_getattro */
    0,                                  /* tp_setattro */
    0,                                  /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_GC,
                                        /* tp_flags */
    "Doubly linked list node iterator", /* tp_doc */
    (traverseproc)dllistiterator_traverse,
                                        /* tp_traverse */
    (inquiry)dllistiterator_clear_refs, /* tp_clear */
    0,                                  /* tp_richcompare */
    0,                                  /* tp_weaklistoffset */
    PyObject_SelfIter,                  /* tp_iter */
    dllistnodeiterator_iternext,        /* tp_iternext */
    0,                                  /* tp_methods */
    0,                                  /* tp_members */
    0,                                  /* tp_getset */
    0,                                  /* tp_base */
    0,                                  /* tp_dict */
    0,                                  /* tp_descr_get */
    0,                                  /* tp_descr_set */
    0,                                  /* tp_dictoffset */
    0,                                  /* tp_init */
    0,                                  /* tp_alloc */
    dllistiterator_new,                 /* tp_new */
};

LLIST_INTERNAL int dllist_init_type(void)
{
    return
        ((PyType_Ready(&DLListType) == 0) &&
         (PyType_Ready(&DLListNodeType) == 0) &&
         (PyType_Ready(&DLListIteratorType) == 0) &&
         (PyType_Ready(&DLListNodeIteratorType) == 0))
        ? 1 : 0;
}

LLIST_INTERNAL void dllist_register(PyObject* module)
{
    Py_INCREF(&DLListType);
    Py_INCREF(&DLListNodeType);
    Py_INCREF(&DLListIteratorType);
    Py_INCREF(&DLListNodeIteratorType);

    PyModule_AddObject(module, "dllist", (PyObject*)&DLListType);
    PyModule_AddObject(module, "dllistnode", (PyObject*)&DLListNodeType);
    PyModule_AddObject(
        module, "dllistiterator", (PyObject*)&DLListIteratorType);
    PyModule_AddObject(
        module, "dllistnodeiterator", (PyObject*)&DLListNodeIteratorType);
}
