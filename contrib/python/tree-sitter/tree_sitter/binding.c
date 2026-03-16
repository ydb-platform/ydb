#define PY_SSIZE_T_CLEAN
#include "Python.h"
#include "tree_sitter/api.h"
#include <wctype.h>

// Types

typedef struct {
    PyObject_HEAD
    TSNode node;
    PyObject *children;
    PyObject *tree;
} Node;

typedef struct {
    PyObject_HEAD
    TSTree *tree;
    PyObject *source;
} Tree;

typedef struct {
    PyObject_HEAD
    TSParser *parser;
} Parser;

typedef struct {
    PyObject_HEAD
    TSTreeCursor cursor;
    PyObject *node;
    PyObject *tree;
} TreeCursor;

typedef struct {
    PyObject_HEAD
    uint32_t capture1_value_id;
    uint32_t capture2_value_id;
    int is_positive;
} CaptureEqCapture;

typedef struct {
    PyObject_HEAD
    uint32_t capture_value_id;
    PyObject *string_value;
    int is_positive;
} CaptureEqString;

typedef struct {
    PyObject_HEAD
    uint32_t capture_value_id;
    PyObject *regex;
    int is_positive;
} CaptureMatchString;

typedef struct {
    PyObject_HEAD
    TSQuery *query;
    PyObject *capture_names;
    PyObject *text_predicates;
} Query;

typedef struct {
    PyObject_HEAD
    TSQueryCapture capture;
} QueryCapture;

typedef struct {
    PyObject_HEAD
    TSRange range;
} Range;

typedef struct {
    TSTreeCursor default_cursor;
    TSQueryCursor *query_cursor;
    PyObject *re_compile;

    PyTypeObject *tree_type;
    PyTypeObject *tree_cursor_type;
    PyTypeObject *parser_type;
    PyTypeObject *node_type;
    PyTypeObject *query_type;
    PyTypeObject *range_type;
    PyTypeObject *query_capture_type;
    PyTypeObject *capture_eq_capture_type;
    PyTypeObject *capture_eq_string_type;
    PyTypeObject *capture_match_string_type;
} ModuleState;

#if PY_VERSION_HEX < 0x030900f0
static ModuleState *global_state = NULL;
static ModuleState *PyType_GetModuleState(PyTypeObject *obj) { return global_state; }
static PyObject *PyType_FromModuleAndSpec(PyObject *module, PyType_Spec *spec, PyObject *bases) {
    return PyType_FromSpecWithBases(spec, bases);
}
#endif

// Point

static PyObject *point_new(TSPoint point) {
    PyObject *row = PyLong_FromSize_t((size_t)point.row);
    PyObject *column = PyLong_FromSize_t((size_t)point.column);
    if (!row || !column) {
        Py_XDECREF(row);
        Py_XDECREF(column);
        return NULL;
    }

    PyObject *obj = PyTuple_Pack(2, row, column);
    Py_XDECREF(row);
    Py_XDECREF(column);
    return obj;
}

// Node

static PyObject *node_new_internal(ModuleState *state, TSNode node, PyObject *tree);
static PyObject *tree_cursor_new_internal(ModuleState *state, TSNode node, PyObject *tree);

static void node_dealloc(Node *self) {
    Py_XDECREF(self->children);
    Py_XDECREF(self->tree);
    Py_TYPE(self)->tp_free(self);
}

static PyObject *node_repr(Node *self) {
    const char *type = ts_node_type(self->node);
    TSPoint start_point = ts_node_start_point(self->node);
    TSPoint end_point = ts_node_end_point(self->node);
    const char *format_string =
        ts_node_is_named(self->node)
            ? "<Node type=%s, start_point=(%u, %u), end_point=(%u, %u)>"
            : "<Node type=\"%s\", start_point=(%u, %u), end_point=(%u, %u)>";
    return PyUnicode_FromFormat(format_string, type, start_point.row, start_point.column,
                                end_point.row, end_point.column);
}

static bool node_is_instance(ModuleState *state, PyObject *self);

static PyObject *node_compare(Node *self, Node *other, int op) {
    ModuleState *state = PyType_GetModuleState(Py_TYPE(self));
    if (node_is_instance(state, (PyObject *)other)) {
        bool result = ts_node_eq(self->node, other->node);
        switch (op) {
        case Py_EQ:
            return PyBool_FromLong(result);
        case Py_NE:
            return PyBool_FromLong(!result);
        default:
            Py_RETURN_FALSE;
        }
    } else {
        Py_RETURN_FALSE;
    }
}

static PyObject *node_sexp(Node *self, PyObject *args) {
    char *string = ts_node_string(self->node);
    PyObject *result = PyUnicode_FromString(string);
    free(string);
    return result;
}

static PyObject *node_walk(Node *self, PyObject *args) {
    ModuleState *state = PyType_GetModuleState(Py_TYPE(self));
    return tree_cursor_new_internal(state, self->node, self->tree);
}

static PyObject *node_child_by_field_id(Node *self, PyObject *args) {
    ModuleState *state = PyType_GetModuleState(Py_TYPE(self));
    TSFieldId field_id;
    if (!PyArg_ParseTuple(args, "H", &field_id)) {
        return NULL;
    }
    TSNode child = ts_node_child_by_field_id(self->node, field_id);
    if (ts_node_is_null(child)) {
        Py_RETURN_NONE;
    }
    return node_new_internal(state, child, self->tree);
}

static PyObject *node_child_by_field_name(Node *self, PyObject *args) {
    ModuleState *state = PyType_GetModuleState(Py_TYPE(self));
    char *name;
    Py_ssize_t length;
    if (!PyArg_ParseTuple(args, "s#", &name, &length)) {
        return NULL;
    }
    TSNode child = ts_node_child_by_field_name(self->node, name, length);
    if (ts_node_is_null(child)) {
        Py_RETURN_NONE;
    }
    return node_new_internal(state, child, self->tree);
}

static PyObject *node_get_id(Node *self, void *payload) {
    return PyLong_FromVoidPtr((void *)self->node.id);
}

static PyObject *node_children_by_field_id_internal(Node *self, TSFieldId field_id) {
    ModuleState *state = PyType_GetModuleState(Py_TYPE(self));
    PyObject *result = PyList_New(0);

    ts_tree_cursor_reset(&state->default_cursor, self->node);
    int ok = ts_tree_cursor_goto_first_child(&state->default_cursor);
    while (ok) {
        if (ts_tree_cursor_current_field_id(&state->default_cursor) == field_id) {
            TSNode tsnode = ts_tree_cursor_current_node(&state->default_cursor);
            PyObject *node = node_new_internal(state, tsnode, self->tree);
            PyList_Append(result, node);
            Py_XDECREF(node);
        }
        ok = ts_tree_cursor_goto_next_sibling(&state->default_cursor);
    }

    return result;
}

static PyObject *node_children_by_field_id(Node *self, PyObject *args) {
    TSFieldId field_id;
    if (!PyArg_ParseTuple(args, "H", &field_id)) {
        return NULL;
    }

    return node_children_by_field_id_internal(self, field_id);
}

static PyObject *node_children_by_field_name(Node *self, PyObject *args) {
    char *name;
    Py_ssize_t length;
    if (!PyArg_ParseTuple(args, "s#", &name, &length)) {
        return NULL;
    }

    const TSLanguage *lang = ts_tree_language(((Tree *)self->tree)->tree);
    TSFieldId field_id = ts_language_field_id_for_name(lang, name, length);
    return node_children_by_field_id_internal(self, field_id);
}

static PyObject *node_field_name_for_child(Node *self, PyObject *args) {
    uint32_t index;
    if (!PyArg_ParseTuple(args, "I", &index)) {
        return NULL;
    }

    const char *field_name = ts_node_field_name_for_child(self->node, index);
    if (field_name == NULL) {
        Py_RETURN_NONE;
    }

    return PyUnicode_FromString(field_name);
}

static PyObject *node_get_type(Node *self, void *payload) {
    return PyUnicode_FromString(ts_node_type(self->node));
}

static PyObject *node_get_is_named(Node *self, void *payload) {
    return PyBool_FromLong(ts_node_is_named(self->node));
}

static PyObject *node_get_is_missing(Node *self, void *payload) {
    return PyBool_FromLong(ts_node_is_missing(self->node));
}

static PyObject *node_get_has_changes(Node *self, void *payload) {
    return PyBool_FromLong(ts_node_has_changes(self->node));
}

static PyObject *node_get_has_error(Node *self, void *payload) {
    return PyBool_FromLong(ts_node_has_error(self->node));
}

static PyObject *node_get_start_byte(Node *self, void *payload) {
    return PyLong_FromSize_t((size_t)ts_node_start_byte(self->node));
}

static PyObject *node_get_end_byte(Node *self, void *payload) {
    return PyLong_FromSize_t((size_t)ts_node_end_byte(self->node));
}

static PyObject *node_get_start_point(Node *self, void *payload) {
    return point_new(ts_node_start_point(self->node));
}

static PyObject *node_get_end_point(Node *self, void *payload) {
    return point_new(ts_node_end_point(self->node));
}

static PyObject *node_get_children(Node *self, void *payload) {
    ModuleState *state = PyType_GetModuleState(Py_TYPE(self));
    if (self->children) {
        Py_INCREF(self->children);
        return self->children;
    }

    long length = (long)ts_node_child_count(self->node);
    PyObject *result = PyList_New(length);
    if (result == NULL) {
        return NULL;
    }
    if (length > 0) {
        ts_tree_cursor_reset(&state->default_cursor, self->node);
        ts_tree_cursor_goto_first_child(&state->default_cursor);
        int i = 0;
        do {
            TSNode child = ts_tree_cursor_current_node(&state->default_cursor);
            if (PyList_SetItem(result, i, node_new_internal(state, child, self->tree))) {
                Py_DECREF(result);
                return NULL;
            }
            i++;
        } while (ts_tree_cursor_goto_next_sibling(&state->default_cursor));
    }
    Py_INCREF(result);
    self->children = result;
    return result;
}

static PyObject *node_get_named_children(Node *self, void *payload) {
    PyObject *children = node_get_children(self, payload);
    if (children == NULL) {
        return NULL;
    }
    // children is retained by self->children
    Py_DECREF(children);

    long named_count = (long)ts_node_named_child_count(self->node);
    PyObject *result = PyList_New(named_count);
    if (result == NULL) {
        return NULL;
    }

    long length = (long)ts_node_child_count(self->node);
    int j = 0;
    for (int i = 0; i < length; i++) {
        PyObject *child = PyList_GetItem(self->children, i);
        if (ts_node_is_named(((Node *)child)->node)) {
            Py_INCREF(child);
            if (PyList_SetItem(result, j++, child)) {
                Py_DECREF(result);
                return NULL;
            }
        }
    }
    return result;
}

static PyObject *node_get_child_count(Node *self, void *payload) {
    long length = (long)ts_node_child_count(self->node);
    PyObject *result = PyLong_FromLong(length);
    return result;
}

static PyObject *node_get_named_child_count(Node *self, void *payload) {
    long length = (long)ts_node_named_child_count(self->node);
    PyObject *result = PyLong_FromLong(length);
    return result;
}

static PyObject *node_get_next_sibling(Node *self, void *payload) {
    ModuleState *state = PyType_GetModuleState(Py_TYPE(self));
    TSNode next_sibling = ts_node_next_sibling(self->node);
    if (ts_node_is_null(next_sibling)) {
        Py_RETURN_NONE;
    }
    return node_new_internal(state, next_sibling, self->tree);
}

static PyObject *node_get_prev_sibling(Node *self, void *payload) {
    ModuleState *state = PyType_GetModuleState(Py_TYPE(self));
    TSNode prev_sibling = ts_node_prev_sibling(self->node);
    if (ts_node_is_null(prev_sibling)) {
        Py_RETURN_NONE;
    }
    return node_new_internal(state, prev_sibling, self->tree);
}

static PyObject *node_get_next_named_sibling(Node *self, void *payload) {
    ModuleState *state = PyType_GetModuleState(Py_TYPE(self));
    TSNode next_named_sibling = ts_node_next_named_sibling(self->node);
    if (ts_node_is_null(next_named_sibling)) {
        Py_RETURN_NONE;
    }
    return node_new_internal(state, next_named_sibling, self->tree);
}

static PyObject *node_get_prev_named_sibling(Node *self, void *payload) {
    ModuleState *state = PyType_GetModuleState(Py_TYPE(self));
    TSNode prev_named_sibling = ts_node_prev_named_sibling(self->node);
    if (ts_node_is_null(prev_named_sibling)) {
        Py_RETURN_NONE;
    }
    return node_new_internal(state, prev_named_sibling, self->tree);
}

static PyObject *node_get_parent(Node *self, void *payload) {
    ModuleState *state = PyType_GetModuleState(Py_TYPE(self));
    TSNode parent = ts_node_parent(self->node);
    if (ts_node_is_null(parent)) {
        Py_RETURN_NONE;
    }
    return node_new_internal(state, parent, self->tree);
}

static PyObject *node_get_text(Node *self, void *payload) {
    Tree *tree = (Tree *)self->tree;
    if (tree == NULL) {
        PyErr_SetString(PyExc_ValueError, "No tree");
        return NULL;
    }
    if (tree->source == Py_None || tree->source == NULL) {
        Py_RETURN_NONE;
    }

    PyObject *start_byte = PyLong_FromSize_t((size_t)ts_node_start_byte(self->node));
    if (start_byte == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to determine start byte");
        return NULL;
    }
    PyObject *end_byte = PyLong_FromSize_t((size_t)ts_node_end_byte(self->node));
    if (end_byte == NULL) {
        Py_DECREF(start_byte);
        PyErr_SetString(PyExc_RuntimeError, "Failed to determine end byte");
        return NULL;
    }
    PyObject *slice = PySlice_New(start_byte, end_byte, NULL);
    Py_DECREF(start_byte);
    Py_DECREF(end_byte);
    if (slice == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "PySlice_New failed");
        return NULL;
    }
    PyObject *node_mv = PyMemoryView_FromObject(tree->source);
    if (node_mv == NULL) {
        Py_DECREF(slice);
        PyErr_SetString(PyExc_RuntimeError, "PyMemoryView_FromObject failed");
        return NULL;
    }
    PyObject *node_slice = PyObject_GetItem(node_mv, slice);
    Py_DECREF(slice);
    Py_DECREF(node_mv);
    if (node_slice == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "PyObject_GetItem failed");
        return NULL;
    }
    PyObject *result = PyBytes_FromObject(node_slice);
    Py_DECREF(node_slice);
    return result;
}

static PyMethodDef node_methods[] = {
    {
        .ml_name = "walk",
        .ml_meth = (PyCFunction)node_walk,
        .ml_flags = METH_NOARGS,
        .ml_doc = "walk()\n--\n\n\
               Get a tree cursor for walking the tree starting at this node.",
    },
    {
        .ml_name = "sexp",
        .ml_meth = (PyCFunction)node_sexp,
        .ml_flags = METH_NOARGS,
        .ml_doc = "sexp()\n--\n\n\
               Get an S-expression representing the node.",
    },
    {
        .ml_name = "child_by_field_id",
        .ml_meth = (PyCFunction)node_child_by_field_id,
        .ml_flags = METH_VARARGS,
        .ml_doc = "child_by_field_id(id)\n--\n\n\
               Get child for the given field id.",
    },
    {
        .ml_name = "child_by_field_name",
        .ml_meth = (PyCFunction)node_child_by_field_name,
        .ml_flags = METH_VARARGS,
        .ml_doc = "child_by_field_name(name)\n--\n\n\
               Get child for the given field name.",
    },
    {
        .ml_name = "children_by_field_id",
        .ml_meth = (PyCFunction)node_children_by_field_id,
        .ml_flags = METH_VARARGS,
        .ml_doc = "children_by_field_id(id)\n--\n\n\
               Get list of child nodes by the field id.",
    },
    {
        .ml_name = "children_by_field_name",
        .ml_meth = (PyCFunction)node_children_by_field_name,
        .ml_flags = METH_VARARGS,
        .ml_doc = "children_by_field_name(name)\n--\n\n\
               Get list of child nodes by the field name.",
    },
    {.ml_name = "field_name_for_child",
     .ml_meth = (PyCFunction)node_field_name_for_child,
     .ml_flags = METH_VARARGS,
     .ml_doc = "field_name_for_child(index)\n-\n\n\
               Get the field name of a child node by the index of child."},
    {NULL},
};

static PyGetSetDef node_accessors[] = {
    {"id", (getter)node_get_id, NULL, "The node's numeric id", NULL},
    {"type", (getter)node_get_type, NULL, "The node's type", NULL},
    {"is_named", (getter)node_get_is_named, NULL, "Is this a named node", NULL},
    {"is_missing", (getter)node_get_is_missing, NULL, "Is this a node inserted by the parser",
     NULL},
    {"has_changes", (getter)node_get_has_changes, NULL,
     "Does this node have text changes since it was parsed", NULL},
    {"has_error", (getter)node_get_has_error, NULL, "Does this node contain any errors", NULL},
    {"start_byte", (getter)node_get_start_byte, NULL, "The node's start byte", NULL},
    {"end_byte", (getter)node_get_end_byte, NULL, "The node's end byte", NULL},
    {"start_point", (getter)node_get_start_point, NULL, "The node's start point", NULL},
    {"end_point", (getter)node_get_end_point, NULL, "The node's end point", NULL},
    {"children", (getter)node_get_children, NULL, "The node's children", NULL},
    {"child_count", (getter)node_get_child_count, NULL, "The number of children for a node", NULL},
    {"named_children", (getter)node_get_named_children, NULL, "The node's named children", NULL},
    {"named_child_count", (getter)node_get_named_child_count, NULL,
     "The number of named children for a node", NULL},
    {"next_sibling", (getter)node_get_next_sibling, NULL, "The node's next sibling", NULL},
    {"prev_sibling", (getter)node_get_prev_sibling, NULL, "The node's previous sibling", NULL},
    {"next_named_sibling", (getter)node_get_next_named_sibling, NULL,
     "The node's next named sibling", NULL},
    {"prev_named_sibling", (getter)node_get_prev_named_sibling, NULL,
     "The node's previous named sibling", NULL},
    {"parent", (getter)node_get_parent, NULL, "The node's parent", NULL},
    {"text", (getter)node_get_text, NULL, "The node's text, if tree has not been edited", NULL},
    {NULL},
};

static PyType_Slot node_type_slots[] = {
    {Py_tp_doc, "A syntax node"},
    {Py_tp_dealloc, node_dealloc},
    {Py_tp_repr, node_repr},
    {Py_tp_richcompare, node_compare},
    {Py_tp_methods, node_methods},
    {Py_tp_getset, node_accessors},
    {0, NULL},
};

static PyType_Spec node_type_spec = {
    .name = "tree_sitter.Node",
    .basicsize = sizeof(Node),
    .itemsize = 0,
    .flags = Py_TPFLAGS_DEFAULT,
    .slots = node_type_slots,
};

static PyObject *node_new_internal(ModuleState *state, TSNode node, PyObject *tree) {
    Node *self = (Node *)state->node_type->tp_alloc(state->node_type, 0);
    if (self != NULL) {
        self->node = node;
        Py_INCREF(tree);
        self->tree = tree;
        self->children = NULL;
    }
    return (PyObject *)self;
}

static bool node_is_instance(ModuleState *state, PyObject *self) {
    return PyObject_IsInstance(self, (PyObject *)state->node_type);
}

// Tree

static PyObject *range_new_internal(ModuleState *state, TSRange range);

static void tree_dealloc(Tree *self) {
    ts_tree_delete(self->tree);
    Py_XDECREF(self->source);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *tree_get_root_node(Tree *self, void *payload) {
    ModuleState *state = PyType_GetModuleState(Py_TYPE(self));
    return node_new_internal(state, ts_tree_root_node(self->tree), (PyObject *)self);
}

static PyObject *tree_get_text(Tree *self, void *payload) {
    PyObject *source = self->source;
    if (source == NULL) {
        Py_RETURN_NONE;
    }
    Py_INCREF(source);
    return source;
}

static PyObject *tree_walk(Tree *self, PyObject *args) {
    ModuleState *state = PyType_GetModuleState(Py_TYPE(self));
    return tree_cursor_new_internal(state, ts_tree_root_node(self->tree), (PyObject *)self);
}

static PyObject *tree_edit(Tree *self, PyObject *args, PyObject *kwargs) {
    unsigned start_byte, start_row, start_column;
    unsigned old_end_byte, old_end_row, old_end_column;
    unsigned new_end_byte, new_end_row, new_end_column;

    char *keywords[] = {
        "start_byte",    "old_end_byte",  "new_end_byte", "start_point",
        "old_end_point", "new_end_point", NULL,
    };

    int ok = PyArg_ParseTupleAndKeywords(
        args, kwargs, "III(II)(II)(II)", keywords, &start_byte, &old_end_byte, &new_end_byte,
        &start_row, &start_column, &old_end_row, &old_end_column, &new_end_row, &new_end_column);

    if (ok) {
        TSInputEdit edit = {
            .start_byte = start_byte,
            .old_end_byte = old_end_byte,
            .new_end_byte = new_end_byte,
            .start_point = {start_row, start_column},
            .old_end_point = {old_end_row, old_end_column},
            .new_end_point = {new_end_row, new_end_column},
        };
        ts_tree_edit(self->tree, &edit);
        Py_XDECREF(self->source);
        self->source = Py_None;
        Py_INCREF(self->source);
    }
    Py_RETURN_NONE;
}

static PyObject *tree_get_changed_ranges(Tree *self, PyObject *args, PyObject *kwargs) {
    ModuleState *state = PyType_GetModuleState(Py_TYPE(self));
    Tree *new_tree = NULL;
    char *keywords[] = {"new_tree", NULL};
    int ok = PyArg_ParseTupleAndKeywords(args, kwargs, "O", keywords, (PyObject **)&new_tree);
    if (!ok)
        return NULL;

    if (!PyObject_IsInstance((PyObject *)new_tree, (PyObject *)state->tree_type)) {
        PyErr_SetString(PyExc_TypeError, "First argument to get_changed_ranges must be a Tree");
        return NULL;
    }

    uint32_t length = 0;
    TSRange *ranges = ts_tree_get_changed_ranges(self->tree, new_tree->tree, &length);

    PyObject *result = PyList_New(length);
    if (!result)
        return NULL;
    for (unsigned i = 0; i < length; i++) {
        PyObject *range = range_new_internal(state, ranges[i]);
        PyList_SetItem(result, i, range);
    }

    free(ranges);
    return result;
}

static PyMethodDef tree_methods[] = {
    {
        .ml_name = "walk",
        .ml_meth = (PyCFunction)tree_walk,
        .ml_flags = METH_NOARGS,
        .ml_doc = "walk()\n--\n\n\
               Get a tree cursor for walking this tree.",
    },
    {
        .ml_name = "edit",
        .ml_meth = (PyCFunction)tree_edit,
        .ml_flags = METH_KEYWORDS | METH_VARARGS,
        .ml_doc = "edit(start_byte, old_end_byte, new_end_byte,\
               start_point, old_end_point, new_end_point)\n--\n\n\
               Edit the syntax tree.",
    },
    {
        .ml_name = "get_changed_ranges",
        .ml_meth = (PyCFunction)tree_get_changed_ranges,
        .ml_flags = METH_KEYWORDS | METH_VARARGS,
        .ml_doc = "get_changed_ranges(new_tree)\n--\n\n\
               Compare old edited tree to new tree and return changed ranges.",
    },
    {NULL},
};

static PyGetSetDef tree_accessors[] = {
    {"root_node", (getter)tree_get_root_node, NULL, "The root node of this tree.", NULL},
    {"text", (getter)tree_get_text, NULL, "The source text for this tree, if unedited.", NULL},
    {NULL},
};

static PyType_Slot tree_type_slots[] = {
    {Py_tp_doc, "A syntax tree"},
    {Py_tp_dealloc, tree_dealloc},
    {Py_tp_methods, tree_methods},
    {Py_tp_getset, tree_accessors},
    {0, NULL},
};

static PyType_Spec tree_type_spec = {
    .name = "tree_sitter.Tree",
    .basicsize = sizeof(Tree),
    .itemsize = 0,
    .flags = Py_TPFLAGS_DEFAULT,
    .slots = tree_type_slots,
};

static PyObject *tree_new_internal(ModuleState *state, TSTree *tree, PyObject *source,
                                   int keep_text) {
    Tree *self = (Tree *)state->tree_type->tp_alloc(state->tree_type, 0);
    if (self != NULL)
        self->tree = tree;

    if (keep_text) {
        self->source = source;
    } else {
        self->source = Py_None;
    }
    Py_INCREF(self->source);
    return (PyObject *)self;
}

// TreeCursor

static void tree_cursor_dealloc(TreeCursor *self) {
    ts_tree_cursor_delete(&self->cursor);
    Py_XDECREF(self->node);
    Py_XDECREF(self->tree);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *tree_cursor_get_node(TreeCursor *self, void *payload) {
    ModuleState *state = PyType_GetModuleState(Py_TYPE(self));
    if (!self->node) {
        self->node =
            node_new_internal(state, ts_tree_cursor_current_node(&self->cursor), self->tree);
    }

    Py_INCREF(self->node);
    return self->node;
}

static PyObject *tree_cursor_current_field_name(TreeCursor *self, PyObject *args) {
    const char *field_name = ts_tree_cursor_current_field_name(&self->cursor);
    if (field_name == NULL) {
        Py_RETURN_NONE;
    }
    return PyUnicode_FromString(field_name);
}

static PyObject *tree_cursor_goto_parent(TreeCursor *self, PyObject *args) {
    bool result = ts_tree_cursor_goto_parent(&self->cursor);
    if (result) {
        Py_XDECREF(self->node);
        self->node = NULL;
    }
    return PyBool_FromLong(result);
}

static PyObject *tree_cursor_goto_first_child(TreeCursor *self, PyObject *args) {
    bool result = ts_tree_cursor_goto_first_child(&self->cursor);
    if (result) {
        Py_XDECREF(self->node);
        self->node = NULL;
    }
    return PyBool_FromLong(result);
}

static PyObject *tree_cursor_goto_next_sibling(TreeCursor *self, PyObject *args) {
    bool result = ts_tree_cursor_goto_next_sibling(&self->cursor);
    if (result) {
        Py_XDECREF(self->node);
        self->node = NULL;
    }
    return PyBool_FromLong(result);
}

static PyObject *tree_cursor_copy(PyObject *self);

static PyMethodDef tree_cursor_methods[] = {
    {
        .ml_name = "current_field_name",
        .ml_meth = (PyCFunction)tree_cursor_current_field_name,
        .ml_flags = METH_NOARGS,
        .ml_doc = "current_field_name()\n--\n\n\
               Get the field name of the tree cursor's current node.\n\n\
               If the current node has the field name, return str. Otherwise, return None.",
    },
    {
        .ml_name = "goto_parent",
        .ml_meth = (PyCFunction)tree_cursor_goto_parent,
        .ml_flags = METH_NOARGS,
        .ml_doc = "goto_parent()\n--\n\n\
               Go to parent.\n\n\
               If the current node is not the root, move to its parent and\n\
               return True. Otherwise, return False.",
    },
    {
        .ml_name = "goto_first_child",
        .ml_meth = (PyCFunction)tree_cursor_goto_first_child,
        .ml_flags = METH_NOARGS,
        .ml_doc = "goto_first_child()\n--\n\n\
               Go to first child.\n\n\
               If the current node has children, move to the first child and\n\
               return True. Otherwise, return False.",
    },
    {
        .ml_name = "goto_next_sibling",
        .ml_meth = (PyCFunction)tree_cursor_goto_next_sibling,
        .ml_flags = METH_NOARGS,
        .ml_doc = "goto_next_sibling()\n--\n\n\
               Go to next sibling.\n\n\
               If the current node has a next sibling, move to the next sibling\n\
               and return True. Otherwise, return False.",
    },
    {
        .ml_name = "copy",
        .ml_meth = (PyCFunction)tree_cursor_copy,
        .ml_flags = METH_NOARGS,
        .ml_doc = "copy()\n--\n\n\
               Make a independent copy of this cursor.\n",
    },
    {NULL},
};

static PyGetSetDef tree_cursor_accessors[] = {
    {"node", (getter)tree_cursor_get_node, NULL, "The current node.", NULL},
    {NULL},
};

static PyType_Slot tree_cursor_type_slots[] = {
    {Py_tp_doc, "A syntax tree cursor"},
    {Py_tp_dealloc, tree_cursor_dealloc},
    {Py_tp_methods, tree_cursor_methods},
    {Py_tp_getset, tree_cursor_accessors},
    {0, NULL},
};

static PyType_Spec tree_cursor_type_spec = {
    .name = "tree_sitter.TreeCursor",
    .basicsize = sizeof(TreeCursor),
    .itemsize = 0,
    .flags = Py_TPFLAGS_DEFAULT,
    .slots = tree_cursor_type_slots,
};

static PyObject *tree_cursor_new_internal(ModuleState *state, TSNode node, PyObject *tree) {
    TreeCursor *self = (TreeCursor *)state->tree_cursor_type->tp_alloc(state->tree_cursor_type, 0);
    if (self != NULL) {
        self->cursor = ts_tree_cursor_new(node);
        Py_INCREF(tree);
        self->tree = tree;
    }
    return (PyObject *)self;
}

static PyObject *tree_cursor_copy(PyObject *self) {
    ModuleState *state = PyType_GetModuleState(Py_TYPE(self));
    TreeCursor *origin = (TreeCursor *)self;
    PyObject *tree = origin->tree;
    TreeCursor *copied =
        (TreeCursor *)state->tree_cursor_type->tp_alloc(state->tree_cursor_type, 0);
    if (copied != NULL) {
        copied->cursor = ts_tree_cursor_copy(&origin->cursor);
        Py_INCREF(tree);
        copied->tree = tree;
    }
    return (PyObject *)copied;
}

// Parser

static PyObject *parser_new(PyTypeObject *type, PyObject *args, PyObject *kwds) {
    Parser *self = (Parser *)type->tp_alloc(type, 0);
    if (self != NULL)
        self->parser = ts_parser_new();
    return (PyObject *)self;
}

static void parser_dealloc(Parser *self) {
    ts_parser_delete(self->parser);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

typedef struct {
    PyObject *read_cb;
    PyObject *previous_return_value;
} ReadWrapperPayload;

static const char *parser_read_wrapper(void *payload, uint32_t byte_offset, TSPoint position,
                                       uint32_t *bytes_read) {
    ReadWrapperPayload *wrapper_payload = payload;
    PyObject *read_cb = wrapper_payload->read_cb;

    // We assume that the parser only needs the return value until the next time
    // this function is called or when ts_parser_parse() returns. We store the
    // return value from the callable in wrapper_payload->previous_return_value so
    // that its reference count will be decremented either during the next call to
    // this wrapper or after ts_parser_parse() has returned.
    Py_XDECREF(wrapper_payload->previous_return_value);
    wrapper_payload->previous_return_value = NULL;

    // Form arguments to callable.
    PyObject *byte_offset_obj = PyLong_FromSize_t((size_t)byte_offset);
    PyObject *position_obj = point_new(position);
    if (!position_obj || !byte_offset_obj) {
        *bytes_read = 0;
        return NULL;
    }

    PyObject *args = PyTuple_Pack(2, byte_offset_obj, position_obj);
    Py_XDECREF(byte_offset_obj);
    Py_XDECREF(position_obj);

    // Call callable.
    PyObject *rv = PyObject_Call(read_cb, args, NULL);
    Py_XDECREF(args);

    // If error or None returned, we've done parsing.
    if (!rv || (rv == Py_None)) {
        Py_XDECREF(rv);
        *bytes_read = 0;
        return NULL;
    }

    // If something other than None is returned, it must be a bytes object.
    if (!PyBytes_Check(rv)) {
        Py_XDECREF(rv);
        PyErr_SetString(PyExc_TypeError, "Read callable must return None or byte buffer type");
        *bytes_read = 0;
        return NULL;
    }

    // Store return value in payload so its reference count can be decremented and
    // return string representation of bytes.
    wrapper_payload->previous_return_value = rv;
    *bytes_read = PyBytes_Size(rv);
    return PyBytes_AsString(rv);
}

static PyObject *parser_parse(Parser *self, PyObject *args, PyObject *kwargs) {
    ModuleState *state = PyType_GetModuleState(Py_TYPE(self));
    PyObject *source_or_callback = NULL;
    PyObject *old_tree_arg = NULL;
    int keep_text = 1;
    static char *keywords[] = {"", "old_tree", "keep_text", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|Op:parse", keywords, &source_or_callback,
                                     &old_tree_arg, &keep_text)) {
        return NULL;
    }

    const TSTree *old_tree = NULL;
    if (old_tree_arg) {
        if (!PyObject_IsInstance(old_tree_arg, (PyObject *)state->tree_type)) {
            PyErr_SetString(PyExc_TypeError, "Second argument to parse must be a Tree");
            return NULL;
        }
        old_tree = ((Tree *)old_tree_arg)->tree;
    }

    TSTree *new_tree = NULL;
    Py_buffer source_view;
    if (!PyObject_GetBuffer(source_or_callback, &source_view, PyBUF_SIMPLE)) {
        // parse a buffer
        const char *source_bytes = (const char *)source_view.buf;
        size_t length = source_view.len;
        new_tree = ts_parser_parse_string(self->parser, old_tree, source_bytes, length);
        PyBuffer_Release(&source_view);
    } else if (PyCallable_Check(source_or_callback)) {
        PyErr_Clear(); // clear the GetBuffer error
        // parse a callable
        ReadWrapperPayload payload = {
            .read_cb = source_or_callback,
            .previous_return_value = NULL,
        };
        TSInput input = {
            .payload = &payload,
            .read = parser_read_wrapper,
            .encoding = TSInputEncodingUTF8,
        };
        new_tree = ts_parser_parse(self->parser, old_tree, input);
        Py_XDECREF(payload.previous_return_value);

        // don't allow tree_new_internal to keep the source text
        source_or_callback = Py_None;
        keep_text = 0;
    } else {
        PyErr_SetString(PyExc_TypeError, "First argument byte buffer type or callable");
        return NULL;
    }

    if (!new_tree) {
        PyErr_SetString(PyExc_ValueError, "Parsing failed");
        return NULL;
    }

    return tree_new_internal(state, new_tree, source_or_callback, keep_text);
}

static PyObject *parser_set_language(Parser *self, PyObject *arg) {
    PyObject *language_id = PyObject_GetAttrString(arg, "language_id");
    if (!language_id) {
        PyErr_SetString(PyExc_TypeError, "Argument to set_language must be a Language");
        return NULL;
    }

    if (!PyLong_Check(language_id)) {
        PyErr_SetString(PyExc_TypeError, "Language ID must be an integer");
        return NULL;
    }

    TSLanguage *language = (TSLanguage *)PyLong_AsVoidPtr(language_id);
    Py_XDECREF(language_id);
    if (!language) {
        PyErr_SetString(PyExc_ValueError, "Language ID must not be null");
        return NULL;
    }

    unsigned version = ts_language_version(language);
    if (version < TREE_SITTER_MIN_COMPATIBLE_LANGUAGE_VERSION ||
        TREE_SITTER_LANGUAGE_VERSION < version) {
        return PyErr_Format(
            PyExc_ValueError, "Incompatible Language version %u. Must be between %u and %u",
            version, TREE_SITTER_MIN_COMPATIBLE_LANGUAGE_VERSION, TREE_SITTER_LANGUAGE_VERSION);
    }

    ts_parser_set_language(self->parser, language);
    Py_RETURN_NONE;
}

static PyMethodDef parser_methods[] = {
    {
        .ml_name = "parse",
        .ml_meth = (PyCFunction)parser_parse,
        .ml_flags = METH_VARARGS | METH_KEYWORDS,
        .ml_doc = "parse(bytes, old_tree=None, keep_text=True)\n--\n\n\
               Parse source code, creating a syntax tree.",
    },
    {
        .ml_name = "set_language",
        .ml_meth = (PyCFunction)parser_set_language,
        .ml_flags = METH_O,
        .ml_doc = "set_language(language)\n--\n\n\
               Set the parser language.",
    },
    {NULL},
};

static PyType_Slot parser_type_slots[] = {
    {Py_tp_doc, "A parser"},
    {Py_tp_new, parser_new},
    {Py_tp_dealloc, parser_dealloc},
    {Py_tp_methods, parser_methods},
    {0, NULL},
};

static PyType_Spec parser_type_spec = {
    .name = "tree_sitter.Parser",
    .basicsize = sizeof(Parser),
    .itemsize = 0,
    .flags = Py_TPFLAGS_DEFAULT,
    .slots = parser_type_slots,
};

// Query Capture

static void capture_dealloc(QueryCapture *self) { Py_TYPE(self)->tp_free(self); }

static PyType_Slot query_capture_type_slots[] = {
    {Py_tp_doc, "A query capture"},
    {Py_tp_dealloc, capture_dealloc},
    {0, NULL},
};

static PyType_Spec query_capture_type_spec = {
    .name = "tree_sitter.Capture",
    .basicsize = sizeof(QueryCapture),
    .itemsize = 0,
    .flags = Py_TPFLAGS_DEFAULT,
    .slots = query_capture_type_slots,
};

static PyObject *query_capture_new_internal(ModuleState *state, TSQueryCapture capture) {
    QueryCapture *self =
        (QueryCapture *)state->query_capture_type->tp_alloc(state->query_capture_type, 0);
    if (self != NULL) {
        self->capture = capture;
    }
    return (PyObject *)self;
}

// Text Predicates

static void capture_eq_capture_dealloc(CaptureEqCapture *self) { Py_TYPE(self)->tp_free(self); }

static void capture_eq_string_dealloc(CaptureEqString *self) {
    Py_XDECREF(self->string_value);
    Py_TYPE(self)->tp_free(self);
}

static void capture_match_string_dealloc(CaptureMatchString *self) {
    Py_XDECREF(self->regex);
    Py_TYPE(self)->tp_free(self);
}

// CaptureEqCapture
static PyType_Slot capture_eq_capture_type_slots[] = {
    {Py_tp_doc, "Text predicate of the form #eq? @capture1 @capture2"},
    {Py_tp_dealloc, capture_eq_capture_dealloc},
    {0, NULL},
};

static PyType_Spec capture_eq_capture_type_spec = {
    .name = "tree_sitter.CaptureEqCapture",
    .basicsize = sizeof(CaptureEqCapture),
    .itemsize = 0,
    .flags = Py_TPFLAGS_DEFAULT,
    .slots = capture_eq_capture_type_slots,
};

// CaptureEqString
static PyType_Slot capture_eq_string_type_slots[] = {
    {Py_tp_doc, "Text predicate of the form #eq? @capture string"},
    {Py_tp_dealloc, capture_eq_string_dealloc},
    {0, NULL},
};

static PyType_Spec capture_eq_string_type_spec = {
    .name = "tree_sitter.CaptureEqString",
    .basicsize = sizeof(CaptureEqString),
    .itemsize = 0,
    .flags = Py_TPFLAGS_DEFAULT,
    .slots = capture_eq_string_type_slots,
};

// CaptureMatchString
static PyType_Slot capture_match_string_type_slots[] = {
    {Py_tp_doc, "Text predicate of the form #eq? @capture regex"},
    {Py_tp_dealloc, capture_match_string_dealloc},
    {0, NULL},
};

static PyType_Spec capture_match_string_type_spec = {
    .name = "tree_sitter.CaptureMatchString",
    .basicsize = sizeof(CaptureMatchString),
    .itemsize = 0,
    .flags = Py_TPFLAGS_DEFAULT,
    .slots = capture_match_string_type_slots,
};

static PyObject *capture_eq_capture_new_internal(ModuleState *state, uint32_t capture1_value_id,
                                                 uint32_t capture2_value_id, int is_positive) {
    CaptureEqCapture *self = (CaptureEqCapture *)state->capture_eq_capture_type->tp_alloc(
        state->capture_eq_capture_type, 0);
    if (self != NULL) {
        self->capture1_value_id = capture1_value_id;
        self->capture2_value_id = capture2_value_id;
        self->is_positive = is_positive;
    }
    return (PyObject *)self;
}

static PyObject *capture_eq_string_new_internal(ModuleState *state, uint32_t capture_value_id,
                                                const char *string_value, int is_positive) {
    CaptureEqString *self = (CaptureEqString *)state->capture_eq_string_type->tp_alloc(
        state->capture_eq_string_type, 0);
    if (self != NULL) {
        self->capture_value_id = capture_value_id;
        self->string_value = PyBytes_FromString(string_value);
        self->is_positive = is_positive;
    }
    return (PyObject *)self;
}

static PyObject *capture_match_string_new_internal(ModuleState *state, uint32_t capture_value_id,
                                                   const char *string_value, int is_positive) {
    CaptureMatchString *self = (CaptureMatchString *)state->capture_match_string_type->tp_alloc(
        state->capture_match_string_type, 0);
    if (self == NULL)
        return NULL;
    self->capture_value_id = capture_value_id;
    self->regex = PyObject_CallFunction(state->re_compile, "s", string_value);
    self->is_positive = is_positive;
    return (PyObject *)self;
}

static bool capture_eq_capture_is_instance(PyObject *self) {
    ModuleState *state = PyType_GetModuleState(Py_TYPE(self));
    return PyObject_IsInstance(self, (PyObject *)state->capture_eq_capture_type);
}

static bool capture_eq_string_is_instance(PyObject *self) {
    ModuleState *state = PyType_GetModuleState(Py_TYPE(self));
    return PyObject_IsInstance(self, (PyObject *)state->capture_eq_string_type);
}

static bool capture_match_string_is_instance(PyObject *self) {
    ModuleState *state = PyType_GetModuleState(Py_TYPE(self));
    return PyObject_IsInstance(self, (PyObject *)state->capture_match_string_type);
}

// Query

static PyObject *query_matches(Query *self, PyObject *args) {
    PyErr_SetString(PyExc_NotImplementedError, "Not Implemented");
    return NULL;
}

static Node *node_for_capture_index(ModuleState *state, uint32_t index, TSQueryMatch match,
                                    Tree *tree) {
    for (unsigned i = 0; i < match.capture_count; i++) {
        TSQueryCapture capture = match.captures[i];
        if (capture.index == index) {
            Node *capture_node = (Node *)node_new_internal(state, capture.node, (PyObject *)tree);
            return capture_node;
        }
    }
    PyErr_SetString(PyExc_ValueError, "An error occurred, capture was not found with given index");
    return NULL;
}

static bool satisfies_text_predicates(Query *query, TSQueryMatch match, Tree *tree) {
    ModuleState *state = PyType_GetModuleState(Py_TYPE(query));
    PyObject *pattern_text_predicates = PyList_GetItem(query->text_predicates, match.pattern_index);
    // if there is no source, ignore the text predicates
    if (tree->source == Py_None || tree->source == NULL) {
        return true;
    }

    Node *node1 = NULL;
    Node *node2 = NULL;
    PyObject *node1_text = NULL;
    PyObject *node2_text = NULL;
    // check if all text_predicates are satisfied
    for (Py_ssize_t j = 0; j < PyList_Size(pattern_text_predicates); j++) {
        PyObject *text_predicate = PyList_GetItem(pattern_text_predicates, j);
        int is_satisfied;
        if (capture_eq_capture_is_instance(text_predicate)) {
            uint32_t capture1_value_id = ((CaptureEqCapture *)text_predicate)->capture1_value_id;
            uint32_t capture2_value_id = ((CaptureEqCapture *)text_predicate)->capture2_value_id;
            node1 = node_for_capture_index(state, capture1_value_id, match, tree);
            node2 = node_for_capture_index(state, capture2_value_id, match, tree);
            if (node1 == NULL || node2 == NULL)
                goto error;
            node1_text = node_get_text(node1, NULL);
            node2_text = node_get_text(node2, NULL);
            if (node1_text == NULL || node2_text == NULL)
                goto error;
            Py_XDECREF(node1);
            Py_XDECREF(node2);
            is_satisfied = PyObject_RichCompareBool(node1_text, node2_text, Py_EQ) ==
                           ((CaptureEqCapture *)text_predicate)->is_positive;
            Py_XDECREF(node1_text);
            Py_XDECREF(node2_text);
            if (!is_satisfied)
                return false;
        } else if (capture_eq_string_is_instance(text_predicate)) {
            uint32_t capture_value_id = ((CaptureEqString *)text_predicate)->capture_value_id;
            node1 = node_for_capture_index(state, capture_value_id, match, tree);
            if (node1 == NULL)
                goto error;
            node1_text = node_get_text(node1, NULL);
            if (node1_text == NULL)
                goto error;
            Py_XDECREF(node1);
            PyObject *string_value = ((CaptureEqString *)text_predicate)->string_value;
            is_satisfied = PyObject_RichCompareBool(node1_text, string_value, Py_EQ) ==
                           ((CaptureEqString *)text_predicate)->is_positive;
            Py_XDECREF(node1_text);
            if (!is_satisfied)
                return false;
        } else if (capture_match_string_is_instance(text_predicate)) {
            uint32_t capture_value_id = ((CaptureMatchString *)text_predicate)->capture_value_id;
            node1 = node_for_capture_index(state, capture_value_id, match, tree);
            if (node1 == NULL)
                goto error;
            node1_text = node_get_text(node1, NULL);
            if (node1_text == NULL)
                goto error;
            Py_XDECREF(node1);
            PyObject *search_result =
                PyObject_CallMethod(((CaptureMatchString *)text_predicate)->regex, "search", "s",
                                    PyBytes_AsString(node1_text));
            Py_XDECREF(node1_text);
            is_satisfied =
                (search_result != Py_None) == ((CaptureMatchString *)text_predicate)->is_positive;
            Py_DECREF(search_result);
            if (!is_satisfied)
                return false;
        }
    }
    return true;

error:
    Py_XDECREF(node1);
    Py_XDECREF(node2);
    Py_XDECREF(node1_text);
    Py_XDECREF(node2_text);
    return false;
}

static PyObject *query_captures(Query *self, PyObject *args, PyObject *kwargs) {
    ModuleState *state = PyType_GetModuleState(Py_TYPE(self));
    char *keywords[] = {
        "node", "start_point", "end_point", "start_byte", "end_byte", NULL,
    };

    Node *node = NULL;
    TSPoint start_point = {.row = 0, .column = 0};
    TSPoint end_point = {.row = UINT32_MAX, .column = UINT32_MAX};
    unsigned start_byte = 0, end_byte = UINT32_MAX;

    int ok = PyArg_ParseTupleAndKeywords(args, kwargs, "O|(II)(II)II", keywords, (PyObject **)&node,
                                         &start_point.row, &start_point.column, &end_point.row,
                                         &end_point.column, &start_byte, &end_byte);
    if (!ok)
        return NULL;

    if (!PyObject_IsInstance((PyObject *)node, (PyObject *)state->node_type)) {
        PyErr_SetString(PyExc_TypeError, "First argument to captures must be a Node");
        return NULL;
    }

    ts_query_cursor_set_byte_range(state->query_cursor, start_byte, end_byte);
    ts_query_cursor_set_point_range(state->query_cursor, start_point, end_point);
    ts_query_cursor_exec(state->query_cursor, self->query, node->node);

    QueryCapture *capture = NULL;
    PyObject *result = PyList_New(0);
    if (result == NULL)
        goto error;

    uint32_t capture_index;
    TSQueryMatch match;
    while (ts_query_cursor_next_capture(state->query_cursor, &match, &capture_index)) {
        capture = (QueryCapture *)query_capture_new_internal(state, match.captures[capture_index]);
        if (capture == NULL)
            goto error;
        if (satisfies_text_predicates(self, match, (Tree *)node->tree)) {
            PyObject *capture_name = PyList_GetItem(self->capture_names, capture->capture.index);
            PyObject *capture_node = node_new_internal(state, capture->capture.node, node->tree);
            PyObject *item = PyTuple_Pack(2, capture_node, capture_name);
            if (item == NULL)
                goto error;
            Py_XDECREF(capture_node);
            PyList_Append(result, item);
            Py_XDECREF(item);
        }
        Py_XDECREF(capture);
    }
    return result;

error:
    Py_XDECREF(result);
    Py_XDECREF(capture);
    return NULL;
}

static void query_dealloc(Query *self) {
    if (self->query)
        ts_query_delete(self->query);
    Py_XDECREF(self->capture_names);
    Py_XDECREF(self->text_predicates);
    Py_TYPE(self)->tp_free(self);
}

static PyMethodDef query_methods[] = {
    {.ml_name = "matches",
     .ml_meth = (PyCFunction)query_matches,
     .ml_flags = METH_VARARGS,
     .ml_doc = "matches(node)\n--\n\n\
               Get a list of all of the matches within the given node."},
    {
        .ml_name = "captures",
        .ml_meth = (PyCFunction)query_captures,
        .ml_flags = METH_KEYWORDS | METH_VARARGS,
        .ml_doc = "captures(node)\n--\n\n\
               Get a list of all of the captures within the given node.",
    },
    {NULL},
};

static PyType_Slot query_type_slots[] = {
    {Py_tp_doc, "A set of patterns to search for in a syntax tree."},
    {Py_tp_dealloc, query_dealloc},
    {Py_tp_methods, query_methods},
    {0, NULL},
};

static PyType_Spec query_type_spec = {
    .name = "tree_sitter.Query",
    .basicsize = sizeof(Query),
    .itemsize = 0,
    .flags = Py_TPFLAGS_DEFAULT,
    .slots = query_type_slots,
};

static PyObject *query_new_internal(ModuleState *state, TSLanguage *language, char *source,
                                    int length) {
    Query *query = (Query *)state->query_type->tp_alloc(state->query_type, 0);
    if (query == NULL)
        return NULL;

    PyObject *pattern_text_predicates = NULL;
    uint32_t error_offset;
    TSQueryError error_type;
    query->query = ts_query_new(language, source, length, &error_offset, &error_type);
    if (!query->query) {
        char *word_start = &source[error_offset];
        char *word_end = word_start;
        while (word_end < &source[length] &&
               (iswalnum(*word_end) || *word_end == '-' || *word_end == '_' || *word_end == '?' ||
                *word_end == '.'))
            word_end++;
        char c = *word_end;
        *word_end = 0;
        switch (error_type) {
        case TSQueryErrorNodeType:
            PyErr_Format(PyExc_NameError, "Invalid node type %s", &source[error_offset]);
            break;
        case TSQueryErrorField:
            PyErr_Format(PyExc_NameError, "Invalid field name %s", &source[error_offset]);
            break;
        case TSQueryErrorCapture:
            PyErr_Format(PyExc_NameError, "Invalid capture name %s", &source[error_offset]);
            break;
        default:
            PyErr_Format(PyExc_SyntaxError, "Invalid syntax at offset %u", error_offset);
        }
        *word_end = c;
        goto error;
    }

    unsigned n = ts_query_capture_count(query->query);
    query->capture_names = PyList_New(n);
    Py_INCREF(Py_None);
    for (unsigned i = 0; i < n; i++) {
        unsigned length;
        const char *capture_name = ts_query_capture_name_for_id(query->query, i, &length);
        PyList_SetItem(query->capture_names, i, PyUnicode_FromStringAndSize(capture_name, length));
    }

    unsigned pattern_count = ts_query_pattern_count(query->query);
    query->text_predicates = PyList_New(pattern_count);
    if (query->text_predicates == NULL)
        goto error;

    for (unsigned i = 0; i < pattern_count; i++) {
        unsigned length;
        const TSQueryPredicateStep *predicate_step =
            ts_query_predicates_for_pattern(query->query, i, &length);
        pattern_text_predicates = PyList_New(0);
        if (pattern_text_predicates == NULL)
            goto error;
        for (unsigned j = 0; j < length; j++) {
            unsigned predicate_len = 0;
            while ((predicate_step + predicate_len)->type != TSQueryPredicateStepTypeDone)
                predicate_len++;

            if (predicate_step->type != TSQueryPredicateStepTypeString) {
                PyErr_Format(
                    PyExc_RuntimeError,
                    "Capture predicate must start with a string i=%d/pattern_count=%d "
                    "j=%d/length=%d predicate_step->type=%d TSQueryPredicateStepTypeDone=%d "
                    "TSQueryPredicateStepTypeCapture=%d TSQueryPredicateStepTypeString=%d",
                    i, pattern_count, j, length, predicate_step->type, TSQueryPredicateStepTypeDone,
                    TSQueryPredicateStepTypeCapture, TSQueryPredicateStepTypeString);
                goto error;
            }

            // Build a predicate for each of the supported predicate function names
            unsigned length;
            const char *operator_name =
                ts_query_string_value_for_id(query->query, predicate_step->value_id, &length);
            if (strcmp(operator_name, "eq?") == 0 || strcmp(operator_name, "not-eq?") == 0) {
                if (predicate_len != 3) {
                    PyErr_SetString(PyExc_RuntimeError,
                                    "Wrong number of arguments to #eq? or #not-eq? predicate");
                    goto error;
                }
                if (predicate_step[1].type != TSQueryPredicateStepTypeCapture) {
                    PyErr_SetString(PyExc_RuntimeError,
                                    "First argument to #eq? or #not-eq? must be a capture name");
                    goto error;
                }
                int is_positive = strcmp(operator_name, "eq?") == 0;
                switch (predicate_step[2].type) {
                case TSQueryPredicateStepTypeCapture:;
                    CaptureEqCapture *capture_eq_capture_predicate =
                        (CaptureEqCapture *)capture_eq_capture_new_internal(
                            state, predicate_step[1].value_id, predicate_step[2].value_id,
                            is_positive);
                    if (capture_eq_capture_predicate == NULL)
                        goto error;
                    PyList_Append(pattern_text_predicates,
                                  (PyObject *)capture_eq_capture_predicate);
                    Py_DECREF(capture_eq_capture_predicate);
                    break;
                case TSQueryPredicateStepTypeString:;
                    const char *string_value = ts_query_string_value_for_id(
                        query->query, predicate_step[2].value_id, &length);
                    CaptureEqString *capture_eq_string_predicate =
                        (CaptureEqString *)capture_eq_string_new_internal(
                            state, predicate_step[1].value_id, string_value, is_positive);
                    if (capture_eq_string_predicate == NULL)
                        goto error;
                    PyList_Append(pattern_text_predicates, (PyObject *)capture_eq_string_predicate);
                    Py_DECREF(capture_eq_string_predicate);
                    break;
                default:
                    PyErr_SetString(PyExc_RuntimeError, "Second argument to #eq? or #not-eq? must "
                                                        "be a capture name or a string literal");
                    goto error;
                }
            } else if (strcmp(operator_name, "match?") == 0 ||
                       strcmp(operator_name, "not-match?") == 0) {
                if (predicate_len != 3) {
                    PyErr_SetString(
                        PyExc_RuntimeError,
                        "Wrong number of arguments to #match? or #not-match? predicate");
                    goto error;
                }
                if (predicate_step[1].type != TSQueryPredicateStepTypeCapture) {
                    PyErr_SetString(
                        PyExc_RuntimeError,
                        "First argument to #match? or #not-match? must be a capture name");
                    goto error;
                }
                if (predicate_step[2].type != TSQueryPredicateStepTypeString) {
                    PyErr_SetString(
                        PyExc_RuntimeError,
                        "Second argument to #match? or #not-match? must be a regex string");
                    goto error;
                }
                const char *string_value =
                    ts_query_string_value_for_id(query->query, predicate_step[2].value_id, &length);
                int is_positive = strcmp(operator_name, "match?") == 0;
                CaptureMatchString *capture_match_string_predicate =
                    (CaptureMatchString *)capture_match_string_new_internal(
                        state, predicate_step[1].value_id, string_value, is_positive);
                if (capture_match_string_predicate == NULL)
                    goto error;
                PyList_Append(pattern_text_predicates, (PyObject *)capture_match_string_predicate);
                Py_DECREF(capture_match_string_predicate);
            }
            predicate_step += predicate_len + 1;
            j += predicate_len;
        }
        PyList_SetItem(query->text_predicates, i, pattern_text_predicates);
    }
    return (PyObject *)query;

error:
    query_dealloc(query);
    Py_XDECREF(pattern_text_predicates);
    return NULL;
}

// Range

static void range_dealloc(Range *self) { Py_TYPE(self)->tp_free(self); }

static PyObject *range_repr(Range *self) {
    const char *format_string =
        "<Range start_point=(%u, %u), start_byte=%u, end_point=(%u, %u), end_byte=%u>";
    return PyUnicode_FromFormat(format_string, self->range.start_point.row,
                                self->range.start_point.column, self->range.start_byte,
                                self->range.end_point.row, self->range.end_point.column,
                                self->range.end_byte);
}

static bool range_is_instance(PyObject *self) {
    ModuleState *state = PyType_GetModuleState(Py_TYPE(self));
    return PyObject_IsInstance(self, (PyObject *)state->range_type);
}

static PyObject *range_compare(Range *self, Range *other, int op) {
    if (range_is_instance((PyObject *)other)) {
        bool result = ((self->range.start_point.row == other->range.start_point.row) &&
                       (self->range.start_point.column == other->range.start_point.column) &&
                       (self->range.start_byte == other->range.start_byte) &&
                       (self->range.end_point.row == other->range.end_point.row) &&
                       (self->range.end_point.column == other->range.end_point.column) &&
                       (self->range.end_byte == other->range.end_byte));
        switch (op) {
        case Py_EQ:
            return PyBool_FromLong(result);
        case Py_NE:
            return PyBool_FromLong(!result);
        default:
            Py_RETURN_FALSE;
        }
    } else {
        Py_RETURN_FALSE;
    }
}

static PyObject *range_get_start_point(Range *self, void *payload) {
    return point_new(self->range.start_point);
}

static PyObject *range_get_end_point(Range *self, void *payload) {
    return point_new(self->range.end_point);
}

static PyObject *range_get_start_byte(Range *self, void *payload) {
    return PyLong_FromSize_t((size_t)(self->range.start_byte));
}

static PyObject *range_get_end_byte(Range *self, void *payload) {
    return PyLong_FromSize_t((size_t)(self->range.end_byte));
}

static PyGetSetDef range_accessors[] = {
    {"start_point", (getter)range_get_start_point, NULL, "The start point of this range", NULL},
    {"start_byte", (getter)range_get_start_byte, NULL, "The start byte of this range", NULL},
    {"end_point", (getter)range_get_end_point, NULL, "The end point of this range", NULL},
    {"end_byte", (getter)range_get_end_byte, NULL, "The end byte of this range", NULL},
    {NULL},
};

static PyType_Slot range_type_slots[] = {
    {Py_tp_doc, "A range within a document."},
    {Py_tp_dealloc, range_dealloc},
    {Py_tp_repr, range_repr},
    {Py_tp_richcompare, range_compare},
    {Py_tp_getset, range_accessors},
    {0, NULL},
};

static PyType_Spec range_type_spec = {
    .name = "tree_sitter.Range",
    .basicsize = sizeof(Range),
    .itemsize = 0,
    .flags = Py_TPFLAGS_DEFAULT,
    .slots = range_type_slots,
};

static PyObject *range_new_internal(ModuleState *state, TSRange range) {
    Range *self = (Range *)state->range_type->tp_alloc(state->range_type, 0);
    if (self != NULL)
        self->range = range;
    return (PyObject *)self;
}

// Module

static PyObject *language_field_id_for_name(PyObject *self, PyObject *args) {
    TSLanguage *language;
    PyObject *language_id;
    char *field_name;
    Py_ssize_t length;
    if (!PyArg_ParseTuple(args, "Os#", &language_id, &field_name, &length)) {
        return NULL;
    }

    language = (TSLanguage *)PyLong_AsVoidPtr(language_id);

    TSFieldId field_id = ts_language_field_id_for_name(language, field_name, length);
    if (field_id == 0) {
        Py_RETURN_NONE;
    }

    return PyLong_FromSize_t((size_t)field_id);
}

static PyObject *language_query(PyObject *self, PyObject *args) {
    ModuleState *state = PyModule_GetState(self);
    TSLanguage *language;
    PyObject *language_id;
    char *source;
    Py_ssize_t length;
    if (!PyArg_ParseTuple(args, "Os#", &language_id, &source, &length)) {
        return NULL;
    }
    language = (TSLanguage *)PyLong_AsVoidPtr(language_id);
    return query_new_internal(state, language, source, length);
}

static void module_free(void *self) {
    ModuleState *state = PyModule_GetState((PyObject *)self);
    ts_query_cursor_delete(state->query_cursor);
    Py_XDECREF(state->tree_type);
    Py_XDECREF(state->tree_cursor_type);
    Py_XDECREF(state->parser_type);
    Py_XDECREF(state->node_type);
    Py_XDECREF(state->query_type);
    Py_XDECREF(state->range_type);
    Py_XDECREF(state->query_capture_type);
    Py_XDECREF(state->capture_eq_capture_type);
    Py_XDECREF(state->capture_eq_string_type);
    Py_XDECREF(state->capture_match_string_type);
    Py_XDECREF(state->re_compile);
}

static PyMethodDef module_methods[] = {
    {
        .ml_name = "_language_field_id_for_name",
        .ml_meth = (PyCFunction)language_field_id_for_name,
        .ml_flags = METH_VARARGS,
        .ml_doc = "(internal)",
    },
    {
        .ml_name = "_language_query",
        .ml_meth = (PyCFunction)language_query,
        .ml_flags = METH_VARARGS,
        .ml_doc = "(internal)",
    },
    {NULL},
};

static struct PyModuleDef module_definition = {
    .m_base = PyModuleDef_HEAD_INIT,
    .m_name = "binding",
    .m_doc = NULL,
    .m_size = sizeof(ModuleState),
    .m_free = module_free,
    .m_methods = module_methods,
};

// simulate PyModule_AddObjectRef for pre-Python 3.10
static int AddObjectRef(PyObject *module, const char *name, PyObject *value) {
    if (value == NULL) {
        PyErr_Format(PyExc_SystemError, "PyModule_AddObjectRef() %s == NULL", name);
        return -1;
    }
    int ret = PyModule_AddObject(module, name, value);
    if (ret == 0) {
        Py_INCREF(value);
    }
    return ret;
}

PyMODINIT_FUNC PyInit_binding(void) {
    PyObject *module = PyModule_Create(&module_definition);
    if (module == NULL) {
        return NULL;
    }

    ModuleState *state = PyModule_GetState(module);

    state->tree_type = (PyTypeObject *)PyType_FromModuleAndSpec(module, &tree_type_spec, NULL);
    state->tree_cursor_type =
        (PyTypeObject *)PyType_FromModuleAndSpec(module, &tree_cursor_type_spec, NULL);
    state->parser_type = (PyTypeObject *)PyType_FromModuleAndSpec(module, &parser_type_spec, NULL);
    state->node_type = (PyTypeObject *)PyType_FromModuleAndSpec(module, &node_type_spec, NULL);
    state->query_type = (PyTypeObject *)PyType_FromModuleAndSpec(module, &query_type_spec, NULL);
    state->range_type = (PyTypeObject *)PyType_FromModuleAndSpec(module, &range_type_spec, NULL);
    state->query_capture_type =
        (PyTypeObject *)PyType_FromModuleAndSpec(module, &query_capture_type_spec, NULL);
    state->capture_eq_capture_type =
        (PyTypeObject *)PyType_FromModuleAndSpec(module, &capture_eq_capture_type_spec, NULL);
    state->capture_eq_string_type =
        (PyTypeObject *)PyType_FromModuleAndSpec(module, &capture_eq_string_type_spec, NULL);
    state->capture_match_string_type =
        (PyTypeObject *)PyType_FromModuleAndSpec(module, &capture_match_string_type_spec, NULL);

    state->query_cursor = ts_query_cursor_new();
    if ((AddObjectRef(module, "Tree", (PyObject *)state->tree_type) < 0) ||
        (AddObjectRef(module, "TreeCursor", (PyObject *)state->tree_cursor_type) < 0) ||
        (AddObjectRef(module, "Parser", (PyObject *)state->parser_type) < 0) ||
        (AddObjectRef(module, "Node", (PyObject *)state->node_type) < 0) ||
        (AddObjectRef(module, "Query", (PyObject *)state->query_type) < 0) ||
        (AddObjectRef(module, "Range", (PyObject *)state->range_type) < 0) ||
        (AddObjectRef(module, "QueryCapture", (PyObject *)state->query_capture_type) < 0) ||
        (AddObjectRef(module, "CaptureEqCapture", (PyObject *)state->capture_eq_capture_type) <
         0) ||
        (AddObjectRef(module, "CaptureEqString", (PyObject *)state->capture_eq_string_type) < 0) ||
        (AddObjectRef(module, "CaptureMatchString", (PyObject *)state->capture_match_string_type) <
         0)) {
        goto cleanup;
    }

    PyObject *re_module = PyImport_ImportModule("re");
    if (re_module == NULL) {
        goto cleanup;
    }
    state->re_compile = PyObject_GetAttrString(re_module, (char *)"compile");
    Py_DECREF(re_module);
    if (state->re_compile == NULL) {
        goto cleanup;
    }

#if PY_VERSION_HEX < 0x030900f0
    global_state = state;
#endif
    return module;

cleanup:
    Py_XDECREF(module);
    return NULL;
}
