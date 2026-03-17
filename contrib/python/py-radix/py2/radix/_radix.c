/*
 * Copyright (c) 2004 Damien Miller <djm@mindrot.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#define PY_SSIZE_T_CLEAN
#include "Python.h"
#include "structmember.h"
#include "_radix/radix.h"

/* $Id$ */

/* for Py3K */
#if PY_MAJOR_VERSION >= 3
# define PyInt_FromLong                 PyLong_FromLong
# define PyString_FromString            PyUnicode_FromString
# define PyString_FromStringAndSize     PyBytes_FromStringAndSize
#endif

/* for version before 2.6 */
#ifndef PyVarObject_HEAD_INIT
# define PyVarObject_HEAD_INIT(type, size)      PyObject_HEAD_INIT(type) size,
#endif
#ifndef Py_TYPE
# define Py_TYPE(ob)    (((PyObject*)(ob))->ob_type)
#endif

/* Prototypes */
struct _RadixObject;
struct _RadixIterObject;
static struct _RadixIterObject *newRadixIterObject(struct _RadixObject *);
static PyObject *radix_Radix(PyObject *, PyObject *);

/* ------------------------------------------------------------------------ */

PyObject *radix_constructor;

/* RadixNode: tree nodes */

typedef struct {
        PyObject_HEAD
        PyObject *user_attr;    /* User-specified attributes */
        PyObject *network;
        PyObject *prefix;
        PyObject *prefixlen;
        PyObject *family;
        PyObject *packed;
        radix_node_t *rn;       /* Actual radix node (pointer to parent) */
} RadixNodeObject;

static PyTypeObject RadixNode_Type;

static RadixNodeObject *
newRadixNodeObject(radix_node_t *rn)
{
        RadixNodeObject *self;
        char network[256], prefix[256];

        /* Sanity check */
        if (rn == NULL || rn->prefix == NULL || 
            (rn->prefix->family != AF_INET && rn->prefix->family != AF_INET6))
                return NULL;

        self = PyObject_New(RadixNodeObject, &RadixNode_Type);
        if (self == NULL)
                return NULL;

        self->rn = rn;

        /* Format addresses for packing into objects */
        prefix_addr_ntop(rn->prefix, network, sizeof(network));
        prefix_ntop(rn->prefix, prefix, sizeof(prefix));

        self->user_attr = PyDict_New();
        self->network = PyString_FromString(network);
        self->prefix = PyString_FromString(prefix);
        self->prefixlen = PyInt_FromLong(rn->prefix->bitlen);
        self->family = PyInt_FromLong(rn->prefix->family);
        self->packed = PyString_FromStringAndSize((char*)&rn->prefix->add,
            rn->prefix->family == AF_INET ? 4 : 16);

        if (self->user_attr == NULL || self->prefixlen == NULL || 
            self->family == NULL || self->network == NULL || 
            self->prefix == NULL) {
                /* RadixNode_dealloc will clean up for us */
                Py_XDECREF(self);
                return (NULL);          
        }

        return self;
}

/* RadixNode methods */

static void
RadixNode_dealloc(RadixNodeObject *self)
{
        Py_XDECREF(self->user_attr);
        Py_XDECREF(self->prefixlen);
        Py_XDECREF(self->family);
        Py_XDECREF(self->network);
        Py_XDECREF(self->prefix);
        Py_XDECREF(self->packed);
        PyObject_Del(self);
}

static PyObject *
Radix_parent(RadixNodeObject *self, void *closure)
{
        PyObject *ret;
        radix_node_t *node;
        node = self->rn;
        /* walk up through parent to find parent node with data */
        for ( ; ; )
        {
                if (node && node->parent)
                {
                        node = node->parent;
                        if (node->data)
                        {
                                ret = node->data;
                                Py_XINCREF(ret);
                                return ret;
                        }
                }
                else
                        break;
        }
        Py_RETURN_NONE;
}
static PyMemberDef RadixNode_members[] = {
        {"data",        T_OBJECT, offsetof(RadixNodeObject, user_attr), READONLY},
        {"network",     T_OBJECT, offsetof(RadixNodeObject, network),   READONLY},
        {"prefix",      T_OBJECT, offsetof(RadixNodeObject, prefix),    READONLY},
        {"prefixlen",   T_OBJECT, offsetof(RadixNodeObject, prefixlen), READONLY},
        {"family",      T_OBJECT, offsetof(RadixNodeObject, family),    READONLY},
        {"packed",      T_OBJECT, offsetof(RadixNodeObject, packed),    READONLY},
        {NULL}
};

static PyGetSetDef node_getter[] = {
        {"parent",
         (getter) Radix_parent,      /* C function to get the attribute */
         NULL,                       /* C function to set the attribute */
         "parent of node",           /* optional doc string */
         NULL                        /* optional additional data for getter and setter */
        },
        {NULL}  /* Sentinel */
};

PyDoc_STRVAR(RadixNode_doc, 
"Node in a radix tree");

static PyTypeObject RadixNode_Type = {
        /* The ob_type field must be initialized in the module init function
         * to be portable to Windows without using C++. */
        PyVarObject_HEAD_INIT(NULL, 0)
        "_radix.RadixNode",      /*tp_name*/
        sizeof(RadixNodeObject),/*tp_basicsize*/
        0,                      /*tp_itemsize*/
        /* methods */
        (destructor)RadixNode_dealloc, /*tp_dealloc*/
        0,                      /*tp_print*/
        0,                      /*tp_getattr*/
        0,                      /*tp_setattr*/
        0,                      /*tp_compare*/
        0,                      /*tp_repr*/
        0,                      /*tp_as_number*/
        0,                      /*tp_as_sequence*/
        0,                      /*tp_as_mapping*/
        0,                      /*tp_hash*/
        0,                      /*tp_call*/
        0,                      /*tp_str*/
        0,                      /*tp_getattro*/
        0,                      /*tp_setattro*/
        0,                      /*tp_as_buffer*/
        Py_TPFLAGS_DEFAULT,     /*tp_flags*/
        RadixNode_doc,          /*tp_doc*/
        0,                      /*tp_traverse*/
        0,                      /*tp_clear*/
        0,                      /*tp_richcompare*/
        0,                      /*tp_weaklistoffset*/
        0,                      /*tp_iter*/
        0,                      /*tp_iternext*/
        0,                      /*tp_methods*/
        RadixNode_members,      /*tp_members*/
        node_getter,            /*tp_getset*/
        0,                      /*tp_base*/
        0,                      /*tp_dict*/
        0,                      /*tp_descr_get*/
        0,                      /*tp_descr_set*/
        0,                      /*tp_dictoffset*/
        0,                      /*tp_init*/
        0,                      /*tp_alloc*/
        0,                      /*tp_new*/
        0,                      /*tp_free*/
        0,                      /*tp_is_gc*/
};

/* ------------------------------------------------------------------------ */

typedef struct _RadixObject {
        PyObject_HEAD
        radix_tree_t *rt;       /* Radix tree for IPv4 & IPv6 addresses */
        unsigned int gen_id;    /* Detect modification during iterations */
} RadixObject;

static PyTypeObject Radix_Type;
#define Radix_CheckExact(op) (Py_TYPE(op) == &Radix_Type)

static RadixObject *
newRadixObject(void)
{
        RadixObject *self;
        radix_tree_t *rt;

        if ((rt = New_Radix()) == NULL)
                return (NULL);
        if ((self = PyObject_New(RadixObject, &Radix_Type)) == NULL) {
                free(rt);
                return (NULL);
        }
        self->rt = rt;
        self->gen_id = 0;
        return (self);
}

/* Radix methods */

static void
Radix_dealloc(RadixObject *self)
{
        radix_node_t *rn;
        RadixNodeObject *node;

        RADIX_TREE_WALK(self->rt, rn) {
                if (rn->data != NULL) {
                        node = rn->data;
                        node->rn = NULL;
                        Py_DECREF(node);
                }
        } RADIX_TREE_WALK_END;

        Destroy_Radix(self->rt, NULL, NULL);
        PyObject_Del(self);
}

static prefix_t
*args_to_prefix(prefix_t *prefix, char *addr, char *packed, Py_ssize_t packlen, long prefixlen)
{
        prefix_t *old_prefix = prefix;
        const char *errmsg;

        if (addr != NULL && packed != NULL) {
                PyErr_SetString(PyExc_TypeError,
                            "Two address types specified. Please pick one.");
                return NULL;
        }

        if (addr == NULL && packed == NULL) {
                PyErr_SetString(PyExc_TypeError,
                            "No address specified (use 'address' or 'packed')");
                return NULL;
        }

        if (addr != NULL) {             /* Parse a string address */
                if ((prefix = prefix_pton_ex(prefix, addr, prefixlen, &errmsg)) == NULL) {
                        PyErr_SetString(PyExc_ValueError, errmsg ? errmsg :
                            "Invalid address format");
                }
        } else if (packed != NULL) {    /* "parse" a packed binary address */
                if ((prefix = prefix_from_blob_ex(prefix, (u_char*)packed, packlen, 
                    prefixlen)) == NULL) {
                        PyErr_SetString(PyExc_ValueError,
                            "Invalid packed address format");
                }
        }
        if (prefix != NULL &&
            prefix->family != AF_INET && prefix->family != AF_INET6) {
                if (old_prefix == NULL)
                        Deref_Prefix(prefix);
                return (NULL);
        }

        return prefix;
}

static PyObject *
create_add_node(RadixObject *self, prefix_t *prefix)
{
        radix_node_t *node;
        RadixNodeObject *node_obj;

        if ((node = radix_lookup(self->rt, prefix)) == NULL) {
                PyErr_SetString(PyExc_MemoryError, "Couldn't add prefix");
                return NULL;
        }

        /*
         * Create a RadixNode object in the data area of the node
         * We duplicate most of the node's identity, because the radix.c:node 
         * itself has a lifetime independent of the Python node object
         * Confusing? yeah...
         */
        if (node->data == NULL) {
                if ((node_obj = newRadixNodeObject(node)) == NULL)
                        return (NULL);
                node->data = node_obj;
        } else
                node_obj = node->data;

        self->gen_id++;
        Py_XINCREF(node_obj);
        return (PyObject *)node_obj;
}

PyDoc_STRVAR(Radix_add_doc,
"Radix.add(network[, masklen][, packed]) -> new RadixNode object\n\
\n\
Adds the network specified by 'network' and 'masklen' to the radix\n\
tree. 'network' may be a string in CIDR format, a unicast host\n\
address or a network address, with the mask length specified using\n\
the optional 'masklen' parameter.\n\
\n\
Alternately, the address may be specified in a packed binary format\n\
using the 'packed' keyword argument (instead of 'network'). This is\n\
useful with binary addresses returned by socket.getpeername(),\n\
socket.inet_ntoa(), etc.\n\
\n\
Both IPv4 and IPv6 addresses/networks are supported and may be mixed in\n\
the same tree.\n\
\n\
This method returns a RadixNode object. Arbitrary data may be strored\n\
in the RadixNode.data dict.");

static PyObject *
Radix_add(RadixObject *self, PyObject *args, PyObject *kw_args)
{
        prefix_t *prefix;
        static char *keywords[] = { "network", "masklen", "packed", NULL };
        PyObject *node_obj;

        char *addr = NULL, *packed = NULL;
        long prefixlen = -1;
        Py_ssize_t packlen = -1;

        if (!PyArg_ParseTupleAndKeywords(args, kw_args, "|zlz#:add", keywords,
            &addr, &prefixlen, &packed, &packlen))
                return NULL;
        if ((prefix = args_to_prefix(NULL, addr, packed, packlen, prefixlen)) == NULL)
                return NULL;

        node_obj = create_add_node(self, prefix);
        Deref_Prefix(prefix);

        return node_obj;
}

PyDoc_STRVAR(Radix_delete_doc,
"Radix.delete(network[, masklen][, packed] -> None\n\
\n\
Deletes the specified network from the radix tree.");

static PyObject *
Radix_delete(RadixObject *self, PyObject *args, PyObject *kw_args)
{
        radix_node_t *node;
        RadixNodeObject *node_obj;
        prefix_t lprefix, *prefix;
        static char *keywords[] = { "network", "masklen", "packed", NULL };

        char *addr = NULL, *packed = NULL;
        long prefixlen = -1;
        Py_ssize_t packlen = -1;

        if (!PyArg_ParseTupleAndKeywords(args, kw_args, "|zlz#:delete", keywords,
            &addr, &prefixlen, &packed, &packlen))
                return NULL;
        if ((prefix = args_to_prefix(&lprefix, addr, packed, packlen, prefixlen)) == NULL)
                return NULL;
        if ((node = radix_search_exact(self->rt, prefix)) == NULL) {
                PyErr_SetString(PyExc_KeyError, "no such address");
                return NULL;
        }
        if (node->data != NULL) {
                node_obj = node->data;
                node_obj->rn = NULL;
                Py_XDECREF(node_obj);
        }

        radix_remove(self->rt, node);

        self->gen_id++;
        Py_INCREF(Py_None);
        return Py_None;
}

PyDoc_STRVAR(Radix_search_exact_doc,
"Radix.search_exact(network[, masklen][, packed] -> RadixNode or None\n\
\n\
Search for the specified network in the radix tree. In order to\n\
match, the 'prefix' must be specified exactly. Contrast with the\n\
Radix.search_best method.\n\
\n\
If no match is found, then this method returns None.");

static PyObject *
Radix_search_exact(RadixObject *self, PyObject *args, PyObject *kw_args)
{
        radix_node_t *node;
        RadixNodeObject *node_obj;
        prefix_t lprefix, *prefix;
        static char *keywords[] = { "network", "masklen", "packed", NULL };

        char *addr = NULL, *packed = NULL;
        long prefixlen = -1;
        Py_ssize_t packlen = -1;

        if (!PyArg_ParseTupleAndKeywords(args, kw_args, "|zlz#:search_exact", keywords,
            &addr, &prefixlen, &packed, &packlen))
                return NULL;
        if ((prefix = args_to_prefix(&lprefix, addr, packed, packlen, prefixlen)) == NULL)
                return NULL;

        node = radix_search_exact(self->rt, prefix);
        if (node == NULL || node->data == NULL) {
                Py_INCREF(Py_None);
                return Py_None;
        }
        node_obj = node->data;
        Py_XINCREF(node_obj);
        return (PyObject *)node_obj;
}

PyDoc_STRVAR(Radix_search_best_doc,
"Radix.search_best(network[, masklen][, packed] -> None\n\
\n\
Search for the specified network in the radix tree.\n\
\n\
search_best will return the best (longest) entry that includes the\n\
specified 'prefix', much like a IP routing table lookup.\n\
\n\
If no match is found, then returns None.");

static PyObject *
Radix_search_best(RadixObject *self, PyObject *args, PyObject *kw_args)
{
        radix_node_t *node;
        RadixNodeObject *node_obj;
        prefix_t lprefix, *prefix;
        static char *keywords[] = { "network", "masklen", "packed", NULL };

        char *addr = NULL, *packed = NULL;
        long prefixlen = -1;
        Py_ssize_t packlen = -1;

        if (!PyArg_ParseTupleAndKeywords(args, kw_args, "|zlz#:search_best", keywords,
            &addr, &prefixlen, &packed, &packlen))
                return NULL;
        if ((prefix = args_to_prefix(&lprefix, addr, packed, packlen, prefixlen)) == NULL)
                return NULL;

        if ((node = radix_search_best(self->rt, prefix)) == NULL || 
            node->data == NULL) {
                Py_INCREF(Py_None);
                return Py_None;
        }
        node_obj = node->data;
        Py_XINCREF(node_obj);
        return (PyObject *)node_obj;
}

PyDoc_STRVAR(Radix_search_worst_doc,
"Radix.search_worst(network[, masklen][, packed] -> None\n\
\n\
Search for the specified network in the radix tree.\n\
\n\
search_worst will return the worst (shortest) entry that includes the\n\
specified 'prefix', much like opposite of a IP routing table lookup.\n\
\n\
If no match is found, then returns None.");

static PyObject *
Radix_search_worst(RadixObject *self, PyObject *args, PyObject *kw_args)
{
        radix_node_t *node;
        RadixNodeObject *node_obj;
        prefix_t lprefix, *prefix;
        static char *keywords[] = { "network", "masklen", "packed", NULL };

        char *addr = NULL, *packed = NULL;
        long prefixlen = -1;
        Py_ssize_t packlen = -1;

        if (!PyArg_ParseTupleAndKeywords(args, kw_args, "|zlz#:search_worst", keywords,
            &addr, &prefixlen, &packed, &packlen))
                return NULL;
        if ((prefix = args_to_prefix(&lprefix, addr, packed, packlen, prefixlen)) == NULL)
                return NULL;

        if ((node = radix_search_worst(self->rt, prefix)) == NULL || 
            node->data == NULL) {
                Py_INCREF(Py_None);
                return Py_None;
        }
        node_obj = node->data;
        Py_XINCREF(node_obj);
        return (PyObject *)node_obj;
}

static int
add_node_to_list(radix_node_t *node, void *arg)
{
        PyObject *ret = arg;

        if (node->data != NULL)
                PyList_Append(ret, ((RadixNodeObject *)node->data));
        return (0);
}

PyDoc_STRVAR(Radix_search_covered_doc,
"Radix.search_covered(network[, masklen][, packed] -> None\n\
\n\
FIXME");

static PyObject *
Radix_search_covered(RadixObject *self, PyObject *args, PyObject *kw_args)
{
        prefix_t lprefix, *prefix;
        PyObject *ret;

        static char *keywords[] = { "network", "masklen", "packed", NULL };

        char *addr = NULL, *packed = NULL;
        long prefixlen = -1;
        Py_ssize_t packlen = -1;

        if (!PyArg_ParseTupleAndKeywords(args, kw_args, "|zlz#:search_covered", keywords, &addr, &prefixlen, &packed, &packlen))
                return NULL;

        if ((prefix = args_to_prefix(&lprefix, addr, packed, packlen, prefixlen)) == NULL)
                return NULL;

        if ((ret = PyList_New(0)) == NULL)
                return NULL;

        radix_search_covered(self->rt, prefix, add_node_to_list, ret, 1);

        return (ret);
}

PyDoc_STRVAR(Radix_search_covering_doc,
"Radix.search_covering(network[, masklen][, packed] -> List of RadixNode\n\
\n\
Returns a list of nodes with a less specific or same prefix than network.\n\
\n\
If no match is found, then returns an empty list.");

static PyObject *
Radix_search_covering(RadixObject *self, PyObject *args, PyObject *kw_args)
{
        prefix_t lprefix, *prefix;
        PyObject *ret;

        static char *keywords[] = { "network", "masklen", "packed", NULL };

        char *addr = NULL, *packed = NULL;
        long prefixlen = -1;
        Py_ssize_t packlen = -1;

        if (!PyArg_ParseTupleAndKeywords(args, kw_args, "|zlz#:search_covering", keywords, &addr, &prefixlen, &packed, &packlen)) {
                return NULL;
        }

        if ((prefix = args_to_prefix(&lprefix, addr, packed, packlen, prefixlen)) == NULL) {
                return NULL;
        }

        if ((ret = PyList_New(0)) == NULL) {
                return NULL;
        }

        radix_search_covering(self->rt, prefix, add_node_to_list, ret);

        return ret;
}

PyDoc_STRVAR(Radix_nodes_doc,
"Radix.nodes(prefix) -> List of RadixNode\n\
\n\
Returns a list containing all the RadixNode objects that have been\n\
entered into the tree. This list may be empty if no prefixes have\n\
been entered.");

static PyObject *
Radix_nodes(RadixObject *self, PyObject *args)
{
        radix_node_t *node;
        PyObject *ret;

        if (!PyArg_ParseTuple(args, ":nodes"))
                return NULL;

        if ((ret = PyList_New(0)) == NULL)
                return NULL;

        RADIX_TREE_WALK(self->rt, node) {
                if (node->data != NULL)
                        PyList_Append(ret, (PyObject *)node->data);
        } RADIX_TREE_WALK_END;

        return (ret);
}

PyDoc_STRVAR(Radix_prefixes_doc,
"Radix.prefixes(prefix) -> List of prefix strings\n\
\n\
Returns a list containing all the prefixes that have been entered\n\
into the tree. This list may be empty if no prefixes have been\n\
entered.");

static PyObject *
Radix_prefixes(RadixObject *self, PyObject *args)
{
        radix_node_t *node;
        PyObject *ret;

        if (!PyArg_ParseTuple(args, ":prefixes"))
                return NULL;

        if ((ret = PyList_New(0)) == NULL)
                return NULL;

        RADIX_TREE_WALK(self->rt, node) {
                if (node->data != NULL) {
                        PyList_Append(ret,
                            ((RadixNodeObject *)node->data)->prefix);
                }
        } RADIX_TREE_WALK_END;

        return (ret);
}

static PyObject *
Radix_getiter(RadixObject *self)
{
        return (PyObject *)newRadixIterObject(self);
}

PyDoc_STRVAR(Radix_doc, "Radix tree");

static PyMethodDef Radix_methods[] = {
        {"add",         (PyCFunction)Radix_add,         METH_VARARGS|METH_KEYWORDS,     Radix_add_doc           },
        {"delete",      (PyCFunction)Radix_delete,      METH_VARARGS|METH_KEYWORDS,     Radix_delete_doc        },
        {"search_exact",(PyCFunction)Radix_search_exact,METH_VARARGS|METH_KEYWORDS,     Radix_search_exact_doc  },
        {"search_best", (PyCFunction)Radix_search_best, METH_VARARGS|METH_KEYWORDS,     Radix_search_best_doc   },
        {"search_worst", (PyCFunction)Radix_search_worst, METH_VARARGS|METH_KEYWORDS,     Radix_search_worst_doc   },
        {"search_covered", (PyCFunction)Radix_search_covered, METH_VARARGS|METH_KEYWORDS,     Radix_search_covered_doc   },
        {"search_covering",(PyCFunction)Radix_search_covering, METH_VARARGS|METH_KEYWORDS,     Radix_search_covering_doc },
        {"nodes",       (PyCFunction)Radix_nodes,       METH_VARARGS,                   Radix_nodes_doc         },
        {"prefixes",    (PyCFunction)Radix_prefixes,    METH_VARARGS,                   Radix_prefixes_doc      },
        {NULL,          NULL}           /* sentinel */
};

static PyTypeObject Radix_Type = {
        /* The ob_type field must be initialized in the module init function
         * to be portable to Windows without using C++. */
        PyVarObject_HEAD_INIT(NULL, 0)
        "_radix.Radix",          /*tp_name*/
        sizeof(RadixObject),    /*tp_basicsize*/
        0,                      /*tp_itemsize*/
        /* methods */
        (destructor)Radix_dealloc, /*tp_dealloc*/
        0,                      /*tp_print*/
        0,                      /*tp_getattr*/
        0,                      /*tp_setattr*/
        0,                      /*tp_compare*/
        0,                      /*tp_repr*/
        0,                      /*tp_as_number*/
        0,                      /*tp_as_sequence*/
        0,                      /*tp_as_mapping*/
        0,                      /*tp_hash*/
        0,                      /*tp_call*/
        0,                      /*tp_str*/
        0,                      /*tp_getattro*/
        0,                      /*tp_setattro*/
        0,                      /*tp_as_buffer*/
        Py_TPFLAGS_DEFAULT,     /*tp_flags*/
        Radix_doc,              /*tp_doc*/
        0,                      /*tp_traverse*/
        0,                      /*tp_clear*/
        0,                      /*tp_richcompare*/
        0,                      /*tp_weaklistoffset*/
        (getiterfunc)Radix_getiter, /*tp_iter*/
        0,                      /*tp_iternext*/
        Radix_methods,          /*tp_methods*/
        0,                      /*tp_members*/
        0,                      /*tp_getset*/
        0,                      /*tp_base*/
        0,                      /*tp_dict*/
        0,                      /*tp_descr_get*/
        0,                      /*tp_descr_set*/
        0,                      /*tp_dictoffset*/
        0,                      /*tp_init*/
        0,                      /*tp_alloc*/
        0,                      /*tp_new*/
        0,                      /*tp_free*/
        0,                      /*tp_is_gc*/
};

/* ------------------------------------------------------------------------ */

/* RadixIter: radix tree iterator */

typedef struct _RadixIterObject {
        PyObject_HEAD
        RadixObject *parent;
        radix_node_t *iterstack[RADIX_MAXBITS+1];
        radix_node_t **sp;
        radix_node_t *rn;
        int af;
        unsigned int gen_id;    /* Detect tree modifications */
} RadixIterObject;

static PyTypeObject RadixIter_Type;

static RadixIterObject *
newRadixIterObject(RadixObject *parent)
{
        RadixIterObject *self;

        self = PyObject_New(RadixIterObject, &RadixIter_Type);
        if (self == NULL)
                return NULL;

        self->parent = parent;
        Py_XINCREF(self->parent);

        self->sp = self->iterstack;
        self->rn = self->parent->rt->head_ipv4;
        self->gen_id = self->parent->gen_id;
        self->af = AF_INET;
        return self;
}

/* RadixIter methods */

static void
RadixIter_dealloc(RadixIterObject *self)
{
        Py_XDECREF(self->parent);
        PyObject_Del(self);
}

static PyObject *
RadixIter_iternext(RadixIterObject *self)
{
        radix_node_t *node;
        PyObject *ret;

        if (self->gen_id != self->parent->gen_id) {
                PyErr_SetString(PyExc_RuntimeWarning,
                    "Radix tree modified during iteration");
                return (NULL);
        }

 again:
        if ((node = self->rn) == NULL) {
                /* We have walked both trees */
                if (self->af == AF_INET6)
                        return NULL;
                /* Otherwise reset and start walk of IPv6 tree */
                self->sp = self->iterstack;
                self->rn = self->parent->rt->head_ipv6;
                self->af = AF_INET6;
                goto again;
        }

        /* Get next node */
        if (self->rn->l) {
                if (self->rn->r)
                        *self->sp++ = self->rn->r;
                self->rn = self->rn->l;
        } else if (self->rn->r)
                self->rn = self->rn->r;
        else if (self->sp != self->iterstack)
                self->rn = *(--self->sp);
        else
                self->rn = NULL;

        if (node->prefix == NULL || node->data == NULL)
                goto again;

        ret = node->data;
        Py_INCREF(ret);
        return (ret);
}

PyDoc_STRVAR(RadixIter_doc, 
"Radix tree iterator");

static PyTypeObject RadixIter_Type = {
        /* The ob_type field must be initialized in the module init function
         * to be portable to Windows without using C++. */
        PyVarObject_HEAD_INIT(NULL, 0)
        "_radix.RadixIter",      /*tp_name*/
        sizeof(RadixIterObject),/*tp_basicsize*/
        0,                      /*tp_itemsize*/
        /* methods */
        (destructor)RadixIter_dealloc, /*tp_dealloc*/
        0,                      /*tp_print*/
        0,                      /*tp_getattr*/
        0,                      /*tp_setattr*/
        0,                      /*tp_compare*/
        0,                      /*tp_repr*/
        0,                      /*tp_as_number*/
        0,                      /*tp_as_sequence*/
        0,                      /*tp_as_mapping*/
        0,                      /*tp_hash*/
        0,                      /*tp_call*/
        0,                      /*tp_str*/
        0,                      /*tp_getattro*/
        0,                      /*tp_setattro*/
        0,                      /*tp_as_buffer*/
        Py_TPFLAGS_DEFAULT,     /*tp_flags*/
        RadixIter_doc,          /*tp_doc*/
        0,                      /*tp_traverse*/
        0,                      /*tp_clear*/
        0,                      /*tp_richcompare*/
        0,                      /*tp_weaklistoffset*/
        0,                      /*tp_iter*/
        (iternextfunc)RadixIter_iternext, /*tp_iternext*/
        0,                      /*tp_methods*/
        0,                      /*tp_members*/
        0,                      /*tp_getset*/
        0,                      /*tp_base*/
        0,                      /*tp_dict*/
        0,                      /*tp_descr_get*/
        0,                      /*tp_descr_set*/
        0,                      /*tp_dictoffset*/
        0,                      /*tp_init*/
        0,                      /*tp_alloc*/
        0,                      /*tp_new*/
        0,                      /*tp_free*/
        0,                      /*tp_is_gc*/
};

/* ------------------------------------------------------------------------ */

/* Radix object creator */

PyDoc_STRVAR(radix_Radix_doc,
"Radix() -> new Radix tree object\n\
\n\
Instantiate a new radix tree object.");

static PyObject *
radix_Radix(PyObject *self, PyObject *args)
{
        RadixObject *rv;

        if (!PyArg_ParseTuple(args, ":Radix"))
                return NULL;
        rv = newRadixObject();
        if (rv == NULL)
                return NULL;
        return (PyObject *)rv;
}

static PyMethodDef radix_methods[] = {
        {"Radix",       radix_Radix,    METH_VARARGS,   radix_Radix_doc },
        {NULL,          NULL}           /* sentinel */
};

PyDoc_STRVAR(module_doc,
"Implementation of a radix tree data structure for network prefixes.\n"
"\n"
"The radix tree is the data structure most commonly used for routing\n"
"table lookups. It efficiently stores network prefixes of varying\n"
"lengths and allows fast lookups of containing networks.\n"
"\n"
"Simple example:\n"
"\n"
"       import radix\n"
"       import socket\n"
"\n"
"       # Create a new tree\n"
"       rtree = radix.Radix()\n"
"\n"
"       # Adding a node returns a RadixNode object. You can create\n"
"       # arbitrary members in its 'data' dict to store your data\n"
"       rnode = rtree.add(\"10.0.0.0/8\")\n"
"       rnode.data[\"blah\"] = \"whatever you want\"\n"
"\n"
"       # You can specify nodes as CIDR addresses, or networks with\n"
"       # separate mask lengths. The following three invocations are\n"
"       # identical:\n"
"       rnode = rtree.add(\"10.0.0.0/16\")\n"
"       rnode = rtree.add(\"10.0.0.0\", 16)\n"
"       rnode = rtree.add(network = \"10.0.0.0\", masklen = 16)\n"
"\n"
"       # It is also possible to specify nodes using binary packed\n"
"       # addresses, such as those returned by the socket module\n"
"       # functions. In this case, the radix module will assume that\n"
"       # a four-byte address is an IPv4 address and a sixteen-byte\n"
"       # address is an IPv6 address. For example:\n"
"       binary_addr = inet_aton(\"172.18.22.0\")\n"
"       rnode = rtree.add(packed = binary_addr, masklen = 23)\n"
"\n"
"       # Exact search will only return prefixes you have entered\n"
"       # You can use all of the above ways to specify the address\n"
"       rnode = rtree.search_exact(\"10.0.0.0/8\")\n"
"       # Get your data back out\n"
"       print rnode.data[\"blah\"]\n"
"       # Use a packed address\n"
"       addr = socket.inet_aton(\"10.0.0.0\")\n"
"       rnode = rtree.search_exact(packed = addr, masklen = 8)\n"
"\n"
"       # Best-match search will return the longest matching prefix\n"
"       # that contains the search term (routing-style lookup)\n"
"       rnode = rtree.search_best(\"10.123.45.6\")\n"
"\n"
"       # Worst-match search will return the shortest matching prefix\n"
"       # that contains the search term (inverse routing-style lookup)\n"
"       rnode = rtree.search_worst(\"10.123.45.6\")\n"
"\n"
"       # There are a couple of implicit members of a RadixNode:\n"
"       print rnode.network     # -> \"10.0.0.0\"\n"
"       print rnode.prefix      # -> \"10.0.0.0/8\"\n"
"       print rnode.prefixlen   # -> 8\n"
"       print rnode.family      # -> socket.AF_INET\n"
"       print rnode.packed      # -> '\\n\\x00\\x00\\x00'\n"
"       print rnode.parent      # -> None\n"
"\n"
"       # IPv6 prefixes are fully supported in the same tree\n"
"       rnode = rtree.add(\"2001:DB8::/32\")\n"
"       rnode = rtree.add(\"::/0\")\n"
"\n"
"       # Use the nodes() method to return all RadixNodes created\n"
"       nodes = rtree.nodes()\n"
"       for rnode in nodes:\n"
"               print rnode.prefix\n"
"\n"
"       # The prefixes() method will return all the prefixes (as a\n"
"       # list of strings) that have been entered\n"
"       prefixes = rtree.prefixes()\n"
"\n"
"       # You can also directly iterate over the tree itself\n"
"       # this would save some memory if the tree is big\n"
"       # NB. Don't modify the tree (add or delete nodes) while\n"
"       # iterating otherwise you will abort the iteration and\n"
"       # receive a RuntimeWarning. Changing a node's data dict\n"
"       # is permitted.\n"
"       for rnode in rtree:\n"
"               print rnode.prefix\n"
);

#if PY_MAJOR_VERSION >= 3
static struct PyModuleDef radix_module_def = {
    PyModuleDef_HEAD_INIT,
    "_radix",
    module_doc,
    -1,
    radix_methods,                         // methods
    NULL,                                  // m_reload
    NULL,                                  // traverse
    NULL,                                  // clear
    NULL                                   // free
};
#endif

static PyObject *module_initialize(void)
{
        PyObject *m, *d;

        if (PyType_Ready(&Radix_Type) < 0)
                return NULL;
        if (PyType_Ready(&RadixNode_Type) < 0)
                return NULL;
#if PY_MAJOR_VERSION >= 3
        m = PyModule_Create(&radix_module_def);
#else
        m = Py_InitModule3("_radix", radix_methods, module_doc);
#endif

        /* Stash the callable constructor for use in Radix.__reduce__ */
        d = PyModule_GetDict(m);
        radix_constructor = PyDict_GetItemString(d, "Radix");

        PyModule_AddIntConstant(m, "__accelerator__", 1);

        return m;
}

#if PY_MAJOR_VERSION >= 3
PyMODINIT_FUNC PyInit__radix(void)
{
        return module_initialize();
}
#else
PyMODINIT_FUNC init_radix(void)
{
        module_initialize();
}
#endif
