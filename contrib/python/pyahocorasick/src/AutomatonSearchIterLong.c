/*
    This is part of pyahocorasick Python module.
    
    AutomatonSearchIterLong implementation

    Author    : Wojciech Muła, wojciech_mula@poczta.onet.pl
    License   : 3-clauses BSD (see LICENSE)
*/

#include "AutomatonSearchIterLong.h"

static PyTypeObject automaton_search_iter_long_type;

static PyObject*
automaton_search_iter_long_new(
    Automaton* automaton,
    PyObject* object,
    int start,
    int end
) {
    AutomatonSearchIterLong* iter;

    iter = (AutomatonSearchIterLong*)PyObject_New(AutomatonSearchIterLong, &automaton_search_iter_long_type);
    if (iter == NULL)
        return NULL;

    iter->automaton = automaton;
    iter->version   = automaton->version;
    iter->object    = object;

    iter->state = automaton->root;
    iter->shift = 0;
    iter->index = start - 1;    // -1 because first instruction in next() increments index
    iter->end   = end;

    iter->last_index = -1;
    iter->last_node  = NULL;

    Py_INCREF(iter->automaton);
    Py_INCREF(iter->object);

    init_input(&iter->input);
    if (!prepare_input((PyObject*)automaton, object, &iter->input)) {
        goto error;
    }

    return (PyObject*)iter;
error:
    Py_DECREF(iter);
    return NULL;
}

#define iter ((AutomatonSearchIterLong*)self)

static void
automaton_search_iter_long_del(PyObject* self) {
    Py_DECREF(iter->automaton);
    Py_DECREF(iter->object);
    destroy_input(&iter->input);
    PyObject_Del(self);
}


static PyObject*
automaton_search_iter_long_iter(PyObject* self) {
    Py_INCREF(self);
    return self;
}


static PyObject*
automaton_build_output_iter_long(PyObject* self) {

    switch (iter->automaton->store) {
        case STORE_LENGTH:
        case STORE_INTS:
            return Py_BuildValue("ii", iter->shift + iter->last_index, iter->last_node->output.integer);

        case STORE_ANY:
            return Py_BuildValue("iO", iter->shift + iter->last_index, iter->last_node->output.object);

        default:
            PyErr_SetString(PyExc_ValueError, "inconsistent internal state!");
            return NULL;
    }
}


static PyObject*
automaton_search_iter_long_next(PyObject* self) {
    PyObject* output = NULL;
    TrieNode* next;
    TrieNode* fail_node = NULL; //< the node to fail over
    int fail_index = -1;        //< the index to fail over
    uint8_t fail_flag = false;  //< whether this iteration is triggered by failover

    if (iter->version != iter->automaton->version) {
        PyErr_SetString(PyExc_ValueError, "underlaying automaton has changed, iterator is not valid anymore");
        return NULL;
    }

return_output:
    if (iter->last_node) {
        Py_XDECREF(output); // Release any existing reference to avoid memory leak
        output = automaton_build_output_iter_long(self);

        // start over, as we don't want overlapped results
        // Note: this leads to quadratic complexity in the worst case
        iter->state      = iter->automaton->root;
        iter->index      = iter->last_index;

        iter->last_node  = NULL;
        iter->last_index = -1;

        return output;
    }

    iter->index += 1;
    while (iter->index < iter->end) {
        if (fail_flag) {
            // when failover start from fail node instead of next
            next = fail_node->fail;
            iter->index = fail_index;
            fail_node = NULL;
            fail_index = -1;
            fail_flag = false;
        } else {
            next = trienode_get_next(iter->state, iter->input.word[iter->index]);
        }
        if (next) {
            if (next->eow) {
                // save the last node on the path
                iter->last_node  = next;
                iter->last_index = iter->index;
            } else if (!iter->last_node && !fail_node && next->fail && next->fail != iter->automaton->root && next->fail->eow) {
                fail_node = next;
                fail_index = iter->index;
            }

            iter->state = next;
            iter->index += 1;
        } else {
            if (iter->last_node) {
                goto return_output;
            } else {
                if (fail_node) {
                  // failover
                  fail_flag = true;
                  continue;
                }
                while (true) {
                    iter->state = iter->state->fail;
                    if (iter->state == NULL) {
                        iter->state = iter->automaton->root;
                        iter->index += 1;
                        break;
                    } else if (trienode_get_next(iter->state, iter->input.word[iter->index])) {
                        break;
                    }
                }
            }
        }
    } // while 

    if (iter->last_node) {
        goto return_output;
    }
    
    return NULL;    // StopIteration
}


static PyObject*
automaton_search_iter_long_set(PyObject* self, PyObject* args) {
    PyObject* object;
    PyObject* flag;
    bool reset;
    struct Input new_input;

    // first argument - required string or buffer
    object = PyTuple_GetItem(args, 0);
    if (object) {
        init_input(&new_input);
        if (!prepare_input((PyObject*)iter->automaton, object, &new_input)) {
            return NULL;
        }
    }
    else
        return NULL;

    // second argument - optional bool
    flag = PyTuple_GetItem(args, 1);
    if (flag) {
        switch (PyObject_IsTrue(flag)) {
            case 0:
                reset = false;
                break;
            case 1:
                reset = true;
                break;
            default:
                return NULL;
        }
    }
    else {
        PyErr_Clear();
        reset = false;
    }

    // update internal state
    Py_XDECREF(iter->object);
    Py_INCREF(object);
    iter->object    = object;

    destroy_input(&iter->input);
    assign_input(&iter->input, &new_input);

    if (!reset)
        iter->shift += (iter->index >= 0) ? iter->index : 0;

    iter->index     = -1;
    iter->end       = new_input.wordlen;

    if (reset) {
        iter->state  = iter->automaton->root;
        iter->shift  = 0;

        iter->last_node  = NULL;
        iter->last_index = -1;
    }

    Py_RETURN_NONE;
}


#undef iter

#define method(name, kind) {#name, automaton_search_iter_long_##name, kind, ""}

static
PyMethodDef automaton_search_iter_long_methods[] = {
    method(set, METH_VARARGS),

    {NULL, NULL, 0, NULL}
};
#undef method


static PyTypeObject automaton_search_iter_long_type = {
    PY_OBJECT_HEAD_INIT
    "ahocorasick.AutomatonSearchIterLong",      /* tp_name */
    sizeof(AutomatonSearchIterLong),            /* tp_size */
    0,                                          /* tp_itemsize? */
    (destructor)automaton_search_iter_long_del, /* tp_dealloc */
    0,                                          /* tp_print */
    0,                                          /* tp_getattr */
    0,                                          /* tp_setattr */
    0,                                          /* tp_reserved */
    0,                                          /* tp_repr */
    0,                                          /* tp_as_number */
    0,                                          /* tp_as_sequence */
    0,                                          /* tp_as_mapping */
    0,                                          /* tp_hash */
    0,                                          /* tp_call */
    0,                                          /* tp_str */
    PyObject_GenericGetAttr,                    /* tp_getattro */
    0,                                          /* tp_setattro */
    0,                                          /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,                         /* tp_flags */
    0,                                          /* tp_doc */
    0,                                          /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    automaton_search_iter_long_iter,            /* tp_iter */
    automaton_search_iter_long_next,            /* tp_iternext */
    automaton_search_iter_long_methods,         /* tp_methods */
    0,                                          /* tp_members */
    0,                                          /* tp_getset */
    0,                                          /* tp_base */
    0,                                          /* tp_dict */
    0,                                          /* tp_descr_get */
    0,                                          /* tp_descr_set */
    0,                                          /* tp_dictoffset */
    0,                                          /* tp_init */
    0,                                          /* tp_alloc */
    0,                                          /* tp_new */
};
