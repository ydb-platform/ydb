/*
    This is part of pyahocorasick Python module.

    Implementation of pickling/unpickling routines for Automaton class

    Author    : Wojciech Muła, wojciech_mula@poczta.onet.pl
    WWW       : http://0x80.pl
    License   : BSD-3-Clause (see LICENSE)
*/

/*
Pickling (automaton___reduce__):

1. assign sequential numbers to nodes in order to replace
   address with these numbers
   (pickle_dump_replace_fail_with_id)
2. save in array all nodes data in the same order as numbers,
   also replace fail and next links with numbers; collect on
   a list all values (python objects) stored in a trie
   (pickle_dump_save);

   Before we start, all nodes of trie are visited and total
   size of pickled data is calculated. If it is small enough
   (less than given threshold), all data is saved in a single
   byte array. Otherwise, data is saved in several byte arrays.

   In either case, the format of byte array is the same:
   * 8 first bytes is number of nodes stored in this
     chunk of memory
   * the number if followed by some raw data.

   When there is just one byte array, it's size is fit to
   needs. If data is split, then each array has exactly the
   same size of bytes, but not all might be used (only the
   last array is fit).

3. clean up
   (pickle_dump_undo_replace or pickle_dump_revert_replace)

Unpickling (automaton_unpickle, called in Automaton constructor)
1. load all nodes from array
2. make number->node lookup table
3. replace numbers stored in fail and next pointers with
   real pointers, reassign python objects as values
*/


#include <string.h>
#include "pickle/pickle_data.c"

typedef struct NodeID {
    TrieNode* fail;         ///< original fail value
    Py_uintptr_t id;        ///< id
} NodeID;

typedef struct DumpState {
    Py_uintptr_t id;        ///< next id
    size_t total_size;      ///< number of nodes
    TrieNode* failed_on;    ///< if fail while numerating, save node in order
                            ///  to revert changes made in trie
} DumpState;


static size_t
get_pickled_size(TrieNode* node) {
    ASSERT(node != NULL);
    return PICKLE_TRIENODE_SIZE + node->n * sizeof(Pair);
}

// replace fail with pairs (fail, id)
static int
pickle_dump_replace_fail_with_id(TrieNode* node, const int depth, void* extra) {

    NodeID* repl;

    ASSERT(sizeof(NodeID*) <= sizeof(TrieNode*));
#define state ((DumpState*)extra)
    repl = (NodeID*)memory_alloc(sizeof(NodeID));
    if (LIKELY(repl != NULL)) {
        state->id += 1;
        state->total_size += get_pickled_size(node);

        repl->id   = state->id;
        repl->fail = node->fail;

        node->fail = (TrieNode*)repl;
        return 1;
    }
    else {
        // error, revert is needed!
        state->failed_on = node;
        return 0;
    }
#undef state
}


// revert changes in trie (in case of error)
static int
pickle_dump_revert_replace(TrieNode* node, const int depth, void* extra) {
#define state ((DumpState*)extra)
    if (state->failed_on != node) {
        NodeID* repl = (NodeID*)(node->fail);
        node->fail = repl->fail;
        memory_free(repl);

        return 1;
    }
    else
        return 0;
#undef state
}


// revert changes in trie
static int
pickle_dump_undo_replace(TrieNode* node, const int depth, void* extra) {
#define state ((DumpState*)extra)
    NodeID* repl = (NodeID*)(node->fail);
    node->fail = repl->fail;
    memory_free(repl);

    return 1;
#undef state
}


static int
pickle_dump_save(TrieNode* node, const int depth, void* extra) {
#define self ((PickleData*)extra)
#define NODEID(object) ((NodeID*)((TrieNode*)object)->fail)

    TrieNode* dump;
    TrieNode* tmp;
    Pair* arr;
    unsigned i;
    size_t size;

    size = get_pickled_size(node);
    if (UNLIKELY(self->top + size > self->size)) {
        if (UNLIKELY(!pickle_data__add_next_buffer(self))) {
            self->error = true;
            return 0;
        }
    }

    dump = (TrieNode*)(self->data + self->top);

    // we do not save the last pointer in array
    arr = (Pair*)(self->data + self->top + PICKLE_TRIENODE_SIZE);

    // append the python object to the list
    if (node->eow and self->values) {
        if (PyList_Append(self->values, node->output.object) == -1) {
            self->error = true;
            return 0;
        }
    }

    // save node data
    if (self->values)
        dump->output.integer = 0;
    else
        dump->output.integer = node->output.integer;

    dump->n         = node->n;
    dump->eow       = node->eow;

    tmp = NODEID(node)->fail;
    if (tmp)
        dump->fail  = (TrieNode*)(NODEID(tmp)->id);
    else
        dump->fail  = NULL;

    // save array of pointers
    for (i=0; i < node->n; i++) {
        TrieNode* child = trienode_get_ith_unsafe(node, i);
        ASSERT(child);
        arr[i].child  = (TrieNode*)(NODEID(child)->id);    // save the id of child node
        arr[i].letter = trieletter_get_ith_unsafe(node, i);
    }

    self->top       += size;
    (*self->count)  += 1;
    return 1;
#undef NODEID
#undef self
}


static PyObject*
automaton___reduce__(PyObject* self, PyObject* args) {
#define automaton ((Automaton*)self)

#define MB ((size_t)(1024*1024))

    const size_t array_size = 16*MB;

    DumpState   state;
    PickleData  data;
    PyObject*   tuple;

    // 0. for an empty automaton do nothing
    if (automaton->count == 0) {
        // the class constructor feed with an empty argument build an empty automaton
        return F(Py_BuildValue)("O()", Py_TYPE(self));
    }

    // 1. numerate nodes
    state.id        = 0;
    state.failed_on = NULL;
    state.total_size = 0;

    trie_traverse(automaton->root, pickle_dump_replace_fail_with_id, &state);
    if (state.failed_on) {
        // revert changes (partial)
        trie_traverse(automaton->root, pickle_dump_revert_replace, &state);

        // and set error
        PyErr_NoMemory();
        return NULL;
    }

    // 2. gather data
    if (!pickle_data__init(&data, automaton->store, state.total_size, array_size))
        goto exception;

    trie_traverse(automaton->root, pickle_dump_save, &data);
    if (UNLIKELY(data.error)) {
        goto exception;
    }

    if (UNLIKELY(!pickle_data__shrink_last_buffer(&data))) {
        goto exception;
    }

    if (automaton->store != STORE_ANY) { // always pickle a Python object
        data.values = Py_None;
        Py_INCREF(data.values);
    }

    /* 3: save tuple:
        * binary data
        * automaton->kind
        * automaton->store
        * automaton->key_type
        * automaton->count
        * automaton->longest_word
        * list of values
    */

    tuple = F(Py_BuildValue)(
        "O(OiiiiiO)",
        Py_TYPE(self),
        data.bytes_list,
        automaton->kind,
        automaton->store,
        automaton->key_type,
        automaton->count,
        automaton->longest_word,
        data.values
    );

    if (data.values == Py_None) {
        data.values = NULL;
    }

    if (UNLIKELY(tuple == NULL)) {
        goto exception;
    }

    // revert all changes
    trie_traverse(automaton->root, pickle_dump_undo_replace, NULL);

    return tuple;

exception:
    // revert all changes
    trie_traverse(automaton->root, pickle_dump_undo_replace, NULL);

    // and free memory
    pickle_data__cleanup(&data);
    return NULL;
#undef automaton
}


static bool
automaton_unpickle__validate_bytes_list(PyObject* bytes_list, size_t* result) {

    PyObject* bytes;
    Py_ssize_t k;
    Py_ssize_t nodes_count;
    const uint8_t* data;

    size_t count = 0;

    // calculate the total number of nodes (and do validate data at the same time)
    for (k=0; k < PyList_GET_SIZE(bytes_list); k++) {
        bytes = PyList_GET_ITEM(bytes_list, k);
        if (UNLIKELY(!F(PyBytes_CheckExact)(bytes))) {
            PyErr_Format(PyExc_ValueError,
                         "Item #%lu on the bytes list is not a bytes object",
                         k);
            return false;
        }

        data = (const uint8_t*)PyBytes_AS_STRING(bytes);

        nodes_count = *((Py_ssize_t*)data);
        if (UNLIKELY(nodes_count <= 0)) {
            PyErr_Format(PyExc_ValueError,
                         "Nodes count for item #%lu on the bytes list is not positive (%lu)",
                         k, nodes_count);
            return false;
        }

        count += nodes_count;
    }

    *result = count;
    return true;
}


static bool
automaton_unpickle(
    Automaton* automaton,
    PyObject* bytes_list,
    PyObject* values
) {
    TrieNode** id2node = NULL;

    TrieNode* node;
    TrieNode* dump;
    Pair* next;
    PyObject* bytes;
    PyObject* value;
    Py_ssize_t nodes_count;
    Py_ssize_t i;

    unsigned id;
    const uint8_t* data;
    const uint8_t* ptr;
    const uint8_t* end;
    unsigned k;
    size_t j;
    unsigned object_idx = 0;
    size_t index;
    size_t count;

    if (!automaton_unpickle__validate_bytes_list(bytes_list, &count)) {
        goto exception;
    }

    id2node = (TrieNode**)memory_alloc((count+1) * sizeof(TrieNode*));
    if (UNLIKELY(id2node == NULL)) {
        goto no_mem;
    }

    // 1. make nodes
    id = 1;
    for (k=0; k < PyList_GET_SIZE(bytes_list); k++) {
        bytes = PyList_GET_ITEM(bytes_list, k);
        data = (const uint8_t*)PyBytes_AS_STRING(bytes);

        nodes_count = *((Py_ssize_t*)data);

        ptr  = data + PICKLE_CHUNK_COUNTER_SIZE;
        end  = ptr + PyBytes_GET_SIZE(bytes) - PICKLE_CHUNK_COUNTER_SIZE;
        for (i=0; i < nodes_count; i++) {
            if (UNLIKELY(ptr + PICKLE_TRIENODE_SIZE > end)) {
                PyErr_Format(PyExc_ValueError,
                             "Data truncated [parsing header of node #%li]: chunk #%di @ offset %ld, expected at least %lu bytes",
                             i, k, ptr - data, PICKLE_TRIENODE_SIZE);
                goto exception;
            }

            dump = (TrieNode*)(ptr);
            node = (TrieNode*)memory_alloc(sizeof(TrieNode));
            if (LIKELY(node != NULL)) {
                node->output    = dump->output;
                node->fail      = dump->fail;
                node->n         = dump->n;
                node->eow       = dump->eow;
                node->next      = NULL;
            }
            else
                goto no_mem;

            ptr += PICKLE_TRIENODE_SIZE;

            id2node[id++] = node;

            if (node->n > 0) {
                if (UNLIKELY(ptr + node->n * sizeof(Pair) > end)) {
                    PyErr_Format(PyExc_ValueError,
                                "Data truncated [parsing children of node #%lu]: "
                                "chunk #%d @ offset %lu, expected at least %ld bytes",
                                 i, k, ptr - data + i, node->n * sizeof(Pair));

                    goto exception;
                }

                node->next = (Pair*)memory_alloc(node->n * sizeof(Pair));
                if (UNLIKELY(node->next == NULL)) {
                    goto no_mem;
                }

                next = (Pair*)(ptr);
                for (j=0; j < node->n; j++) {
                    node->next[j] = next[j];
                }

                ptr += node->n * sizeof(Pair);
            }
        }
    }

    // 2. restore pointers and references to pyobjects
    for (i=1; i < id; i++) {
        node = id2node[i];

        // references
        if (values and node->eow) {
            value = F(PyList_GetItem)(values, object_idx);
            if (value) {
                Py_INCREF(value);
                node->output.object = value;
                object_idx += 1;
            }
            else
                goto exception;
        }

        // pointers
        if (node->fail) {
            index = (size_t)(node->fail);
            if (LIKELY(index < count + 1)) {
                node->fail = id2node[index];
            } else {
                PyErr_Format(PyExc_ValueError,
                             "Node #%lu malformed: the fail link points to node #%lu, while there are %lu nodes",
                             i - 1, index, count);
                goto exception;
            }
        }

        for (j=0; j < node->n; j++) {
            index = (size_t)(node->next[j].child);
            if (LIKELY(index < count + 1)) {
                node->next[j].child = id2node[index];
            } else {
                PyErr_Format(PyExc_ValueError,
                             "Node #%lu malformed: next link #%lu points to node #%lu, while there are %lu nodes",
                             i - 1, j, index, count);
                goto exception;
            }
        }
    }

    automaton->root = id2node[1];

    memory_free(id2node);
    return 1;

no_mem:
    PyErr_NoMemory();
exception:
    // free memory
    if (id2node) {
        for (i=1; i < id; i++) {
            trienode_free(id2node[i]);
        }

        memory_free(id2node);
    }

    // If there is value list and some of its items were already
    // referenced, release them
    if (values) {
        for (i=0; i < object_idx; i++) {
            Py_XDECREF(F(PyList_GetItem)(values, i));
        }
    }

    return 0;
}

