#include "automaton_save.h"

#include "../custompickle.h"
#include "../pyhelpers.h"
#include "savebuffer.h"


// --- public -----------------------------------------------------------

static bool
automaton_save_impl(Automaton* automaton, const char* path, PyObject* serializer);

PyObject*
automaton_save(PyObject* self, PyObject* args) {

    SaveLoadParameters params;
    Automaton* automaton;
    int ret;

    automaton = (Automaton*)self;

    if (UNLIKELY(!automaton_save_load_parse_args(automaton->store, args, &params))) {
        return NULL;
    }

    ret = automaton_save_impl(automaton, PyBytes_AsString(params.path), params.callback);
    Py_DECREF(params.path);

    if (LIKELY(ret))
        Py_RETURN_NONE;
    else
        return NULL;
}

// --- private ----------------------------------------------------------

static int
automaton_save_node(TrieNode* node, const int depth, void* extra);

static bool
automaton_save_impl(Automaton* automaton, const char* path, PyObject* serializer) {

    CustompickleHeader header;
    CustompickleFooter footer;
    SaveBuffer         output;
    int                ret;

    ret = savebuffer_init(&output,
                          serializer,
                          automaton->store,
                          path,
                          SAVEBUFFER_DEFAULT_SIZE);
    if (!ret)
        return false;

    custompickle_initialize_header(&header, automaton);

    // 1. save header
    savebuffer_store(&output, (const char*)&header, sizeof(header));

    // 2. save nodes
    if (automaton->kind != EMPTY) {
        trie_traverse(automaton->root, automaton_save_node, &output);
        if (UNLIKELY(PyErr_Occurred() != NULL)) {
            goto exception;
        }
    }

    // 3. save footer
    custompickle_initialize_footer(&footer, output.nodes_count);
    savebuffer_store(&output, (const char*)&footer, sizeof(footer));

    savebuffer_finalize(&output);

    return true;

exception:
    savebuffer_finalize(&output);

    return false;
}


static int
automaton_save_node(TrieNode* node, const int depth, void* extra) {

    SaveBuffer* output;
    TrieNode* dump;
    PyObject* bytes;

    output = (SaveBuffer*)extra;

    // 1. save actual address of node
    savebuffer_store_pointer(output, (void*)node);

    // 2. obtain buffer
    dump = (TrieNode*)savebuffer_acquire(output, PICKLE_TRIENODE_SIZE);

    if (output->store != STORE_ANY)
        dump->output.integer = node->output.integer;

    dump->n         = node->n;
    dump->eow       = node->eow;
    dump->fail      = node->fail;

    // 3. pickle python value associated with word
    if (node->eow && output->store == STORE_ANY) {
        bytes = F(PyObject_CallFunctionObjArgs)(output->serializer, node->output.object, NULL);
        if (UNLIKELY(bytes == NULL)) {
            return 0;
        }

        if (UNLIKELY(!F(PyBytes_CheckExact)(bytes))) {
            PyErr_SetString(PyExc_TypeError, "serializer must return bytes object");
            return 0;
        }

        // store the size of buffer in trie node [which is not saved yet in the file]
        *(size_t*)(&dump->output.integer) = PyBytes_GET_SIZE(bytes);
    } else {
        bytes = NULL;
    }

    // 4. save array of pointers
    if (node->n > 0) {
        savebuffer_store(output, (const char*)node->next, node->n * sizeof(Pair));
    }

    // 5. save pickled data, if any
    if (bytes) {
        savebuffer_store(output, PyBytes_AS_STRING(bytes), PyBytes_GET_SIZE(bytes));
        Py_DECREF(bytes);
    }

    output->nodes_count += 1;

    return 1;
}
