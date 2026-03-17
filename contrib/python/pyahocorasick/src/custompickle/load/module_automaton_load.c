#include "module_automaton_load.h"

#include "../../Automaton.h"
#include "loadbuffer.h"


// --- public -----------------------------------------------------------

static bool
automaton_load_impl(Automaton* automaton, const char* path, PyObject* deserializer);

PyObject*
module_automaton_load(PyObject* module, PyObject* args) {

    SaveLoadParameters params;
    Automaton* automaton;
    int ret;

    automaton = (Automaton*)automaton_create();
    if (UNLIKELY(automaton == NULL)) {
        return NULL;
    }

    if (UNLIKELY(!automaton_save_load_parse_args(automaton->store, args, &params))) {
        Py_DECREF(automaton);
        return NULL;
    }

    ret = automaton_load_impl(automaton, PyBytes_AsString(params.path), params.callback);
    Py_DECREF(params.path);

    if (LIKELY(ret))
        return (PyObject*)automaton;
    else
        return NULL;
}

// ----private ----------------------------------------------------------

static bool
automaton_load_node(LoadBuffer* input);

static TrieNode*
automaton_load_fixup_pointers(LoadBuffer* input);

static bool
automaton_load_impl(Automaton* automaton, const char* path, PyObject* deserializer) {

    TrieNode* root;
    LoadBuffer input;
    CustompickleHeader header;
    CustompickleFooter footer;
    size_t i;

    if (!loadbuffer_open(&input, path, deserializer)) {
        return false;
    }

    if (!loadbuffer_init(&input, &header, &footer)) {
        goto exception;
    }

    if (header.data.kind == TRIE || header.data.kind == AHOCORASICK) {
        for (i=0; i < input.capacity; i++) {
            if (UNLIKELY(!automaton_load_node(&input))) {
                goto exception;
            }
        }

        root = automaton_load_fixup_pointers(&input);
        if (UNLIKELY(root == NULL)) {
            goto exception;
        }
    } else if (header.data.kind == EMPTY) {

        root = NULL;

    } else {
        PyErr_SetString(PyExc_ValueError, "automaton kind save in file is invalid");
        goto exception;
    }

    loadbuffer_close(&input);

    // setup object
    automaton->kind          = header.data.kind;
    automaton->store         = header.data.store;
    automaton->key_type      = header.data.key_type;
    automaton->count         = header.data.words_count;
    automaton->longest_word  = header.data.longest_word;
    automaton->version       = 0;
    automaton->stats.version = -1;
    automaton->root          = root;

    return true;

exception:
    loadbuffer_close(&input);
    return false;
}

static bool
automaton_load_node(LoadBuffer* input) {

    PyObject* bytes; // XXX: it might be reused (i.e. be part of input)
    PyObject* object;
    TrieNode* original;
    TrieNode* node;
    size_t size;
    int ret;

    // 1. get original address of upcoming node
    ret = loadbuffer_loadinto(input, &original, TrieNode*);
    if (UNLIKELY(!ret)) {
        return false;
    }

    // 2. load node data
    node = (TrieNode*)memory_alloc(sizeof(TrieNode));
    if (UNLIKELY(node == NULL)) {
        PyErr_NoMemory();
        return false;
    }

    ret = loadbuffer_load(input, (char*)node, PICKLE_TRIENODE_SIZE);
    if (UNLIKELY(!ret)) {
        memory_free(node);
        return false;
    }

    node->next = NULL;

    // 3. load next pointers
    if (node->n > 0) {
        size = sizeof(Pair) * node->n;
        node->next = (Pair*)memory_alloc(size);
        if (UNLIKELY(node->next == NULL)) {
            PyErr_NoMemory();
            goto exception;
        }

        ret = loadbuffer_load(input, (char*)(node->next), size);
        if (UNLIKELY(!ret)) {
            goto exception;
        }
    }

    // 4. load custom python object
    if (node->eow && input->store == STORE_ANY) {
        size = (size_t)(node->output.integer);
        bytes = F(PyBytes_FromStringAndSize)(NULL, size);
        if (UNLIKELY(bytes == NULL)) {
            goto exception;
        }

        ret = loadbuffer_load(input, PyBytes_AS_STRING(bytes), size);
        if (UNLIKELY(!ret)) {
            Py_DECREF(bytes);
            goto exception;
        }

        object = F(PyObject_CallFunction)(input->deserializer, "O", bytes);
        if (UNLIKELY(object == NULL)) {
            Py_DECREF(bytes);
            goto exception;
        }

        node->output.object = object;
        Py_DECREF(bytes);
    }

    input->lookup[input->size].original = original;
    input->lookup[input->size].current  = node;
    input->size += 1;

    return true;

exception:
    memory_safefree(node->next);
    memory_free(node);

    return false;
}


static int
addresspair_cmp(const void* a, const void *b) {
    const TrieNode* Aptr;
    const TrieNode* Bptr;
    uintptr_t A;
    uintptr_t B;

    Aptr = ((AddressPair*)a)->original;
    Bptr = ((AddressPair*)b)->original;

    A = (uintptr_t)Aptr;
    B = (uintptr_t)Bptr;

    if (A < B) {
        return -1;
    } else if (A > B) {
        return +1;
    } else {
        return 0;
    }
}


static TrieNode*
lookup_address(LoadBuffer* input, TrieNode* original) {

    AddressPair* pair;

    pair = (AddressPair*)bsearch(&original,
                                 input->lookup,
                                 input->size,
                                 sizeof(AddressPair),
                                 addresspair_cmp);

    if (LIKELY(pair != NULL)) {
        return pair->current;
    } else {
        return NULL;
    }
}


static bool
automaton_load_fixup_node(LoadBuffer* input, TrieNode* node) {

    size_t i;

    if (input->kind == AHOCORASICK && node->fail != NULL) {
        node->fail = lookup_address(input, node->fail);
        if (UNLIKELY(node->fail == NULL)) {
            return false;
        }
    }

    if (node->n > 0) {
        for (i=0; i < node->n; i++) {
            node->next[i].child = lookup_address(input, node->next[i].child);
            if (UNLIKELY(node->next[i].child == NULL)) {
                return false;
            }
        }
    }

    return true;
}


static TrieNode*
automaton_load_fixup_pointers(LoadBuffer* input) {

    TrieNode* root;
    TrieNode* node;
    size_t i;

    ASSERT(input != NULL);

    // 1. root is the first node stored in the array
    root = input->lookup[0].current;

    // 2. sort array to make it bsearch-able
    qsort(input->lookup, input->size, sizeof(AddressPair), addresspair_cmp);

    // 3. convert all next and fail pointers to current pointers
    for (i=0; i < input->size; i++) {
        node = input->lookup[i].current;
        if (UNLIKELY(!automaton_load_fixup_node(input, node))) {
            PyErr_Format(PyExc_ValueError, "Detected malformed pointer during unpickling node %lu", i);
            return NULL;
        }
    }

    loadbuffer_invalidate(input);

    return root;
}
