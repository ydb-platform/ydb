/*
    This is part of pyahocorasick Python module.

    Automaton class implementation.
    (this file includes Automaton_pickle.c)

    Author    : Wojciech Muła, wojciech_mula@poczta.onet.pl
    WWW       : http://0x80.pl
    License   : BSD-3-Clause (see LICENSE)
*/

#include "Automaton.h"
#include "slist.h"
#include "inline_doc.h"
#include "custompickle/save/automaton_save.h"

static PyTypeObject automaton_type;


static bool
check_store(const int store) {
    switch (store) {
        case STORE_LENGTH:
        case STORE_INTS:
        case STORE_ANY:
            return true;

        default:
            PyErr_SetString(
                PyExc_ValueError,
                "store value must be one of ahocorasick.STORE_LENGTH, STORE_INTS or STORE_ANY"
            );
            return false;
    } // switch
}


static bool
check_kind(const int kind) {
    switch (kind) {
        case EMPTY:
        case TRIE:
        case AHOCORASICK:
            return true;

        default:
            PyErr_SetString(
                PyExc_ValueError,
                "kind value must be one of ahocorasick.EMPTY, TRIE or AHOCORASICK"
            );
            return false;
    }
}


static bool
check_key_type(const int store) {
    switch (store) {
        case KEY_STRING:
        case KEY_SEQUENCE:
            return true;

        default:
            PyErr_SetString(
                PyExc_ValueError,
                "key_type must have value KEY_STRING or KEY_SEQUENCE"
            );
            return false;
    } // switch
}

static PyObject*
automaton_create() {

    Automaton* automaton;

    automaton = (Automaton*)F(PyObject_New)(Automaton, &automaton_type);
    if (UNLIKELY(automaton == NULL)) {
        return NULL;
    }

    automaton->kind = EMPTY;
    automaton->store = STORE_ANY;
    automaton->key_type = KEY_STRING;
    automaton->count = 0;
    automaton->longest_word = 0;

    automaton->version = 0;
    automaton->stats.version = -1;

    automaton->root = NULL;

    return (PyObject*)automaton;
}

static PyObject*
automaton_new(PyTypeObject* self, PyObject* args, PyObject* kwargs) {
    Automaton* automaton;
    int key_type;
    int store;

    automaton = (Automaton*)automaton_create();
    if (UNLIKELY(automaton == NULL))
        return NULL;


    if (UNLIKELY(PyTuple_Size(args) == 7)) {

        int             word_count;
        int             longest_word;
        AutomatonKind   kind;
        KeysStore       store;
        KeyType         key_type;
        PyObject*       bytes_list = NULL;
        PyObject*       values = NULL;

        const char* fmt = "OiiiiiO";

        if (!F(PyArg_ParseTuple)(args, fmt, &bytes_list, &kind, &store, &key_type, &word_count, &longest_word, &values)) {
            PyErr_SetString(PyExc_ValueError, "Unable to load from pickle.");
            goto error;
        }

        if (!check_store(store) || !check_kind(kind) || !check_key_type(key_type)) {
            goto error;
        }

        if (!PyList_CheckExact(bytes_list)) {
            PyErr_SetString(PyExc_TypeError, "Expected list");
            goto error;
        }

        if (kind != EMPTY) {
            if (values == Py_None) {
                Py_XDECREF(values);
                values = NULL;
            }

            if (automaton_unpickle(automaton, bytes_list, values)) {
                automaton->kind     = kind;
                automaton->store    = store;
                automaton->key_type = key_type;
                automaton->count    = word_count;
                automaton->longest_word = longest_word;
            }
            else
                goto error;
        }
    }
    else {
        store    = STORE_ANY;
        key_type = KEY_STRING;

        // construct new object
        if (F(PyArg_ParseTuple)(args, "ii", &store, &key_type)) {
            if (not check_store(store)) {
                goto error;
            }

            if (not check_key_type(key_type)) {
                goto error;
            }
        }
        else if (F(PyArg_ParseTuple)(args, "i", &store)) {
            if (not check_store(store)) {
                goto error;
            }
        }

        PyErr_Clear();
        automaton->store    = store;
        automaton->key_type = key_type;
    }

//ok:
    return (PyObject*)automaton;

error:
    Py_XDECREF(automaton);
    return NULL;
}


static void
automaton_del(PyObject* self) {
#define automaton ((Automaton*)self)
    automaton_clear(self, NULL);
    PyObject_Del(self);
#undef automaton
}


static Py_ssize_t
automaton_len(PyObject* self) {
#define automaton ((Automaton*)self)
    return automaton->count;
#undef automaton
}


static PyObject*
automaton_add_word(PyObject* self, PyObject* args) {
#define automaton ((Automaton*)self)
    // argument
    PyObject* py_value = NULL;
    struct Input input;

    Py_ssize_t integer = 0;
    TrieNode* node;
    bool new_word;

    if (!prepare_input_from_tuple(self, args, 0, &input)) {
        return NULL;
    }

    switch (automaton->store) {
        case STORE_ANY:
            py_value = F(PyTuple_GetItem)(args, 1);
            if (not py_value) {
                PyErr_SetString(PyExc_ValueError, "A value object is required as second argument.");
                goto py_exception;
            }
            break;

        case STORE_INTS:
            py_value = F(PyTuple_GetItem)(args, 1);
            if (py_value) {
                if (F(PyNumber_Check)(py_value)) {
                    integer = F(PyNumber_AsSsize_t)(py_value, PyExc_ValueError);
                    if (integer == -1 and PyErr_Occurred())
                        goto py_exception;
                }
                else {
                    PyErr_SetString(PyExc_TypeError, "An integer value is required as second argument.");
                    goto py_exception;
                }
            }
            else {
                // default
                PyErr_Clear();
                integer = automaton->count + 1;
            }
            break;

        case STORE_LENGTH:
            integer = input.wordlen;
            break;

        default:
            PyErr_SetString(PyExc_SystemError, "Invalid value for this key: see documentation for supported values.");
            goto py_exception;
    }

    node = NULL;
    new_word = false;

    if (input.wordlen > 0) {
        node = trie_add_word(automaton, input.word, input.wordlen, &new_word);

        if (node == NULL) {
            PyErr_NoMemory();
            goto py_exception;
        }
    }

    destroy_input(&input);

    if (node) {
        switch (automaton->store) {
            case STORE_ANY:
                if (not new_word and node->eow)
                    // replace
                    Py_DECREF(node->output.object);

                Py_INCREF(py_value);
                node->output.object = py_value;
                break;

            default:
                node->output.integer = integer;
        } // switch

        if (new_word) {
            automaton->version += 1; // change version only when new word appeared
            if (input.wordlen > automaton->longest_word)
                automaton->longest_word = (int)input.wordlen;

            Py_RETURN_TRUE;
        }
        else {
            Py_RETURN_FALSE;
        }
    }

    Py_RETURN_FALSE;

py_exception:
    destroy_input(&input);
    return NULL;
}

static TristateResult
automaton_remove_word_aux(PyObject* self, PyObject* args, PyObject** value) {
#define automaton ((Automaton*)self)
    struct Input input;

    if (!prepare_input_from_tuple(self, args, 0, &input)) {
        return MEMORY_ERROR;
    }

    if (input.wordlen == 0) {
        destroy_input(&input);
        return FALSE;
    }

    *value = trie_remove_word(automaton, input.word, input.wordlen);
    destroy_input(&input);

    if (UNLIKELY(PyErr_Occurred() != NULL)) {
        return MEMORY_ERROR;
    } else {
        return (*value != NULL) ? TRUE : FALSE;
    }
}


static PyObject*
automaton_remove_word(PyObject* self, PyObject* args) {
    PyObject* value;

    switch (automaton_remove_word_aux(self, args, &value)) {
        case FALSE:
            Py_RETURN_FALSE;
            break;

        case TRUE:
            if (automaton->store == STORE_ANY) {
                // value is meaningful
                Py_DECREF(value);
            }

            automaton->version += 1;
            automaton->count   -= 1;
            Py_RETURN_TRUE;
            break;

        case MEMORY_ERROR:
        default:
            return NULL;
    }
}


static PyObject*
automaton_pop(PyObject* self, PyObject* args) {
    PyObject* value;

    switch (automaton_remove_word_aux(self, args, &value)) {
        case FALSE:
            PyErr_SetNone(PyExc_KeyError);
            return NULL;

        case TRUE:
            automaton->version += 1;
            automaton->count   -= 1;
            return value; // there's no need to increase refcount, the value was removed

        case MEMORY_ERROR:
        default:
            return NULL;
    }
}


static void
clear_aux(TrieNode* node, KeysStore store) {

    unsigned i;

    if (node) {
        switch (store) {
            case STORE_INTS:
            case STORE_LENGTH:
                // nop
                break;

            case STORE_ANY:
                if (node->eow && node->output.object)
                    Py_DECREF(node->output.object);
                break;
        }

        for (i=0; i < node->n; i++) {
            TrieNode* child = trienode_get_ith_unsafe(node, i);
            if (child != node) // avoid self-loops!
                clear_aux(child, store);
        }

        trienode_free(node);
    }
#undef automaton
}


static PyObject*
automaton_clear(PyObject* self, PyObject* args) {
#define automaton ((Automaton*)self)
    clear_aux(automaton->root, automaton->store);
    automaton->count = 0;
    automaton->longest_word = 0;
    automaton->kind = EMPTY;
    automaton->root = NULL;
    automaton->version += 1;

    Py_RETURN_NONE;
#undef automaton
}


static int
automaton_contains(PyObject* self, PyObject* args) {
#define automaton ((Automaton*)self)
    TrieNode* node;
    struct Input input;

    if (!prepare_input(self, args, &input)) {
        return -1;
    }

    node = trie_find(automaton->root, input.word, input.wordlen);

    destroy_input(&input);

    return (node and node->eow);
#undef automaton
}


static PyObject*
automaton_exists(PyObject* self, PyObject* args) {
    PyObject* word;

    word = F(PyTuple_GetItem)(args, 0);
    if (word)
        switch (automaton_contains(self, word)) {
            case 1:
                Py_RETURN_TRUE;

            case 0:
                Py_RETURN_FALSE;

            default:
                return NULL;
        }
    else
        return NULL;
}


static PyObject*
automaton_match(PyObject* self, PyObject* args) {
#define automaton ((Automaton*)self)
    TrieNode* node;
    struct Input input;

    if (!prepare_input_from_tuple(self, args, 0, &input)) {
        return NULL;
    }

    node = trie_find(automaton->root, input.word, input.wordlen);;

    destroy_input(&input);

    if (node)
        Py_RETURN_TRUE;
    else
        Py_RETURN_FALSE;
#undef automaton
}


static PyObject*
automaton_longest_prefix(PyObject* self, PyObject* args) {
#define automaton ((Automaton*)self)
    int len;
    struct Input input;

    if (!prepare_input_from_tuple(self, args, 0, &input)) {
        return NULL;
    }

    len = trie_longest(automaton->root, input.word, input.wordlen);

    destroy_input(&input);

    return F(Py_BuildValue)("i", len);
#undef automaton
}


static PyObject*
automaton_get(PyObject* self, PyObject* args) {
#define automaton ((Automaton*)self)
    struct Input input;
    PyObject* py_def;
    Py_ssize_t k;
    TrieNode* node;

    k = PyTuple_GET_SIZE(args);

    if (k < 1 || k > 2) {
        PyErr_Format(PyExc_TypeError, "get() takes one or two arguments (%ld given)", k);
        return NULL;
    }

    if (!prepare_input_from_tuple(self, args, 0, &input)) {
        return NULL;
    }

    node = trie_find(automaton->root, input.word, input.wordlen);

    destroy_input(&input);

    if (node and node->eow) {
        switch (automaton->store) {
            case STORE_INTS:
            case STORE_LENGTH:
                return F(Py_BuildValue)("i", node->output.integer);

            case STORE_ANY:
                Py_INCREF(node->output.object);
                return node->output.object;

            default:
                PyErr_SetNone(PyExc_ValueError);
                return NULL;
        }
    }
    else {
        py_def = F(PyTuple_GetItem)(args, 1);
        if (py_def) {
            Py_INCREF(py_def);
            return py_def;
        }
        else {
            PyErr_Clear();
            PyErr_SetNone(PyExc_KeyError);
            return NULL;
        }
    }
#undef automaton
}

typedef struct AutomatonQueueItem {
    LISTITEM_data;
    TrieNode*   node;
} AutomatonQueueItem;


static PyObject*
automaton_make_automaton(PyObject* self, PyObject* args) {
#define automaton ((Automaton*)self)

    AutomatonQueueItem* item;
    List queue;
    unsigned i;

    TrieNode* node;
    TrieNode* child;
    TrieNode* state;
    TRIE_LETTER_TYPE letter;


    if (automaton->kind != TRIE)
        Py_RETURN_FALSE;

    list_init(&queue);

    // 1. setup nodes at first level: they fail back to the root
    ASSERT(automaton->root);

    for (i=0; i < automaton->root->n; i++) {
        TrieNode* child = trienode_get_ith_unsafe(automaton->root, i);
        ASSERT(child);
        // fail edges go to the root
        // every other letters loop on root - implicit (see automaton_next)
        child->fail = automaton->root;

        item = (AutomatonQueueItem*)list_item_new(sizeof(AutomatonQueueItem));
        if (item) {
            item->node = child;
            list_append(&queue, (ListItem*)item);
        }
        else
            goto no_mem;
    }

    // 2. make links
    while (true) {
        AutomatonQueueItem* item = (AutomatonQueueItem*)list_pop_first(&queue);
        if (item == NULL)
            break;
        else {
            node = item->node;
            memory_free(item);
        }

        for (i=0; i < node->n; i++) {
            child  = trienode_get_ith_unsafe(node, i);
            letter = trieletter_get_ith_unsafe(node, i);
            ASSERT(child);

            item = (AutomatonQueueItem*)list_item_new(sizeof(AutomatonQueueItem));
            if (item) {
                item->node = child;
                list_append(&queue, (ListItem*)item);
            }
            else
                goto no_mem;

            state = node->fail;
            ASSERT(state);
            ASSERT(child);
            while (state != automaton->root and\
                   not trienode_get_next(state, letter)) {

                state = state->fail;
                ASSERT(state);
            }

            child->fail = trienode_get_next(state, letter);
            if (child->fail == NULL)
                child->fail = automaton->root;

            ASSERT(child->fail);
        }
    }

    automaton->kind = AHOCORASICK;
    automaton->version += 1;
    list_delete(&queue);
    Py_RETURN_NONE;
#undef automaton

no_mem:
    list_delete(&queue);
    PyErr_NoMemory();
    return NULL;
}


static PyObject*
automaton_find_all(PyObject* self, PyObject* args) {
#define automaton ((Automaton*)self)

    struct Input input;
    Py_ssize_t start;
    Py_ssize_t end;
    PyObject* callback;
    PyObject* callback_ret;

    Py_ssize_t i;
    TrieNode* state;
    TrieNode* tmp;

    if (automaton->kind != AHOCORASICK)
        Py_RETURN_NONE;

    // arg 1
    if (!prepare_input_from_tuple(self, args, 0, &input)) {
        return NULL;
    }

    // arg 2
    callback = F(PyTuple_GetItem)(args, 1);
    if (callback == NULL) {
        destroy_input(&input);
        return NULL;
    }
    else
    if (not F(PyCallable_Check)(callback)) {
        PyErr_SetString(PyExc_TypeError, "The callback argument must be a callable such as a function.");
        destroy_input(&input);
        return NULL;
    }

    // parse start/end
    if (pymod_parse_start_end(args, 2, 3, 0, input.wordlen, &start, &end)) {
        destroy_input(&input);
        return NULL;
    }

    state = automaton->root;
    for (i=start; i < end; i++) {
        state = tmp = ahocorasick_next(state, automaton->root, input.word[i]);

        // return output
        while (tmp) {
            if (tmp->eow) {
                if (automaton->store == STORE_ANY)
                    callback_ret = F(PyObject_CallFunction)(callback, "iO", i, tmp->output.object);
                else
                    callback_ret = F(PyObject_CallFunction)(callback, "ii", i, tmp->output.integer);

                if (callback_ret == NULL) {
                    destroy_input(&input);
                    return NULL;
                } else
                    Py_DECREF(callback_ret);
            }

            tmp = tmp->fail;
        }
    }
#undef automaton

    destroy_input(&input);
    Py_RETURN_NONE;
}

static PyObject*
automaton_items_create(PyObject* self, PyObject* args, const ItemsType type) {
#define automaton ((Automaton*)self)
    PyObject* arg1 = NULL;
    PyObject* arg2 = NULL;
    PyObject* arg3 = NULL;
    TRIE_LETTER_TYPE* word = NULL;
    TRIE_LETTER_TYPE* tmp = NULL;
    Py_ssize_t wordlen = 0;

    TRIE_LETTER_TYPE wildcard;
    bool use_wildcard = false;
    PatternMatchType matchtype = MATCH_AT_LEAST_PREFIX;

    AutomatonItemsIter* iter;

    bool word_is_copy = false;
    bool tmp_is_copy = false;

    // arg 1: prefix/prefix pattern
    if (args)
        arg1 = F(PyTuple_GetItem)(args, 0);
    else
        arg1 = NULL;

    if (arg1) {
        arg1 = pymod_get_string(arg1, &word, &wordlen, &word_is_copy);
        if (arg1 == NULL)
            goto error;
    }
    else {
        PyErr_Clear();
        word = NULL;
        wordlen = 0;
    }

    // arg 2: wildcard
    if (args)
        arg2 = F(PyTuple_GetItem)(args, 1);
    else
        arg2 = NULL;

    if (arg2) {
        Py_ssize_t len = 0;

        arg2 = pymod_get_string(arg2, &tmp, &len, &tmp_is_copy);
        if (arg2 == NULL) {
            goto error;
        } else {
            if (len == 1) {
                wildcard = tmp[0];
                use_wildcard = true;
            }
            else {
                PyErr_SetString(PyExc_ValueError, "Wildcard must be a single character.");
                goto error;
            }
        }
    }
    else {
        PyErr_Clear();
        wildcard = 0;
        use_wildcard = false;
    }

    // arg3: matchtype
    matchtype = MATCH_AT_LEAST_PREFIX;
    if (args) {
        arg3 = F(PyTuple_GetItem)(args, 2);
        if (arg3) {
            Py_ssize_t val = F(PyNumber_AsSsize_t)(arg3, PyExc_OverflowError);
            if (val == -1 and PyErr_Occurred())
                goto error;

            switch ((PatternMatchType)val) {
                case MATCH_AT_LEAST_PREFIX:
                case MATCH_AT_MOST_PREFIX:
                case MATCH_EXACT_LENGTH:
                    matchtype = (PatternMatchType)val;
                    break;

                default:
                    PyErr_SetString(PyExc_ValueError,
                        "The optional how third argument must be one of: "
                        "MATCH_EXACT_LENGTH, MATCH_AT_LEAST_PREFIX or MATCH_AT_LEAST_PREFIX"
                    );
                    goto error;
            }
        }
        else {
            PyErr_Clear();
            if (use_wildcard)
                matchtype = MATCH_EXACT_LENGTH;
            else
                matchtype = MATCH_AT_LEAST_PREFIX;
        }
    }

    //
    iter = (AutomatonItemsIter*)automaton_items_iter_new(
                    automaton,
                    word,
                    wordlen,
                    use_wildcard,
                    wildcard,
                    matchtype);

    maybe_decref(word_is_copy, arg1)
    maybe_decref(tmp_is_copy, arg2)
    maybe_free(word_is_copy, word)
    maybe_free(tmp_is_copy, tmp)

    if (iter) {
        iter->type = type;
        return (PyObject*)iter;
    }
    else
        return NULL;


error:
    maybe_decref(word_is_copy, arg1)
    maybe_decref(tmp_is_copy, arg2)
    maybe_free(word_is_copy, word)
    maybe_free(tmp_is_copy, tmp)
    return NULL;
#undef automaton
}


static PyObject*
automaton_keys(PyObject* self, PyObject* args) {
    return automaton_items_create(self, args, ITER_KEYS);
}


static PyObject*
automaton_iterate(PyObject* self) {
    return automaton_items_create(self, NULL, ITER_KEYS);
}


static PyObject*
automaton_values(PyObject* self, PyObject* args) {
    return automaton_items_create(self, args, ITER_VALUES);
}


static PyObject*
automaton_items(PyObject* self, PyObject* args) {
    return automaton_items_create(self, args, ITER_ITEMS);
}


static PyObject*
automaton_iter(PyObject* self, PyObject* args, PyObject* keywds) {
#define automaton ((Automaton*)self)
    static char *kwlist[] = {"string", "start", "end", "ignore_white_space", NULL};

    PyObject* object;
    Py_ssize_t start, start_tmp = -1;
    Py_ssize_t end, end_tmp = -1;
    int ignore_white_space_tmp = -1;
    bool ignore_white_space = false;

    if (automaton->kind != AHOCORASICK) {
        PyErr_SetString(PyExc_AttributeError,"Not an Aho-Corasick automaton yet: "
            "call add_word to add some keys and call make_automaton to "
            "convert the trie to an automaton.");
        return NULL;
    }

    if (!F(PyArg_ParseTupleAndKeywords)(args, keywds, "O|iii", kwlist, &object, &start_tmp, &end_tmp, &ignore_white_space_tmp)) {
        return NULL;
    }

    if (ignore_white_space_tmp == 1) {
        ignore_white_space = true;
    }

    if (object) {
        if (automaton->key_type == KEY_STRING) {
#ifdef PY3K
    #ifdef AHOCORASICK_UNICODE
        if (F(PyUnicode_Check)(object)) {
            start   = 0;
            #if PY_MINOR_VERSION >= 3
                end = PyUnicode_GET_LENGTH(object);
            #else
                end = PyUnicode_GET_SIZE(object);
            #endif
        }
        else {
            PyErr_SetString(PyExc_TypeError, "string required");
            return NULL;
        }
    #else
        if (F(PyBytes_Check)(object)) {
            start   = 0;
            end     = PyBytes_GET_SIZE(object);
        }
        else {
            PyErr_SetString(PyExc_TypeError, "bytes required");
            return NULL;
        }
    #endif
#else
        if (F(PyString_Check)(object)) {
            start   = 0;
            end     = PyString_GET_SIZE(object);
        } else {
            PyErr_SetString(PyExc_TypeError, "string required");
            return NULL;
        }
#endif
        }
        else {
        if (F(PyTuple_Check)(object)) {
            start = 0;
            end = PyTuple_GET_SIZE(object);
        } else {
            PyErr_SetString(PyExc_TypeError, "tuple required");
            return NULL;
        }
        }
    }
    else
        return NULL;

    if (start_tmp != -1) {
        start = start_tmp;
    }

    if (end_tmp != -1) {
        end = end_tmp;
    }

    return automaton_search_iter_new(
        automaton,
        object,
        (int)start,
        (int)end,
        ignore_white_space
    );
#undef automaton
}


static PyObject*
automaton_iter_long(PyObject* self, PyObject* args) {
#define automaton ((Automaton*)self)

	PyObject* object;
	Py_ssize_t start;
	Py_ssize_t end;

	if (automaton->kind != AHOCORASICK) {
		PyErr_SetString(PyExc_AttributeError, "not an automaton yet; add some words and call make_automaton");
		return NULL;
	}

	object = PyTuple_GetItem(args, 0);
    if (object == NULL)
        return NULL;

    if (automaton->key_type == KEY_STRING) {
#ifdef PY3K
    #ifdef AHOCORASICK_UNICODE
        if (F(PyUnicode_Check)(object)) {
            start   = 0;
            #if PY_MINOR_VERSION >= 3
                end = PyUnicode_GET_LENGTH(object);
            #else
                end = PyUnicode_GET_SIZE(object);
            #endif
        }
        else {
            PyErr_SetString(PyExc_TypeError, "string required");
            return NULL;
        }
    #else
        if (F(PyBytes_Check)(object)) {
            start   = 0;
            end     = PyBytes_GET_SIZE(object);
        }
        else {
            PyErr_SetString(PyExc_TypeError, "bytes required");
            return NULL;
        }
    #endif
#else
        if (F(PyString_Check)(object)) {
            start   = 0;
            end     = PyString_GET_SIZE(object);
        } else {
            PyErr_SetString(PyExc_TypeError, "string required");
            return NULL;
        }
#endif
    }
    else {
        if (F(PyTuple_Check)(object)) {
            start = 0;
            end = PyTuple_GET_SIZE(object);
        } else {
            PyErr_SetString(PyExc_TypeError, "tuple required");
            return NULL;
        }
    }

	if (pymod_parse_start_end(args, 1, 2, start, end, &start, &end))
		return NULL;

	return automaton_search_iter_long_new(
		automaton,
		object,
		start,
		end
	);
#undef automaton
}


static void
get_stats_aux(TrieNode* node, AutomatonStatistics* stats, int depth) {

    unsigned i;

    stats->nodes_count  += 1;
    stats->words_count  += (int)(node->eow);
    stats->links_count  += node->n;
    stats->total_size   += trienode_get_size(node);

    if (depth > stats->longest_word)
        stats->longest_word = depth;

    for (i=0; i < node->n; i++)
        get_stats_aux(trienode_get_ith_unsafe(node, i), stats, depth + 1);
}

static void
get_stats(Automaton* automaton) {
    automaton->stats.nodes_count    = 0;
    automaton->stats.words_count    = 0;
    automaton->stats.longest_word   = 0;
    automaton->stats.links_count    = 0;
    automaton->stats.sizeof_node    = sizeof(TrieNode);
    automaton->stats.total_size     = 0;

    if (automaton->kind != EMPTY)
        get_stats_aux(automaton->root, &automaton->stats, 0);

    automaton->stats.version        = automaton->version;
}


static PyObject*
automaton_get_stats(PyObject* self, PyObject* args) {
#define automaton ((Automaton*)self)

    PyObject* dict;

    if (automaton->stats.version != automaton->version)
        get_stats(automaton);

    dict = F(Py_BuildValue)(
        "{s:k,s:k,s:k,s:k,s:i,s:k}",
        "nodes_count",  automaton->stats.nodes_count,
        "words_count",  automaton->stats.words_count,
        "longest_word", automaton->stats.longest_word,
        "links_count",  automaton->stats.links_count,
        "sizeof_node",  automaton->stats.sizeof_node,
        "total_size",   automaton->stats.total_size
    );
    return dict;
#undef automaton
}


typedef struct DumpAux {
    PyObject*   nodes;
    PyObject*   edges;
    PyObject*   fail;
    char        error;
} DumpAux;

static int
dump_aux(TrieNode* node, const int depth, void* extra) {
#define Dump ((DumpAux*)extra)
    PyObject* tuple;
    TrieNode* child;
    unsigned i;

#define append_tuple(list) \
    if (tuple == NULL) { \
        Dump->error = 1; \
        return 0; \
    } \
    else if (PyList_Append(list, tuple) < 0) { \
        Dump->error = 1; \
        return 0; \
    }


    // 1.
    tuple = F(Py_BuildValue)("ii", node, (int)(node->eow));
    append_tuple(Dump->nodes)

    // 2.
    for (i=0; i < node->n; i++) {
        child = trienode_get_ith_unsafe(node, i);
        tuple = F(Py_BuildValue)("ici", node, trieletter_get_ith_unsafe(node, i), child);
        append_tuple(Dump->edges)
    }

    // 3.
    if (node->fail) {
        tuple = F(Py_BuildValue)("ii", node, node->fail);
        append_tuple(Dump->fail);
    }

    return 1;
#undef append_tuple
#undef Dump
}


static PyObject*
automaton_dump(PyObject* self, PyObject* args) {
#define automaton ((Automaton*)self)
    DumpAux dump;

    if (automaton->kind == EMPTY)
        Py_RETURN_NONE;

    dump.nodes = 0;
    dump.edges = 0;
    dump.fail  = 0;
    dump.error = 0;

    dump.nodes = F(PyList_New)(0);
    dump.edges = F(PyList_New)(0);
    dump.fail  = F(PyList_New)(0);
    if (dump.edges == NULL or dump.fail == NULL or dump.nodes == NULL)
        goto error;

    trie_traverse(automaton->root, dump_aux, &dump);
    if (dump.error)
        goto error;
    else
        return F(Py_BuildValue)("OOO", dump.nodes, dump.edges, dump.fail);

error:
    Py_XDECREF(dump.nodes);
    Py_XDECREF(dump.edges);
    Py_XDECREF(dump.fail);
    return NULL;

#undef automaton
}


static PyObject*
automaton___sizeof__(PyObject* self, PyObject* args) {
#define automaton ((Automaton*)self)
    Py_ssize_t size = sizeof(Automaton);

    if (automaton->kind != EMPTY) {
        if (automaton->stats.version != automaton->version) {
            get_stats(automaton);
        }

        size += automaton->stats.total_size;
    }

    return Py_BuildValue("i", size);
#undef automaton
}


#include "Automaton_pickle.c"


#define method(name, kind) {#name, (PyCFunction)automaton_##name, kind, automaton_##name##_doc}
static
PyMethodDef automaton_methods[] = {
    method(add_word,        METH_VARARGS),
    method(remove_word,     METH_VARARGS),
    method(pop,             METH_VARARGS),
    method(clear,           METH_NOARGS),
    method(exists,          METH_VARARGS),
    method(match,           METH_VARARGS),
    method(longest_prefix,  METH_VARARGS),
    method(get,             METH_VARARGS),
    method(make_automaton,  METH_NOARGS),
    method(find_all,        METH_VARARGS),
    method(iter,            METH_VARARGS|METH_KEYWORDS),
	method(iter_long,		METH_VARARGS),
    method(keys,            METH_VARARGS),
    method(values,          METH_VARARGS),
    method(items,           METH_VARARGS),
    method(get_stats,       METH_NOARGS),
    method(dump,            METH_NOARGS),
    method(__reduce__,      METH_VARARGS),
    method(__sizeof__,      METH_VARARGS),
    method(save,            METH_VARARGS),

    {NULL, NULL, 0, NULL}
};
#undef method


static
PySequenceMethods automaton_as_sequence;


static
PyMemberDef automaton_members[] = {
    {
        "kind",
        T_INT,
        offsetof(Automaton, kind),
        READONLY,
        "Read-only attribute maintained automatically.\nKind for this Automaton instance.\nOne of ahocorasick.EMPTY, TRIE or AHOCORASICK."
    },

    {
        "store",
        T_INT,
        offsetof(Automaton, store),
        READONLY,
        "Read-only attribute set when creating an Automaton().\nType of values accepted by this Automaton.\nOne of ahocorasick.STORE_ANY, STORE_INTS or STORE_LEN."
    },

    {NULL}
};

static PyTypeObject automaton_type = {
    PY_OBJECT_HEAD_INIT
    "ahocorasick.Automaton",                    /* tp_name */
    sizeof(Automaton),                          /* tp_size */
    0,                                          /* tp_itemsize? */
    (destructor)automaton_del,                  /* tp_dealloc */
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
    automaton_constructor_doc,                  /* tp_doc */
    0,                                          /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    automaton_iterate,                          /* tp_iter */
    0,                                          /* tp_iternext */
    automaton_methods,                          /* tp_methods */
    automaton_members,                          /* tp_members */
    0,                                          /* tp_getset */
    0,                                          /* tp_base */
    0,                                          /* tp_dict */
    0,                                          /* tp_descr_get */
    0,                                          /* tp_descr_set */
    0,                                          /* tp_dictoffset */
    0,                                          /* tp_init */
    0,                                          /* tp_alloc */
    automaton_new,                              /* tp_new */
};

