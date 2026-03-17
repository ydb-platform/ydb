/*
    This is part of pyahocorasick Python module.

    Automaton class methods

    Author    : Wojciech Muła, wojciech_mula@poczta.onet.pl
    WWW       : http://0x80.pl
    License   : BSD-3-Clause (see LICENSE)
*/
#ifndef ahocorasick_Automaton_h_included
#define ahocorasick_Automaton_h_included

#include "common.h"
#include "trie.h"

typedef enum {
    EMPTY       = 0,
    TRIE        = 1,
    AHOCORASICK = 2
} AutomatonKind;


static bool
check_kind(const int kind);


typedef enum {
    STORE_INTS   = 10,
    STORE_LENGTH = 20,
    STORE_ANY    = 30
} KeysStore;


static bool
check_store(const int store);


typedef enum {
    KEY_STRING   = 100,
    KEY_SEQUENCE = 200
} KeyType;


static bool
check_key_type(const int key_type);


struct Input {
    Py_ssize_t          wordlen;
    TRIE_LETTER_TYPE*   word;
    PyObject*           py_word;
    bool is_copy;
};


typedef struct AutomatonStatistics {
    int         version;

    Py_ssize_t     nodes_count;        ///< total number of nodes
    Py_ssize_t     words_count;        ///< len(automaton)
    Py_ssize_t     longest_word;       ///< longest word
    Py_ssize_t     links_count;        ///< links count
    Py_ssize_t     sizeof_node;        ///< size of single node (a C structure)
    Py_ssize_t     total_size;         ///< total size in bytes
} AutomatonStatistics;


typedef struct Automaton {
    PyObject_HEAD

    AutomatonKind   kind;   ///< current kind of automaton
    KeysStore       store;  ///< type of values: copy of string, bare integer, python  object
    KeyType         key_type;    ///< type of keys: strings or integer sequences
    int             count;  ///< number of distinct words
    int             longest_word;   ///< length of the longest word
    TrieNode*       root;   ///< root of a trie

    int             version;    ///< current version of automaton, incremented by add_word, clean and make_automaton; used to lazy invalidate iterators

    AutomatonStatistics stats;  ///< statistics
} Automaton;

/*------------------------------------------------------------------------*/

static bool
automaton_unpickle(
    Automaton* automaton,
    PyObject* bytes_list,
    PyObject* values
);

static PyObject*
automaton_create(void);

/* __init__ */
static PyObject*
automaton_new(PyTypeObject* self, PyObject* args, PyObject* kwargs);

/* clear() */
static PyObject*
automaton_clear(PyObject* self, PyObject* args);

/* len() */
static Py_ssize_t
automaton_len(PyObject* self);

/* add_word */
static PyObject*
automaton_add_word(PyObject* self, PyObject* args);

/* clear() */
static PyObject*
automaton_clear(PyObject* self, PyObject* args);

/* __contains__ */
static int
automaton_contains(PyObject* self, PyObject* args);

/* exists() */
static PyObject*
automaton_exists(PyObject* self, PyObject* args);

/* match() */
static PyObject*
automaton_match(PyObject* self, PyObject* args);

/* get() */
static PyObject*
automaton_get(PyObject* self, PyObject* args);

/* make_automaton() */
static PyObject*
automaton_make_automaton(PyObject* self, PyObject* args);

/* find_all() */
static PyObject*
automaton_find_all(PyObject* self, PyObject* args);

/* keys() */
static PyObject*
automaton_keys(PyObject* self, PyObject* args);

/* values() */
static PyObject*
automaton_values(PyObject* self, PyObject* args);

/* items() */
static PyObject*
automaton_items(PyObject* self, PyObject* args);

/* iter() */
static PyObject*
automaton_iter(PyObject* self, PyObject* args, PyObject* keywds);

/* iter_long() */
static PyObject*
automaton_iter_long(PyObject* self, PyObject* args);

/* get_stats() */
static PyObject*
automaton_get_stats(PyObject* self, PyObject* args);

#endif
