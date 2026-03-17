/*
    This is part of pyahocorasick Python module.

    Trie implementation

    Author    : Wojciech Muła, wojciech_mula@poczta.onet.pl
    WWW       : http://0x80.pl
    License   : BSD-3-Clause (see LICENSE)
*/

#include "trie.h"


static TrieNode*
trie_add_word(Automaton* automaton, const TRIE_LETTER_TYPE* word, const size_t wordlen, bool* new_word) {

    TrieNode* node;
    TrieNode* child;
    unsigned i;

    if (automaton->kind == EMPTY) {
        ASSERT(automaton->root == NULL);
        automaton->root = trienode_new(false);
        if (automaton->root == NULL)
            return NULL;
    }

    node = automaton->root;

    for (i=0; i < wordlen; i++) {
        const TRIE_LETTER_TYPE letter = word[i];

        child = trienode_get_next(node, letter);
        if (child == NULL) {
            child = trienode_new(false);
            if (LIKELY(child != NULL)) {
                if (UNLIKELY(trienode_set_next(node, letter, child) == NULL)) {
                    memory_free(child);
                    return NULL;
                }
            } else {
                // Note: in case of memory error, the already allocate nodes
                //       are still reachable from the root and will be free
                //       upon automaton destruction.
                return NULL;
            }
        }

        node = child;
    }

    if (node->eow == false) {
        node->eow = true;
        *new_word = true;
        automaton->count += 1;
    }
    else
        *new_word = false;

    automaton->kind = TRIE;

    return node;
}


static PyObject*
trie_remove_word(Automaton* automaton, const TRIE_LETTER_TYPE* word, const size_t wordlen) {

    PyObject* object;
    TrieNode* node;
    TrieNode* tmp;
    TrieNode* last_multiway;
    unsigned last_multiway_index;
    unsigned i;

    if (automaton->root == NULL) {
        return NULL;
    }

    node = automaton->root;

    last_multiway = node;
    last_multiway_index = 0;
    for (i=0; i < wordlen; i++) {
        const TRIE_LETTER_TYPE letter = word[i];

        node = trienode_get_next(node, letter);
        if (node == NULL) {
            return NULL;
        }

        // Save the last node along path which has more children
        // or is a terminating node.
        if (node->n > 1 || (node->n == 1 && node->eow)) {
            last_multiway = node;
            last_multiway_index = i + 1;
        }
    }

    if (node->eow != true) {
        return NULL;
    }

    object = node->output.object;

    if (trienode_is_leaf(node)) {
        // Remove a linear list that starts at the last_multiway node
        // and ends at the last [found] one.

        // 1. Unlink the tail from the trie
        node = trienode_get_next(last_multiway, word[last_multiway_index]);
        ASSERT(node != NULL);

        if (UNLIKELY(trienode_unset_next_pointer(last_multiway, node) == MEMORY_ERROR)) {
            PyErr_NoMemory();
            return NULL;
        }

        // 2. Free the tail (reference to value from the last element was already saved)
        for (i = last_multiway_index + 1; i < wordlen; i++) {
            tmp = trienode_get_next(node, word[i]);
            ASSERT(tmp->n <= 1);
            trienode_free(node);
            node = tmp;
        }

        trienode_free(node);

    } else {
        // just unmark the terminating node
        node->eow = false;
    }

    automaton->kind = TRIE;
    return object;
}


static TrieNode* PURE
trie_find(TrieNode* root, const TRIE_LETTER_TYPE* word, const size_t wordlen) {
    TrieNode* node;
    size_t i;

    node = root;

    if (node != NULL) {
        for (i=0; i < wordlen; i++) {
            node = trienode_get_next(node, word[i]);
            if (node == NULL)
                return NULL;
        }
    }

    return node;
}


static int PURE
trie_longest(TrieNode* root, const TRIE_LETTER_TYPE* word, const size_t wordlen) {
    TrieNode* node;
    int len = 0;
    size_t i;

    node = root;
    for (i=0; i < wordlen; i++) {
        node = trienode_get_next(node, word[i]);
        if (node == NULL)
            break;
        else
            len += 1;
    }

    return len;
}


static TrieNode* PURE
ahocorasick_next(TrieNode* node, TrieNode* root, const TRIE_LETTER_TYPE letter) {
    TrieNode* next = node;
    TrieNode* tmp;

    while (next) {
        tmp = trienode_get_next(next, letter);
        if (tmp)
            // found link
            return tmp;
        else
            // or go back through fail edges
            next = next->fail;
    }

    // or return root node
    return root;
}

static int
trie_traverse_aux(
    TrieNode* node,
    const int depth,
    trie_traverse_callback callback,
    void *extra
) {
    unsigned i;
    if (callback(node, depth, extra) == 0)
        return 0;

    for (i=0; i < node->n; i++) {
        if (trie_traverse_aux(trienode_get_ith_unsafe(node, i), depth + 1, callback, extra) == 0)
            return 0;
    }

    return 1;
}


static void
trie_traverse(
    TrieNode* root,
    trie_traverse_callback callback,
    void *extra
) {
    ASSERT(root);
    ASSERT(callback);
    trie_traverse_aux(root, 0, callback, extra);
}


size_t PURE
trienode_get_size(const TrieNode* node) {
    return sizeof(TrieNode) + node->n * sizeof(TrieNode*);
}
