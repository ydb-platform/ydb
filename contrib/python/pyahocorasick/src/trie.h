/*
    This is part of pyahocorasick Python module.

    Trie declarations

    Author    : Wojciech Muła, wojciech_mula@poczta.onet.pl
    WWW       : http://0x80.pl
    License   : BSD-3-Clause (see LICENSE)
*/

#ifndef ahocorasick_trie_h_included
#define ahocorasick_trie_h_included

#include "common.h"
#include "trienode.h"
#include "Automaton.h"

/* add new word to a trie, returns last node on a path for that word */
static TrieNode*
trie_add_word(Automaton* automaton, const TRIE_LETTER_TYPE* word, const size_t wordlen, bool* new_word);

/* remove word from a trie, returns associated object if was any */
static PyObject*
trie_remove_word(Automaton* automaton, const TRIE_LETTER_TYPE* word, const size_t wordlen);

/* returns last node on a path for given word */
static TrieNode* PURE
trie_find(TrieNode* root, const TRIE_LETTER_TYPE* word, const size_t wordlen);

/* returns node linked by edge labeled with letter including paths going
   through fail links */
static TrieNode* PURE
ahocorasick_next(TrieNode* node, TrieNode* root, const TRIE_LETTER_TYPE letter);

typedef int (*trie_traverse_callback)(TrieNode* node, const int depth, void* extra);

/* traverse trie in DFS order, for each node callback is called
   if callback returns false, then traversing stop */
static void
trie_traverse(
    TrieNode* root,
    trie_traverse_callback callback,
    void *extra
);

/* returns total size of node and it's internal structures */
size_t PURE
trienode_get_size(const TrieNode* node);

#endif
