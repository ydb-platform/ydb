/*
    This is part of pyahocorasick Python module.

    Trie node declarations

    Author    : Wojciech Muła, wojciech_mula@poczta.onet.pl
    WWW       : http://0x80.pl
    License   : BSD-3-Clause (see LICENSE)
*/

#ifndef ahocorasick_trienode_h_included
#define ahocorasick_trienode_h_included

#include "common.h"

struct TrieNode;


#pragma pack(push)
#pragma pack(1)
typedef struct Pair {
    TRIE_LETTER_TYPE letter;    ///< edge label
    struct TrieNode* child;     ///< next pointer
} Pair;
#pragma pack(pop)

/* links to children nodes are stored in dynamic table */
typedef struct TrieNode {
    union {
        PyObject*   object;     ///< valid when kind = STORE_ANY
        Py_uintptr_t integer;   ///< valid when kind in [STORE_LENGTH, STORE_INTS]
    } output; ///< output function, valid when eow is true
    struct TrieNode*    fail;   ///< fail node

#if TRIE_LETTER_SIZE == 1
    uint16_t            n;      ///< length of next
#else
    uint32_t            n;      ///< length of next
#endif
    uint8_t             eow;    ///< end of word marker
    Pair*               next;   ///< table of letters and associated next pointers
} TrieNode;


typedef enum {
    MEMORY_ERROR,
    TRUE,
    FALSE
} TristateResult;


/* allocate new node */
static TrieNode*
trienode_new(const char eow);

/* free node */
static void
trienode_free(TrieNode* node);

/* returns child node linked by edge labelled with letter */
static TrieNode* PURE
trienode_get_next(TrieNode* node, const TRIE_LETTER_TYPE letter);

/* link with child node by edge labelled with letter */
static TrieNode*
trienode_set_next(TrieNode* node, const TRIE_LETTER_TYPE letter, TrieNode* child);

/* remove link to given children */
static TristateResult
trienode_unset_next_pointer(TrieNode* node, TrieNode* child);

static TrieNode* PURE
trienode_get_ith_unsafe(TrieNode* node, size_t letter);

static TRIE_LETTER_TYPE PURE
trieletter_get_ith_unsafe(TrieNode* node, size_t letter);

#define trienode_is_leaf(node) ((node)->n == 0)

static void
trienode_dump_to_file(TrieNode* node, FILE* f);

#define trienode_dump(node) trienode_dump_to_file(node, stdout)

#ifdef DEBUG_LAYOUT
void trienode_dump_layout();
#endif

#endif
