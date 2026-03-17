/*
    This is part of pyahocorasick Python module.

    Trie implementation

    Author    : Wojciech Muła, wojciech_mula@poczta.onet.pl
    WWW       : http://0x80.pl
    License   : BSD-3-Clause (see LICENSE)
*/

#include "trienode.h"

static TrieNode*
trienode_new(const char eow) {
    TrieNode* node = (TrieNode*)memory_alloc(sizeof(TrieNode));
    if (node) {
        node->output.integer = 0;
        node->output.object = NULL;
        node->fail      = NULL;

        node->n     = 0;
        node->eow       = eow;
        node->next  = NULL;
    }

    return node;
}

static void
trienode_free(TrieNode* node) {

    ASSERT(node);

    if (node->n > 0) {
        memory_free(node->next);
    }

    memory_free(node);
}


static TrieNode* PURE
trienode_get_next(TrieNode* node, const TRIE_LETTER_TYPE letter) {

    unsigned i;
    Pair* next;

    ASSERT(node);
    next = (Pair*)node->next;

    for (i=0; i < node->n; i++)
        if (next[i].letter == letter) {
            return next[i].child;
        }

    return NULL;
}


static TristateResult
trienode_unset_next_pointer(TrieNode* node, TrieNode* child) {

    unsigned i;
    unsigned index;
    Pair* next;

    ASSERT(node);
    for (i=0; i < node->n; i++) {
        if (node->next[i].child == child) {
            index = i;
            goto found;
        }
    }

    return FALSE;

found:
    if (node->n == 1) {
        // there is just one node
        node->n = 0;
        memory_free(node->next);
        node->next = NULL;
        return TRUE;
    }

    // there are more nodes, reallocation is needed

    next = (Pair*)memory_alloc((node->n - 1) * sizeof(Pair));
    if (UNLIKELY(next == NULL)) {
        return MEMORY_ERROR;
    }

    for (i=0; i < index; i++) {
        next[i] = node->next[i];
    }

    for (i=index + 1; i < node->n; i++) {
        next[i - 1] = node->next[i];
    }

    memory_free(node->next);
    node->next = next;
    node->n -= 1;
    return TRUE;
}


static TrieNode* PURE
trienode_get_ith_unsafe(TrieNode* node, size_t index) {
    ASSERT(node);

    return node->next[index].child;
}


static TRIE_LETTER_TYPE PURE
trieletter_get_ith_unsafe(TrieNode* node, size_t index) {
    ASSERT(node);

    return node->next[index].letter;
}


static TrieNode*
trienode_set_next(TrieNode* node, const TRIE_LETTER_TYPE letter, TrieNode* child) {

    int n;
    void* next;

    ASSERT(node);
    ASSERT(child);
    ASSERT(trienode_get_next(node, letter) == NULL);

    n = node->n;
    next = (TrieNode**)memory_realloc(node->next, (n + 1) * (sizeof(Pair)));
    if (next) {

        node->next = next;
        node->next[n].letter = letter;
        node->next[n].child = child;
        node->n += 1;

        return child;
    }
    else
        return NULL;
}


#ifdef DEBUG_LAYOUT
void trienode_dump_layout() {
#define field_size(TYPE, name) sizeof(((TYPE*)NULL)->name)
#define field_ofs(TYPE, name) offsetof(TYPE, name)
#define field_dump(TYPE, name) printf("- %-12s: %d %d\n", #name, field_size(TYPE, name), field_ofs(TYPE, name));

    printf("TrieNode (size=%lu):\n", sizeof(TrieNode));
    field_dump(TrieNode, output);
    field_dump(TrieNode, fail);
    field_dump(TrieNode, n);
    field_dump(TrieNode, eow);
    field_dump(TrieNode, next);

    printf("Pair (size=%lu):\n", sizeof(Pair));
    field_dump(Pair, letter);
    field_dump(Pair, child);

#undef field_dump
#undef field_size
#undef field_ofs
}
#endif


UNUSED static void
trienode_dump_to_file(TrieNode* node, FILE* f) {
    unsigned i;

    ASSERT(node != NULL);
    ASSERT(f != NULL);

    if (node->n == 0)
        fprintf(f, "leaf ");

    fprintf(f, "node %p\n", node);
    if (node->eow)
        fprintf(f, "- eow [%p]\n", node->output.object);

    fprintf(f, "- fail: %p\n", node->fail);
    if (node->n > 0) {
        if (node->next == NULL) {
            fprintf(f, "- %d next: %p\n", node->n, node->next);
        } else {
            fprintf(f, "- %d next: [(%d; %p)", node->n, node->next[0].letter, node->next[0].child);
            for (i=1; i < node->n; i++)
                fprintf(f, ", (%d; %p)", node->next[i].letter, node->next[i].child);
            fprintf(f, "]\n");
        }
    }
}

