#ifndef ART_ART_H
#define ART_ART_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/*
 * This file contains an implementation of an Adaptive Radix Tree as described
 * in https://db.in.tum.de/~leis/papers/ART.pdf.
 *
 * The ART contains the keys in _byte lexographical_ order.
 *
 * Other features:
 *  * Fixed 48 bit key length: all keys are assumed to be be 48 bits in size.
 *    This allows us to put the key and key prefixes directly in nodes, reducing
 *    indirection at no additional memory overhead.
 *  * Key compression: the only inner nodes created are at points where key
 *    chunks _differ_. This means that if there are two entries with different
 *    high 48 bits, then there is only one inner node containing the common key
 *    prefix, and two leaves.
 *  * Intrusive leaves: the leaf struct is included in user values. This removes
 *    a layer of indirection.
 */

// Fixed length of keys in the ART. All keys are assumed to be of this length.
#define ART_KEY_BYTES 6

#ifdef __cplusplus
extern "C" {
namespace roaring {
namespace internal {
#endif

typedef uint8_t art_key_chunk_t;
typedef struct art_node_s art_node_t;

/**
 * Wrapper to allow an empty tree.
 */
typedef struct art_s {
    art_node_t *root;
} art_t;

/**
 * Values inserted into the tree have to be cast-able to art_val_t. This
 * improves performance by reducing indirection.
 *
 * NOTE: Value pointers must be unique! This is because each value struct
 * contains the key corresponding to the value.
 */
typedef struct art_val_s {
    art_key_chunk_t key[ART_KEY_BYTES];
} art_val_t;

/**
 * Compares two keys, returns their relative order:
 *  * Key 1 <  key 2: returns a negative value
 *  * Key 1 == key 2: returns 0
 *  * Key 1 >  key 2: returns a positive value
 */
int art_compare_keys(const art_key_chunk_t key1[],
                     const art_key_chunk_t key2[]);

/**
 * Inserts the given key and value.
 */
void art_insert(art_t *art, const art_key_chunk_t *key, art_val_t *val);

/**
 * Returns the value erased, NULL if not found.
 */
art_val_t *art_erase(art_t *art, const art_key_chunk_t *key);

/**
 * Returns the value associated with the given key, NULL if not found.
 */
art_val_t *art_find(const art_t *art, const art_key_chunk_t *key);

/**
 * Returns true if the ART is empty.
 */
bool art_is_empty(const art_t *art);

/**
 * Frees the nodes of the ART except the values, which the user is expected to
 * free.
 */
void art_free(art_t *art);

/**
 * Returns the size in bytes of the ART. Includes size of pointers to values,
 * but not the values themselves.
 */
size_t art_size_in_bytes(const art_t *art);

/**
 * Prints the ART using printf, useful for debugging.
 */
void art_printf(const art_t *art);

/**
 * Callback for validating the value stored in a leaf.
 *
 * Should return true if the value is valid, false otherwise
 * If false is returned, `*reason` should be set to a static string describing
 * the reason for the failure.
 */
typedef bool (*art_validate_cb_t)(const art_val_t *val, const char **reason);

/**
 * Validate the ART tree, ensuring it is internally consistent.
 */
bool art_internal_validate(const art_t *art, const char **reason,
                           art_validate_cb_t validate_cb);

/**
 * ART-internal iterator bookkeeping. Users should treat this as an opaque type.
 */
typedef struct art_iterator_frame_s {
    art_node_t *node;
    uint8_t index_in_node;
} art_iterator_frame_t;

/**
 * Users should only access `key` and `value` in iterators. The iterator is
 * valid when `value != NULL`.
 */
typedef struct art_iterator_s {
    art_key_chunk_t key[ART_KEY_BYTES];
    art_val_t *value;

    uint8_t depth;  // Key depth
    uint8_t frame;  // Node depth

    // State for each node in the ART the iterator has travelled from the root.
    // This is `ART_KEY_BYTES + 1` because it includes state for the leaf too.
    art_iterator_frame_t frames[ART_KEY_BYTES + 1];
} art_iterator_t;

/**
 * Creates an iterator initialzed to the first or last entry in the ART,
 * depending on `first`. The iterator is not valid if there are no entries in
 * the ART.
 */
art_iterator_t art_init_iterator(const art_t *art, bool first);

/**
 * Returns an initialized iterator positioned at a key equal to or greater than
 * the given key, if it exists.
 */
art_iterator_t art_lower_bound(const art_t *art, const art_key_chunk_t *key);

/**
 * Returns an initialized iterator positioned at a key greater than the given
 * key, if it exists.
 */
art_iterator_t art_upper_bound(const art_t *art, const art_key_chunk_t *key);

/**
 * The following iterator movement functions return true if a new entry was
 * encountered.
 */
bool art_iterator_move(art_iterator_t *iterator, bool forward);
bool art_iterator_next(art_iterator_t *iterator);
bool art_iterator_prev(art_iterator_t *iterator);

/**
 * Moves the iterator forward to a key equal to or greater than the given key.
 */
bool art_iterator_lower_bound(art_iterator_t *iterator,
                              const art_key_chunk_t *key);

/**
 * Insert the value and positions the iterator at the key.
 */
void art_iterator_insert(art_t *art, art_iterator_t *iterator,
                         const art_key_chunk_t *key, art_val_t *val);

/**
 * Erase the value pointed at by the iterator. Moves the iterator to the next
 * leaf. Returns the value erased or NULL if nothing was erased.
 */
art_val_t *art_iterator_erase(art_t *art, art_iterator_t *iterator);

#ifdef __cplusplus
}  // extern "C"
}  // namespace roaring
}  // namespace internal
#endif

#endif
