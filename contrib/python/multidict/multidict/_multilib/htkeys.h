#include "pythoncapi_compat.h"

#ifndef _MULTIDICT_HTKEYS_H
#define _MULTIDICT_HTKEYS_H

#ifdef __cplusplus
extern "C" {
#endif

#include <Python.h>
#include <stdbool.h>

#include "atomic_helpers.h"

/* Implementation note.
identity always has exact PyUnicode_Type type, not a subclass.
It guarantees that identity hashing and comparison never calls
Python code back, and these operations has no weird side effects,
e.g. deletion the key from multidict.

Taking into account the fact that all multidict operations except
repr(md), repr(md_proxy), or repr(view) never access to the key
itself but identity instead, borrowed references during iteration
over pair_list for, e.g., md.get() or md.pop() is safe.
*/

typedef struct entry {
    Py_hash_t hash;
    PyObject* identity;
    PyObject* key;
    PyObject* value;
} entry_t;

#define DKIX_EMPTY (-1) /* empty (never used) slot */
#define DKIX_DUMMY (-2) /* deleted slot */

#define HT_LOG_MINSIZE 3
#define HT_MINSIZE 8
#define HT_PERTURB_SHIFT 5

#define HT_LOG_RESUME_SLOTS_MINSIZE 10
/* Probe steps after perturb is 0 before resume slots are allocated */
#define HT_RESUME_SLOTS_MIN_STEPS 32

/* Py_NO_INLINE is 3.11+ */
#if defined(__GNUC__) || defined(__clang__)
#define HT_UNLIKELY(x) __builtin_expect(!!(x), 0)
#define HT_COLD __attribute__((cold, noinline))
#elif defined(_MSC_VER)
#define HT_UNLIKELY(x) (x)
#define HT_COLD __declspec(noinline)
#else
#define HT_UNLIKELY(x) (x)
#define HT_COLD
#endif

typedef struct _htkeys {
    /* Size of the hash table (indices). It must be a power of 2. */
    uint8_t log2_size;

    /* Size of the hash table (indices) by bytes. */
    uint8_t log2_index_bytes;

    /* Number of usable entries in dk_entries. */
    Py_ssize_t usable;

    /* Number of used entries in dk_entries. */
    Py_ssize_t nentries;

    /* Allocated on the first long probe, see htkeys_resume_slots_bytes(). */
    void* resume_slots;

#ifdef Py_GIL_DISABLED
    Py_ssize_t num_readers;

    struct _htkeys* retired_next;
#endif

    /* Actual hash table of dk_size entries. It holds indices in dk_entries,
       or DKIX_EMPTY(-1) or DKIX_DUMMY(-2).

       Indices must be: 0 <= indice < USABLE_FRACTION(dk_size).

       The size in bytes of an indice depends on dk_size:

       - 1 byte if htkeys_nslots() <= 0xff (char*)
       - 2 bytes if htkeys_nslots() <= 0xffff (int16_t*)
       - 4 bytes if htkeys_nslots() <= 0xffffffff (int32_t*)
       - 8 bytes otherwise (int64_t*)

       Dynamically sized, SIZEOF_VOID_P is minimum. */
    char indices[]; /* char is required to avoid strict aliasing. */

} htkeys_t;

#if SIZEOF_VOID_P > 4
static inline Py_ssize_t
htkeys_nslots(const htkeys_t* keys)
{
    return ((int64_t)1) << keys->log2_size;
}
#else
static inline Py_ssize_t
htkeys_nslots(const htkeys_t* keys)
{
    return 1 << keys->log2_size;
}
#endif

static inline Py_ssize_t
htkeys_mask(const htkeys_t* keys)
{
    return htkeys_nslots(keys) - 1;
}

static inline entry_t*
htkeys_entries(const htkeys_t* dk)
{
    int8_t* indices = (int8_t*)(dk->indices);
    size_t index = (size_t)1 << dk->log2_index_bytes;
    return (entry_t*)(&indices[index]);
}

#ifdef Py_GIL_DISABLED
#define LOAD_INDEX(keys, size, idx)  \
    atomic_load_int##size##_relaxed( \
        &((const int##size##_t*)(keys->indices))[idx])
#define STORE_INDEX(keys, size, idx, value)                                   \
    atomic_store_int##size##_relaxed(&((int##size##_t*)(keys->indices))[idx], \
                                     (int##size##_t)value)
#else
#define LOAD_INDEX(keys, size, idx) \
    ((const int##size##_t*)(keys->indices))[idx]
#define STORE_INDEX(keys, size, idx, value) \
    ((int##size##_t*)(keys->indices))[idx] = (int##size##_t)value
#endif

/* lookup indices.  returns DKIX_EMPTY, DKIX_DUMMY, or ix >=0 */
static inline Py_ssize_t
htkeys_get_index(const htkeys_t* keys, Py_ssize_t i)
{
    uint8_t log2size = keys->log2_size;
    Py_ssize_t ix;

    if (log2size < 8) {
        ix = LOAD_INDEX(keys, 8, i);
    } else if (log2size < 16) {
        ix = LOAD_INDEX(keys, 16, i);
    }
#if SIZEOF_VOID_P > 4
    else if (log2size >= 32) {
        ix = LOAD_INDEX(keys, 64, i);
    }
#endif
    else {
        ix = LOAD_INDEX(keys, 32, i);
    }
    assert(ix >= DKIX_DUMMY);
    return ix;
}

/* write to indices. */
static inline void
htkeys_set_index(htkeys_t* keys, Py_ssize_t i, Py_ssize_t ix)
{
    uint8_t log2size = keys->log2_size;

    assert(ix >= DKIX_DUMMY);

    if (log2size < 8) {
        assert(ix <= 0x7f);
        STORE_INDEX(keys, 8, i, ix);
    } else if (log2size < 16) {
        assert(ix <= 0x7fff);
        STORE_INDEX(keys, 16, i, ix);
    }
#if SIZEOF_VOID_P > 4
    else if (log2size >= 32) {
        STORE_INDEX(keys, 64, i, ix);
    }
#endif
    else {
        assert(ix <= 0x7fffffff);
        STORE_INDEX(keys, 32, i, ix);
    }
}

/* USABLE_FRACTION is the maximum dictionary load.
 * Increasing this ratio makes dictionaries more dense resulting in more
 * collisions.  Decreasing it improves sparseness at the expense of spreading
 * indices over more cache lines and at the cost of total memory consumed.
 *
 * USABLE_FRACTION must obey the following:
 *     (0 < USABLE_FRACTION(n) < n) for all n >= 2
 *
 * USABLE_FRACTION should be quick to calculate.
 * Fractions around 1/2 to 2/3 seem to work well in practice.
 */
static inline Py_ssize_t
USABLE_FRACTION(Py_ssize_t n)
{
    return (n << 1) / 3;
}

// Return the index of the most significant 1 bit in 'x'. This is the smallest
// integer k such that x < 2**k. Equivalent to floor(log2(x)) + 1 for x != 0.
static inline int
_ht_bit_length(unsigned long x)
{
#if (defined(__clang__) || defined(__GNUC__))
    if (x != 0) {
        // __builtin_clzl() is available since GCC 3.4.
        // Undefined behavior for x == 0.
        return (int)sizeof(unsigned long) * 8 - __builtin_clzl(x);
    } else {
        return 0;
    }
#elif defined(_MSC_VER)
    // _BitScanReverse() is documented to search 32 bits.
    Py_BUILD_ASSERT(sizeof(unsigned long) <= 4);
    unsigned long msb;
    if (_BitScanReverse(&msb, x)) {
        return (int)msb + 1;
    } else {
        return 0;
    }
#else
    const int BIT_LENGTH_TABLE[32] = {0, 1, 2, 2, 3, 3, 3, 3, 4, 4, 4,
                                      4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5,
                                      5, 5, 5, 5, 5, 5, 5, 5, 5, 5};
    int msb = 0;
    while (x >= 32) {
        msb += 6;
        x >>= 6;
    }
    msb += BIT_LENGTH_TABLE[x];
    return msb;
#endif
}

/* Find the smallest dk_size >= minsize. */
static inline uint8_t
calculate_log2_keysize(Py_ssize_t minsize)
{
#if SIZEOF_LONG == SIZEOF_SIZE_T
    minsize = (minsize | HT_MINSIZE) - 1;
    return _ht_bit_length(minsize | (HT_MINSIZE - 1));
#elif defined(_MSC_VER)
    // On 64bit Windows, sizeof(long) == 4.
    minsize = (minsize | HT_MINSIZE) - 1;
    unsigned long msb;
    _BitScanReverse64(&msb, (uint64_t)minsize);
    return (uint8_t)(msb + 1);
#else
    uint8_t log2_size;
    for (log2_size = HT_LOG_MINSIZE; (((Py_ssize_t)1) << log2_size) < minsize;
         log2_size++);
    return log2_size;
#endif
}

/* estimate_keysize is reverse function of USABLE_FRACTION.
 *
 * This can be used to reserve enough size to insert n entries without
 * resizing.
 */
static inline uint8_t
estimate_log2_keysize(Py_ssize_t n)
{
    return calculate_log2_keysize((n * 3 + 1) / 2);
}

/* This immutable, empty PyDictKeysObject is used for PyDict_Clear()
 * (which cannot fail and thus can do no allocation).
 *
 * See https://github.com/python/cpython/pull/127568#discussion_r1868070614
 * for the rationale of using log2_index_bytes=3 instead of 0.
 */
static const htkeys_t empty_htkeys = {
    .log2_size = 0,
    .log2_index_bytes = 3,
    .usable = 0, /* immutable */
    .nentries = 0,
    .resume_slots = NULL,
#ifdef Py_GIL_DISABLED
    .num_readers = 0,
    .retired_next = NULL,
#endif
    .indices = {DKIX_EMPTY,
                DKIX_EMPTY,
                DKIX_EMPTY,
                DKIX_EMPTY,
                DKIX_EMPTY,
                DKIX_EMPTY,
                DKIX_EMPTY,
                DKIX_EMPTY},
};

/* resume_slots[i] is 0 or 1 + the last slot used by a probe that reached slot
   i with perturb == 0. From there the probe sequence only depends on the slot
   and slots never become empty again, so probing can resume. Stored like the
   indices, uint16_t below 2**16 slots and uint32_t below 2**32, slot + 1 must
   fit. */
static inline size_t
htkeys_resume_slots_bytes(uint8_t log2_size)
{
    if (log2_size < HT_LOG_RESUME_SLOTS_MINSIZE || log2_size >= 32) {
        return 0;
    }
    if (log2_size < 16) {
        return sizeof(uint16_t) << log2_size;
    }
    return sizeof(uint32_t) << log2_size;
}

static inline Py_ssize_t
htkeys_sizeof(htkeys_t* keys)
{
    Py_ssize_t usable = USABLE_FRACTION((size_t)1 << keys->log2_size);
    return (sizeof(htkeys_t) + ((size_t)1 << keys->log2_index_bytes) +
            sizeof(entry_t) * usable);
}

static inline htkeys_t*
htkeys_new(uint8_t log2_size)
{
    assert(log2_size >= HT_LOG_MINSIZE);

    Py_ssize_t usable = USABLE_FRACTION(((size_t)1) << log2_size);
    uint8_t log2_bytes;

    if (log2_size < 8) {
        log2_bytes = log2_size;
    } else if (log2_size < 16) {
        log2_bytes = log2_size + 1;
    }
#if SIZEOF_VOID_P > 4
    else if (log2_size >= 32) {
        log2_bytes = log2_size + 3;
    }
#endif
    else {
        log2_bytes = log2_size + 2;
    }

    htkeys_t* keys = NULL;
    /* TODO: CPython uses freelist of key objects with unicode type
       and log2_size == PyDict_LOG_MINSIZE */
    keys = PyMem_Malloc(sizeof(htkeys_t) + ((size_t)1 << log2_bytes) +
                        sizeof(entry_t) * usable);
    if (keys == NULL) {
        PyErr_NoMemory();
        return NULL;
    }

    keys->log2_size = log2_size;
    keys->log2_index_bytes = log2_bytes;
    keys->resume_slots = NULL;
    keys->nentries = 0;
    keys->usable = usable;
#ifdef Py_GIL_DISABLED
    keys->num_readers = 0;
    keys->retired_next = NULL;
#endif
    memset(&keys->indices[0], 0xff, ((size_t)1 << log2_bytes));
    memset(
        &keys->indices[(size_t)1 << log2_bytes], 0, sizeof(entry_t) * usable);
    return keys;
}

static inline void
htkeys_free(htkeys_t* dk)
{
    /* TODO: CPython uses freelist of key objects with unicode type
       and log2_size == PyDict_LOG_MINSIZE */
    if (dk->resume_slots != NULL) {
        PyMem_Free(dk->resume_slots);
    }
    PyMem_Free(dk);
}

/* Returns the identity's hash folded into its non-negative half (see the
   MD_HASH_MARK comment in hashtable.h), or -1 if hashing raised. Only the
   value returned to the caller is folded; the unicode object's own cached
   hash slot is left untouched, since it is shared with the rest of the
   process. */
static inline Py_hash_t
_unicode_hash(PyObject* o)
{
    assert(PyUnicode_CheckExact(o));
    PyASCIIObject* ascii = (PyASCIIObject*)o;
    Py_hash_t hash = atomic_load_ssize_relaxed(&ascii->hash);
    if (hash == -1) {
        hash = PyUnicode_Type.tp_hash(o);
        if (hash == -1) {
            return -1;
        }
    }
    return hash & PY_SSIZE_T_MAX;
}

/* Values for the same key share a probe sequence, so without resume slots the
   n-th one walks past the n - 1 before it. Called once perturb is 0 and
   slot i is next to probe. Resume slots are only allocated once a probe here
   is long, so tables without long chains don't pay for them. */
HT_COLD static Py_ssize_t
_htkeys_find_empty_slot_resume(htkeys_t* keys, size_t i)
{
    const size_t mask = htkeys_mask(keys);
    const size_t start = i;
    const bool small = keys->log2_size < 16;
    void* resume_slots = keys->resume_slots;
    if (resume_slots != NULL) {
        size_t resume = small ? ((uint16_t*)resume_slots)[start]
                              : ((uint32_t*)resume_slots)[start];
        if (resume != 0) {
            /* the resume slot is used, start after it */
            i = ((resume - 1) * 5 + 1) & mask;
        }
    }
    size_t steps = 0;
    while (htkeys_get_index(keys, i) != DKIX_EMPTY) {
        i = (i * 5 + 1) & mask;
        steps++;
    }
    if (resume_slots == NULL) {
        size_t nbytes = htkeys_resume_slots_bytes(keys->log2_size);
        if (steps < HT_RESUME_SLOTS_MIN_STEPS || nbytes == 0) {
            return (Py_ssize_t)i;
        }
        resume_slots = keys->resume_slots = PyMem_Malloc(nbytes);
        if (resume_slots == NULL) {
            return (Py_ssize_t)i;
        }
        memset(resume_slots, 0, nbytes);
    }
    if (small) {
        ((uint16_t*)resume_slots)[start] = (uint16_t)(i + 1);
    } else {
        ((uint32_t*)resume_slots)[start] = (uint32_t)(i + 1);
    }
    return (Py_ssize_t)i;
}

/*
Internal routine used by ht_resize() to build a hashtable of entries.
*/
static inline int
htkeys_build_indices(htkeys_t* keys, entry_t* ep, Py_ssize_t n, bool update)
{
    size_t mask = htkeys_mask(keys);
    if (keys->resume_slots != NULL) {
        memset(
            keys->resume_slots, 0, htkeys_resume_slots_bytes(keys->log2_size));
    }
    for (Py_ssize_t ix = 0; ix != n; ix++, ep++) {
        Py_hash_t hash = ep->hash;
#ifdef Py_GIL_DISABLED
        /* Unconditionally, not just when update: under free threading
           a marked entry copied in here can belong to an entirely
           different, concurrently-suspended _md_replace()/_md_update()
           call (on some other key) that this resize's own update flag
           knows nothing about -- see the comment in
           _md_check_consistency(). Indexing it by its temporary marked
           hash would place it somewhere its real hash's probe sequence
           never looks, making it permanently unfindable once the
           owning call unmarks it back. */
        if (hash < 0) {
            hash &= PY_SSIZE_T_MAX;
        }
#else
        if (update && hash < 0) {
            hash &= PY_SSIZE_T_MAX;
        }
#endif
        size_t i = hash & mask;
        for (size_t perturb = hash; htkeys_get_index(keys, i) != DKIX_EMPTY;) {
            perturb >>= HT_PERTURB_SHIFT;
            i = mask & (i * 5 + perturb + 1);
            if (HT_UNLIKELY(perturb == 0)) {
                i = (size_t)_htkeys_find_empty_slot_resume(keys, i);
                break;
            }
        }
        htkeys_set_index(keys, i, ix);
    }
    return 0;
}

/* Uses keys, mask, i and perturb from the caller and returns. */
#define _HT_FIND_EMPTY_SLOT(size)                           \
    while (LOAD_INDEX(keys, size, i) != DKIX_EMPTY) {       \
        perturb >>= HT_PERTURB_SHIFT;                       \
        i = (i * 5 + perturb + 1) & mask;                   \
        if (HT_UNLIKELY(perturb == 0)) {                    \
            return _htkeys_find_empty_slot_resume(keys, i); \
        }                                                   \
    }                                                       \
    return (Py_ssize_t)i;

/* Internal function to find slot for an item from its hash
   when it is known that the key is not present in the dict.
   The caller must fill the returned slot, resume slots skip past it.

   Unswitched by index size by hand so the perturb check stays cheap.
 */
static inline Py_ssize_t
htkeys_find_empty_slot(htkeys_t* keys, Py_hash_t hash)
{
    const size_t mask = htkeys_mask(keys);
    size_t i = hash & mask;
    size_t perturb = (size_t)hash;
    uint8_t log2size = keys->log2_size;
    if (log2size < 8) {
        while (LOAD_INDEX(keys, 8, i) != DKIX_EMPTY) {
            perturb >>= HT_PERTURB_SHIFT;
            i = (i * 5 + perturb + 1) & mask;
        }
        return (Py_ssize_t)i;
    } else if (log2size < 16) {
        _HT_FIND_EMPTY_SLOT(16)
    }
#if SIZEOF_VOID_P > 4
    else if (log2size >= 32) {
        _HT_FIND_EMPTY_SLOT(64)
    }
#endif
    else {
        _HT_FIND_EMPTY_SLOT(32)
    }
}

#undef _HT_FIND_EMPTY_SLOT

/* Iterator over slots/indexes for given hash.
   N.B. The iterator MIGHT return the same slot
   multiple times, eiter consequently (1, 2, 2, 3)
   or with different slots in the middle (1, 2, 3, 1).

   The caller is responsible to mark visited slots
   and cleanup the mark after the iteration finish.

   See ht_finder_t for an object designed for such operations.
*/

typedef struct _htkeysiter {
    htkeys_t* keys;
    size_t mask;  // htkeys_mask(keys)
    size_t slot;  // masked hash, Py_hash_t h & mask;
    size_t perturb;
    Py_ssize_t index;
} htkeysiter_t;

static inline void
htkeysiter_init(htkeysiter_t* iter, htkeys_t* keys, Py_hash_t hash)
{
    iter->keys = keys;
    iter->mask = htkeys_mask(keys);
    iter->perturb = (size_t)hash;
    iter->slot = hash & iter->mask;
    iter->index = htkeys_get_index(iter->keys, iter->slot);
}

static inline void
htkeysiter_next(htkeysiter_t* iter)
{
    iter->perturb >>= HT_PERTURB_SHIFT;
    iter->slot = (iter->slot * 5 + iter->perturb + 1) & iter->mask;
    iter->index = htkeys_get_index(iter->keys, iter->slot);
}

#ifdef __cplusplus
}
#endif
#endif
