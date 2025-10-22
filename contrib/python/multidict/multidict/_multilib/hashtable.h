#include "pythoncapi_compat.h"

#ifndef _MULTIDICT_HASHTABLE_H
#define _MULTIDICT_HASHTABLE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "dict.h"
#include "htkeys.h"
#include "istr.h"
#include "state.h"

typedef struct _md_pos {
    Py_ssize_t pos;
    uint64_t version;
} md_pos_t;

typedef struct _md_finder {
    MultiDictObject *md;
    htkeysiter_t iter;
    uint64_t version;
    Py_hash_t hash;
    PyObject *identity;  // borrowed ref
} md_finder_t;

typedef enum _UpdateOp {
    Extend,
    Update,
    Merge,
} UpdateOp;

/*
The multidict's implementation is close to Python's dict except for multiple
keys.

It starts from the empty hashtable, which grows by a power of 2 starting from
8: 8, 16, 32, 64, 128, ...  The amount of items is 2/3 of the hashtable size
(1/3 of the table is never allocated).

The table is resized if needed, and bulk updates (extend(), update(), and
constructor calls) pre-allocate many items at once, reducing the amount of
potential hashtable resizes.

Item deletion puts DKIX_DUMMY special index in the hashtable. In opposite to
the standard dict, DKIX_DUMMY is never replaced with an index of the new entry
except by hashtable indices rebuild. It allows to keep the insertion order for
multiple equal keys. The index table rebuild happens on the keys table size
changeing and if the number of DKIX_DUMMY slots grows to 1/4 of the total
amount.

The iteration for operations like getall() is a little tricky. The next index
calculation could return the already visited index before reaching the end. To
eliminate duplicates, the code marks already visited entries by entry->hash =
-1. -1 hash is an invalid hash value that could be used as a marker. After the
iteration finishes, all marked entries are restored.  Double iteration over the
indices still has O(1) amortized time, it is ok.

`.add()`, `val = md[key]`, `md[key] = val`, `md.setdefault()` all have O(1).
`.getall()` / `.popall()` have O(N) where N is the amount of returned items.
`.update()` / `extend()` have O(N+M) where N and M are amount of items
in the left and right arguments.

`.copy()` and constuction from multidict is super fast.
*/

/* GROWTH_RATE. Growth rate upon hitting maximum load.
 * Currently set to used*3.
 * This means that dicts double in size when growing without deletions,
 * but have more head room when the number of deletions is on a par with the
 * number of insertions.  See also bpo-17563 and bpo-33205.
 *
 * GROWTH_RATE was set to used*4 up to version 3.2.
 * GROWTH_RATE was set to used*2 in version 3.3.0
 * GROWTH_RATE was set to used*2 + capacity/2 in 3.4.0-3.6.0.
 */
static inline Py_ssize_t
GROWTH_RATE(MultiDictObject *md)
{
    return md->used * 3;
}

#ifndef NDEBUG
static inline int
_md_check_consistency(MultiDictObject *md, bool update);
static inline int
_md_dump(MultiDictObject *md);

#define ASSERT_CONSISTENT(md, update) assert(_md_check_consistency(md, update))
#else
#define ASSERT_CONSISTENT(md, update) assert(1)
#endif

static inline int
_str_cmp(PyObject *s1, PyObject *s2)
{
    PyObject *ret = PyUnicode_RichCompare(s1, s2, Py_EQ);
    if (Py_IsTrue(ret)) {
        Py_DECREF(ret);
        return 1;
    } else if (ret == NULL) {
        return -1;
    } else {
        Py_DECREF(ret);
        return 0;
    }
}

static inline PyObject *
_key_to_identity(mod_state *state, PyObject *key)
{
    if (IStr_Check(state, key)) {
        return Py_NewRef(((istrobject *)key)->canonical);
    }
    if (PyUnicode_CheckExact(key)) {
        return Py_NewRef(key);
    }
    if (PyUnicode_Check(key)) {
        return PyUnicode_FromObject(key);
    }
    PyErr_SetString(PyExc_TypeError,
                    "MultiDict keys should be either str "
                    "or subclasses of str");
    return NULL;
}

static inline PyObject *
_ci_key_to_identity(mod_state *state, PyObject *key)
{
    if (IStr_Check(state, key)) {
        return Py_NewRef(((istrobject *)key)->canonical);
    }
    if (PyUnicode_Check(key)) {
        PyObject *ret = PyObject_CallMethodNoArgs(key, state->str_lower);
        if (ret == NULL) {
            goto fail;
        }
        if (!PyUnicode_CheckExact(ret)) {
            PyObject *tmp = PyUnicode_FromObject(ret);
            Py_CLEAR(ret);
            if (tmp == NULL) {
                return NULL;
            }
            ret = tmp;
        }
        return ret;
    }
    PyErr_SetString(PyExc_TypeError,
                    "CIMultiDict keys should be either str "
                    "or subclasses of str");
fail:
    return NULL;
}

static inline PyObject *
_arg_to_key(mod_state *state, PyObject *key, PyObject *identity)
{
    if (PyUnicode_Check(key)) {
        return Py_NewRef(key);
    }
    PyErr_SetString(PyExc_TypeError,
                    "MultiDict keys should be either str "
                    "or subclasses of str");
    return NULL;
}

static inline PyObject *
_ci_arg_to_key(mod_state *state, PyObject *key, PyObject *identity)
{
    if (IStr_Check(state, key)) {
        return Py_NewRef(key);
    }
    if (PyUnicode_Check(key)) {
        return IStr_New(state, key, identity);
    }
    PyErr_SetString(PyExc_TypeError,
                    "CIMultiDict keys should be either str "
                    "or subclasses of str");
    return NULL;
}

static inline int
_md_resize(MultiDictObject *md, uint8_t log2_newsize, bool update)
{
    htkeys_t *oldkeys, *newkeys;

    if (log2_newsize >= SIZEOF_SIZE_T * 8) {
        PyErr_NoMemory();
        return -1;
    }
    assert(log2_newsize >= HT_LOG_MINSIZE);

    oldkeys = md->keys;

    /* Allocate a new table. */
    newkeys = htkeys_new(log2_newsize);
    assert(newkeys);
    if (newkeys == NULL) {
        return -1;
    }
    // New table must be large enough.
    assert(newkeys->usable >= md->used);

    Py_ssize_t numentries = md->used;
    entry_t *oldentries = htkeys_entries(oldkeys);
    entry_t *newentries = htkeys_entries(newkeys);
    if (oldkeys->nentries == numentries) {
        memcpy(newentries, oldentries, numentries * sizeof(entry_t));
    } else {
        entry_t *new_ep = newentries;
        entry_t *old_ep = oldentries;
        Py_ssize_t oldnumentries = oldkeys->nentries;
        for (Py_ssize_t i = 0; i < oldnumentries; ++i, ++old_ep) {
            if (old_ep->identity != NULL) {
                *new_ep++ = *old_ep;
            }
        }
    }

    if (htkeys_build_indices(newkeys, newentries, numentries, update) < 0) {
        return -1;
    }

    md->keys = newkeys;

    if (oldkeys != &empty_htkeys) {
        htkeys_free(oldkeys);
    }

    md->keys->usable = md->keys->usable - numentries;
    md->keys->nentries = numentries;
    ASSERT_CONSISTENT(md, update);
    return 0;
}

static inline int
_md_shrink(MultiDictObject *md, bool update)
{
    htkeys_t *keys = md->keys;
    Py_ssize_t nentries = keys->nentries;
    entry_t *entries = htkeys_entries(keys);
    entry_t *new_ep = entries;
    entry_t *old_ep = entries;
    Py_ssize_t newnentries = nentries;
    for (Py_ssize_t i = 0; i < nentries; ++i, ++old_ep) {
        if (old_ep->identity != NULL) {
            if (new_ep != old_ep) {
                *new_ep = *old_ep;
            }
            new_ep++;
        } else {
            newnentries -= 1;
        }
    }
    keys->nentries = newnentries;
    keys->usable += nentries - newnentries;
    memset(&keys->indices[0], 0xff, ((size_t)1 << keys->log2_index_bytes));
    memset(new_ep, 0, sizeof(entry_t) * (size_t)(nentries - newnentries));
    if (htkeys_build_indices(keys, entries, newnentries, update) < 0) {
        return -1;
    }
    ASSERT_CONSISTENT(md, update);
    return 0;
}

static inline int
_md_resize_for_insert(MultiDictObject *md)
{
    if (md->used < md->keys->nentries) {
        return _md_shrink(md, false);
    } else {
        return _md_resize(md, calculate_log2_keysize(GROWTH_RATE(md)), false);
    }
}

static inline int
_md_resize_for_update(MultiDictObject *md)
{
    if (md->used < md->keys->nentries) {
        return _md_shrink(md, true);
    } else {
        return _md_resize(md, calculate_log2_keysize(GROWTH_RATE(md)), true);
    }
}

static inline int
_md_reserve(MultiDictObject *md, Py_ssize_t extra_size, bool update)
{
    uint8_t new_size = estimate_log2_keysize(extra_size + md->used);
    if (new_size > md->keys->log2_size) {
        return _md_resize(md, new_size, update);
    }
    return 0;
}

static inline int
md_reserve(MultiDictObject *md, Py_ssize_t extra_size)
{
    return _md_reserve(md, extra_size, false);
}

static inline int
md_init(MultiDictObject *md, mod_state *state, bool is_ci, Py_ssize_t minused)
{
    md->state = state;
    md->is_ci = is_ci;
    md->used = 0;
    md->version = NEXT_VERSION(md->state);

    const uint8_t log2_max_presize = 17;
    const Py_ssize_t max_presize = ((Py_ssize_t)1) << log2_max_presize;
    uint8_t log2_newsize;
    htkeys_t *new_keys;

    if (minused <= USABLE_FRACTION(HT_MINSIZE)) {
        md->keys = &empty_htkeys;
        ASSERT_CONSISTENT(md, false);
        return 0;
    }
    /* There are no strict guarantee that returned dict can contain minused
     * items without resize.  So we create medium size dict instead of very
     * large dict or MemoryError.
     */
    if (minused > USABLE_FRACTION(max_presize)) {
        log2_newsize = log2_max_presize;
    } else {
        log2_newsize = estimate_log2_keysize(minused);
    }

    new_keys = htkeys_new(log2_newsize);
    if (new_keys == NULL) return -1;
    md->keys = new_keys;
    ASSERT_CONSISTENT(md, false);
    return 0;
}

static inline int
md_clone_from_ht(MultiDictObject *md, MultiDictObject *other)
{
    ASSERT_CONSISTENT(other, false);
    md->state = other->state;
    md->used = other->used;
    md->version = other->version;
    md->is_ci = other->is_ci;
    if (other->keys != &empty_htkeys) {
        size_t size = htkeys_sizeof(other->keys);
        htkeys_t *keys = PyMem_Malloc(size);
        if (keys == NULL) {
            PyErr_NoMemory();
            return -1;
        }
        memcpy(keys, other->keys, size);
        entry_t *entry = htkeys_entries(keys);
        for (Py_ssize_t idx = 0; idx < keys->nentries; idx++, entry++) {
            Py_XINCREF(entry->identity);
            Py_XINCREF(entry->key);
            Py_XINCREF(entry->value);
        }
        md->keys = keys;
    } else {
        md->keys = &empty_htkeys;
    }
    ASSERT_CONSISTENT(md, false);
    return 0;
}

static inline PyObject *
md_calc_identity(MultiDictObject *md, PyObject *key)
{
    if (md->is_ci) return _ci_key_to_identity(md->state, key);
    return _key_to_identity(md->state, key);
}

static inline PyObject *
md_calc_key(MultiDictObject *md, PyObject *key, PyObject *identity)
{
    if (md->is_ci) return _ci_arg_to_key(md->state, key, identity);
    return _arg_to_key(md->state, key, identity);
}

static inline Py_ssize_t
md_len(MultiDictObject *md)
{
    return md->used;
}

static inline PyObject *
_md_ensure_key(MultiDictObject *md, entry_t *entry)
{
    assert(entry >= htkeys_entries(md->keys));
    assert(entry < htkeys_entries(md->keys) + md->keys->nentries);
    PyObject *key = md_calc_key(md, entry->key, entry->identity);
    if (key == NULL) {
        return NULL;
    }
    if (key != entry->key) {
        Py_SETREF(entry->key, key);
    } else {
        Py_CLEAR(key);
    }
    return Py_NewRef(entry->key);
}

static inline int
_md_add_with_hash_steal_refs(MultiDictObject *md, Py_hash_t hash,
                             PyObject *identity, PyObject *key,
                             PyObject *value)
{
    htkeys_t *keys = md->keys;
    if (keys->usable <= 0 || keys == &empty_htkeys) {
        /* Need to resize. */
        if (_md_resize_for_insert(md) < 0) {
            return -1;
        }
        keys = md->keys;  // updated by resizing
    }

    Py_ssize_t hashpos = htkeys_find_empty_slot(keys, hash);
    htkeys_set_index(keys, hashpos, keys->nentries);

    entry_t *entry = htkeys_entries(keys) + keys->nentries;

    entry->identity = identity;
    entry->key = key;
    entry->value = value;
    entry->hash = hash;

    md->version = NEXT_VERSION(md->state);
    md->used += 1;
    keys->usable -= 1;
    keys->nentries += 1;
    return 0;
}

static inline int
_md_add_with_hash(MultiDictObject *md, Py_hash_t hash, PyObject *identity,
                  PyObject *key, PyObject *value)
{
    Py_INCREF(identity);
    Py_INCREF(key);
    Py_INCREF(value);
    return _md_add_with_hash_steal_refs(md, hash, identity, key, value);
}

static inline int
_md_add_for_upd_steal_refs(MultiDictObject *md, Py_hash_t hash,
                           PyObject *identity, PyObject *key, PyObject *value)
{
    htkeys_t *keys = md->keys;
    if (keys->usable <= 0 || keys == &empty_htkeys) {
        /* Need to resize. */
        if (_md_resize_for_update(md) < 0) {
            return -1;
        }
        keys = md->keys;  // updated by resizing
    }
    Py_ssize_t hashpos = htkeys_find_empty_slot(keys, hash);
    htkeys_set_index(keys, hashpos, keys->nentries);

    entry_t *entry = htkeys_entries(keys) + keys->nentries;

    entry->identity = identity;
    entry->key = key;
    entry->value = value;
    entry->hash = -1;

    md->version = NEXT_VERSION(md->state);
    md->used += 1;
    keys->usable -= 1;
    keys->nentries += 1;
    return 0;
}

static inline int
_md_add_for_upd(MultiDictObject *md, Py_hash_t hash, PyObject *identity,
                PyObject *key, PyObject *value)
{
    Py_INCREF(identity);
    Py_INCREF(key);
    Py_INCREF(value);
    return _md_add_for_upd_steal_refs(md, hash, identity, key, value);
}

static inline int
md_add(MultiDictObject *md, PyObject *key, PyObject *value)
{
    PyObject *identity = md_calc_identity(md, key);
    if (identity == NULL) {
        goto fail;
    }
    Py_hash_t hash = _unicode_hash(identity);
    if (hash == -1) {
        goto fail;
    }
    int ret = _md_add_with_hash(md, hash, identity, key, value);
    ASSERT_CONSISTENT(md, false);
    Py_DECREF(identity);
    return ret;
fail:
    Py_XDECREF(identity);
    return -1;
}

static inline int
_md_del_at(MultiDictObject *md, size_t slot, entry_t *entry)
{
    htkeys_t *keys = md->keys;
    assert(keys != &empty_htkeys);
    Py_CLEAR(entry->identity);
    Py_CLEAR(entry->key);
    Py_CLEAR(entry->value);
    htkeys_set_index(keys, slot, DKIX_DUMMY);
    md->used -= 1;
    return 0;
}

static inline int
_md_del_at_for_upd(MultiDictObject *md, size_t slot, entry_t *entry)
{
    /* half deletion,
       the entry could be replaced later with key and value set
       or it will be finally cleaned up with identity=NULL,
       used -= 1, and setting the hash to DKIX_DUMMY
       in md_post_update()
    */
    assert(md->keys != &empty_htkeys);
    Py_CLEAR(entry->key);
    Py_CLEAR(entry->value);
    return 0;
}

static inline int
md_del(MultiDictObject *md, PyObject *key)
{
    PyObject *identity = md_calc_identity(md, key);
    if (identity == NULL) {
        goto fail;
    }

    Py_hash_t hash = _unicode_hash(identity);
    if (hash == -1) {
        goto fail;
    }

    bool found = false;

    htkeysiter_t iter;
    htkeysiter_init(&iter, md->keys, hash);

    entry_t *entries = htkeys_entries(md->keys);

    for (; iter.index != DKIX_EMPTY; htkeysiter_next(&iter)) {
        if (iter.index < 0) {
            continue;
        }
        entry_t *entry = entries + iter.index;
        if (hash != entry->hash) {
            continue;
        }
        int tmp = _str_cmp(entry->identity, identity);
        if (tmp < 0) {
            goto fail;
        }
        if (tmp == 0) {
            continue;
        }

        found = true;
        if (_md_del_at(md, iter.slot, entry) < 0) {
            goto fail;
        }
    }

    if (!found) {
        PyErr_SetObject(PyExc_KeyError, key);
        goto fail;
    } else {
        md->version = NEXT_VERSION(md->state);
    }
    Py_DECREF(identity);
    ASSERT_CONSISTENT(md, false);
    return 0;
fail:
    Py_XDECREF(identity);
    return -1;
}

static inline uint64_t
md_version(MultiDictObject *md)
{
    return md->version;
}

static inline void
md_init_pos(MultiDictObject *md, md_pos_t *pos)
{
    pos->pos = 0;
    pos->version = md->version;
}

static inline int
md_next(MultiDictObject *md, md_pos_t *pos, PyObject **pidentity,
        PyObject **pkey, PyObject **pvalue)
{
    int ret = 0;

    if (pos->version != md->version) {
        PyErr_SetString(PyExc_RuntimeError,
                        "MultiDict is changed during iteration");
        ret = -1;
        goto cleanup;
    }

    if (pos->pos >= md->keys->nentries) {
        goto cleanup;
    }

    entry_t *entries = htkeys_entries(md->keys);
    entry_t *entry = entries + pos->pos;

    while (entry->identity == NULL) {
        pos->pos += 1;
        if (pos->pos >= md->keys->nentries) {
            goto cleanup;
        }
        entry += 1;
    }

    if (pidentity) {
        *pidentity = Py_NewRef(entry->identity);
    }

    if (pkey) {
        assert(entry->key != NULL);
        *pkey = _md_ensure_key(md, entry);
        if (*pkey == NULL) {
            assert(PyErr_Occurred());
            ret = -1;
            goto cleanup;
        }
    }
    if (pvalue) {
        *pvalue = Py_NewRef(entry->value);
    }

    ++pos->pos;
    return 1;
cleanup:
    if (pidentity) {
        *pidentity = NULL;
    }
    if (pkey) {
        *pkey = NULL;
    }
    if (pvalue) {
        *pvalue = NULL;
    }
    return ret;
}

static inline int
md_init_finder(MultiDictObject *md, PyObject *identity, md_finder_t *finder)
{
    finder->version = md->version;
    finder->md = md;
    finder->identity = identity;
    finder->hash = _unicode_hash(identity);
    if (finder->hash == -1) {
        return -1;
    }
    htkeysiter_init(&finder->iter, finder->md->keys, finder->hash);
    return 0;
}

static inline Py_ssize_t
md_finder_slot(md_finder_t *finder)
{
    assert(finder->md != NULL);
    return finder->iter.slot;
}

static inline Py_ssize_t
md_finder_index(md_finder_t *finder)
{
    assert(finder->md != NULL);
    assert(finder->iter.index >= 0);
    return finder->iter.index;
}

static inline int
md_find_next(md_finder_t *finder, PyObject **pkey, PyObject **pvalue)
{
    int ret = 0;
    assert(finder->iter.keys == finder->md->keys);
    if (finder->iter.keys != finder->md->keys ||
        finder->version != finder->md->version) {
        ret = -1;
        PyErr_SetString(PyExc_RuntimeError,
                        "MultiDict is changed during iteration");
        goto cleanup;
    }

    entry_t *entries = htkeys_entries(finder->md->keys);

    for (; finder->iter.index != DKIX_EMPTY; htkeysiter_next(&finder->iter)) {
        if (finder->iter.index < 0) {
            continue;
        }
        entry_t *entry = entries + finder->iter.index;
        if (entry->hash != finder->hash) {
            continue;
        }
        int tmp = _str_cmp(finder->identity, entry->identity);
        if (tmp < 0) {
            ret = -1;
            goto cleanup;
        }
        if (tmp == 0) {
            continue;
        }

        /* found, mark the entry as visited */
        entry->hash = -1;

        if (pkey) {
            *pkey = _md_ensure_key(finder->md, entry);
            if (*pkey == NULL) {
                ret = -1;
                goto cleanup;
            }
        }
        if (pvalue) {
            *pvalue = Py_NewRef(entry->value);
        }
        return 1;
    }
    ret = 0;
cleanup:
    if (pkey) {
        *pkey = NULL;
    }
    if (pvalue) {
        *pvalue = NULL;
    }
    return ret;
}

static inline void
md_finder_cleanup(md_finder_t *finder)
{
    if (finder->md == NULL) {
        return;
    }

    htkeysiter_init(&finder->iter, finder->md->keys, finder->hash);
    entry_t *entries = htkeys_entries(finder->md->keys);
    for (; finder->iter.index != DKIX_EMPTY; htkeysiter_next(&finder->iter)) {
        if (finder->iter.index < 0) {
            continue;
        }
        entry_t *entry = entries + finder->iter.index;
        if (entry->hash == -1) {
            entry->hash = finder->hash;
        }
    }
    ASSERT_CONSISTENT(finder->md, false);
    finder->md = NULL;
}

static inline int
md_contains(MultiDictObject *md, PyObject *key, PyObject **pret)
{
    if (!PyUnicode_Check(key)) {
        return 0;
    }

    PyObject *identity = md_calc_identity(md, key);
    if (identity == NULL) {
        goto fail;
    }

    Py_hash_t hash = _unicode_hash(identity);
    if (hash == -1) {
        goto fail;
    }

    htkeysiter_t iter;
    htkeysiter_init(&iter, md->keys, hash);
    entry_t *entries = htkeys_entries(md->keys);

    for (; iter.index != DKIX_EMPTY; htkeysiter_next(&iter)) {
        if (iter.index < 0) {
            continue;
        }
        entry_t *entry = entries + iter.index;
        if (hash != entry->hash) {
            continue;
        }
        int tmp = _str_cmp(identity, entry->identity);
        if (tmp > 0) {
            Py_DECREF(identity);
            if (pret != NULL) {
                *pret = _md_ensure_key(md, entry);
                if (*pret == NULL) {
                    goto fail;
                }
            }
            return 1;
        } else if (tmp < 0) {
            goto fail;
        }
    }

    Py_DECREF(identity);
    if (pret != NULL) {
        *pret = NULL;
    }
    return 0;
fail:
    Py_XDECREF(identity);
    if (pret != NULL) {
        *pret = NULL;
    }
    return -1;
}

static inline int
md_get_one(MultiDictObject *md, PyObject *key, PyObject **ret)
{
    PyObject *identity = md_calc_identity(md, key);
    if (identity == NULL) {
        goto fail;
    }

    Py_hash_t hash = _unicode_hash(identity);
    if (hash == -1) {
        goto fail;
    }

    htkeysiter_t iter;
    htkeysiter_init(&iter, md->keys, hash);
    entry_t *entries = htkeys_entries(md->keys);

    for (; iter.index != DKIX_EMPTY; htkeysiter_next(&iter)) {
        if (iter.index < 0) {
            continue;
        }
        entry_t *entry = entries + iter.index;
        if (hash != entry->hash) {
            continue;
        }
        int tmp = _str_cmp(identity, entry->identity);
        if (tmp > 0) {
            Py_DECREF(identity);
            *ret = Py_NewRef(entry->value);
            return 1;
        } else if (tmp < 0) {
            goto fail;
        }
    }

    Py_DECREF(identity);
    return 0;
fail:
    Py_XDECREF(identity);
    return -1;
}

static inline int
md_get_all(MultiDictObject *md, PyObject *key, PyObject **ret)
{
    int tmp;
    PyObject *value = NULL;
    *ret = NULL;

    md_finder_t finder = {0};

    PyObject *identity = md_calc_identity(md, key);
    if (identity == NULL) {
        goto fail;
    }

    if (md_init_finder(md, identity, &finder) < 0) {
        assert(PyErr_Occurred());
        goto fail;
    }

    while ((tmp = md_find_next(&finder, NULL, &value)) > 0) {
        if (*ret == NULL) {
            *ret = PyList_New(1);
            if (*ret == NULL) {
                goto fail;
            }
            PyList_SET_ITEM(*ret, 0, value);
            value = NULL;  // stealed by PyList_SET_ITEM
        } else {
            if (PyList_Append(*ret, value) < 0) {
                goto fail;
            }
            Py_CLEAR(value);
        }
    }
    if (tmp < 0) {
        goto fail;
    }

    if (*ret != NULL) {
        // there is no need to restore hashes if none was marked
        md_finder_cleanup(&finder);
    }
    Py_DECREF(identity);
    return *ret != NULL;
fail:
    md_finder_cleanup(&finder);
    Py_XDECREF(identity);
    Py_XDECREF(value);
    Py_CLEAR(*ret);
    return -1;
}

static inline int
md_set_default(MultiDictObject *md, PyObject *key, PyObject *value,
               PyObject **result)
{
    *result = NULL;
    PyObject *identity = md_calc_identity(md, key);
    if (identity == NULL) {
        goto fail;
    }

    Py_hash_t hash = _unicode_hash(identity);
    if (hash == -1) {
        goto fail;
    }

    htkeysiter_t iter;
    htkeysiter_init(&iter, md->keys, hash);
    entry_t *entries = htkeys_entries(md->keys);

    for (; iter.index != DKIX_EMPTY; htkeysiter_next(&iter)) {
        if (iter.index < 0) {
            continue;
        }
        entry_t *entry = entries + iter.index;

        if (hash != entry->hash) {
            continue;
        }
        int tmp = _str_cmp(identity, entry->identity);
        if (tmp > 0) {
            Py_DECREF(identity);
            ASSERT_CONSISTENT(md, false);
            *result = Py_NewRef(entry->value);
            return 1;
        } else if (tmp < 0) {
            goto fail;
        }
    }

    if (_md_add_with_hash(md, hash, identity, key, value) < 0) {
        goto fail;
    }

    Py_DECREF(identity);
    ASSERT_CONSISTENT(md, false);
    *result = Py_NewRef(value);
    return 0;
fail:
    Py_XDECREF(identity);
    return -1;
}

static inline int
md_pop_one(MultiDictObject *md, PyObject *key, PyObject **ret)
{
    PyObject *value = NULL;

    PyObject *identity = md_calc_identity(md, key);
    if (identity == NULL) {
        goto fail;
    }

    Py_hash_t hash = _unicode_hash(identity);
    if (hash == -1) {
        goto fail;
    }

    htkeysiter_t iter;
    htkeysiter_init(&iter, md->keys, hash);
    entry_t *entries = htkeys_entries(md->keys);

    for (; iter.index != DKIX_EMPTY; htkeysiter_next(&iter)) {
        if (iter.index < 0) {
            continue;
        }
        entry_t *entry = entries + iter.index;

        if (hash != entry->hash) {
            continue;
        }
        int tmp = _str_cmp(identity, entry->identity);
        if (tmp > 0) {
            value = Py_NewRef(entry->value);
            if (_md_del_at(md, iter.slot, entry) < 0) {
                goto fail;
            }
            Py_DECREF(identity);
            *ret = value;
            md->version = NEXT_VERSION(md->state);
            ASSERT_CONSISTENT(md, false);
            return 1;
        } else if (tmp < 0) {
            goto fail;
        }
    }

    ASSERT_CONSISTENT(md, false);
    return 0;
fail:
    Py_XDECREF(value);
    Py_XDECREF(identity);
    return -1;
}

static inline int
md_pop_all(MultiDictObject *md, PyObject *key, PyObject **ret)
{
    PyObject *lst = NULL;

    PyObject *identity = md_calc_identity(md, key);
    if (identity == NULL) {
        goto fail;
    }

    Py_hash_t hash = _unicode_hash(identity);
    if (hash == -1) {
        goto fail;
    }

    if (md_len(md) == 0) {
        Py_DECREF(identity);
        return 0;
    }

    htkeysiter_t iter;
    htkeysiter_init(&iter, md->keys, hash);
    entry_t *entries = htkeys_entries(md->keys);

    for (; iter.index != DKIX_EMPTY; htkeysiter_next(&iter)) {
        if (iter.index < 0) {
            continue;
        }
        entry_t *entry = entries + iter.index;

        if (hash != entry->hash) {
            continue;
        }
        int tmp = _str_cmp(identity, entry->identity);
        if (tmp > 0) {
            if (lst == NULL) {
                lst = PyList_New(1);
                if (lst == NULL) {
                    goto fail;
                }
                if (PyList_SetItem(lst, 0, Py_NewRef(entry->value)) < 0) {
                    goto fail;
                }
            } else if (PyList_Append(lst, entry->value) < 0) {
                goto fail;
            }
            if (_md_del_at(md, iter.slot, entry) < 0) {
                goto fail;
            }
            md->version = NEXT_VERSION(md->state);
        } else if (tmp < 0) {
            goto fail;
        }
    }

    *ret = lst;
    Py_DECREF(identity);
    ASSERT_CONSISTENT(md, false);
    return lst != NULL;
fail:
    Py_XDECREF(identity);
    Py_XDECREF(lst);
    return -1;
}

static inline PyObject *
md_pop_item(MultiDictObject *md)
{
    if (md->used == 0) {
        PyErr_SetString(PyExc_KeyError, "empty multidict");
        return NULL;
    }

    entry_t *entries = htkeys_entries(md->keys);

    Py_ssize_t pos = md->keys->nentries - 1;
    entry_t *entry = entries + pos;
    while (pos >= 0 && entry->identity == NULL) {
        pos--;
        entry--;
    }
    assert(pos >= 0);

    PyObject *key = md_calc_key(md, entry->key, entry->identity);
    if (key == NULL) {
        return NULL;
    }
    PyObject *ret = PyTuple_Pack(2, key, entry->value);
    Py_CLEAR(key);
    if (ret == NULL) {
        return NULL;
    }

    htkeysiter_t iter;
    htkeysiter_init(&iter, md->keys, entry->hash);

    for (; iter.index != pos; htkeysiter_next(&iter)) {
    }
    if (_md_del_at(md, iter.slot, entry) < 0) {
        return NULL;
    }
    md->version = NEXT_VERSION(md->state);
    ASSERT_CONSISTENT(md, false);
    return ret;
}

static inline int
_md_replace(MultiDictObject *md, PyObject *key, PyObject *value,
            PyObject *identity, Py_hash_t hash)
{
    int found = 0;
    md_finder_t finder = {0};
    if (md_init_finder(md, identity, &finder) < 0) {
        assert(PyErr_Occurred());
        goto fail;
    }
    entry_t *entries = htkeys_entries(md->keys);

    int tmp;

    // don't grab neither key nor value but use the calculated index
    while ((tmp = md_find_next(&finder, NULL, NULL)) > 0) {
        entry_t *entry = entries + md_finder_index(&finder);
        if (!found) {
            found = 1;
            Py_SETREF(entry->key, Py_NewRef(key));
            Py_SETREF(entry->value, Py_NewRef(value));
            entry->hash = -1;
        } else {
            if (_md_del_at(md, md_finder_slot(&finder), entry) < 0) {
                goto fail;
            }
        }
    }
    if (tmp < 0) {
        goto fail;
    }

    md_finder_cleanup(&finder);
    if (!found) {
        if (_md_add_with_hash(md, hash, identity, key, value) < 0) {
            goto fail;
        }
        return 0;
    } else {
        md->version = NEXT_VERSION(md->state);
        return 0;
    }
fail:
    md_finder_cleanup(&finder);
    return -1;
}

static inline int
md_replace(MultiDictObject *md, PyObject *key, PyObject *value)
{
    PyObject *identity = md_calc_identity(md, key);
    if (identity == NULL) {
        goto fail;
    }

    Py_hash_t hash = _unicode_hash(identity);
    if (hash == -1) {
        goto fail;
    }

    int ret = _md_replace(md, key, value, identity, hash);
    Py_DECREF(identity);
    ASSERT_CONSISTENT(md, false);
    return ret;
fail:
    Py_XDECREF(identity);
    return -1;
}

static inline int
_md_update(MultiDictObject *md, Py_hash_t hash, PyObject *identity,
           PyObject *key, PyObject *value)
{
    htkeysiter_t iter;
    htkeysiter_init(&iter, md->keys, hash);
    entry_t *entries = htkeys_entries(md->keys);
    bool found = false;

    for (; iter.index != DKIX_EMPTY; htkeysiter_next(&iter)) {
        if (iter.index < 0) {
            continue;
        }
        entry_t *entry = entries + iter.index;
        if (hash != entry->hash) {
            continue;
        }
        int tmp = _str_cmp(identity, entry->identity);
        if (tmp > 0) {
            if (!found) {
                found = true;
                if (entry->key == NULL) {
                    /* entry->key could be NULL if it was deleted
                       by the previous _md_update call during the iteration
                       in md_update_from* functions. */
                    assert(entry->value == NULL);
                    entry->key = Py_NewRef(key);
                    entry->value = Py_NewRef(value);
                } else {
                    Py_SETREF(entry->key, Py_NewRef(key));
                    Py_SETREF(entry->value, Py_NewRef(value));
                }
                entry->hash = -1;
            } else {
                if (_md_del_at_for_upd(md, iter.slot, entry) < 0) {
                    goto fail;
                }
            }
        } else if (tmp < 0) {
            goto fail;
        }
    }

    if (!found) {
        if (_md_add_for_upd(md, hash, identity, key, value) < 0) {
            goto fail;
        }
    }
    return 0;
fail:
    return -1;
}

static inline int
_md_merge(MultiDictObject *md, Py_hash_t hash, PyObject *identity,
          PyObject *key, PyObject *value)
{
    htkeysiter_t iter;
    htkeysiter_init(&iter, md->keys, hash);
    entry_t *entries = htkeys_entries(md->keys);

    for (; iter.index != DKIX_EMPTY; htkeysiter_next(&iter)) {
        if (iter.index < 0) {
            continue;
        }
        entry_t *entry = entries + iter.index;
        if (hash != entry->hash) {
            continue;
        }
        int tmp = _str_cmp(identity, entry->identity);
        if (tmp > 0) {
            return 0;
        } else if (tmp < 0) {
            goto fail;
        }
    }

    if (_md_add_for_upd(md, hash, identity, key, value) < 0) {
        goto fail;
    }
    return 0;
fail:
    return -1;
}

static inline void
md_post_update(MultiDictObject *md)
{
    htkeys_t *keys = md->keys;
    size_t num_slots = htkeys_nslots(keys);
    entry_t *entries = htkeys_entries(keys);
    for (size_t slot = 0; slot < num_slots; slot++) {
        Py_ssize_t index = htkeys_get_index(keys, slot);
        if (index >= 0) {
            entry_t *entry = entries + index;
            if (entry->key == NULL) {
                /* the entry is marked for deletion during .update() call
                   and not replaced with a new value */
                Py_CLEAR(entry->identity);
                htkeys_set_index(keys, slot, DKIX_DUMMY);
                md->used -= 1;
            }
            if (entry->hash == -1) {
                entry->hash = _unicode_hash(entry->identity);
            }
            assert(entry->hash != -1);
        }
    }
    ASSERT_CONSISTENT(md, false);
}

static inline int
md_update_from_ht(MultiDictObject *md, MultiDictObject *other, UpdateOp op)
{
    Py_ssize_t pos;
    Py_hash_t hash;
    PyObject *identity = NULL;
    PyObject *key = NULL;
    bool recalc_identity = md->is_ci != other->is_ci;

    if (other->used == 0) {
        return 0;
    }

    entry_t *entries = htkeys_entries(other->keys);

    for (pos = 0; pos < other->keys->nentries; pos++) {
        entry_t *entry = entries + pos;
        if (entry->identity == NULL) {
            continue;
        }
        if (recalc_identity) {
            identity = md_calc_identity(md, entry->key);
            if (identity == NULL) {
                goto fail;
            }
            hash = _unicode_hash(identity);
            if (hash == -1) {
                goto fail;
            }
            /* materialize key */
            key = md_calc_key(other, entry->key, identity);
            if (key == NULL) {
                goto fail;
            }
        } else {
            identity = entry->identity;
            hash = entry->hash;
            key = entry->key;
        }
        switch (op) {
            case Update:
                if (_md_update(md, hash, identity, key, entry->value) < 0) {
                    goto fail;
                }
                break;
            case Extend:
                if (_md_add_with_hash(md, hash, identity, key, entry->value) <
                    0) {
                    goto fail;
                }
                break;
            case Merge:
                if (_md_merge(md, hash, identity, key, entry->value) < 0) {
                    goto fail;
                }
                break;
        }
        if (recalc_identity) {
            Py_CLEAR(identity);
            Py_CLEAR(key);
        }
    }
    return 0;
fail:
    if (recalc_identity) {
        Py_CLEAR(identity);
        Py_CLEAR(key);
    }
    return -1;
}

static inline int
md_update_from_dict(MultiDictObject *md, PyObject *kwds, UpdateOp op)
{
    Py_ssize_t pos = 0;
    PyObject *identity = NULL;
    PyObject *key = NULL;
    PyObject *value = NULL;

    assert(PyDict_CheckExact(kwds));

    // PyDict_Next returns borrowed refs
    while (PyDict_Next(kwds, &pos, &key, &value)) {
        Py_INCREF(key);
        identity = md_calc_identity(md, key);
        if (identity == NULL) {
            goto fail;
        }
        Py_hash_t hash = _unicode_hash(identity);
        if (hash == -1) {
            goto fail;
        }
        switch (op) {
            case Update: {
                if (_md_update(md, hash, identity, key, value) < 0) {
                    goto fail;
                }
                Py_CLEAR(identity);
                Py_CLEAR(key);
                break;
            }
            case Extend: {
                int tmp = _md_add_with_hash_steal_refs(
                    md, hash, identity, key, Py_NewRef(value));
                if (tmp < 0) {
                    Py_DECREF(value);
                    goto fail;
                }

                identity = NULL;
                key = NULL;
                value = NULL;
                break;
            }
            case Merge: {
                if (_md_merge(md, hash, identity, key, value) < 0) {
                    goto fail;
                }
                Py_CLEAR(identity);
                Py_CLEAR(key);
                break;
            }
        }
    }
    return 0;
fail:
    Py_CLEAR(identity);
    Py_CLEAR(key);
    return -1;
}

static inline void
_err_not_sequence(Py_ssize_t i)
{
    PyErr_Format(PyExc_TypeError,
                 "multidict cannot convert sequence element #%zd"
                 " to a sequence",
                 i);
}

static inline void
_err_bad_length(Py_ssize_t i, Py_ssize_t n)
{
    PyErr_Format(PyExc_ValueError,
                 "multidict update sequence element #%zd "
                 "has length %zd; 2 is required",
                 i,
                 n);
}

static inline void
_err_cannot_fetch(Py_ssize_t i, const char *name)
{
    PyErr_Format(PyExc_ValueError,
                 "multidict update sequence element #%zd's "
                 "%s could not be fetched",
                 name,
                 i);
}

static int
_md_parse_item(Py_ssize_t i, PyObject *item, PyObject **pkey,
               PyObject **pvalue)
{
    Py_ssize_t n;

    if (PyTuple_CheckExact(item)) {
        n = PyTuple_GET_SIZE(item);
        if (n != 2) {
            _err_bad_length(i, n);
            goto fail;
        }
        *pkey = Py_NewRef(PyTuple_GET_ITEM(item, 0));
        *pvalue = Py_NewRef(PyTuple_GET_ITEM(item, 1));
    } else if (PyList_CheckExact(item)) {
        n = PyList_GET_SIZE(item);
        if (n != 2) {
            _err_bad_length(i, n);
            goto fail;
        }
        *pkey = Py_NewRef(PyList_GET_ITEM(item, 0));
        *pvalue = Py_NewRef(PyList_GET_ITEM(item, 1));
    } else {
        if (!PySequence_Check(item)) {
            _err_not_sequence(i);
            goto fail;
        }
        n = PySequence_Size(item);
        if (n != 2) {
            _err_bad_length(i, n);
            goto fail;
        }
        *pkey = PySequence_ITEM(item, 0);
        *pvalue = PySequence_ITEM(item, 1);
        if (*pkey == NULL) {
            _err_cannot_fetch(i, "key");
            goto fail;
        }
        if (*pvalue == NULL) {
            _err_cannot_fetch(i, "value");
            goto fail;
        }
    }
    return 0;
fail:
    Py_CLEAR(*pkey);
    Py_CLEAR(*pvalue);
    return -1;
}

static inline int
md_update_from_seq(MultiDictObject *md, PyObject *seq, UpdateOp op)
{
    PyObject *it = NULL;
    PyObject *item = NULL;  // seq[i]

    PyObject *key = NULL;
    PyObject *value = NULL;
    PyObject *identity = NULL;

    Py_ssize_t i;
    Py_ssize_t size = -1;

    enum { LIST, TUPLE, ITER } kind;

    if (PyList_CheckExact(seq)) {
        kind = LIST;
        size = PyList_GET_SIZE(seq);
        if (size == 0) {
            return 0;
        }
    } else if (PyTuple_CheckExact(seq)) {
        kind = TUPLE;
        size = PyTuple_GET_SIZE(seq);
        if (size == 0) {
            return 0;
        }
    } else {
        kind = ITER;
        it = PyObject_GetIter(seq);
        if (it == NULL) {
            goto fail;
        }
    }

    for (i = 0;; ++i) {  // i - index into seq of current element
        switch (kind) {
            case LIST:
                if (i >= size) {
                    goto exit;
                }
                item = PyList_GET_ITEM(seq, i);
                if (item == NULL) {
                    goto fail;
                }
                Py_INCREF(item);
                break;
            case TUPLE:
                if (i >= size) {
                    goto exit;
                }
                item = PyTuple_GET_ITEM(seq, i);
                if (item == NULL) {
                    goto fail;
                }
                Py_INCREF(item);
                break;
            case ITER:
                item = PyIter_Next(it);
                if (item == NULL) {
                    if (PyErr_Occurred()) {
                        goto fail;
                    }
                    goto exit;
                }
        }

        if (_md_parse_item(i, item, &key, &value) < 0) {
            goto fail;
        }

        identity = md_calc_identity(md, key);
        if (identity == NULL) {
            goto fail;
        }

        Py_hash_t hash = _unicode_hash(identity);
        if (hash == -1) {
            goto fail;
        }

        switch (op) {
            case Update:
                if (_md_update(md, hash, identity, key, value) < 0) {
                    goto fail;
                }
                Py_CLEAR(identity);
                Py_CLEAR(key);
                Py_CLEAR(value);
                break;
            case Extend:
                if (_md_add_with_hash_steal_refs(
                        md, hash, identity, key, value) < 0) {
                    goto fail;
                }
                identity = NULL;
                key = NULL;
                value = NULL;
                break;
            case Merge:
                if (_md_merge(md, hash, identity, key, value) < 0) {
                    goto fail;
                }
                Py_CLEAR(identity);
                Py_CLEAR(key);
                Py_CLEAR(value);
                break;
        }
        Py_CLEAR(item);
    }

exit:
    Py_CLEAR(it);
    return 0;

fail:
    Py_CLEAR(identity);
    Py_CLEAR(it);
    Py_CLEAR(item);
    Py_CLEAR(key);
    Py_CLEAR(value);
    return -1;
}

static inline int
md_eq(MultiDictObject *md, MultiDictObject *other)
{
    if (md == other) {
        return 1;
    }

    if (md_len(md) != md_len(other)) {
        return 0;
    }

    Py_ssize_t pos1 = 0;
    Py_ssize_t pos2 = 0;

    entry_t *lft_entries = htkeys_entries(md->keys);
    entry_t *rht_entries = htkeys_entries(other->keys);
    for (;;) {
        if (pos1 >= md->keys->nentries || pos2 >= other->keys->nentries) {
            return 1;
        }
        entry_t *entry1 = lft_entries + pos1;
        if (entry1->identity == NULL) {
            pos1++;
            continue;
        }
        entry_t *entry2 = rht_entries + pos2;
        if (entry2->identity == NULL) {
            pos2++;
            continue;
        }

        if (entry1->hash != entry2->hash) {
            return 0;
        }

        int cmp = _str_cmp(entry1->identity, entry2->identity);
        if (cmp < 0) {
            return -1;
        };
        if (cmp == 0) {
            return 0;
        }

        cmp = PyObject_RichCompareBool(entry1->value, entry2->value, Py_EQ);
        if (cmp < 0) {
            return -1;
        };
        if (cmp == 0) {
            return 0;
        }
        pos1++;
        pos2++;
    }
    return 1;
}

static inline int
md_eq_to_mapping(MultiDictObject *md, PyObject *other)
{
    PyObject *key = NULL;
    PyObject *avalue = NULL;
    PyObject *bvalue;

    Py_ssize_t other_len;

    if (!PyMapping_Check(other)) {
        PyErr_Format(PyExc_TypeError,
                     "other argument must be a mapping, not %s",
                     Py_TYPE(other)->tp_name);
        return -1;
    }

    other_len = PyMapping_Size(other);
    if (other_len < 0) {
        return -1;
    }
    if (md_len(md) != other_len) {
        return 0;
    }

    md_pos_t pos;
    md_init_pos(md, &pos);

    for (;;) {
        int ret = md_next(md, &pos, NULL, &key, &avalue);
        if (ret < 0) {
            return -1;
        }
        if (ret == 0) {
            break;
        }
        ret = PyMapping_GetOptionalItem(other, key, &bvalue);
        Py_CLEAR(key);
        if (ret < 0) {
            Py_CLEAR(avalue);
            return -1;
        }

        if (bvalue == NULL) {
            Py_CLEAR(avalue);
            return 0;
        }

        int eq = PyObject_RichCompareBool(avalue, bvalue, Py_EQ);
        Py_CLEAR(bvalue);
        Py_CLEAR(avalue);

        if (eq <= 0) {
            return eq;
        }
    }

    return 1;
}

static inline PyObject *
md_repr(MultiDictObject *md, PyObject *name, bool show_keys, bool show_values)
{
    PyObject *key = NULL;
    PyObject *value = NULL;

    bool comma = false;
    uint64_t version = md->version;

    PyUnicodeWriter *writer = PyUnicodeWriter_Create(1024);
    if (writer == NULL) return NULL;

    if (PyUnicodeWriter_WriteChar(writer, '<') < 0) {
        goto fail;
    }
    if (PyUnicodeWriter_WriteStr(writer, name) < 0) {
        goto fail;
    }
    if (PyUnicodeWriter_WriteChar(writer, '(') < 0) {
        goto fail;
    }

    entry_t *entries = htkeys_entries(md->keys);

    for (Py_ssize_t pos = 0; pos < md->keys->nentries; ++pos) {
        if (version != md->version) {
            PyErr_SetString(PyExc_RuntimeError,
                            "MultiDict changed during iteration");
            return NULL;
        }
        entry_t *entry = entries + pos;
        if (entry->identity == NULL) {
            continue;
        }
        key = Py_NewRef(entry->key);
        value = Py_NewRef(entry->value);

        if (comma) {
            if (PyUnicodeWriter_WriteChar(writer, ',') < 0) {
                goto fail;
            }
            if (PyUnicodeWriter_WriteChar(writer, ' ') < 0) {
                goto fail;
            }
        }
        if (show_keys) {
            if (PyUnicodeWriter_WriteChar(writer, '\'') < 0) {
                goto fail;
            }
            /* Don't need to convert key to istr, the text is the same*/
            if (PyUnicodeWriter_WriteStr(writer, key) < 0) {
                goto fail;
            }
            if (PyUnicodeWriter_WriteChar(writer, '\'') < 0) {
                goto fail;
            }
        }
        if (show_keys && show_values) {
            if (PyUnicodeWriter_WriteChar(writer, ':') < 0) {
                goto fail;
            }
            if (PyUnicodeWriter_WriteChar(writer, ' ') < 0) {
                goto fail;
            }
        }
        if (show_values) {
            if (PyUnicodeWriter_WriteRepr(writer, value) < 0) {
                goto fail;
            }
        }

        comma = true;
        Py_CLEAR(key);
        Py_CLEAR(value);
    }

    if (PyUnicodeWriter_WriteChar(writer, ')') < 0) {
        goto fail;
    }
    if (PyUnicodeWriter_WriteChar(writer, '>') < 0) {
        goto fail;
    }
    return PyUnicodeWriter_Finish(writer);
fail:
    Py_CLEAR(key);
    Py_CLEAR(value);
    PyUnicodeWriter_Discard(writer);
    return NULL;
}

/***********************************************************************/

static inline int
md_traverse(MultiDictObject *md, visitproc visit, void *arg)
{
    if (md->used == 0) {
        return 0;
    }

    entry_t *entries = htkeys_entries(md->keys);
    for (Py_ssize_t pos = 0; pos < md->keys->nentries; pos++) {
        entry_t *entry = entries + pos;
        if (entry->identity != NULL) {
            Py_VISIT(entry->key);
            Py_VISIT(entry->value);
        }
    }

    return 0;
}

static inline int
md_clear(MultiDictObject *md)
{
    if (md->keys == NULL || md->keys == &empty_htkeys) {
        return 0;
    }
    md->version = NEXT_VERSION(md->state);

    entry_t *entries = htkeys_entries(md->keys);
    for (Py_ssize_t pos = 0; pos < md->keys->nentries; pos++) {
        entry_t *entry = entries + pos;
        if (entry->identity != NULL) {
            Py_CLEAR(entry->identity);
            Py_CLEAR(entry->key);
            Py_CLEAR(entry->value);
        }
    }

    md->used = 0;
    if (md->keys != &empty_htkeys) {
        htkeys_free(md->keys);
        md->keys = &empty_htkeys;
    }
    ASSERT_CONSISTENT(md, false);
    return 0;
}

#ifndef NDEBUG

static inline int
_md_check_consistency(MultiDictObject *md, bool update)
{
    //    ASSERT_WORLD_STOPPED_OR_DICT_LOCKED(op);

#define CHECK(expr) assert(expr)
    //    do { if (!(expr)) { assert(0 && Py_STRINGIFY(expr)); } } while (0)

    htkeys_t *keys = md->keys;
    CHECK(keys != NULL);
    Py_ssize_t calc_usable = USABLE_FRACTION(htkeys_nslots(keys));

    // In the free-threaded build, shared keys may be concurrently modified,
    // so use atomic loads.
    Py_ssize_t usable = keys->usable;
    Py_ssize_t nentries = keys->nentries;

    CHECK(0 <= md->used && md->used <= calc_usable);
    CHECK(0 <= usable && usable <= calc_usable);
    CHECK(0 <= nentries && nentries <= calc_usable);
    CHECK(usable + nentries <= calc_usable);

    for (Py_ssize_t i = 0; i < htkeys_nslots(keys); i++) {
        Py_ssize_t ix = htkeys_get_index(keys, i);
        CHECK(DKIX_DUMMY <= ix && ix <= calc_usable);
    }

    entry_t *entries = htkeys_entries(keys);
    for (Py_ssize_t i = 0; i < calc_usable; i++) {
        entry_t *entry = &entries[i];
        PyObject *identity = entry->identity;

        if (identity != NULL) {
            if (!update) {
                CHECK(entry->hash != -1);
                CHECK(entry->key != NULL);
                CHECK(entry->value != NULL);
            } else {
                if (entry->key == NULL) {
                    CHECK(entry->value == NULL);
                } else {
                    CHECK(entry->value != NULL);
                }
            }

            CHECK(PyUnicode_CheckExact(identity));
            if (entry->hash != -1) {
                Py_hash_t hash = _unicode_hash(identity);
                CHECK(entry->hash == hash);
            }
        }
    }
    return 1;

#undef CHECK
}

static inline int
_md_dump(MultiDictObject *md)
{
    htkeys_t *keys = md->keys;
    printf("Dump %p [%zd from %zd usable %zd nentries %zd]\n",
           (void *)md,
           md->used,
           htkeys_nslots(keys),
           keys->usable,
           keys->nentries);
    for (Py_ssize_t i = 0; i < htkeys_nslots(keys); i++) {
        Py_ssize_t ix = htkeys_get_index(keys, i);
        printf("  %zd -> %zd\n", i, ix);
    }
    printf("  --------\n");
    entry_t *entries = htkeys_entries(keys);
    for (Py_ssize_t i = 0; i < keys->nentries; i++) {
        entry_t *entry = &entries[i];
        PyObject *identity = entry->identity;

        if (identity == NULL) {
            printf("  %zd [deleted]\n", i);
        } else {
            printf("  %zd h=%20zd, i=\'", i, entry->hash);
            PyObject_Print(entry->identity, stdout, Py_PRINT_RAW);
            printf("\', k=\'");
            PyObject_Print(entry->key, stdout, Py_PRINT_RAW);
            printf("\', v=\'");
            PyObject_Print(entry->value, stdout, Py_PRINT_RAW);
            printf("\'\n");
        }
    }
    printf("\n");
    return 1;
}
#endif  // NDEBUG

#ifdef __cplusplus
}
#endif
#endif
