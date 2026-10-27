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

#include "atomic_helpers.h"
#include "dict.h"
#include "htkeys.h"
#include "istr.h"
#include "state.h"

typedef struct _md_pos {
    Py_ssize_t pos;
    uint64_t version;
} md_pos_t;

typedef struct _md_finder {
    MultiDictObject* md;
    htkeysiter_t iter;
    uint64_t version;
    Py_hash_t hash;
    PyObject* identity;  // borrowed ref
} md_finder_t;

typedef enum _UpdateOp {
    Extend,
    Update,
    Merge,
} UpdateOp;

#define MD_HASH_MARK PY_SSIZE_T_MIN

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
eliminate duplicates, the code marks already visited entries. Entry hashes are
folded non-negative (_unicode_hash() masks with PY_SSIZE_T_MAX), so
MD_HASH_MARK, the hash range's high bit, can mark a hash as temporarily
invalid: OR it in, AND it out with PY_SSIZE_T_MAX to restore. A real folded
hash never has that bit set, so a marked entry is simply one whose hash is
negative. After the iteration finishes, all marked entries are restored. Double
iteration over the indices still has O(1) amortized time, it is ok.

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
GROWTH_RATE(MultiDictObject* md)
{
    return md->used * 3;
}

#if 0
static inline int
_md_check_consistency(MultiDictObject* md, bool update);
static inline int
_md_dump(MultiDictObject* md);

#define ASSERT_CONSISTENT(md, update) assert(_md_check_consistency(md, update))
#else
#define ASSERT_CONSISTENT(md, update) assert(1)
#endif

static inline bool
_str_cmp(PyObject* s1, PyObject* s2)
{
    /* implementation is borrowed from PyUnicode_Equal() but without
       type checks, arguments are identities that are always strings */
    assert(PyUnicode_Check(s1));
    assert(PyUnicode_Check(s2));

    if (s1 == s2) {
        return true;
    }
    Py_ssize_t len = PyUnicode_GET_LENGTH(s1);
    if (PyUnicode_GET_LENGTH(s2) != len) {
        return false;
    }

    int kind = PyUnicode_KIND(s1);
    if (PyUnicode_KIND(s2) != kind) {
        return false;
    }

    const void* data1 = PyUnicode_DATA(s1);
    const void* data2 = PyUnicode_DATA(s2);
    return (memcmp(data1, data2, len * kind) == 0);
}

static inline PyObject*
_key_to_identity(mod_state* state, PyObject* key)
{
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

static inline PyObject*
_ci_key_to_identity(mod_state* state, PyObject* key)
{
    if (IStr_Check(state, key)) {
        return Py_NewRef(((istrobject*)key)->canonical);
    }
    if (PyUnicode_Check(key)) {
        PyObject* ret = PyObject_CallMethodNoArgs(key, state->str_lower);
        if (ret == NULL) {
            goto fail;
        }
        if (!PyUnicode_CheckExact(ret)) {
            PyObject* tmp = PyUnicode_FromObject(ret);
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

static inline PyObject*
_arg_to_key(mod_state* state, PyObject* key, PyObject* identity)
{
    if (PyUnicode_Check(key)) {
        return Py_NewRef(key);
    }
    PyErr_SetString(PyExc_TypeError,
                    "MultiDict keys should be either str "
                    "or subclasses of str");
    return NULL;
}

static inline PyObject*
_ci_arg_to_key(mod_state* state, PyObject* key, PyObject* identity)
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

#ifdef Py_GIL_DISABLED

/*
Lock-free read support.

md->num_active_readers is a coarse "some lock-free reader is in flight on
this object" gate: incremented before a reader ever dereferences a
keys table, decremented once it's done. A table is only ever freed
once this reads 0 at a point synchronized (seq_cst on both sides, see
atomic_helpers.h) with the swap that retired it. That ordering is the
safety argument in full:

  reader:  num_active_readers += 1        (A, seq_cst)
           keys = load(md->keys)      (B, seq_cst)
           keys->num_readers += 1         (C, relaxed; skipped for
                                        &empty_htkeys, which is never
                                        retired or freed)
           ... walk keys ...
           keys->num_readers -= 1         (D, relaxed)
           num_active_readers -= 1        (E, seq_cst)

  writer:  store(md->keys, new)       (seq_cst)
           if load(num_active_readers) == 0 (seq_cst): free old immediately
           else: move old onto md->retired, freed by a later drain

A is always sequenced-before B on the reader's own thread, so if the
writer's check observes num_active_readers == 0, no reader can be
*starting* a walk of any table that was retired before that check --
none can be caught between A and B for the table being freed. Anything
weaker than seq_cst here (plain acquire/release, or relaxed) is not
enough: the writer's store to md->keys and its read of num_active_readers,
versus the reader's write to num_active_readers and its read of
md->keys, is a criss-cross on two independent atomics (the same shape
as Dekker's algorithm), and only a single global seq_cst order over all
four operations closes it -- see the design discussion that produced
this file for the specific interleaving that a weaker order permits.

That guarantee is coarser than it looks, though: num_active_readers
reaching zero does not mean every reader that incremented it has also
reached its own D/E -- a reader can be preempted between C and D for
an arbitrary stretch. So keys->num_readers (C/D) is the actual
per-table authority on whether a specific table is safe to free, not
a redundant check of what the coarse gate already guarantees.
_md_drain_retired() treats it that way: a table whose own num_readers
is still nonzero is pushed back onto md->retired for a later attempt
instead of freed.

_md_reader_exit()'s own num_active_readers decrement (E above) is a
seq_cst atomic_fetch_add_ssize(), which hands back the pre-decrement
count for free. When that count was 1, this reader's decrement is the
one that brings the global gate to 0, at the exact same linearization
point a writer's atomic_load_ssize(&md->num_active_readers) == 0 check
would observe. The safety argument above never distinguished who
performs that check; it only depends on num_active_readers reaching 0
under seq_cst. So the reader may drain md->retired right there instead
of leaving every table on it stranded until some future writer happens
to retire another one and observe the same zero (see md->retired below
for why a concurrent drain from this path is safe against a writer
retiring into the same list at the same time).

md->retired is a lock-free stack (Treiber-style), not a plain
writer-owned list: with readers now able to drain it too, pushes
(_md_retire()) and pop-alls (_md_drain_retired()) can run concurrently
with each other, on different threads, with no lock in common. A
pop-all is a single atomic_exchange_ptr() that swaps the whole chain
out for NULL and hands the caller sole ownership of whatever it
returns; any push racing that exchange either lands before it (and
gets swept up in the same pop) or after it (and starts a fresh chain
from NULL), never in between, because there is no "in between" for a
single atomic RMW. A push cannot use that same trick: it has to link
the new node's ->retired_next to the current head before publishing
the node, and if it read that head with a plain load, a pop-all could
slip in after the load and free the very chain the push is about to
link to, publishing a node whose ->retired_next dangles. The
atomic_compare_exchange_ptr() loop in _md_retire() closes that window
instead of merely narrowing it: the head is only published once the
CAS confirms nothing changed it since the read that fed
->retired_next, and if something did (a pop-all ran, or another
push), the loop rereads the new head and relinks before retrying, so
->retired_next is never stale at the moment the node actually becomes
visible.
*/

/* Every write to md->keys that a lock-free reader could observe must
   use this, matching _md_reader_enter()'s atomic_load_ptr(): mixing a
   plain store here with an atomic load there is a data race regardless
   of what the surrounding critical section or num_active_readers protocol
   otherwise guarantees, and on architectures weaker than x86 a plain
   store carries no ordering guarantee at all relative to the
   num_active_readers check the retiring code depends on. */
static inline void
_md_store_keys(MultiDictObject* md, htkeys_t* keys)
{
    atomic_store_ptr((void**)&md->keys, keys);
}

/* md_len() reads md->used lock-free via atomic_load_ssize_relaxed(); every
   write to it needs the matching relaxed atomic op for the same reason
   _md_store_keys() exists above. */
static inline void
_md_store_used(MultiDictObject* md, Py_ssize_t used)
{
    atomic_store_ssize_relaxed(&md->used, used);
}

static inline void
_md_add_used(MultiDictObject* md, Py_ssize_t delta)
{
    atomic_fetch_add_ssize_relaxed(&md->used, delta);
}

static inline htkeys_t*
_md_reader_enter(MultiDictObject* md)
{
    atomic_fetch_add_ssize(&md->num_active_readers, 1);
    htkeys_t* keys = (htkeys_t*)atomic_load_ptr((void* const*)&md->keys);
    if (keys != &empty_htkeys) {
        atomic_fetch_add_ssize_relaxed(&keys->num_readers, 1);
    }
    return keys;
}

static inline void
_md_drain_retired(MultiDictObject* md);

static inline void
_md_reader_exit(MultiDictObject* md, htkeys_t* keys)
{
    if (keys != &empty_htkeys) {
        atomic_fetch_add_ssize_relaxed(&keys->num_readers, -1);
    }
    Py_ssize_t prev_active_readers =
        atomic_fetch_add_ssize(&md->num_active_readers, -1);
    if (prev_active_readers == 1) {
        _md_drain_retired(md);
    }
}

static inline void
_md_free_retired(htkeys_t* keys)
{
    entry_t* entries = htkeys_entries(keys);
    /* Only md_clear()'s retired tables have live entries to release here:
   _md_resize()'s old table has its ownership already transferred to
   the new table via memcpy, so its nentries is reset to 0 before
   retirement, making this loop a no-op for that case. */
    for (Py_ssize_t i = 0; i < keys->nentries; i++) {
        Py_CLEAR(entries[i].identity);
        Py_CLEAR(entries[i].key);
        Py_CLEAR(entries[i].value);
    }
    htkeys_free(keys);
}

static inline void
_md_drain_retired(MultiDictObject* md)
{
    if (atomic_load_ssize(&md->num_active_readers) != 0) {
        return;
    }
    htkeys_t* t = (htkeys_t*)atomic_exchange_ptr((void**)&md->retired, NULL);

    /* The coarse gate above can read zero while a specific table's own
       num_readers is still nonzero -- a reader can be preempted between
       incrementing it and decrementing it. A table is only actually
       safe to free once its own count is zero, so anything still
       nonzero goes back onto md->retired for a later attempt instead of
       being freed here. */
    htkeys_t* pending_head = NULL;
    htkeys_t* pending_tail = NULL;
    while (t != NULL) {
        htkeys_t* next = t->retired_next;
        if (atomic_load_ssize_relaxed(&t->num_readers) == 0) {
            _md_free_retired(t);
        } else {
            t->retired_next = pending_head;
            pending_head = t;
            if (pending_tail == NULL) {
                pending_tail = t;
            }
        }
        t = next;
    }

    if (pending_head != NULL) {
        htkeys_t* old_head =
            (htkeys_t*)atomic_load_ptr((void* const*)&md->retired);
        for (;;) {
            pending_tail->retired_next = old_head;
            if (atomic_compare_exchange_ptr(
                    (void**)&md->retired, (void**)&old_head, pending_head)) {
                break;
            }
        }
    }
}

static inline void
_md_retire(MultiDictObject* md, htkeys_t* keys)
{
    if (keys == &empty_htkeys) {
        return;
    }
    _md_drain_retired(md);

    htkeys_t* old_head =
        (htkeys_t*)atomic_load_ptr((void* const*)&md->retired);
    for (;;) {
        keys->retired_next = old_head;
        if (atomic_compare_exchange_ptr(
                (void**)&md->retired, (void**)&old_head, keys)) {
            break;
        }
    }
    _md_drain_retired(md);
}

#if PY_VERSION_HEX >= 0x030e0000
#define _MD_HAVE_TRYINCREF 1
#else
#define _MD_HAVE_TRYINCREF 0
#endif

/*
Lock-free-safe access to individual entry fields.

The table-retirement scheme above only protects an htkeys_t blob's own
memory. It says nothing about a single entry's identity/value fields
*within* a table that is still md->keys, still published, still being
mutated in the ordinary way by add()/__setitem__/pop()/update() -- all
of which run under md's critical section, which does not exclude a
lock-free reader at all.

entry->identity doubles as the "is this slot populated" signal a
lock-free walk checks first (mirroring CPython's own me_key in
compare_unicode_unicode_threadsafe): insertion publishes every other
field an entry needs (hash, key, value) before publishing identity,
last, and deletion clears identity. entry->identity is never replaced
with a *different* non-NULL identity while an entry stays populated
(only key/value change on replace/update), so the only transitions a
reader can race are NULL -> real (insertion) and real -> NULL
(deletion) -- never real -> different-real.

That ordering alone is not enough to safely dereference the identity
or value object's *contents* (_str_cmp, PyUnstable_TryIncRef's own
caller), though: a concurrent delete's Py_CLEAR() is an ordinary
decref with no deferred reclamation, so it can free the object
immediately. Every lock-free read of entry->identity or entry->value
therefore needs PyUnstable_TryIncRef() (safe even if the object is
mid-teardown on another thread; fails cleanly instead of racing it)
followed by re-reading the field to confirm it still holds what was
just incref'd -- if either step fails, the field changed or is
changing under us and the caller must fall back to the critical
section, exactly like CPython's DKIX_KEY_CHANGED retry.
*/

static inline PyObject*
_md_entry_load_identity(entry_t* entry)
{
    return (PyObject*)atomic_load_ptr((void* const*)&entry->identity);
}

static inline void
_md_entry_publish_identity(entry_t* entry, PyObject* identity)
{
#if _MD_HAVE_TRYINCREF
    PyUnstable_EnableTryIncRef(identity);
#endif
    atomic_store_ptr((void**)&entry->identity, identity);
}

static inline void
_md_entry_clear_identity(entry_t* entry)
{
    PyObject* old = _md_entry_load_identity(entry);
    atomic_store_ptr((void**)&entry->identity, NULL);
    Py_XDECREF(old);
}

static inline PyObject*
_md_entry_load_value(entry_t* entry)
{
    return (PyObject*)atomic_load_ptr((void* const*)&entry->value);
}

static inline void
_md_entry_publish_value(entry_t* entry, PyObject* value)
{
#if _MD_HAVE_TRYINCREF
    PyUnstable_EnableTryIncRef(value);
#endif
    atomic_store_ptr((void**)&entry->value, value);
}

static inline void
_md_entry_store_value(entry_t* entry, PyObject* value)
{
    PyObject* old = _md_entry_load_value(entry);
    _md_entry_publish_value(entry, value);
    Py_XDECREF(old);
}

static inline void
_md_entry_clear_value(entry_t* entry)
{
    PyObject* old = _md_entry_load_value(entry);
    atomic_store_ptr((void**)&entry->value, NULL);
    Py_XDECREF(old);
}

/* entry->hash also needs an atomic accessor once a lock-free reader
   compares against it: _md_replace()/_md_update() overwrite it in
   place on an already-populated entry (a plain write racing the
   reader's plain read is still a data race even though Py_hash_t
   isn't a pointer and can't crash on a torn value). Relaxed is enough
   -- it's read only after the identity check above already
   established happens-before for everything else in the entry;
   nothing else depends on this specific field's ordering. */
static inline Py_hash_t
_md_entry_load_hash(entry_t* entry)
{
    return (Py_hash_t)atomic_load_ssize_relaxed((Py_ssize_t*)&entry->hash);
}

static inline void
_md_entry_store_hash(entry_t* entry, Py_hash_t hash)
{
    atomic_store_ssize_relaxed((Py_ssize_t*)&entry->hash, (Py_ssize_t)hash);
}

#if _MD_HAVE_TRYINCREF
/* Tries to safely grab a strong reference to *addr's current value for
   a lock-free reader: PyUnstable_TryIncRef() (fails cleanly if the
   object is concurrently being torn down) followed by re-reading
   *addr to confirm it is still what was just incref'd. Returns NULL
   (with no reference held) if either step fails, meaning the caller
   must fall back to the critical section; the field may be NULL
   legitimately (not populated / deleted), which is reported the same
   way, since either way the caller cannot proceed lock-free. Only
   defined where PyUnstable_TryIncRef() exists at all (see the
   _MD_HAVE_TRYINCREF comment above); callers must be equally
   guarded. */
static inline PyObject*
_md_entry_try_get_ref(PyObject** addr)
{
    PyObject* value = (PyObject*)atomic_load_ptr((void* const*)addr);
    if (value == NULL) {
        return NULL;
    }
    if (!PyUnstable_TryIncRef(value)) {
        return NULL;
    }
    if ((PyObject*)atomic_load_ptr((void* const*)addr) != value) {
        Py_DECREF(value);
        return NULL;
    }
    return value;
}
#endif /* _MD_HAVE_TRYINCREF */

#endif /* Py_GIL_DISABLED */

static inline int
_md_resize(MultiDictObject* md, uint8_t log2_newsize, bool update)
{
    if (log2_newsize >= SIZEOF_SIZE_T * 8) {
        PyErr_NoMemory();
        return -1;
    }
    assert(log2_newsize >= HT_LOG_MINSIZE);

    htkeys_t* newkeys = htkeys_new(log2_newsize);
    if (newkeys == NULL) {
        return -1;
    }

    htkeys_t* oldkeys = md->keys;
    Py_ssize_t numentries = md->used;
    entry_t* oldentries = htkeys_entries(oldkeys);
    entry_t* newentries = htkeys_entries(newkeys);
    if (oldkeys->nentries == numentries) {
        memcpy(newentries, oldentries, numentries * sizeof(entry_t));
    } else {
        entry_t* new_ep = newentries;
        entry_t* old_ep = oldentries;
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

    newkeys->usable = newkeys->usable - numentries;
    newkeys->nentries = numentries;

#ifdef Py_GIL_DISABLED
    _md_store_keys(md, newkeys);
#else
    md->keys = newkeys;
#endif

#ifdef Py_GIL_DISABLED
    /* Bump the version on every resize, not just when a caller's
       own insert/delete/replace would bump it anyway: a freed
       htkeys_t can get reallocated at the very same address by a
       later resize (same size class, common in practice), so code
       elsewhere that detects "did md->keys change under me" by
       comparing the raw pointer alone (see _md_replace()'s and
       _md_update()'s comments) needs a companion signal that can't
       coincidentally repeat. */
    md->version = NEXT_VERSION(md->state);

    /* Ownership of oldkeys's entries has already moved to newkeys via
       the memcpy/copy loop above; zeroing nentries tells
       _md_retire()'s cleanup there is nothing left to decref, only
       memory to free. */
    if (oldkeys != &empty_htkeys) {
        oldkeys->nentries = 0;
    }
    _md_retire(md, oldkeys);
#else
    if (oldkeys != &empty_htkeys) {
        htkeys_free(oldkeys);
    }
#endif

    ASSERT_CONSISTENT(md, update);
    return 0;
}

static inline int
_md_shrink(MultiDictObject* md, bool update)
{
#ifdef Py_GIL_DISABLED
    /* The in-place compaction below rewrites the currently-published
       table's entries and indices while md->keys keeps pointing at it
       the whole time -- safe when every reader holds the critical
       section (mutually exclusive with this function), not safe
       against a lock-free reader concurrently walking the very memory
       being rewritten. _md_resize() already has the build-a-new-table,
       swap, retire-the-old-one shape lock-free reads need; reusing it
       at the *current* size does exactly what shrinking means here
       (drop the dummy-slot gaps) without a second, duplicate
       implementation of that shape. */
    return _md_resize(md, md->keys->log2_size, update);
#else
    htkeys_t* keys = md->keys;
    Py_ssize_t nentries = keys->nentries;
    entry_t* entries = htkeys_entries(keys);
    entry_t* new_ep = entries;
    entry_t* old_ep = entries;
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
#endif
}

static inline int
_md_resize_for_insert(MultiDictObject* md)
{
    if (md->used < md->keys->nentries) {
        return _md_shrink(md, false);
    } else {
        return _md_resize(md, calculate_log2_keysize(GROWTH_RATE(md)), false);
    }
}

static inline int
_md_resize_for_update(MultiDictObject* md)
{
    if (md->used < md->keys->nentries) {
        return _md_shrink(md, true);
    } else {
        return _md_resize(md, calculate_log2_keysize(GROWTH_RATE(md)), true);
    }
}

static inline int
_md_reserve(MultiDictObject* md, Py_ssize_t extra_size, bool update)
{
    uint8_t new_size = estimate_log2_keysize(extra_size + md->used);
    if (new_size > md->keys->log2_size) {
        return _md_resize(md, new_size, update);
    }
    return 0;
}

static inline int
md_reserve(MultiDictObject* md, Py_ssize_t extra_size)
{
    return _md_reserve(md, extra_size, false);
}

static inline int
md_clear(MultiDictObject* md);

static inline int
md_init(MultiDictObject* md, bool is_ci, Py_ssize_t minused)
{
    assert(md->state != NULL);
    htkeys_t* new_keys = (htkeys_t*)&empty_htkeys;

    if (minused > USABLE_FRACTION(HT_MINSIZE)) {
        const uint8_t log2_max_presize = 17;
        const Py_ssize_t max_presize = ((Py_ssize_t)1) << log2_max_presize;
        uint8_t log2_newsize;
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
    }

    md_clear(md);
    md->is_ci = is_ci;
#ifdef Py_GIL_DISABLED
    _md_store_used(md, 0);
#else
    md->used = 0;
#endif
    md->version = NEXT_VERSION(md->state);
#ifdef Py_GIL_DISABLED
    _md_store_keys(md, new_keys);
#else
    md->keys = new_keys;
#endif
    ASSERT_CONSISTENT(md, false);
    return 0;
}

static inline int
md_clone_from_ht(MultiDictObject* md, MultiDictObject* other)
{
    ASSERT_CONSISTENT(other, false);

    htkeys_t* keys = (htkeys_t*)&empty_htkeys;
    htkeys_t* src = other->keys;
    while (src != &empty_htkeys) {
        size_t size = htkeys_sizeof(src);

        /* Allocating can transiently suspend our critical section on
           `other`, for the same reason explained in _md_resize(): a
           blocking PyMem_Malloc() may release the lock, letting another
           thread that also locks `other` run to completion (e.g. resizing
           other->keys and freeing this exact buffer) before we resume.
           `size` and `src` were computed before the call and cannot be
           trusted afterward, so re-read other->keys and retry if it no
           longer matches what we sized the allocation for, instead of
           copying `size` bytes from a possibly different (or freed)
           buffer. */
        keys = PyMem_Malloc(size);
        if (keys == NULL) {
            PyErr_NoMemory();
            return -1;
        }

        htkeys_t* fresh_src = other->keys;
        if (fresh_src != src) {
            PyMem_Free(keys);
            keys = (htkeys_t*)&empty_htkeys;
            src = fresh_src;
            continue;
        }

        memcpy(keys, fresh_src, size);
        keys->resume_slots = NULL;
#ifdef Py_GIL_DISABLED
        keys->num_readers = 0;
        keys->retired_next = NULL;
#endif
        entry_t* entry = htkeys_entries(keys);
        for (Py_ssize_t idx = 0; idx < keys->nentries; idx++, entry++) {
            Py_XINCREF(entry->identity);
            Py_XINCREF(entry->key);
            Py_XINCREF(entry->value);
        }
        break;
    }

    /* No allocation happens between here and the writes to md below, so
       this snapshot of other's remaining fields is consistent with the
       keys buffer just copied above. */
    Py_ssize_t used = other->used;
    uint64_t version = other->version;
    bool is_ci = other->is_ci;

    md_clear(md);
#ifdef Py_GIL_DISABLED
    _md_store_used(md, used);
#else
    md->used = used;
#endif
    md->version = version;
    md->is_ci = is_ci;
#ifdef Py_GIL_DISABLED
    _md_store_keys(md, keys);
#else
    md->keys = keys;
#endif
    ASSERT_CONSISTENT(md, false);
    return 0;
}

static inline PyObject*
md_calc_identity(MultiDictObject* md, PyObject* key)
{
    if (md->is_ci) return _ci_key_to_identity(md->state, key);
    return _key_to_identity(md->state, key);
}

static inline PyObject*
_md_calc_key(MultiDictObject* md, PyObject* key, PyObject* identity)
{
    if (md->is_ci) return _ci_arg_to_key(md->state, key, identity);
    return _arg_to_key(md->state, key, identity);
}

static inline Py_ssize_t
md_len(MultiDictObject* md)
{
    return atomic_load_ssize_relaxed(&md->used);
}

static inline PyObject*
_md_ensure_key(MultiDictObject* md, entry_t* entry)
{
    assert(entry >= htkeys_entries(md->keys));
    assert(entry < htkeys_entries(md->keys) + md->keys->nentries);
    PyObject* key = _md_calc_key(md, entry->key, entry->identity);
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
_md_add_with_hash_steal_refs(MultiDictObject* md, Py_hash_t hash,
                             PyObject* identity, PyObject* key,
                             PyObject* value)
{
    htkeys_t* keys = md->keys;
    if (keys->usable <= 0 || keys == &empty_htkeys) {
        /* Need to resize. */
        if (_md_resize_for_insert(md) < 0) {
            return -1;
        }
        keys = md->keys;  // updated by resizing
    }

    Py_ssize_t hashpos = htkeys_find_empty_slot(keys, hash);
    htkeys_set_index(keys, hashpos, keys->nentries);

    entry_t* entry = htkeys_entries(keys) + keys->nentries;

#ifdef Py_GIL_DISABLED
    /* identity is published last: it's the field a lock-free reader
       checks first (before ever touching hash/key/value), treating
       NULL as "not populated yet, keep probing". See the comment
       above _md_entry_load_identity(). */
    entry->key = key;
    _md_entry_store_hash(entry, hash);
    _md_entry_store_value(entry, value);
    _md_entry_publish_identity(entry, identity);
#else
    entry->identity = identity;
    entry->key = key;
    entry->value = value;
    entry->hash = hash;
#endif

    md->version = NEXT_VERSION(md->state);
#ifdef Py_GIL_DISABLED
    _md_add_used(md, 1);
#else
    md->used += 1;
#endif
    keys->usable -= 1;
    keys->nentries += 1;
    return 0;
}

static inline int
_md_add_with_hash(MultiDictObject* md, Py_hash_t hash, PyObject* identity,
                  PyObject* key, PyObject* value)
{
    Py_INCREF(identity);
    Py_INCREF(key);
    Py_INCREF(value);
    return _md_add_with_hash_steal_refs(md, hash, identity, key, value);
}

static inline int
_md_add_for_upd_steal_refs(MultiDictObject* md, Py_hash_t hash,
                           PyObject* identity, PyObject* key, PyObject* value)
{
    htkeys_t* keys = md->keys;
    if (keys->usable <= 0 || keys == &empty_htkeys) {
        /* Need to resize. */
        if (_md_resize_for_update(md) < 0) {
            return -1;
        }
        keys = md->keys;  // updated by resizing
    }
    Py_ssize_t hashpos = htkeys_find_empty_slot(keys, hash);
    htkeys_set_index(keys, hashpos, keys->nentries);

    entry_t* entry = htkeys_entries(keys) + keys->nentries;

#ifdef Py_GIL_DISABLED
    entry->key = key;
    _md_entry_store_hash(entry, hash | MD_HASH_MARK);
    _md_entry_store_value(entry, value);
    _md_entry_publish_identity(entry, identity);
#else
    entry->identity = identity;
    entry->key = key;
    entry->value = value;
    entry->hash = hash | MD_HASH_MARK;
#endif

    md->version = NEXT_VERSION(md->state);
#ifdef Py_GIL_DISABLED
    _md_add_used(md, 1);
#else
    md->used += 1;
#endif
    keys->usable -= 1;
    keys->nentries += 1;
    return 0;
}

static inline int
_md_add_for_upd(MultiDictObject* md, Py_hash_t hash, PyObject* identity,
                PyObject* key, PyObject* value)
{
    Py_INCREF(identity);
    Py_INCREF(key);
    Py_INCREF(value);
    return _md_add_for_upd_steal_refs(md, hash, identity, key, value);
}

static inline int
md_add(MultiDictObject* md, PyObject* key, PyObject* value)
{
    PyObject* identity = md_calc_identity(md, key);
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

static inline void
_md_del_at(MultiDictObject* md, size_t slot, entry_t* entry)
{
    htkeys_t* keys = md->keys;
    assert(keys != &empty_htkeys);
#ifdef Py_GIL_DISABLED
    /* Null out every field and finish md's bookkeeping (index, used)
       before dropping any reference. A decref below can transiently
       suspend this thread's critical section -- freeing an object can
       contend the same allocator lock as PyMem_Malloc(), see the
       comment above _md_resize() -- letting a concurrent resize run
       in between. If that resize caught this entry with some fields
       already NULL and others (or md->used, or the index) not yet
       updated, it would see md in a state that is neither "entry
       still there" nor "entry gone" and corrupt itself. Saving the
       objects locally and decref'ing them only once md is already
       fully self-consistent means a concurrent resize -- however far
       into this function it catches us -- always sees a coherent
       view. entry->key is read/written as a plain pointer: unlike
       identity/value, no lock-free reader ever touches it (see the
       comment above _md_entry_load_identity()). */
    PyObject* identity = _md_entry_load_identity(entry);
    PyObject* key = entry->key;
    PyObject* value = _md_entry_load_value(entry);

    atomic_store_ptr((void**)&entry->identity, NULL);
    entry->key = NULL;
    atomic_store_ptr((void**)&entry->value, NULL);
    htkeys_set_index(keys, slot, DKIX_DUMMY);
    _md_add_used(md, -1);

    Py_XDECREF(identity);
    Py_XDECREF(key);
    Py_XDECREF(value);
#else
    Py_CLEAR(entry->identity);
    Py_CLEAR(entry->key);
    Py_CLEAR(entry->value);
    htkeys_set_index(keys, slot, DKIX_DUMMY);
    md->used -= 1;
#endif
}

static inline void
_md_del_at_for_upd(MultiDictObject* md, size_t slot, entry_t* entry)
{
    /* half deletion,
       the entry could be replaced later with key and value set
       or it will be finally cleaned up with identity=NULL,
       used -= 1, and setting the hash to DKIX_DUMMY
       in md_post_update()
    */
    assert(md->keys != &empty_htkeys);
#ifdef Py_GIL_DISABLED
    /* Null out both fields before dropping either reference: a decref
       between the two can transiently suspend the critical section
       (same allocator-lock mechanism as _md_resize()'s PyMem_Malloc(),
       see its comment) and let a concurrent resize free the table
       `entry` lives in, in which case the *second* field access below
       would be a use-after-free rather than just an inconsistency a
       caller could later detect. */
    PyObject* old_key = entry->key;
    PyObject* old_value = _md_entry_load_value(entry);
    entry->key = NULL;
    atomic_store_ptr((void**)&entry->value, NULL);
    Py_XDECREF(old_key);
    Py_XDECREF(old_value);
#else
    Py_CLEAR(entry->key);
    Py_CLEAR(entry->value);
#endif
}

static inline int
md_del(MultiDictObject* md, PyObject* key)
{
    PyObject* identity = md_calc_identity(md, key);
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

    entry_t* entries = htkeys_entries(md->keys);

    for (; iter.index != DKIX_EMPTY; htkeysiter_next(&iter)) {
        if (iter.index < 0) {
            continue;
        }
        entry_t* entry = entries + iter.index;
        if (hash != entry->hash) {
            continue;
        }
        if (!_str_cmp(entry->identity, identity)) {
            continue;
        }

        found = true;
        _md_del_at(md, iter.slot, entry);
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
md_version(MultiDictObject* md)
{
    return md->version;
}

static inline void
md_init_pos(MultiDictObject* md, md_pos_t* pos)
{
    pos->pos = 0;
    pos->version = md->version;
}

static inline int
md_next(MultiDictObject* md, md_pos_t* pos, PyObject** pidentity,
        PyObject** pkey, PyObject** pvalue)
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

    entry_t* entries = htkeys_entries(md->keys);
    entry_t* entry = entries + pos->pos;

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
            // *pidentity was already set above; release it before cleanup
            // NULLs it, otherwise the identity reference leaks.
            if (pidentity) {
                Py_CLEAR(*pidentity);
            }
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

static inline void
md_init_pos_reverse(MultiDictObject* md, md_pos_t* pos)
{
    pos->pos = md->keys->nentries - 1;
    pos->version = md->version;
}

static inline int
md_prev(MultiDictObject* md, md_pos_t* pos, PyObject** pidentity,
        PyObject** pkey, PyObject** pvalue)
{
    int ret = 0;

    if (pos->version != md->version) {
        PyErr_SetString(PyExc_RuntimeError,
                        "MultiDict is changed during iteration");
        ret = -1;
        goto cleanup;
    }

    if (pos->pos < 0) {
        goto cleanup;
    }

    entry_t* entries = htkeys_entries(md->keys);
    entry_t* entry = entries + pos->pos;

    while (entry->identity == NULL) {
        pos->pos -= 1;
        if (pos->pos < 0) {
            goto cleanup;
        }
        entry -= 1;
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
            // *pidentity was already set above; release it before cleanup
            // NULLs it, otherwise the identity reference leaks.
            if (pidentity) {
                Py_CLEAR(*pidentity);
            }

            goto cleanup;
        }
    }
    if (pvalue) {
        *pvalue = Py_NewRef(entry->value);
    }

    --pos->pos;
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
md_init_finder(MultiDictObject* md, PyObject* identity, md_finder_t* finder)
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
md_finder_slot(md_finder_t* finder)
{
    assert(finder->md != NULL);
    return finder->iter.slot;
}

static inline Py_ssize_t
md_finder_index(md_finder_t* finder)
{
    assert(finder->md != NULL);
    assert(finder->iter.index >= 0);
    return finder->iter.index;
}

static inline int
md_find_next(md_finder_t* finder, PyObject** pkey, PyObject** pvalue)
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

    entry_t* entries = htkeys_entries(finder->md->keys);

    for (; finder->iter.index != DKIX_EMPTY; htkeysiter_next(&finder->iter)) {
        if (finder->iter.index < 0) {
            continue;
        }
        entry_t* entry = entries + finder->iter.index;
        if (entry->hash != finder->hash) {
            continue;
        }
        if (!_str_cmp(finder->identity, entry->identity)) {
            continue;
        }

        /* found, mark the entry as visited */
#ifdef Py_GIL_DISABLED
        _md_entry_store_hash(entry, finder->hash | MD_HASH_MARK);
#else
        entry->hash = finder->hash | MD_HASH_MARK;
#endif

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
md_finder_cleanup(md_finder_t* finder)
{
    if (finder->md == NULL) {
        return;
    }

    htkeysiter_init(&finder->iter, finder->md->keys, finder->hash);
    entry_t* entries = htkeys_entries(finder->md->keys);
    for (; finder->iter.index != DKIX_EMPTY; htkeysiter_next(&finder->iter)) {
        if (finder->iter.index < 0) {
            continue;
        }
        entry_t* entry = entries + finder->iter.index;
        if (entry->hash == (finder->hash | MD_HASH_MARK)) {
#ifdef Py_GIL_DISABLED
            _md_entry_store_hash(entry, finder->hash);
#else
            entry->hash = finder->hash;
#endif
        }
    }
    ASSERT_CONSISTENT(finder->md, false);
    finder->md = NULL;
}

static inline int
_md_contains_locked(MultiDictObject* md, PyObject* identity, Py_hash_t hash,
                    PyObject** pret)
{
    htkeysiter_t iter;
    htkeysiter_init(&iter, md->keys, hash);
    entry_t* entries = htkeys_entries(md->keys);

    for (; iter.index != DKIX_EMPTY; htkeysiter_next(&iter)) {
        if (iter.index < 0) {
            continue;
        }
        entry_t* entry = entries + iter.index;
        if (hash != entry->hash) {
            continue;
        }
        if (_str_cmp(identity, entry->identity)) {
            if (pret != NULL) {
                *pret = _md_ensure_key(md, entry);
                if (*pret == NULL) {
                    return -1;
                }
            }
            return 1;
        }
    }
    if (pret != NULL) {
        *pret = NULL;
    }
    return 0;
}

#if defined(Py_GIL_DISABLED) && _MD_HAVE_TRYINCREF

static inline int
_md_contains_lockfree(MultiDictObject* md, PyObject* identity, Py_hash_t hash)
{
    htkeys_t* keys = _md_reader_enter(md);
    htkeysiter_t iter;
    htkeysiter_init(&iter, keys, hash);
    entry_t* entries = htkeys_entries(keys);

    int result = 0;
    for (; iter.index != DKIX_EMPTY; htkeysiter_next(&iter)) {
        if (iter.index < 0) {
            continue;
        }
        entry_t* entry = entries + iter.index;

        PyObject* entry_identity = _md_entry_try_get_ref(&entry->identity);
        if (entry_identity == NULL) {
            if (_md_entry_load_identity(entry) == NULL) {
                continue;  // not populated (or deleted); keep probing
            }
            result = 2;  // _MD_NEED_LOCK
            break;
        }

        /* Masked, not a raw comparison: entry->hash can legitimately
           carry MD_HASH_MARK right now if a different, concurrently-
           suspended _md_replace()/_md_update() call has this exact
           entry marked (see the comment above MD_HASH_MARK). A raw
           comparison would treat that as "wrong hash, not a match" and
           skip a key that is genuinely present both before and after
           that operation -- a false miss, not just a stale read. */
        if ((_md_entry_load_hash(entry) & PY_SSIZE_T_MAX) != hash) {
            Py_DECREF(entry_identity);
            continue;
        }

        bool matched = _str_cmp(identity, entry_identity);
        Py_DECREF(entry_identity);
        if (matched) {
            result = 1;
            break;
        }
    }

    _md_reader_exit(md, keys);
    return result;
}

#endif /* Py_GIL_DISABLED && _MD_HAVE_TRYINCREF */

static inline int
md_contains(MultiDictObject* md, PyObject* key, PyObject** pret)
{
    if (!PyUnicode_Check(key)) {
        return 0;
    }

    PyObject* identity = md_calc_identity(md, key);
    if (identity == NULL) {
        if (pret != NULL) {
            *pret = NULL;
        }
        return -1;
    }

    Py_hash_t hash = _unicode_hash(identity);
    if (hash == -1) {
        Py_DECREF(identity);
        if (pret != NULL) {
            *pret = NULL;
        }
        return -1;
    }

    int result;
#if defined(Py_GIL_DISABLED) && _MD_HAVE_TRYINCREF
    if (pret == NULL) {
        result = _md_contains_lockfree(md, identity, hash);
        if (result != 2 /* _MD_NEED_LOCK */) {
            Py_DECREF(identity);
            return result;
        }
    }
    Py_BEGIN_CRITICAL_SECTION(md);
    result = _md_contains_locked(md, identity, hash, pret);
    Py_END_CRITICAL_SECTION();
#elif defined(Py_GIL_DISABLED)
    Py_BEGIN_CRITICAL_SECTION(md);
    result = _md_contains_locked(md, identity, hash, pret);
    Py_END_CRITICAL_SECTION();
#else
    result = _md_contains_locked(md, identity, hash, pret);
#endif
    Py_DECREF(identity);
    return result;
}

static inline int
_md_get_one_locked(MultiDictObject* md, PyObject* identity, Py_hash_t hash,
                   PyObject** ret)
{
    htkeysiter_t iter;
    htkeysiter_init(&iter, md->keys, hash);
    entry_t* entries = htkeys_entries(md->keys);

    for (; iter.index != DKIX_EMPTY; htkeysiter_next(&iter)) {
        if (iter.index < 0) {
            continue;
        }
        entry_t* entry = entries + iter.index;
        if (hash != entry->hash) {
            continue;
        }
        if (_str_cmp(identity, entry->identity)) {
            *ret = Py_NewRef(entry->value);
            return 1;
        }
    }
    return 0;
}

#if defined(Py_GIL_DISABLED) && _MD_HAVE_TRYINCREF

/* Sentinel meaning "could not complete lock-free"; never returned to
   md_get_one()'s own caller, only used between the two functions
   below. Distinct from 1 (found) / 0 (not found) / -1 (error). */
#define _MD_NEED_LOCK 2

static inline int
_md_get_one_lockfree(MultiDictObject* md, PyObject* identity, Py_hash_t hash,
                     PyObject** ret)
{
    htkeys_t* keys = _md_reader_enter(md);
    htkeysiter_t iter;
    htkeysiter_init(&iter, keys, hash);
    entry_t* entries = htkeys_entries(keys);

    int result = 0;
    for (; iter.index != DKIX_EMPTY; htkeysiter_next(&iter)) {
        if (iter.index < 0) {
            continue;
        }
        entry_t* entry = entries + iter.index;

        PyObject* entry_identity = _md_entry_try_get_ref(&entry->identity);
        if (entry_identity == NULL) {
            if (_md_entry_load_identity(entry) == NULL) {
                continue;  // not populated (or deleted); keep probing
            }
            result = _MD_NEED_LOCK;  // racing a concurrent change
            break;
        }

        /* Masked, not a raw comparison -- see the identical comment in
           _md_contains_lockfree(): entry->hash can be legitimately
           MD_HASH_MARK-ed by a different, concurrently-suspended
           _md_replace()/_md_update() call right now, and a raw
           comparison would wrongly treat a present key as absent. */
        if ((_md_entry_load_hash(entry) & PY_SSIZE_T_MAX) != hash) {
            Py_DECREF(entry_identity);
            continue;
        }

        bool matched = _str_cmp(identity, entry_identity);
        Py_DECREF(entry_identity);
        if (!matched) {
            continue;
        }

        PyObject* value = _md_entry_try_get_ref(&entry->value);
        if (value == NULL) {
            result = _MD_NEED_LOCK;
            break;
        }
        *ret = value;
        result = 1;
        break;
    }

    _md_reader_exit(md, keys);
    return result;
}

static inline int
md_get_one(MultiDictObject* md, PyObject* key, PyObject** ret)
{
    PyObject* identity = md_calc_identity(md, key);
    if (identity == NULL) {
        return -1;
    }
    Py_hash_t hash = _unicode_hash(identity);
    if (hash == -1) {
        Py_DECREF(identity);
        return -1;
    }

    int result = _md_get_one_lockfree(md, identity, hash, ret);
    if (result != _MD_NEED_LOCK) {
        Py_DECREF(identity);
        return result;
    }

    Py_BEGIN_CRITICAL_SECTION(md);
    result = _md_get_one_locked(md, identity, hash, ret);
    Py_END_CRITICAL_SECTION();
    Py_DECREF(identity);
    return result;
}

#undef _MD_NEED_LOCK

#elif defined(Py_GIL_DISABLED)

static inline int
md_get_one(MultiDictObject* md, PyObject* key, PyObject** ret)
{
    PyObject* identity = md_calc_identity(md, key);
    if (identity == NULL) {
        return -1;
    }
    Py_hash_t hash = _unicode_hash(identity);
    if (hash == -1) {
        Py_DECREF(identity);
        return -1;
    }
    int result;
    Py_BEGIN_CRITICAL_SECTION(md);
    result = _md_get_one_locked(md, identity, hash, ret);
    Py_END_CRITICAL_SECTION();
    Py_DECREF(identity);
    return result;
}

#else /* !Py_GIL_DISABLED */

static inline int
md_get_one(MultiDictObject* md, PyObject* key, PyObject** ret)
{
    PyObject* identity = md_calc_identity(md, key);
    if (identity == NULL) {
        return -1;
    }
    Py_hash_t hash = _unicode_hash(identity);
    if (hash == -1) {
        Py_DECREF(identity);
        return -1;
    }
    int result = _md_get_one_locked(md, identity, hash, ret);
    Py_DECREF(identity);
    return result;
}

#endif /* Py_GIL_DISABLED */

static inline int
md_get_all(MultiDictObject* md, PyObject* key, PyObject** ret)
{
    int tmp;
    PyObject* value = NULL;
    *ret = NULL;

    md_finder_t finder = {0};

    PyObject* identity = md_calc_identity(md, key);
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

/* Collect every (key, value) pair matching `identity` into a fresh list,
   fully marking and restoring the finder chain before returning. Run any
   user code (a value comparison that may call a custom __eq__) against
   the result only after this returns, never mid-walk: entries stay
   marked until md_finder_cleanup(), and reentering the same MultiDict
   (e.g. via getall()) while marked would hide some matching entries.

   `with_keys` selects values (false) or (key, value) tuples (true). */
static inline PyObject*
md_finder_collect(MultiDictObject* md, PyObject* identity, bool with_keys)
{
    md_finder_t finder = {0};
    PyObject* key = NULL;
    PyObject* value = NULL;
    PyObject* item;
    int tmp;

    PyObject* ret = PyList_New(0);
    if (ret == NULL) {
        return NULL;
    }

    if (md_init_finder(md, identity, &finder) < 0) {
        assert(PyErr_Occurred());
        Py_DECREF(ret);
        return NULL;
    }

    while ((tmp = md_find_next(&finder, with_keys ? &key : NULL, &value)) >
           0) {
        if (with_keys) {
            item = PyTuple_Pack(2, key, value);
            Py_CLEAR(key);
            Py_CLEAR(value);
            if (item == NULL) {
                goto fail;
            }
        } else {
            item = value;
            value = NULL;
        }
        tmp = PyList_Append(ret, item);
        Py_DECREF(item);
        if (tmp < 0) {
            goto fail;
        }
    }
    md_finder_cleanup(&finder);
    if (tmp < 0) {
        goto fail_no_cleanup;
    }
    return ret;
fail:
    md_finder_cleanup(&finder);
fail_no_cleanup:
    Py_CLEAR(key);
    Py_CLEAR(value);
    Py_DECREF(ret);
    return NULL;
}

/* Restore every entry hash md_to_dict()'s walk left marked.

   md_finder_cleanup() restores one hash chain, which is what a single
   getall() needs. md_to_dict() instead keeps the marks of every key it has
   collected, so that a later duplicate of the same key tests as collected
   and is skipped, and clears the whole table once at the end. */
static inline void
_md_restore_all_hashes(MultiDictObject* md)
{
    entry_t* entries = htkeys_entries(md->keys);
    Py_ssize_t nentries = md->keys->nentries;
    for (Py_ssize_t pos = 0; pos < nentries; pos++) {
        entry_t* entry = entries + pos;
#ifdef Py_GIL_DISABLED
        Py_hash_t hash = _md_entry_load_hash(entry);
        if (hash < 0) {
            _md_entry_store_hash(entry, hash & PY_SSIZE_T_MAX);
        }
#else
        if (entry->hash < 0) {
            entry->hash &= PY_SSIZE_T_MAX;
        }
#endif
    }
}

static inline int
md_to_dict(MultiDictObject* md, PyObject** ret)
{
    PyObject* key = NULL;
    PyObject* value = NULL;
    PyObject* lst = NULL;
    PyObject* pos_obj = NULL;
    md_finder_t finder = {0};
    uint64_t version = md->version;
    int tmp;

    *ret = NULL;

    /* Collected as [position, values, position, values, ...], in the order
       the keys are first seen; the walk below cannot build the keys yet,
       see the second loop for why. */
    PyObject* pending = PyList_New(0);
    if (pending == NULL) {
        return -1;
    }

    /* Walk the entries in insertion order, so every key is collected at its
       first spelling; md_find_next() walks a hash chain, which is not
       insertion-ordered. Nothing in this loop runs Python, which is what
       makes it safe to leave the marks set until the walk is over. */
    for (Py_ssize_t pos = 0; pos < md->keys->nentries; pos++) {
        entry_t* entry = htkeys_entries(md->keys) + pos;
        if (entry->identity == NULL) {
            continue;  // deleted
        }
        if (entry->hash < 0) {
            continue;  // marked, so collected already under its first key
        }

        if (md_init_finder(md, entry->identity, &finder) < 0) {
            goto fail;
        }
        /* Collects this key's values in insertion order, marking every entry
           it visits. The marks stay: they are what makes the duplicates of
           this key, later in the walk, test as collected. */
        while ((tmp = md_find_next(&finder, NULL, &value)) > 0) {
            if (lst == NULL) {
                lst = PyList_New(1);
                if (lst == NULL) {
                    goto fail;
                }
                PyList_SET_ITEM(lst, 0, value);
                value = NULL;  // stolen by PyList_SET_ITEM
            } else {
                if (PyList_Append(lst, value) < 0) {
                    goto fail;
                }
                Py_CLEAR(value);
            }
        }
        if (tmp < 0) {
            goto fail;
        }
        if (lst == NULL) {
            continue;  // not reachable from its own hash chain
        }

        pos_obj = PyLong_FromSsize_t(pos);
        if (pos_obj == NULL) {
            goto fail;
        }
        if (PyList_Append(pending, pos_obj) < 0) {
            goto fail;
        }
        if (PyList_Append(pending, lst) < 0) {
            goto fail;
        }
        Py_CLEAR(pos_obj);
        Py_CLEAR(lst);
    }

    _md_restore_all_hashes(md);

    /* Only now, with nothing left marked, may the keys be built and hashed.
       Both calls below can run a str subclass's own __hash__, __eq__ or
       __del__, and code that re-enters this multidict from there must not
       meet a table in which the collected keys read as absent. A mutation
       from there is refused the way md_next() refuses one. */
    *ret = PyDict_New();
    if (*ret == NULL) {
        goto fail_restored;
    }
    Py_ssize_t npending = PyList_GET_SIZE(pending);
    for (Py_ssize_t i = 0; i < npending; i += 2) {
        Py_ssize_t pos = PyLong_AsSsize_t(PyList_GET_ITEM(pending, i));
        key = _md_ensure_key(md, htkeys_entries(md->keys) + pos);
        if (key == NULL) {
            goto fail_restored;
        }
        if (PyDict_SetItem(*ret, key, PyList_GET_ITEM(pending, i + 1)) < 0) {
            goto fail_restored;
        }
        Py_CLEAR(key);
        /* Checked after each step, so a mutation is caught before the next
           one reads an entry the table may since have moved. */
        if (md->version != version) {
            PyErr_SetString(PyExc_RuntimeError,
                            "MultiDict is changed during iteration");
            goto fail_restored;
        }
    }

    Py_DECREF(pending);
    return 0;
fail:
    _md_restore_all_hashes(md);
fail_restored:
    Py_XDECREF(pos_obj);
    Py_XDECREF(key);
    Py_XDECREF(value);
    Py_XDECREF(lst);
    Py_DECREF(pending);
    Py_CLEAR(*ret);
    return -1;
}

static inline int
md_set_default(MultiDictObject* md, PyObject* key, PyObject* value,
               PyObject** result)
{
    *result = NULL;
    PyObject* identity = md_calc_identity(md, key);
    if (identity == NULL) {
        goto fail;
    }

    Py_hash_t hash = _unicode_hash(identity);
    if (hash == -1) {
        goto fail;
    }

    htkeysiter_t iter;
    htkeysiter_init(&iter, md->keys, hash);
    entry_t* entries = htkeys_entries(md->keys);

    for (; iter.index != DKIX_EMPTY; htkeysiter_next(&iter)) {
        if (iter.index < 0) {
            continue;
        }
        entry_t* entry = entries + iter.index;

        if (hash != entry->hash) {
            continue;
        }
        if (_str_cmp(identity, entry->identity)) {
            Py_DECREF(identity);
            ASSERT_CONSISTENT(md, false);
            *result = Py_NewRef(entry->value);
            return 1;
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
md_pop_one(MultiDictObject* md, PyObject* key, PyObject** ret)
{
    PyObject* value = NULL;

    PyObject* identity = md_calc_identity(md, key);
    if (identity == NULL) {
        goto fail;
    }

    Py_hash_t hash = _unicode_hash(identity);
    if (hash == -1) {
        goto fail;
    }

    htkeysiter_t iter;
    htkeysiter_init(&iter, md->keys, hash);
    entry_t* entries = htkeys_entries(md->keys);

    for (; iter.index != DKIX_EMPTY; htkeysiter_next(&iter)) {
        if (iter.index < 0) {
            continue;
        }
        entry_t* entry = entries + iter.index;

        if (hash != entry->hash) {
            continue;
        }
        if (_str_cmp(identity, entry->identity)) {
            value = Py_NewRef(entry->value);
            _md_del_at(md, iter.slot, entry);
            Py_DECREF(identity);
            *ret = value;
            md->version = NEXT_VERSION(md->state);
            ASSERT_CONSISTENT(md, false);
            return 1;
        }
    }
    Py_DECREF(identity);
    ASSERT_CONSISTENT(md, false);
    return 0;
fail:
    Py_XDECREF(value);
    Py_XDECREF(identity);
    return -1;
}

static inline int
md_pop_all(MultiDictObject* md, PyObject* key, PyObject** ret)
{
    PyObject* lst = NULL;

    PyObject* identity = md_calc_identity(md, key);
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
    entry_t* entries = htkeys_entries(md->keys);

    for (; iter.index != DKIX_EMPTY; htkeysiter_next(&iter)) {
        if (iter.index < 0) {
            continue;
        }
        entry_t* entry = entries + iter.index;

        if (hash != entry->hash) {
            continue;
        }
        if (_str_cmp(identity, entry->identity)) {
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
            _md_del_at(md, iter.slot, entry);
            md->version = NEXT_VERSION(md->state);
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

static inline PyObject*
md_pop_item(MultiDictObject* md)
{
    if (md->used == 0) {
        PyErr_SetString(PyExc_KeyError, "empty multidict");
        return NULL;
    }

    entry_t* entries = htkeys_entries(md->keys);

    Py_ssize_t pos = md->keys->nentries - 1;
    entry_t* entry = entries + pos;
    while (pos >= 0 && entry->identity == NULL) {
        pos--;
        entry--;
    }
    assert(pos >= 0);

    PyObject* key = _md_calc_key(md, entry->key, entry->identity);
    if (key == NULL) {
        return NULL;
    }
    PyObject* ret = PyTuple_Pack(2, key, entry->value);
    Py_CLEAR(key);
    if (ret == NULL) {
        return NULL;
    }

    htkeysiter_t iter;
    htkeysiter_init(&iter, md->keys, entry->hash);

    for (; iter.index != pos; htkeysiter_next(&iter)) {
    }
    _md_del_at(md, iter.slot, entry);
    md->version = NEXT_VERSION(md->state);
    ASSERT_CONSISTENT(md, false);
    return ret;
}

static inline int
_md_replace(MultiDictObject* md, PyObject* key, PyObject* value,
            PyObject* identity, Py_hash_t hash)
{
    int found = 0;

    /* Loops at most once per concurrent resize this scan actually
       collides with (see the Py_GIL_DISABLED branch below); a fresh
       finder each pass means a retry costs nothing beyond redoing the
       scan. */
    for (;;) {
        md_finder_t finder = {0};
        if (md_init_finder(md, identity, &finder) < 0) {
            assert(PyErr_Occurred());
            return -1;
        }

        int tmp;
        bool stale = false;

        // don't grab neither key nor value but use the calculated index
        while ((tmp = md_find_next(&finder, NULL, NULL)) > 0) {
#ifdef Py_GIL_DISABLED
            htkeys_t* keys_before = md->keys;
            uint64_t version_before = md->version;
#endif
            entry_t* entries = htkeys_entries(md->keys);
            entry_t* entry = entries + md_finder_index(&finder);
            if (!found) {
                found = 1;
#ifdef Py_GIL_DISABLED
                /* Finish every store to this slot before dropping the
                   old key/value: a decref can transiently suspend the
                   critical section (same allocator-lock mechanism as
                   _md_resize()'s PyMem_Malloc(), see its comment),
                   letting a concurrent resize free the table `entry`
                   lives in. Deferring the decref to locally-saved
                   pointers, once entry is already in its final form,
                   means a concurrent resize's copy always sees this
                   slot correctly replaced, whether or not it caught us
                   mid-decref. If it did, this finder's iterator is now
                   walking a table md->keys has moved past -- restart
                   the scan below with a fresh one. The rescan finds
                   this very entry again (still marked, from the store
                   below) and correctly treats it as invisible rather
                   than a second occurrence to replace. */
                PyObject* old_key = entry->key;
                PyObject* old_value = _md_entry_load_value(entry);
                entry->key = Py_NewRef(key);
                _md_entry_publish_value(entry, Py_NewRef(value));
                _md_entry_store_hash(entry, finder.hash | MD_HASH_MARK);
                Py_DECREF(old_key);
                Py_DECREF(old_value);
#else
                Py_SETREF(entry->key, Py_NewRef(key));
                Py_SETREF(entry->value, Py_NewRef(value));
                entry->hash = finder.hash | MD_HASH_MARK;
#endif
            } else {
                _md_del_at(md, md_finder_slot(&finder), entry);
            }
#ifdef Py_GIL_DISABLED
            /* Checking the pointer alone isn't enough: a freed table
               can get reallocated at the very same address by a later
               resize (same size class, common in practice), which
               would make a pointer-only check miss the change. Every
               mutation bumps md->version, including ones that don't
               otherwise touch md->keys, so compare both. */
            if (md->keys != keys_before || md->version != version_before) {
                stale = true;
                break;
            }
#endif
        }
        if (stale) {
            /* Deliberately not calling md_finder_cleanup() here: it
               would unmark the entry the block above just replaced,
               making the rescan below treat it as a second occurrence
               instead of skipping it. The eventual, non-stale finder's
               own cleanup unmarks everything this hash's chain still
               has marked, from every attempt, not just its own. */
            continue;
        }
        if (tmp < 0) {
            md_finder_cleanup(&finder);
            return -1;
        }

        md_finder_cleanup(&finder);
        if (!found) {
            if (_md_add_with_hash(md, hash, identity, key, value) < 0) {
                return -1;
            }
            return 0;
        } else {
            md->version = NEXT_VERSION(md->state);
            return 0;
        }
    }
}

static inline int
md_replace(MultiDictObject* md, PyObject* key, PyObject* value)
{
    PyObject* identity = md_calc_identity(md, key);
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
_md_update(MultiDictObject* md, Py_hash_t hash, PyObject* identity,
           PyObject* key, PyObject* value)
{
    bool found = false;

    /* See _md_replace()'s comment for why this can loop: a decref
       below can transiently suspend the critical section and let a
       concurrent resize replace md->keys out from under this scan. */
    for (;;) {
        htkeysiter_t iter;
        htkeysiter_init(&iter, md->keys, hash);
        bool stale = false;

        for (; iter.index != DKIX_EMPTY; htkeysiter_next(&iter)) {
            if (iter.index < 0) {
                continue;
            }
#ifdef Py_GIL_DISABLED
            htkeys_t* keys_before = md->keys;
            uint64_t version_before = md->version;
#endif
            entry_t* entries = htkeys_entries(md->keys);
            entry_t* entry = entries + iter.index;
            if (hash != entry->hash) {
                continue;
            }
            if (_str_cmp(identity, entry->identity)) {
                if (!found) {
                    found = true;
                    if (entry->key == NULL) {
                        /* entry->key could be NULL if it was deleted
                           by the previous _md_update call during the iteration
                           in md_update_from* functions. */
                        assert(entry->value == NULL);
                        entry->key = Py_NewRef(key);
#ifdef Py_GIL_DISABLED
                        _md_entry_publish_value(entry, Py_NewRef(value));
                        _md_entry_store_hash(entry, hash | MD_HASH_MARK);
#else
                        entry->value = Py_NewRef(value);
                        entry->hash = hash | MD_HASH_MARK;
#endif
                    } else {
#ifdef Py_GIL_DISABLED
                        /* Defer the old key/value's decref until entry
                           is already in its final form -- see
                           _md_replace()'s comment on the identical
                           pattern above. */
                        PyObject* old_key = entry->key;
                        PyObject* old_value = _md_entry_load_value(entry);
                        entry->key = Py_NewRef(key);
                        _md_entry_publish_value(entry, Py_NewRef(value));
                        _md_entry_store_hash(entry, hash | MD_HASH_MARK);
                        Py_DECREF(old_key);
                        Py_DECREF(old_value);
#else
                        Py_SETREF(entry->key, Py_NewRef(key));
                        Py_SETREF(entry->value, Py_NewRef(value));
                        entry->hash = hash | MD_HASH_MARK;
#endif
                    }
                } else {
                    _md_del_at_for_upd(md, iter.slot, entry);
                }
            }
#ifdef Py_GIL_DISABLED
            /* See _md_replace()'s comment on why both the pointer and
               the version are checked. */
            if (md->keys != keys_before || md->version != version_before) {
                stale = true;
                break;
            }
#endif
        }
        if (stale) {
            /* No cleanup to skip here (unlike _md_replace()'s finder):
               md_post_update() unmarks everything once, for the whole
               batch, at the very end -- so the marks this attempt
               already made simply need to survive into the rescan,
               which they do since nothing above touches them. */
            continue;
        }
        break;
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
_md_merge(MultiDictObject* md, Py_hash_t hash, PyObject* identity,
          PyObject* key, PyObject* value)
{
    htkeysiter_t iter;
    htkeysiter_init(&iter, md->keys, hash);
    entry_t* entries = htkeys_entries(md->keys);

    for (; iter.index != DKIX_EMPTY; htkeysiter_next(&iter)) {
        if (iter.index < 0) {
            continue;
        }
        entry_t* entry = entries + iter.index;
        if (hash != entry->hash) {
            continue;
        }
        if (_str_cmp(identity, entry->identity)) {
            return 0;
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
md_post_update(MultiDictObject* md)
{
    /* See _md_replace()'s comment for why this can loop: a decref
       below can transiently suspend the critical section and let a
       concurrent resize replace md->keys mid-sweep. Restarting from
       slot 0 against the current table is safe either way: an entry
       this function already finished (identity cleared) is exactly
       the kind _md_resize()'s copy already drops, so it simply isn't
       there to revisit; anything not yet finished still has its
       original identity and is copied over unchanged. */
    for (;;) {
        htkeys_t* keys = md->keys;
#ifdef Py_GIL_DISABLED
        uint64_t version_before = md->version;
#endif
        size_t num_slots = htkeys_nslots(keys);
        entry_t* entries = htkeys_entries(keys);
        bool stale = false;
        for (size_t slot = 0; slot < num_slots; slot++) {
            Py_ssize_t index = htkeys_get_index(keys, slot);
            if (index >= 0) {
                entry_t* entry = entries + index;
                if (entry->key == NULL) {
                    /* the entry is marked for deletion during .update() call
                       and not replaced with a new value */
#ifdef Py_GIL_DISABLED
                    PyObject* old_identity = _md_entry_load_identity(entry);
                    atomic_store_ptr((void**)&entry->identity, NULL);
                    htkeys_set_index(keys, slot, DKIX_DUMMY);
                    _md_add_used(md, -1);
                    Py_XDECREF(old_identity);
                    /* See _md_replace()'s comment on why both the
                       pointer and the version are checked. */
                    if (md->keys != keys || md->version != version_before) {
                        stale = true;
                        break;
                    }
#else
                    Py_CLEAR(entry->identity);
                    htkeys_set_index(keys, slot, DKIX_DUMMY);
                    md->used -= 1;
#endif
                }
                if (entry->hash < 0) {
#ifdef Py_GIL_DISABLED
                    _md_entry_store_hash(entry, entry->hash & PY_SSIZE_T_MAX);
#else
                    entry->hash &= PY_SSIZE_T_MAX;
#endif
                }
            }
        }
        if (!stale) {
            break;
        }
    }
    md->version = NEXT_VERSION(md->state);
    ASSERT_CONSISTENT(md, false);
}

static inline int
md_update_from_ht(MultiDictObject* md, MultiDictObject* other, UpdateOp op)
{
    Py_ssize_t pos;
    Py_hash_t hash;
    PyObject* identity = NULL;
    PyObject* key = NULL;
    bool recalc_identity = md->is_ci != other->is_ci;

    if (other->used == 0) {
        return 0;
    }

    if (md == other && op != Extend) {
        /* update(self) and merge(self) leave the dict unchanged: every key
           already maps to its own values.  Short-circuit -- doing the work in
           place would soft-delete and reinsert the very entries we iterate. */
        return 0;
    }

    /* Pre-allocate room for other's items so the inserts below cannot trigger
       a resize of md->keys.  This is what makes extend(self) (md IS other,
       e.g. ``d.extend(d)``) safe: a resize would free the very entries array
       we iterate here, a use-after-free.  Reserving up front also lets us
       snapshot the entry count so self-extension does not reprocess the
       entries it just appended. */
    if (md_reserve(md, other->used) < 0) {
        return -1;
    }

    entry_t* entries = htkeys_entries(other->keys);
    Py_ssize_t nentries = other->keys->nentries;

    for (pos = 0; pos < nentries; pos++) {
        entry_t* entry = entries + pos;
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
            key = _md_calc_key(other, entry->key, identity);
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
md_extend_self(MultiDictObject* md)
{
    if (md_reserve(md, md->keys->nentries) < 0) {
        return -1;
    }

    Py_ssize_t nentries = md->keys->nentries;
    entry_t* entries = htkeys_entries(md->keys);
    for (Py_ssize_t pos = 0; pos < nentries; pos++) {
        entry_t* entry = entries + pos;
        if (entry->identity != NULL) {
            if (_md_add_with_hash(md,
                                  entry->hash,
                                  entry->identity,
                                  entry->key,
                                  entry->value) < 0) {
                return -1;
            }
        }
    }
    return 0;
}

static inline int
md_update_from_dict(MultiDictObject* md, PyObject* kwds, UpdateOp op)
{
    Py_ssize_t pos = 0;
    PyObject* identity = NULL;
    PyObject* key = NULL;
    PyObject* value = NULL;

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

static inline int
md_update_from_kwnames(MultiDictObject* md, PyObject* const* args,
                       Py_ssize_t nargs, PyObject* kwnames)
{
    Py_ssize_t nkwargs = PyTuple_GET_SIZE(kwnames);
    if (md_reserve(md, nkwargs) < 0) {
        return -1;
    }
    for (Py_ssize_t i = 0; i < nkwargs; i++) {
        PyObject* key = PyTuple_GET_ITEM(kwnames, i);  // borrowed
        assert(PyUnicode_Check(key));
        Py_INCREF(key);
        PyObject* identity = md_calc_identity(md, key);
        if (identity == NULL) {
            Py_DECREF(key);
            return -1;
        }
        Py_hash_t hash = _unicode_hash(identity);
        if (hash == -1) {
            Py_DECREF(identity);
            Py_DECREF(key);
            return -1;
        }
        PyObject* value = args[nargs + i];  // borrowed
        if (_md_add_with_hash_steal_refs(
                md, hash, identity, key, Py_NewRef(value)) < 0) {
            Py_DECREF(value);
            Py_DECREF(identity);
            Py_DECREF(key);
            return -1;
        }
    }
    return 0;
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
_err_cannot_fetch(Py_ssize_t i, const char* name)
{
    PyErr_Format(PyExc_ValueError,
                 "multidict update sequence element #%zd's "
                 "%s could not be fetched",
                 i,
                 name);
}

static int
_md_parse_item(Py_ssize_t i, PyObject* item, PyObject** pkey,
               PyObject** pvalue)
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
        if (*pkey == NULL) {
            _err_cannot_fetch(i, "key");
            goto fail;
        }
        *pvalue = PySequence_ITEM(item, 1);
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
md_update_from_seq(MultiDictObject* md, PyObject* seq, UpdateOp op)
{
    PyObject* it = NULL;
    PyObject* item = NULL;  // seq[i]

    PyObject* key = NULL;
    PyObject* value = NULL;
    PyObject* identity = NULL;
    PyObject* items = NULL;

    Py_ssize_t i;
    Py_ssize_t size = -1;

    enum { LIST, TUPLE, ITER } kind;

    if (!PyList_CheckExact(seq) && !PyTuple_CheckExact(seq)) {
        items = PyMapping_Items(seq);
        if (items != NULL) {
            seq = items;
        } else {
            if (!PyErr_ExceptionMatches(PyExc_AttributeError) &&
                !PyErr_ExceptionMatches(PyExc_TypeError)) {
                // propagate MemoryError / KeyboardInterrupt / etc.
                goto fail;
            }
            // seq is not a mapping; fall back to treating it as a sequence
            PyErr_Clear();
        }
    }

    if (PyList_CheckExact(seq)) {
        kind = LIST;
        size = PyList_GET_SIZE(seq);
        if (size == 0) {
            goto exit;
        }
    } else if (PyTuple_CheckExact(seq)) {
        kind = TUPLE;
        size = PyTuple_GET_SIZE(seq);
        if (size == 0) {
            goto exit;
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
                /* Re-read the length every iteration.  Building the identity
                   below can run arbitrary Python (a str-subclass key's
                   .lower(), an __eq__), which may shrink seq; a stale cached
                   size would let PyList_GET_ITEM read past the end. */
                if (i >= PyList_GET_SIZE(seq)) {
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
            case ITER: {
                int res = PyIter_NextItem(it, &item);
                if (res < 0) {
                    goto fail;
                }
                if (res == 0) {
                    goto exit;
                }
                break;
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
    Py_CLEAR(items);
    return 0;

fail:
    Py_CLEAR(identity);
    Py_CLEAR(it);
    Py_CLEAR(item);
    Py_CLEAR(key);
    Py_CLEAR(value);
    Py_CLEAR(items);
    return -1;
}

static inline int
md_eq(MultiDictObject* md, MultiDictObject* other)
{
    if (md == other) {
        return 1;
    }

    if (md_len(md) != md_len(other)) {
        return 0;
    }

    Py_ssize_t pos1 = 0;
    Py_ssize_t pos2 = 0;

    entry_t* lft_entries = htkeys_entries(md->keys);
    entry_t* rht_entries = htkeys_entries(other->keys);
    for (;;) {
        if (pos1 >= md->keys->nentries || pos2 >= other->keys->nentries) {
            return 1;
        }
        entry_t* entry1 = lft_entries + pos1;
        if (entry1->identity == NULL) {
            pos1++;
            continue;
        }
        entry_t* entry2 = rht_entries + pos2;
        if (entry2->identity == NULL) {
            pos2++;
            continue;
        }

        if (entry1->hash != entry2->hash) {
            return 0;
        }

        if (!_str_cmp(entry1->identity, entry2->identity)) {
            return 0;
        }

        int cmp =
            PyObject_RichCompareBool(entry1->value, entry2->value, Py_EQ);
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
md_eq_to_mapping(MultiDictObject* md, PyObject* other)
{
    PyObject* key = NULL;
    PyObject* avalue = NULL;
    PyObject* bvalue;

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

static inline PyObject*
md_repr(MultiDictObject* md, PyObject* obj, bool show_keys, bool show_values)
{
    int reprenter = Py_ReprEnter(obj);
    if (reprenter != 0) {
        return reprenter > 0 ? PyUnicode_FromString("...") : NULL;
    }

    PyObject* name =
        PyObject_GetAttr((PyObject*)Py_TYPE(obj), md->state->str_name);
    if (name == NULL) {
        Py_ReprLeave(obj);
        return NULL;
    }

    PyObject* key = NULL;
    PyObject* value = NULL;

    bool comma = false;
    uint64_t version = md->version;

    PyUnicodeWriter* writer = PyUnicodeWriter_Create(1024);
    if (writer == NULL) {
        Py_CLEAR(name);
        Py_ReprLeave(obj);
        return NULL;
    }

    if (PyUnicodeWriter_WriteChar(writer, '<') < 0) {
        goto fail;
    }
    if (PyUnicodeWriter_WriteStr(writer, name) < 0) {
        goto fail;
    }
    if (PyUnicodeWriter_WriteChar(writer, '(') < 0) {
        goto fail;
    }

    entry_t* entries = htkeys_entries(md->keys);

    for (Py_ssize_t pos = 0; pos < md->keys->nentries; ++pos) {
        if (version != md->version) {
            PyErr_SetString(PyExc_RuntimeError,
                            "MultiDict changed during iteration");
            goto fail;  // discard the writer instead of leaking it
        }
        entry_t* entry = entries + pos;
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
            /* Fast path: ASCII keys without characters that would be escaped
             * by repr() can be wrapped in single quotes directly. Falls back
             * to PyUnicodeWriter_WriteRepr for keys containing quotes,
             * backslashes, or non-printable characters so the output stays
             * a valid Python string literal. */
            int fast = 0;
            if (PyUnicode_IS_ASCII(key)) {
                Py_ssize_t klen = PyUnicode_GET_LENGTH(key);
                const unsigned char* kdata =
                    (const unsigned char*)PyUnicode_DATA(key);
                fast = 1;
                for (Py_ssize_t ki = 0; ki < klen; ++ki) {
                    unsigned char c = kdata[ki];
                    if (c < 0x20 || c == 0x7f || c == '\'' || c == '\\') {
                        fast = 0;
                        break;
                    }
                }
            }
            if (fast) {
                if (PyUnicodeWriter_WriteChar(writer, '\'') < 0) {
                    goto fail;
                }
                if (PyUnicodeWriter_WriteStr(writer, key) < 0) {
                    goto fail;
                }
                if (PyUnicodeWriter_WriteChar(writer, '\'') < 0) {
                    goto fail;
                }
            } else {
                if (PyUnicodeWriter_WriteRepr(writer, key) < 0) {
                    goto fail;
                }
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
    Py_CLEAR(name);
    Py_ReprLeave(obj);
    return PyUnicodeWriter_Finish(writer);
fail:
    Py_CLEAR(key);
    Py_CLEAR(value);
    Py_CLEAR(name);
    PyUnicodeWriter_Discard(writer);
    Py_ReprLeave(obj);
    return NULL;
}

/***********************************************************************/

static inline int
md_traverse(MultiDictObject* md, visitproc visit, void* arg)
{
    if (md->used == 0) {
        return 0;
    }

    entry_t* entries = htkeys_entries(md->keys);
    for (Py_ssize_t pos = 0; pos < md->keys->nentries; pos++) {
        entry_t* entry = entries + pos;
        if (entry->identity != NULL) {
            Py_VISIT(entry->key);
            Py_VISIT(entry->value);
        }
    }

    return 0;
}

static inline int
md_clear(MultiDictObject* md)
{
    if (md->keys == NULL || md->keys == &empty_htkeys) {
        return 0;
    }
    md->version = NEXT_VERSION(md->state);

    // Publish the empty table before releasing any entry's reference: a
    // decref below may run arbitrary Python code (a __del__), which can
    // suspend this critical section. If md->keys still pointed at the old
    // table while that happens, a concurrent, correctly-locked reader
    // could observe entries mid-clear (identity already NULL, key/value
    // not yet). Swapping first means a suspended thread only ever sees
    // either the fully-populated old table or the fully-empty one.
    htkeys_t* old_keys = md->keys;
#ifdef Py_GIL_DISABLED
    _md_store_used(md, 0);
    _md_store_keys(md, (htkeys_t*)&empty_htkeys);
#else
    md->used = 0;
    md->keys = (htkeys_t*)&empty_htkeys;
#endif

#ifdef Py_GIL_DISABLED
    _md_retire(md, old_keys);
#else
    entry_t* entries = htkeys_entries(old_keys);
    Py_ssize_t nentries = old_keys->nentries;
    for (Py_ssize_t pos = 0; pos < nentries; pos++) {
        entry_t* entry = entries + pos;
        if (entry->identity != NULL) {
            Py_CLEAR(entry->identity);
            Py_CLEAR(entry->key);
            Py_CLEAR(entry->value);
        }
    }
    htkeys_free(old_keys);
#endif
    ASSERT_CONSISTENT(md, false);
    return 0;
}

#if 0

static inline int
_md_check_consistency(MultiDictObject* md, bool update)
{
    //    ASSERT_WORLD_STOPPED_OR_DICT_LOCKED(op);

#define CHECK(expr) assert(expr)
    //    do { if (!(expr)) { assert(0 && Py_STRINGIFY(expr)); } } while (0)

    htkeys_t* keys = md->keys;
    CHECK(keys != NULL);
    Py_ssize_t calc_usable = USABLE_FRACTION(htkeys_nslots(keys));

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

    entry_t* entries = htkeys_entries(keys);
    for (Py_ssize_t i = 0; i < calc_usable; i++) {
        entry_t* entry = &entries[i];
        PyObject* identity = entry->identity;

        if (identity != NULL) {
#ifdef Py_GIL_DISABLED
            /* `update` describes only this call's own operation, not
               whether some entirely different, concurrently-suspended
               thread's _md_replace()/_md_update() (on some other key)
               currently has an entry of its own marked (hash < 0) or
               half-deleted (key == NULL, identity kept) pending that
               thread's own cleanup -- critical section suspension
               means that can be true regardless of what this call's
               update flag says. So always use the tolerant checks
               here; the strict !update ones remain meaningful only
               where nothing else can be concurrently mid-operation,
               i.e. the GIL build below. */
            if (entry->key == NULL) {
                CHECK(entry->value == NULL);
            } else {
                CHECK(entry->value != NULL);
            }
#else
            if (!update) {
                CHECK(entry->hash >= 0);
                CHECK(entry->key != NULL);
                CHECK(entry->value != NULL);
            } else {
                if (entry->key == NULL) {
                    CHECK(entry->value == NULL);
                } else {
                    CHECK(entry->value != NULL);
                }
            }
#endif

            CHECK(PyUnicode_CheckExact(identity));
            if (entry->hash >= 0) {
                Py_hash_t hash = _unicode_hash(identity);
                CHECK(entry->hash == hash);
            }
        }
    }
    return 1;

#undef CHECK
}

static inline int
_md_dump(MultiDictObject* md)
{
    htkeys_t* keys = md->keys;
    printf("Dump %p [%zd from %zd usable %zd nentries %zd]\n",
           (void*)md,
           md->used,
           htkeys_nslots(keys),
           keys->usable,
           keys->nentries);
    for (Py_ssize_t i = 0; i < htkeys_nslots(keys); i++) {
        Py_ssize_t ix = htkeys_get_index(keys, i);
        printf("  %zd -> %zd\n", i, ix);
    }
    printf("  --------\n");
    entry_t* entries = htkeys_entries(keys);
    for (Py_ssize_t i = 0; i < keys->nentries; i++) {
        entry_t* entry = &entries[i];
        PyObject* identity = entry->identity;

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
