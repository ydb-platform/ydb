/* Ordered Dictionary object implementation using a hash table and a vector of
   pointers to the items.
*/
/*

  This file has been directly derived from and retains many algorithms from
  objectdict.c in the Python 2.5.1 source distribution. Its licensing therefore
  is governed by the license as distributed with Python 2.5.1 available in the
  file LICNESE in the source distribution of ordereddict

  Copyright (c) 2001, 2002, 2003, 2004, 2005, 2006, 2007  Python Software
  Foundation; All Rights Reserved"

  Copyrigh (c) 2007-10-13 onwards: Anthon van der Neut
*/

/*
Ordering by key insertion order (KIO) instead of key/val insertion order
(KVIO) is less expensive  (as the list of keys does not have to be updated).
*/

#include "Python.h"
#include "ordereddict.h"

#if PY_VERSION_HEX < 0x03000000
#define PyUNISTR_Object       PyStringObject
#define PyUNISTR_Concat       PyString_Concat
#define PyUNISTR_ConcatAndDel PyString_ConcatAndDel
#define PyUNISTR_CheckExact   PyString_CheckExact
#define PyUNISTR_FromString   PyString_FromString
#define PyUNISTR_FromFormat   PyString_FromFormat
#define PyUNISTR_Join         _PyString_Join
#define PyUNISTR_Eq           _PyString_Eq
#define Py_hash_t             long
#define Py_hash_ssize_t       Py_ssize_t
#define OB_HASH               ob_shash
#else

/* Return 1 if two unicode objects are equal, 0 if not.
 * unicode_eq() is called when the hash of two unicode objects is equal.
 */
#if PY_VERSION_HEX < 0x03030000
Py_LOCAL_INLINE(int)
unicode_eq(PyObject *aa, PyObject *bb)
{
    register PyUnicodeObject *a = (PyUnicodeObject *)aa;
    register PyUnicodeObject *b = (PyUnicodeObject *)bb;

    if (a->length != b->length)
        return 0;
    if (a->length == 0)
        return 1;
    if (a->str[0] != b->str[0])
        return 0;
    if (a->length == 1)
        return 1;
    return memcmp(a->str, b->str, a->length * sizeof(Py_UNICODE)) == 0;
}
#else
Py_LOCAL_INLINE(int)
unicode_eq(PyObject *aa, PyObject *bb)
{
    register PyUnicodeObject *a = (PyUnicodeObject *)aa;
    register PyUnicodeObject *b = (PyUnicodeObject *)bb;

    if (PyUnicode_READY(a) == -1 || PyUnicode_READY(b) == -1) {
        assert(0 && "unicode_eq ready fail");
        return 0;
    }

    if (PyUnicode_GET_LENGTH(a) != PyUnicode_GET_LENGTH(b))
        return 0;
    if (PyUnicode_GET_LENGTH(a) == 0)
        return 1;
    if (PyUnicode_KIND(a) != PyUnicode_KIND(b))
        return 0;
    return memcmp(PyUnicode_1BYTE_DATA(a), PyUnicode_1BYTE_DATA(b),
                  PyUnicode_GET_LENGTH(a) * PyUnicode_KIND(a)) == 0;
}
#endif

#if PY_VERSION_HEX < 0x03030000
#define PyUNISTR_Object       PyUnicodeObject
#else
#define PyUNISTR_Object       PyASCIIObject
#endif
#define PyUNISTR_Concat       PyUnicode_Append
#define PyUNISTR_ConcatAndDel PyUnicode_AppendAndDel
#define PyUNISTR_CheckExact   PyUnicode_CheckExact
#define PyUNISTR_FromString   PyUnicode_FromString
#define PyUNISTR_FromFormat   PyUnicode_FromFormat
#define PyUNISTR_Join         PyUnicode_Join
#define PyUNISTR_Eq           unicode_eq
#define Py_hash_ssize_t       Py_hash_t
#define OB_HASH               hash
#endif

#if PY_VERSION_HEX < 0x02050000
#define SPR "%d"
#else
#define SPR "%ld"
#endif

#if PY_VERSION_HEX < 0x02080000
#define Py_TYPE(ob)            (((PyObject*)(ob))->ob_type)
#endif

#ifdef NDEBUG
#undef NDEBUG
#endif

#define DEFERRED_ADDRESS(ADDR) 0

/* Set a key error with the specified argument, wrapping it in a
 * tuple automatically so that tuple keys are not unpacked as the
 * exception arguments. */
static void
set_key_error(PyObject *arg)
{
    PyObject *tup;
    tup = PyTuple_Pack(1, arg);
    if (!tup)
        return; /* caller will expect error to be set anyway */
    PyErr_SetObject(PyExc_KeyError, tup);
    Py_DECREF(tup);
}

/* Define this out if you don't want conversion statistics on exit. */
#undef SHOW_CONVERSION_COUNTS

/* See large comment block below.  This must be >= 1. */
#define PERTURB_SHIFT 5

/*
see object/dictobject.c for subtilities of the base dict implementation
*/

/* Object used as dummy key to fill deleted entries */
static PyObject *dummy = NULL; /* Initialized by first call to newPyDictObject() */

#ifdef Py_REF_DEBUG
PyObject *
_PyOrderedDict_Dummy(void)
{
    return dummy;
}
#endif

/* relaxed: allow init etc. of ordereddict from dicts if true */
static int ordereddict_relaxed = 0;
/* Key Value Insertion Order: rearrange at end on update if true */
static int ordereddict_kvio = 0;

/* forward declarations */
static PyOrderedDictEntry *
lookdict_string(PyOrderedDictObject *mp, PyObject *key, Py_hash_t hash);
int PyOrderedDict_CopySome(PyObject *a, PyObject *b,
                           Py_ssize_t start, Py_ssize_t step,
                           Py_ssize_t count, int override);

#ifdef SHOW_CONVERSION_COUNTS
static long created = 0L;
static long converted = 0L;

static void
show_counts(void)
{
    fprintf(stderr, "created %ld string dicts\n", created);
    fprintf(stderr, "converted %ld to normal dicts\n", converted);
    fprintf(stderr, "%.2f%% conversion rate\n", (100.0*converted)/created);
}
#endif

/* Debug statistic to compare allocations with reuse through the free list */
#undef SHOW_ALLOC_COUNT
#ifdef SHOW_ALLOC_COUNT
static size_t count_alloc = 0;
static size_t count_reuse = 0;

static void
show_alloc(void)
{
    fprintf(stderr, "Dict allocations: %" PY_FORMAT_SIZE_T "d\n",
        count_alloc);
    fprintf(stderr, "Dict reuse through freelist: %" PY_FORMAT_SIZE_T
        "d\n", count_reuse);
    fprintf(stderr, "%.2f%% reuse rate\n\n",
        (100.0*count_reuse/(count_alloc+count_reuse)));
}
#endif

/* Initialization macros.
   There are two ways to create a dict:  PyOrderedDict_New() is the main C API
   function, and the tp_new slot maps to dict_new().  In the latter case we
   can save a little time over what PyOrderedDict_New does because it's guaranteed
   that the PyOrderedDictObject struct is already zeroed out.
   Everyone except dict_new() should use EMPTY_TO_MINSIZE (unless they have
   an excellent reason not to).
*/

#define INIT_NONZERO_DICT_SLOTS(mp) do {				\
	(mp)->ma_table = (mp)->ma_smalltable;				\
	(mp)->od_otablep = (mp)->ma_smallotablep;           \
	(mp)->ma_mask = PyOrderedDict_MINSIZE - 1;				\
    } while(0)

#define EMPTY_TO_MINSIZE(mp) do {					\
	memset((mp)->ma_smalltable, 0, sizeof((mp)->ma_smalltable));	\
	memset((mp)->ma_smallotablep, 0, sizeof((mp)->ma_smallotablep));	\
	(mp)->ma_used = (mp)->od_fill = (mp)->od_state = 0;				\
	INIT_NONZERO_DICT_SLOTS(mp);					\
    } while(0)

/* 	(mp)->od_cmp = (mp)->od_key = NULL;				\*/

#define INIT_SORT_FUNCS(SD) do {						\
	SD->sd_cmp = Py_None; Py_INCREF(Py_None);		\
	SD->sd_key = Py_None; Py_INCREF(Py_None);		\
	SD->sd_value = Py_None; Py_INCREF(Py_None);		\
	} while(0)


#define OD_KVIO_BIT		(1<<0)
#define OD_RELAXED_BIT	(1<<1)
#define OD_REVERSE_BIT	(1<<2)

#define KVIO(mp)	(mp->od_state & OD_KVIO_BIT)
#define RELAXED(mp)	(mp->od_state & OD_RELAXED_BIT)
#define REVERSE(mp)	(mp->od_state & OD_REVERSE_BIT)

/* Dictionary reuse scheme to save calls to malloc, free, and memset */
#ifndef PyDict_MAXFREELIST
#define PyDict_MAXFREELIST 80
#endif
static PyOrderedDictObject *free_list[PyDict_MAXFREELIST];
static int numfree = 0;

void
PyOrderedDict_Fini(void)
{
    PyOrderedDictObject *op;

    while (numfree) {
        op = free_list[--numfree];
        assert(PyOrderedDict_CheckExact(op));
        PyObject_GC_Del(op);
    }
}

PyObject *
PyOrderedDict_New(void)
{
    register PyOrderedDictObject *mp;
    assert(dummy != NULL);   /* initialisation in the module init */
#ifdef SHOW_CONVERSION_COUNTS
    Py_AtExit(show_counts);
#endif
#ifdef SHOW_ALLOC_COUNT
    Py_AtExit(show_alloc);
#endif
    if (numfree) {
        mp = free_list[--numfree];
        assert (mp != NULL);
        assert (Py_TYPE(mp) == &PyOrderedDict_Type);
        _Py_NewReference((PyObject *)mp);
        if (mp->od_fill) {
            EMPTY_TO_MINSIZE(mp);
        } else {
	    /* At least set ma_table and ma_mask; these are wrong
	       if an empty but presized dict is added to freelist */
	    INIT_NONZERO_DICT_SLOTS(mp);
        }
        assert (mp->ma_used == 0);
        assert (mp->ma_table == mp->ma_smalltable);
        assert (mp->od_otablep == mp->ma_smallotablep);
        assert (mp->ma_mask == PyOrderedDict_MINSIZE - 1);
#ifdef SHOW_ALLOC_COUNT
        count_reuse++;
#endif
    } else {
        mp = PyObject_GC_New(PyOrderedDictObject, &PyOrderedDict_Type);
        if (mp == NULL)
            return NULL;
        EMPTY_TO_MINSIZE(mp);
#ifdef SHOW_ALLOC_COUNT
        count_alloc++;
#endif
    }
    mp->ma_lookup = lookdict_string;
#ifdef SHOW_CONVERSION_COUNTS
    ++created;
#endif
    PyObject_GC_Track(mp);
    return (PyObject *)mp;
}


PyObject *
PySortedDict_New(void)
{
    register PyOrderedDictObject *mp;
    register PySortedDictObject *sd;
    assert(dummy != NULL);
    mp = (PyOrderedDictObject *) PyObject_GC_New(PySortedDictObject, &PySortedDict_Type);
    if (mp == NULL)
        return NULL;
    EMPTY_TO_MINSIZE(mp);
    mp->ma_lookup = lookdict_string;
    sd = (PySortedDictObject*)mp;
    INIT_SORT_FUNCS(sd);
#ifdef SHOW_CONVERSION_COUNTS
    ++created;
#endif
    PyObject_GC_Track(mp);
    return (PyObject *)mp;
}

/*
The basic lookup function used by all operations.
This is based on Algorithm D from Knuth Vol. 3, Sec. 6.4.
Open addressing is preferred over chaining since the link overhead for
chaining would be substantial (100% with typical malloc overhead).

The initial probe index is computed as hash mod the table size. Subsequent
probe indices are computed as explained earlier.

All arithmetic on hash should ignore overflow.

(The details in this version are due to Tim Peters, building on many past
contributions by Reimer Behrends, Jyrki Alakuijala, Vladimir Marangozov and
Christian Tismer).

lookdict() is general-purpose, and may return NULL if (and only if) a
comparison raises an exception (this was new in Python 2.5).
lookdict_string() below is specialized to string keys, comparison of which can
never raise an exception; that function can never return NULL.  For both, when
the key isn't found a PyOrderedDictEntry* is returned for which the me_value field is
NULL; this is the slot in the dict at which the key would have been found, and
the caller can (if it wishes) add the <key, value> pair to the returned
PyOrderedDictEntry *.
*/
static PyOrderedDictEntry *
lookdict(PyOrderedDictObject *mp, PyObject *key, register Py_hash_t hash)
{
    register size_t i;
    register size_t perturb;
    register PyOrderedDictEntry *freeslot;
    register size_t mask = (size_t)mp->ma_mask;
    PyOrderedDictEntry *ep0 = mp->ma_table;
    register PyOrderedDictEntry *ep;
    register int cmp;
    PyObject *startkey;

    i = (size_t)hash & mask;
    ep = &ep0[i];
    if (ep->me_key == NULL || ep->me_key == key)
        return ep;

    if (ep->me_key == dummy)
        freeslot = ep;
    else {
        if (ep->me_hash == hash) {
            startkey = ep->me_key;
	    Py_INCREF(startkey);
            cmp = PyObject_RichCompareBool(startkey, key, Py_EQ);
	    Py_DECREF(startkey);
            if (cmp < 0)
                return NULL;
            if (ep0 == mp->ma_table && ep->me_key == startkey) {
                if (cmp > 0)
                    return ep;
            } else {
                /* The compare did major nasty stuff to the
                 * dict:  start over.
                 * XXX A clever adversary could prevent this
                 * XXX from terminating.
                 */
                return lookdict(mp, key, hash);
            }
        }
        freeslot = NULL;
    }

    /* In the loop, me_key == dummy is by far (factor of 100s) the
       least likely outcome, so test for that last. */
    for (perturb = hash; ; perturb >>= PERTURB_SHIFT) {
        i = (i << 2) + i + perturb + 1;
        ep = &ep0[i & mask];
        if (ep->me_key == NULL)
            return freeslot == NULL ? ep : freeslot;
        if (ep->me_key == key)
            return ep;
        if (ep->me_hash == hash && ep->me_key != dummy) {
            startkey = ep->me_key;
	    Py_INCREF(startkey);
            cmp = PyObject_RichCompareBool(startkey, key, Py_EQ);
	    Py_DECREF(startkey);
            if (cmp < 0)
                return NULL;
            if (ep0 == mp->ma_table && ep->me_key == startkey) {
                if (cmp > 0)
                    return ep;
            } else {
                /* The compare did major nasty stuff to the
                 * dict:  start over.
                 * XXX A clever adversary could prevent this
                 * XXX from terminating.
                 */
                return lookdict(mp, key, hash);
            }
        } else if (ep->me_key == dummy && freeslot == NULL)
            freeslot = ep;
    }
    assert(0);	/* NOT REACHED */
    return 0;
}

/*
 * Hacked up version of lookdict which can assume keys are always strings;
 * this assumption allows testing for errors during PyObject_RichCompareBool()
 * to be dropped; string-string comparisons never raise exceptions.  This also
 * means we don't need to go through PyObject_RichCompareBool(); we can always
 * use PyUNISTR_Eq() directly.
 *
 * This is valuable because dicts with only string keys are very common.
 */
static PyOrderedDictEntry *
lookdict_string(PyOrderedDictObject *mp, PyObject *key, register Py_hash_t hash)
{
    register size_t i;
    register size_t perturb;
    register PyOrderedDictEntry *freeslot;
    register size_t mask = (size_t)mp->ma_mask;
    PyOrderedDictEntry *ep0 = mp->ma_table;
    register PyOrderedDictEntry *ep;

    /* Make sure this function doesn't have to handle non-string keys,
       including subclasses of str; e.g., one reason to subclass
       strings is to override __eq__, and for speed we don't cater to
       that here. */
    if (!PyUNISTR_CheckExact(key)) {
#ifdef SHOW_CONVERSION_COUNTS
        ++converted;
#endif
        mp->ma_lookup = lookdict;
        return lookdict(mp, key, hash);
    }
    i = hash & mask;
    ep = &ep0[i];
    if (ep->me_key == NULL || ep->me_key == key)
        return ep;
    if (ep->me_key == dummy)
        freeslot = ep;
    else {
        if (ep->me_hash == hash && PyUNISTR_Eq(ep->me_key, key))
            return ep;
        freeslot = NULL;
    }

    /* In the loop, me_key == dummy is by far (factor of 100s) the
       least likely outcome, so test for that last. */
    for (perturb = hash; ; perturb >>= PERTURB_SHIFT) {
        i = (i << 2) + i + perturb + 1;
        ep = &ep0[i & mask];
        if (ep->me_key == NULL)
            return freeslot == NULL ? ep : freeslot;
        if (ep->me_key == key
                || (ep->me_hash == hash
                    && ep->me_key != dummy
                    && PyUNISTR_Eq(ep->me_key, key)))
            return ep;
        if (ep->me_key == dummy && freeslot == NULL)
            freeslot = ep;
    }
    assert(0);	/* NOT REACHED */
    return 0;
}

static int
dump_ordereddict_head(register PyOrderedDictObject *mp)
{
    if (mp == NULL) {
        printf("ordereddict header printing received NULL");
        return -1;
    }
    if (PySortedDict_CheckExact(mp))
        printf("sorteddict");
    else
        printf("ordereddict");
    printf(": fill " SPR ", ", mp->od_fill);
    printf("used " SPR ", ", mp->ma_used);
    printf("mask " SPR ", ", mp->ma_mask);
    printf("mask " SPR ", ", mp->ma_mask);
    printf("\nbits: ");
    if (KVIO(mp))
        printf("kvio ");
    if (RELAXED(mp))
        printf("relax ");
    if (REVERSE(mp))
        printf("reverse ");
    printf("\n");
    return 0;
}

static void
dump_sorteddict_fun(register PySortedDictObject *mp)
{
    printf("cmp %p, key %p, value %p\n", mp->sd_cmp, mp->sd_key, mp->sd_value);
}


static void
dump_otablep(register PyOrderedDictObject *mp)
{
    Py_ssize_t index;
    PyOrderedDictEntry **p;
    printf("mp %p\n", mp);
    for (index = 0, p = mp->od_otablep; index < mp->ma_used; index++, p++) {
        printf("index " SPR " %p %p\n", index, p, *p);
    }
}

/*
https://github.com/pbrady/fastcache/issues/32
mentions no tracking with GC_TRACK in extensions
*/

/* #if (PY_VERSION_HEX < 0x02070000) */
#if 1
#define MAINTAIN_TRACKING(mp, key, value)
#define _PyDict_MaybeUntrack(x)
#else
#ifdef SHOW_TRACK_COUNT
#define INCREASE_TRACK_COUNT \
    (count_tracked++, count_untracked--);
#define DECREASE_TRACK_COUNT \
    (count_tracked--, count_untracked++);
#else
#define INCREASE_TRACK_COUNT
#define DECREASE_TRACK_COUNT
#endif

#define MAINTAIN_TRACKING(mp, key, value) \
    do { \
        if (!_PyObject_GC_IS_TRACKED(mp)) { \
            if (_PyObject_GC_MAY_BE_TRACKED(key) || \
                _PyObject_GC_MAY_BE_TRACKED(value)) { \
                _PyObject_GC_TRACK(mp); \
                INCREASE_TRACK_COUNT \
            } \
        } \
    } while(0)

 PyAPI_FUNC(void)
_PyOrderedDict_MaybeUntrack(PyObject *op)
{
    PyDictObject *mp;
    PyObject *value;
    Py_ssize_t mask, i;
    PyDictEntry *ep;

    if (!PyDict_CheckExact(op) || !_PyObject_GC_IS_TRACKED(op))
        return;

    mp = (PyDictObject *) op;
    ep = mp->ma_table;
    mask = mp->ma_mask;
    for (i = 0; i <= mask; i++) {
        if ((value = ep[i].me_value) == NULL)
            continue;
        if (_PyObject_GC_MAY_BE_TRACKED(value) ||
            _PyObject_GC_MAY_BE_TRACKED(ep[i].me_key))
            return;
    }
    DECREASE_TRACK_COUNT
    _PyObject_GC_UNTRACK(op);
}
#endif

/*
Internal routine to insert a new item into the table when you have entry object.
Used by insertdict.
*/
static int
insertdict_by_entry(register PyOrderedDictObject *mp, PyObject *key, Py_hash_t hash,
                    PyOrderedDictEntry *ep, PyObject *value, Py_ssize_t index)
{
    PyObject *old_value;
    Py_ssize_t oindex;
    register PyOrderedDictEntry **epp = NULL;

    MAINTAIN_TRACKING(mp, key, value);
    if (ep->me_value != NULL) { /* updating a value */
        old_value = ep->me_value;
        ep->me_value = value;
        if (index != -1) {
            if (index == -2) /* kvio */
                index = mp->ma_used-1;
            for (oindex = 0, epp = mp->od_otablep; oindex < mp->ma_used;
                    oindex++, epp++)
                if (*epp == ep)
                    break;
            /* epp now points to item and oindex is its index (optimize?) */
            /* if index == oindex we don't have to anything */
            if (index < oindex) {
                epp = mp->od_otablep;
                epp += index;
                memmove(epp + 1, epp, (oindex - index) * sizeof(PyOrderedDictEntry *));
                *epp = ep;
            } else if ((index == oindex + 1) && (index == mp->ma_used)) {
				/* nothing to do for inserting beyond last with same key */
            } else if (index > oindex) {
                /*
                printf("moving %d %d %p\n", index, oindex, epp);
                dump_otablep(mp); */
                memmove(epp, epp + 1, (index - oindex) * sizeof(PyOrderedDictEntry *));
                mp->od_otablep[index] = ep;
                /*
                dump_otablep(mp);
                */
            }
        }
        Py_DECREF(old_value); /* which **CAN** re-enter */
        Py_DECREF(key);
    } else { /* new value */
        if (ep->me_key == NULL)
            mp->od_fill++;
        else {
            assert(ep->me_key == dummy);
            Py_DECREF(dummy);
        }
        ep->me_key = key;
        ep->me_hash = (Py_ssize_t)hash;
        ep->me_value = value;
        if (index < 0)
            mp->od_otablep[mp->ma_used] = ep;
        else {
            epp = mp->od_otablep;
            epp += index;
            /* make space */
            memmove(epp + 1, epp, (mp->ma_used - index) * sizeof(PyOrderedDictEntry *));
            *epp = ep;
        }
        mp->ma_used++;
    }
    return 0;
}

/*
Internal routine to insert a new item into the table.
Used both by the internal resize routine and by the public insert routine.
Eats a reference to key and one to value.
Returns -1 if an error occurred, or 0 on success.
*/
static int
insertdict(register PyOrderedDictObject *mp, PyObject *key, Py_hash_t hash,
           PyObject *value, Py_ssize_t index)
{
    register PyOrderedDictEntry *ep;

    assert(mp->ma_lookup != NULL);
    ep = mp->ma_lookup(mp, key, hash);
    if (ep == NULL) {
        Py_DECREF(key);
        Py_DECREF(value);
        return -1;
    }
    return insertdict_by_entry(mp, key, hash, ep, value, index);
}

/*
Internal routine to insert a new item into the table when you have entry object.
Used by insertdict.
*/
static int
insertsorteddict_by_entry(register PyOrderedDictObject *mp, PyObject *key, Py_hash_t hash,
                    PyOrderedDictEntry *ep, PyObject *value)
{
    PyObject *old_value;
    Py_ssize_t index = 0, lower, upper;
    int res;
    register PySortedDictObject *sd = (PySortedDictObject *) mp;
    register PyOrderedDictEntry **epp = NULL;

    MAINTAIN_TRACKING(mp, key, value);
    if (ep->me_value != NULL) { /* updating a value */
        old_value = ep->me_value;
        ep->me_value = value;
        Py_DECREF(old_value); /* which **CAN** re-enter */
        Py_DECREF(key);
        if (sd->sd_value != Py_None || sd->sd_cmp != Py_None) {
            PyErr_SetString(PyExc_NotImplementedError,
                            "updating a value for a cmp/value sorted dict not implemented"
                           );
            return -1;
        }
    } else { /* new value */
        if (ep->me_key == NULL)
            mp->od_fill++;
        else {
            assert(ep->me_key == dummy);
            Py_DECREF(dummy);
        }
        ep->me_key = key;
        ep->me_hash = (Py_ssize_t)hash;
        ep->me_value = value;
        /* determine epp */
        epp = mp->od_otablep;
        lower = 0;
        upper = mp->ma_used;
        if (sd->sd_key != Py_None && sd->sd_key != Py_True) {
            PyObject *transkey;
            PyObject *chkkey;
            transkey = PyObject_CallFunctionObjArgs(sd->sd_key, key, NULL);
            if (transkey == NULL)
                transkey = key;
            while (lower < upper) {
                index = (lower+upper) / 2;
                chkkey = PyObject_CallFunctionObjArgs(sd->sd_key,(epp[index])->me_key, NULL);
                if (chkkey == NULL)
                    chkkey = (epp[index])->me_key;
                res = PyObject_RichCompareBool(chkkey, transkey, Py_GT);
                if (res == 0)
                    lower = index + 1;
                else if (res == 1)
                    upper = index;
                else
                    return -1; /* res was -1 -> error */
            }
        } else {
            while (lower < upper) {
                index = (lower+upper) / 2;
                res = PyObject_RichCompareBool((epp[index])->me_key, key, Py_GT);
                if (res == 0)
                    lower = index + 1;
                else if (res == 1)
                    upper = index;
                else
                    return -1; /* res was -1 -> error */
            }
        }
        epp += lower;
        /* make space */
        memmove(epp + 1, epp, (mp->ma_used - lower) * sizeof(PyOrderedDictEntry *));
        *epp = ep;
        mp->ma_used++;
    }
    return 0;
}

static int
insertsorteddict(register PyOrderedDictObject *mp, PyObject *key, Py_hash_t hash,
                 PyObject *value)
{
    register PyOrderedDictEntry *ep;

    /* printf("insert sorted dict\n"); */
    assert(mp->ma_lookup != NULL);
    ep = mp->ma_lookup(mp, key, hash);
    if (ep == NULL) {
        Py_DECREF(key);
        Py_DECREF(value);
        return -1;
    }
    return insertsorteddict_by_entry(mp, key, hash, ep, value);
}


/*
Internal routine used by dictresize() to insert an item which is
known to be absent from the dict.  This routine also assumes that
the dict contains no deleted entries.  Besides the performance benefit,
using insertdict() in dictresize() is dangerous (SF bug #1456209).
Note that no refcounts are changed by this routine; if needed, the caller
is responsible for incref'ing `key` and `value`.
*/
static void
insertdict_clean(register PyOrderedDictObject *mp, PyObject *key, Py_hash_t hash,
                 PyObject *value)
{
    register size_t i;
    register size_t perturb;
    register size_t mask = (size_t)mp->ma_mask;
    PyOrderedDictEntry *ep0 = mp->ma_table;
    register PyOrderedDictEntry *ep;

    MAINTAIN_TRACKING(mp, key, value);
    i = hash & mask;
    ep = &ep0[i];
    for (perturb = hash; ep->me_key != NULL; perturb >>= PERTURB_SHIFT) {
        i = (i << 2) + i + perturb + 1;
        ep = &ep0[i & mask];
    }
    assert(ep->me_value == NULL);
    mp->od_fill++;
    ep->me_key = key;
    ep->me_hash = (Py_ssize_t)hash;
    ep->me_value = value;
    mp->od_otablep[mp->ma_used] = ep;
    mp->ma_used++;
}

/*
Restructure the table by allocating a new table and reinserting all
items again.  When entries have been deleted, the new table may
actually be smaller than the old one.
*/
static int
dictresize(PyOrderedDictObject *mp, Py_ssize_t minused)
{
    Py_ssize_t newsize;
    PyOrderedDictEntry *oldtable, *newtable, *ep, **epp;
    PyOrderedDictEntry **oldotablep, **newotablep;
    register Py_ssize_t i, j;
    int is_oldtable_malloced;
    int reusing_smalltable;
    PyOrderedDictEntry small_copy[PyOrderedDict_MINSIZE];
    PyOrderedDictEntry *small_ocopyp[PyOrderedDict_MINSIZE];

    assert(minused >= 0);

    /* Find the smallest table size > minused. */
    for (newsize = PyOrderedDict_MINSIZE;
            newsize <= minused && newsize > 0;
            newsize <<= 1)
        ;
    if (newsize <= 0) {
        PyErr_NoMemory();
        return -1;
    }

    /* Get space for a new table. */
    oldtable = mp->ma_table;
    oldotablep = mp->od_otablep;
    assert(oldtable != NULL);
    assert(oldotablep != NULL);
    is_oldtable_malloced = oldtable != mp->ma_smalltable;

    reusing_smalltable = 0;

    if (newsize == PyOrderedDict_MINSIZE) {
        /* A large table is shrinking, or we can't get any smaller. */
        newtable = mp->ma_smalltable;
        newotablep = mp->ma_smallotablep;
        if (newtable == oldtable) {
            if (mp->od_fill == mp->ma_used) {
                /* No dummies, so no point doing anything. */
                return 0;
            }
            /* We're not going to resize it, but rebuild the
               table anyway to purge old dummy entries.
               Subtle:  This is *necessary* if fill==size,
               as lookdict needs at least one virgin slot to
               terminate failing searches.  If fill < size, it's
               merely desirable, as dummies slow searches. */
            assert(mp->od_fill > mp->ma_used);
            memcpy(small_copy, oldtable, sizeof(small_copy));
            /* Small_ocopyp must point into small_copy */
            for (i = 0; i < PyOrderedDict_MINSIZE; i++) {
                small_ocopyp[i] = oldotablep[i] ? &small_copy[oldotablep[i]-&oldtable[0]]: NULL;
            }
            oldtable = small_copy;
            reusing_smalltable = 1;
        }
    } else {
        newtable = PyMem_NEW(PyOrderedDictEntry, newsize);
        if (newtable == NULL) {
            PyErr_NoMemory();
            return -1;
        }
        newotablep = PyMem_NEW(PyOrderedDictEntry*, newsize);
        if (newotablep == NULL) {
            PyErr_NoMemory();
            return -1;
        }
    }

    /* Make the dict empty, using the new table. */
    assert(newtable != oldtable);
    assert(newotablep != oldotablep);
    mp->ma_table = newtable;
    mp->od_otablep = newotablep;
    mp->ma_mask = newsize - 1;
    memset(newtable, 0, sizeof(PyOrderedDictEntry) * newsize);
    memcpy(newotablep, oldotablep, sizeof(PyOrderedDictEntry *) * mp->ma_used);
    epp = mp->od_otablep;
    j = mp->ma_used;
    mp->ma_used = 0;
    i = mp->od_fill;
    mp->od_fill = 0;

    /* Copy the data over; this is refcount-neutral for active entries;
       dummy entries aren't copied over, of course */

    for (epp = reusing_smalltable ? small_ocopyp: mp->od_otablep; j > 0; epp++, j--) {
        insertdict_clean(mp, (*epp)->me_key, (long)(*epp)->me_hash,
                         (*epp)->me_value);
    }
    for (ep = oldtable; i > 0; ep++) {
        if (ep->me_value != NULL) {	/* active entry */
            --i;
        } else if (ep->me_key != NULL) {	/* dummy entry */
            --i;
            assert(ep->me_key == dummy);
            Py_DECREF(ep->me_key);
        }
        /* else key == value == NULL:  nothing to do */
    }

    if (is_oldtable_malloced) {
        PyMem_DEL(oldtable);
        PyMem_DEL(oldotablep);
    }
    return 0;
}


/* Create a new dictionary pre-sized to hold an estimated number of elements.
   Underestimates are okay because the dictionary will resize as necessary.
   Overestimates just mean the dictionary will be more sparse than usual.
*/

PyAPI_FUNC(PyObject *)
_PyOrderedDict_NewPresized(Py_ssize_t minused)
{
    PyObject *op = PyOrderedDict_New();

    if (minused>5 && op != NULL && dictresize((PyOrderedDictObject *)op, minused) == -1) {
        Py_DECREF(op);
        return NULL;
    }
    return op;
}


/* Note that, for historical reasons, PyOrderedDict_GetItem() suppresses all errors
 * that may occur (originally dicts supported only string keys, and exceptions
 * weren't possible).  So, while the original intent was that a NULL return
 * meant the key wasn't present, in reality it can mean that, or that an error
 * (suppressed) occurred while computing the key's hash, or that some error
 * (suppressed) occurred when comparing keys in the dict's internal probe
 * sequence.  A nasty example of the latter is when a Python-coded comparison
 * function hits a stack-depth error, which can cause this to return NULL
 * even if the key is present.
 */
PyObject *
PyOrderedDict_GetItem(PyObject *op, PyObject *key)
{
    Py_hash_t hash;
    PyOrderedDictObject *mp = (PyOrderedDictObject *)op;
    PyOrderedDictEntry *ep;
    PyThreadState *tstate;

    if (!PyOrderedDict_Check(op))
        return NULL;
    if (!PyUNISTR_CheckExact(key) ||
            (hash = ((PyUNISTR_Object *) key)->OB_HASH) == -1) {
        hash = PyObject_Hash(key);
        if (hash == -1) {
            PyErr_Clear();
            return NULL;
        }
    }

    /* We can arrive here with a NULL tstate during initialization: try
       running "python -Wi" for an example related to string interning.
       Let's just hope that no exception occurs then... This must be
       _PyThreadState_Current and not PyThreadState_GET() because in debug
       mode, the latter complains if tstate is NULL. */
#if PY_VERSION_HEX < 0x03000000
    tstate = _PyThreadState_Current;
#else
    tstate = (PyThreadState*)_Py_atomic_load_relaxed(
        &_PyThreadState_Current);
#endif
    if (tstate != NULL && tstate->curexc_type != NULL) {
        /* preserve the existing exception */
        PyObject *err_type, *err_value, *err_tb;
        PyErr_Fetch(&err_type, &err_value, &err_tb);
        ep = (mp->ma_lookup)(mp, key, hash);
        /* ignore errors */
        PyErr_Restore(err_type, err_value, err_tb);
        if (ep == NULL)
            return NULL;
    } else {
        ep = (mp->ma_lookup)(mp, key, hash);
        if (ep == NULL) {
            PyErr_Clear();
            return NULL;
        }
    }
    return ep->me_value;
}

static int
dict_set_item_by_hash_or_entry(register PyObject *op, PyObject *key,
                               Py_hash_t hash, PyOrderedDictEntry *ep, PyObject *value)
{
    register PyOrderedDictObject *mp;
    register Py_ssize_t n_used;
    mp = (PyOrderedDictObject *)op;
    assert(mp->od_fill <= mp->ma_mask);  /* at least one empty slot */
    n_used = mp->ma_used;
    Py_INCREF(value);
    Py_INCREF(key);
#if PY_MAJOR_VERSION < 3
    if (PySortedDict_Check(op)) {
#else
		if (PySortedDict_CheckExact(op)) {
#endif
        if (insertsorteddict(mp, key, hash, value) != 0)
            return -1;
    } else if (insertdict(mp, key, hash, value, KVIO(mp) ? -2: -1) != 0)
        return -1;
    /* If we added a key, we can safely resize.  Otherwise just return!
     * If fill >= 2/3 size, adjust size.  Normally, this doubles or
     * quaduples the size, but it's also possible for the dict to shrink
     * (if od_fill is much larger than ma_used, meaning a lot of dict
     * keys have been * deleted).
     *
     * Quadrupling the size improves average dictionary sparseness
     * (reducing collisions) at the cost of some memory and iteration
     * speed (which loops over every possible entry).  It also halves
     * the number of expensive resize operations in a growing dictionary.
     *
     * Very large dictionaries (over 50K items) use doubling instead.
     * This may help applications with severe memory constraints.
     */
    if (!(mp->ma_used > n_used && mp->od_fill*3 >= (mp->ma_mask+1)*2))
        return 0;
    return dictresize(mp, (mp->ma_used > 50000 ? 2 : 4) * mp->ma_used);
}

/* CAUTION: PyOrderedDict_SetItem() must guarantee that it won't resize the
 * dictionary if it's merely replacing the value for an existing key.
 * This means that it's safe to loop over a dictionary with PyOrderedDict_Next()
 * and occasionally replace a value -- but you can't insert new keys or
 * remove them.
 * This does never hold for kvio
 */
int
PyOrderedDict_SetItem(register PyObject *op, PyObject *key, PyObject *value)
{
    register Py_hash_t hash;

    if (!PyOrderedDict_Check(op)) {
        PyErr_BadInternalCall();
        return -1;
    }
    assert(key);
    assert(value);
    if (PyUNISTR_CheckExact(key)) {
        hash = ((PyUNISTR_Object *)key)->OB_HASH;
        if (hash == -1)
            hash = PyObject_Hash(key);
    } else {
        hash = PyObject_Hash(key);
        if (hash == -1)
            return -1;
    }
    return dict_set_item_by_hash_or_entry(op, key, hash, NULL, value);
}

int
PyOrderedDict_InsertItem(register PyOrderedDictObject *mp, Py_ssize_t index,
                         PyObject *key, PyObject *value)
{
    register Py_hash_t hash;
    register Py_ssize_t n_used;

#if PY_MAJOR_VERSION < 3
    if (PySortedDict_Check(mp)) {
#else
    if (PySortedDict_CheckExact(mp)) {
#endif
        PyErr_SetString(PyExc_TypeError,
                        "sorteddict does not support insert()");
        return -1;
    }
    if (!PyOrderedDict_Check(mp)) {
        PyErr_BadInternalCall();
        return -1;
    }
    assert(key);
    assert(value);
    if (index < 0)
        index += mp->ma_used;
    /* test to see if index is in range */
    if (index > mp->ma_used)
        index = mp->ma_used;
    else if (index < 0)
        index = 0;
    if (PyUNISTR_CheckExact(key)) {
        hash = ((PyUNISTR_Object *)key)->OB_HASH;
        if (hash == -1)
            hash = PyObject_Hash(key);
    } else {
        hash = PyObject_Hash(key);
        if (hash == -1)
            return -1;
    }
    assert(mp->od_fill <= mp->ma_mask);  /* at least one empty slot */
    n_used = mp->ma_used;
    Py_INCREF(value);
    Py_INCREF(key);
    if (insertdict(mp, key, hash, value, index) != 0)
        return -1;
    /* If we added a key, we can safely resize.  Otherwise just return!
     * If fill >= 2/3 size, adjust size.  Normally, this doubles or
     * quaduples the size, but it's also possible for the dict to shrink
     * (if od_fill is much larger than ma_used, meaning a lot of dict
     * keys have been * deleted).
     *
     * Quadrupling the size improves average dictionary sparseness
     * (reducing collisions) at the cost of some memory and iteration
     * speed (which loops over every possible entry).  It also halves
     * the number of expensive resize operations in a growing dictionary.
     *
     * Very large dictionaries (over 50K items) use doubling instead.
     * This may help applications with severe memory constraints.
     */
    if (!(mp->ma_used > n_used && mp->od_fill*3 >= (mp->ma_mask+1)*2))
        return 0;
    return dictresize(mp, (mp->ma_used > 50000 ? 2 : 4) * mp->ma_used);
}


static int
del_inorder(PyOrderedDictObject *op, PyOrderedDictEntry* ep)
{
    register Py_ssize_t count = op->ma_used;
    PyOrderedDictEntry **tmp = op->od_otablep;
    while (count--) {
        if (*tmp == ep) {
            memmove(tmp, tmp+1, count * sizeof(PyOrderedDictEntry *));
            return 1;
        }
        tmp++;
    }
    return 0; /* not found */
}

int
PyOrderedDict_DelItem(PyObject *op, PyObject *key)
{
    register PyOrderedDictObject *mp;
    register Py_hash_t hash;
    register PyOrderedDictEntry *ep;
    PyObject *old_value, *old_key;

    if (!PyOrderedDict_Check(op)) {
        PyErr_BadInternalCall();
        return -1;
    }
    assert(key);
    if (!PyUNISTR_CheckExact(key) ||
            (hash = ((PyUNISTR_Object *) key)->OB_HASH) == -1) {
        hash = PyObject_Hash(key);
        if (hash == -1)
            return -1;
    }
    mp = (PyOrderedDictObject *)op;
    ep = (mp->ma_lookup)(mp, key, hash);
    /* at this point we have to move all the entries beyond the one found
    back on space (this could be optimised by deferring)  */
    del_inorder(mp, ep);
    if (ep == NULL)
        return -1;
    if (ep->me_value == NULL) {
        set_key_error(key);
        return -1;
    }
    old_key = ep->me_key;
    assert(ep->me_key);
    Py_INCREF(dummy);
    ep->me_key = dummy;
    old_value = ep->me_value;
    ep->me_value = NULL;
    mp->ma_used--;
    Py_DECREF(old_value);
    Py_DECREF(old_key);
    return 0;
}

void
PyOrderedDict_Clear(PyObject *op)
{
    PyOrderedDictObject *mp;
    PyOrderedDictEntry *ep, *table, **otablep;
    int table_is_malloced;
    Py_ssize_t fill;
    PyOrderedDictEntry small_copy[PyOrderedDict_MINSIZE];
#ifdef Py_DEBUG
    Py_ssize_t i, n;
#endif

    if (!PyOrderedDict_Check(op))
        return;
    mp = (PyOrderedDictObject *)op;
#ifdef Py_DEBUG
    n = mp->ma_mask + 1;
    i = 0;
#endif

    table = mp->ma_table;
    otablep = mp->od_otablep;
    assert(table != NULL);
    assert(otablep != NULL);
    table_is_malloced = table != mp->ma_smalltable;

    /* This is delicate.  During the process of clearing the dict,
     * decrefs can cause the dict to mutate.  To avoid fatal confusion
     * (voice of experience), we have to make the dict empty before
     * clearing the slots, and never refer to anything via mp->xxx while
     * clearing.
     */
    fill = mp->od_fill;
    if (table_is_malloced)
        EMPTY_TO_MINSIZE(mp);

    else if (fill > 0) {
        /* It's a small table with something that needs to be cleared.
         * Afraid the only safe way is to copy the dict entries into
         * another small table first.
         */
        memcpy(small_copy, table, sizeof(small_copy));
        table = small_copy;
        EMPTY_TO_MINSIZE(mp);
    }
    /* else it's a small table that's already empty */

    /* Now we can finally clear things.  If C had refcounts, we could
     * assert that the refcount on table is 1 now, i.e. that this function
     * has unique access to it, so decref side-effects can't alter it.
     */
    for (ep = table; fill > 0; ++ep) {
#ifdef Py_DEBUG
        assert(i < n);
        ++i;
#endif
        if (ep->me_key) {
            --fill;
            Py_DECREF(ep->me_key);
            Py_XDECREF(ep->me_value);
        }
#ifdef Py_DEBUG
        else
            assert(ep->me_value == NULL);
#endif
    }

    if (table_is_malloced) {
        PyMem_DEL(table);
        PyMem_DEL(otablep);
    }
}

/*
 * Iterate over a dict.  Use like so:
 *
 *     Py_ssize_t i;
 *     PyObject *key, *value;
 *     i = 0;   # important!  i should not otherwise be changed by you
 *     while (PyOrderedDict_Next(yourdict, &i, &key, &value)) {
 *              Refer to borrowed references in key and value.
 *     }
 *
 * CAUTION:  In general, it isn't safe to use PyOrderedDict_Next in a loop that
 * mutates the dict.  One exception:  it is safe if the loop merely changes
 * the values associated with the keys (but doesn't insert new keys or
 * delete keys), via PyOrderedDict_SetItem().
 */
int
PyOrderedDict_Next(PyObject *op, Py_ssize_t *ppos, PyObject **pkey, PyObject **pvalue)
{
    register Py_ssize_t i;
    register PyOrderedDictEntry **epp;

    if (!PyOrderedDict_Check(op) && !PySortedDict_Check(op))
        return 0;
    i = *ppos;
    if (i < 0)
        return 0;
    /* review: not sure why different from 2.5.1 here. */
    if (i >= ((PyOrderedDictObject *)op)->ma_used)
        return 0;
    *ppos = i+1;
    epp = ((PyOrderedDictObject *)op)->od_otablep;
    if (pkey)
        *pkey = epp[i]->me_key;
    if (pvalue)
        *pvalue = epp[i]->me_value;
    return 1;
}

/* Internal version of PyOrderedDict_Next that returns a hash value in addition to the key and value.*/
int
_PyOrderedDict_Next(PyObject *op, Py_ssize_t *ppos, PyObject **pkey, PyObject **pvalue, Py_hash_t *phash)
{
    register Py_ssize_t i;
    register Py_ssize_t mask;
    register PyOrderedDictEntry *ep;

    if (!PyOrderedDict_Check(op))
        return 0;
    i = *ppos;
    if (i < 0)
        return 0;
    ep = ((PyOrderedDictObject *)op)->ma_table;
    mask = ((PyOrderedDictObject *)op)->ma_mask;
    while (i <= mask && ep[i].me_value == NULL)
        i++;
    *ppos = i+1;
    if (i > mask)
        return 0;
    *phash = (long)(ep[i].me_hash);
    if (pkey)
        *pkey = ep[i].me_key;
    if (pvalue)
        *pvalue = ep[i].me_value;
    return 1;
}

/* Methods */

static void
dict_dealloc(register PyOrderedDictObject *mp)
{
    register PyOrderedDictEntry *ep;
    Py_ssize_t fill = mp->od_fill;
    PyObject_GC_UnTrack(mp);
    Py_TRASHCAN_SAFE_BEGIN(mp)
    for (ep = mp->ma_table; fill > 0; ep++) {
        if (ep->me_key) {
            --fill;
            Py_DECREF(ep->me_key);
            Py_XDECREF(ep->me_value);
        }
    }
    if (mp->ma_table != mp->ma_smalltable) {
        PyMem_DEL(mp->ma_table);
        PyMem_DEL(mp->od_otablep);
    }
    if (numfree < PyDict_MAXFREELIST && Py_TYPE(mp) == &PyOrderedDict_Type)
        free_list[numfree++] = mp;
    else
        Py_TYPE(mp)->tp_free((PyObject *)mp);
    Py_TRASHCAN_SAFE_END(mp)
}

#if PY_MAJOR_VERSION < 3
static int
ordereddict_print(register PyOrderedDictObject *mp, register FILE *fp, register int flags)
{
    register Py_ssize_t i;
    register Py_ssize_t any;
    char *typestr = "ordered";
    int status;
    PyOrderedDictEntry **epp;

    if (PySortedDict_CheckExact(mp))
        typestr = "sorted";
    status = Py_ReprEnter((PyObject*)mp);
    if (status != 0) {
        if (status < 0)
            return status;
        Py_BEGIN_ALLOW_THREADS
        fprintf(fp, "%sdict([...])", typestr);
        Py_END_ALLOW_THREADS
        return 0;
    }

    Py_BEGIN_ALLOW_THREADS
    fprintf(fp, "%sdict([", typestr);
    Py_END_ALLOW_THREADS
    any = 0;
    epp = mp->od_otablep;
    for (i = 0; i < mp->ma_used; i++) {
        PyObject *pvalue = (*epp)->me_value;
        /* Prevent PyObject_Repr from deleting value during
           key format */
        Py_INCREF(pvalue);
        if (any++ > 0)
            Py_BEGIN_ALLOW_THREADS
            fprintf(fp, ", ");
	    Py_END_ALLOW_THREADS
        Py_BEGIN_ALLOW_THREADS
        fprintf(fp, "(");
	Py_END_ALLOW_THREADS
        if (PyObject_Print((PyObject *)((*epp)->me_key), fp, 0)!=0) {
            Py_DECREF(pvalue);
            Py_ReprLeave((PyObject*)mp);
            return -1;
        }
        Py_BEGIN_ALLOW_THREADS
        fprintf(fp, ", ");
	Py_END_ALLOW_THREADS
        if (PyObject_Print(pvalue, fp, 0) != 0) {
            Py_DECREF(pvalue);
            Py_ReprLeave((PyObject*)mp);
            return -1;
        }
        Py_DECREF(pvalue);
        Py_BEGIN_ALLOW_THREADS
        fprintf(fp, ")");
	Py_END_ALLOW_THREADS
        epp++;
    }
    Py_BEGIN_ALLOW_THREADS
    fprintf(fp, "])");
    Py_END_ALLOW_THREADS
    Py_ReprLeave((PyObject*)mp);
    return 0;
}
#endif

static PyObject *
basedict_repr(PyOrderedDictObject *mp, char *typestr)
{
    Py_ssize_t i;
    PyObject *s, *temp, *comma = NULL, *rightpar = NULL;
    PyObject *pieces = NULL, *result = NULL;
    PyObject *key, *value;
/*    char *typestr = "ordered"; */

    /* if (PySortedDict_CheckExact(mp))*/
/*
#if PY_MAJOR_VERSION < 3
    if (PySortedDict_Check(mp))
#else
		if (PySortedDict_Check(mp))
#endif
        typestr = "sorted";
*/
    i = Py_ReprEnter((PyObject *)mp);
    if (i != 0) {
        return i > 0 ? PyUNISTR_FromFormat("%sdict([...])", typestr) : NULL;
    }

    if (mp->ma_used == 0) {
        result = PyUNISTR_FromFormat("%sdict([])", typestr);
        goto Done;
    }

    pieces = PyList_New(0);
    if (pieces == NULL)
        goto Done;

    comma = PyUNISTR_FromString(", ");
    if (comma == NULL)
        goto Done;
    rightpar = PyUNISTR_FromString(")");
    if (rightpar == NULL)
        goto Done;

    /* Do repr() on each key+value pair, and insert ": " between them.
       Note that repr may mutate the dict. */
    i = 0;
    while (PyOrderedDict_Next((PyObject *)mp, &i, &key, &value)) {
        int status;
        /* Prevent repr from deleting value during key format. */
        Py_INCREF(value);
        s = PyUNISTR_FromString("(");
        PyUNISTR_ConcatAndDel(&s, PyObject_Repr(key));
        PyUNISTR_Concat(&s, comma);
        PyUNISTR_ConcatAndDel(&s, PyObject_Repr(value));
        Py_DECREF(value);
        PyUNISTR_Concat(&s, rightpar);
        if (s == NULL)
            goto Done;
        status = PyList_Append(pieces, s);
        Py_DECREF(s);  /* append created a new ref */
        if (status < 0)
            goto Done;
    }

    /* Add "[]" decorations to the first and last items. */
    assert(PyList_GET_SIZE(pieces) > 0);
    s = PyUNISTR_FromFormat("%sdict([", typestr);
    if (s == NULL)
        goto Done;
    temp = PyList_GET_ITEM(pieces, 0);
    PyUNISTR_ConcatAndDel(&s, temp);
    PyList_SET_ITEM(pieces, 0, s);
    if (s == NULL)
        goto Done;

    s = PyUNISTR_FromString("])");
    if (s == NULL)
        goto Done;
    temp = PyList_GET_ITEM(pieces, PyList_GET_SIZE(pieces) - 1);
    PyUNISTR_ConcatAndDel(&temp, s);
    PyList_SET_ITEM(pieces, PyList_GET_SIZE(pieces) - 1, temp);
    if (temp == NULL)
        goto Done;

    /* Paste them all together with ", " between. */
    result = PyUNISTR_Join(comma, pieces);

Done:
    Py_XDECREF(pieces);
    Py_XDECREF(comma);
    Py_XDECREF(rightpar);
    Py_ReprLeave((PyObject *)mp);
    return result;
}

static PyObject *
ordereddict_repr(PyOrderedDictObject *mp)
{
		return basedict_repr(mp, "ordered");
}

static PyObject *
sorteddict_repr(PySortedDictObject *mp)
{
		return basedict_repr((PyOrderedDictObject *)mp, "sorted");
}

static Py_ssize_t
dict_length(PyOrderedDictObject *mp)
{
    return mp->ma_used;
}

static PyObject *
dict_subscript(PyOrderedDictObject *mp, register PyObject *key)
{
    PyObject *v;
    Py_hash_t hash;
    PyOrderedDictEntry *ep;
    if (PySlice_Check(key)) {
        Py_ssize_t start, stop, step, slicelength;
        PyObject* result;

        if (PySlice_GetIndicesEx(
#if PY_VERSION_HEX < 0x03000000
                  (PySliceObject*)
#endif
                                 key, mp->ma_used,
                                 &start, &stop, &step, &slicelength) < 0) {
            return NULL;
        }
        result = PyOrderedDict_New();
        if (!result) return NULL;
        if (slicelength <= 0) return result;
        if (PyOrderedDict_CopySome(result, (PyObject *) mp, start, step, slicelength, 1) == 0)
            return result;
        Py_DECREF(result);
        return NULL;
    }
    assert(mp->ma_table != NULL);
    if (!PyUNISTR_CheckExact(key) ||
            (hash = ((PyUNISTR_Object *) key)->OB_HASH) == -1) {
        hash = PyObject_Hash(key);
        if (hash == -1)
            return NULL;
    }
    ep = (mp->ma_lookup)(mp, key, hash);
    if (ep == NULL)
        return NULL;
    v = ep->me_value;
    if (v == NULL) {
        if (!PyOrderedDict_CheckExact(mp) && !PySortedDict_CheckExact(mp)) {
            /* Look up __missing__ method if we're a subclass. */
#if PY_VERSION_HEX < 0x02070000
            PyObject *missing;
            static PyObject *missing_str = NULL;
            if (missing_str == NULL)
                missing_str =
                    PyString_InternFromString("__missing__");
            missing = _PyType_Lookup(Py_TYPE(mp), missing_str);
            if (missing != NULL)
                return PyObject_CallFunctionObjArgs(missing,
                                                    (PyObject *)mp, key, NULL);
#else
            PyObject *missing, *res;
            static PyObject *missing_str = NULL;
            missing = _PyObject_LookupSpecial((PyObject *)mp,
                                              "__missing__",
                                              &missing_str);
            if (missing != NULL) {
                res = PyObject_CallFunctionObjArgs(missing,
                                                   key, NULL);
                Py_DECREF(missing);
                return res;
            }
            else if (PyErr_Occurred())
                return NULL;
#endif
        }
        set_key_error(key);
        return NULL;
    } else
        Py_INCREF(v);
    return v;
}

/* a[ilow:ihigh] = v if v != NULL.
 * del a[ilow:ihigh] if v == NULL.
 *
 * Special speed gimmick:  when v is NULL and ihigh - ilow <= 8, it's
 * guaranteed the call cannot fail.
 */
static Py_ssize_t
dict_ass_slice(PyOrderedDictObject *self, Py_ssize_t ilow, Py_ssize_t ihigh, PyObject *value)
{
    PyObject *recycle_on_stack[8];
    PyObject **recycle = recycle_on_stack; /* will allocate more if needed */
    Py_ssize_t result = -1, i;
    Py_ssize_t num_to_delete = 0, s;
    PyOrderedDictEntry **epp;

    if (PySortedDict_CheckExact(self)) {
        PyErr_Format(PyExc_TypeError,
                     "sorteddict does not support slice %s", value ? "assignment" : "deletion");
        return -1;
    }
    if (ilow < 0)
        ilow = 0;
    else if (ilow > self->ma_used)
        ilow = self->ma_used;

    if (ihigh < ilow)
        ihigh = ilow;
    else if (ihigh > self->ma_used)
        ihigh = self->ma_used;

    if (value != NULL) {
        if  (PyObject_Length(value) != (ihigh - ilow)) {
            PyErr_SetString(PyExc_ValueError,
                            "slice assignment: wrong size"
                           );
            return -1;
        }
        if (!PyOrderedDict_CheckExact(value)) {
            PyErr_SetString(PyExc_TypeError,
                            "slice assignment: argument must be ordereddict"
                           );
            return -1;
        }
    }

    /* for now lazy implementation: first delete then insert */
#define DELETION_AND_OVERWRITING_SEPERATE 0
#if DELETION_AND_OVERWRITING_SEPERATE == 1
    if (value == NULL) {
#endif
        s = (ihigh - ilow) * 2 * sizeof(PyObject *);
        if (s > sizeof(recycle_on_stack)) {
            recycle = (PyObject **)PyMem_MALLOC(s);
            if (recycle == NULL) {
                PyErr_NoMemory();
                goto Error;
            }

        }
        epp = self->od_otablep;
        epp += ilow;
        for (i = ilow; i < ihigh; i++, epp++) {
            /* AvdN: ToDo DECREF key and value */
            recycle[num_to_delete++] = (*epp)->me_key;
            Py_INCREF(dummy);
            (*epp)->me_key = dummy;
            recycle[num_to_delete++] = (*epp)->me_value;
            (*epp)->me_value = NULL;
        }
        epp = self->od_otablep;
        memmove(epp+ilow, epp+ihigh, (self->ma_used - ihigh) * sizeof(PyOrderedDictEntry *));
        self->ma_used -= (ihigh - ilow);
        result = 0;
#if DELETION_AND_OVERWRITING_SEPERATE == 1
    } else {
        /* assignment first delete slice */
        /* then delete any items whose keys are in itereable that are already in */
        PyErr_SetString(PyExc_NotImplementedError,
                        "ordered dictionary does not support slice assignment"
                       );
        result = -1;
    }
#endif
    for (i = num_to_delete - 1; i >= 0; --i)
        Py_XDECREF(recycle[i]);
#if DELETION_AND_OVERWRITING_SEPERATE != 1
    if (value != NULL) { /* now insert */
        epp = ((PyOrderedDictObject *) value)->od_otablep;
        for (i = ilow; i < ihigh; i++) {
            if(PyOrderedDict_InsertItem(self, i, (*epp)->me_key, (*epp)->me_value) != 0)
                return -1;
            epp++;
        }
    }
#endif
Error:
    if (recycle != recycle_on_stack)
        PyMem_FREE(recycle);
    return result;
}

static Py_ssize_t
dict_ass_subscript(PyOrderedDictObject *self, PyObject *item, PyObject *value)
{
    if (PySlice_Check(item)) {
        Py_ssize_t start, stop, step, slicelength;
        if (PySortedDict_CheckExact(self)) {
            PyErr_Format(PyExc_TypeError,
                         "sorteddict does not support slice %s", value ? "assignment" : "deletion");
            return -1;
        }
        if (PySlice_GetIndicesEx(
#if PY_VERSION_HEX < 0x03000000
                  (PySliceObject*)
#endif
                                 item, self->ma_used,
                                 &start, &stop, &step, &slicelength) < 0) {
            return -1;
        }

        /* treat L[slice(a,b)] = v _exactly_ like L[a:b] = v */
        if (step == 1 && ((PySliceObject*)item)->step == Py_None)
            return dict_ass_slice(self, start, stop, value);

        /* do soemthing about step == -1 ? */

        if (slicelength <= 0)
            return 0;
        if (value == NULL) {
            /* delete slice */
            /* printf("Deleting %d %d %d %d %p\n", start, stop, step, slicelength, value);*/
            while (slicelength--) {
                /* ToDo optimize */
                if (step > 0) { /* do it from the back to preserve right indices */
                    dict_ass_slice(self, start + slicelength *  step,
                                   start + (slicelength * step) + 1, NULL);
                } else {
                    dict_ass_slice(self, start,
                                   start  + 1, NULL);
                    start += step;
                }
            }
            return 0;
        } else {
            /* assign slice */
            Py_ssize_t count = slicelength, start2 = start;
            PyOrderedDictEntry **epp;
            /* printf("Assigning %d %d %d %d %d %p\n", start, stop, step, slicelength, PyObject_Length(value), value); */
            if  (PyObject_Length(value) != slicelength) {
                PyErr_SetString(PyExc_ValueError,
                                "slice assignment: wrong size"
                               );
                return -1;
            }
            if (!PyOrderedDict_CheckExact(value)) {
                PyErr_SetString(PyExc_TypeError,
                                "slice assignment: argument must be ordereddict"
                               );
                return -1;
            }
            while (count--) {
                /* ToDo optimize */
                if (step > 0) { /* do it from the back to preserve right indices */
                    dict_ass_slice(self, start2 + count *  step,
                                   start2 + (count * step) + 1, NULL);
                } else {
                    dict_ass_slice(self, start2, start2  + 1, NULL);
                    start2 += step;
                }
            }
            count = slicelength;
            start2 = start;
            epp = ((PyOrderedDictObject *) value)->od_otablep;
            if (step < 0) {
                epp += slicelength;
            }
            while (count--) {
                /* ToDo optimize */
                if (step > 0) { /* do it from the front */
                    if(PyOrderedDict_InsertItem(self, start2, (*epp)->me_key, (*epp)->me_value) != 0)
                        return -1;
                    start2 += step;
                    epp++;
                } else {
                    epp--;
                    if(PyOrderedDict_InsertItem(self, start2 + count * step, (*epp)->me_key, (*epp)->me_value) != 0)
                        return -1;
                }
            }
            return 0;

        }
    }
    if (value == NULL)
        return PyOrderedDict_DelItem((PyObject *)self, item);
    else
        return PyOrderedDict_SetItem((PyObject *)self, item, value);
}

static PyMappingMethods dict_as_mapping = {
    (lenfunc)dict_length, /*mp_length*/
    (binaryfunc)dict_subscript, /*mp_subscript*/
    (objobjargproc)dict_ass_subscript, /*mp_ass_subscript*/
};

static PyObject *
dict_keys(register PyOrderedDictObject *mp, PyObject *args, PyObject *kwds)
{
    register PyObject *v;
    register Py_ssize_t i;
    PyOrderedDictEntry **epp;
    Py_ssize_t n;

    int reverse = 0;
    static char *kwlist[] = {"reverse", 0};

    if (args != NULL)
        if (!PyArg_ParseTupleAndKeywords(args, kwds, "|i:keys",
                                         kwlist, &reverse))
            return NULL;


again:
    n = mp->ma_used;
    v = PyList_New(n);
    if (v == NULL)
        return NULL;
    if (n != mp->ma_used) {
        /* Durnit.  The allocations caused the dict to resize.
         * Just 	 over, this shouldn't normally happen.
         */
        Py_DECREF(v);
        goto again;
    }
    if (reverse) {
        epp = mp->od_otablep + (n-1);
        reverse = -1;
    } else {
        epp = mp->od_otablep;
        reverse = 1;
    }
    for (i = 0; i < n; i++) {
        PyObject *key = (*epp)->me_key;
        Py_INCREF(key);
        PyList_SET_ITEM(v, i, key);
        epp += reverse;
    }
    return v;
}

static PyObject *
dict_values(register PyOrderedDictObject *mp, PyObject *args, PyObject *kwds)
{
    register PyObject *v;
    register Py_ssize_t i;
    PyOrderedDictEntry **epp;
    Py_ssize_t n;

    int reverse = 0;
    static char *kwlist[] = {"reverse", 0};

    if (args != NULL)
        if (!PyArg_ParseTupleAndKeywords(args, kwds, "|i:values",
                                         kwlist, &reverse))
            return NULL;

again:
    n = mp->ma_used;
    v = PyList_New(n);
    if (v == NULL)
        return NULL;
    if (n != mp->ma_used) {
        /* Durnit.  The allocations caused the dict to resize.
         * Just start over, this shouldn't normally happen.
         */
        Py_DECREF(v);
        goto again;
    }
    if (reverse) {
        epp = mp->od_otablep + (n-1);
        reverse = -1;
    } else {
        epp = mp->od_otablep;
        reverse = 1;
    }
    for (i = 0; i < n; i++) {
        PyObject *value = (*epp)->me_value;
        Py_INCREF(value);
        PyList_SET_ITEM(v, i, value);
        epp += reverse;
    }
    return v;
}

static PyObject *
dict_items(register PyOrderedDictObject *mp, PyObject *args, PyObject *kwds)
{
    register PyObject *v;
    register Py_ssize_t i, n;
    PyObject *item, *key, *value;
    PyOrderedDictEntry **epp;

    int reverse = 0;
    static char *kwlist[] = {"reverse", 0};

    if (args != NULL)
        if (!PyArg_ParseTupleAndKeywords(args, kwds, "|i:items",
                                         kwlist, &reverse))
            return NULL;

    /* Preallocate the list of tuples, to avoid allocations during
     * the loop over the items, which could trigger GC, which
     * could resize the dict. :-(
     */
again:
    n = mp->ma_used;
    v = PyList_New(n);
    if (v == NULL)
        return NULL;
    for (i = 0; i < n; i++) {
        item = PyTuple_New(2);
        if (item == NULL) {
            Py_DECREF(v);
            return NULL;
        }
        PyList_SET_ITEM(v, i, item);
    }
    if (n != mp->ma_used) {
        /* Durnit.  The allocations caused the dict to resize.
         * Just start over, this shouldn't normally happen.
         */
        Py_DECREF(v);
        goto again;
    }
    /* Nothing we do below makes any function calls. */
    if (reverse) {
        epp = mp->od_otablep + (n-1);
        reverse = -1;
    } else {
        epp = mp->od_otablep;
        reverse = 1;
    }
    for (i = 0; i < n; i++) {
        key = (*epp)->me_key;
        value = (*epp)->me_value;
        item = PyList_GET_ITEM(v, i);
        Py_INCREF(key);
        PyTuple_SET_ITEM(item, 0, key);
        Py_INCREF(value);
        PyTuple_SET_ITEM(item, 1, value);
        epp += reverse;
    }
    return v;
}

static PyObject *
dict_fromkeys(PyObject *cls, PyObject *args)
{
    PyObject *seq;
    PyObject *value = Py_None;
    PyObject *it;	/* iter(seq) */
    PyObject *key;
    PyObject *d;
    int status;

    if (!PyArg_UnpackTuple(args, "fromkeys", 1, 2, &seq, &value))
        return NULL;

    d = PyObject_CallObject(cls, NULL);
    if (d == NULL)
        return NULL;


    if ((PyOrderedDict_CheckExact(d) || PySortedDict_CheckExact(d)) && ((PyDictObject *)d)->ma_used == 0) {
        if (PyAnySet_CheckExact(seq)) {
            PyOrderedDictObject *mp = (PyOrderedDictObject *)d;
            Py_ssize_t pos = 0;
            PyObject *key;
            Py_hash_t hash;

            if (dictresize(mp, PySet_GET_SIZE(seq))) {
    	    Py_DECREF(d);
                return NULL;
            }

            while (_PySet_NextEntry(seq, &pos, &key, &hash)) {
                Py_INCREF(key);
                Py_INCREF(value);
                if (insertdict(mp, key, hash, value, -1)) {
    	        Py_DECREF(d);
                    return NULL;
                }
            }
            return d;
        }
    }

    it = PyObject_GetIter(seq);
    if (it == NULL) {
        Py_DECREF(d);
        return NULL;
    }

#ifndef OLD
   if (PyOrderedDict_CheckExact(d) || PySortedDict_CheckExact(d)) {
        while ((key = PyIter_Next(it)) != NULL) {
            status = PyOrderedDict_SetItem(d, key, value);
            Py_DECREF(key);
            if (status < 0)
                goto Fail;
        }
    } else {
        while ((key = PyIter_Next(it)) != NULL) {
            status = PyObject_SetItem(d, key, value);
            Py_DECREF(key);
            if (status < 0)
                goto Fail;
        }
    }
    if (PyErr_Occurred())
        goto Fail;
#else
    for (;;) {
        key = PyIter_Next(it);
        if (key == NULL) {
            if (PyErr_Occurred())
                goto Fail;
            break;
        }
        status = PyObject_SetItem(d, key, value);
        Py_DECREF(key);
        if (status < 0)
            goto Fail;
    }

#endif
    Py_DECREF(it);
    return d;

Fail:
    Py_DECREF(it);
    Py_DECREF(d);
    return NULL;
}

/* called by init, update and setitems */
static int
dict_update_common(PyObject *self, PyObject *args, PyObject *kwds, char *args_name)
{
    PyObject *arg = NULL;
    int result = 0, tmprelax = 0;

    static char *kwlist[] = {"src", "relax", 0};

    if (args != NULL)
        if (!PyArg_ParseTupleAndKeywords(args, kwds, args_name,
                                         kwlist, &arg, &tmprelax))
            return -1;

    if (arg != NULL) {
        if (PyObject_HasAttrString(arg, "keys"))
            result = PyOrderedDict_Merge(self, arg, 1, tmprelax);
        else
            result = PyOrderedDict_MergeFromSeq2(self, arg, 1);
    }
    /* do not initialise from keywords at all */
    /* if (result == 0 && kwds != NULL)
    	result = PyOrderedDict_Merge(self, kwds, 1); */
    return result;
}

static PyObject *
dict_update(PyObject *self, PyObject *args, PyObject *kwds)
{
    if (dict_update_common(self, args, kwds, "|Oi:update") != -1)
        Py_RETURN_NONE;
    return NULL;
}

/* Update unconditionally replaces existing items.
   Merge has a 3rd argument 'override'; if set, it acts like Update,
   otherwise it leaves existing items unchanged.

   PyOrderedDict_{Update,Merge} update/merge from a mapping object.

   PyOrderedDict_MergeFromSeq2 updates/merges from any iterable object
   producing iterable objects of length 2.
*/

int
PyOrderedDict_MergeFromSeq2(PyObject *d, PyObject *seq2, int override)
{
    PyObject *it;	/* iter(seq2) */
    Py_ssize_t i;	/* index into seq2 of current element */
    PyObject *item;	/* seq2[i] */
    PyObject *fast;	/* item as a 2-tuple or 2-list */

    assert(d != NULL);
    assert(PyOrderedDict_Check(d));
    assert(seq2 != NULL);

    it = PyObject_GetIter(seq2);
    if (it == NULL)
        return -1;

    for (i = 0; ; ++i) {
        PyObject *key, *value;
        Py_ssize_t n;

        fast = NULL;
        item = PyIter_Next(it);
        if (item == NULL) {
            if (PyErr_Occurred())
                goto Fail;
            break;
        }

        /* Convert item to sequence, and verify length 2. */
        fast = PySequence_Fast(item, "");
        if (fast == NULL) {
            if (PyErr_ExceptionMatches(PyExc_TypeError))
                PyErr_Format(PyExc_TypeError,
                             "cannot convert dictionary update "
                             "sequence element #%zd to a sequence",
                             i);
            goto Fail;
        }
        n = PySequence_Fast_GET_SIZE(fast);
        if (n != 2) {
            PyErr_Format(PyExc_ValueError,
                         "dictionary update sequence element #%zd "
                         "has length %zd; 2 is required",
                         i, n);
            goto Fail;
        }

        /* Update/merge with this (key, value) pair. */
        key = PySequence_Fast_GET_ITEM(fast, 0);
        value = PySequence_Fast_GET_ITEM(fast, 1);
        if (override || PyOrderedDict_GetItem(d, key) == NULL) {
            int status = PyOrderedDict_SetItem(d, key, value);
            if (status < 0)
                goto Fail;
        }
        Py_DECREF(fast);
        Py_DECREF(item);
    }

    i = 0;
    goto Return;
Fail:
    Py_XDECREF(item);
    Py_XDECREF(fast);
    i = -1;
Return:
    Py_DECREF(it);
    return Py_SAFE_DOWNCAST(i, Py_ssize_t, int);
}

int
PyOrderedDict_Update(PyObject *a, PyObject *b)
{
    return PyOrderedDict_Merge(a, b, 1, 0);
}

int
PyOrderedDict_Merge(PyObject *a, PyObject *b, int override, int relaxed)
{
    register PyOrderedDictObject *mp, *other;
    register Py_ssize_t i;
    PyOrderedDictEntry *entry, **epp;

    /* We accept for the argument either a concrete ordered dictionary object,
     * or an abstract "mapping" object.  For the former, we can do
     * things quite efficiently.  For the latter, we only require that
     * PyMapping_Keys() and PyObject_GetItem() be supported.
     */
    if (a == NULL || !PyOrderedDict_Check(a) || b == NULL) {
        PyErr_BadInternalCall();
        return -1;
    }
    mp = (PyOrderedDictObject*)a;
    /* sorted dicts are always done with individual elements */
    if (!PySortedDict_CheckExact(a) && PyOrderedDict_CheckExact(b)) {
        other = (PyOrderedDictObject *) b;
        if (other == mp || other->ma_used == 0)
            /* a.update(a) or a.update({}); nothing to do */
            return 0;
        if (mp->ma_used == 0)
            /* Since the target dict is empty, PyOrderedDict_GetItem()
             * always returns NULL.  Setting override to 1
             * skips the unnecessary test.
             */
            override = 1;
        /* Do one big resize at the start, rather than
         * incrementally resizing as we insert new items.  Expect
         * that there will be no (or few) overlapping keys.
         */
        if ((mp->od_fill + other->ma_used)*3 >= (mp->ma_mask+1)*2) {
            if (dictresize(mp, (mp->ma_used + other->ma_used)*2) != 0)
                return -1;
        }
        epp = other->od_otablep;
        for (i = 0; i < other->ma_used; i++) {
            entry = *epp++;
            /* entry->me_value is never NULL when following the otablep */
            /*
            if (entry->me_value != NULL &&
                (override ||
                 PyOrderedDict_GetItem(a, entry->me_key) == NULL)) {
            */
            if (override || PyOrderedDict_GetItem(a, entry->me_key) == NULL) {
                Py_INCREF(entry->me_key);
                Py_INCREF(entry->me_value);
                if (insertdict(mp, entry->me_key,
                               (long)entry->me_hash,
                               entry->me_value, -1) != 0)
                    return -1;
            }
        }
    } else if (relaxed || RELAXED(mp)) {
        /* Do it the generic, slower way */
        PyObject *keys = PyMapping_Keys(b);
        PyObject *iter;
        PyObject *key, *value;
        int status;

        if (keys == NULL)
            /* Docstring says this is equivalent to E.keys() so
             * if E doesn't have a .keys() method we want
             * AttributeError to percolate up.  Might as well
             * do the same for any other error.
             */
            return -1;

        iter = PyObject_GetIter(keys);
        Py_DECREF(keys);
        if (iter == NULL)
            return -1;

        for (key = PyIter_Next(iter); key; key = PyIter_Next(iter)) {
            if (!override && PyDict_GetItem(a, key) != NULL) {
                Py_DECREF(key);
                continue;
            }
            value = PyObject_GetItem(b, key);
            if (value == NULL) {
                Py_DECREF(iter);
                Py_DECREF(key);
                return -1;
            }
            status = PyOrderedDict_SetItem(a, key, value);
            Py_DECREF(key);
            Py_DECREF(value);
            if (status < 0) {
                Py_DECREF(iter);
                return -1;
            }
        }
        Py_DECREF(iter);
        if (PyErr_Occurred())
            /* Iterator completed, via error */
            return -1;
    } else {
        PyErr_SetString(PyExc_TypeError,
                        "source has undefined order");
        return -1;
    }
    return 0;
}


/*
   assume that the start step and count are all within the
   borders of what b provides
*/
int
PyOrderedDict_CopySome(PyObject *a, PyObject *b,
                       Py_ssize_t start, Py_ssize_t step,
                       Py_ssize_t count, int override)
{
    register PyOrderedDictObject *mp, *other;
    register Py_ssize_t i;
    PyOrderedDictEntry *entry, **epp;

    /* We accept for the argument either a concrete ordered dictionary object
     */
    if (a == NULL || !PyOrderedDict_Check(a) || b == NULL) {
        PyErr_BadInternalCall();
        return -1;
    }
    mp = (PyOrderedDictObject*)a;
    if (PyOrderedDict_CheckExact(b) || PySortedDict_CheckExact(b)) {
        other = (PyOrderedDictObject*)b;
        if (other == mp || other->ma_used == 0)
            /* a.update(a) or a.update({}); nothing to do */
            return 0;
        if (mp->ma_used == 0)
            /* Since the target dict is empty, PyOrderedDict_GetItem()
             * always returns NULL.  Setting override to 1
             * skips the unnecessary test.
             */
            override = 1;
        /* Do one big resize at the start, rather than
         * incrementally resizing as we insert new items.  Expect
         * that there will be no (or few) overlapping keys.
         */
        if ((mp->od_fill + count)*3 >= (mp->ma_mask+1)*2) {
            if (dictresize(mp, (mp->ma_used + count)*2) != 0)
                return -1;
        }
        epp = other->od_otablep;
        epp += start;
        for (i = 0; i < count; i++, epp += step) {
            entry = *epp;
            if (override || PyOrderedDict_GetItem(a, entry->me_key) == NULL) {
                Py_INCREF(entry->me_key);
                Py_INCREF(entry->me_value);
                if (insertdict(mp, entry->me_key,
                               (long)entry->me_hash,
                               entry->me_value, -1) != 0)
                    return -1;
            }
        }
    } else {
        PyErr_SetString(PyExc_TypeError,
                        "source has undefined order");
        return -1;
    }
    return 0;
}

static PyObject *
dict_copy(register PyOrderedDictObject *mp)
{
    return PyOrderedDict_Copy((PyObject*)mp);
}

PyObject *
PyOrderedDict_Copy(PyObject *o)
{
    PyObject *copy;

    if (o == NULL || !PyOrderedDict_Check(o)) {
        PyErr_BadInternalCall();
        return NULL;
    }
    if (PySortedDict_CheckExact(o)) {
        copy = PySortedDict_New();
        if (copy == NULL)
            return NULL;
        ((PySortedDictObject *) copy)->sd_cmp = ((PySortedDictObject *) o)->sd_cmp;
        ((PySortedDictObject *) copy)->sd_key = ((PySortedDictObject *) o)->sd_key;
        ((PySortedDictObject *) copy)->sd_value = ((PySortedDictObject *) o)->sd_value;
    } else {
        copy = PyOrderedDict_New();
        if (copy == NULL)
            return NULL;
    }
    ((PyOrderedDictObject *) copy)->od_state = ((PyOrderedDictObject *) o)->od_state;
    if (PyOrderedDict_Merge(copy, o, 1, 0) == 0)
        return copy;
    Py_DECREF(copy);
    return NULL;
}

Py_ssize_t
PyOrderedDict_Size(PyObject *mp)
{
    if (mp == NULL || !PyOrderedDict_Check(mp)) {
        PyErr_BadInternalCall();
        return -1;
    }
    return ((PyOrderedDictObject *)mp)->ma_used;
}

PyObject *
PyOrderedDict_Keys(PyObject *mp)
{
    if (mp == NULL || !PyOrderedDict_Check(mp)) {
        PyErr_BadInternalCall();
        return NULL;
    }
    return dict_keys((PyOrderedDictObject *)mp, NULL, NULL);
}

PyObject *
PyOrderedDict_Values(PyObject *mp)
{
    if (mp == NULL || !PyOrderedDict_Check(mp)) {
        PyErr_BadInternalCall();
        return NULL;
    }
    return dict_values((PyOrderedDictObject *)mp, NULL, NULL);
}

PyObject *
PyOrderedDict_Items(PyObject *mp)
{
    if (mp == NULL || !PyOrderedDict_Check(mp)) {
        PyErr_BadInternalCall();
        return NULL;
    }
    return dict_items((PyOrderedDictObject *)mp, NULL, NULL);
}

#if PY_VERSION_HEX < 0x03000000
/* Subroutine which returns the smallest key in a for which b's value
   is different or absent.  The value is returned too, through the
   pval argument.  Both are NULL if no key in a is found for which b's status
   differs.  The refcounts on (and only on) non-NULL *pval and function return
   values must be decremented by the caller (characterize() increments them
   to ensure that mutating comparison and PyOrderedDict_GetItem calls can't delete
   them before the caller is done looking at them). */

static PyObject *
characterize(PyOrderedDictObject *a, PyOrderedDictObject *b, PyObject **pval)
{
    PyObject *akey = NULL; /* smallest key in a s.t. a[akey] != b[akey] */
    PyObject *aval = NULL; /* a[akey] */
    Py_ssize_t i;
    int cmp;

    for (i = 0; i <= a->ma_mask; i++) {
        PyObject *thiskey, *thisaval, *thisbval;
        if (a->ma_table[i].me_value == NULL)
            continue;
        thiskey = a->ma_table[i].me_key;
        Py_INCREF(thiskey);  /* keep alive across compares */
        if (akey != NULL) {
            cmp = PyObject_RichCompareBool(akey, thiskey, Py_LT);
            if (cmp < 0) {
                Py_DECREF(thiskey);
                goto Fail;
            }
            if (cmp > 0 ||
                    i > a->ma_mask ||
                    a->ma_table[i].me_value == NULL) {
                /* Not the *smallest* a key; or maybe it is
                 * but the compare shrunk the dict so we can't
                 * find its associated value anymore; or
                 * maybe it is but the compare deleted the
                 * a[thiskey] entry.
                 */
                Py_DECREF(thiskey);
                continue;
            }
        }

        /* Compare a[thiskey] to b[thiskey]; cmp <- true iff equal. */
        thisaval = a->ma_table[i].me_value;
        assert(thisaval);
        Py_INCREF(thisaval);   /* keep alive */
        thisbval = PyOrderedDict_GetItem((PyObject *)b, thiskey);
        if (thisbval == NULL)
            cmp = 0;
        else {
            /* both dicts have thiskey:  same values? */
            cmp = PyObject_RichCompareBool(
                      thisaval, thisbval, Py_EQ);
            if (cmp < 0) {
                Py_DECREF(thiskey);
                Py_DECREF(thisaval);
                goto Fail;
            }
        }
        if (cmp == 0) {
            /* New winner. */
            Py_XDECREF(akey);
            Py_XDECREF(aval);
            akey = thiskey;
            aval = thisaval;
        } else {
            Py_DECREF(thiskey);
            Py_DECREF(thisaval);
        }
    }
    *pval = aval;
    return akey;

Fail:
    Py_XDECREF(akey);
    Py_XDECREF(aval);
    *pval = NULL;
    return NULL;
}

static int
dict_compare(PyOrderedDictObject *a, PyOrderedDictObject *b)
{
    PyObject *adiff, *bdiff, *aval, *bval;
    int res;

    /* Compare lengths first */
    if (a->ma_used < b->ma_used)
        return -1;	/* a is shorter */
    else if (a->ma_used > b->ma_used)
        return 1;	/* b is shorter */

    /* Same length -- check all keys */
    bdiff = bval = NULL;
    adiff = characterize(a, b, &aval);
    if (adiff == NULL) {
        assert(!aval);
        /* Either an error, or a is a subset with the same length so
         * must be equal.
         */
        res = PyErr_Occurred() ? -1 : 0;
        goto Finished;
    }
    bdiff = characterize(b, a, &bval);
    if (bdiff == NULL && PyErr_Occurred()) {
        assert(!bval);
        res = -1;
        goto Finished;
    }
    res = 0;
    if (bdiff) {
        /* bdiff == NULL "should be" impossible now, but perhaps
         * the last comparison done by the characterize() on a had
         * the side effect of making the dicts equal!
         */
        res = PyObject_Compare(adiff, bdiff);
    }
    if (res == 0 && bval != NULL)
        res = PyObject_Compare(aval, bval);

Finished:
    Py_XDECREF(adiff);
    Py_XDECREF(bdiff);
    Py_XDECREF(aval);
    Py_XDECREF(bval);
    return res;
}
#endif

/* Return 1 if dicts equal, 0 if not, -1 if error.
 * Gets out as soon as any difference is detected.
 * Uses only Py_EQ comparison.
 */
static int
dict_equal(PyOrderedDictObject *a, PyOrderedDictObject *b)
{
    Py_ssize_t i;
    PyOrderedDictEntry **app, **bpp;

    if (a->ma_used != b->ma_used)
        /* can't be equal if # of entries differ */
        return 0;

    /* Same # of entries -- check all of 'em.  Exit early on any diff. */

    for (i = 0, app = a->od_otablep, bpp = b->od_otablep; i < a->ma_used;
            i++, app++, bpp++) {
        int cmp;
        PyObject *aval = (*app)->me_value;
        PyObject *bval = (*bpp)->me_value;
        PyObject *akey = (*app)->me_key;
        PyObject *bkey = (*bpp)->me_key;
        /* temporarily bump aval's refcount to ensure it stays
           alive until we're done with it */
        Py_INCREF(aval);
        Py_INCREF(bval);
        /* ditto for key */
        Py_INCREF(akey);
        Py_INCREF(bkey);
        cmp = PyObject_RichCompareBool(akey, bkey, Py_EQ);
        if (cmp > 0) /* keys compare ok, now do values */
            cmp = PyObject_RichCompareBool(aval, bval, Py_EQ);
        Py_DECREF(bkey);
        Py_DECREF(akey);
        Py_DECREF(bval);
        Py_DECREF(aval);
        if (cmp <= 0)  /* error or not equal */
            return cmp;
    }
    return 1;
}

static PyObject *
dict_richcompare(PyObject *v, PyObject *w, int op)
{
    int cmp;
    PyObject *res;

    if (!PyOrderedDict_Check(v) || !PyOrderedDict_Check(w)) {
        res = Py_NotImplemented;
    } else if (op == Py_EQ || op == Py_NE) {
        cmp = dict_equal((PyOrderedDictObject *)v, (PyOrderedDictObject *)w);
        if (cmp < 0)
            return NULL;
        res = (cmp == (op == Py_EQ)) ? Py_True : Py_False;
    } else {
#if PY_VERSION_HEX < 0x03000000
         /* Py3K warning if comparison isn't == or !=  */
         if (PyErr_WarnPy3k("dict inequality comparisons not supported "
                            "in 3.x", 1) < 0) {
             return NULL;
         }
#endif
         res = Py_NotImplemented;
    }
    Py_INCREF(res);
    return res;
}

static PyObject *
dict_contains(register PyOrderedDictObject *mp, PyObject *key)
{
    Py_hash_t hash;
    PyOrderedDictEntry *ep;

    if (!PyUNISTR_CheckExact(key) ||
            (hash = ((PyUNISTR_Object *) key)->OB_HASH) == -1) {
        hash = PyObject_Hash(key);
        if (hash == -1)
            return NULL;
    }
    ep = (mp->ma_lookup)(mp, key, hash);
    if (ep == NULL)
        return NULL;
    return PyBool_FromLong(ep->me_value != NULL);
}

#if PY_VERSION_HEX < 0x03000000
static PyObject *
dict_has_key(register PyOrderedDictObject *mp, PyObject *key)
{
    if (Py_Py3kWarningFlag &&
            PyErr_Warn(PyExc_DeprecationWarning,
                       "dict.has_key() not supported in 3.x") < 0)
        return NULL;
    return dict_contains(mp, key);
}
#endif

static PyObject *
dict_get(register PyOrderedDictObject *mp, PyObject *args)
{
    PyObject *key;
    PyObject *failobj = Py_None;
    PyObject *val = NULL;
    Py_hash_t hash;
    PyOrderedDictEntry *ep;

    if (!PyArg_UnpackTuple(args, "get", 1, 2, &key, &failobj))
        return NULL;

    if (!PyUNISTR_CheckExact(key) ||
            (hash = ((PyUNISTR_Object *) key)->OB_HASH) == -1) {
        hash = PyObject_Hash(key);
        if (hash == -1)
            return NULL;
    }
    ep = (mp->ma_lookup)(mp, key, hash);
    if (ep == NULL)
        return NULL;
    val = ep->me_value;
    if (val == NULL)
        val = failobj;
    Py_INCREF(val);
    return val;
}


static PyObject *
dict_setdefault(register PyOrderedDictObject *mp, PyObject *args)
{
    PyObject *key;
    PyObject *failobj = Py_None;
    PyObject *val = NULL;
    Py_hash_t hash;
    PyOrderedDictEntry *ep;

    if (!PyArg_UnpackTuple(args, "setdefault", 1, 2, &key, &failobj))
        return NULL;

    if (!PyUNISTR_CheckExact(key) ||
            (hash = ((PyUNISTR_Object *) key)->OB_HASH) == -1) {
        hash = PyObject_Hash(key);
        if (hash == -1)
            return NULL;
    }
    ep = (mp->ma_lookup)(mp, key, hash);
    if (ep == NULL)
        return NULL;
    val = ep->me_value;
    if (val == NULL) {
        if (dict_set_item_by_hash_or_entry((PyObject*)mp, key, hash, ep,
                                           failobj) == 0)
            val = failobj;
    }
    Py_XINCREF(val);
    return val;
}


static PyObject *
dict_clear(register PyOrderedDictObject *mp)
{
    PyOrderedDict_Clear((PyObject *)mp);
    Py_RETURN_NONE;
}

static PyObject *
dict_pop(PyOrderedDictObject *mp, PyObject *args)
{
    Py_hash_t hash;
    PyOrderedDictEntry *ep;
    PyObject *old_value, *old_key;
    PyObject *key, *deflt = NULL;

    if(!PyArg_UnpackTuple(args, "pop", 1, 2, &key, &deflt))
        return NULL;
    if (mp->ma_used == 0) {
        if (deflt) {
            Py_INCREF(deflt);
            return deflt;
        }
        PyErr_SetString(PyExc_KeyError,
                        "pop(): dictionary is empty");
        return NULL;
    }
    if (!PyUNISTR_CheckExact(key) ||
            (hash = ((PyUNISTR_Object *) key)->OB_HASH) == -1) {
        hash = PyObject_Hash(key);
        if (hash == -1)
            return NULL;
    }
    ep = (mp->ma_lookup)(mp, key, hash);
    if (ep == NULL)
        return NULL;
    if (ep->me_value == NULL) {
        if (deflt) {
            Py_INCREF(deflt);
            return deflt;
        }
        set_key_error(key);
        return NULL;
    }
    old_key = ep->me_key;
    Py_INCREF(dummy);
    ep->me_key = dummy;
    old_value = ep->me_value;
    ep->me_value = NULL;
    del_inorder(mp, ep);
    mp->ma_used--;
    Py_DECREF(old_key);
    return old_value;
}

static PyObject *
dict_popitem(PyOrderedDictObject *mp, PyObject *args)
{
    Py_hash_ssize_t i = -1, j;
    PyOrderedDictEntry **epp;
    PyObject *res;

    /* Allocate the result tuple before checking the size.  Believe it
     * or not, this allocation could trigger a garbage collection which
     * could empty the dict, so if we checked the size first and that
     * happened, the result would be an infinite loop (searching for an
     * entry that no longer exists).  Note that the usual popitem()
     * idiom is "while d: k, v = d.popitem()". so needing to throw the
     * tuple away if the dict *is* empty isn't a significant
     * inefficiency -- possible, but unlikely in practice.
     */
    if (!PyArg_ParseTuple(args, "|n:popitem", &i))
        return NULL;

    res = PyTuple_New(2);
    if (res == NULL)
        return NULL;
    if (mp->ma_used == 0) {
        Py_DECREF(res);
        PyErr_SetString(PyExc_KeyError,
                        "popitem(): dictionary is empty");
        return NULL;
    }
    if (i < 0)
        j = mp->ma_used + i;
    else
        j = i;
    if (j < 0 || j >= mp->ma_used) {
        Py_DECREF(res);
        PyErr_SetString(PyExc_KeyError,
                        "popitem(): index out of range");
        return NULL;
    }
    epp = mp->od_otablep;
    epp += j;
    PyTuple_SET_ITEM(res, 0, (*epp)->me_key);
    PyTuple_SET_ITEM(res, 1, (*epp)->me_value);
    Py_INCREF(dummy);
    (*epp)->me_key = dummy;
    (*epp)->me_value = NULL;
    mp->ma_used--;
    if (i != -1) { /* for default case -1, we don't have to do anything */
        /* ma_used has already been decremented ! */
        memmove(epp, epp+1, (mp->ma_used - j) * sizeof(PyOrderedDictEntry *));
    }
    return res;
}

static int
dict_traverse(PyObject *op, visitproc visit, void *arg)
{
    Py_ssize_t i = 0;
    PyObject *pk;
    PyObject *pv;

    while (PyOrderedDict_Next(op, &i, &pk, &pv)) {
        Py_VISIT(pk);
        Py_VISIT(pv);
    }
    return 0;
}

static int
dict_tp_clear(PyObject *op)
{
    PyOrderedDict_Clear(op);
    return 0;
}

#if PY_MAJOR_VERSION < 3
extern PyTypeObject PyOrderedDictIterKey_Type; /* Forward */
extern PyTypeObject PyOrderedDictIterValue_Type; /* Forward */
extern PyTypeObject PyOrderedDictIterItem_Type; /* Forward */
#endif
static PyObject *dictiter_new(PyOrderedDictObject *, PyTypeObject *,
                              PyObject *args, PyObject *kwds);

#if PY_MAJOR_VERSION < 3
static PyObject *
dict_iterkeys(PyOrderedDictObject *dict, PyObject *args, PyObject *kwds)
{
    return dictiter_new(dict, &PyOrderedDictIterKey_Type, args, kwds);
}

static PyObject *
dict_itervalues(PyOrderedDictObject *dict, PyObject *args, PyObject *kwds)
{
    return dictiter_new(dict, &PyOrderedDictIterValue_Type, args, kwds);
}

static PyObject *
dict_iteritems(PyOrderedDictObject *dict, PyObject *args, PyObject *kwds)
{
    return dictiter_new(dict, &PyOrderedDictIterItem_Type, args, kwds);
}
#endif

static PyObject *
dict_sizeof(PyDictObject *mp)
{
    Py_ssize_t res;

    res = sizeof(PyOrderedDictObject);
    if (mp->ma_table != mp->ma_smalltable)
        res = res + (mp->ma_mask + 1) * sizeof(PyOrderedDictEntry);
#if PY_VERSION_HEX < 0x03000000
    return PyInt_FromSize_t(res);
#else
    return PyLong_FromSize_t(res);
#endif
}

static PyObject *
dict_index(register PyOrderedDictObject *mp, PyObject *key)
{
    Py_hash_t hash;
    PyOrderedDictEntry *ep, **tmp;
    register Py_ssize_t index;

    if (!PyUNISTR_CheckExact(key) ||
            (hash = ((PyUNISTR_Object *) key)->OB_HASH) == -1) {
        hash = PyObject_Hash(key);
        if (hash == -1)
            return NULL;
    }
    ep = (mp->ma_lookup)(mp, key, hash);
    if (ep == NULL || ep->me_value == NULL) {
        PyErr_SetString(PyExc_ValueError,
                        "ordereddict.index(x): x not a key in ordereddict"
                       );
        return NULL;
    }

    for (index = 0, tmp = mp->od_otablep; index < mp->ma_used; index++, tmp++) {
        if (*tmp == ep) {
#if PY_VERSION_HEX < 0x03000000
            return PyInt_FromSize_t(index);
#else
            return PyLong_FromSize_t(index);
#endif
        }
    }
    return NULL; /* not found */
}

static PyObject *
dict_insert(PyOrderedDictObject *mp, PyObject *args)
{
    Py_ssize_t i;
    PyObject *key;
    PyObject *val;

#if PY_VERSION_HEX >= 0x02050000
    if (!PyArg_ParseTuple(args, "nOO:insert", &i, &key, &val))
#else
    if (!PyArg_ParseTuple(args, "iOO:insert", &i, &key, &val))
#endif
        return NULL;
    if(PyOrderedDict_InsertItem(mp, i, key, val) != 0)
        return NULL;
    Py_RETURN_NONE;
}

static PyObject *
dict_reverse(register PyOrderedDictObject *mp)
{
    PyOrderedDictEntry **epps, **eppe, *tmp;

    epps = mp->od_otablep;
    eppe = epps + ((mp->ma_used)-1);
    while (epps < eppe) {
        tmp = *epps;
        *epps++ = *eppe;
        *eppe-- = tmp;
    }
    Py_RETURN_NONE;
}

static PyObject *
dict_setkeys(register PyOrderedDictObject *mp, PyObject *keys)
{
    PyOrderedDictEntry **newtable, *item;
    Py_ssize_t size = mp->ma_used * sizeof(PyOrderedDictEntry *), i, oldindex;
    PyObject *key = NULL;
    PyObject *it;
    Py_hash_t hash;

    if (PySortedDict_CheckExact(mp)) {
        PyErr_SetString(PyExc_TypeError,
                        "sorteddict does not support setkeys() assignment");
        return NULL;
    }

    /* determine length -> ok if ok
    if ok, then we still don't know if all keys will be found
    	so we allocate an array of ma_mask+1 size (which is what was used for
    	last resize and start filling that.
    	On finish, memcopy (so we don't have to worry about where the
    	values actually are (allocated or in smallbuffer), and
    	delete the tmp stuff,
    	if some key cannot be found (or is double) we don't update
    */

    newtable = PyMem_NEW(PyOrderedDictEntry *, size);
    if (newtable == NULL) {
        PyErr_NoMemory();
        return NULL;
    }

    i = PyObject_Length(keys);
    if ((i >=0) && (i != mp->ma_used)) {
        PyErr_Format(PyExc_ValueError,
                     "ordereddict setkeys requires sequence of length #%zd; "
                     "provided was length %zd",
                     mp->ma_used, i);
        return NULL;
    }
    if (i == -1) PyErr_Clear();


    it = PyObject_GetIter(keys);
    if (it == NULL)
        return NULL;

    for (i = 0; ; ++i) {
        key = PyIter_Next(it);
        if (key == NULL) {
            if (PyErr_Occurred()) break;
            if (i != mp->ma_used) {
                PyErr_Format(PyExc_ValueError,
                             "ordereddict setkeys requires sequence of length #%zd; "
                             "provided was length %zd",
                             mp->ma_used, i);
                break;
            }
            memcpy(mp->od_otablep, newtable, size);
            PyMem_DEL(newtable);
            Py_DECREF(it);
            Py_RETURN_NONE;
        }
        if (i >= mp->ma_used) {
            PyErr_Format(PyExc_ValueError,
                         "ordereddict setkeys requires sequence of max length #%zd; "
                         "a longer sequence was provided",
                         mp->ma_used);
            Py_DECREF(it);
            return NULL;
        }
        /* find the item with this key */
        if (!PyUNISTR_CheckExact(key) ||
                (hash = ((PyUNISTR_Object *) key)->OB_HASH) == -1) {
            hash = PyObject_Hash(key);
            if (hash == -1)
                break;
        }
        item = (mp->ma_lookup)(mp, key, hash);
        if (item == NULL || item->me_value == NULL) {
            PyErr_Format(PyExc_KeyError,
                         "ordereddict setkeys unknown key at pos " SPR,
                         i);
            break;
        }
        /* PyObject_Print((PyObject *)item->me_key, stdout, 0);*/
        /* check if a pointer to this item has been set */
        for (oldindex = 0; oldindex < i; oldindex++) {
            if (newtable[oldindex] == item) {
                PyErr_Format(PyExc_KeyError,
                             "ordereddict setkeys same key at pos " SPR "and " SPR,
                             oldindex, i);
                break;
            }
        }
        /* insert the pointer to this item */
        newtable[i] = item;
    }
    PyMem_DEL(newtable);
    Py_XDECREF(key);
    Py_DECREF(it);
    return NULL;
}

static PyObject *
dict_setvalues(register PyOrderedDictObject *mp, PyObject *values)
{
    PyObject *it;	/* iter(seq2) */
    Py_ssize_t i;	/* index into seq2 of current element */
    PyObject *item = NULL;	/* values[i] */
    PyOrderedDictEntry **epp = mp->od_otablep, *tmp;

    assert(mp != NULL);
    assert(PyOrderedDict_Check(mp));
    assert(values != NULL);

    i = PyObject_Length(values);
    /* printf("\nlength %d %d\n", i, mp->ma_used); */
    if ((i >=0) && (i != mp->ma_used)) {
        PyErr_Format(PyExc_ValueError,
                     "ordereddict setvalues requires sequence of length #%zd; "
                     "provided was length %zd",
                     mp->ma_used, i);
        return NULL;
    }
    if (i == -1) PyErr_Clear();


    it = PyObject_GetIter(values);
    if (it == NULL)
        return NULL;

    for (i = 0; ; ++i) {
        item = PyIter_Next(it);
        if (item == NULL) {
            if (PyErr_Occurred()) break;
            if (i != mp->ma_used) {
                PyErr_Format(PyExc_ValueError,
                             "ordereddict setvalues requires sequence of length #%zd; "
                             "provided was length %zd, ordereddict partially updated",
                             mp->ma_used, i);
                break;
            }
            Py_DECREF(it);
            Py_RETURN_NONE;
        }
        if (i >= mp->ma_used) {
            PyErr_Format(PyExc_ValueError,
                         "ordereddict setvalues requires sequence of max length #%zd; "
                         "a longer sequence was provided, ordereddict fully updated",
                         mp->ma_used);
            Py_DECREF(it);
            return NULL;
        }
        tmp = *epp++;
        Py_DECREF(tmp->me_value);
        tmp->me_value = item;
    }
    Py_XDECREF(item);
    Py_DECREF(it);
    return NULL;
}

static PyObject *
dict_setitems(register PyObject *mp,  PyObject *args, PyObject *kwds)
{
    PyOrderedDict_Clear((PyObject *)mp);
    if (dict_update_common(mp, args, kwds, "|Oi:setitems") != -1)
        Py_RETURN_NONE;
    return NULL;
}

static PyObject *
dict_rename(register PyOrderedDictObject *mp, PyObject *args)
{
    PyObject *oldkey, *newkey;
    PyObject *val = NULL;
    Py_hash_t hash;
    PyOrderedDictEntry *ep, **epp;
    register Py_ssize_t index;

    if (PySortedDict_CheckExact(mp)) {
        PyErr_SetString(PyExc_TypeError,
                        "sorteddict does not support rename()");
        return NULL;
    }
    if (!PyArg_UnpackTuple(args, "get", 1, 2, &oldkey, &newkey))
        return NULL;

    if (!PyUNISTR_CheckExact(oldkey) ||
            (hash = ((PyUNISTR_Object *) oldkey)->OB_HASH) == -1) {
        hash = PyObject_Hash(oldkey);
        if (hash == -1)
            return NULL;
    }
    ep = (mp->ma_lookup)(mp, oldkey, hash);
    if (ep == NULL || ep->me_value == NULL)
        return NULL;
    epp = mp->od_otablep;
    for (index = 0; index < mp->ma_used; index++, epp++)
        if (*epp == ep)
            break;
    if (*epp != ep)
        return NULL; /* this is bad! */

    oldkey = ep->me_key; /* now point to key from item */
    val = ep->me_value;
    Py_INCREF(dummy);
    ep->me_key = dummy;
    ep->me_value = NULL;
    memmove(epp, epp+1, (mp->ma_used - index) * sizeof(PyOrderedDictEntry *));
    mp->ma_used--;
    Py_DECREF(oldkey);
    if(PyOrderedDict_InsertItem(mp, index, newkey, val) != 0)
        return NULL;
    Py_DECREF(val);
    Py_RETURN_NONE;
}

#if PY_VERSION_HEX < 0x03000000
#define REDUCE

/* support for pickling */
static PyObject *
dict_reduce(PyOrderedDictObject *self)
{
    PyObject *result, *it, *dict=NULL;
    it = dictiter_new(self, &PyOrderedDictIterItem_Type, NULL, NULL);
    dict = Py_None;
    Py_INCREF(dict);
    Py_INCREF(dict);
    if (PySortedDict_CheckExact(self)) {
        if (((PySortedDictObject *) self)->sd_cmp == NULL)
            printf("NULL!!!!\n");
        result = Py_BuildValue("O(()OOOi)NNO", self->ob_type,
                               ((PySortedDictObject *) self)->sd_cmp,
                               ((PySortedDictObject *) self)->sd_key,
                               ((PySortedDictObject *) self)->sd_value,
                               REVERSE(self), dict, dict, it);
    } else {
        result = Py_BuildValue("O(()ii)NNO", self->ob_type, RELAXED(self), KVIO(self), dict, dict, it);
    }
    return result;
}
#endif

static PyObject *
ordereddict_getstate(register PyOrderedDictObject *mp)
{
#if PY_MAJOR_VERSION >= 3
    return PyLong_FromLong(mp->od_state);
#else
    return PyInt_FromLong(mp->od_state);
#endif
}

static PyObject *
ordereddict_dump(register PyOrderedDictObject *mp)
{
    if (dump_ordereddict_head(mp) != -1)
        dump_otablep(mp);
    if (PySortedDict_CheckExact(mp))
        dump_sorteddict_fun((PySortedDictObject *) mp);
    Py_RETURN_NONE;
}

#if PY_VERSION_HEX < 0x03000000
PyDoc_STRVAR(has_key__doc__,
             "D.has_key(k) -> True if D has a key k, else False");
#endif

PyDoc_STRVAR(contains__doc__,
             "D.__contains__(k) -> True if D has a key k, else False");

#ifdef REDUCE
PyDoc_STRVAR(reduce__doc__, "Return state information for pickling.");
#endif

PyDoc_STRVAR(getitem__doc__, "x.__getitem__(y) <==> x[y]");

PyDoc_STRVAR(sizeof__doc__,
"D.__sizeof__() -> size of D in memory, in bytes");

PyDoc_STRVAR(get__doc__,
             "D.get(k[,d]) -> D[k] if k in D, else d.  d defaults to None.");

PyDoc_STRVAR(setdefault_doc__,
             "D.setdefault(k[,d]) -> D.get(k,d), also set D[k]=d if k not in D");

PyDoc_STRVAR(pop__doc__,
             "D.pop(k[,d]) -> v, remove specified key and return the corresponding value.\n\
If key is not found, d is returned if given, otherwise KeyError is raised");

PyDoc_STRVAR(popitem__doc__,
             "D.popitem([index]) -> (k, v), remove and return indexed (key, value) pair as a\n\
2-tuple (default is last); but raise KeyError if D is empty.");

#if PY_VERSION_HEX < 0x03000000
PyDoc_STRVAR(keys__doc__,
             "D.keys([reverse=False]) -> list of D's keys, optionally reversed");

PyDoc_STRVAR(items__doc__,
             "D.items() -> list of D's (key, value) pairs, as 2-tuples");

PyDoc_STRVAR(values__doc__,
             "D.values() -> list of D's values");
#endif

PyDoc_STRVAR(update__doc__,
"D.update([E,] **F) -> None.  Update D from dict/iterable E and F.\n"
"If E present and has a .keys() method, does:     for k in E: D[k] = E[k]\n\
If E present and lacks .keys() method, does:     for (k, v) in E: D[k] = v\n\
In either case, this is followed by: for k in F: D[k] = F[k]");

PyDoc_STRVAR(fromkeys__doc__,
             "dict.fromkeys(S[,v]) -> New dict with keys from S and values equal to v.\n\
v defaults to None.");

PyDoc_STRVAR(clear__doc__,
             "D.clear() -> None.  Remove all items from D.");

PyDoc_STRVAR(copy__doc__,
             "D.copy() -> a shallow copy of D");

#if PY_VERSION_HEX < 0x03000000
PyDoc_STRVAR(iterkeys__doc__,
             "D.iterkeys([reverse=False]) -> an iterator over the keys of D");

PyDoc_STRVAR(itervalues__doc__,
             "D.itervalues() -> an iterator over the values of D");

PyDoc_STRVAR(iteritems__doc__,
             "D.iteritems() -> an iterator over the (key, value) items of D");
#endif

PyDoc_STRVAR(index_doc,
             "D.index(key) -> return position of key in ordered dict");

PyDoc_STRVAR(insert_doc,
             "D.insert(index, key, value) -> add/update (key, value) and insert key at index");

PyDoc_STRVAR(reverse_doc,
             "D.reverse() -> reverse the order of the keys of D");

PyDoc_STRVAR(setkeys_doc,
             "D.setkeys(keys) -> set the keys of D (keys must be iterable and a permutation of .keys())");

PyDoc_STRVAR(setvalues_doc,
             "D.setvalues(values) -> set D values to values (must be iterable)");

PyDoc_STRVAR(setitems_doc,
             "D.setitems(items) -> clear D and then set items");

PyDoc_STRVAR(rename_doc,
             "D.rename(oldkey, newkey) -> exchange keys without changing order");

PyDoc_STRVAR(getstate_doc,
             "D.getstate() -> return the state integer");

PyDoc_STRVAR(dump_doc,
             "D.dump() -> print internals of an orereddict");


/* Forward */
static PyObject *dictkeys_new(PyObject *);
static PyObject *dictitems_new(PyObject *);
static PyObject *dictvalues_new(PyObject *);

#if PY_VERSION_HEX < 0x03000000
PyDoc_STRVAR(viewkeys__doc__,
             "D.viewkeys() -> a set-like object providing a view on D's keys");
PyDoc_STRVAR(viewitems__doc__,
             "D.viewitems() -> a set-like object providing a view on D's items");
PyDoc_STRVAR(viewvalues__doc__,
             "D.viewvalues() -> an object providing a view on D's values");
#else
PyDoc_STRVAR(viewkeys__doc__,
             "D.keys() -> a set-like object providing a view on D's keys");
PyDoc_STRVAR(viewitems__doc__,
             "D.items() -> a set-like object providing a view on D's items");
PyDoc_STRVAR(viewvalues__doc__,
             "D.values() -> an object providing a view on D's values");
#endif

static PyMethodDef ordereddict_methods[] = {
    {
        "__contains__",(PyCFunction)dict_contains,   METH_O | METH_COEXIST,
        contains__doc__
    },
    {
        "__getitem__", (PyCFunction)dict_subscript, METH_O | METH_COEXIST,
        getitem__doc__
    },
    {"__sizeof__",      (PyCFunction)dict_sizeof,       METH_NOARGS,
     sizeof__doc__},
#ifdef REDUCE

    {"__reduce__", (PyCFunction)dict_reduce, METH_NOARGS, reduce__doc__},
#endif
#if PY_VERSION_HEX < 0x03000000
    {
        "has_key",	(PyCFunction)dict_has_key,      METH_O,
        has_key__doc__
    },
#endif
    {
        "get",         (PyCFunction)dict_get,          METH_VARARGS,
        get__doc__
    },
    {
        "setdefault",  (PyCFunction)dict_setdefault,   METH_VARARGS,
        setdefault_doc__
    },
    {
        "pop",         (PyCFunction)dict_pop,          METH_VARARGS,
        pop__doc__
    },
    {
        "popitem",	(PyCFunction)dict_popitem,	METH_VARARGS,
        popitem__doc__
    },
#if PY_VERSION_HEX < 0x03000000
    {
        "keys",	(PyCFunction)dict_keys,		METH_VARARGS | METH_KEYWORDS,
        keys__doc__
    },
    {
        "items",	(PyCFunction)dict_items,	METH_VARARGS | METH_KEYWORDS,
        items__doc__
    },
    {
        "values",	(PyCFunction)dict_values,	METH_VARARGS | METH_KEYWORDS,
        values__doc__
    },

#if PY_VERSION_HEX >= 0x02070000
    {"viewkeys",        (PyCFunction)dictkeys_new,      METH_NOARGS,
     viewkeys__doc__},
    {"viewitems",       (PyCFunction)dictitems_new,     METH_NOARGS,
     viewitems__doc__},
    {"viewvalues",      (PyCFunction)dictvalues_new,    METH_NOARGS,
     viewvalues__doc__},
#endif
#else  /* Py3K */
    {"keys",        (PyCFunction)dictkeys_new,      METH_NOARGS,
     viewkeys__doc__},
    {"items",       (PyCFunction)dictitems_new,     METH_NOARGS,
     viewitems__doc__},
    {"values",      (PyCFunction)dictvalues_new,    METH_NOARGS,
     viewvalues__doc__},
#endif
    {
        "update",	(PyCFunction)dict_update,	METH_VARARGS | METH_KEYWORDS,
        update__doc__
    },
    {
        "fromkeys",	(PyCFunction)dict_fromkeys,	METH_VARARGS | METH_CLASS,
        fromkeys__doc__
    },
    {
        "clear",	(PyCFunction)dict_clear,	METH_NOARGS,
        clear__doc__
    },
    {
        "copy",	(PyCFunction)dict_copy,		METH_NOARGS,
        copy__doc__
    },
#if PY_VERSION_HEX < 0x03000000
    {
        "iterkeys",	(PyCFunction)dict_iterkeys,	METH_VARARGS | METH_KEYWORDS,
        iterkeys__doc__
    },
    {
        "itervalues",	(PyCFunction)dict_itervalues,	METH_VARARGS | METH_KEYWORDS,
        itervalues__doc__
    },
    {
        "iteritems",	(PyCFunction)dict_iteritems,	METH_VARARGS | METH_KEYWORDS,
        iteritems__doc__
    },
#endif
    {"index",       (PyCFunction)dict_index,     METH_O, index_doc},
    {"insert",      (PyCFunction)dict_insert,    METH_VARARGS, insert_doc},
    {"reverse",     (PyCFunction)dict_reverse,   METH_NOARGS, reverse_doc},
    {"setkeys",     (PyCFunction)dict_setkeys,   METH_O, setkeys_doc},
    {"setvalues",   (PyCFunction)dict_setvalues, METH_O, setvalues_doc},
    {"setitems",    (PyCFunction)dict_setitems,  METH_VARARGS | METH_KEYWORDS, setitems_doc},
    {"rename",     (PyCFunction)dict_rename,   METH_VARARGS, rename_doc},
    {"getstate",     (PyCFunction)ordereddict_getstate,   METH_NOARGS, getstate_doc},
    {"dump",     (PyCFunction)ordereddict_dump,   METH_NOARGS, dump_doc},
    {NULL,		NULL}	/* sentinel */
};

/* Return 1 if `key` is in dict `op`, 0 if not, and -1 on error. */
int
PyOrderedDict_Contains(PyObject *op, PyObject *key)
{
    Py_hash_t hash;
    PyOrderedDictObject *mp = (PyOrderedDictObject *)op;
    PyOrderedDictEntry *ep;

    if (!PyUNISTR_CheckExact(key) ||
            (hash = ((PyUNISTR_Object *) key)->OB_HASH) == -1) {
        hash = PyObject_Hash(key);
        if (hash == -1)
            return -1;
    }
    ep = (mp->ma_lookup)(mp, key, hash);
    return ep == NULL ? -1 : (ep->me_value != NULL);
}

/* Internal version of PyOrderedDict_Contains used when the hash value is already known */
int
_PyOrderedDict_Contains(PyObject *op, PyObject *key, Py_hash_t hash)
{
    PyOrderedDictObject *mp = (PyOrderedDictObject *)op;
    PyOrderedDictEntry *ep;

    ep = (mp->ma_lookup)(mp, key, hash);
    return ep == NULL ? -1 : (ep->me_value != NULL);
}

static PyObject *
PyOderedDict_Slice(PyObject *op, register Py_ssize_t ilow,
                   register Py_ssize_t ihigh)
{
    PyOrderedDictObject *mp = (PyOrderedDictObject *)op;
    PyOrderedDictObject *slice;

    if (mp == NULL || !PyOrderedDict_Check(mp)) {
        PyErr_BadInternalCall();
        return NULL;
    }
    slice = (PyOrderedDictObject *) PyOrderedDict_New();
    if (slice == NULL)
        return NULL;
    /* [:] -> ilow = 0, ihigh MAXINT */
    if (ilow < 0)
        ilow += mp->ma_used;
    if (ihigh < 0)
        ihigh += mp->ma_used;
    if (ilow < 0)
        ilow = 0;
    else if (ilow > mp->ma_used)
        ilow = mp->ma_used;
    if (ihigh < ilow)
        ihigh = ilow;
    else if (ihigh > mp->ma_used)
        ihigh = mp->ma_used;

    if (PyOrderedDict_CopySome((PyObject *) slice,
                               op, ilow, 1, (ihigh-ilow), 1) == 0) {
        return (PyObject *) slice;
    }
    Py_DECREF(slice);
    return NULL;
}

/* Hack to implement "key in dict" */
static PySequenceMethods dict_as_sequence = {
    0,			/* sq_length */
    0,			/* sq_concat */
    0,			/* sq_repeat */
    0,			/* sq_item */
    (ssizessizeargfunc)PyOderedDict_Slice,			/* sq_slice */
    0,			/* sq_ass_item */
    (ssizessizeobjargproc)dict_ass_slice,			/* sq_ass_slice */
    PyOrderedDict_Contains,	/* sq_contains */
    0,			/* sq_inplace_concat */
    0,			/* sq_inplace_repeat */
};

static PyObject *
dict_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    PyObject *self;

    assert(type != NULL && type->tp_alloc != NULL);
    assert(dummy != NULL);
    self = type->tp_alloc(type, 0);
    if (self != NULL) {
        PyOrderedDictObject *d = (PyOrderedDictObject *)self;
        /* It's guaranteed that tp->alloc zeroed out the struct. */
        assert(d->ma_table == NULL && d->od_fill == 0 && d->ma_used == 0);
        INIT_NONZERO_DICT_SLOTS(d);
        d->ma_lookup = lookdict_string;
        /* The object has been implicitly tracked by tp_alloc */
        if (type == &PyOrderedDict_Type)
            _PyObject_GC_UNTRACK(d);
#ifdef SHOW_CONVERSION_COUNTS
        ++created;
#endif
#ifdef SHOW_TRACK_COUNT
        if (_PyObject_GC_IS_TRACKED(d))
            count_tracked++;
        else
            count_untracked++;
#endif
    }
    return self;
}

static PyObject *
sorteddict_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    PyObject *self;

    assert(type != NULL && type->tp_alloc != NULL);
    assert(dummy != NULL);
    self = type->tp_alloc(type, 0);
    if (self != NULL) {
        PyOrderedDictObject *d = (PyOrderedDictObject *)self;
        /* It's guaranteed that tp->alloc zeroed out the struct. */
        assert(d->ma_table == NULL && d->od_fill == 0 && d->ma_used == 0);
        INIT_NONZERO_DICT_SLOTS(d);
        d->ma_lookup = lookdict_string;
        INIT_SORT_FUNCS(((PySortedDictObject *) self));
        if (type == &PySortedDict_Type)
            _PyObject_GC_UNTRACK(d);
#ifdef SHOW_CONVERSION_COUNTS
        ++created;
#endif
#ifdef SHOW_TRACK_COUNT
        if (_PyObject_GC_IS_TRACKED(d))
            count_tracked++;
        else
            count_untracked++;
#endif
    }
    return self;
}

static int
ordereddict_init(PyObject *self, PyObject *args, PyObject *kwds)
{
    PyObject *arg = NULL;
    int result = 0, tmprelax = -1, tmpkvio = -1;

    static char *kwlist[] = {"src", "relax", "kvio", 0};
    if (args != NULL) {
        if (!PyArg_ParseTupleAndKeywords(args, kwds, "|Oii:ordereddict",
                                         kwlist, &arg,  &tmprelax, &tmpkvio)) {
            return -1;
        }
    }
    if (tmpkvio == -1)
        tmpkvio = ordereddict_kvio;
    if (tmpkvio)
        ((PyOrderedDictObject *)self)->od_state |= OD_KVIO_BIT;
    if (tmprelax == -1)
        tmprelax = ordereddict_relaxed;
    if (tmprelax)
        ((PyOrderedDictObject *)self)->od_state |= OD_RELAXED_BIT;

    if (arg != NULL) {
        if (PyObject_HasAttrString(arg, "keys"))
            result = PyOrderedDict_Merge(self, arg, 1, tmprelax);
        else
            result = PyOrderedDict_MergeFromSeq2(self, arg, 1);
    }
    /* do not initialise from keywords at all */
    return result;
}

static int
sorteddict_init(PyObject *self, PyObject *args, PyObject *kwds)
{
    PyObject *arg = NULL, *cmpfun = NULL, *keyfun = NULL, *valuefun = NULL;
    int result = 0, reverse = 0;

    static char *kwlist[] = {"src", "cmp", "key", "value", "reverse", 0};
    if (args != NULL)
        if (!PyArg_ParseTupleAndKeywords(args, kwds, "|OOOOi:sorteddict",
                                         kwlist, &arg, &cmpfun, &keyfun, &valuefun, &reverse))
            return -1;
    if (reverse)
        ((PyOrderedDictObject *)self)->od_state |= OD_REVERSE_BIT;
    /* always relaxed about order of source */
    ((PyOrderedDictObject *)self)->od_state |= OD_RELAXED_BIT;

    if (keyfun != NULL && keyfun != Py_False)
        ((PySortedDictObject *)self)->sd_key = keyfun;

    if (arg != NULL) {
        if (PyObject_HasAttrString(arg, "keys"))
            result = PyOrderedDict_Merge(self, arg, 1, 1);
        else
            result = PyOrderedDict_MergeFromSeq2(self, arg, 1);
    }
    /* do not initialise from keywords at all */
    return result;
}

static PyObject *
dict_iter(PyOrderedDictObject *dict)
{
    return dictiter_new(dict, &PyOrderedDictIterKey_Type, NULL, NULL);
}

PyDoc_STRVAR(ordereddict_doc,
             "ordereddict() -> new empty dictionary.\n"
             "dict(orderddict) -> new dictionary initialized from a mappings object's\n"
             "    (key, value) pairs.\n"
//"dict(iterable) -> new dictionary initialized as if via:\n"
//"    d = {}\n"
//"    for k, v in iterable:\n"
//"        d[k] = v\n"
//"dict(**kwargs) -> new dictionary initialized with the name=value pairs\n"
//"    in the keyword argument list.  For example:  dict(one=1, two=2)"
            );

PyTypeObject PyOrderedDict_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_ordereddict.ordereddict",
    sizeof(PyOrderedDictObject),
    0,
    (destructor)dict_dealloc,           /* tp_dealloc */
#if PY_MAJOR_VERSION < 3
    (printfunc)ordereddict_print,       /* tp_print */
#else
    0,			                /* tp_print */
#endif
    0,					/* tp_getattr */
    0,					/* tp_setattr */
#if PY_MAJOR_VERSION < 3
    (cmpfunc)dict_compare,		/* tp_compare */
#else
    0,                                  /* tp_reserved */
#endif
    (reprfunc)ordereddict_repr,		/* tp_repr */
    0,					/* tp_as_number */
    &dict_as_sequence,			/* tp_as_sequence */
    &dict_as_mapping,			/* tp_as_mapping */
    (hashfunc)PyObject_HashNotImplemented,  /* tp_hash */
    0,					/* tp_call */
    0,					/* tp_str */
    PyObject_GenericGetAttr,		/* tp_getattro */
    0,					/* tp_setattro */
    0,					/* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC |
    Py_TPFLAGS_BASETYPE | Py_TPFLAGS_DICT_SUBCLASS, /* tp_flags */
    ordereddict_doc,			/* tp_doc */
    dict_traverse,			/* tp_traverse */
    dict_tp_clear,			/* tp_clear */
    dict_richcompare,			/* tp_richcompare */
    0,					/* tp_weaklistoffset */
    (getiterfunc)dict_iter,		/* tp_iter */
    0,					/* tp_iternext */
    ordereddict_methods,		/* tp_methods */
    0,					/* tp_members */
    0,					/* tp_getset */
    DEFERRED_ADDRESS(&PyDict_Type),					/* tp_base */
    0,					/* tp_dict */
    0,					/* tp_descr_get */
    0,					/* tp_descr_set */
    0,					/* tp_dictoffset */
    ordereddict_init,			/* tp_init */
    PyType_GenericAlloc,		/* tp_alloc */
    dict_new,				/* tp_new */
    PyObject_GC_Del,        		/* tp_free */
};


PyDoc_STRVAR(sorteddict_doc,
             "sorteddict() -> new empty dictionary.\n"
            );


PyTypeObject PySortedDict_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_ordereddict.sorteddict",
    sizeof(PySortedDictObject),
    0,
    (destructor)dict_dealloc,		/* tp_dealloc */
#if PY_MAJOR_VERSION < 3
    (printfunc)ordereddict_print,			/* tp_print */
#else
    0,			                /* tp_print */
#endif
    0,					/* tp_getattr */
    0,					/* tp_setattr */
#if PY_MAJOR_VERSION < 3
    (cmpfunc)dict_compare,			/* tp_compare */
#else
    0,                                          /* tp_reserved */
#endif
    (reprfunc)sorteddict_repr,			/* tp_repr */
    0,					/* tp_as_number */
    &dict_as_sequence,			/* tp_as_sequence */
    &dict_as_mapping,			/* tp_as_mapping */
    (hashfunc)PyObject_HashNotImplemented,  /* tp_hash */
    0,					/* tp_call */
    0,					/* tp_str */
    PyObject_GenericGetAttr,		/* tp_getattro */
    0,					/* tp_setattro */
    0,					/* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC |
    Py_TPFLAGS_BASETYPE | Py_TPFLAGS_DICT_SUBCLASS, /* tp_flags */
    sorteddict_doc,				/* tp_doc */
    dict_traverse,				/* tp_traverse */
    dict_tp_clear,				/* tp_clear */
    dict_richcompare,			/* tp_richcompare */
    0,					/* tp_weaklistoffset */
    (getiterfunc)dict_iter,			/* tp_iter */
    0,					/* tp_iternext */
    ordereddict_methods,				/* tp_methods */
    0,					/* tp_members */
    0,					/* tp_getset */
    DEFERRED_ADDRESS(&PyDict_Type),					/* tp_base */
    0,					/* tp_dict */
    0,					/* tp_descr_get */
    0,					/* tp_descr_set */
    0,					/* tp_dictoffset */
    sorteddict_init,				/* tp_init */
    PyType_GenericAlloc,			/* tp_alloc */
    sorteddict_new,				/* tp_new */
    PyObject_GC_Del,        		/* tp_free */
};



/* Dictionary iterator types */

typedef struct {
    PyObject_HEAD
    PyOrderedDictObject *di_dict; /* Set to NULL when iterator is exhausted */
    Py_ssize_t di_used;
    Py_ssize_t di_pos;
    PyObject* di_result; /* reusable result tuple for iteritems */
    Py_ssize_t len;
    int step;
} ordereddictiterobject;

static PyObject *
dictiter_new(PyOrderedDictObject *dict, PyTypeObject *itertype,
             PyObject *args, PyObject *kwds)
{
    ordereddictiterobject *di;
    int reverse = 0;
    static char *kwlist[] = {"reverse", 0};

    if (args != NULL)
        if (!PyArg_ParseTupleAndKeywords(args, kwds, "|i:keys",
                                         kwlist, &reverse))
            return NULL;

    /* review: introduce GC_New */
    di = PyObject_GC_New(ordereddictiterobject, itertype);
    if (di == NULL)
        return NULL;
    Py_INCREF(dict);
    di->di_dict = dict;
    di->di_used = dict->ma_used;
    di->len = dict->ma_used;
    if (reverse) {
        di->di_pos = (dict->ma_used) - 1;
        di->step = -1;
    } else {
        di->di_pos = 0;
        di->step = 1;
    }
    if (itertype == &PyOrderedDictIterItem_Type) {
        di->di_result = PyTuple_Pack(2, Py_None, Py_None);
        if (di->di_result == NULL) {
            Py_DECREF(di);
            return NULL;
        }
    } else
        di->di_result = NULL;
    PyObject_GC_Track(di);
    return (PyObject *)di;
}

static void
dictiter_dealloc(ordereddictiterobject *di)
{
    Py_XDECREF(di->di_dict);
    Py_XDECREF(di->di_result);
    PyObject_GC_Del(di);
}

static int
dictiter_traverse(ordereddictiterobject *di, visitproc visit, void *arg)
{
    Py_VISIT(di->di_dict);
    Py_VISIT(di->di_result);
    return 0;
}

static PyObject *
dictiter_len(ordereddictiterobject *di)
{
    Py_ssize_t len = 0;
    if (di->di_dict != NULL && di->di_used == di->di_dict->ma_used)
        len = di->len;
#if PY_VERSION_HEX < 0x03000000
    return PyInt_FromSize_t(len);
#else
    return PyLong_FromSize_t(len);
#endif
}

PyDoc_STRVAR(length_hint_doc, "Private method returning an estimate of len(list(it)).");

static PyMethodDef dictiter_methods[] = {
    {"__length_hint__", (PyCFunction)dictiter_len, METH_NOARGS, length_hint_doc},
    {NULL,		NULL}		/* sentinel */
};

static PyObject *dictiter_iternextkey(ordereddictiterobject *di)
{
    PyObject *key;
    register Py_ssize_t i;
    register PyOrderedDictEntry **epp;
    PyOrderedDictObject *d = di->di_dict;

    if (d == NULL)
        return NULL;
    assert (PyOrderedDict_Check(d));

    if (di->di_used != d->ma_used) {
        PyErr_SetString(PyExc_RuntimeError,
                        "dictionary changed size during iteration");
        di->di_used = -1; /* Make this state sticky */
        return NULL;
    }

    i = di->di_pos;
    if (i < 0)
        goto fail;
    if (i >= d->ma_used)
        goto fail;
    epp = d->od_otablep;
    di->di_pos = i+di->step;
    di->len--; /* len can be calculated */
    key = epp[i]->me_key;
    Py_INCREF(key);
    return key;

fail:
    Py_DECREF(d);
    di->di_dict = NULL;
    return NULL;
}

PyTypeObject PyOrderedDictIterKey_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_ordereddict.keyiterator",		/* tp_name */
    sizeof(ordereddictiterobject),			/* tp_basicsize */
    0,					/* tp_itemsize */
    /* methods */
    (destructor)dictiter_dealloc, 		/* tp_dealloc */
    0,					/* tp_print */
    0,					/* tp_getattr */
    0,					/* tp_setattr */
    0,					/* tp_compare */
    0,					/* tp_repr */
    0,					/* tp_as_number */
    0,					/* tp_as_sequence */
    0,					/* tp_as_mapping */
    0,					/* tp_hash */
    0,					/* tp_call */
    0,					/* tp_str */
    PyObject_GenericGetAttr,		/* tp_getattro */
    0,					/* tp_setattro */
    0,					/* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,	/* tp_flags */
    0,					/* tp_doc */
    (traverseproc)dictiter_traverse,    /* tp_traverse */
    0,					/* tp_clear */
    0,					/* tp_richcompare */
    0,					/* tp_weaklistoffset */
    PyObject_SelfIter,			/* tp_iter */
    (iternextfunc)dictiter_iternextkey,	/* tp_iternext */
    dictiter_methods,			/* tp_methods */
    0,
};

static PyObject *dictiter_iternextvalue(ordereddictiterobject *di)
{
    PyObject *value;
    register Py_ssize_t i;
    register PyOrderedDictEntry **epp;
    PyOrderedDictObject *d = di->di_dict;

    if (d == NULL)
        return NULL;
    assert (PyOrderedDict_Check(d));

    if (di->di_used != d->ma_used) {
        PyErr_SetString(PyExc_RuntimeError,
                        "dictionary changed size during iteration");
        di->di_used = -1; /* Make this state sticky */
        return NULL;
    }

    i = di->di_pos;
    if (i < 0 || i >= d->ma_used)
        goto fail;
    epp = d->od_otablep;
    di->di_pos = i+di->step;
    di->len--; /* len can be calculated */
    value = epp[i]->me_value;
    Py_INCREF(value);
    return value;

fail:
    Py_DECREF(d);
    di->di_dict = NULL;
    return NULL;
}

PyTypeObject PyOrderedDictIterValue_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_ordereddict.valueiterator",		/* tp_name */
    sizeof(ordereddictiterobject),			/* tp_basicsize */
    0,					/* tp_itemsize */
    /* methods */
    (destructor)dictiter_dealloc, 		/* tp_dealloc */
    0,					/* tp_print */
    0,					/* tp_getattr */
    0,					/* tp_setattr */
    0,					/* tp_compare */
    0,					/* tp_repr */
    0,					/* tp_as_number */
    0,					/* tp_as_sequence */
    0,					/* tp_as_mapping */
    0,					/* tp_hash */
    0,					/* tp_call */
    0,					/* tp_str */
    PyObject_GenericGetAttr,		/* tp_getattro */
    0,					/* tp_setattro */
    0,					/* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,	/* tp_flags */
    0,					/* tp_doc */
    (traverseproc)dictiter_traverse,    /* tp_traverse */
    0,					/* tp_clear */
    0,					/* tp_richcompare */
    0,					/* tp_weaklistoffset */
    PyObject_SelfIter,			/* tp_iter */
    (iternextfunc)dictiter_iternextvalue,	/* tp_iternext */
    dictiter_methods,			/* tp_methods */
    0,
};

static PyObject *dictiter_iternextitem(ordereddictiterobject *di)
{
    PyObject *key, *value, *result = di->di_result;
    register Py_ssize_t i;
    register PyOrderedDictEntry **epp;
    PyOrderedDictObject *d = di->di_dict;

    if (d == NULL)
        return NULL;
    assert (PyOrderedDict_Check(d));

    if (di->di_used != d->ma_used) {
        PyErr_SetString(PyExc_RuntimeError,
                        "dictionary changed size during iteration");
        di->di_used = -1; /* Make this state sticky */
        return NULL;
    }

    i = di->di_pos;
    if (i < 0)
        goto fail;

    /* review: differs in 2.5.6 */
    if (i >= d->ma_used)
        goto fail;
    epp = d->od_otablep;
    di->di_pos = i+di->step;
    if (result->ob_refcnt == 1) {
        Py_INCREF(result);
        Py_DECREF(PyTuple_GET_ITEM(result, 0));
        Py_DECREF(PyTuple_GET_ITEM(result, 1));
    } else {
        result = PyTuple_New(2);
        if (result == NULL)
            return NULL;
    }
    di->len--; /* len can be calculated */
    key = epp[i]->me_key;
    value = epp[i]->me_value;
    Py_INCREF(key);
    Py_INCREF(value);
    PyTuple_SET_ITEM(result, 0, key);
    PyTuple_SET_ITEM(result, 1, value);
    return result;

fail:
    Py_DECREF(d);
    di->di_dict = NULL;
    return NULL;
}

PyTypeObject PyOrderedDictIterItem_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_ordereddict.itemiterator",		/* tp_name */
    sizeof(ordereddictiterobject),			/* tp_basicsize */
    0,					/* tp_itemsize */
    /* methods */
    (destructor)dictiter_dealloc, 		/* tp_dealloc */
    0,					/* tp_print */
    0,					/* tp_getattr */
    0,					/* tp_setattr */
    0,					/* tp_compare */
    0,					/* tp_repr */
    0,					/* tp_as_number */
    0,					/* tp_as_sequence */
    0,					/* tp_as_mapping */
    0,					/* tp_hash */
    0,					/* tp_call */
    0,					/* tp_str */
    PyObject_GenericGetAttr,		/* tp_getattro */
    0,					/* tp_setattro */
    0,					/* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,	/* tp_flags */
    0,					/* tp_doc */
    (traverseproc)dictiter_traverse,    /* tp_traverse */
    0,					/* tp_clear */
    0,					/* tp_richcompare */
    0,					/* tp_weaklistoffset */
    PyObject_SelfIter,			/* tp_iter */
    (iternextfunc)dictiter_iternextitem,	/* tp_iternext */
    dictiter_methods,			/* tp_methods */
    0,
};

/*******************************************************************/

static PyObject *
getset_relaxed(PyObject *self, PyObject *args)
{
    int n = -1, oldval = ordereddict_relaxed;
    if (!PyArg_ParseTuple(args, "|i", &n))
        return NULL;
    if (n != -1) {
        ordereddict_relaxed = n;
    }
    return PyBool_FromLong(oldval);
}

static PyObject *
getset_kvio(PyObject *self, PyObject *args)
{
    int n = -1, oldval = ordereddict_kvio;
    if (!PyArg_ParseTuple(args, "|i", &n))
        return NULL;
    if (n != -1) {
        ordereddict_kvio = n;
    }
    return PyBool_FromLong(oldval);
}

static PyMethodDef ordereddict_functions[] = {
    {
        "relax",	getset_relaxed,	METH_VARARGS,
        "get/set routine for allowing global undeordered dict initialisation"
    },
    {
        "kvio",	getset_kvio,	METH_VARARGS,
        "get/set routine for allowing global KeyValue Insertion Order initialisation"
    },
    {NULL,		NULL}		/* sentinel */
};

#if PY_VERSION_HEX >= 0x02070000
/* dictionary views are 2.7+ */

/***********************************************/
/* View objects for keys(), items(), values(). */
/***********************************************/

/* The instance lay-out is the same for all three; but the type differs. */

typedef struct {
    PyObject_HEAD
    PyOrderedDictObject *dv_dict;
} dictviewobject;

static void
dictview_dealloc(dictviewobject *dv)
{
    Py_XDECREF(dv->dv_dict);
    PyObject_GC_Del(dv);
}

static int
dictview_traverse(dictviewobject *dv, visitproc visit, void *arg)
{
    Py_VISIT(dv->dv_dict);
    return 0;
}

static Py_ssize_t
dictview_len(dictviewobject *dv)
{
    Py_ssize_t len = 0;
    if (dv->dv_dict != NULL)
        len = dv->dv_dict->ma_used;
    return len;
}

static PyObject *
dictview_new(PyObject *dict, PyTypeObject *type)
{
    dictviewobject *dv;
    if (dict == NULL) {
        PyErr_BadInternalCall();
        return NULL;
    }
    if (!PyDict_Check(dict)) {
        /* XXX Get rid of this restriction later */
        PyErr_Format(PyExc_TypeError,
                     "%s() requires a dict argument, not '%s'",
                     type->tp_name, dict->ob_type->tp_name);
        return NULL;
    }
    dv = PyObject_GC_New(dictviewobject, type);
    if (dv == NULL)
        return NULL;
    Py_INCREF(dict);
    dv->dv_dict = (PyOrderedDictObject *)dict;
    PyObject_GC_Track(dv);
    return (PyObject *)dv;
}

/* TODO(guido): The views objects are not complete:

 * support more set operations
 * support arbitrary mappings?
   - either these should be static or exported in dictobject.h
   - if public then they should probably be in builtins
*/

/* Return 1 if self is a subset of other, iterating over self;
   0 if not; -1 if an error occurred. */
static int
all_contained_in(PyObject *self, PyObject *other)
{
    PyObject *iter = PyObject_GetIter(self);
    int ok = 1;

    if (iter == NULL)
        return -1;
    for (;;) {
        PyObject *next = PyIter_Next(iter);
        if (next == NULL) {
            if (PyErr_Occurred())
                ok = -1;
            break;
        }
        ok = PySequence_Contains(other, next);
        Py_DECREF(next);
        if (ok <= 0)
            break;
    }
    Py_DECREF(iter);
    return ok;
}

static PyObject *
dictview_richcompare(PyObject *self, PyObject *other, int op)
{
    Py_ssize_t len_self, len_other;
    int ok;
    PyObject *result;

    assert(self != NULL);
    assert(PyDictViewSet_Check(self));
    assert(other != NULL);

    if (!PyAnySet_Check(other) && !PyDictViewSet_Check(other)) {
        Py_INCREF(Py_NotImplemented);
        return Py_NotImplemented;
    }

    len_self = PyObject_Size(self);
    if (len_self < 0)
        return NULL;
    len_other = PyObject_Size(other);
    if (len_other < 0)
        return NULL;

    ok = 0;
    switch(op) {

    case Py_NE:
    case Py_EQ:
        if (len_self == len_other)
            ok = all_contained_in(self, other);
        if (op == Py_NE && ok >= 0)
            ok = !ok;
        break;

    case Py_LT:
        if (len_self < len_other)
            ok = all_contained_in(self, other);
        break;

      case Py_LE:
          if (len_self <= len_other)
              ok = all_contained_in(self, other);
          break;

    case Py_GT:
        if (len_self > len_other)
            ok = all_contained_in(other, self);
        break;

    case Py_GE:
        if (len_self >= len_other)
            ok = all_contained_in(other, self);
        break;

    }
    if (ok < 0)
        return NULL;
    result = ok ? Py_True : Py_False;
    Py_INCREF(result);
    return result;
}

static PyObject *
dictview_repr(dictviewobject *dv)
{
    PyObject *seq;
    PyObject *result;
#if PY_MAJOR_VERSION < 3
    PyObject *seq_str;
#endif

    seq = PySequence_List((PyObject *)dv);
    if (seq == NULL)
        return NULL;

#if PY_MAJOR_VERSION < 3
    seq_str = PyObject_Repr(seq);
    if (seq_str == NULL) {
        Py_DECREF(seq);
        return NULL;
    }
    result = PyUNISTR_FromFormat("%s(%s)", Py_TYPE(dv)->tp_name,
                                 PyString_AS_STRING(seq_str));
    Py_DECREF(seq_str);
#else
    result = PyUnicode_FromFormat("%s(%R)", Py_TYPE(dv)->tp_name, seq);
#endif
    Py_DECREF(seq);
    return result;
}

/*** dict_keys ***/

static PyObject *
dictkeys_iter(dictviewobject *dv)
{
    if (dv->dv_dict == NULL) {
        Py_RETURN_NONE;
    }
    return dictiter_new(dv->dv_dict, &PyOrderedDictIterKey_Type, NULL, NULL);
}

static int
dictkeys_contains(dictviewobject *dv, PyObject *obj)
{
    if (dv->dv_dict == NULL)
        return 0;
    return PyDict_Contains((PyObject *)dv->dv_dict, obj);
}

static PySequenceMethods dictkeys_as_sequence = {
    (lenfunc)dictview_len,              /* sq_length */
    0,                                  /* sq_concat */
    0,                                  /* sq_repeat */
    0,                                  /* sq_item */
    0,                                  /* sq_slice */
    0,                                  /* sq_ass_item */
    0,                                  /* sq_ass_slice */
    (objobjproc)dictkeys_contains,      /* sq_contains */
};

static PyObject*
dictviews_sub(PyObject* self, PyObject *other)
{
    PyObject *result = PySet_New(self);
    PyObject *tmp;
    if (result == NULL)
        return NULL;

    tmp = PyObject_CallMethod(result, "difference_update", "O", other);
    if (tmp == NULL) {
        Py_DECREF(result);
        return NULL;
    }

    Py_DECREF(tmp);
    return result;
}

static PyObject*
dictviews_and(PyObject* self, PyObject *other)
{
    PyObject *result = PySet_New(self);
    PyObject *tmp;
    if (result == NULL)
        return NULL;

    tmp = PyObject_CallMethod(result, "intersection_update", "O", other);
    if (tmp == NULL) {
        Py_DECREF(result);
        return NULL;
    }

    Py_DECREF(tmp);
    return result;
}

static PyObject*
dictviews_or(PyObject* self, PyObject *other)
{
    PyObject *result = PySet_New(self);
    PyObject *tmp;
    if (result == NULL)
        return NULL;

    tmp = PyObject_CallMethod(result, "update", "O", other);
    if (tmp == NULL) {
        Py_DECREF(result);
        return NULL;
    }

    Py_DECREF(tmp);
    return result;
}

static PyObject*
dictviews_xor(PyObject* self, PyObject *other)
{
    PyObject *result = PySet_New(self);
    PyObject *tmp;
    if (result == NULL)
        return NULL;

    tmp = PyObject_CallMethod(result, "symmetric_difference_update", "O",
                              other);
    if (tmp == NULL) {
        Py_DECREF(result);
        return NULL;
    }

    Py_DECREF(tmp);
    return result;
}

static PyNumberMethods dictviews_as_number = {
    0,                                  /*nb_add*/
    (binaryfunc)dictviews_sub,          /*nb_subtract*/
    0,                                  /*nb_multiply*/
#if PY_MAJOR_VERSION < 3
    0,                                  /*nb_divide*/
#endif
    0,                                  /*nb_remainder*/
    0,                                  /*nb_divmod*/
    0,                                  /*nb_power*/
    0,                                  /*nb_negative*/
    0,                                  /*nb_positive*/
    0,                                  /*nb_absolute*/
    0,                                  /*nb_nonzero/nb_bool*/
    0,                                  /*nb_invert*/
    0,                                  /*nb_lshift*/
    0,                                  /*nb_rshift*/
    (binaryfunc)dictviews_and,          /*nb_and*/
    (binaryfunc)dictviews_xor,          /*nb_xor*/
    (binaryfunc)dictviews_or,           /*nb_or*/
};

static PyMethodDef dictkeys_methods[] = {
    {NULL,              NULL}           /* sentinel */
};

PyTypeObject PyOrderedDictKeys_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "dict_keys",                                /* tp_name */
    sizeof(dictviewobject),                     /* tp_basicsize */
    0,                                          /* tp_itemsize */
    /* methods */
    (destructor)dictview_dealloc,               /* tp_dealloc */
    0,                                          /* tp_print */
    0,                                          /* tp_getattr */
    0,                                          /* tp_setattr */
    0,                                          /* tp_reserved */
    (reprfunc)dictview_repr,                    /* tp_repr */
    &dictviews_as_number,                       /* tp_as_number */
    &dictkeys_as_sequence,                      /* tp_as_sequence */
    0,                                          /* tp_as_mapping */
    0,                                          /* tp_hash */
    0,                                          /* tp_call */
    0,                                          /* tp_str */
    PyObject_GenericGetAttr,                    /* tp_getattro */
    0,                                          /* tp_setattro */
    0,                                          /* tp_as_buffer */
#if PY_MAJOR_VERSION < 3
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC |
        Py_TPFLAGS_CHECKTYPES,                  /* tp_flags */
#else
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,     /* tp_flags */
#endif
    0,                                          /* tp_doc */
    (traverseproc)dictview_traverse,            /* tp_traverse */
    0,                                          /* tp_clear */
    dictview_richcompare,                       /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    (getiterfunc)dictkeys_iter,                 /* tp_iter */
    0,                                          /* tp_iternext */
    dictkeys_methods,                           /* tp_methods */
    0,
};

static PyObject *
dictkeys_new(PyObject *dict)
{
    return dictview_new(dict, &PyOrderedDictKeys_Type);
}

/*** dict_items ***/

static PyObject *
dictitems_iter(dictviewobject *dv)
{
    if (dv->dv_dict == NULL) {
        Py_RETURN_NONE;
    }
    return dictiter_new(dv->dv_dict, &PyOrderedDictIterItem_Type, NULL, NULL);
}

static int
dictitems_contains(dictviewobject *dv, PyObject *obj)
{
    PyObject *key, *value, *found;
    if (dv->dv_dict == NULL)
        return 0;
    if (!PyTuple_Check(obj) || PyTuple_GET_SIZE(obj) != 2)
        return 0;
    key = PyTuple_GET_ITEM(obj, 0);
    value = PyTuple_GET_ITEM(obj, 1);
    found = PyDict_GetItem((PyObject *)dv->dv_dict, key);
    if (found == NULL) {
        if (PyErr_Occurred())
            return -1;
        return 0;
    }
    return PyObject_RichCompareBool(value, found, Py_EQ);
}

static PySequenceMethods dictitems_as_sequence = {
    (lenfunc)dictview_len,              /* sq_length */
    0,                                  /* sq_concat */
    0,                                  /* sq_repeat */
    0,                                  /* sq_item */
    0,                                  /* sq_slice */
    0,                                  /* sq_ass_item */
    0,                                  /* sq_ass_slice */
    (objobjproc)dictitems_contains,     /* sq_contains */
};

static PyMethodDef dictitems_methods[] = {
    {NULL,              NULL}           /* sentinel */
};

PyTypeObject PyOrderedDictItems_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "dict_items",                               /* tp_name */
    sizeof(dictviewobject),                     /* tp_basicsize */
    0,                                          /* tp_itemsize */
    /* methods */
    (destructor)dictview_dealloc,               /* tp_dealloc */
    0,                                          /* tp_print */
    0,                                          /* tp_getattr */
    0,                                          /* tp_setattr */
    0,                                          /* tp_reserved */
    (reprfunc)dictview_repr,                    /* tp_repr */
    &dictviews_as_number,                       /* tp_as_number */
    &dictitems_as_sequence,                     /* tp_as_sequence */
    0,                                          /* tp_as_mapping */
    0,                                          /* tp_hash */
    0,                                          /* tp_call */
    0,                                          /* tp_str */
    PyObject_GenericGetAttr,                    /* tp_getattro */
    0,                                          /* tp_setattro */
    0,                                          /* tp_as_buffer */
#if PY_MAJOR_VERSION < 3
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC |
        Py_TPFLAGS_CHECKTYPES,                  /* tp_flags */
#else
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,     /* tp_flags */
#endif
    0,                                          /* tp_doc */
    (traverseproc)dictview_traverse,            /* tp_traverse */
    0,                                          /* tp_clear */
    dictview_richcompare,                       /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    (getiterfunc)dictitems_iter,                /* tp_iter */
    0,                                          /* tp_iternext */
    dictitems_methods,                          /* tp_methods */
    0,
};

static PyObject *
dictitems_new(PyObject *dict)
{
    return dictview_new(dict, &PyOrderedDictItems_Type);
}


/*** dict_values ***/

static PyObject *
dictvalues_iter(dictviewobject *dv)
{
    if (dv->dv_dict == NULL) {
        Py_RETURN_NONE;
    }
    return dictiter_new(dv->dv_dict, &PyOrderedDictIterValue_Type, NULL, NULL);
}


static PySequenceMethods dictvalues_as_sequence = {
    (lenfunc)dictview_len,              /* sq_length */
    0,                                  /* sq_concat */
    0,                                  /* sq_repeat */
    0,                                  /* sq_item */
    0,                                  /* sq_slice */
    0,                                  /* sq_ass_item */
    0,                                  /* sq_ass_slice */
    (objobjproc)0,                      /* sq_contains */
};

static PyMethodDef dictvalues_methods[] = {
    {NULL,              NULL}           /* sentinel */
};

PyTypeObject PyOrderedDictValues_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "dict_values",                              /* tp_name */
    sizeof(dictviewobject),                     /* tp_basicsize */
    0,                                          /* tp_itemsize */
    /* methods */
    (destructor)dictview_dealloc,               /* tp_dealloc */
    0,                                          /* tp_print */
    0,                                          /* tp_getattr */
    0,                                          /* tp_setattr */
    0,                                          /* tp_reserved */
    (reprfunc)dictview_repr,                    /* tp_repr */
    0,                                          /* tp_as_number */
    &dictvalues_as_sequence,                    /* tp_as_sequence */
    0,                                          /* tp_as_mapping */
    0,                                          /* tp_hash */
    0,                                          /* tp_call */
    0,                                          /* tp_str */
    PyObject_GenericGetAttr,                    /* tp_getattro */
    0,                                          /* tp_setattro */
    0,                                          /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,/* tp_flags */
    0,                                          /* tp_doc */
    (traverseproc)dictview_traverse,            /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    (getiterfunc)dictvalues_iter,               /* tp_iter */
    0,                                          /* tp_iternext */
    dictvalues_methods,                         /* tp_methods */
    0,
};

static PyObject *
dictvalues_new(PyObject *dict)
{
    return dictview_new(dict, &PyOrderedDictValues_Type);
}

#endif  /* PY_VERSION_HEX >= 0x02070000 */


/************************************************************************/


#if PY_MAJOR_VERSION >= 3
    static struct PyModuleDef moduledef = {
        PyModuleDef_HEAD_INIT,
        "_ordereddict",      /* m_name */
        ordereddict_doc,     /* m_doc */
        -1,                  /* m_size */
        ordereddict_functions,    /* m_methods */
        NULL,                /* m_reload */
        NULL,                /* m_traverse */
        NULL,                /* m_clear */
        NULL,                /* m_free */
    };
#endif


static PyObject *
ruamel_ordereddict_moduleinit(void)
{
    PyObject *m;

    /* moved here as we have two primitives and dictobject.c had
       no initialisation function */
    if (dummy == NULL) { /* Auto-initialize dummy */
        dummy = PyUNISTR_FromString("<dummy key>");
        if (dummy == NULL)
            return NULL;
#ifdef SHOW_CONVERSION_COUNTS
        Py_AtExit(show_counts);
#endif
    }

    /* Fill in deferred data addresses.  This must be done before
       PyType_Ready() is called.  Note that PyType_Ready() automatically
       initializes the ob.ob_type field to &PyType_Type if it's NULL,
       so it's not necessary to fill in ob_type first. */
    PyOrderedDict_Type.tp_base = &PyDict_Type;
    PySortedDict_Type.tp_base = &PyOrderedDict_Type;

    if (PyType_Ready(&PyOrderedDict_Type) < 0)
        return NULL;
    if (PyType_Ready(&PySortedDict_Type) < 0)
        return NULL;

    /* AvdN: TODO understand why it is necessary or not (as it seems)
    to PyTypeReady the iterator types
    */

#if PY_MAJOR_VERSION >= 3
    m = PyModule_Create(&moduledef);
#else
    m = Py_InitModule3("_ordereddict",
                       ordereddict_functions,
                       ordereddict_doc
                       // , NULL, PYTHON_API_VERSION
                      );
#endif
    if (m == NULL)
        return NULL;

    /* this allows PyVarObject_HEAD_INIT to take NULL as first
    parameter: https://docs.python.org/3.1/extending/windows.html
    */
    if (PyType_Ready(&PyOrderedDict_Type) < 0)
        return NULL;

    Py_INCREF(&PyOrderedDict_Type);
    if (PyModule_AddObject(m, "ordereddict",
                           (PyObject *) &PyOrderedDict_Type) < 0)
        Py_INCREF(&PySortedDict_Type);
    if (PyModule_AddObject(m, "sorteddict",
                           (PyObject *) &PySortedDict_Type) < 0)
        return NULL;
    return m;
}

#if PY_MAJOR_VERSION < 3
    PyMODINIT_FUNC init_ordereddict(void)
    {
        ruamel_ordereddict_moduleinit();
    }
#else
    PyMODINIT_FUNC PyInit__ordereddict(void)
    {
        return ruamel_ordereddict_moduleinit();
    }
#endif
