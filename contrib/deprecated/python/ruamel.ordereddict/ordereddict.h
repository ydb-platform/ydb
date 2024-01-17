#ifndef Py_ORDEREDDICTOBJECT_H
#define Py_ORDEREDDICTOBJECT_H
#ifdef __cplusplus
extern "C" {
#endif

#if defined _MSC_VER || defined __CYGWIN__
#undef PyAPI_FUNC
#undef PyAPI_DATA
#define PyAPI_FUNC(RTYPE) __declspec(dllexport) RTYPE
#define PyAPI_DATA(RTYPE) __declspec(dllexport) RTYPE
#endif

/* Ordered Dictionary object implementation using a hash table and a vector of
   pointers to the items.
*/
/*

  This file has been directly derived from dictobject.h in the Python 2.5.1
  source distribution. Its licensing therefore   is governed by the license
  as distributed with Python 2.5.1 available in the
  file LICNESE in the source distribution of ordereddict

  Copyright (c) 2001, 2002, 2003, 2004, 2005, 2006, 2007  Python Software
  Foundation; All Rights Reserved"

  2007-10-13: Anthon van der Neut
*/

/* Dictionary object type -- mapping from hashable object to object */

/* The distribution includes a separate file, Objects/dictnotes.txt,
   describing explorations into dictionary design and optimization.
   It covers typical dictionary use patterns, the parameters for
   tuning dictionaries, and several ideas for possible optimizations.
*/

/*
There are three kinds of slots in the table:


1. Unused.  me_key == me_value == NULL
   Does not hold an active (key, value) pair now and never did.  Unused can
   transition to Active upon key insertion.  This is the only case in which
   me_key is NULL, and is each slot's initial state.

2. Active.  me_key != NULL and me_key != dummy and me_value != NULL
   Holds an active (key, value) pair.  Active can transition to Dummy upon
   key deletion.  This is the only case in which me_value != NULL.

3. Dummy.  me_key == dummy and me_value == NULL
   Previously held an active (key, value) pair, but that was deleted and an
   active pair has not yet overwritten the slot.  Dummy can transition to
   Active upon key insertion.  Dummy slots cannot be made Unused again
   (cannot have me_key set to NULL), else the probe sequence in case of
   collision would have no way to know they were once active.

Note: .popitem() abuses the me_hash field of an Unused or Dummy slot to
hold a search finger.  The me_hash field of Unused or Dummy slots has no
meaning otherwise.
*/

#if PY_VERSION_HEX < 0x02050000
#ifdef _MSC_VER
	typedef int Py_ssize_t;
#else
typedef ssize_t			Py_ssize_t;
#endif
typedef Py_ssize_t		(*lenfunc)(PyObject *);
typedef intintargfunc	ssizessizeargfunc;
typedef intintobjargproc	ssizessizeobjargproc;

#define PyInt_FromSize_t(A)	  PyInt_FromLong((long) A)
#endif

/* PyOrderedDict_MINSIZE is the minimum size of a dictionary.  This many slots are
 * allocated directly in the dict object (in the ma_smalltable member).
 * It must be a power of 2, and at least 4.  8 allows dicts with no more
 * than 5 active entries to live in ma_smalltable (and so avoid an
 * additional malloc); instrumentation suggested this suffices for the
 * majority of dicts (consisting mostly of usually-small instance dicts and
 * usually-small dicts created to pass keyword arguments).
 */
#define PyOrderedDict_MINSIZE 8

typedef struct {
	/* Cached hash code of me_key.  Note that hash codes are C longs.
	 * We have to use Py_ssize_t instead because dict_popitem() abuses
	 * me_hash to hold a search finger.
	 */
	Py_ssize_t me_hash;
	PyObject *me_key;
	PyObject *me_value;
} PyOrderedDictEntry;

/*
To ensure the lookup algorithm terminates, there must be at least one Unused
slot (NULL key) in the table.
The value od_fill is the number of non-NULL keys (sum of Active and Dummy);
ma_used is the number of non-NULL, non-dummy keys (== the number of non-NULL
values == the number of Active items).
To avoid slowing down lookups on a near-full table, we resize the table when
it's two-thirds full.
*/
typedef struct _ordereddictobject PyOrderedDictObject;
struct _ordereddictobject {
#if PY_MAJOR_VERSION < 3
	PyObject_HEAD
#else
	PyObject_VAR_HEAD
#endif
	Py_ssize_t od_fill;  /* # Active + # Dummy */
	Py_ssize_t ma_used;  /* # Active */

	/* The table contains ma_mask + 1 slots, and that's a power of 2.
	 * We store the mask instead of the size because the mask is more
	 * frequently needed.
	 */
	Py_ssize_t ma_mask;

	/* ma_table points to ma_smalltable for small tables, else to
	 * additional malloc'ed memory.  ma_table is never NULL!  This rule
	 * saves repeated runtime null-tests in the workhorse getitem and
	 * setitem calls.
	 */
	PyOrderedDictEntry *ma_table;
	PyOrderedDictEntry *(*ma_lookup)(PyOrderedDictObject *mp, PyObject *key, long hash);
	PyOrderedDictEntry ma_smalltable[PyOrderedDict_MINSIZE];
	/* for small arrays, ordered table pointer points to small array of tables */
	PyOrderedDictEntry **od_otablep;
	PyOrderedDictEntry *ma_smallotablep[PyOrderedDict_MINSIZE];
	/* for storing kvio, relaxed bits */
    long od_state;
};

typedef struct _sorteddictobject PySortedDictObject;
struct _sorteddictobject {
    struct _ordereddictobject od;
	PyObject *sd_cmp;
	PyObject *sd_key;
	PyObject *sd_value;
};


PyAPI_DATA(PyTypeObject) PyOrderedDict_Type;
PyAPI_DATA(PyTypeObject) PySortedDict_Type;
#if PY_VERSION_HEX >= 0x02070000
PyAPI_DATA(PyTypeObject) PyOrderedDictIterKey_Type;
PyAPI_DATA(PyTypeObject) PyOrderedDictIterValue_Type;
PyAPI_DATA(PyTypeObject) PyOrderedDictIterItem_Type;
#endif
PyAPI_DATA(PyTypeObject) PyOrderedDictKeys_Type;
PyAPI_DATA(PyTypeObject) PyOrderedDictItems_Type;
PyAPI_DATA(PyTypeObject) PyOrderedDictValues_Type;

#if PY_VERSION_HEX >= 0x02080000
  /* AvdN: this might need reviewing for > 2.7 */
  #define PyOrderedDict_Check(op) \
                 PyType_FastSubclass(Py_TYPE(op), Py_TPFLAGS_DICT_SUBCLASS)
  #define PySortedDict_Check(op) \
                 PyType_FastSubclass(Py_TYPE(op), Py_TPFLAGS_DICT_SUBCLASS)
  #define PyOrderedDict_CheckExact(op) (Py_TYPE(op) == &PyOrderedDict_Type)
  #define PySortedDict_CheckExact(op) (Py_TYPE(op) == &PySortedDict_Type)
#else
  #define PyOrderedDict_Check(op) PyObject_TypeCheck(op, &PyOrderedDict_Type)
  #define PySortedDict_Check(op) PyObject_TypeCheck(op, &PySortedDict_Type)
  #define PyOrderedDict_CheckExact(op) ((op)->ob_type == &PyOrderedDict_Type)
  #define PySortedDict_CheckExact(op) ((op)->ob_type == &PySortedDict_Type)
#endif

PyAPI_FUNC(PyObject *) PyOrderedDict_New(void);
PyAPI_FUNC(PyObject *) PyOrderedDict_GetItem(PyObject *mp, PyObject *key);
PyAPI_FUNC(int) PyOrderedDict_SetItem(PyObject *mp, PyObject *key, PyObject *item);
PyAPI_FUNC(int) PyOrderedDict_DelItem(PyObject *mp, PyObject *key);
PyAPI_FUNC(void) PyOrderedDict_Clear(PyObject *mp);
PyAPI_FUNC(int) PyOrderedDict_Next(
	PyObject *mp, Py_ssize_t *pos, PyObject **key, PyObject **value);
PyAPI_FUNC(int) _PyOrderedDict_Next(
	PyObject *mp, Py_ssize_t *pos, PyObject **key, PyObject **value, long *hash);
PyAPI_FUNC(PyObject *) PyOrderedDict_Keys(PyObject *mp);
PyAPI_FUNC(PyObject *) PyOrderedDict_Values(PyObject *mp);
PyAPI_FUNC(PyObject *) PyOrderedDict_Items(PyObject *mp);
PyAPI_FUNC(Py_ssize_t) PyOrderedDict_Size(PyObject *mp);
PyAPI_FUNC(PyObject *) PyOrderedDict_Copy(PyObject *mp);
PyAPI_FUNC(int) PyOrderedDict_Contains(PyObject *mp, PyObject *key);
PyAPI_FUNC(int) _PyOrderedDict_Contains(PyObject *mp, PyObject *key, long hash);
PyAPI_FUNC(PyObject *) _PyOrderedDict_NewPresized(Py_ssize_t minused);
PyAPI_FUNC(void) _PyOrderedDict_MaybeUntrack(PyObject *mp);

/* PyOrderedDict_Update(mp, other) is equivalent to PyOrderedDict_Merge(mp, other, 1). */
PyAPI_FUNC(int) PyOrderedDict_Update(PyObject *mp, PyObject *other);

/* PyOrderedDict_Merge updates/merges from a mapping object (an object that
   supports PyMapping_Keys() and PyObject_GetItem()).  If override is true,
   the last occurrence of a key wins, else the first.  The Python
   dict.update(other) is equivalent to PyOrderedDict_Merge(dict, other, 1).
*/
PyAPI_FUNC(int) PyOrderedDict_Merge(PyObject *mp,
				   PyObject *other,
				   int override, int relaxed);

/* PyOrderedDict_MergeFromSeq2 updates/merges from an iterable object producing
   iterable objects of length 2.  If override is true, the last occurrence
   of a key wins, else the first.  The Python dict constructor dict(seq2)
   is equivalent to dict={}; PyOrderedDict_MergeFromSeq(dict, seq2, 1).
*/
PyAPI_FUNC(int) PyOrderedDict_MergeFromSeq2(PyObject *d,
					   PyObject *seq2,
					   int override);

#ifdef __cplusplus
}
#endif
#endif /* !Py_ORDEREDDICTOBJECT_H */
