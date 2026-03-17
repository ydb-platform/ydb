// This file is part of Invenio.
// Copyright (C) 2007, 2008, 2009, 2010, 2011, 2014, 2015, 2016 CERN.
//
// SPDX-License-Identifier: LGPL-3.0-or-later
//
// Invenio is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License as
// published by the Free Software Foundation; either version 3 of the
// License, or (at your option) any later version.
//
// Invenio is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Invenio; if not, write to the Free Software Foundation, Inc.,
// 59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.

#ifndef INTBITSET_H
#define INTBITSET_H

#include <Python.h>

#if PY_VERSION_HEX < 0x02050000
#ifndef PY_SSIZE_T_CLEAN
typedef int Py_ssize_t;
#endif
#endif

// Fake declarations: do not call them!
#if PY_VERSION_HEX >= 0x03000000
PyObject* PyString_FromStringAndSize(const char *v, Py_ssize_t len);
#endif

typedef unsigned long long int word_t;
typedef unsigned char bool_t;

extern const int wordbytesize;
extern const int wordbitsize;
extern const int maxelem;

typedef struct {
    int size;
    int allocated;
    word_t trailing_bits;
    int tot;
    word_t *bitset;
} IntBitSet;

IntBitSet *intBitSetCreate(register const int size, const bool_t trailing_bits);
IntBitSet *intBitSetCreateFromBuffer(const void * const buf, const Py_ssize_t bufsize);
IntBitSet *intBitSetResetFromBuffer(IntBitSet *const bitset, const void *const buf, const Py_ssize_t bufsize);
IntBitSet *intBitSetReset(IntBitSet *const bitset);
void intBitSetDestroy(IntBitSet *const bitset);
IntBitSet *intBitSetClone(const IntBitSet * const bitset);
int intBitSetGetSize(IntBitSet * const bitset);
int intBitSetGetAllocated(const IntBitSet * const bitset);
int intBitSetGetTot(IntBitSet * const bitset);
void intBitSetResize(IntBitSet *const bitset, register const unsigned int allocated);
bool_t intBitSetIsInElem(const IntBitSet * const bitset, register const unsigned int elem);
void intBitSetAddElem(IntBitSet *const bitset, register const unsigned int elem);
void intBitSetDelElem(IntBitSet *const bitset, register const unsigned int elem);
bool_t intBitSetEmpty(const IntBitSet * const bitset);
IntBitSet *intBitSetUnion(IntBitSet *const x, IntBitSet *const y);
IntBitSet *intBitSetXor(IntBitSet *const x, IntBitSet *const y);
IntBitSet *intBitSetIntersection(IntBitSet *const x, IntBitSet *const y);
IntBitSet *intBitSetSub(IntBitSet *const x, IntBitSet *const y);
IntBitSet *intBitSetIUnion(IntBitSet *const dst, IntBitSet *const src);
IntBitSet *intBitSetIXor(IntBitSet *const dst, IntBitSet *const src);
IntBitSet *intBitSetIIntersection(IntBitSet *const dst, IntBitSet *const src);
IntBitSet *intBitSetISub(IntBitSet *const x, IntBitSet *const y);
bool_t intBitSetIsDisjoint(const IntBitSet * const x, const IntBitSet * const y);
int intBitSetAdaptMax(IntBitSet *const x, IntBitSet *const y);
int intBitSetAdaptMin(IntBitSet *const x, IntBitSet *const y);
int intBitSetGetNext(const IntBitSet *const x, register int last);
int intBitSetGetLast(const IntBitSet *const x);
/** Compare.
 * Compare two intbitset.
 * Returns 0 if the two bitset are equals.
 * Returns 1 if x is proper subset of y
 * Returns 2 if y is proper subset of x
 * Returns 3 if x != y
 */
unsigned char intBitSetCmp(IntBitSet *const x, IntBitSet *const y);

#endif
