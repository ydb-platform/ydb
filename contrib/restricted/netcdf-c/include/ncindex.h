/*
Copyright (c) 1998-2018 University Corporation for Atmospheric Research/Unidata
See COPYRIGHT for license information.
*/

#ifndef NCINDEX_H
#define NCINDEX_H

/* If defined, then the hashmap is used.
   This for performance experimentation
*/
#undef NCNOHASH

#undef NCINDEXDEBUG

#include "nclist.h"
#include "nchashmap.h" /* Also includes name map and id map */

/* Forward (see nc4internal.h)*/
struct NC_OBJ;

/*
This index data structure is an ordered list of objects. It is
used pervasively in libsrc4 to store metadata relationships.
The goal is to provide by-name and i'th indexed access (via
NClist) to the objects in the index.  Using NCindex might be
overkill for some relationships, but we can sort that out later.
As a rule, we use this to store definitional relationships such
as (in groups) dimension definitions, variable definitions, type
defs and subgroup defs. We do not, as a rule, use this to store
reference relationships such as the list of dimensions for a
variable.

See docs/indexind.dox for more detailed documentation

*/

/* Generic list + matching hashtable */
typedef struct NCindex {
   NClist* list;
#ifndef NCNOHASH
   NC_hashmap* map;
#endif
} NCindex;

/* Locate object by name in an NCindex */
extern struct NC_OBJ* ncindexlookup(NCindex* index, const char* name);

/* Get ith object in the index vector */
extern struct NC_OBJ* ncindexith(NCindex* index, size_t i);

/* See if x is contained in the index and return its vector permission*/
extern int ncindexfind(NCindex* index, struct NC_OBJ* o);

/* Add object to the end of the vector, also insert into the hashmaps; */
/* Return 1 if ok, 0 otherwise.*/
extern int ncindexadd(NCindex* index, struct NC_OBJ* obj);

/* Insert object at ith position of the vector, also insert into the hashmaps; */
/* Return 1 if ok, 0 otherwise.*/
extern int ncindexset(NCindex* index, size_t i, struct NC_OBJ* obj);

/* Get a copy of the vector contents */
extern struct NC_OBJ** ncindexdup(NCindex* index);

/* Count the non-null entries in an NCindex */
extern int ncindexcount(NCindex* index);

/* Rebuild index using all objects in the vector */
/* Return 1 if ok, 0 otherwise.*/
extern int ncindexrebuild(NCindex* index);

/* "Remove" ith object from the index;
    WARNING: Replaces it with NULL in the list.
*/
/* Return 1 if ok, 0 otherwise.*/
extern int ncindexidel(NCindex* index,size_t i);

/* Free an index. */
/* Return 1 if ok; 0 otherwise */
extern int ncindexfree(NCindex* index);

/* Create an index: size == 0 => use defaults */
/* Return index if ok; NULL otherwise */
extern NCindex* ncindexnew(size_t initsize);

extern int ncindexverify(NCindex* lm, int dump);

/* Lookup object in index; return NULL if not found */
extern struct NC_OBJ* ncindexlookup(NCindex*, const char* name);

/* Inline functions */

/* Test if index has been initialized */
#define ncindexinitialized(index) ((index) != NULL && (index)->list != NULL)

/* Get number of entries in an index */
#ifdef NCINDEXDEBUG
static int ncindexsize(NCindex* index)
{
   int i;
   if(index == NULL) return 0;
   i = nclistlength(index->list);
   return i;
}
#else
#define ncindexsize(index) ((index)==NULL?0:(nclistlength((index)->list)))
#endif

#endif /*ncindexH*/
