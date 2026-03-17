/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT file for more information. */
#ifndef NCLIST_H
#define NCLIST_H 1

#include "ncexternl.h"
#include <stddef.h>

/* Define the type of the elements in the list*/

#if defined(_CPLUSPLUS_) || defined(__CPLUSPLUS__)
extern "C" {
#endif

EXTERNL int nclistisnull(void*);

typedef struct NClist {
  void** content;
  size_t alloc;
  size_t length;
  int extendible;
} NClist;

EXTERNL NClist* nclistnew(void);
EXTERNL int nclistfree(NClist*);
EXTERNL int nclistfreeall(NClist*);
EXTERNL int nclistclearall(NClist*);
EXTERNL int nclistsetalloc(NClist*,size_t);
EXTERNL int nclistsetlength(NClist*,size_t);

/* Set the ith element; will overwrite previous contents; expand if needed */
EXTERNL int nclistset(NClist*,size_t,void*);
/* Get value at position i */
EXTERNL void* nclistget(const NClist*,size_t);/* Return the ith element of l */
/* Insert at position i; will push up elements i..|seq|. */
EXTERNL int nclistinsert(NClist*,size_t,void*);
/* Remove element at position i; will move higher elements down */
EXTERNL void* nclistremove(NClist* l, size_t i);

/* Tail operations */
EXTERNL int nclistpush(NClist*,const void*); /* Add at Tail */
EXTERNL void* nclistpop(NClist*);
EXTERNL void* nclisttop(NClist*);

/* Look for pointer match */
EXTERNL int nclistcontains(NClist*, void*);

/* Look for value match as string */
EXTERNL int nclistmatch(NClist*, const char*, int casesensistive);

/* Remove element by value; only removes first encountered */
EXTERNL int nclistelemremove(NClist* l, void* elem);

/* remove duplicates */
EXTERNL int nclistunique(NClist*);

/* Create a clone of a list; if deep, then assume it is a list of strings */
EXTERNL NClist* nclistclone(const NClist*, int deep);

/* Extract the contents of a list, leaving list empty */
EXTERNL void* nclistextract(NClist*);

/* Set the contents of the list; mark extendible as false  */
EXTERNL int nclistsetcontents(NClist*, void**, size_t, size_t length);

/* Append an uncounted NULL to the end of the list */
EXTERNL int nclistnull(NClist*);

/* Following are always "in-lined"*/
#define nclistclear(l) nclistsetlength((l),0)
#define nclistextend(l,len) nclistsetalloc((l),(len)+(l->alloc))
#define nclistcontents(l)  ((l)==NULL?NULL:(l)->content)
#define nclistlength(l)  ((l)==NULL?0:(l)->length)

#if defined(_CPLUSPLUS_) || defined(__CPLUSPLUS__)
}
#endif

#endif /*NCLIST_H*/
