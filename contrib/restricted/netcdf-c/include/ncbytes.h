/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT file for more information. */

#ifndef NCBYTES_H
#define NCBYTES_H 1

typedef struct NCbytes {
  char* content;
  unsigned long alloc;
  unsigned long length;
  int extendible; /* 0 => fail if an attempt is made to extend this buffer*/
} NCbytes;

#include "ncexternl.h"

#if defined(_CPLUSPLUS_) || defined(__CPLUSPLUS__) || defined(__CPLUSPLUS)
extern "C" {
#endif

EXTERNL NCbytes* ncbytesnew(void);
EXTERNL void ncbytesfree(NCbytes*);
EXTERNL int ncbytessetalloc(NCbytes*,unsigned long);
EXTERNL int ncbytessetlength(NCbytes*,unsigned long);
EXTERNL int ncbytesfill(NCbytes*, char fill);

/* Produce a duplicate of the contents*/
EXTERNL char* ncbytesdup(NCbytes*);
/* Extract the contents and leave buffer empty */
EXTERNL char* ncbytesextract(NCbytes*);

/* Return the ith byte; -1 if no such index */
EXTERNL int ncbytesget(NCbytes*,unsigned long);
/* Set the ith byte */
EXTERNL int ncbytesset(NCbytes*,unsigned long,char);

/* Append one byte */
EXTERNL int ncbytesappend(NCbytes*,char); /* Add at Tail */
/* Append n bytes */
EXTERNL int ncbytesappendn(NCbytes*,const void*,unsigned long); /* Add at Tail */

/* Null terminate the byte string without extending its length (for debugging) */
EXTERNL int ncbytesnull(NCbytes*);

/* Remove char at position i */
EXTERNL int ncbytesremove(NCbytes*,unsigned long);

/* Concatenate a null-terminated string to the end of the buffer */
EXTERNL int ncbytescat(NCbytes*,const char*);

/* Set the contents of the buffer; mark the buffer extendible = false */
EXTERNL int ncbytessetcontents(NCbytes*, void*, unsigned long, unsigned long length);

/* Following are always "in-lined"*/
#define ncbyteslength(bb) ((bb)!=NULL?(bb)->length:0)
#define ncbytesalloc(bb) ((bb)!=NULL?(bb)->alloc:0)
#define ncbytescontents(bb) (((bb)!=NULL && (bb)->content!=NULL)?(bb)->content:(char*)"")
#define ncbytesextend(bb,len) ncbytessetalloc((bb),(len)+(bb->alloc))
#define ncbytesclear(bb) ((bb)!=NULL?(bb)->length=0:0)
#define ncbytesavail(bb,n) ((bb)!=NULL?((bb)->alloc - (bb)->length) >= (n):0)
#define ncbytesextendible(bb) ((bb)!=NULL?((bb)->nonextendible?0:1):0)

#if defined(_CPLUSPLUS_) || defined(__CPLUSPLUS__) || defined(__CPLUSPLUS)
}
#endif

#endif /*NCBYTES_H*/
