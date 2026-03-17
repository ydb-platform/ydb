/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *   $Header$
 *********************************************************************/
#ifndef NCHASHMAP_H
#define NCHASHMAP_H

#if HAVE_CONFIG_H
#include "config.h"
#endif
#include <stdint.h>

/*
This hashmap is optimized to assume null-terminated strings as the
key.

Data is presumed to be an index into some other table Assume it
can be compared using simple == The key is some hash of some
null terminated string.

One problem here is that we need to do a final equality check on
the name string to avoid an accidental hash collision. It would
be nice if we had a large enough hashkey that was known to have
an extremely low probability of collisions so we could compare
the hashkeys to determine exact match. A quick internet search
indicates that this is rather more tricky than just using
e.g. crc64 or such.  Needs some thought.
*/

/*! Hashmap-related structs.
  NOTES:
  1. 'data' is the an arbitrary uintptr_t integer or void* pointer.
  2. hashkey is a crc32 hash of key
    
  WARNINGS:
  1. It is critical that |uintptr_t| == |void*|
*/

/** The type and # bits in a hashkey */
#ifndef nchashkey_t
#define nchashkey_t unsigned
#define NCHASHKEYBITS (sizeof(nchashkey_t)*8)
#endif

typedef struct NC_hentry {
    int flags;
    uintptr_t data;
    nchashkey_t hashkey; /* Hash id (crc)*/
    size_t keysize;
    char* key; /* copy of the key string; kept as unsigned char */
} NC_hentry;

/*
The hashmap object must give us the hash table (table),
the |table| size, and the # of defined entries in the table
*/
typedef struct NC_hashmap {
  size_t alloc; /* allocated # of entries */
  size_t active; /* # of active entries */
  NC_hentry* table;
} NC_hashmap;

/* defined in nchashmap.c */

/*
There are two "kinds" of functions:
1. those that take the key+size -- they compute the hashkey internally.
2. those that take the hashkey directly
*/

/** Creates a new hashmap near the given size. */
extern NC_hashmap* NC_hashmapnew(size_t startsize);

/** Inserts a new element into the hashmap; takes key+size */
/* key points to size bytes to convert to hash key */
extern int NC_hashmapadd(NC_hashmap*, uintptr_t data, const char* key, size_t keysize);

/** Removes the storage for the element of the key; takes key+size.
    Return 1 if found, 0 otherwise; returns the data in datap if !null
*/
extern int NC_hashmapremove(NC_hashmap*, const char* key, size_t keysize, uintptr_t* datap);

/** Returns the data for the key; takes key+size.
    Return 1 if found, 0 otherwise; returns the data in datap if !null
*/
extern int NC_hashmapget(NC_hashmap*, const char* key, size_t keysize, uintptr_t* datap);

/** Change the data for the specified key; takes hashkey.
    Return 1 if found, 0 otherwise
*/
extern int NC_hashmapsetdata(NC_hashmap*, const char* key, size_t keylen, uintptr_t newdata);

/** Returns the number of active elements. */
extern size_t NC_hashmapcount(NC_hashmap*);

/** Reclaims the hashmap structure. */
extern int NC_hashmapfree(NC_hashmap*);

/* Return the hash key for specified key; takes key+size*/
extern nchashkey_t NC_hashmapkey(const char* key, size_t size);

/* Return the ith entry info:
@param map
@param i
@param entryp contains 0 if not active, otherwise the data
@param keyp contains null if not active, otherwise the key
@return NC_EINVAL if no more entries
*/
extern int NC_hashmapith(NC_hashmap* map, size_t i, uintptr_t* datap, const char** keyp);

#endif /*NCHASHMAP_H*/

