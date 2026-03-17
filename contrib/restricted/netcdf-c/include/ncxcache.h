/*
Copyright (c) 1998-2018 University Corporation for Atmospheric Research/Unidata
See COPYRIGHT for license information.
*/

#ifndef NCXCACHE_H
#define NCXCACHE_H

#include "nclist.h"
#include "ncexhash.h" /* Also includes name map and id map */

/* Define the implementation.
   if defined, then the user's object
   is assumed to hold the double linked list node,
   otherwise, it is created here.
*/
#define NCXUSER

/*
This cache data structure is an ordered list of objects. It is
used to create an LRU cache of arbitrary objects.
*/

/* Doubly linked list element */
typedef struct NCxnode {
    struct NCxnode* next;
    struct NCxnode* prev;
    void* content; /* associated data of some kind may be unused*/
} NCxnode;

typedef struct NCxcache {
   NCxnode lru;
   NCexhashmap* map;
} NCxcache;

/* Locate object by hashkey */
EXTERNL int ncxcachelookup(NCxcache* cache, ncexhashkey_t hkey, void** objp);

/* Insert object into the cache >*/
EXTERNL int ncxcacheinsert(NCxcache* cache, ncexhashkey_t hkey, void* obj);

/* Bring to front of the LRU queue */
EXTERNL int ncxcachetouch(NCxcache* cache, ncexhashkey_t hkey);

/* "Remove" object from the cache; return object */
EXTERNL int ncxcacheremove(NCxcache* cache, ncexhashkey_t hkey, void** obj);

/* Free cache. */
EXTERNL void ncxcachefree(NCxcache* cache);

/* Create a cache: size == 0 => use defaults */
EXTERNL int ncxcachenew(size_t initsize, NCxcache**) ;

/* Macro function */

/* Get the number of entries in an NCxcache */
#define ncxcachecount(cache) (cache == NULL ? 0 : ncexhashcount((cache)->map))

EXTERNL void* ncxcachefirst(NCxcache* cache);
EXTERNL void* ncxcachelast(NCxcache* cache);

/* Return the hash key for specified key; takes key+size; an alias for the one in ncexhash */
EXTERNL ncexhashkey_t ncxcachekey(const void* key, size_t size);

/* Debugging */
EXTERNL void ncxcacheprint(NCxcache* cache);

#endif /*NCXCACHE_H*/
