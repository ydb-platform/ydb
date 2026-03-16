/*
Copyright (c) 1998-2018 University Corporation for Atmospheric Research/Unidata
See LICENSE.txt for license information.
*/

#ifndef NCEXHASH_H
#define NCEXHASH_H

#if HAVE_CONFIG_H
#include "config.h"
#endif
#include <stdint.h>
#include "netcdf.h"

/*
Implement extendible hashing as defined in:
````
R. Fagin, J. Nievergelt, N. Pippenger, and H. Strong, "Extendible Hashing — a fast access method for dynamic files", ACM Transactions on Database Systems, vol. 4, No. 3, pp. 315-344, 1979.
````
*/

/*! Hashmap-related structs.
  NOTES:
  1. 'data' is the an arbitrary uintptr_t integer or void* pointer.
  2. hashkey is a crc64 hash of key -- it is assumed to be unique for keys.
    
  WARNINGS:
  1. It is critical that |uintptr_t| == |void*|
*/

#define ncexhashkey_t unsigned long long
#define NCEXHASHKEYBITS 64

typedef struct NCexentry {
    ncexhashkey_t hashkey; /* Hash id */
    uintptr_t data;
} NCexentry;

typedef struct NCexleaf {
    int uid; /* primarily for debug */
    struct NCexleaf* next; /* linked list of all leaves for cleanup */
    int depth; /* local depth */
    int active; /* index of the first empty slot */
    NCexentry* entries; /* |entries| == leaflen*/
} NCexleaf;

/* Top Level Vector */
typedef struct NCexhashmap {
    int leaflen; /* # entries a leaf can store */
    int depth; /* Global depth */
    NCexleaf* leaves; /* head of the linked list of leaves */
    int nactive; /* # of active entries in whole table */
    NCexleaf** directory; /* |directory| == 2^depth */
    int uid; /* unique id counter */
    /* Allow a single iterator over the entries */
    struct {
	int walking; /* 0=>not in use */
	int index; /* index of current entry in leaf */
	NCexleaf* leaf; /* leaf we are walking */
    } iterator;
} NCexhashmap;

/** Creates a new exhash using LSB */
EXTERNL NCexhashmap* ncexhashnew(int leaflen);

/** Reclaims the exhash structure. */
EXTERNL void ncexhashmapfree(NCexhashmap*);

/** Returns the number of active elements. */
EXTERNL int ncexhashcount(NCexhashmap*);

/* Hash key based API */

/* Lookup by Hash Key */
EXTERNL int ncexhashget(NCexhashmap*, ncexhashkey_t hkey, uintptr_t*);

/* Insert by Hash Key */
EXTERNL int ncexhashput(NCexhashmap*, ncexhashkey_t hkey, uintptr_t data);

/* Remove by Hash Key */
EXTERNL int ncexhashremove(NCexhashmap*, ncexhashkey_t hkey, uintptr_t* datap);

/** Change the data for the specified key; takes hashkey. */
EXTERNL int ncexhashsetdata(NCexhashmap*, ncexhashkey_t hkey, uintptr_t newdata, uintptr_t* olddatap);

/** Get map parameters */
EXTERNL int ncexhashinqmap(NCexhashmap* map, int* leaflenp, int* depthp, int* nactivep, int* uidp, int* walkingp);

/* Return the hash key for specified key; takes key+size*/
EXTERNL ncexhashkey_t ncexhashkey(const unsigned char* key, size_t size);

/* Walk the entries in some order */
/*
@return NC_NOERR if keyp and datap are valid
@return NC_ERANGE if iteration is finished
@return NC_EINVAL for all other errors
*/
EXTERNL int ncexhashiterate(NCexhashmap* map, ncexhashkey_t* keyp, uintptr_t* datap);

/* Debugging */
EXTERNL void ncexhashprint(NCexhashmap*);
EXTERNL void ncexhashprintstats(NCexhashmap*);
EXTERNL void ncexhashprintdir(NCexhashmap*, NCexleaf** dir);
EXTERNL void ncexhashprintleaf(NCexhashmap*, NCexleaf* leaf);
EXTERNL void ncexhashprintentry(NCexhashmap* map, NCexentry* entry);
EXTERNL char* ncexbinstr(ncexhashkey_t hkey, int depth);

/* Macro defined functions */

/** Get map parameters */
#define ncexhashmaplength(map) ((map)==NULL?0:(map)->nactive)


#endif /*NCEXHASH_H*/

