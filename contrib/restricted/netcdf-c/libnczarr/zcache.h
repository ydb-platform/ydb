/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#ifndef ZCACHE_H
#define ZCACHE_H

/* This holds all the fields
   to support either impl of cache
*/

struct NCxcache;

/* Note in the following: the term "real"
   refers to the unfiltered/uncompressed data
   The term filtered refers to the result of running
   the real data through the filter chain. Note that the
   sizeof the filtered data might be larger than the size of
   the real data.
   The term "raw" is used to refer to the data on disk and it may either
   be real or filtered.
*/

typedef struct NCZCacheEntry {
    struct List {void* next; void* prev; void* unused;} list;
    int modified;
    size64_t indices[NC_MAX_VAR_DIMS];
    struct ChunkKey {
	char* varkey; /* key to the containing variable */
        char* chunkkey; /* name of the chunk */
    } key;
    size64_t hashkey;
    int isfiltered; /* 1=>data contains filtered data else real data */
    int isfixedstring; /* 1 => data contains the fixed strings, 0 => data contains pointers to strings */
    size64_t size; /* |data| */
    void* data; /* contains either filtered or real data */
} NCZCacheEntry;

typedef struct NCZChunkCache {
    int valid; /* 0 => following fields need to be re-calculated */
    NC_VAR_INFO_T* var; /* backlink */
    size64_t ndims; /* true ndims == var->ndims + scalar */
    size64_t chunksize; /* for real data */
    size64_t chunkcount; /* cross product of chunksizes */
    void* fillchunk; /* enough fillvalues to fill a real chunk */
    struct ChunkCache params;
    size_t used; /* How much total space is being used */
    NClist* mru; /* NClist<NCZCacheEntry> all cache entries in mru order */
    struct NCxcache* xcache;
    char dimension_separator;
} NCZChunkCache;

/**************************************************/

#define FILTERED(cache) (nclistlength((NClist*)(cache)->var->filters))

extern int NCZ_set_var_chunk_cache(int ncid, int varid, size_t size, size_t nelems, float preemption);
extern int NCZ_adjust_var_cache(NC_VAR_INFO_T *var);
extern int NCZ_create_chunk_cache(NC_VAR_INFO_T* var, size64_t, char dimsep, NCZChunkCache** cachep);
extern void NCZ_free_chunk_cache(NCZChunkCache* cache);
extern int NCZ_read_cache_chunk(NCZChunkCache* cache, const size64_t* indices, void** datap);
extern int NCZ_flush_chunk_cache(NCZChunkCache* cache);
extern size64_t NCZ_cache_entrysize(NCZChunkCache* cache);
extern NCZCacheEntry* NCZ_cache_entry(NCZChunkCache* cache, const size64_t* indices);
extern size64_t NCZ_cache_size(NCZChunkCache* cache);
extern int NCZ_buildchunkpath(NCZChunkCache* cache, const size64_t* chunkindices, struct ChunkKey* key);
extern int NCZ_ensure_fill_chunk(NCZChunkCache* cache);
extern int NCZ_reclaim_fill_chunk(NCZChunkCache* cache);
extern int NCZ_chunk_cache_modify(NCZChunkCache* cache, const size64_t* indices);

#endif /*ZCACHE_H*/
