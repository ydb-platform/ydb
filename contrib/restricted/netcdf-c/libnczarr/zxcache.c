/* Copyright 2018, University Corporation for Atmospheric
 * Research. See COPYRIGHT file for copying and redistribution
 * conditions. */

/**
 * @file @internal The functions which control NCZ
 * caching. These caching controls allow the user to change the cache
 * sizes of ZARR before opening files.
 *
 * @author Dennis Heimbigner, Ed Hartnett
 */

#include "zincludes.h"
#include "zcache.h"
#include "ncxcache.h"
#include "zfilter.h"
#include <stddef.h>

#undef DEBUG

#undef FLUSH

#define LEAFLEN 32

#define USEPARAMSIZE 0xffffffffffffffff

/* Forward */
static int get_chunk(NCZChunkCache* cache, NCZCacheEntry* entry);
static int put_chunk(NCZChunkCache* cache, NCZCacheEntry*);
static int verifycache(NCZChunkCache* cache);
static int flushcache(NCZChunkCache* cache);
static int constraincache(NCZChunkCache* cache, size64_t needed);

static void
setmodified(NCZCacheEntry* e, int tf)
{
    e->modified = tf;
}

/**************************************************/
/* Dispatch table per-var cache functions */

/**
 * @internal Set chunk cache size for a variable. This is the internal
 * function called by nc_set_var_chunk_cache().
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param size Size in bytes to set cache.
 * @param nelems # of entries in cache
 * @param preemption Controls cache swapping.
 *
 * @returns ::NC_NOERR No error.
 * @returns ::NC_EBADID Bad ncid.
 * @returns ::NC_ENOTVAR Invalid variable ID.
 * @returns ::NC_ESTRICTNC3 Attempting netcdf-4 operation on strict
 * nc3 netcdf-4 file.
 * @returns ::NC_EINVAL Invalid input.
 * @returns ::NC_EHDFERR HDF5 error.
 * @author Ed Hartnett
 */
int
NCZ_set_var_chunk_cache(int ncid, int varid, size_t cachesize, size_t nelems, float preemption)
{
    NC_GRP_INFO_T *grp;
    NC_FILE_INFO_T *h5;
    NC_VAR_INFO_T *var;
    NCZ_VAR_INFO_T *zvar;
    int retval = NC_NOERR;

    /* Check input for validity. */
    if (preemption < 0 || preemption > 1)
        {retval = NC_EINVAL; goto done;}

    /* Find info for this file and group, and set pointer to each. */
    if ((retval = nc4_find_nc_grp_h5(ncid, NULL, &grp, &h5)))
        goto done;
    assert(grp && h5);

    /* Find the var. */
    if (!(var = (NC_VAR_INFO_T *)ncindexith(grp->vars, (size_t)varid)))
        {retval = NC_ENOTVAR; goto done;}
    assert(var && var->hdr.id == varid);

    zvar = (NCZ_VAR_INFO_T*)var->format_var_info;
    assert(zvar != NULL && zvar->cache != NULL);

    /* Set the values. */
    var->chunkcache.size = cachesize;
    var->chunkcache.nelems = nelems;
    var->chunkcache.preemption = preemption;

    /* Fix up cache */
    if((retval = NCZ_adjust_var_cache(var))) goto done;
done:
    return retval;
}

/**
 * @internal Adjust the chunk cache of a var for better
 * performance.
 *
 * @note For contiguous and compact storage vars, or when parallel I/O
 * is in use, this function will do nothing and return ::NC_NOERR;
 *
 * @param grp Pointer to group info struct.
 * @param var Pointer to var info struct.
 *
 * @return ::NC_NOERR No error.
 * @author Ed Hartnett
 */
int
NCZ_adjust_var_cache(NC_VAR_INFO_T *var)
{
    int stat = NC_NOERR;
    NCZ_VAR_INFO_T* zvar = (NCZ_VAR_INFO_T*)var->format_var_info;
    NCZChunkCache* zcache = NULL;

    zcache = zvar->cache;
    if(zcache->valid) goto done;

#ifdef DEBUG
fprintf(stderr,"xxx: adjusting cache for: %s\n",var->hdr.name);
#endif

    /* completely empty the cache */
    flushcache(zcache);

    /* Reclaim any existing fill_chunk */
    if((stat = NCZ_reclaim_fill_chunk(zcache))) goto done;
    /* Reset the parameters */
    zvar->cache->params.size = var->chunkcache.size;
    zvar->cache->params.nelems = var->chunkcache.nelems;
    zvar->cache->params.preemption = var->chunkcache.preemption;
#ifdef DEBUG
    fprintf(stderr,"%s.cache.adjust: size=%ld nelems=%ld\n",
        var->hdr.name,(unsigned long)zvar->cache->maxsize,(unsigned long)zvar->cache->maxentries);
#endif
    /* One more thing, adjust the chunksize and count*/
    zcache->chunksize = zvar->chunksize;
    zcache->chunkcount = 1;
    if(var->ndims > 0) {
	size_t i;
	for(i=0;i<var->ndims;i++) {
	    zcache->chunkcount *= var->chunksizes[i];
        }
    }
    zcache->valid = 1;
done:
    return stat;
}

/**************************************************/
/**
 * Create a chunk cache object
 *
 * @param var containing var
 * @param entrysize Size in bytes of an entry
 * @param cachep return cache pointer
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EINVAL Bad preemption.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_create_chunk_cache(NC_VAR_INFO_T* var, size64_t chunksize, char dimsep, NCZChunkCache** cachep)
{
    int stat = NC_NOERR;
    NCZChunkCache* cache = NULL;
    void* fill = NULL;
    NCZ_VAR_INFO_T* zvar = NULL;
	
    if(chunksize == 0) return NC_EINVAL;

    zvar = (NCZ_VAR_INFO_T*)var->format_var_info;
    if((cache = calloc(1,sizeof(NCZChunkCache))) == NULL)
	{stat = NC_ENOMEM; goto done;}
    cache->var = var;
    cache->ndims = var->ndims + zvar->scalar;
    cache->fillchunk = NULL;
    cache->chunksize = chunksize;
    cache->dimension_separator = dimsep;
    zvar->cache = cache;

    cache->chunkcount = 1;
    if(var->ndims > 0) {
	size_t i;
	for(i=0;i<var->ndims;i++) {
	    cache->chunkcount *= var->chunksizes[i];
        }
    }
    
    /* Set default cache parameters */
    cache->params = NC_getglobalstate()->chunkcache;

#ifdef FLUSH
    cache->maxentries = 1;
#endif

#ifdef DEBUG
    fprintf(stderr,"%s.cache: nelems=%ld size=%ld\n",
        var->hdr.name,(unsigned long)cache->maxentries,(unsigned long)cache->maxsize);
#endif
    if((stat = ncxcachenew(LEAFLEN,&cache->xcache))) goto done;
    if((cache->mru = nclistnew()) == NULL)
	{stat = NC_ENOMEM; goto done;}
    nclistsetalloc(cache->mru,cache->params.nelems);

    if(cachep) {*cachep = cache; cache = NULL;}
done:
    nullfree(fill);
    NCZ_free_chunk_cache(cache);
    return THROW(stat);
}

static void
free_cache_entry(NCZChunkCache* cache, NCZCacheEntry* entry)
{
    if(entry) {
        int tid = cache->var->type_info->hdr.id;
	if(tid == NC_STRING && !entry->isfixedstring) {
            NC_reclaim_data(cache->var->container->nc4_info->controller,tid,entry->data,cache->chunkcount);
	}
	nullfree(entry->data);
	nullfree(entry->key.varkey);
	nullfree(entry->key.chunkkey);
	nullfree(entry);
    }
}

void
NCZ_free_chunk_cache(NCZChunkCache* cache)
{
    if(cache == NULL) return;

    ZTRACE(4,"cache.var=%s",cache->var->hdr.name);

    /* Iterate over the entries */
    while(nclistlength(cache->mru) > 0) {
	void* ptr;
        NCZCacheEntry* entry = nclistremove(cache->mru,0);
	(void)ncxcacheremove(cache->xcache,entry->hashkey,&ptr);
	assert(ptr == entry);
        free_cache_entry(cache,entry);
    }
#ifdef DEBUG
fprintf(stderr,"|cache.free|=%ld\n",nclistlength(cache->mru));
#endif
    ncxcachefree(cache->xcache);
    nclistfree(cache->mru);
    cache->mru = NULL;
    (void)NCZ_reclaim_fill_chunk(cache);
    nullfree(cache);
    (void)ZUNTRACE(NC_NOERR);
}

size64_t
NCZ_cache_entrysize(NCZChunkCache* cache)
{
    assert(cache);
    return cache->chunksize;
}

/* Return number of active entries in cache */
size64_t
NCZ_cache_size(NCZChunkCache* cache)
{
    assert(cache);
    return nclistlength(cache->mru);
}

int
NCZ_read_cache_chunk(NCZChunkCache* cache, const size64_t* indices, void** datap)
{
    int stat = NC_NOERR;
    int rank = cache->ndims;
    NCZCacheEntry* entry = NULL;
    ncexhashkey_t hkey = 0;
    int created = 0;

    /* the hash key */
    hkey = ncxcachekey(indices,sizeof(size64_t)*cache->ndims);
    /* See if already in cache */
    stat = ncxcachelookup(cache->xcache,hkey,(void**)&entry);
    switch(stat) {
    case NC_NOERR:
        /* Move to front of the lru */
        (void)ncxcachetouch(cache->xcache,hkey);
        break;
    case NC_ENOOBJECT: case NC_EEMPTY:
        entry = NULL; /* not found; */
	break;
    default: goto done;
    }

    if(entry == NULL) { /*!found*/
	/* Create a new entry */
	if((entry = calloc(1,sizeof(NCZCacheEntry)))==NULL)
	    {stat = NC_ENOMEM; goto done;}
	memcpy(entry->indices,indices,(size_t)rank*sizeof(size64_t));
        /* Create the key for this cache */
        if((stat = NCZ_buildchunkpath(cache,indices,&entry->key))) goto done;
        entry->hashkey = hkey;
	assert(entry->data == NULL && entry->size == 0);
	/* Try to read the object from "disk"; might change size; will create if non-existent */
	if((stat=get_chunk(cache,entry))) goto done;
	assert(entry->data != NULL);
	/* Ensure cache constraints not violated; but do it before entry is added */
	if((stat=verifycache(cache))) goto done;
        nclistpush(cache->mru,entry);
	if((stat = ncxcacheinsert(cache->xcache,entry->hashkey,entry))) goto done;
    }

#ifdef DEBUG
fprintf(stderr,"|cache.read.lru|=%ld\n",nclistlength(cache->mru));
#endif
    if(datap) *datap = entry->data;
    entry = NULL;
    
done:
    if(created && stat == NC_NOERR)  stat = NC_EEMPTY; /* tell upper layers */
    if(entry) free_cache_entry(cache,entry);
    return THROW(stat);
}

#if 0
int
NCZ_write_cache_chunk(NCZChunkCache* cache, const size64_t* indices, void* content)
{
    int stat = NC_NOERR;
    int rank = cache->ndims;
    NCZCacheEntry* entry = NULL;
    ncexhashkey_t hkey;
    
    /* create the hash key */
    hkey = ncxcachekey(indices,sizeof(size64_t)*cache->ndims);

    if(entry == NULL) { /*!found*/
	/* Create a new entry */
	if((entry = calloc(1,sizeof(NCZCacheEntry)))==NULL)
	    {stat = NC_ENOMEM; goto done;}
	memcpy(entry->indices,indices,rank*sizeof(size64_t));
        if((stat = NCZ_buildchunkpath(cache,indices,&entry->key))) goto done;
        entry->hashkey = hkey;
	/* Create the local copy space */
	entry->size = cache->chunksize;
	if((entry->data = calloc(1,cache->chunksize)) == NULL)
	    {stat = NC_ENOMEM; goto done;}
	memcpy(entry->data,content,cache->chunksize);
    }
    setmodified(entry,1);
    nclistpush(cache->mru,entry); /* MRU order */
#ifdef DEBUG
fprintf(stderr,"|cache.write|=%ld\n",nclistlength(cache->mru));
#endif
    entry = NULL;

    /* Ensure cache constraints not violated */
    if((stat=verifycache(cache))) goto done;

done:
    if(entry) free_cache_entry(cache,entry);
    return THROW(stat);
}
#endif

/* Constrain cache */
static int
verifycache(NCZChunkCache* cache)
{
    int stat = NC_NOERR;

#if 0
    /* Sanity check; make sure at least one entry is always allowed */
    if(nclistlength(cache->mru) == 1)
	goto done;
#endif
    if((stat = constraincache(cache,USEPARAMSIZE))) goto done;
done:
    return stat;
}

/* Completely flush cache */

static int
flushcache(NCZChunkCache* cache)
{
    int stat = NC_NOERR;
#if 0
    size_t oldsize = cache->params.size;
    cache->params.size = 0;
    stat = constraincache(cache,USEPARAMSIZE);
    cache->params.size = oldsize;
#else
    stat = constraincache(cache,USEPARAMSIZE);
#endif
    return stat;
}


/* Remove entries to ensure cache is not
   violating any of its constraints.
   On entry, constraints might be violated.
   Make sure that the entryinuse (NULL => no constraint) is not reclaimed.
@param cache
@param needed make sure there is room for this much space; USEPARAMSIZE => ensure no more than  cache params is used.
*/

static int
constraincache(NCZChunkCache* cache, size64_t needed)
{
    int stat = NC_NOERR;
    size64_t final_size;

    /* If the cache is empty then do nothing */
    if(cache->used == 0) goto done;

    if(needed == USEPARAMSIZE)
        final_size = cache->params.size;
    else if(cache->used > needed)
        final_size = cache->used - needed;
    else
        final_size = 0;

    /* Flush from LRU end if we are at capacity */
    while(nclistlength(cache->mru) > cache->params.nelems || cache->used > final_size) {
	size_t i;
	void* ptr;
	NCZCacheEntry* e = ncxcachelast(cache->xcache); /* last entry is the least recently used */
	if(e == NULL) break;
        if((stat = ncxcacheremove(cache->xcache,e->hashkey,&ptr))) goto done;
   	assert(e == ptr);
        for(i=0;i<nclistlength(cache->mru);i++) {
	    e = nclistget(cache->mru,i);
	    if(ptr == e) break;
	}
  	assert(e != NULL);
	assert(i >= 0 && i < nclistlength(cache->mru));
	nclistremove(cache->mru,i);
	assert(cache->used >= e->size);
	/* Note that |old chunk data| may not be same as |new chunk data| because of filters */
	cache->used -= e->size; /* old size */
	if(e->modified) /* flush to file */
	    stat=put_chunk(cache,e);
	/* reclaim */
        nullfree(e->data); nullfree(e->key.varkey); nullfree(e->key.chunkkey); nullfree(e);
    }
#ifdef DEBUG
fprintf(stderr,"|cache.makeroom|=%ld\n",nclistlength(cache->mru));
#endif
done:
    return stat;
}

/**
Push modified cache entries to disk.
Also make sure the cache size is correct.
@param cache
@return NC_EXXX error
*/
int
NCZ_flush_chunk_cache(NCZChunkCache* cache)
{
    int stat = NC_NOERR;
    size_t i;

    ZTRACE(4,"cache.var=%s |cache|=%d",cache->var->hdr.name,(int)nclistlength(cache->mru));

    if(NCZ_cache_size(cache) == 0) goto done;
    
    /* Iterate over the entries in hashmap */
    for(i=0;i<nclistlength(cache->mru);i++) {
        NCZCacheEntry* entry = nclistget(cache->mru,i);
        if(entry->modified) {
	    /* Write out this chunk in toto*/
  	    if((stat=put_chunk(cache,entry)))
	        goto done;
	}
        setmodified(entry,0);
    }
    /* Re-compute space used */
    cache->used = 0;
    for(i=0;i<nclistlength(cache->mru);i++) {
        NCZCacheEntry* entry = nclistget(cache->mru,i);
        cache->used += entry->size;
    }
    /* Make sure cache size and nelems are correct */
    if((stat=verifycache(cache))) goto done;


done:
    return ZUNTRACE(stat);
}

/* Ensure existence of some kind of fill chunk */
int
NCZ_ensure_fill_chunk(NCZChunkCache* cache)
{
    int stat = NC_NOERR;
    size_t i;
    NC_VAR_INFO_T* var = cache->var;
    nc_type typeid = var->type_info->hdr.id;
    size_t typesize = var->type_info->size;

    if(cache->fillchunk) goto done;

    if((cache->fillchunk = malloc(cache->chunksize))==NULL)
        {stat = NC_ENOMEM; goto done;}
    if(var->no_fill) {
        /* use zeros */
	memset(cache->fillchunk,0,cache->chunksize);
	goto done;
    }
    if((stat = NCZ_ensure_fill_value(var))) goto done;
    if(typeid == NC_STRING) {
        char* src = *((char**)(var->fill_value));
	char** dst = (char**)(cache->fillchunk);
        for(i=0;i<cache->chunkcount;i++) dst[i] = strdup(src);
    } else
    switch (typesize) {
    case 1: {
        unsigned char c = *((unsigned char*)var->fill_value);
        memset(cache->fillchunk,c,cache->chunksize);
        } break;
    case 2: {
        unsigned short fv = *((unsigned short*)var->fill_value);
        unsigned short* p2 = (unsigned short*)cache->fillchunk;
        for(i=0;i<cache->chunksize;i+=typesize) *p2++ = fv;
        } break;
    case 4: {
        unsigned int fv = *((unsigned int*)var->fill_value);
        unsigned int* p4 = (unsigned int*)cache->fillchunk;
        for(i=0;i<cache->chunksize;i+=typesize) *p4++ = fv;
        } break;
    case 8: {
        unsigned long long fv = *((unsigned long long*)var->fill_value);
        unsigned long long* p8 = (unsigned long long*)cache->fillchunk;
        for(i=0;i<cache->chunksize;i+=typesize) *p8++ = fv;
        } break;
    default: {
        unsigned char* p;
        for(p=cache->fillchunk,i=0;i<cache->chunksize;i+=typesize,p+=typesize)
            memcpy(p,var->fill_value,typesize);
        } break;
    }
done:
    return NC_NOERR;
}
    
int
NCZ_reclaim_fill_chunk(NCZChunkCache* zcache)
{
    int stat = NC_NOERR;
    if(zcache && zcache->fillchunk) {
	NC_VAR_INFO_T* var = zcache->var;
	int tid = var->type_info->hdr.id;
	size_t chunkcount = zcache->chunkcount;
        stat = NC_reclaim_data_all(var->container->nc4_info->controller,tid,zcache->fillchunk,chunkcount);
	zcache->fillchunk = NULL;
    }
    return stat;
}

int
NCZ_chunk_cache_modify(NCZChunkCache* cache, const size64_t* indices)
{
    int stat = NC_NOERR;
    ncexhashkey_t hkey = 0;
    NCZCacheEntry* entry = NULL;

    /* the hash key */
    hkey = ncxcachekey(indices,sizeof(size64_t)*cache->ndims);

    /* See if already in cache */
    if((stat=ncxcachelookup(cache->xcache, hkey, (void**)&entry))) {stat = NC_EINTERNAL; goto done;}
    setmodified(entry,1);

done:
    return THROW(stat);
}

/**************************************************/
/*
From Zarr V2 Specification:
"The compressed sequence of bytes for each chunk is stored under
a key formed from the index of the chunk within the grid of
chunks representing the array.  To form a string key for a
chunk, the indices are converted to strings and concatenated
with the dimension_separator character ('.' or '/') separating
each index. For example, given an array with shape (10000,
10000) and chunk shape (1000, 1000) there will be 100 chunks
laid out in a 10 by 10 grid. The chunk with indices (0, 0)
provides data for rows 0-1000 and columns 0-1000 and is stored
under the key "0.0"; the chunk with indices (2, 4) provides data
for rows 2000-3000 and columns 4000-5000 and is stored under the
key "2.4"; etc."
*/

/**
 * @param R Rank
 * @param chunkindices The chunk indices
 * @param dimsep the dimension separator
 * @param keyp Return the chunk key string
 */
int
NCZ_buildchunkkey(size_t R, const size64_t* chunkindices, char dimsep, char** keyp)
{
    int stat = NC_NOERR;
    size_t r;
    NCbytes* key = ncbytesnew();

    if(keyp) *keyp = NULL;

    assert(islegaldimsep(dimsep));
    
    for(r=0;r<R;r++) {
	char sindex[64];
        if(r > 0) ncbytesappend(key,dimsep);
	/* Print as decimal with no leading zeros */
	snprintf(sindex,sizeof(sindex),"%lu",(unsigned long)chunkindices[r]);	
	ncbytescat(key,sindex);
    }
    ncbytesnull(key);
    if(keyp) *keyp = ncbytesextract(key);

    ncbytesfree(key);
    return THROW(stat);
}

/**
 * @internal Push data to chunk of a file.
 * If chunk does not exist, create it
 *
 * @param file Pointer to file info struct.
 * @param proj Chunk projection
 * @param datalen size of data
 * @param data Buffer containing the chunk data to write
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
put_chunk(NCZChunkCache* cache, NCZCacheEntry* entry)
{
    int stat = NC_NOERR;
    NC_FILE_INFO_T* file = NULL;
    NCZ_FILE_INFO_T* zfile = NULL;
    NCZMAP* map = NULL;
    char* path = NULL;
    nc_type tid = NC_NAT;
    void* strchunk = NULL;

    ZTRACE(5,"cache.var=%s entry.key=%s",cache->var->hdr.name,entry->key);
    LOG((3, "%s: var: %p", __func__, cache->var));

    file = (cache->var->container)->nc4_info;
    zfile = file->format_file_info;
    map = zfile->map;

    /* Collect some info */
    tid = cache->var->type_info->hdr.id;

    if(tid == NC_STRING && !entry->isfixedstring) {
        /* Convert from char* to char[strlen] format */
        int maxstrlen = NCZ_get_maxstrlen((NC_OBJ*)cache->var);
        assert(maxstrlen > 0);
        if((strchunk = malloc((size_t)cache->chunkcount * (size_t)maxstrlen))==NULL) {stat = NC_ENOMEM; goto done;}
        /* copy char* to char[] format */
        if((stat = NCZ_char2fixed((const char**)entry->data,strchunk,cache->chunkcount,maxstrlen))) goto done;
        /* Reclaim the old chunk */
        if((stat = NC_reclaim_data_all(file->controller,tid,entry->data,cache->chunkcount))) goto done;
        entry->data = NULL;
        entry->data = strchunk; strchunk = NULL;
        entry->size = (cache->chunkcount * (size64_t)maxstrlen);
        entry->isfixedstring = 1;
    }


#ifdef NETCDF_ENABLE_NCZARR_FILTERS
    /* Make sure the entry is in filtered state */
    if(!entry->isfiltered) {
        NC_VAR_INFO_T* var = cache->var;
        void* filtered = NULL; /* pointer to the filtered data */
	size_t flen; /* length of filtered data */
	/* Get the filter chain to apply */
	NClist* filterchain = (NClist*)var->filters;
	if(nclistlength(filterchain) > 0) {
	    /* Apply the filter chain to get the filtered data; will reclaim entry->data */
	    if((stat = NCZ_applyfilterchain(file,var,filterchain,entry->size,entry->data,&flen,&filtered,ENCODING))) goto done;
	    /* Fix up the cache entry */
	    /* Note that if filtered is different from entry->data, then entry->data will have been freed */
	    entry->data = filtered;
 	    entry->size = flen;
            entry->isfiltered = 1;
	}
    }
#endif

    path = NCZ_chunkpath(entry->key);
    stat = nczmap_write(map,path,entry->size,entry->data);
    nullfree(path); path = NULL;

    switch(stat) {
    case NC_NOERR:
	break;
    case NC_ENOOBJECT: case NC_EEMPTY:
    default: goto done;
    }
done:
    nullfree(strchunk);
    nullfree(path);
    return ZUNTRACE(stat);
}

/**
 * @internal Push data from memory to file.
 *
 * @param cache Pointer to parent cache
 * @param key chunk key
 * @param entry cache entry to read into
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
get_chunk(NCZChunkCache* cache, NCZCacheEntry* entry)
{
    int stat = NC_NOERR;
    NCZMAP* map = NULL;
    NC_FILE_INFO_T* file = NULL;
    NCZ_FILE_INFO_T* zfile = NULL;
    NC_TYPE_INFO_T* xtype = NULL;
    char** strchunk = NULL;
    size64_t size;
    int empty = 0;
    char* path = NULL;
    int tid;

    ZTRACE(5,"cache.var=%s entry.key=%s sep=%d",cache->var->hdr.name,entry->key,cache->dimension_separator);
    
    LOG((3, "%s: file: %p", __func__, file));

    file = (cache->var->container)->nc4_info;
    zfile = file->format_file_info;
    map = zfile->map;
    assert(map);

    /* Collect some info */
    xtype = cache->var->type_info;
    tid = xtype->hdr.id;

    /* get size of the "raw" data on "disk" */
    path = NCZ_chunkpath(entry->key);
    stat = nczmap_len(map,path,&size);
    nullfree(path); path = NULL;
    switch(stat) {
    case NC_NOERR: entry->size = size; break;
    case NC_ENOOBJECT: case NC_EEMPTY: empty = 1; stat = NC_NOERR; break;
    default: goto done;
    }

    /* make room in the cache */
    if((stat = constraincache(cache,size))) goto done;    

    if(!empty) {
        /* Make sure we have a place to read it */
        if((entry->data = (void*)calloc(1,entry->size)) == NULL)
	    {stat = NC_ENOMEM; goto done;}
	/* Read the raw data */
        path = NCZ_chunkpath(entry->key);
        stat = nczmap_read(map,path,0,entry->size,(char*)entry->data);
        nullfree(path); path = NULL;
        switch (stat) {
        case NC_NOERR: break;
        case NC_ENOOBJECT: case NC_EEMPTY: empty = 1; stat = NC_NOERR;break;
	default: goto done;
	}
        entry->isfiltered = (int)FILTERED(cache); /* Is the data being read filtered? */
	if(tid == NC_STRING)
	    entry->isfixedstring = 1; /* fill cache is in char[maxstrlen] format */
    }
    if(empty) {
	/* fake the chunk */
        setmodified(entry,(file->no_write?0:1));
	entry->size = cache->chunksize;
	entry->data = NULL;
        entry->isfixedstring = 0;
        entry->isfiltered = 0;
        /* apply fill value */
	if(cache->fillchunk == NULL)
	    {if((stat = NCZ_ensure_fill_chunk(cache))) goto done;}
	if((entry->data = calloc(1,entry->size))==NULL) {stat = NC_ENOMEM; goto done;}
	if((stat = NCZ_copy_data(file,cache->var,cache->fillchunk,cache->chunkcount,ZREADING,entry->data))) goto done;
	stat = NC_NOERR;
    }
#ifdef NETCDF_ENABLE_NCZARR_FILTERS
    /* Make sure the entry is in unfiltered state */
    if(!empty && entry->isfiltered) {
        NC_VAR_INFO_T* var = cache->var;
        void* unfiltered = NULL; /* pointer to the unfiltered data */
        void* filtered = NULL; /* pointer to the filtered data */
	size_t unflen; /* length of unfiltered data */
	assert(tid != NC_STRING || entry->isfixedstring);
	/* Get the filter chain to apply */
	NClist* filterchain = (NClist*)var->filters;
	if(nclistlength(filterchain) == 0) {stat = NC_EFILTER; goto done;}
	/* Apply the filter chain to get the unfiltered data */
	filtered = entry->data;
	entry->data = NULL;
	if((stat = NCZ_applyfilterchain(file,var,filterchain,entry->size,filtered,&unflen,&unfiltered,!ENCODING))) goto done;
	/* Fix up the cache entry */
	entry->data = unfiltered;
	entry->size = unflen;
	entry->isfiltered = 0;
    }
#endif

    if(tid == NC_STRING && entry->isfixedstring) {
        /* Convert from char[strlen] to char* format */
	int maxstrlen = NCZ_get_maxstrlen((NC_OBJ*)cache->var);
	assert(maxstrlen > 0);
	/* copy char[] to char* format */
	if((strchunk = (char**)malloc(sizeof(char*)*cache->chunkcount))==NULL)
  	    {stat = NC_ENOMEM; goto done;}
	if((stat = NCZ_fixed2char(entry->data,strchunk,cache->chunkcount,maxstrlen))) goto done;
	/* Reclaim the old chunk */
	nullfree(entry->data);
	entry->data = NULL;
	entry->data = strchunk; strchunk = NULL;
	entry->size = cache->chunkcount * sizeof(char*);
	entry->isfixedstring = 0;
    }

    /* track new chunk */
    cache->used += entry->size;

done:
    nullfree(strchunk);
    nullfree(path);
    return ZUNTRACE(stat);
}

int
NCZ_buildchunkpath(NCZChunkCache* cache, const size64_t* chunkindices, struct ChunkKey* key)
{
    int stat = NC_NOERR;
    char* chunkname = NULL;
    char* varkey = NULL;

    assert(key != NULL);
    /* Get the chunk object name */
    if((stat = NCZ_buildchunkkey(cache->ndims, chunkindices, cache->dimension_separator, &chunkname))) goto done;
    /* Get the var object key */
    if((stat = NCZ_varkey(cache->var,&varkey))) goto done;
    key->varkey = varkey; varkey = NULL;
    key->chunkkey = chunkname; chunkname = NULL;    

done:
    nullfree(chunkname);
    nullfree(varkey);
    return THROW(stat);
}

void
NCZ_dumpxcacheentry(NCZChunkCache* cache, NCZCacheEntry* e, NCbytes* buf)
{
    char s[8192];
    char idx[64];
    size_t i;

    ncbytescat(buf,"{");
    snprintf(s,sizeof(s),"modified=%u isfiltered=%u indices=",
	(unsigned)e->modified,
	(unsigned)e->isfiltered
	);
    ncbytescat(buf,s);
    for(i=0;i<cache->ndims;i++) {
	snprintf(idx,sizeof(idx),"%s%llu",(i==0?"":"."),e->indices[i]);
	ncbytescat(buf,idx);
    }
    snprintf(s,sizeof(s),"size=%llu data=%p",
	e->size,
	e->data
	);
    ncbytescat(buf,s);
    ncbytescat(buf,"}");
}

void
NCZ_printxcache(NCZChunkCache* cache)
{
    static char xs[20000];
    NCbytes* buf = ncbytesnew();
    char s[8192];
    size_t i;

    ncbytescat(buf,"NCZChunkCache:\n");
    snprintf(s,sizeof(s),"\tvar=%s\n\tndims=%u\n\tchunksize=%u\n\tchunkcount=%u\n\tfillchunk=%p\n",
    	cache->var->hdr.name,
    	(unsigned)cache->ndims,
    	(unsigned)cache->chunksize,
    	(unsigned)cache->chunkcount,
	cache->fillchunk
	);
    ncbytescat(buf,s);

    snprintf(s,sizeof(s),"\tmaxentries=%u\n\tmaxsize=%u\n\tused=%u\n\tdimsep='%c'\n",
    	(unsigned)cache->params.nelems,
	(unsigned)cache->params.size,
    	(unsigned)cache->used,
    	cache->dimension_separator
	);
    ncbytescat(buf,s);
    
    snprintf(s,sizeof(s),"\tmru: (%u)\n",(unsigned)nclistlength(cache->mru));
    ncbytescat(buf,s);
    if(nclistlength(cache->mru)==0)    
        ncbytescat(buf,"\t\t<empty>\n");
    for(i=0;i<nclistlength(cache->mru);i++) {
	NCZCacheEntry* e = (NCZCacheEntry*)nclistget(cache->mru,i);
	snprintf(s,sizeof(s),"\t\t[%zu] ", i);
	ncbytescat(buf,s);
	if(e == NULL)
	    ncbytescat(buf,"<null>");
	else
	    NCZ_dumpxcacheentry(cache, e, buf);
	ncbytescat(buf,"\n");
    }

    xs[0] = '\0';
    strlcat(xs,ncbytescontents(buf),sizeof(xs));
    ncbytesfree(buf);
    fprintf(stderr,"%s\n",xs);
}
