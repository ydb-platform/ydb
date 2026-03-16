/* Copyright 2018, University Corporation for Atmospheric
 * Research. See COPYRIGHT file for copying and redistribution
 * conditions. */
/**
 * @file
 * The netCDF-4 functions which control HDF5 caching. These caching
 * controls allow the user to change the cache sizes of HDF5 before
 * opening files.
 *
 * @author Ed Hartnett
 */

#include "config.h"
#include "nc4internal.h"

/**
 * Set chunk cache size. Only affects netCDF-4/HDF5 files
 * opened/created *after* it is called.
 *
 * The HDF5 chunk cache for each dataset is used by HDF5 when reading
 * and writing files. The size of the chunk cache can be set with this
 * function (for all variables in the file) or on a variable basis
 * with nc_set_var_chunk_cache().
 *
 * Increasing the size of the cache only helps if data access patterns
 * support it. If data is read in one sequential pass through the
 * file, then the cache will not help much. If data are read from the
 * same file multiple times, the chunk cache can improve performance.
 *
 * The current settings for the file level chunk cache can be obtained
 * with nc_get_chunk_cache().
 *
 * For more information on HDF5 caching, see
 * https://support.hdfgroup.org/HDF5/doc/RM/RM_H5P.html#Property-SetCache.
 *
 * @param size Size in bytes to set cache. The default value is 64 MB;
 * the default may be changed with configure option
 * --with-chunk-cache-size.
 *
 * @param nelems Number of elements to hold in cache. This is passed
 * to the nslots parameter of the HDF5 function H5Pset_cache(). This
 * should be a prime number at least ten times larger than the maximum
 * number of chunks that are set in the cache. The default value is
 * 4133; the default may be set with configure option
 * --with-chunk-cache-nelems.
 *
 * @param preemption Preemption strategy, a float between 0 and 1
 * inclusive and indicates the weighting according to which chunks
 * which have been fully read or written are penalized when
 * determining which chunks to flush from cache. A value of 0 means
 * fully read or written chunks are treated no differently than other
 * chunks (the preemption is strictly LRU) while a value of 1 means
 * fully read or written chunks are always preempted before other
 * chunks. If your application only reads or writes data once, this
 * can be safely set to 1. Otherwise, this should be set lower
 * depending on how often you re-read or re-write the same data. The
 * default value is 0.75; the default may be set with configure option
 * --with-chunk-cache-preemption.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EINVAL Bad preemption.
 * @author Ed Hartnett
 * @ingroup datasets
 */
int
nc_set_chunk_cache(size_t size, size_t nelems, float preemption)
{
    NCglobalstate* gs = NC_getglobalstate();
    if (preemption < 0 || preemption > 1)
        return NC_EINVAL;
    gs->chunkcache.size = size;
    gs->chunkcache.nelems = nelems;
    gs->chunkcache.preemption = preemption;
    return NC_NOERR;
}

/**
 * Get current netCDF chunk cache settings. These settings may be
 * changed with nc_set_chunk_cache(). This function does not get the
 * settings from HDF5, it simply reports the current netCDF chunk
 * cache settings.
 *
 * @param sizep Pointer that gets size in bytes to set cache. Ignored
 * if NULL.
 * @param nelemsp Pointer that gets number of elements to hold in
 * cache. Ignored if NULL.
 * @param preemptionp Pointer that gets preemption strategy (between 0
 * and 1). Ignored if NULL.
 *
 * @return ::NC_NOERR No error.
 * @author Ed Hartnett
 * @ingroup datasets
 */
int
nc_get_chunk_cache(size_t *sizep, size_t *nelemsp, float *preemptionp)
{
    NCglobalstate* gs = NC_getglobalstate();
    if (sizep)
        *sizep = gs->chunkcache.size;

    if (nelemsp)
        *nelemsp = gs->chunkcache.nelems;

    if (preemptionp)
        *preemptionp = gs->chunkcache.preemption;
    return NC_NOERR;
}

/**
 * @internal Set the chunk cache. This is like nc_set_chunk_cache()
 * but with integers instead of size_t, and with an integer preemption
 * (which is the float preemtion * 100). This was required for fortran
 * to avoid size_t issues.
 * Note: if netcdf-4 is completely disabled, then the definitions in
 * libdispatch/dfile.c take effect.
 *
 * @param size Cache size.
 * @param nelems Number of elements.
 * @param preemption Preemption * 100.
 *
 * @return NC_NOERR No error.
 * @author Ed Hartnett
 */
int
nc_set_chunk_cache_ints(int size, int nelems, int preemption)
{
    NCglobalstate* gs = NC_getglobalstate();
    if (size <= 0 || nelems <= 0 || preemption < 0 || preemption > 100)
        return NC_EINVAL;
    gs->chunkcache.size = (size_t)size;
    gs->chunkcache.nelems = (size_t)nelems;
    gs->chunkcache.preemption = (float)preemption / 100;
    return NC_NOERR;
}

/**
 * @internal Get the chunk cache settings. This is like
 * nc_get_chunk_cache() but with integers instead of size_t, and with
 * an integer preemption (which is the float preemtion * 100). This
 * was required for fortran to avoid size_t issues.
 * Note: if netcdf-4 is completely disabled, then the definitions in
 * libdispatch/dfile.c take effect.
 *
 * @param sizep Pointer that gets cache size.
 * @param nelemsp Pointer that gets number of elements.
 * @param preemptionp Pointer that gets preemption * 100.
 *
 * @return NC_NOERR No error.
 * @author Ed Hartnett
 */
int
nc_get_chunk_cache_ints(int *sizep, int *nelemsp, int *preemptionp)
{
    NCglobalstate* gs = NC_getglobalstate();
    if (sizep)
        *sizep = (int)gs->chunkcache.size;
    if (nelemsp)
        *nelemsp = (int)gs->chunkcache.nelems;
    if (preemptionp)
        *preemptionp = (int)(gs->chunkcache.preemption * 100);

    return NC_NOERR;
}

#ifndef USE_HDF5
/* See definitions in libhd5/hdf5var.c */
/* Make sure they are always defined */
/* Note: if netcdf-4 is completely disabled, then the definitions in
 * libdispatch/dfile.c take effect.
 */

int
nc_set_var_chunk_cache_ints(int ncid, int varid, int size, int nelems,
                            int preemption)
{
    return NC_NOERR;
}

int
nc_def_var_chunking_ints(int ncid, int varid, int storage, int *chunksizesp)
{
    return NC_NOERR;
}

#endif /*USE_HDF5*/
