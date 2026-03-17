/*
 * Copyright 2018, University Corporation for Atmospheric Research
 * See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */
/**
 * @file
 * Functions for working with filters. 
 */

#include "config.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#ifdef _MSC_VER
#include <io.h>
#endif

#include "netcdf.h"
#include "netcdf_filter.h"
#include "ncdispatch.h"
#include "nc4internal.h"
#include "nclog.h"

#ifdef USE_HDF5
#include "hdf5internal.h"
#endif

#ifdef NETCDF_ENABLE_NCZARR
#include "zdispatch.h"
#endif

/*
Unified filter related code
*/

/**************************************************/
/* Per-variable filters */

/**
Find the set of filters (if any) associated with a variable.
Assumes HDF5 format using unsigned ints.

@param ncid NetCDF or group ID, from a previous call to nc_open(),
nc_create(), nc_def_grp(), or associated inquiry functions such as
nc_inq_ncid().
@param varid Variable ID
@param nfiltersp Pointer that gets the number of filters; may be zero.
@param ids return the filter ids (caller allocates)

@returns ::NC_NOERR No error.
@returns ::NC_ENOTNC4 Not a netCDF-4 file.
@returns ::NC_EBADID Bad ncid
@returns ::NC_ENOTVAR Invalid variable ID.
@returns ::NC_EINVAL Invalid arguments
@ingroup variables
@author Dennis Heimbigner
*/
EXTERNL int
nc_inq_var_filter_ids(int ncid, int varid, size_t* nfiltersp, unsigned int* ids)
{
    NC* ncp;
    int stat = NC_check_id(ncid,&ncp);
    if(stat != NC_NOERR) return stat;
    TRACE(nc_inq_var_filter_ids);
    if((stat = ncp->dispatch->inq_var_filter_ids(ncid,varid,nfiltersp,ids))) goto done;

done:
   return stat;
}

/**
Find the the param info about filter (if any)
associated with a variable and with specified id.
Assumes HDF5 format using unsigned ints.

@param ncid NetCDF or group ID, from a previous call to nc_open(),
nc_create(), nc_def_grp(), or associated inquiry functions such as
nc_inq_ncid().

@param varid Variable ID
@param id The filter id of interest
@param nparamsp (Out) Storage which will get the number of parameters to the filter
@param params (Out) Storage which will get associated parameters.
Note: the caller must allocate and free.

@returns ::NC_NOERR No error.
@returns ::NC_ENOTNC4 Not a netCDF-4 file.
@returns ::NC_EBADID Bad ncid.
@returns ::NC_ENOTVAR Invalid variable ID.
@returns ::NC_ENOFILTER Specified filter not defined for this variable.
@ingroup variables
@author Dennis Heimbigner
*/
EXTERNL int
nc_inq_var_filter_info(int ncid, int varid, unsigned int id, size_t* nparamsp, unsigned int* params)
{
    NC* ncp;
    int stat = NC_check_id(ncid,&ncp);
    if(stat != NC_NOERR) return stat;
    TRACE(nc_inq_var_filter_info);
    if((stat = ncp->dispatch->inq_var_filter_info(ncid,varid,id,nparamsp,params))) goto done;

done:
     if(stat == NC_ENOFILTER) nclog(NCLOGWARN,"Undefined filter: %u",(unsigned)id);
     return stat;
}

/**
   Define a new variable filter
   Assumes HDF5 format using unsigned ints.
   Only variables with chunked storage can use filters.

   @param ncid File and group ID.
   @param varid Variable ID.
   @param id Filter ID.
   @param nparams Number of filter parameters.
   @param params Filter parameters.

   @return ::NC_NOERR No error.
   @return ::NC_EINVAL Variable must be chunked.
   @return ::NC_EBADID Bad ID.
   @author Dennis Heimbigner
*/

EXTERNL int
nc_def_var_filter(int ncid, int varid, unsigned int id, size_t nparams, const unsigned int* params)
{
    int stat = NC_NOERR;
    NC* ncp;

    TRACE(nc_inq_var_filter);
    if((stat = NC_check_id(ncid,&ncp))) return stat;
    if((stat = ncp->dispatch->def_var_filter(ncid,varid,id,nparams,params))) goto done;
done:
     if(stat == NC_ENOFILTER) nclog(NCLOGWARN,"Undefined filter: %u",(unsigned)id);
    return stat;
}

/**
Find the first filter (if any) associated with a variable.
Assumes HDF5 format using unsigned int.
   
@param ncid NetCDF or group ID, from a previous call to nc_open(),
nc_create(), nc_def_grp(), or associated inquiry functions such as
nc_inq_ncid().

@param varid Variable ID

@param idp Storage which will get the filter id; a return value of zero means variable has no filters.

@param nparamsp Storage which will get the number of parameters to the filter

@param params Storage which will get associated parameters (call allocates and frees).

This is redundant over the multi-filter API, so
it can be implemented in terms of those functions.

@returns ::NC_NOERR No error.
@returns ::NC_ENOTNC4 Not a netCDF-4 file.
@returns ::NC_EBADID Bad ncid.
@returns ::NC_ENOTVAR Invalid variable ID.

@ingroup variables
@author Dennis Heimbigner
*/
EXTERNL int
nc_inq_var_filter(int ncid, int varid, unsigned int* idp, size_t* nparamsp, unsigned int* params)
{
    NC* ncp;
    size_t nfilters;
    unsigned int* ids = NULL;
    int stat = NC_check_id(ncid,&ncp);

    if(stat != NC_NOERR) return stat;
    TRACE(nc_inq_var_filter);

    /* Get the number of filters on this variable */
    if((stat = nc_inq_var_filter_ids(ncid,varid,&nfilters, NULL))) goto done;
    /* If no filters, then return zero */
    if(nfilters == 0) {
	if(idp) *idp = 0;
	goto done;
    }
    /* Get the filter ids */
    if((ids = calloc(sizeof(unsigned int),nfilters)) == NULL) {stat = NC_ENOMEM; goto done;}
    if((stat = nc_inq_var_filter_ids(ncid,varid,&nfilters, ids))) goto done;
    /* Get params for the first filter */
    if((stat = nc_inq_var_filter_info(ncid,varid,ids[0],nparamsp,params))) goto done;
    if(idp) *idp = ids[0];
 done:
    nullfree(ids);        
    return stat;
}

/** Test if filter is available. Would prefer
 * returning a list of all available filters, but HDF5
 * does not support that capability.
 *
 * @param ncid ID of file for which filter list is desired
 * @param id filter id of interest
 *
 * @return NC_NOERR if the filter is available
 * @return NC_EBADID if ncid is invalid
 * @return NC_ENOFILTER if filter is not available.
 * @author Dennis Heimbigner
*/

EXTERNL int
nc_inq_filter_avail(int ncid, unsigned id)
{
    int stat = NC_NOERR;
    NC* ncp;

    stat = NC_check_id(ncid,&ncp);
    if(stat != NC_NOERR) return stat;
    if((stat = ncp->dispatch->inq_filter_avail(ncid,id))) goto done;
done:
    return stat;
}

/**************************************************/
/* Functions for accessing standardized filters */

/**
 * Turn on bzip2 compression for a variable.
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param level From 1 to 9. Set the block size to 100k, 200k ... 900k
 * when compressing. (bzip2 default level is 9).
 *
 * @return 0 for success, error code otherwise.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
nc_def_var_bzip2(int ncid, int varid, int level)
{
    int stat = NC_NOERR;
    unsigned ulevel;
    
    if((stat = nc_inq_filter_avail(ncid,H5Z_FILTER_BZIP2))) goto done;
    /* Filter is available */
    /* 1 <= Level <= 9 */
    if (level < 1 || level > 9)
        return NC_EINVAL;
    ulevel = (unsigned) level; /* Keep bit pattern */
    if((stat = nc_def_var_filter(ncid,varid,H5Z_FILTER_BZIP2,1,&ulevel))) goto done;
done:
    return stat;
}

/**
 * Learn whether bzip2 compression is on for a variable, and, if so,
 * the level setting.
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param hasfilterp Pointer that gets a 0 if bzip2 is not in use for this
 * var, and a 1 if it is. Ignored if NULL.
 * @param levelp Pointer that gets the level setting (from 1 to 9), if
 * bzip2 is in use. Ignored if NULL.
 *
 * @return 0 for success, error code otherwise.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
nc_inq_var_bzip2(int ncid, int varid, int* hasfilterp, int *levelp)
{
    int stat = NC_NOERR;
    size_t nparams;
    unsigned params = 0;
    int hasfilter = 0;
    
    if((stat = nc_inq_filter_avail(ncid,H5Z_FILTER_BZIP2))) goto done;
    /* Filter is available */
    /* Get filter info */
    stat = nc_inq_var_filter_info(ncid,varid,H5Z_FILTER_BZIP2,&nparams,NULL);
    if(stat == NC_ENOFILTER) {stat = NC_NOERR; hasfilter = 0; goto done;}
    if(stat != NC_NOERR) goto done;
    hasfilter = 1;
    if(nparams != 1) {stat = NC_EFILTER; goto done;}
    if((stat = nc_inq_var_filter_info(ncid,varid,H5Z_FILTER_BZIP2,&nparams,&params))) goto done;
done:
    if(levelp) *levelp = (int)params;
    if(hasfilterp) *hasfilterp = hasfilter;
    return stat;
}

/**
 * Turn on Zstandard compression for a variable.
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param level From -131072 to 22 (depends on Zstandard version). 
 * when compressing. Regular compression levels are from 1 up to 19.
 * Use levels >= 20, labeled `--ultra`, cautiously: they require more memory. 
 * Negative compression levels that extend the range of speed vs. ratio preferences.
 * The lower the level, the faster the speed (at the cost of compression).
 *
 * @return 0 for success, error code otherwise.
 * @author Charlie Zender, Dennis Heimbigner, Ed Hartnett
 */
int
nc_def_var_zstandard(int ncid, int varid, int level)
{
#ifdef HAVE_ZSTD
    int stat = NC_NOERR;
    unsigned ulevel;
    
    if((stat = nc_inq_filter_avail(ncid,H5Z_FILTER_ZSTD))) goto done;
    /* Filter is available */
    /* Level must be between -131072 and 22 on Zstandard v. 1.4.5 (~202009)
       Earlier versions have fewer levels (especially fewer negative levels) */
    if (level < -131072 || level > 22)
        return NC_EINVAL;
    ulevel = (unsigned) level; /* Keep bit pattern */
    if((stat = nc_def_var_filter(ncid,varid,H5Z_FILTER_ZSTD,1,&ulevel))) goto done;
done:
    return stat;
#else
    return NC_NOERR;
#endif /*HAVE_ZSTD*/
}

/**
 * Learn whether Zstandard compression is on for a variable, and, if so,
 * the level setting.
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param hasfilterp Pointer that gets a 0 if Zstandard is not in use for this
 * var, and a 1 if it is. Ignored if NULL.
 * @param levelp Pointer that gets the level setting (from -131072 to 22), if
 * Zstandard is in use. Ignored if NULL.
 *
 * @return 0 for success, error code otherwise.
 * @author Charlie Zender, Dennis Heimbigner, Ed Hartnett
 */
int
nc_inq_var_zstandard(int ncid, int varid, int* hasfilterp, int *levelp)
{
#ifdef HAVE_ZSTD
    int stat = NC_NOERR;
    size_t nparams;
    unsigned params = 0;
    int hasfilter = 0;
    
    if((stat = nc_inq_filter_avail(ncid,H5Z_FILTER_ZSTD))) goto done;
    /* Filter is available */
    /* Get filter info */
    stat = nc_inq_var_filter_info(ncid,varid,H5Z_FILTER_ZSTD,&nparams,NULL);
    if(stat == NC_ENOFILTER) {stat = NC_NOERR; hasfilter = 0; goto done;}
    if(stat != NC_NOERR) goto done;
    hasfilter = 1;
    if(nparams != 1) {stat = NC_EFILTER; goto done;}
    if((stat = nc_inq_var_filter_info(ncid,varid,H5Z_FILTER_ZSTD,&nparams,&params))) goto done;
done:
    if(levelp) *levelp = (int)params;
    if(hasfilterp) *hasfilterp = hasfilter;
    return stat;
#else
    return NC_NOERR;
#endif /*HAVE_ZSTD*/
}

/**
 * Turn on blosc for a variable.
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param subcompressor The subcompressor.
 * @param level The level setting.
 * @param blocksize The block size.
 * @param addshuffle If non-zero, turn on shuffle.
 *
 * @return 0 for success, error code otherwise.
 * @author Dennis Heimbigner
 * @ingroup variables
 */
int
nc_def_var_blosc(int ncid, int varid, unsigned subcompressor, unsigned level, unsigned blocksize, unsigned addshuffle)
{
#ifdef HAVE_BLOSC
    int stat = NC_NOERR;
    unsigned params[7];;
    
    if((stat = nc_inq_filter_avail(ncid,H5Z_FILTER_BLOSC))) goto done;
    /* Filter is available */

    /* Verify parameters */
    if(addshuffle > (unsigned)BLOSC_BITSHUFFLE) {stat = NC_EINVAL; goto done;}
    if(subcompressor > (unsigned)BLOSC_ZSTD) {stat = NC_EINVAL; goto done;}

    /* Set the parameters */
    params[0] = 0;
    params[1] = 0;
    params[2] = 0;
    params[3] = blocksize;
    params[4] = level;
    params[5] = addshuffle;
    params[6] = subcompressor;
    if((stat = nc_def_var_filter(ncid,varid,H5Z_FILTER_BLOSC,7,params))) goto done;
done:
    return stat;
#else
    return NC_NOERR;
#endif
}

/**
 * Learn whether Blosc compression is on for a variable, and, if so,
 * the settings.
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param hasfilterp Pointer that gets a 0 if blosc is not in use for this
 * var, and a 1 if it is. Ignored if NULL.
 * @param subcompressorp Pointer that gets the subcompressor, if
 * blosc is in use. Ignored if NULL.
 * @param levelp Pointer that gets the level setting, if
 * blosc is in use. Ignored if NULL.
 * @param blocksizep Pointer that gets the block size, if
 * blosc is in use. Ignored if NULL.
 * @param addshufflep Pointer that gets non-zero value if shuffle is
 * in use, if blosc is in use. Ignored if NULL.
 *
 * @return 0 for success, error code otherwise.
 * @author Dennis Heimbigner
 */
int
nc_inq_var_blosc(int ncid, int varid, int* hasfilterp, unsigned* subcompressorp,
		 unsigned* levelp, unsigned* blocksizep, unsigned* addshufflep)
{
#ifdef HAVE_BLOSC
    int stat = NC_NOERR;
    size_t nparams;
    unsigned params[7];
    int hasfilter = 0;
    
    if((stat = nc_inq_filter_avail(ncid,H5Z_FILTER_BLOSC))) goto done;
    /* Filter is available */

    /* Get filter info */
    stat = nc_inq_var_filter_info(ncid,varid,H5Z_FILTER_BLOSC,&nparams,NULL);
    if(stat == NC_ENOFILTER) {stat = NC_NOERR; hasfilter = 0; goto done;}
    if(stat != NC_NOERR) goto done;
    hasfilter = 1;
    if(nparams != 7) {stat = NC_EFILTER; goto done;}
    if((stat = nc_inq_var_filter_info(ncid,varid,H5Z_FILTER_BLOSC,&nparams,params))) goto done;
    if(blocksizep) *blocksizep = params[3];
    if(levelp) *levelp = params[4];
    if(addshufflep) *addshufflep = params[5];
    if(subcompressorp) *subcompressorp = params[6];
done:
    if(hasfilterp) *hasfilterp = hasfilter;
    return stat;
#else
    return NC_NOERR;
#endif
}
