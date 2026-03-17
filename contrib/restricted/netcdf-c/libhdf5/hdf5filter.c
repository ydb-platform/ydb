/* Copyright 2003-2018, University Corporation for Atmospheric
 * Research. See the COPYRIGHT file for copying and redistribution
 * conditions.
 */
/**
 * @file @internal Internal netcdf-4 functions for filters
 *
 * This file contains functions internal to the netcdf4 library. None of
 * the functions in this file are exposed in the exetnal API. These
 * functions all relate to the manipulation of netcdf-4 filters
 *
 * @author Dennis Heimbigner
 */

#include "config.h"
#include <stddef.h>
#include <stdlib.h>
#include "hdf5internal.h"
#include "hdf5debug.h"
#include "netcdf.h"
#include "netcdf_filter.h"

#ifdef NETCDF_ENABLE_BLOSC
#error #include <blosc.h>
#endif

#undef TFILTERS

/* Forward */
static int NC4_hdf5_filter_free(struct NC_HDF5_Filter* spec);

/**************************************************/
/**
 * @file
 * @internal
 * Internal netcdf hdf5 filter functions.
 *
 * This file contains functions internal to the libhdf5 library.
 * None of the functions in this file are exposed in the exetnal API. These
 * functions all relate to the manipulation of netcdf-4's var->filters list.
 *
 * @author Dennis Heimbigner
 */
#ifdef TFILTERS
static void
printfilter1(struct NC_HDF5_Filter* nfs)
{
    int i;
    if(nfs == NULL) {
	fprintf(stderr,"{null}");
	return;
    }
    fprintf(stderr,"{%u,(%u)",nfs->filterid,(int)nfs->nparams);
    for(i=0;i<nfs->nparams;i++) {
      fprintf(stderr," %s",nfs->params[i]);
    }
    fprintf(stderr,"}");
}

static void
printfilter(struct NC_HDF5_Filter* nfs, const char* tag, int line)
{
    fprintf(stderr,"%s: line=%d: ",tag,line);
    printfilter1(nfs);
    fprintf(stderr,"\n");
}

static void
printfilterlist(NC_VAR_INFO_T* var, const char* tag, int line)
{
    int i;
    const char* name;
    if(var == NULL) name = "null";
    else if(var->hdr.name == NULL) name = "?";
    else name = var->hdr.name;
    fprintf(stderr,"%s: line=%d: var=%s filters=",tag,line,name);
    if(var != NULL) {
	NClist* filters = (NClist*)var->filters;
        for(i=0;i<nclistlength(filters);i++) {
	    struct NC_HDF5_Filter* nfs = (struct NC_HDF5_Filter*)nclistget(filters,i);
	    fprintf(stderr,"[%d]",i);
	    printfilter1(nfs);
	}
    }
    fprintf(stderr,"\n");
}

#define PRINTFILTER(nfs, tag) printfilter(nfs,tag,__LINE__)
#define PRINTFILTERLIST(var,tag) printfilterlist(var,tag,__LINE__)
#else
#define PRINTFILTER(nfs, tag)
#define PRINTFILTERLIST(var,tag)
#endif

int
NC4_hdf5_filter_freelist(NC_VAR_INFO_T* var)
{
    int stat=NC_NOERR;
    NClist* filters = (NClist*)var->filters;

    if(filters == NULL) goto done;
PRINTFILTERLIST(var,"free: before");
    /* Free the filter list leaving ptrs in place */
    for(size_t i = 0; i < nclistlength(filters);i++) {
	struct NC_HDF5_Filter* spec = (struct NC_HDF5_Filter*)nclistget(filters,i);
	assert((spec->nparams == 0 && spec->params == NULL) || (spec->nparams > 0 && spec->params != NULL));
	if(spec->nparams > 0) nullfree(spec->params);
	nullfree(spec);
    }
PRINTFILTERLIST(var,"free: after");
    nclistfree(filters);
    var->filters = NULL;
done:
    return stat;
}

static int
NC4_hdf5_filter_free(struct NC_HDF5_Filter* spec)
{
    if(spec == NULL) goto done;
PRINTFILTER(spec,"free");
    if(spec->nparams > 0) nullfree(spec->params)
    free(spec);
done:
    return NC_NOERR;
}

/**
 * Add filter to netCDF's list of filters for a variable.
 * @internal
 *
 * This function handles necessary filter ordering for shuffle and
 * fletcher32 filters.
 *
 * @param var Pointer to the NC_VAR_INFO_T for a variable.
 * @param id HDF5 filter ID.
 * @param nparams Number of parameters in filter parameter list.
 * @param params Array that holds nparams unsigned ints - the filter parameters.
 * @param flags NetCDF flags about the filter.
 *
 * @return ::NC_NOERR on success, error code otherwise.
 * @author Dennis Heimbigner
 */
int
NC4_hdf5_addfilter(NC_VAR_INFO_T* var, unsigned int id, size_t nparams, const unsigned int* params, int flags)
{
    int stat = NC_NOERR;
    struct NC_HDF5_Filter* fi = NULL;
    int olddef = 0; /* 1=>already defined */
    NClist* flist = (NClist*)var->filters;
    
    if(nparams > 0 && params == NULL)
	{stat = NC_EINVAL; goto done;}
    
    if((stat=NC4_hdf5_filter_lookup(var,id,&fi))==NC_NOERR) {
	assert(fi != NULL);
        /* already exists */
	olddef = 1;	
    } else {
	stat = NC_NOERR;
        if((fi = calloc(1,sizeof(struct NC_HDF5_Filter))) == NULL)
	    {stat = NC_ENOMEM; goto done;}
        fi->filterid = id;
	olddef = 0;
    }    
    fi->nparams = nparams;
    if(fi->params != NULL) {
	nullfree(fi->params);
	fi->params = NULL;
    }
    assert(fi->params == NULL);
    if(fi->nparams > 0) {
	if((fi->params = (unsigned int*)malloc(sizeof(unsigned int)*fi->nparams)) == NULL)
	    {stat = NC_ENOMEM; goto done;}
        memcpy(fi->params,params,sizeof(unsigned int)*fi->nparams);
    }
    fi->flags = flags;
    if(!olddef) {
	size_t pos = nclistlength(flist);
        /* Need to be careful about where we insert fletcher32 and shuffle */
	if(nclistlength(flist) > 0) {
	    if(id == H5Z_FILTER_FLETCHER32)
		pos = 0; /* always first filter */
	    else if(id == H5Z_FILTER_SHUFFLE) {
		/* See if first filter is fletcher32 */
	        struct NC_HDF5_Filter* f0 = (struct NC_HDF5_Filter*)nclistget(flist,0);
		if(f0->filterid == H5Z_FILTER_FLETCHER32) pos = 1; else pos = 0;
	    }
	}
        nclistinsert(flist,pos,fi);
PRINTFILTERLIST(var,"add");
    }
    fi = NULL; /* either way,its in the var->filters list */

done:
    if(fi) NC4_hdf5_filter_free(fi);    
    return THROW(stat);
}

/**
 * @internal
 * Remove a filter from netCDF's list of filters for a variable.
 *
 * @param var Pointer to the NC_VAR_INFO_T for a variable.
 * @param id HDF5 filter ID.
 *
 * @return ::NC_NOERR on success, error code otherwise.
 * @author Dennis Heimbigner
 */
int
NC4_hdf5_filter_remove(NC_VAR_INFO_T* var, unsigned int id)
{
    NClist* flist = (NClist*)var->filters;

    /* Walk backwards */
    for(size_t k = nclistlength(flist); k-->0;) {
	struct NC_HDF5_Filter* f = (struct NC_HDF5_Filter*)nclistget(flist,k);
        if(f->filterid == id) {
	    /* Remove from variable */
    	    nclistremove(flist,k);
#ifdef TFILTERS
PRINTFILTERLIST(var,"remove");
fprintf(stderr,"\tid=%s\n",id);
#endif
	    /* Reclaim */
	    NC4_hdf5_filter_free(f);
	    return NC_NOERR;
	}
    }
    return NC_ENOFILTER;
}

/**
 * @internal
 * Find a filter in netCDF's list of filters for a variable.
 *
 * @param var Pointer to the NC_VAR_INFO_T for a variable.
 * @param id HDF5 filter ID.
 * @param specp Filter specification.
 *
 * @return ::NC_NOERR on success, error code otherwise.
 * @author Dennis Heimbigner
 */
int
NC4_hdf5_filter_lookup(NC_VAR_INFO_T* var, unsigned int id, struct NC_HDF5_Filter** specp)
{
    size_t i;
    NClist* flist = (NClist*)var->filters;
    
    if(flist == NULL) {
	if((flist = nclistnew())==NULL)
	    return NC_ENOMEM;
	var->filters = (void*)flist;
    }
    for(i=0;i<nclistlength(flist);i++) {
	struct NC_HDF5_Filter* spec = (struct NC_HDF5_Filter*)nclistget(flist,i);
	if(id == spec->filterid) {
	    if(specp) *specp = spec;
	    return NC_NOERR;
	}
    }
    return NC_ENOFILTER;
}

/**
 * @internal
 * Add a filter for a variable in a netCDF/HDF5 file.
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param id HDF5 filter ID.
 * @param nparams Number of parameters in filter parameter list.
 * @param params Array that holds nparams unsigned ints - the filter parameters.
 *
 * @return ::NC_NOERR on success, error code otherwise.
 * @author Dennis Heimbigner
 */
int
NC4_hdf5_def_var_filter(int ncid, int varid, unsigned int id, size_t nparams,
                   const unsigned int* params)
{
    int stat = NC_NOERR;
    NC *nc;
    NC_FILE_INFO_T* h5 = NULL;
    NC_GRP_INFO_T* grp = NULL;
    NC_VAR_INFO_T* var = NULL;
    struct NC_HDF5_Filter* oldspec = NULL;
    int flags = 0;
    htri_t avail = -1;
    int havedeflate = 0;
    int haveszip = 0;

    LOG((2, "%s: ncid 0x%x varid %d", __func__, ncid, varid));

    if((stat = NC_check_id(ncid,&nc))) return stat;
    assert(nc);

    /* Find info for this file and group and var, and set pointer to each. */
    if ((stat = nc4_hdf5_find_grp_h5_var(ncid, varid, &h5, &grp, &var)))
	{stat = THROW(stat); goto done;}

    assert(h5 && var && var->hdr.id == varid);

    /* If the HDF5 dataset has already been created, then it is too
     * late to set all the extra stuff. */
    if (!(h5->flags & NC_INDEF))
	{stat = THROW(NC_EINDEFINE); goto done;}
    if (!var->ndims)
	{stat = NC_EINVAL; goto done;} /* For scalars, complain */
    if (var->created)
        {stat = THROW(NC_ELATEDEF); goto done;}
    /* Can't turn on parallel and szip before HDF5 1.10.2. */
#ifdef USE_PARALLEL
#ifndef HDF5_SUPPORTS_PAR_FILTERS
    if (h5->parallel == NC_TRUE)
        {stat = THROW(NC_EINVAL); goto done;}
#endif /* HDF5_SUPPORTS_PAR_FILTERS */
#endif /* USE_PARALLEL */

	/* See if this filter is missing or not */
	if((avail = H5Zfilter_avail(id)) < 0)
 	    {stat = NC_EHDFERR; goto done;} /* Something in HDF5 went wrong */
	if(avail == 0)
	    {stat = NC_ENOFILTER; goto done;} /* filter not available */

	/* Lookup incoming id to see if already defined for this variable*/
        switch((stat=NC4_hdf5_filter_lookup(var,id,&oldspec))) {
	case NC_NOERR: break; /* already defined */
        case NC_ENOFILTER: break; /*not defined*/
        default: goto done;
	}
	stat = NC_NOERR; /* reset */

	/* See if deflate &/or szip is defined */
	switch ((stat = NC4_hdf5_filter_lookup(var,H5Z_FILTER_DEFLATE,NULL))) {
	case NC_NOERR: havedeflate = 1; break;
	case NC_ENOFILTER: havedeflate = 0; break;	
	default: goto done;
	}
	switch ((stat = NC4_hdf5_filter_lookup(var,H5Z_FILTER_SZIP,NULL))) {
	case NC_NOERR: haveszip = 1; break;
	case NC_ENOFILTER: haveszip = 0; break;	
	default: goto done;
	}
	stat = NC_NOERR; /* reset */

	if(!avail) {
            NC_HDF5_VAR_INFO_T* hdf5_var = (NC_HDF5_VAR_INFO_T *)var->format_var_info;
	    flags |= NC_HDF5_FILTER_MISSING;
	    /* mark variable as unreadable */
	    hdf5_var->flags |= NC_HDF5_VAR_FILTER_MISSING;
	}

	/* If incoming filter not already defined, then check for conflicts */
	if(oldspec == NULL) {
            if(id == H5Z_FILTER_DEFLATE) {
		int level;
                if(nparams != 1)
                    {stat = THROW(NC_EFILTER); goto done;}/* incorrect no. of parameters */
   	        level = (int)params[0];
                if (level < NC_MIN_DEFLATE_LEVEL || level > NC_MAX_DEFLATE_LEVEL)
                    {stat = THROW(NC_EINVAL); goto done;}
                /* If szip compression is already applied, return error. */
	        if(haveszip) {stat = THROW(NC_EINVAL); goto done;}
            }
            if(id == H5Z_FILTER_SZIP) { /* Do error checking */
                if(nparams != 2)
                    {stat = THROW(NC_EFILTER); goto done;}/* incorrect no. of parameters */
                /* Pixels per block must be an even number, <= 32. */
                if (params[1] % 2 || params[1] > NC_MAX_PIXELS_PER_BLOCK)
                    {stat = THROW(NC_EINVAL); goto done;}
                /* If zlib compression is already applied, return error. */
	        if(havedeflate) {stat = THROW(NC_EINVAL); goto done;}
            }
            /* Filter => chunking */
	    var->storage = NC_CHUNKED;
            /* Determine default chunksizes for this variable unless already specified */
            if(var->chunksizes && !var->chunksizes[0]) {
	        /* Should this throw error? */
                if((stat = nc4_find_default_chunksizes2(grp, var)))
	            goto done;
                /* Adjust the cache. */
                if ((stat = nc4_adjust_var_cache(grp, var)))
                    goto done;
            }
	}
	/* More error checking */
        if(id == H5Z_FILTER_SZIP) { /* szip X chunking error checking */
	    /* For szip, the pixels_per_block parameter must not be greater
	     * than the number of elements in a chunk of data. */
            size_t num_elem = 1;
            int d;
            for (d = 0; d < var->ndims; d++)
                if (var->dim[d]->len)
		    num_elem *= var->dim[d]->len;
            /* Pixels per block must be <= number of elements. */
            if (params[1] > num_elem)
                {stat = THROW(NC_EINVAL); goto done;}
        }
	/* addfilter can handle case where filter is already defined, and will just replace parameters */
        if((stat = NC4_hdf5_addfilter(var,id,nparams,params,flags)))
                goto done;
#ifdef USE_PARALLEL
#ifdef HDF5_SUPPORTS_PAR_FILTERS
        /* Switch to collective access. HDF5 requires collevtive access
         * for filter use with parallel I/O. */
        if (h5->parallel)
            var->parallel_access = NC_COLLECTIVE;
#else
        if (h5->parallel)
            {stat = THROW(NC_EINVAL); goto done;}
#endif /* HDF5_SUPPORTS_PAR_FILTERS */
#endif /* USE_PARALLEL */

done:
    return stat;
}

/**
 * @internal
 * Inquire about a variable's filters in a netCDF/HDF5 file.
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param nfiltersp A pointer that gets the number of filters for the variable.
 * @param ids A pointer that gets the ids of the filters.
 *
 * @return ::NC_NOERR on success, error code otherwise.
 * @author Dennis Heimbigner
 */
int
NC4_hdf5_inq_var_filter_ids(int ncid, int varid, size_t* nfiltersp, unsigned int* ids)
{
    int stat = NC_NOERR;
    NC *nc;
    NC_FILE_INFO_T* h5 = NULL;
    NC_GRP_INFO_T* grp = NULL;
    NC_VAR_INFO_T* var = NULL;
    NClist* flist = NULL;
    size_t nfilters;

    LOG((2, "%s: ncid 0x%x varid %d", __func__, ncid, varid));

    if((stat = NC_check_id(ncid,&nc))) return stat;
    assert(nc);

    /* Find info for this file and group and var, and set pointer to each. */
    if ((stat = nc4_hdf5_find_grp_h5_var(ncid, varid, &h5, &grp, &var)))
	{stat = THROW(stat); goto done;}

    assert(h5 && var && var->hdr.id == varid);

    flist = var->filters;

    nfilters = nclistlength(flist);
    if(nfilters > 0 && ids != NULL) {
	size_t k;
	for(k=0;k<nfilters;k++) {
	    struct NC_HDF5_Filter* f = (struct NC_HDF5_Filter*)nclistget(flist,k);
	    ids[k] = f->filterid;
	}
    }
    if(nfiltersp) *nfiltersp = nfilters;
 
done:
    return stat;

}

/**
 * @internal
 * Inquire about a filter for a variable in a netCDF/HDF5 file.
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param id The HDF5 filter ID.
 * @param nparamsp A pointer that gets the number of parameters for the filter.
 * @param params A pointer that gets nparamsp parameters for the filter.
 *
 * @return ::NC_NOERR on success, error code otherwise.
 * @author Dennis Heimbigner
 */
int
NC4_hdf5_inq_var_filter_info(int ncid, int varid, unsigned int id, size_t* nparamsp, unsigned int* params)
{
    int stat = NC_NOERR;
    NC *nc;
    NC_FILE_INFO_T* h5 = NULL;
    NC_GRP_INFO_T* grp = NULL;
    NC_VAR_INFO_T* var = NULL;
    struct NC_HDF5_Filter* spec = NULL;

    LOG((2, "%s: ncid 0x%x varid %d", __func__, ncid, varid));

    if((stat = NC_check_id(ncid,&nc))) return stat;
    assert(nc);

    /* Find info for this file and group and var, and set pointer to each. */
    if ((stat = nc4_hdf5_find_grp_h5_var(ncid, varid, &h5, &grp, &var)))
	{stat = THROW(stat); goto done;}

    assert(h5 && var && var->hdr.id == varid);

    if((stat = NC4_hdf5_filter_lookup(var,id,&spec))) goto done;
    if(nparamsp) *nparamsp = spec->nparams;
    if(params && spec->nparams > 0) {
	memcpy(params,spec->params,sizeof(unsigned int)*spec->nparams);
    }
 
done:
    return stat;

}

/* Return ID for the first missing filter; 0 if no missing filters */
int
NC4_hdf5_find_missing_filter(NC_VAR_INFO_T* var, unsigned int* idp)
{
    size_t i;
    int stat = NC_NOERR;
    NClist* flist = (NClist*)var->filters;
    int id = 0;
    
    for(i=0;i<nclistlength(flist);i++) {
	struct NC_HDF5_Filter* spec = (struct NC_HDF5_Filter*)nclistget(flist,i);
	if(spec->flags & NC_HDF5_FILTER_MISSING) {id = spec->filterid; break;}
    }
    if(idp) *idp = id;
    return stat;
}

int
NC4_hdf5_filter_initialize(void)
{
    return NC_NOERR;
}

int
NC4_hdf5_filter_finalize(void)
{
    return NC_NOERR;
}

/**
 * @internal
 * Test if a filter is available in HDF5.
 *
 * @param ncid File ID (ignored).
 * @param id The HDF5 filter ID.
 *
 * @return ::NC_NOERR on success, error code otherwise.
 * @author Dennis Heimbigner
 */
int
NC4_hdf5_inq_filter_avail(int ncid, unsigned id)
{
    int stat = NC_NOERR;
    htri_t avail = -1;
    NC_UNUSED(ncid);
    /* See if this filter is available or not */
    if((avail = H5Zfilter_avail(id)) < 0)
 	{stat = NC_EHDFERR; goto done;} /* Something in HDF5 went wrong */
    if(avail == 0) stat = NC_ENOFILTER;
done:
    return stat;
}
