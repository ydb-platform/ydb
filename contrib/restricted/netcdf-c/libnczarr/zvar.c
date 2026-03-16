/* Copyright 2003-2019, University Corporation for Atmospheric
 * Research. See COPYRIGHT file for copying and redistribution
 * conditions.*/

/**
 * @file
 * @internal This file handles the ZARR variable functions.
 *
 * @author Dennis Heimbigner, Ed Hartnett
 */

#include "zincludes.h"
#include <math.h> /* For pow() used below. */

/* Mnemonics */
#define CREATE 0
#define NOCREATE 1


#ifdef LOGGING
static void
reportchunking(const char* title, NC_VAR_INFO_T* var)
{
    int i;
    char buf[8192];

    buf[0] = '\0'; /* for strlcat */
    strlcat(buf,title,sizeof(buf));
    strlcat(buf,"chunksizes for var ",sizeof(buf));
    strlcat(buf,var->hdr.name,sizeof(buf));
    strlcat(buf,"sizes=",sizeof(buf));
    for(i=0;i<var->ndims;i++) {
	char digits[64];
	if(i > 0) strlcat(buf,",",sizeof(buf));
	snprintf(digits,sizeof(digits),"%ld",(unsigned long)var->chunksizes[i]);
	strlcat(buf,digits,sizeof(buf));
    }
    LOG((3,"%s",buf));
}    
#endif

/* Mnemonic */
#define READING 1
#define WRITING 0

/** @internal Default size for unlimited dim chunksize. */
#define DEFAULT_1D_UNLIM_SIZE (4096)

/** Number of bytes in 64 KB. */
#define SIXTY_FOUR_KB (65536)

/** @internal Temp name used when renaming vars to preserve varid
 * order. */
#define NC_TEMP_NAME "_netcdf4_temporary_variable_name_for_rename"

/**
 * @internal Check a set of chunksizes to see if they specify a chunk
 * that is too big.
 *
 * @param grp Pointer to the group info.
 * @param var Pointer to the var info.
 * @param chunksizes Array of chunksizes to check.
 *
 * @returns ::NC_NOERR No error.
 * @returns ::NC_EBADID Bad ncid.
 * @returns ::NC_ENOTVAR Invalid variable ID.
 * @returns ::NC_EBADCHUNK Bad chunksize.
 */
static int
check_chunksizes(NC_GRP_INFO_T *grp, NC_VAR_INFO_T *var, const size_t *chunksizes)
{
    double dprod;
    size_t type_len;
    int d;
    int retval = NC_NOERR;

    if ((retval = nc4_get_typelen_mem(grp->nc4_info, var->type_info->hdr.id, &type_len)))
	goto done;
    if (var->type_info->nc_type_class == NC_VLEN)
	dprod = (double)sizeof(nc_hvl_t);
    else
	dprod = (double)type_len;
    for (d = 0; d < var->ndims; d++)
	dprod *= (double)chunksizes[d];

    if (dprod > (double) NC_MAX_UINT)
	{retval = NC_EBADCHUNK; goto done;}
done:
    return retval;
}

/**
 * @internal Determine some default chunksizes for a variable.
 *
 * @param grp Pointer to the group info.
 * @param var Pointer to the var info.
 *
 * @returns ::NC_NOERR for success
 * @returns ::NC_EBADID Bad ncid.
 * @returns ::NC_ENOTVAR Invalid variable ID.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
ncz_find_default_chunksizes2(NC_GRP_INFO_T *grp, NC_VAR_INFO_T *var)
{
    int d;
    size_t type_size;
    float num_values = 1, num_unlim = 0;
    int retval;
    size_t suggested_size;
#ifdef LOGGING
    double total_chunk_size;
#endif

    type_size = var->type_info->size;

#ifdef LOGGING
    /* Later this will become the total number of bytes in the default
     * chunk. */
    total_chunk_size = (double) type_size;
#endif

    if(var->chunksizes == NULL) {
	if((var->chunksizes = calloc(1,sizeof(size_t)*var->ndims)) == NULL)
	    return NC_ENOMEM;
    }

    /* How many values in the variable (or one record, if there are
     * unlimited dimensions). */
    for (d = 0; d < var->ndims; d++)
    {
	assert(var->dim[d]);
	if (! var->dim[d]->unlimited)
	    num_values *= (float)var->dim[d]->len;
	else {
	    num_unlim++;
	    var->chunksizes[d] = 1; /* overwritten below, if all dims are unlimited */
	}
    }
    /* Special case to avoid 1D vars with unlim dim taking huge amount
       of space (DEFAULT_CHUNK_SIZE bytes). Instead we limit to about
       4KB */
    if (var->ndims == 1 && num_unlim == 1) {
	if (DEFAULT_CHUNK_SIZE / type_size <= 0)
	    suggested_size = 1;
	else if (DEFAULT_CHUNK_SIZE / type_size > DEFAULT_1D_UNLIM_SIZE)
	    suggested_size = DEFAULT_1D_UNLIM_SIZE;
	else
	    suggested_size = DEFAULT_CHUNK_SIZE / type_size;
	var->chunksizes[0] = suggested_size / type_size;
	LOG((4, "%s: name %s dim %d DEFAULT_CHUNK_SIZE %d num_values %f type_size %d "
	     "chunksize %ld", __func__, var->hdr.name, d, DEFAULT_CHUNK_SIZE, num_values, type_size, var->chunksizes[0]));
    }
    if (var->ndims > 1 && var->ndims == num_unlim) { /* all dims unlimited */
	suggested_size = pow((double)DEFAULT_CHUNK_SIZE/type_size, 1.0/(double)(var->ndims));
	for (d = 0; d < var->ndims; d++)
	{
	    var->chunksizes[d] = suggested_size ? suggested_size : 1;
	    LOG((4, "%s: name %s dim %d DEFAULT_CHUNK_SIZE %d num_values %f type_size %d "
		 "chunksize %ld", __func__, var->hdr.name, d, DEFAULT_CHUNK_SIZE, num_values, type_size, var->chunksizes[d]));
	}
    }

    /* Pick a chunk length for each dimension, if one has not already
     * been picked above. */
    for (d = 0; d < var->ndims; d++)
	if (!var->chunksizes[d])
	{
	    suggested_size = (pow((double)DEFAULT_CHUNK_SIZE/(num_values * type_size),
				  1.0/(double)(var->ndims - num_unlim)) * var->dim[d]->len - .5);
	    if (suggested_size > var->dim[d]->len)
		suggested_size = var->dim[d]->len;
	    var->chunksizes[d] = suggested_size ? suggested_size : 1;
	    LOG((4, "%s: name %s dim %d DEFAULT_CHUNK_SIZE %d num_values %f type_size %d "
		 "chunksize %ld", __func__, var->hdr.name, d, DEFAULT_CHUNK_SIZE, num_values, type_size, var->chunksizes[d]));
	}

#ifdef LOGGING
    /* Find total chunk size. */
    for (d = 0; d < var->ndims; d++)
	total_chunk_size *= (double) var->chunksizes[d];
    LOG((4, "total_chunk_size %f", total_chunk_size));
#endif

    /* But did this result in a chunk that is too big? */
    retval = check_chunksizes(grp, var, var->chunksizes);
    if (retval)
    {
	/* Other error? */
	if (retval != NC_EBADCHUNK)
	    return THROW(retval);

	/* Chunk is too big! Reduce each dimension by half and try again. */
	for ( ; retval == NC_EBADCHUNK; retval = check_chunksizes(grp, var, var->chunksizes))
	    for (d = 0; d < var->ndims; d++)
		var->chunksizes[d] = var->chunksizes[d]/2 ? var->chunksizes[d]/2 : 1;
    }

    /* Do we have any big data overhangs? They can be dangerous to
     * babies, the elderly, or confused campers who have had too much
     * beer. */
    for (d = 0; d < var->ndims; d++)
    {
	size_t num_chunks;
	size_t overhang;
	assert(var->chunksizes[d] > 0);
	num_chunks = (var->dim[d]->len + var->chunksizes[d] - 1) / var->chunksizes[d];
	if(num_chunks > 0) {
	    overhang = (num_chunks * var->chunksizes[d]) - var->dim[d]->len;
	    var->chunksizes[d] -= overhang / num_chunks;
	}
    }

#ifdef LOGGING
reportchunking("find_default: ",var);
#endif
    return NC_NOERR;
}

#if 0
/**
 * @internal Give a var a secret ZARR name. This is needed when a var
 * is defined with the same name as a dim, but it is not a coord var
 * of that dim. In that case, the var uses a secret name inside the
 * ZARR file.
 *
 * @param var Pointer to var info.
 * @param name Name to use for base of secret name.
 *
 * @returns ::NC_NOERR No error.
 * @returns ::NC_EMAXNAME Name too long to fit secret prefix.
 * @returns ::NC_ENOMEM Out of memory.
 * @author Dennis Heimbigner, Ed Hartnett
 */
static int
give_var_secret_name(NC_VAR_INFO_T *var, const char *name)
{
    /* Set a different ncz name for this variable to avoid name
     * clash. */
    if (strlen(name) + strlen(NON_COORD_PREPEND) > NC_MAX_NAME)
	return NC_EMAXNAME;
	size_t ncz_name_size = (strlen(NON_COORD_PREPEND) + strlen(name) + 1) *
	                       sizeof(char);
    if (!(var->ncz_name = malloc(ncz_name_size)))
	return NC_ENOMEM;

    snprintf(var->ncz_name, ncz_name_size, "%s%s", NON_COORD_PREPEND, name);

    return NC_NOERR;
}
#endif /*0*/

/**
 * @internal This is called when a new netCDF-4 variable is defined
 * with nc_def_var().
 *
 * @param ncid File ID.
 * @param name Name.
 * @param xtype Type.
 * @param ndims Number of dims. ZARR has maximum of 32.
 * @param dimidsp Array of dim IDs.
 * @param varidp Gets the var ID.
 *
 * @returns ::NC_NOERR No error.
 * @returns ::NC_EBADID Bad ncid.
 * @returns ::NC_ENOTVAR Invalid variable ID.
 * @returns ::NC_ENOTNC4 Attempting netcdf-4 operation on file that is
 * not netCDF-4/NCZ.
 * @returns ::NC_ESTRICTNC3 Attempting netcdf-4 operation on strict nc3
 * netcdf-4 file.
 * @returns ::NC_ELATEDEF Too late to change settings for this variable.
 * @returns ::NC_ENOTINDEFINE Not in define mode.
 * @returns ::NC_EPERM File is read only.
 * @returns ::NC_EMAXDIMS Classic model file exceeds ::NC_MAX_VAR_DIMS.
 * @returns ::NC_ESTRICTNC3 Attempting to create netCDF-4 type var in
 * classic model file
 * @returns ::NC_EBADNAME Bad name.
 * @returns ::NC_EBADTYPE Bad type.
 * @returns ::NC_ENOMEM Out of memory.
 * @returns ::NC_EHDFERR Error returned by ZARR layer.
 * @returns ::NC_EINVAL Invalid input
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_def_var(int ncid, const char *name, nc_type xtype, int ndims,
	    const int *dimidsp, int *varidp)
{
    NC_GRP_INFO_T *grp;
    NC_VAR_INFO_T *var;
    NC_DIM_INFO_T *dim;
    NC_FILE_INFO_T *h5;
    NC_TYPE_INFO_T *type = NULL;
    NCZ_VAR_INFO_T* zvar = NULL;
    char norm_name[NC_MAX_NAME + 1];
    int d;
    int retval;
    NCglobalstate* gstate = NC_getglobalstate();

    ZTRACE(1,"ncid=%d name=%s xtype=%d ndims=%d dimids=%s",ncid,name,xtype,ndims,nczprint_idvector(ndims,dimidsp));
    
    /* Find info for this file and group, and set pointer to each. */
    if ((retval = nc4_find_grp_h5(ncid, &grp, &h5)))
	BAIL(retval);
    assert(grp && grp->format_grp_info && h5);

#ifdef LOOK
    /* HDF5 allows maximum of 32 dimensions. */
    if (ndims > H5S_MAX_RANK)
	BAIL(NC_EMAXDIMS);
#endif

    /* If it's not in define mode, strict nc3 files error out,
     * otherwise switch to define mode. This will also check that the
     * file is writable. */
    if (!(h5->flags & NC_INDEF))
    {
	if (h5->cmode & NC_CLASSIC_MODEL)
	    BAIL(NC_ENOTINDEFINE);
	if ((retval = NCZ_redef(ncid)))
	    BAIL(retval);
    }
    assert(!h5->no_write);

    /* Check and normalize the name. */
    if ((retval = nc4_check_name(name, norm_name)))
	BAIL(retval);

    /* Not a Type is, well, not a type.*/
    if (xtype == NC_NAT)
	BAIL(NC_EBADTYPE);

    /* For classic files, only classic types are allowed. */
    if (h5->cmode & NC_CLASSIC_MODEL && xtype > NC_DOUBLE)
	BAIL(NC_ESTRICTNC3);

    /* For classic files limit number of dims. */
    if (h5->cmode & NC_CLASSIC_MODEL && ndims > NC_MAX_VAR_DIMS)
	BAIL(NC_EMAXDIMS);

    /* cast needed for braindead systems with signed size_t */
    if ((unsigned long) ndims > X_INT_MAX) /* Backward compat */
	BAIL(NC_EINVAL);

    /* Check that this name is not in use as a var, grp, or type. */
    if ((retval = nc4_check_dup_name(grp, norm_name)))
	BAIL(retval);

    /* For non-scalar vars, dim IDs must be provided. */
    if (ndims && !dimidsp)
	BAIL(NC_EINVAL);

    /* Check all the dimids to make sure they exist. */
    for (d = 0; d < ndims; d++)
	if ((retval = nc4_find_dim(grp, dimidsp[d], &dim, NULL)))
	    BAIL(retval);

    /* These degrubbing messages sure are handy! */
    LOG((2, "%s: name %s type %d ndims %d", __func__, norm_name, xtype, ndims));
#ifdef LOGGING
    {
	int dd;
	for (dd = 0; dd < ndims; dd++)
	    LOG((4, "dimid[%d] %d", dd, dimidsp[dd]));
    }
#endif

    /* If this is a user-defined type, there is a type struct with
     * all the type information. For atomic types, fake up a type
     * struct. */
    if((retval = ncz_gettype(h5,grp,xtype,&type)))
	BAIL(retval);

    /* Create a new var and fill in some cache setting values. */
    if ((retval = nc4_var_list_add(grp, norm_name, ndims, &var)))
	BAIL(retval);

    /* Add storage for NCZ-specific var info. */
    if (!(var->format_var_info = calloc(1, sizeof(NCZ_VAR_INFO_T))))
	BAIL(NC_ENOMEM);
    zvar = var->format_var_info;
    zvar->common.file = h5;
    zvar->scalar = (ndims == 0 ? 1 : 0);

    zvar->dimension_separator = gstate->zarr.dimension_separator;
    assert(zvar->dimension_separator != 0);

    /* Set these state flags for the var. */
    var->is_new_var = NC_TRUE;
    var->meta_read = NC_TRUE;
    var->atts_read = NC_TRUE;

#ifdef NETCDF_ENABLE_NCZARR_FILTERS
    /* Set the filter list */
    assert(var->filters == NULL);
    var->filters = (void*)nclistnew();
#endif

    /* Point to the type, and increment its ref. count */
    var->type_info = type;
#ifdef LOOK
var->type_info->rc++;
#endif
    type = NULL;

    /* Propagate the endianness */
    var->endianness = var->type_info->endianness;

    /* Set variables no_fill to match the database default unless the
     * variable type is variable length (NC_STRING or NC_VLEN) or is
     * user-defined type. */
    if (var->type_info->nc_type_class <= NC_STRING)
	var->no_fill = (h5->fill_mode == NC_NOFILL);

    /* Assign dimensions to the variable. At the same time, check to
     * see if this is a coordinate variable. If so, it will have the
     * same name as one of its dimensions. If it is a coordinate var,
     * is it a coordinate var in the same group as the dim? Also, check
     * whether we should use contiguous or chunked storage. */
    var->storage = NC_CHUNKED;
    for (d = 0; d < ndims; d++)
    {
	NC_GRP_INFO_T *dim_grp;
	/* Look up each dimension */
	if ((retval = nc4_find_dim(grp, dimidsp[d], &dim, &dim_grp)))
	    BAIL(retval);
	assert(dim && dim->format_dim_info);
	/* Check for unlimited dimension and turn off contiguous storage. */
	if (dim->unlimited)
	    var->storage = NC_CHUNKED;
	/* Track dimensions for variable */
	var->dimids[d] = dimidsp[d];
	var->dim[d] = dim;
    }

    /* Determine default chunksizes for this variable. (Even for
     * variables which may be contiguous.) */
    LOG((4, "allocating array of %d size_t to hold chunksizes for var %s",
	 var->ndims, var->hdr.name));
    if(!var->chunksizes) {
	if(var->ndims) {
            if (!(var->chunksizes = calloc(var->ndims, sizeof(size_t))))
	        BAIL(NC_ENOMEM);
	    if ((retval = ncz_find_default_chunksizes2(grp, var)))
	        BAIL(retval);
        } else {
	    /* Pretend that scalars are like var[1] */
	    if (!(var->chunksizes = calloc(1, sizeof(size_t))))
	        BAIL(NC_ENOMEM);
	    var->chunksizes[0] = 1;
	}
    }
    
    /* Compute the chunksize cross product */
    zvar->chunkproduct = 1;
    if(!zvar->scalar)
        {for(d=0;d<var->ndims;d++) {zvar->chunkproduct *= var->chunksizes[d];}}
    zvar->chunksize = zvar->chunkproduct * var->type_info->size;

    /* Set cache defaults */
    var->chunkcache = gstate->chunkcache;

    /* Create the cache */
    if((retval=NCZ_create_chunk_cache(var,zvar->chunkproduct*var->type_info->size,zvar->dimension_separator,&zvar->cache)))
	BAIL(retval);

    /* Set the per-variable chunkcache defaults */
    zvar->cache->params = var->chunkcache;

    /* Return the varid. */
    if (varidp)
	*varidp = var->hdr.id;
    LOG((4, "new varid %d", var->hdr.id));

exit:
    if (type)
	if ((retval = nc4_type_free(type)))
	    BAILLOG(retval);
    return ZUNTRACE(retval);
}

/**
 * @internal This functions sets extra stuff about a netCDF-4 variable which
 * must be set before the enddef but after the def_var.
 *
 * @note All pointer parameters may be NULL, in which case they are ignored.
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param shuffle Pointer to shuffle setting.
 * @param deflate Pointer to deflate setting.
 * @param deflate_level Pointer to deflate level.
 * @param fletcher32 Pointer to fletcher32 setting.
 * @param contiguous Pointer to contiguous setting.
 * @param chunksizes Array of chunksizes.
 * @param no_fill Pointer to no_fill setting.
 * @param fill_value Pointer to fill value.
 * @param endianness Pointer to endianness setting.
 *
 * @returns ::NC_NOERR for success
 * @returns ::NC_EBADID Bad ncid.
 * @returns ::NC_ENOTVAR Invalid variable ID.
 * @returns ::NC_ENOTNC4 Attempting netcdf-4 operation on file that is
 * not netCDF-4/NCZ.
 * @returns ::NC_ESTRICTNC3 Attempting netcdf-4 operation on strict nc3
 * netcdf-4 file.
 * @returns ::NC_ELATEDEF Too late to change settings for this variable.
 * @returns ::NC_ENOTINDEFINE Not in define mode.
 * @returns ::NC_EPERM File is read only.
 * @returns ::NC_EINVAL Invalid input
 * @returns ::NC_EBADCHUNK Bad chunksize.
 * @author Dennis Heimbigner, Ed Hartnett
 */
static int
ncz_def_var_extra(int ncid, int varid, int *shuffle, int *unused1,
		 int *unused2, int *fletcher32, int *storagep,
		 const size_t *chunksizes, int *no_fill,
		 const void *fill_value, int *endianness,
		 int *quantize_mode, int *nsd)
{
    NC_GRP_INFO_T *grp;
    NC_FILE_INFO_T *h5;
    NC_VAR_INFO_T *var;
    NCZ_VAR_INFO_T *zvar;
    int d;
    int retval = NC_NOERR;
    int storage = NC_CHUNKED;
    size_t contigchunksizes[NC_MAX_VAR_DIMS]; /* Fake chunksizes if storage is contiguous or compact */

    LOG((2, "%s: ncid 0x%x varid %d", __func__, ncid, varid));

    ZTRACE(2,"ncid=%d varid=%d shuffle=%d fletcher32=%d no_fill=%d, fill_value=%p endianness=%d quantize_mode=%d nsd=%d",
           ncid,varid,
	   (shuffle?*shuffle:-1),
   	   (fletcher32?*fletcher32:-1),
	   (no_fill?*no_fill:-1),
	   fill_value,
	   (endianness?*endianness:-1),
	   (quantize_mode?*quantize_mode:-1),
   	   (nsd?*nsd:-1)
	   );

    /* Find info for this file and group, and set pointer to each. */
    if ((retval = nc4_find_nc_grp_h5(ncid, NULL, &grp, &h5)))
	goto done;
    assert(grp && h5);

    /* Trying to write to a read-only file? No way, Jose! */
    if (h5->no_write)
	{retval = NC_EPERM; goto done;}

    /* Find the var. */
    if (!(var = (NC_VAR_INFO_T *)ncindexith(grp->vars, varid)))
	{retval = NC_ENOTVAR; goto done;}
    assert(var && var->hdr.id == varid);

    zvar = var->format_var_info;

    ZTRACEMORE(1,"\tstoragep=%d chunksizes=%s",(storagep?*storagep:-1),(chunksizes?nczprint_sizevector(var->ndims,chunksizes):"null"));
    
    /* Can't turn on parallel and deflate/fletcher32/szip/shuffle
     * before HDF5 1.10.3. */
#ifdef NETCDF_ENABLE_NCZARR_FILTERS
#ifndef HDF5_SUPPORTS_PAR_FILTERS
    if (h5->parallel == NC_TRUE)
	if (nclistlength(((NClist*)var->filters)) > 0  || fletcher32 || shuffle)
	    {retval = NC_EINVAL; goto done;}
#endif
#endif

    /* If the HDF5 dataset has already been created, then it is too
     * late to set all the extra stuff. */
    if (var->created)
        {retval = NC_ELATEDEF; goto done;}

#if 0
    /* Check compression options. */
    if (deflate && !deflate_level)
	{retval = NC_EINVAL; goto done;}

    /* Valid deflate level? */
    if (deflate)
    {
	if (*deflate)
	    if (*deflate_level < NC_MIN_DEFLATE_LEVEL ||
		*deflate_level > NC_MAX_DEFLATE_LEVEL)
		{retval = NC_EINVAL; goto done;}

	/* For scalars, just ignore attempt to deflate. */
	if (!var->ndims)
	    goto done;

	/* If szip is in use, return an error. */
	if ((retval = nc_inq_var_szip(ncid, varid, &option_mask, NULL)))
	    goto done;
	if (option_mask)
	    {retval = NC_EINVAL; goto done;}

	/* Set the deflate settings. */
	var->storage = NC_CONTIGUOUS;
	var->deflate = *deflate;
	if (*deflate)
	    var->deflate_level = *deflate_level;
	LOG((3, "%s: *deflate_level %d", __func__, *deflate_level));
    }
#endif

    /* Shuffle filter? */
    if (shuffle && *shuffle) {
	    retval = nc_inq_var_filter_info(ncid,varid,H5Z_FILTER_SHUFFLE,NULL,NULL);
	    if(!retval || retval == NC_ENOFILTER) {
	        if((retval = NCZ_def_var_filter(ncid,varid,H5Z_FILTER_SHUFFLE,0,NULL))) return retval;
                var->storage = NC_CHUNKED;
	    }
    }

    /* Fletcher32 checksum error protection? */
    if (fletcher32 && fletcher32) {
	retval = nc_inq_var_filter_info(ncid,varid,H5Z_FILTER_FLETCHER32,NULL,NULL);
	if(!retval || retval == NC_ENOFILTER) {
	    if((retval = NCZ_def_var_filter(ncid,varid,H5Z_FILTER_FLETCHER32,0,NULL))) return retval;
            var->storage = NC_CHUNKED;
	    }
    }

    /* Handle storage settings. */
    if (storagep)
    {
	storage = *storagep;
	/* Does the user want a contiguous or compact dataset? Not so
	 * fast! Make sure that there are no unlimited dimensions, and
	 * no filters in use for this data. */
	if (storage != NC_CHUNKED)
	{
#ifdef NCZARR_FILTERS
	    if (nclistlength(((NClist*)var->filters)) > 0)
		{retval = NC_EINVAL; goto done;}
#endif
	    for (d = 0; d < var->ndims; d++) {
		if (var->dim[d]->unlimited)
		    {retval = NC_EINVAL; goto done;}
	        contigchunksizes[d] = var->dim[d]->len; /* Fake a single big chunk */
	    }
	    chunksizes = (const size_t*)contigchunksizes;
	    storage = NC_CHUNKED; /*only chunked supported */
	}

	if (storage == NC_CHUNKED && var->ndims == 0) {
	    {retval = NC_EINVAL; goto done;}
	} else if (storage == NC_CHUNKED && var->ndims > 0) {
	    var->storage = NC_CHUNKED;
	    
	    /* If the user provided chunksizes, check that they are valid
	     * and that their total size of chunk is less than 4 GB. */
	    if (chunksizes)
	    {
		/* Check the chunksizes for validity. */
		if ((retval = check_chunksizes(grp, var, chunksizes)))
		    goto done;

		/* Ensure chunksize is smaller than dimension size */
		for (d = 0; d < var->ndims; d++)
		    if (!var->dim[d]->unlimited && var->dim[d]->len > 0 &&
			chunksizes[d] > var->dim[d]->len)
			{retval = NC_EBADCHUNK; goto done;}
	    }
	}

	/* Is this a variable with a chunksize greater than the current
	 * cache size? */
	if (var->storage == NC_CHUNKED)
	{
	    int anyzero = 0; /* check for any zero length chunksizes */
	    zvar = var->format_var_info;
	    assert(zvar->cache != NULL);
	    zvar->cache->valid = 0;
	    if(chunksizes) {
		for (d = 0; d < var->ndims; d++) {
		    var->chunksizes[d] = chunksizes[d];
		    if(chunksizes[d] == 0) anyzero = 1;
		}
	    }
	    /* If chunksizes == NULL or anyzero then use defaults */
	    if(chunksizes == NULL || anyzero) { /* Use default chunking */
		if ((retval = ncz_find_default_chunksizes2(grp, var)))
		    goto done;
	    }
	    assert(var->chunksizes != NULL);
	    /* Set the chunksize product for this variable. */
	    zvar->chunkproduct = 1;
	    for (d = 0; d < var->ndims; d++)
		zvar->chunkproduct *= var->chunksizes[d];
            zvar->chunksize = zvar->chunkproduct * var->type_info->size;
	}
	/* Adjust cache */
        if((retval = NCZ_adjust_var_cache(var))) goto done;
    
#ifdef LOGGING
	{
	    int dfalt = (chunksizes == NULL);
	    reportchunking(dfalt ? "extra: default: " : "extra: user: ", var);
	}
#endif
    }
    
    /* Are we setting a fill modes? */
    if (no_fill)
    {
	if (*no_fill)
	{
	    /* NC_STRING types may not turn off fill mode. It's disallowed
	     * by HDF5 and will cause a HDF5 error later. */
	    if (*no_fill)
		if (var->type_info->hdr.id == NC_STRING)
		    {retval = NC_EINVAL; goto done;}

	    /* Set the no-fill mode. */
	    var->no_fill = NC_TRUE;
	}
	else
	    var->no_fill = NC_FALSE;
    }

    /* Are we setting a fill value? */
    if (fill_value && no_fill && !(*no_fill))
    {
	/* Copy the fill_value. */
	LOG((4, "Copying fill value into metadata for variable %s",
	     var->hdr.name));

	/* If there's a _FillValue attribute, delete it. */
	retval = NCZ_del_att(ncid, varid, NC_FillValue);
	if (retval && retval != NC_ENOTATT)
	    goto done;

        /* Create a _FillValue attribute; will also fill in var->fill_value */
	if ((retval = nc_put_att(ncid, varid, NC_FillValue, var->type_info->hdr.id,
				 1, fill_value)))
	    goto done;
        /* Reclaim any existing fill_chunk */
        if((retval = NCZ_reclaim_fill_chunk(zvar->cache))) goto done;
    } else if (var->fill_value && no_fill && (*no_fill)) { /* Turning off fill value? */
        /* If there's a _FillValue attribute, delete it. */
        retval = NCZ_del_att(ncid, varid, NC_FillValue);
        if (retval && retval != NC_ENOTATT) return retval;
	if((retval = NCZ_reclaim_fill_value(var))) return retval;
    }

    /* Is the user setting the endianness? */
    if (endianness)
    {
	/* Setting endianness is only premitted on atomic integer and
	 * atomic float types. */
	switch (var->type_info->hdr.id)
	{
	case NC_BYTE:
	case NC_SHORT:
	case NC_INT:
	case NC_FLOAT:
	case NC_DOUBLE:
	case NC_UBYTE:
	case NC_USHORT:
	case NC_UINT:
	case NC_INT64:
	case NC_UINT64:
	    break;
	default:
	    {retval = NC_EINVAL; goto done;}
	}
	var->type_info->endianness = *endianness;
	/* Propagate */
	var->endianness = *endianness;
    }

    /* Remember quantization settings. They will be used when data are
     * written.
     * Code block is identical to one in hdf5var.c---consider functionalizing */
    if (quantize_mode)
    {
	/* Only four valid mode settings. */
	if (*quantize_mode != NC_NOQUANTIZE &&
	    *quantize_mode != NC_QUANTIZE_BITGROOM &&
	    *quantize_mode != NC_QUANTIZE_GRANULARBR &&
	    *quantize_mode != NC_QUANTIZE_BITROUND)
	    return NC_EINVAL;

	if (*quantize_mode == NC_QUANTIZE_BITGROOM ||
	    *quantize_mode == NC_QUANTIZE_GRANULARBR ||
	    *quantize_mode == NC_QUANTIZE_BITROUND)
	  {

	    /* Only float and double types can have quantization. */
	    if (var->type_info->hdr.id != NC_FLOAT &&
		var->type_info->hdr.id != NC_DOUBLE)
		return NC_EINVAL;
	    
	    /* All quantization codecs require number of significant digits */
	    if (!nsd)
		return NC_EINVAL;

	    /* NSD must be in range. */
	    if (*nsd <= 0)
		return NC_EINVAL;

	    if (*quantize_mode == NC_QUANTIZE_BITGROOM ||
		*quantize_mode == NC_QUANTIZE_GRANULARBR)
	      {
		if (var->type_info->hdr.id == NC_FLOAT &&
		    *nsd > NC_QUANTIZE_MAX_FLOAT_NSD)
		  return NC_EINVAL;
		if (var->type_info->hdr.id == NC_DOUBLE &&
		    *nsd > NC_QUANTIZE_MAX_DOUBLE_NSD)
		  return NC_EINVAL;
	      }
	    else if (*quantize_mode == NC_QUANTIZE_BITROUND)
	      {
		if (var->type_info->hdr.id == NC_FLOAT &&
		    *nsd > NC_QUANTIZE_MAX_FLOAT_NSB)
		  return NC_EINVAL;
		if (var->type_info->hdr.id == NC_DOUBLE &&
		    *nsd > NC_QUANTIZE_MAX_DOUBLE_NSB)
		  return NC_EINVAL;
	      }

	    var->nsd = *nsd;
	}
	
	var->quantize_mode = *quantize_mode;

	/* If quantization is turned off, then set nsd to 0. */
	if (*quantize_mode == NC_NOQUANTIZE)
	    var->nsd = 0;
    }

done:
    return ZUNTRACE(retval);
}

/**
 * @internal Set compression settings on a variable. This is called by
 * nc_def_var_deflate().
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param shuffle True to turn on the shuffle filter.
 * @param deflate True to turn on deflation.
 * @param deflate_level A number between 0 (no compression) and 9
 * (maximum compression).
 *
 * @returns ::NC_NOERR No error.
 * @returns ::NC_EBADID Bad ncid.
 * @returns ::NC_ENOTVAR Invalid variable ID.
 * @returns ::NC_ENOTNC4 Attempting netcdf-4 operation on file that is
 * not netCDF-4/NCZ.
 * @returns ::NC_ELATEDEF Too late to change settings for this variable.
 * @returns ::NC_ENOTINDEFINE Not in define mode.
 * @returns ::NC_EINVAL Invalid input
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_def_var_deflate(int ncid, int varid, int shuffle, int deflate,
		    int deflate_level)
{
    int stat = NC_NOERR;
    unsigned int level = (unsigned int)deflate_level;
    /* Set shuffle first */
    if((stat = ncz_def_var_extra(ncid, varid, &shuffle, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL))) goto done;
    if(deflate)
	stat = nc_def_var_filter(ncid, varid, H5Z_FILTER_DEFLATE,1,&level);
    if(stat) goto done;
done:
    return stat;
}

/**
 * @internal Set checksum on a variable. This is called by
 * nc_def_var_fletcher32().
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param fletcher32 Pointer to fletcher32 setting.
 *
 * @returns ::NC_NOERR No error.
 * @returns ::NC_EBADID Bad ncid.
 * @returns ::NC_ENOTVAR Invalid variable ID.
 * @returns ::NC_ENOTNC4 Attempting netcdf-4 operation on file that is
 * not netCDF-4/NCZ.
 * @returns ::NC_ELATEDEF Too late to change settings for this variable.
 * @returns ::NC_ENOTINDEFINE Not in define mode.
 * @returns ::NC_EINVAL Invalid input
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_def_var_fletcher32(int ncid, int varid, int fletcher32)
{
    return ncz_def_var_extra(ncid, varid, NULL, NULL, NULL, &fletcher32,
			    NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}

/**
 * @internal Define chunking stuff for a var. This is called by
 * nc_def_var_chunking(). Chunking is required in any dataset with one
 * or more unlimited dimensions in NCZ, or any dataset using a
 * filter.
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param contiguous Pointer to contiguous setting.
 * @param chunksizesp Array of chunksizes.
 *
 * @returns ::NC_NOERR No error.
 * @returns ::NC_EBADID Bad ncid.
 * @returns ::NC_ENOTVAR Invalid variable ID.
 * @returns ::NC_ENOTNC4 Attempting netcdf-4 operation on file that is
 * not netCDF-4/NCZ.
 * @returns ::NC_ELATEDEF Too late to change settings for this variable.
 * @returns ::NC_ENOTINDEFINE Not in define mode.
 * @returns ::NC_EINVAL Invalid input
 * @returns ::NC_EBADCHUNK Bad chunksize.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_def_var_chunking(int ncid, int varid, int contiguous, const size_t *chunksizesp)
{
    return ncz_def_var_extra(ncid, varid, NULL, NULL, NULL, NULL,
			    &contiguous, chunksizesp, NULL, NULL, NULL, NULL, NULL);
}

/**
 * @internal Define chunking stuff for a var. This is called by
 * the fortran API.
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param contiguous Pointer to contiguous setting.
 * @param chunksizesp Array of chunksizes.
 *
 * @returns ::NC_NOERR No error.
 * @returns ::NC_EBADID Bad ncid.
 * @returns ::NC_ENOTVAR Invalid variable ID.
 * @returns ::NC_ENOTNC4 Attempting netcdf-4 operation on file that is
 * not netCDF-4/NCZ.
 * @returns ::NC_ELATEDEF Too late to change settings for this variable.
 * @returns ::NC_ENOTINDEFINE Not in define mode.
 * @returns ::NC_EINVAL Invalid input
 * @returns ::NC_EBADCHUNK Bad chunksize.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
ncz_def_var_chunking_ints(int ncid, int varid, int contiguous, int *chunksizesp)
{
    NC_VAR_INFO_T *var;
    size_t *cs;
    int i, retval;

    /* Get pointer to the var. */
    if ((retval = nc4_find_grp_h5_var(ncid, varid, NULL, NULL, &var)))
	return THROW(retval);
    assert(var);

    /* Allocate space for the size_t copy of the chunksizes array. */
    if (var->ndims)
	if (!(cs = malloc(var->ndims * sizeof(size_t))))
	    return NC_ENOMEM;

    /* Copy to size_t array. */
    for (i = 0; i < var->ndims; i++)
	cs[i] = chunksizesp[i];

    retval = ncz_def_var_extra(ncid, varid, NULL, NULL, NULL, NULL,
			      &contiguous, cs, NULL, NULL, NULL, NULL, NULL);

    if (var->ndims)
	free(cs);
    return THROW(retval);
}

/**
 * @internal This functions sets fill value and no_fill mode for a
 * netCDF-4 variable. It is called by nc_def_var_fill().
 *
 * @note All pointer parameters may be NULL, in which case they are ignored.
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param no_fill No_fill setting.
 * @param fill_value Pointer to fill value.
 *
 * @returns ::NC_NOERR for success
 * @returns ::NC_EBADID Bad ncid.
 * @returns ::NC_ENOTVAR Invalid variable ID.
 * @returns ::NC_ENOTNC4 Attempting netcdf-4 operation on file that is
 * not netCDF-4/NCZ.
 * @returns ::NC_ESTRICTNC3 Attempting netcdf-4 operation on strict nc3
 * netcdf-4 file.
 * @returns ::NC_ELATEDEF Too late to change settings for this variable.
 * @returns ::NC_ENOTINDEFINE Not in define mode.
 * @returns ::NC_EPERM File is read only.
 * @returns ::NC_EINVAL Invalid input
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_def_var_fill(int ncid, int varid, int no_fill, const void *fill_value)
{
    return ncz_def_var_extra(ncid, varid, NULL, NULL, NULL, NULL, NULL,
			    NULL, &no_fill, fill_value, NULL, NULL, NULL);
}

/**
 * @internal This functions sets endianness for a netCDF-4
 * variable. Called by nc_def_var_endian().
 *
 * @note All pointer parameters may be NULL, in which case they are ignored.
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param endianness Endianness setting.
 *
 * @returns ::NC_NOERR for success
 * @returns ::NC_EBADID Bad ncid.
 * @returns ::NC_ENOTVAR Invalid variable ID.
 * @returns ::NC_ENOTNC4 Attempting netcdf-4 operation on file that is
 * not netCDF-4/NCZ.
 * @returns ::NC_ESTRICTNC3 Attempting netcdf-4 operation on strict nc3
 * netcdf-4 file.
 * @returns ::NC_ELATEDEF Too late to change settings for this variable.
 * @returns ::NC_ENOTINDEFINE Not in define mode.
 * @returns ::NC_EPERM File is read only.
 * @returns ::NC_EINVAL Invalid input
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_def_var_endian(int ncid, int varid, int endianness)
{
    return ncz_def_var_extra(ncid, varid, NULL, NULL, NULL, NULL, NULL,
			    NULL, NULL, NULL, &endianness, NULL, NULL);
}

/**
 * @internal Set quantization settings on a variable. This is
 * called by nc_def_var_quantize().
 *
 * Quantization allows the user to specify a number of significant
 * digits for variables of type ::NC_FLOAT or ::NC_DOUBLE. (Attempting
 * to set quantize for other types will result in an ::NC_EINVAL
 * error.)
 *
 * When quantize is turned on, and the number of significant digits
 * (NSD) has been specified, then the netCDF library will quantize according
 * to the selected algorithm. BitGroom interprets NSD as decimal digits
 * will apply all zeros or all ones (alternating) to bits which are not 
 * needed to specify the value to the number of significant decimal digits. 
 * BitGroom retain the same number of bits for all values of a variable. 
 * BitRound (BR) interprets NSD as binary digits (i.e., bits) and keeps the
 * the user-specified number of significant bits then rounds the result
 * to the nearest representable number according to IEEE rounding rules.
 * BG and BR both retain a uniform number of significant bits for all 
 * values of a variable. Granular BitRound interprest NSD as decimal
 * digits. GranularBR determines the number of bits to necessary to 
 * retain the user-specified number of significant digits individually
 * for every value of the variable. GranularBR then applies the BR
 * quantization algorithm on a granular, value-by-value, rather than
 * uniformly for the entire variable. GranularBR quantizes more bits
 * than BG, and is thus more compressive and less accurate than BG.
 * BR knows bits and makes no guarantees about decimal precision.
 * All quantization algorithms change the values of the data, and make 
 * it more compressible.
 *
 * Quantizing the data does not reduce the size of the data on disk,
 * but combining quantize with compression will allow for better
 * compression. Since the data values are changed, the use of quantize
 * and compression such as deflate constitute lossy compression.
 *
 * Producers of large datasets may find that using quantize with
 * compression will result in significant improvement in the final data
 * size.
 *
 * Variables which use quantize will have added an attribute with name
 * ::NC_QUANTIZE_BITGROOM_ATT_NAME, ::NC_QUANTIZE_GRANULARBR_ATT_NAME,
 * or ::NC_QUANTIZE_BITROUND_ATT_NAME that contains the number of
 * significant digits. Users should not delete or change this attribute.
 * This is the only record that quantize has been applied to the data.
 *
 * As with the deflate settings, quantize settings may only be
 * modified before the first call to nc_enddef(). Once nc_enddef() is
 * called for the file, quantize settings for any variable in the file
 * may not be changed.
 *
 * Use of quantization is fully backwards compatible with existing
 * versions and packages that can read compressed netCDF data. A
 * variable which has been quantized is readable to older versions of
 * the netCDF libraries, and to netCDF-Java.
 *
 * @param ncid File ID.
 * @param varid Variable ID. NC_GLOBAL may not be used.
 * @param quantize_mode Quantization mode. May be ::NC_NOQUANTIZE or
 * ::NC_QUANTIZE_BITGROOM, ::NC_QUANTIZE_BITROUND or ::NC_QUANTIZE_GRANULARBR.
 * @param nsd Number of significant digits (either decimal or binary). 
 * May be any integer from 1 to ::NC_QUANTIZE_MAX_FLOAT_NSD (for variables 
 * of type ::NC_FLOAT) or ::NC_QUANTIZE_MAX_DOUBLE_NSD (for variables 
 * of type ::NC_DOUBLE) for mode ::NC_QUANTIZE_BITGROOM and mode
 * ::NC_QUANTIZE_GRANULARBR. May be any integer from 1 to 
 * ::NC_QUANTIZE_MAX_FLOAT_NSB (for variables of type ::NC_FLOAT) or 
 * ::NC_QUANTIZE_MAX_DOUBLE_NSB (for variables of type ::NC_DOUBLE) 
 * for mode ::NC_QUANTIZE_BITROUND.
 *
 * @returns ::NC_NOERR No error.
 * @returns ::NC_EBADID Bad ncid.
 * @returns ::NC_ENOTVAR Invalid variable ID.
 * @returns ::NC_ENOTNC4 Attempting netcdf-4 operation on file that is
 * not netCDF-4/HDF5.
 * @returns ::NC_ELATEDEF Too late to change settings for this variable.
 * @returns ::NC_ENOTINDEFINE Not in define mode.
 * @returns ::NC_EINVAL Invalid input
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
NCZ_def_var_quantize(int ncid, int varid, int quantize_mode, int nsd)
{
    return ncz_def_var_extra(ncid, varid, NULL, NULL, NULL, NULL,
			    NULL, NULL, NULL, NULL, NULL,
			    &quantize_mode, &nsd);
}

/**
Ensure that the quantize information for a variable is defined.
Keep a flag in the NCZ_VAR_INFO_T struct to indicate if quantize
info is defined, and if not, read the attribute.
*/
int
NCZ_ensure_quantizer(int ncid, NC_VAR_INFO_T* var)
{
    int nsd = 0;

    /* Read the attribute */
    if(NCZ_get_att(ncid,var->hdr.id,NC_QUANTIZE_BITGROOM_ATT_NAME,&nsd,NC_INT)==NC_NOERR) {
	var->quantize_mode = NC_QUANTIZE_BITGROOM;
        var->nsd = nsd;
    } else if(NCZ_get_att(ncid,var->hdr.id,NC_QUANTIZE_GRANULARBR_ATT_NAME,&nsd,NC_INT)==NC_NOERR) {
	var->quantize_mode = NC_QUANTIZE_GRANULARBR;
        var->nsd = nsd;
    } else if(NCZ_get_att(ncid,var->hdr.id,NC_QUANTIZE_BITROUND_ATT_NAME,&nsd,NC_INT)==NC_NOERR) {
	var->quantize_mode = NC_QUANTIZE_BITROUND;
        var->nsd = nsd;
    } else {
	var->quantize_mode = NC_NOQUANTIZE;
        var->nsd = 0;
    }
    if(var->quantize_mode < 0) var->quantize_mode = 0;
    return NC_NOERR;
}

/**
 * @internal Get quantize information about a variable. Pass NULL for
 * whatever you don't care about. Note that this can require reading
 * all the attributes for the variable.
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param quantize_modep Gets quantize mode.
 * @param nsdp Gets Number of Significant Digits if quantize is in use.
 *
 * @returns ::NC_NOERR No error.
 * @returns ::NC_EBADID Bad ncid.
 * @returns ::NC_ENOTVAR Bad varid.
 * @returns ::NC_EINVAL Invalid input.
 * @author Ed Hartnett
 */
int
NCZ_inq_var_quantize(int ncid, int varid, int *quantize_modep,
		     int *nsdp)
{
    NC_VAR_INFO_T *var;
    int retval;

    LOG((2, "%s: ncid 0x%x varid %d", __func__, ncid, varid));

    /* Find info for this file and group, and set pointer to each. */
    /* Get pointer to the var. */
    if ((retval = nc4_find_grp_h5_var(ncid, varid, NULL, NULL, &var)))
        return retval;
    if (!var)
        return NC_ENOTVAR;	
    assert(var->hdr.id == varid);
    if(var->quantize_mode == -1)
        {if((retval = NCZ_ensure_quantizer(ncid, var))) return retval;}
    /* Copy the data to the user's data buffers. */
    if (quantize_modep)
        *quantize_modep = var->quantize_mode;
    if (nsdp)
        *nsdp = var->nsd;
    return NC_NOERR;
 }

/**
 * @internal Rename a var to "bubba," for example. This is called by
 * nc_rename_var() for netCDF-4 files. This results in complexities
 * when coordinate variables are involved.

 * Whenever a var has the same name as a dim, and also uses that dim
 * as its first dimension, then that var is aid to be a coordinate
 * variable for that dimensions. Coordinate variables are represented
 * in the ZARR by making them dimscales. Dimensions without coordinate
 * vars are represented by datasets which are dimscales, but have a
 * special attribute marking them as dimscales without associated
 * coordinate variables.
 *
 * When a var is renamed, we must detect whether it has become a
 * coordinate var (by being renamed to the same name as a dim that is
 * also its first dimension), or whether it is no longer a coordinate
 * var. These cause flags to be set in NC_VAR_INFO_T which are used at
 * enddef time to make changes in the ZARR file.
 *
 * @param ncid File ID.
 * @param varid Variable ID
 * @param name New name of the variable.
 *
 * @returns ::NC_NOERR No error.
 * @returns ::NC_EBADID Bad ncid.
 * @returns ::NC_ENOTVAR Invalid variable ID.
 * @returns ::NC_EBADNAME Bad name.
 * @returns ::NC_EMAXNAME Name is too long.
 * @returns ::NC_ENAMEINUSE Name in use.
 * @returns ::NC_ENOMEM Out of memory.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_rename_var(int ncid, int varid, const char *name)
{
    NC_GRP_INFO_T *grp;
    NC_FILE_INFO_T *h5;
    NC_VAR_INFO_T *var;
    int retval = NC_NOERR;

    if (!name)
	return NC_EINVAL;

    LOG((2, "%s: ncid 0x%x varid %d name %s", __func__, ncid, varid, name));

    ZTRACE(1,"ncid=%d varid=%d name='%s'",ncid,varid,name);

    /* Find info for this file and group, and set pointer to each. */
    if ((retval = nc4_find_grp_h5(ncid, &grp, &h5)))
	return THROW(retval);
    assert(h5 && grp && grp->format_grp_info);

    /* Is the new name too long? */
    if (strlen(name) > NC_MAX_NAME)
	return NC_EMAXNAME;

    /* Trying to write to a read-only file? No way, Jose! */
    if (h5->no_write)
	return NC_EPERM;

    /* Check name validity, if strict nc3 rules are in effect for this
     * file. */
    if ((retval = NC_check_name(name)))
	return THROW(retval);

    /* Get the variable wrt varid */
    if (!(var = (NC_VAR_INFO_T *)ncindexith(grp->vars, varid)))
	return NC_ENOTVAR;

    /* Check if new name is in use; note that renaming to same name is
       still an error according to the nc_test/test_write.c
       code. Why?*/
    if (ncindexlookup(grp->vars, name))
	return NC_ENAMEINUSE;

    /* If we're not in define mode, new name must be of equal or
       less size, if strict nc3 rules are in effect for this . */
    if (!(h5->flags & NC_INDEF) && strlen(name) > strlen(var->hdr.name) &&
	(h5->cmode & NC_CLASSIC_MODEL))
	return NC_ENOTINDEFINE;

#ifdef LOOK
    /* Is there another dim with this name, for which this var will not
     * be a coord var? If so, we have to create a dim without a
     * variable for the old name. */
    if ((other_dim = (NC_DIM_INFO_T *)ncindexlookup(grp->dim, name)) &&
	strcmp(name, var->dim[0]->hdr.name))
    {
	/* Create a dim without var dataset for old dim. */
	if ((retval = ncz_create_dim_wo_var(other_dim)))
	    return THROW(retval);

	/* Give this var a secret ZARR name so it can co-exist in file
	 * with dim wp var dataset. Base the secret name on the new var
	 * name. */
	if ((retval = give_var_secret_name(var, name)))
	    return THROW(retval);
	use_secret_name++;
    }

    /* Change the ZARR file, if this var has already been created
       there. */
    if (var->created)
    {
	int v;
	char *ncz_name; /* Dataset will be renamed to this. */
	ncz_name = use_secret_name ? var->ncz_name: (char *)name;

	/* Do we need to read var metadata? */
	if (!var->meta_read)
	    if ((retval = ncz_get_var_meta(var)))
		return THROW(retval);

	if (var->ndims)
	{
	    NCZ_DIM_INFO_T *ncz_d0;
	    ncz_d0 = (NCZ_DIM_INFO_T *)var->dim[0]->format_dim_info;

	    /* Is there an existing dimscale-only dataset of this name? If
	     * so, it must be deleted. */
	    if (ncz_d0->hdf_dimscaleid)
	    {
		if ((retval = delete_dimscale_dataset(grp, var->dim[0]->hdr.id,
						      var->dim[0])))
		    return THROW(retval);
	    }
	}

	LOG((3, "Moving dataset %s to %s", var->hdr.name, name));
	if (H5Lmove(ncz_grp->hdf_grpid, var->hdr.name, ncz_grp->hdf_grpid,
		    ncz_name, H5P_DEFAULT, H5P_DEFAULT) < 0)
	    return NC_EHDFERR;

	/* Rename all the vars in this file with a varid greater than
	 * this var. Varids are assigned based on dataset creation time,
	 * and we have just changed that for this var. We must do the
	 * same for all vars with a > varid, so that the creation order
	 * will continue to be correct. */
	for (v = var->hdr.id + 1; v < ncindexsize(grp->vars); v++)
	{
	    NC_VAR_INFO_T *my_var;
	    my_var = (NC_VAR_INFO_T *)ncindexith(grp->vars, v);
	    assert(my_var);

	    LOG((3, "mandatory rename of %s to same name", my_var->hdr.name));

	    /* Rename to temp name. */
	    if (H5Lmove(ncz_grp->hdf_grpid, my_var->hdr.name, ncz_grp->hdf_grpid,
			NC_TEMP_NAME, H5P_DEFAULT, H5P_DEFAULT) < 0)
		return NC_EHDFERR;

	    /* Rename to real name. */
	    if (H5Lmove(ncz_grp->hdf_grpid, NC_TEMP_NAME, ncz_grp->hdf_grpid,
			my_var->hdr.name, H5P_DEFAULT, H5P_DEFAULT) < 0)
		return NC_EHDFERR;
	}
    }
#endif

    /* Now change the name in our metadata. */
    free(var->hdr.name);
    if (!(var->hdr.name = strdup(name)))
	return NC_ENOMEM;
    LOG((3, "var is now %s", var->hdr.name));

    /* rebuild index. */
    if (!ncindexrebuild(grp->vars))
	return NC_EINTERNAL;

#ifdef LOOK
    /* Check if this was a coordinate variable previously, but names
     * are different now */
    if (var->dimscale && strcmp(var->hdr.name, var->dim[0]->hdr.name))
    {
	/* Break up the coordinate variable */
	if ((retval = ncz_break_coord_var(grp, var, var->dim[0])))
	    return THROW(retval);
    }

    /* Check if this should become a coordinate variable. */
    if (!var->dimscale)
    {
	/* Only variables with >0 dimensions can become coordinate
	 * variables. */
	if (var->ndims)
	{
	    NC_GRP_INFO_T *dim_grp;
	    NC_DIM_INFO_T *dim;

	    /* Check to see if this is became a coordinate variable.  If
	     * so, it will have the same name as dimension index 0. If it
	     * is a coordinate var, is it a coordinate var in the same
	     * group as the dim? */
	    if ((retval = ncz_find_dim(grp, var->dimids[0], &dim, &dim_grp)))
		return THROW(retval);
	    if (!strcmp(dim->hdr.name, name) && dim_grp == grp)
	    {
		/* Reform the coordinate variable. */
		if ((retval = ncz_reform_coord_var(grp, var, dim)))
		    return THROW(retval);
		var->became_coord_var = NC_TRUE;
	    }
	}
    }
#endif

    return THROW(retval);
}

/**
 * @internal Write an array of data to a variable. This is called by
 * nc_put_vara() and other nc_put_vara_* functions, for netCDF-4
 * files.
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param startp Array of start indices.
 * @param countp Array of counts.
 * @param op pointer that gets the data.
 * @param memtype The type of these data in memory.
 *
 * @returns ::NC_NOERR for success
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_put_vara(int ncid, int varid, const size_t *startp,
	     const size_t *countp, const void *op, int memtype)
{
    return NCZ_put_vars(ncid, varid, startp, countp, NULL, op, memtype);
}

/**
 * @internal Read an array of values. This is called by nc_get_vara()
 * for netCDF-4 files, as well as all the other nc_get_vara_*
 * functions.
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param startp Array of start indices.
 * @param countp Array of counts.
 * @param ip pointer that gets the data.
 * @param memtype The type of these data after it is read into memory.

 * @returns ::NC_NOERR for success
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_get_vara(int ncid, int varid, const size_t *startp,
	     const size_t *countp, void *ip, int memtype)
{
    return NCZ_get_vars(ncid, varid, startp, countp, NULL, ip, memtype);
}

/**
 * @internal Do some common check for NCZ_put_vars and
 * NCZ_get_vars. These checks have to be done when both reading and
 * writing data.
 *
 * @param mem_nc_type Pointer to type of data in memory.
 * @param var Pointer to var info struct.
 * @param h5 Pointer to ZARR file info struct.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner, Ed Hartnett
 */
static int
check_for_vara(nc_type *mem_nc_type, NC_VAR_INFO_T *var, NC_FILE_INFO_T *h5)
{
    int retval;

    /* If mem_nc_type is NC_NAT, it means we want to use the file type
     * as the mem type as well. */
    assert(mem_nc_type);
    if (*mem_nc_type == NC_NAT)
	*mem_nc_type = var->type_info->hdr.id;
    assert(*mem_nc_type);

    /* No NC_CHAR conversions, you pervert! */
    if (var->type_info->hdr.id != *mem_nc_type &&
	(var->type_info->hdr.id == NC_CHAR || *mem_nc_type == NC_CHAR))
	return NC_ECHAR;

    /* If we're in define mode, we can't read or write data. */
    if (h5->flags & NC_INDEF)
    {
	if (h5->cmode & NC_CLASSIC_MODEL)
	    return NC_EINDEFINE;
	if ((retval = ncz_enddef_netcdf4_file(h5)))
	    return THROW(retval);
    }

    return NC_NOERR;
}

#ifdef LOGGING
/**
 * @intarnal Print some debug info about dimensions to the log.
 */
static void
log_dim_info(NC_VAR_INFO_T *var, size64_t *fdims, size64_t *fmaxdims,
	     size64_t *start, size64_t *count)
{
    int d2;

    /* Print some debugging info... */
    LOG((4, "%s: var name %s ndims %d", __func__, var->hdr.name, var->ndims));
    LOG((4, "File space, and requested:"));
    for (d2 = 0; d2 < var->ndims; d2++)
    {
	LOG((4, "fdims[%d]=%Ld fmaxdims[%d]=%Ld", d2, fdims[d2], d2,
	     fmaxdims[d2]));
	LOG((4, "start[%d]=%Ld	count[%d]=%Ld", d2, start[d2], d2, count[d2]));
    }
}
#endif /* LOGGING */

/**
 * @internal Write a strided array of data to a variable. This is
 * called by nc_put_vars() and other nc_put_vars_* functions, for
 * netCDF-4 files. Also the nc_put_vara() calls end up calling this
 * with a NULL stride parameter.
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param startp Array of start indices. Must always be provided by
 * caller for non-scalar vars.
 * @param countp Array of counts. Will default to counts of full
 * dimension size if NULL.
 * @param stridep Array of strides. Will default to strides of 1 if
 * NULL.
 * @param data The data to be written.
 * @param mem_nc_type The type of the data in memory.
 *
 * @returns ::NC_NOERR No error.
 * @returns ::NC_EBADID Bad ncid.
 * @returns ::NC_ENOTVAR Var not found.
 * @returns ::NC_EHDFERR ZARR function returned error.
 * @returns ::NC_EINVALCOORDS Incorrect start.
 * @returns ::NC_EEDGE Incorrect start/count.
 * @returns ::NC_ENOMEM Out of memory.
 * @returns ::NC_EMPI MPI library error (parallel only)
 * @returns ::NC_ECANTEXTEND Can't extend dimension for write.
 * @returns ::NC_ERANGE Data conversion error.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_put_vars(int ncid, int varid, const size_t *startp, const size_t *countp,
	     const ptrdiff_t *stridep, const void *data, nc_type mem_nc_type)
{
    NC_GRP_INFO_T *grp;
    NC_FILE_INFO_T *h5;
    NC_VAR_INFO_T *var;
    NC_DIM_INFO_T *dim;
#ifdef LOOK
    hid_t file_spaceid = 0, mem_spaceid = 0, xfer_plistid = 0;
#endif
    size64_t fdims[NC_MAX_VAR_DIMS];
    size64_t start[NC_MAX_VAR_DIMS], count[NC_MAX_VAR_DIMS];
    size64_t stride[NC_MAX_VAR_DIMS], ones[NC_MAX_VAR_DIMS];
    int retval, range_error = 0, i, d2;
    void *bufr = NULL;
    int bufrd = 0; /* 1 => we allocated bufr */
    int need_to_convert = 0;
    int zero_count = 0; /* true if a count is zero */
    size_t len = 1;
    size64_t fmaxdims[NC_MAX_VAR_DIMS];
    NCZ_VAR_INFO_T* zvar;

    NC_UNUSED(fmaxdims);

#ifndef LOOK
    NC_UNUSED(fmaxdims);
#endif
    
    /* Find info for this file, group, and var. */
    if ((retval = nc4_find_grp_h5_var(ncid, varid, &h5, &grp, &var)))
	return THROW(retval);
    assert(h5 && grp && var && var->hdr.id == varid && var->format_var_info);

    LOG((3, "%s: var->hdr.name %s mem_nc_type %d", __func__,
	 var->hdr.name, mem_nc_type));

    if(h5->no_write)
        return NC_EPERM;

    zvar = (NCZ_VAR_INFO_T*)var->format_var_info;

    /* Cannot convert to user-defined types. */
    if (mem_nc_type >= NC_FIRSTUSERTYPEID)
	return THROW(NC_EINVAL);

    /* Check some stuff about the type and the file. If the file must
     * be switched from define mode, it happens here. */
    if ((retval = check_for_vara(&mem_nc_type, var, h5)))
	return THROW(retval);
    assert(!var->ndims || (startp && countp));

    /* Convert from size_t and ptrdiff_t to size64_t */
    /* Also do sanity checks */
    if(var->ndims == 0) { /* scalar */
	start[0] = 0;
	count[0] = 1;
	stride[0] = 1;
	ones[0] = 1;
    } else {
        for (i = 0; i < var->ndims; i++)
        {
    	    /* Check for non-positive stride. */
	    if (stridep && stridep[i] <= 0)
	        return NC_ESTRIDE;

	    fdims[i] = var->dim[i]->len;
	    start[i] = startp[i];
	    count[i] = countp ? countp[i] : fdims[i];
	    stride[i] = stridep ? stridep[i] : 1;
	    ones[i] = 1;

  	    /* Check to see if any counts are zero. */
	    if (!count[i])
	        zero_count++;
	}
    }


#ifdef LOGGING
    log_dim_info(var, fdims, fdims, start, count);
#endif

    /* Check dimension bounds. Remember that unlimited dimensions can
     * put data beyond their current length. */
    for (d2 = 0; d2 < var->ndims; d2++)
    {
	size64_t endindex = start[d2] + stride[d2] * (count[d2] - 1); /* last index written */
	dim = var->dim[d2];
	assert(dim && dim->hdr.id == var->dimids[d2]);
	if (count[d2] == 0)
	    endindex = start[d2]; /* fixup for zero read count */
	if (!dim->unlimited)
	{
	    /* Allow start to equal dim size if count is zero. */
	    if (start[d2] > fdims[d2] || (start[d2] == fdims[d2] && count[d2] > 0))
		BAIL_QUIET(NC_EINVALCOORDS);
	    if (!zero_count && endindex >= fdims[d2])
		BAIL_QUIET(NC_EEDGE);
	}
    }


#ifdef LOOK
    /* Now you would think that no one would be crazy enough to write
       a scalar dataspace with one of the array function calls, but you
       would be wrong. So let's check to see if the dataset is
       scalar. If it is, we won't try to set up a hyperslab. */
    if (H5Sget_simple_extent_type(file_spaceid) == H5S_SCALAR)
    {
	if ((mem_spaceid = H5Screate(H5S_SCALAR)) < 0)
	    BAIL(NC_EHDFERR);
    }
    else
    {
	if (H5Sselect_hyperslab(file_spaceid, H5S_SELECT_SET, start, stride,
				ones, count) < 0)
	    BAIL(NC_EHDFERR);

	/* Create a space for the memory, just big enough to hold the slab
	   we want. */
	if ((mem_spaceid = H5Screate_simple(var->ndims, count, NULL)) < 0)
	    BAIL(NC_EHDFERR);
    }
#endif


    /* Are we going to convert any data? (No converting of compound or
     * opaque or vlen types.) We also need to call this code if we are doing
     * quantization. */
    if ((mem_nc_type != var->type_info->hdr.id &&
	mem_nc_type != NC_COMPOUND && mem_nc_type != NC_OPAQUE
	&& mem_nc_type != NC_VLEN)
	|| var->quantize_mode > 0)
    {
	size_t file_type_size;

	/* We must convert - allocate a buffer. */
	need_to_convert++;
	if(zvar->scalar)
	    len = 1;
        else for (d2=0; d2<var->ndims; d2++)
	    len *= countp[d2];

	LOG((4, "converting data for var %s type=%d len=%d", var->hdr.name,
	     var->type_info->hdr.id, len));

	/* Later on, we will need to know the size of this type in the
	 * file. */
	assert(var->type_info->size);
	file_type_size = var->type_info->size;

	/* If we're reading, we need bufr to have enough memory to store
	 * the data in the file. If we're writing, we need bufr to be
	 * big enough to hold all the data in the file's type. */
	if (len > 0) {
	    assert(bufr == NULL);
	    if (!(bufr = malloc(len * file_type_size)))
		BAIL(NC_ENOMEM);
	    bufrd = 1;
	}
    }
    else
	bufr = (void *)data;

    /* Write this hyperslab from memory to file. Does the dataset have to be
       extended? If it's already extended to the required size, it will
       do no harm to reextend it to that size. */
    if (var->ndims)
    {

	for (d2 = 0; d2 < var->ndims; d2++)
	{
	    size64_t endindex = start[d2] + stride[d2] * (count[d2] - 1); /* last index written */
	    if (count[d2] == 0)
		endindex = start[d2];
	    dim = var->dim[d2];
	    assert(dim && dim->hdr.id == var->dimids[d2]);
	    if (dim->unlimited)
	    {
		if (!zero_count && endindex >= fdims[d2])
		{
		    dim->len = (endindex+1);
		}
		else
		    dim->len = fdims[d2];

		if (!zero_count && endindex >= dim->len)
		{
		    dim->len = endindex+1;
		    dim->extended = NC_TRUE;
		}
	    }
	}



#ifdef LOOK
	/* If we need to extend it, we also need a new file_spaceid
	   to reflect the new size of the space. */
	if (need_to_extend)
	{
	    LOG((4, "extending dataset"));
	    /* Convert xtend_size back to hsize_t for use with
	     * H5Dset_extent. */
	    for (d2 = 0; d2 < var->ndims; d2++)
		fdims[d2] = (size64_t)xtend_size[d2];
	    if (H5Dset_extent(ncz_var->hdf_datasetid, fdims) < 0)
		BAIL(NC_EHDFERR);
	    if (file_spaceid > 0 && H5Sclose(file_spaceid) < 0)
		BAIL2(NC_EHDFERR);
	    if ((file_spaceid = H5Dget_space(ncz_var->hdf_datasetid)) < 0)
		BAIL(NC_EHDFERR);
	    if (H5Sselect_hyperslab(file_spaceid, H5S_SELECT_SET,
				    start, stride, ones, count) < 0)
		BAIL(NC_EHDFERR);
	}
#endif
    }

    /* Do we need to convert the data? */
    if (need_to_convert)
    {
	if(var->quantize_mode < 0) {if((retval = NCZ_ensure_quantizer(ncid,var))) BAIL(retval);}
	assert(bufr != NULL);
	if ((retval = nc4_convert_type(data, bufr, mem_nc_type, var->type_info->hdr.id,
				       len, &range_error, var->fill_value,
				       (h5->cmode & NC_CLASSIC_MODEL),
				       var->quantize_mode, var->nsd)))
	    BAIL(retval);
    }

#ifdef LOOK
    /* Write the data. At last! */
    LOG((4, "about to write datasetid 0x%x mem_spaceid 0x%x "
	 "file_spaceid 0x%x", ncz_var->hdf_datasetid, mem_spaceid, file_spaceid));
    if (H5Dwrite(ncz_var->hdf_datasetid,
		 ((NCZ_TYPE_INFO_T *)var->type_info->format_type_info)->hdf_typeid,
		 mem_spaceid, file_spaceid, xfer_plistid, bufr) < 0)
	BAIL(NC_EHDFERR);
#endif /*LOOK*/

    if((retval = NCZ_transferslice(var, WRITING, start, count, stride, bufr, var->type_info->hdr.id)))
	BAIL(retval);

    /* Remember that we have written to this var so that Fill Value
     * can't be set for it. */
    if (!var->written_to)
	var->written_to = NC_TRUE;

    /* For strict netcdf-3 rules, ignore erange errors between UBYTE
     * and BYTE types. */
    if ((h5->cmode & NC_CLASSIC_MODEL) &&
	(var->type_info->hdr.id == NC_UBYTE || var->type_info->hdr.id == NC_BYTE) &&
	(mem_nc_type == NC_UBYTE || mem_nc_type == NC_BYTE) &&
	range_error)
	range_error = 0;

exit:
#ifdef LOOK
    if (file_spaceid > 0 && H5Sclose(file_spaceid) < 0)
	BAIL2(NC_EHDFERR);
    if (mem_spaceid > 0 && H5Sclose(mem_spaceid) < 0)
	BAIL2(NC_EHDFERR);
    if (xfer_plistid && (H5Pclose(xfer_plistid) < 0))
	BAIL2(NC_EPARINIT);
#endif
    if (bufrd && bufr) free(bufr);

    /* If there was an error return it, otherwise return any potential
       range error value. If none, return NC_NOERR as usual.*/
    if (retval)
	return THROW(retval);
    if (range_error)
	return NC_ERANGE;
    return NC_NOERR;
}

/**
 * @internal Read a strided array of data from a variable. This is
 * called by nc_get_vars() for netCDF-4 files, as well as all the
 * other nc_get_vars_* functions.
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param startp Array of start indices. Must be provided for
 * non-scalar vars.
 * @param countp Array of counts. Will default to counts of extent of
 * dimension if NULL.
 * @param stridep Array of strides. Will default to strides of 1 if
 * NULL.
 * @param data The data to be written.
 * @param mem_nc_type The type of the data in memory. (Convert to this
 * type from file type.)
 *
 * @returns ::NC_NOERR No error.
 * @returns ::NC_EBADID Bad ncid.
 * @returns ::NC_ENOTVAR Var not found.
 * @returns ::NC_EHDFERR ZARR function returned error.
 * @returns ::NC_EINVALCOORDS Incorrect start.
 * @returns ::NC_EEDGE Incorrect start/count.
 * @returns ::NC_ENOMEM Out of memory.
 * @returns ::NC_EMPI MPI library error (parallel only)
 * @returns ::NC_ECANTEXTEND Can't extend dimension for write.
 * @returns ::NC_ERANGE Data conversion error.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_get_vars(int ncid, int varid, const size_t *startp, const size_t *countp,
	     const ptrdiff_t *stridep, void *data, nc_type mem_nc_type)
{
    NC_GRP_INFO_T *grp;
    NC_FILE_INFO_T *h5;
    NC_VAR_INFO_T *var;
    NC_DIM_INFO_T *dim;
    size_t file_type_size;
    size64_t count[NC_MAX_VAR_DIMS];
    size64_t fdims[NC_MAX_VAR_DIMS]; /* size of the dimensions */
    size64_t start[NC_MAX_VAR_DIMS];
    size64_t stride[NC_MAX_VAR_DIMS];
    size64_t ones[NC_MAX_VAR_DIMS];
    int no_read = 0, provide_fill = 0;
    int fill_value_size[NC_MAX_VAR_DIMS];
    int retval, range_error = 0, i, d2;
    void *bufr = NULL;
    int need_to_convert = 0;
    size_t len = 1;
    NCZ_VAR_INFO_T* zvar = NULL;

    /* Find info for this file, group, and var. */
    if ((retval = nc4_find_grp_h5_var(ncid, varid, &h5, &grp, &var)))
	return THROW(retval);
    assert(h5 && grp && var && var->hdr.id == varid && var->format_var_info &&
	   var->type_info && var->type_info->size &&
	   var->type_info->format_type_info);

    LOG((3, "%s: var->hdr.name %s mem_nc_type %d", __func__,
	 var->hdr.name, mem_nc_type));

    zvar = (NCZ_VAR_INFO_T*)var->format_var_info;

    /* Check some stuff about the type and the file. Also end define
     * mode, if needed. */
    if ((retval = check_for_vara(&mem_nc_type, var, h5)))
	return THROW(retval);
    assert((!var->ndims || (startp && countp)));

    /* Convert from size_t and ptrdiff_t to size64_t. Also do sanity
     * checks. */
    if(var->ndims == 0) { /* scalar */
	start[0] = 0;
	count[0] = 1;
	stride[0] = 1;
	ones[0] = 1;
    } else {
        for (i = 0; i < var->ndims; i++)
        {
	    /* If any of the stride values are non-positive, fail. */
	    if (stridep && stridep[i] <= 0)
	        return NC_ESTRIDE;
	    start[i] = startp[i];
	    count[i] = countp[i];
	    stride[i] = stridep ? stridep[i] : 1;

	    ones[i] = 1;
	    /* if any of the count values are zero don't actually read. */
	    if (count[i] == 0)
	        no_read++;

	    /* Get dimension sizes also */
	    fdims[i] = var->dim[i]->len;
	    /* if any of the counts are zero don't actually read. */
	    if (count[i] == 0)
	        no_read++;
	}
    }

#ifdef LOGGING
    log_dim_info(var, fdims, fdims, start, count);
#endif

    /* Check the type_info fields. */
    assert(var->type_info && var->type_info->size &&
           var->type_info->format_type_info);

    /* Later on, we will need to know the size of this type in the
     * file. */
    file_type_size = var->type_info->size;

    /* Are we going to convert any data? (No converting of compound or
     * opaque types.) */
    if (mem_nc_type != var->type_info->hdr.id &&
        mem_nc_type != NC_COMPOUND && mem_nc_type != NC_OPAQUE)
    {
        /* We must convert - allocate a buffer. */
        need_to_convert++;
	if(zvar->scalar) {
	    len *= countp[0];	
        } else {
	    for (d2 = 0; d2 < (var->ndims); d2++)
                len *= countp[d2];
        }
        LOG((4, "converting data for var %s type=%d len=%d", var->hdr.name,
 		       var->type_info->hdr.id, len));

        /* If we're reading, we need bufr to have enough memory to store
         * the data in the file. If we're writing, we need bufr to be
         * big enough to hold all the data in the file's type. */
        if (len > 0)
            if (!(bufr = malloc(len * file_type_size)))
                BAIL(NC_ENOMEM);
    }
    else
        if (!bufr)
            bufr = data;

    /* Check dimension bounds. Remember that unlimited dimensions can
     * read/write data beyond their largest current length. */
    for (d2 = 0; d2 < var->ndims; d2++)
    {
	size64_t endindex = start[d2] + stride[d2] * (count[d2] - 1); /* last index read */
	dim = var->dim[d2];
	assert(dim && dim->hdr.id == var->dimids[d2]);
	if (count[d2] == 0)
	    endindex = start[d2]; /* fixup for zero read count */
	if (dim->unlimited)
	{
	    size64_t ulen = (size64_t)dim->len;
	    /* Check for out of bound requests. */
	    /* Allow start to equal dim size if count is zero. */
	    if (start[d2] > ulen || (start[d2] == ulen && count[d2] > 0))
		BAIL_QUIET(NC_EINVALCOORDS);
	    if (count[d2] && endindex >= ulen)
		BAIL_QUIET(NC_EEDGE);

	    /* Things get a little tricky here. If we're getting a GET
	       request beyond the end of this var's current length in
	       an unlimited dimension, we'll later need to return the
	       fill value for the variable. */
	    if (!no_read)
	    {
		if (start[d2] >= (size64_t)fdims[d2])
		    fill_value_size[d2] = count[d2];
		else if (endindex >= fdims[d2])
		    fill_value_size[d2] = count[d2] - ((fdims[d2] - start[d2])/stride[d2]);
		else
		    fill_value_size[d2] = 0;
		count[d2] -= fill_value_size[d2];
		if (count[d2] == 0)
		    no_read++;
		if (fill_value_size[d2])
		    provide_fill++;
	    }
	    else
		fill_value_size[d2] = count[d2];
	}
	else /* Dim is not unlimited. */
	{
	    /* Check for out of bound requests. */
	    /* Allow start to equal dim size if count is zero. */
	    if (start[d2] > (size64_t)fdims[d2] ||
		(start[d2] == (size64_t)fdims[d2] && count[d2] > 0))
		BAIL_QUIET(NC_EINVALCOORDS);
	    if (count[d2] && endindex >= fdims[d2])
		BAIL_QUIET(NC_EEDGE);
	    /* Set the fill value boundary */
	    fill_value_size[d2] = count[d2];
	}
    }

    if (!no_read)
    {
#ifdef LOOK
	/* Now you would think that no one would be crazy enough to write
	   a scalar dataspace with one of the array function calls, but you
	   would be wrong. So let's check to see if the dataset is
	   scalar. If it is, we won't try to set up a hyperslab. */
	if (H5Sget_simple_extent_type(file_spaceid) == H5S_SCALAR)
	{
	    if ((mem_spaceid = H5Screate(H5S_SCALAR)) < 0)
		BAIL(NC_EHDFERR);
	    scalar++;
	}
	else
	{
	    if (H5Sselect_hyperslab(file_spaceid, H5S_SELECT_SET,
				    start, stride, ones, count) < 0)
		BAIL(NC_EHDFERR);
	    /* Create a space for the memory, just big enough to hold the slab
	       we want. */
	    if ((mem_spaceid = H5Screate_simple(var->ndims, count, NULL)) < 0)
		BAIL(NC_EHDFERR);
	}
#endif

#ifdef LOOK
	/* Fix bug when reading ZARR files with variable of type
	 * fixed-length string.	 We need to make it look like a
	 * variable-length string, because that's all netCDF-4 data
	 * model supports, lacking anonymous dimensions.  So
	 * variable-length strings are in allocated memory that user has
	 * to free, which we allocate here. */
	if (var->type_info->nc_type_class == NC_STRING &&
	    H5Tget_size(ncz_type->hdf_typeid) > 1 &&
	    !H5Tis_variable_str(ncz_type->hdf_typeid))
	{
	    size64_t fstring_len;

	    if ((fstring_len = H5Tget_size(ncz_type->hdf_typeid)) == 0)
		BAIL(NC_EHDFERR);
	    if (!(*(char **)data = malloc(1 + fstring_len)))
		BAIL(NC_ENOMEM);
	    bufr = *(char **)data;
	}
#endif

#ifdef LOOK
	/* Create the data transfer property list. */
	if ((xfer_plistid = H5Pcreate(H5P_DATASET_XFER)) < 0)
	    BAIL(NC_EHDFERR);

	/* Read this hyperslab into memory. */
	LOG((5, "About to H5Dread some data..."));
	if (H5Dread(ncz_var->hdf_datasetid,
		    ((NCZ_TYPE_INFO_T *)var->type_info->format_type_info)->native_hdf_typeid,
		    mem_spaceid, file_spaceid, xfer_plistid, bufr) < 0)
	    BAIL(NC_EHDFERR);
#endif /*LOOK*/

	if((retval = NCZ_transferslice(var, READING, start, count, stride, bufr, var->type_info->hdr.id)))
	    BAIL(retval);
    } /* endif ! no_read */

    /* Now we need to fake up any further data that was asked for,
       using the fill values instead. First skip past the data we
       just read, if any. */
    if (!zvar->scalar && provide_fill)
    {
	void *filldata;
	size_t real_data_size = 0;
	size_t fill_len;

	/* Skip past the real data we've already read. */
	if (!no_read)
	    for (real_data_size = file_type_size, d2 = 0; d2 < var->ndims; d2++)
		real_data_size *= count[d2];

	/* Get the fill value from the ZARR variable. Memory will be
	 * allocated. */
	if (NCZ_ensure_fill_value(var))
	    BAIL(NC_EINVAL);

	/* How many fill values do we need? */
	for (fill_len = 1, d2 = 0; d2 < var->ndims; d2++)
	    fill_len *= (fill_value_size[d2] ? fill_value_size[d2] : 1);

	/* Copy the fill value into the rest of the data buffer. */
	filldata = (char *)data + real_data_size;
	for (i = 0; i < fill_len; i++)
	{
	    /* Copy one instance of the fill_value */
	    if((retval = NC_copy_data(h5->controller,var->type_info->hdr.id,var->fill_value,1,filldata)))
	        BAIL(retval);
	    filldata = (char *)filldata + file_type_size;
	}
    }

    /* Convert data type if needed. */
    if (need_to_convert)
    {
	if(var->quantize_mode < 0) {if((retval = NCZ_ensure_quantizer(ncid,var))) BAIL(retval);}
	if ((retval = nc4_convert_type(bufr, data, var->type_info->hdr.id, mem_nc_type,
					   len, &range_error, var->fill_value,
				           (h5->cmode & NC_CLASSIC_MODEL), var->quantize_mode,
				           var->nsd)))
	   BAIL(retval);
        /* For strict netcdf-3 rules, ignore erange errors between UBYTE
	 * and BYTE types. */
	if ((h5->cmode & NC_CLASSIC_MODEL) &&
		(var->type_info->hdr.id == NC_UBYTE || var->type_info->hdr.id == NC_BYTE) &&
		(mem_nc_type == NC_UBYTE || mem_nc_type == NC_BYTE) &&
		range_error)
		range_error = 0;
    }

exit:
#ifdef LOOK
    if (file_spaceid > 0)
	if (H5Sclose(file_spaceid) < 0)
	    BAIL2(NC_EHDFERR);
    if (mem_spaceid > 0)
	if (H5Sclose(mem_spaceid) < 0)
	    BAIL2(NC_EHDFERR);
    if (xfer_plistid > 0)
	if (H5Pclose(xfer_plistid) < 0)

	    BAIL2(NC_EHDFERR);
#endif
    if (need_to_convert && bufr)
	free(bufr);
    /* If there was an error return it, otherwise return any potential
       range error value. If none, return NC_NOERR as usual.*/
    if (retval)
	return THROW(retval);
    if (range_error)
	return THROW(NC_ERANGE);
    return NC_NOERR;
}

/**
 * @internal Get all the information about a variable. Pass NULL for
 * whatever you don't care about.
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param name Gets name.
 * @param xtypep Gets type.
 * @param ndimsp Gets number of dims.
 * @param dimidsp Gets array of dim IDs.
 * @param nattsp Gets number of attributes.
 * @param shufflep Gets shuffle setting.
 * @param deflatep Gets deflate setting.
 * @param deflate_levelp Gets deflate level.
 * @param fletcher32p Gets fletcher32 setting.
 * @param contiguousp Gets contiguous setting.
 * @param chunksizesp Gets chunksizes.
 * @param no_fill Gets fill mode.
 * @param fill_valuep Gets fill value.
 * @param endiannessp Gets one of ::NC_ENDIAN_BIG ::NC_ENDIAN_LITTLE
 * @param idp Pointer to memory to store filter id.
 * @param nparamsp Pointer to memory to store filter parameter count.
 * @param params Pointer to vector of unsigned integers into which
 * to store filter parameters.
 *
 * @returns ::NC_NOERR No error.
 * @returns ::NC_EBADID Bad ncid.
 * @returns ::NC_ENOTVAR Bad varid.
 * @returns ::NC_ENOMEM Out of memory.
 * @returns ::NC_EINVAL Invalid input.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_inq_var_all(int ncid, int varid, char *name, nc_type *xtypep,
		     int *ndimsp, int *dimidsp, int *nattsp,
		     int *shufflep, int *unused4, int *unused5,
		     int *fletcher32p, int *storagep, size_t *chunksizesp,
		     int *no_fill, void *fill_valuep, int *endiannessp,
		     unsigned int *unused1, size_t *unused2, unsigned int *unused3)
{
    NC_FILE_INFO_T *h5;
    NC_GRP_INFO_T *grp;
    NC_VAR_INFO_T *var = NULL;
    int retval;

    ZTRACE(1,"ncid=%d varid=%d",ncid,varid);

    LOG((2, "%s: ncid 0x%x varid %d", __func__, ncid, varid));

    /* Find the file, group, and var info, and do lazy att read if
     * needed. */
    if ((retval = ncz_find_grp_var_att(ncid, varid, NULL, 0, 0, NULL,
					    &h5, &grp, &var, NULL)))
	goto done;
    assert(grp && h5);

    /* Short-circuit the filter-related inquiries */
    if(shufflep) {
	*shufflep = 0;
	if((retval = NCZ_inq_var_filter_info(ncid,varid,H5Z_FILTER_SHUFFLE,NULL,NULL))==NC_NOERR)
	    *shufflep = 1;
    }
    retval = NC_NOERR; /* reset */

    if(fletcher32p) {
	*fletcher32p = 0;
	if((retval = NCZ_inq_var_filter_info(ncid,varid,H5Z_FILTER_FLETCHER32,NULL,NULL))==NC_NOERR)
	    *fletcher32p = 1;
    }
    retval = NC_NOERR; /* reset */

    /* Now that lazy atts have been read, use the libsrc4 function to
     * get the answers. */
    retval = NC4_inq_var_all(ncid, varid, name, xtypep, ndimsp, dimidsp, nattsp,
			   NULL, unused4, unused5, NULL,
			   storagep, chunksizesp, no_fill, fill_valuep,
			   endiannessp, unused1, unused2, unused3);
done:
    return ZUNTRACEX(retval,"xtype=%d natts=%d shuffle=%d fletcher32=%d no_fill=%d endianness=%d ndims=%d dimids=%s storage=%d chunksizes=%s",
	   (xtypep?*xtypep:-1),
   	   (nattsp?*nattsp:-1),
   	   (shufflep?*shufflep:-1),
   	   (fletcher32p?*fletcher32p:-1),
	   (no_fill?*no_fill:-1),
	   (endiannessp?*endiannessp:-1),
   	   (ndimsp?*ndimsp:-1), 
	   (dimidsp?nczprint_idvector(var->ndims,dimidsp):"null"),
	   (storagep?*storagep:-1),
	   (chunksizesp?nczprint_sizevector(var->ndims,chunksizesp):"null"));
}

#ifdef LOOK
/**
 * @internal A wrapper for NCZ_set_var_chunk_cache(), we need this
 * version for fortran. Negative values leave settings as they are.
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param size Size in bytes to set cache.
 * @param nelems Number of elements in cache.
 * @param preemption Controls cache swapping.
 *
 * @returns ::NC_NOERR for success
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
ncz_set_var_chunk_cache_ints(int ncid, int varid, int size, int nelems,
			    int preemption)
{
    size_t real_size = H5D_CHUNK_CACHE_NBYTES_DEFAULT;
    size_t real_nelems = H5D_CHUNK_CACHE_NSLOTS_DEFAULT;
    float real_preemption = CHUNK_CACHE_PREEMPTION;

    if (size >= 0)
	real_size = ((size_t) size) * MEGABYTE;

    if (nelems >= 0)
	real_nelems = nelems;

    if (preemption >= 0)
	real_preemption = preemption / 100.;

    return NCZ_set_var_chunk_cache(ncid, varid, real_size, real_nelems,
					real_preemption);
}
#endif

int
ncz_gettype(NC_FILE_INFO_T* h5, NC_GRP_INFO_T* container, int xtype, NC_TYPE_INFO_T** typep)
{
    int retval = NC_NOERR;
    NC_TYPE_INFO_T* type = NULL;
    NCZ_TYPE_INFO_T* ztype = NULL;

    /* If this is a user-defined type, there is a type struct with
     * all the type information. For atomic types, fake up a type
     * struct. */
    if (xtype <= NC_STRING)
    {
	size_t len;
	char name[NC_MAX_NAME];

	/* Get type name and length. */
	if((retval = NC4_inq_atomic_type(xtype,name,&len)))
	    BAIL(retval);

	/* Create new NC_TYPE_INFO_T struct for this atomic type. */
	if ((retval = nc4_type_new(len, name, xtype, &type)))
	    BAIL(retval);
	assert(type->rc == 0);
	type->container = container;
	type->endianness = (NC_isLittleEndian()?NC_ENDIAN_LITTLE:NC_ENDIAN_BIG);
	type->size = len;

	/* Allocate storage for NCZ-specific type info. */
	if (!(ztype = calloc(1, sizeof(NCZ_TYPE_INFO_T))))
	    return NC_ENOMEM;
	type->format_type_info = ztype;
	ztype->common.file = h5;
	ztype = NULL;

	/* Set the "class" of the type */
	if (xtype == NC_CHAR)
	    type->nc_type_class = NC_CHAR;
	else
	{
	    if(xtype == NC_FLOAT || xtype == NC_DOUBLE)
		type->nc_type_class = NC_FLOAT;
	    else if(xtype < NC_STRING)
		type->nc_type_class = NC_INT;
	    else
		type->nc_type_class = NC_STRING;
	}
    }
    else
    {
#ifdef LOOK
	/* If this is a user defined type, find it. */
	if (nc4_find_type(grp->nc4_info, xtype, &type))
#endif
	    BAIL(NC_EBADTYPE);
    }

    /* increment its ref. count */
    type->rc++;

    if(typep) {*typep = type; type = NULL;}
    return THROW(NC_NOERR);

exit:
    if (type)
	retval = nc4_type_free(type);
    nullfree(ztype);
    return THROW(retval);
}

#if 0
/**
Given start+count+stride+dim vectors, determine the largest
index touched per dimension. If that index is greater-than
the dimension size, then do one of two things:
1. If the dimension is fixed size, then return NC_EDIMSIZE.
2. If the dimension is unlimited, then extend the size of that
   dimension to cover that maximum point.

@param var
@param start vector
@param count vector
@param stride vector
@param reading vs writing
@return NC_EXXX error code
*/
int
NCZ_update_dim_extents(NC_VAR_INFO_T* var, size64_t* start, size64_t* count, size64_t* stride, int reading)
{
    int r;
    int rank = var->ndims;

    NC_UNUSED(reading);

    for(r=0;r<rank;r++) {
	NC_DIM_INFO_T* dim = var->dim[r];
        size64_t endpoint; /* compute last point touched */
	endpoint = start[r] + stride[r]*count[r] - stride[r];
	if(dim->len < endpoint) {
	    if(!dim->unlimited) return NC_EDIMSIZE;
	    /*else*/ dim->len = endpoint+1; 
	}
    }
    return NC_NOERR;
}
#endif
