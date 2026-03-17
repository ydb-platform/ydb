/* Copyright 2003-2019, University Corporation for Atmospheric
 * Research. See COPYRIGHT file for copying and redistribution
 * conditions.*/
/**
 * @file
 * @internal This file handles the HDF5 variable functions.
 *
 * @author Ed Hartnett
 */

#include "config.h"
#include "nc4internal.h"
#include "hdf5internal.h"
#include "hdf5err.h" /* For BAIL2 */
#include <math.h> /* For pow() used below. */

#include "netcdf.h"
#include "netcdf_filter.h"

/** @internal Temp name used when renaming vars to preserve varid
 * order. */
#define NC_TEMP_NAME "_netcdf4_temporary_variable_name_for_rename"

/** Number of bytes in 64 KB. */
#define SIXTY_FOUR_KB (65536)

#ifdef LOGGING
/**
 * Report the chunksizes selected for a variable.
 *
 * @param title A text title for the report.
 * @param var Pointer to the var of interest.
 *
 * @author Dennis Heimbigner
 */
static void
reportchunking(const char *title, NC_VAR_INFO_T *var)
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

/**
 * @internal If the HDF5 dataset for this variable is open, then close
 * it and reopen it, with the perhaps new settings for chunk caching.
 *
 * @param grp Pointer to the group info.
 * @param var Pointer to the var info.
 *
 * @returns ::NC_NOERR No error.
 * @returns ::NC_EHDFERR HDF5 error.
 * @author Ed Hartnett
 */
int
nc4_reopen_dataset(NC_GRP_INFO_T *grp, NC_VAR_INFO_T *var)
{
    NC_HDF5_VAR_INFO_T *hdf5_var;
    hid_t access_pid;
    hid_t grpid;

    assert(var && var->format_var_info && grp && grp->format_grp_info);

    /* Get the HDF5-specific var info. */
    hdf5_var = (NC_HDF5_VAR_INFO_T *)var->format_var_info;

    if (hdf5_var->hdf_datasetid)
    {
        /* Get the HDF5 group id. */
        grpid = ((NC_HDF5_GRP_INFO_T *)(grp->format_grp_info))->hdf_grpid;


        if ((access_pid = H5Pcreate(H5P_DATASET_ACCESS)) < 0)
            return NC_EHDFERR;
        if (H5Pset_chunk_cache(access_pid, var->chunkcache.nelems,
                               var->chunkcache.size,
                               var->chunkcache.preemption) < 0)
            return NC_EHDFERR;
        if (H5Dclose(hdf5_var->hdf_datasetid) < 0)
            return NC_EHDFERR;
        if ((hdf5_var->hdf_datasetid = H5Dopen2(grpid, var->hdr.name, access_pid)) < 0)
            return NC_EHDFERR;
        if (H5Pclose(access_pid) < 0)
            return NC_EHDFERR;
    }

    return NC_NOERR;
}

/**
 * @internal Give a var a secret HDF5 name. This is needed when a var
 * is defined with the same name as a dim, but it is not a coord var
 * of that dim. In that case, the var uses a secret name inside the
 * HDF5 file.
 *
 * @param var Pointer to var info.
 * @param name Name to use for base of secret name.
 *
 * @returns ::NC_NOERR No error.
 * @returns ::NC_EMAXNAME Name too long to fit secret prefix.
 * @returns ::NC_ENOMEM Out of memory.
 * @author Ed Hartnett
 */
static int
give_var_secret_name(NC_VAR_INFO_T *var, const char *name)
{
    /* Set a different hdf5 name for this variable to avoid name
     * clash. */
    if (strlen(name) + strlen(NON_COORD_PREPEND) > NC_MAX_NAME)
        return NC_EMAXNAME;
    size_t alt_name_size = (strlen(NON_COORD_PREPEND) + strlen(name) + 1) *
                           sizeof(char);
    if (!(var->alt_name = malloc(alt_name_size)))
        return NC_ENOMEM;

    snprintf(var->alt_name, alt_name_size, "%s%s", NON_COORD_PREPEND, name);

    return NC_NOERR;
}

/**
 * @internal This is called when a new netCDF-4 variable is defined
 * with nc_def_var().
 *
 * @param ncid File ID.
 * @param name Name.
 * @param xtype Type.
 * @param ndims Number of dims. HDF5 has maximum of 32.
 * @param dimidsp Array of dim IDs.
 * @param varidp Gets the var ID.
 *
 * @returns ::NC_NOERR No error.
 * @returns ::NC_EBADID Bad ncid.
 * @returns ::NC_ENOTVAR Invalid variable ID.
 * @returns ::NC_ENOTNC4 Attempting netcdf-4 operation on file that is
 * not netCDF-4/HDF5.
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
 * @returns ::NC_EHDFERR Error returned by HDF5 layer.
 * @returns ::NC_EINVAL Invalid input
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
NC4_def_var(int ncid, const char *name, nc_type xtype, int ndims,
            const int *dimidsp, int *varidp)
{
    NC_GRP_INFO_T *grp;
    NC_VAR_INFO_T *var;
    NC_DIM_INFO_T *dim;
    NC_FILE_INFO_T *h5;
    NC_TYPE_INFO_T *type = NULL;
    NC_HDF5_TYPE_INFO_T *hdf5_type;
    NC_HDF5_GRP_INFO_T *hdf5_grp;
    NC_HDF5_VAR_INFO_T* hdf5_var;
    char norm_name[NC_MAX_NAME + 1];
    int d;
    int retval;

    /* Find info for this file and group, and set pointer to each. */
    if ((retval = nc4_find_grp_h5(ncid, &grp, &h5)))
        BAIL(retval);
    assert(grp && grp->format_grp_info && h5);

    /* Get HDF5-specific group info. */
    hdf5_grp = (NC_HDF5_GRP_INFO_T *)grp->format_grp_info;

    /* HDF5 allows maximum of 32 dimensions. */
    if (ndims > H5S_MAX_RANK)
        BAIL(NC_EMAXDIMS);

    /* If it's not in define mode, strict nc3 files error out,
     * otherwise switch to define mode. This will also check that the
     * file is writable. */
    if (!(h5->flags & NC_INDEF))
    {
        if (h5->cmode & NC_CLASSIC_MODEL)
            BAIL(NC_ENOTINDEFINE);
        if ((retval = NC4_redef(ncid)))
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
    if (xtype <= NC_STRING)
    {
        size_t len;
	char name[NC_MAX_NAME];

        /* Get type length. */
        if ((retval = nc4_get_typelen_mem(h5, xtype, &len)))
            BAIL(retval);

        /* Create new NC_TYPE_INFO_T struct for this atomic type. */
	if((retval=NC4_inq_atomic_type(xtype,name,NULL)))
	    BAIL(retval);
        if ((retval = nc4_type_new(len, name, xtype, &type)))
            BAIL(retval);
        type->endianness = NC_ENDIAN_NATIVE;
        type->size = len;

        /* Allocate storage for HDF5-specific type info. */
        if (!(hdf5_type = calloc(1, sizeof(NC_HDF5_TYPE_INFO_T))))
            BAIL(NC_ENOMEM);
        type->format_type_info = hdf5_type;

        /* Get HDF5 typeids. */
        if ((retval = nc4_get_hdf_typeid(h5, xtype, &hdf5_type->hdf_typeid,
                                         type->endianness)))
            BAIL(retval);

        /* Get the native HDF5 typeid. */
        if ((hdf5_type->native_hdf_typeid = H5Tget_native_type(hdf5_type->hdf_typeid,
                                                               H5T_DIR_DEFAULT)) < 0)
            BAIL(NC_EHDFERR);

        /* Set the "class" of the type */
        if (xtype == NC_CHAR)
            type->nc_type_class = NC_CHAR;
        else
        {
            H5T_class_t class;

            if ((class = H5Tget_class(hdf5_type->hdf_typeid)) < 0)
                BAIL(NC_EHDFERR);
            switch(class)
            {
            case H5T_STRING:
                type->nc_type_class = NC_STRING;
                break;

            case H5T_INTEGER:
                type->nc_type_class = NC_INT;
                break;

            case H5T_FLOAT:
                type->nc_type_class = NC_FLOAT;
                break;

            default:
                BAIL(NC_EBADTYPID);
            }
        }
    }
    else
    {
        /* If this is a user defined type, find it. */
        if (nc4_find_type(grp->nc4_info, xtype, &type))
            BAIL(NC_EBADTYPE);
    }

    /* Create a new var and fill in some HDF5 cache setting values. */
    if ((retval = nc4_var_list_add(grp, norm_name, ndims, &var)))
        BAIL(retval);

    /* Add storage for HDF5-specific var info. */
    if (!(var->format_var_info = calloc(1, sizeof(NC_HDF5_VAR_INFO_T))))
        BAIL(NC_ENOMEM);

    hdf5_var = (NC_HDF5_VAR_INFO_T*)var->format_var_info;

    /* Set these state flags for the var. */
    var->is_new_var = NC_TRUE;
    var->meta_read = NC_TRUE;
    var->atts_read = NC_TRUE;

    /* Create filter list */
    var->filters = (void*)nclistnew();

    /* Point to the type, and increment its ref. count */
    var->type_info = type;
    var->type_info->rc++;
    type = NULL;

    /* Propagate the endianness */
    var->endianness = var->type_info->endianness;

    /* Set variables no_fill to match the database default unless the
     * variable type is variable length (NC_STRING or NC_VLEN) or is
     * user-defined type. */
    if (var->type_info->nc_type_class < NC_STRING)
        var->no_fill = h5->fill_mode;

    /* Assign dimensions to the variable. At the same time, check to
     * see if this is a coordinate variable. If so, it will have the
     * same name as one of its dimensions. If it is a coordinate var,
     * is it a coordinate var in the same group as the dim? Also, check
     * whether we should use contiguous or chunked storage. */
    var->storage = NC_CONTIGUOUS;
    for (d = 0; d < ndims; d++)
    {
        NC_GRP_INFO_T *dim_grp;
        NC_HDF5_DIM_INFO_T *hdf5_dim;

        /* Look up each dimension */
        if ((retval = nc4_find_dim(grp, dimidsp[d], &dim, &dim_grp)))
            BAIL(retval);
        assert(dim && dim->format_dim_info);
        hdf5_dim = (NC_HDF5_DIM_INFO_T *)dim->format_dim_info;

        /* Check for dim index 0 having the same name, in the same group */
        if (d == 0 && dim_grp == grp && strcmp(dim->hdr.name, norm_name) == 0)
        {
            hdf5_var->dimscale = NC_TRUE;
            dim->coord_var = var;

            /* Use variable's dataset ID for the dimscale ID. So delete
             * the HDF5 DIM_WITHOUT_VARIABLE dataset that was created for
             * this dim. */
            if (hdf5_dim->hdf_dimscaleid)
            {
                /* Detach dimscale from any variables using it */
                if ((retval = rec_detach_scales(grp, dimidsp[d],
                                                hdf5_dim->hdf_dimscaleid)) < 0)
                    BAIL(retval);

                /* Close the HDF5 DIM_WITHOUT_VARIABLE dataset. */
                if (H5Dclose(hdf5_dim->hdf_dimscaleid) < 0)
                    BAIL(NC_EHDFERR);
                hdf5_dim->hdf_dimscaleid = 0;

                /* Now delete the DIM_WITHOUT_VARIABLE dataset (it will be
                 * recreated later, if necessary). */
                if (H5Gunlink(hdf5_grp->hdf_grpid, dim->hdr.name) < 0)
                    BAIL(NC_EDIMMETA);
            }
        }

        /* Check for unlimited dimension. If present, we must use
         * chunked storage. */
        if (dim->unlimited)
        {
            var->storage = NC_CHUNKED;
        }

        /* Track dimensions for variable */
        var->dimids[d] = dimidsp[d];
        var->dim[d] = dim;
    }

    /* Determine default chunksizes for this variable. (Even for
     * variables which may be contiguous.) */
    LOG((4, "allocating array of %d size_t to hold chunksizes for var %s",
         var->ndims, var->hdr.name));
    if (var->ndims) {
        if (!(var->chunksizes = calloc(var->ndims, sizeof(size_t))))
            BAIL(NC_ENOMEM);
        if ((retval = nc4_find_default_chunksizes2(grp, var)))
            BAIL(retval);
        /* Is this a variable with a chunksize greater than the current
         * cache size? */
        if ((retval = nc4_adjust_var_cache(grp, var)))
            BAIL(retval);
    }

    /* If the user names this variable the same as a dimension, but
     * doesn't use that dimension first in its list of dimension ids,
     * is not a coordinate variable. I need to change its HDF5 name,
     * because the dimension will cause a HDF5 dataset to be created,
     * and this var has the same name. */
    dim = (NC_DIM_INFO_T*)ncindexlookup(grp->dim,norm_name);
    if (dim && (!var->ndims || dimidsp[0] != dim->hdr.id))
        if ((retval = give_var_secret_name(var, var->hdr.name)))
            BAIL(retval);

    /* If this is a coordinate var, it is marked as a HDF5 dimension
     * scale. (We found dim above.) Otherwise, allocate space to
     * remember whether dimension scales have been attached to each
     * dimension. */
    if (!hdf5_var->dimscale && ndims)
        if (!(hdf5_var->dimscale_attached = calloc((size_t)ndims, sizeof(nc_bool_t))))
            BAIL(NC_ENOMEM);

    /* Return the varid. */
    if (varidp)
        *varidp = var->hdr.id;
    LOG((4, "new varid %d", var->hdr.id));

exit:
    if (type)
        if ((retval = nc4_type_free(type)))
            BAIL2(retval);

    return retval;
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
 * @param storage Pointer to storage setting.
 * @param chunksizes Array of chunksizes.
 * @param no_fill Pointer to no_fill setting.
 * @param fill_value Pointer to fill value.
 * @param endianness Pointer to endianness setting.
 * @param quantize_mode Pointer to quantization mode.
 * @param nsd Pointer to number of significant digits.
 *
 * @returns ::NC_NOERR for success
 * @returns ::NC_EBADID Bad ncid.
 * @returns ::NC_ENOTVAR Invalid variable ID.
 * @returns ::NC_ENOTNC4 Attempting netcdf-4 operation on file that is
 * not netCDF-4/HDF5.
 * @returns ::NC_ESTRICTNC3 Attempting netcdf-4 operation on strict nc3
 * netcdf-4 file.
 * @returns ::NC_ELATEDEF Too late to change settings for this variable.
 * @returns ::NC_ENOTINDEFINE Not in define mode.
 * @returns ::NC_EPERM File is read only.
 * @returns ::NC_EINVAL Invalid input
 * @returns ::NC_EBADCHUNK Bad chunksize.
 * @author Ed Hartnett
 */
static int
nc_def_var_extra(int ncid, int varid, int *shuffle, int *unused1,
                 int *unused2, int *fletcher32, int *storage,
                 const size_t *chunksizes, int *no_fill,
                 const void *fill_value, int *endianness,
		 int *quantize_mode, int *nsd)
{
    NC_GRP_INFO_T *grp;
    NC_FILE_INFO_T *h5;
    NC_VAR_INFO_T *var;
    int d;
    int retval;

    LOG((2, "%s: ncid 0x%x varid %d", __func__, ncid, varid));

    /* Find info for this file and group, and set pointer to each. */
    if ((retval = nc4_find_nc_grp_h5(ncid, NULL, &grp, &h5)))
        return retval;
    assert(grp && h5);

    /* Trying to write to a read-only file? No way, Jose! */
    if (h5->no_write)
        return NC_EPERM;

    /* Find the var. */
    if (!(var = (NC_VAR_INFO_T *)ncindexith(grp->vars, (size_t)varid)))
        return NC_ENOTVAR;
    assert(var && var->hdr.id == varid);

    /* Can't turn on parallel and deflate/fletcher32/szip/shuffle
     * before HDF5 1.10.3. */
#ifndef HDF5_SUPPORTS_PAR_FILTERS
    if (h5->parallel == NC_TRUE)
        if (nclistlength((NClist*)var->filters) > 0  || fletcher32 || shuffle)
            return NC_EINVAL;
#endif

    /* If the HDF5 dataset has already been created, then it is too
     * late to set all the extra stuff. */
    if (var->created)
        return NC_ELATEDEF;

    /* Cannot set filters of any sort on scalars */
    if(var->ndims == 0) {
        if(shuffle && *shuffle)
            return NC_EINVAL;
        if(fletcher32 && *fletcher32)
            return NC_EINVAL;
    }

    /* Shuffle filter? */
    if (shuffle && *shuffle) {
	    retval = nc_inq_var_filter_info(ncid,varid,H5Z_FILTER_SHUFFLE,NULL,NULL);
	    if(!retval || retval == NC_ENOFILTER) {
	        if((retval = nc_def_var_filter(ncid,varid,H5Z_FILTER_SHUFFLE,0,NULL))) return retval;
                var->storage = NC_CHUNKED;
	    }
    }

    /* Fletcher32 checksum error protection? */
    if (fletcher32 && *fletcher32) {
	retval = nc_inq_var_filter_info(ncid,varid,H5Z_FILTER_FLETCHER32,NULL,NULL);
	if(!retval || retval == NC_ENOFILTER) {
	    if((retval = nc_def_var_filter(ncid,varid,H5Z_FILTER_FLETCHER32,0,NULL))) return retval;
            var->storage = NC_CHUNKED;
	    }
    }

#ifdef USE_PARALLEL
    /* If filter is being applied with
     * parallel I/O writes, then switch to collective access. HDF5
     * requires collevtive access for filter use with parallel
     * I/O. */
    if (h5->parallel && (nclistlength((NClist*)var->filters) > 0))
            var->parallel_access = NC_COLLECTIVE;
#endif /* USE_PARALLEL */

    /* Handle storage settings. */
    if (storage)
    {
        /* Does the user want a contiguous or compact dataset? Not so
         * fast! Make sure that there are no unlimited dimensions, and
         * no filters in use for this data. */
        if (*storage != NC_CHUNKED)
        {
            if (nclistlength(((NClist*)var->filters)) > 0)
                return NC_EINVAL;
	    for (d = 0; d < var->ndims; d++)
                if (var->dim[d]->unlimited)
                    return NC_EINVAL;
        }

        /* Handle chunked storage settings. */
        if (*storage == NC_CHUNKED && var->ndims == 0)
        {
            /* Chunked not allowed for scalar vars. */
            return NC_EINVAL;
        }
        else if (*storage == NC_CHUNKED)
        {
            var->storage = NC_CHUNKED;

            /* If the user provided chunksizes, check that they are not too
             * big, and that their total size of chunk is less than 4 GB. */
            if (chunksizes)
            {
                /* Check the chunksizes for validity. */
                if ((retval = nc4_check_chunksizes(grp, var, chunksizes)))
                    return retval;

                /* Ensure chunksize is smaller than dimension size */
                for (d = 0; d < var->ndims; d++)
                    if (!var->dim[d]->unlimited && var->dim[d]->len > 0 &&
                        chunksizes[d] > var->dim[d]->len)
                        return NC_EBADCHUNK;

                /* Set the chunksizes for this variable. */
                for (d = 0; d < var->ndims; d++)
                    var->chunksizes[d] = chunksizes[d];
            }
        }
        else if (*storage == NC_CONTIGUOUS)
        {
            var->storage = NC_CONTIGUOUS;
        }
        else if (*storage == NC_COMPACT)
        {
            size_t ndata = 1;

            /* Find the number of elements in the data. */
            for (d = 0; d < var->ndims; d++)
                ndata *= var->dim[d]->len;

            /* Ensure var is small enough to fit in compact
             * storage. It must be <= 64 KB. */
            if (ndata * var->type_info->size > SIXTY_FOUR_KB)
                return NC_EVARSIZE;

            var->storage = NC_COMPACT;
        }
    }

    /* Is this a variable with a chunksize greater than the current
     * cache size? */
    if (var->storage == NC_CHUNKED)
    {
        /* Determine default chunksizes for this variable (do nothing
         * for scalar vars). */
        if (var->chunksizes == NULL || var->chunksizes[0] == 0)
            if ((retval = nc4_find_default_chunksizes2(grp, var)))
                return retval;

        /* Adjust the cache. */
        if ((retval = nc4_adjust_var_cache(grp, var)))
            return retval;
    }

#ifdef LOGGING
    {
        int dfalt = (chunksizes == NULL);
        reportchunking(dfalt ? "extra: default: " : "extra: user: ", var);
    }
#endif

    /* Are we setting a fill modes? */
    if (no_fill)
    {
        if (*no_fill)
        {
            /* NC_STRING types may not turn off fill mode. It's disallowed
             * by HDF5 and will cause a HDF5 error later. */
            if (*no_fill)
                if (var->type_info->hdr.id == NC_STRING)
                    return NC_EINVAL;

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
        retval = NC4_HDF5_del_att(ncid, varid, NC_FillValue);
        if (retval && retval != NC_ENOTATT)
            return retval;

        /* Create a _FillValue attribute; will also fill in var->fill_value */
        if ((retval = nc_put_att(ncid, varid, NC_FillValue, var->type_info->hdr.id,
                                 1, fill_value)))
            return retval;
    } else if (var->fill_value && no_fill && (*no_fill)) { /* Turning off fill value? */
        /* If there's a _FillValue attribute, delete it. */
        retval = NC4_HDF5_del_att(ncid, varid, NC_FillValue);
        if (retval && retval != NC_ENOTATT) return retval;
	if((retval = NC_reclaim_data_all(h5->controller,var->type_info->hdr.id,var->fill_value,1))) return retval;
	var->fill_value = NULL;
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
            return NC_EINVAL;
        }
        var->type_info->endianness = *endianness;
	/* Propagate */
	var->endianness = *endianness;
    }

    /* Remember quantization settings. They will be used when data are
     * written.
     * Code block is identical to one in zvar.c---consider functionalizing */
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

    return NC_NOERR;
}

/**
 * @internal Set zlib compression settings on a variable. This is
 * called by nc_def_var_deflate().
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
 * not netCDF-4/HDF5.
 * @returns ::NC_ELATEDEF Too late to change settings for this variable.
 * @returns ::NC_ENOTINDEFINE Not in define mode.
 * @returns ::NC_EINVAL Invalid input
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
NC4_def_var_deflate(int ncid, int varid, int shuffle, int deflate,
                    int deflate_level)
{
    int stat;
    unsigned int level = (unsigned int)deflate_level;

    /* Set shuffle first */
    if ((stat = nc_def_var_extra(ncid, varid, &shuffle, NULL, NULL, NULL,
                                 NULL, NULL, NULL, NULL, NULL, NULL, NULL)))
        return stat;

    /* Don't turn on deflate if deflate_level = 0. It's a valid zlib
     * setting, but results in a write slowdown, and a file that is
     * larger than the uncompressed file would be. So when
     * deflate_level is 0, don't use compression. */
    if (deflate && deflate_level)
        if ((stat = nc_def_var_filter(ncid, varid, H5Z_FILTER_DEFLATE, 1, &level)))
            return stat;

    return NC_NOERR;
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
 * and compression such as DEFLATE constitute lossy compression.
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
NC4_def_var_quantize(int ncid, int varid, int quantize_mode, int nsd)
{
    return nc_def_var_extra(ncid, varid, NULL, NULL, NULL, NULL,
			    NULL, NULL, NULL, NULL, NULL,
			    &quantize_mode, &nsd);
}

/**
 * @internal Get quantize information about a variable. Pass NULL for
 * whatever you don't care about.
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
NC4_inq_var_quantize(int ncid, int varid, int *quantize_modep,
		     int *nsdp)
{
    NC_VAR_INFO_T *var;
    int retval;

    LOG((2, "%s: ncid 0x%x varid %d", __func__, ncid, varid));

    /* Find info for this file and group, and set pointer to each. */
    /* Get pointer to the var. */
    if ((retval = nc4_hdf5_find_grp_h5_var(ncid, varid, NULL, NULL, &var)))
        return retval;
    if (!var)
        return NC_ENOTVAR;	
    assert(var->hdr.id == varid);

    /* Copy the data to the user's data buffers. */
    if (quantize_modep)
        *quantize_modep = var->quantize_mode;
    if (nsdp)
        *nsdp = var->nsd;

    return 0;
}

#if 0
/**
 * @internal Remove a filter from filter list for a variable
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param id filter id to remove
 *
 * @returns ::NC_NOERR No error.
 * @returns ::NC_EBADID Bad ncid.
 * @returns ::NC_ENOTVAR Invalid variable ID.
 * @returns ::NC_ENOTNC4 Attempting netcdf-4 operation on file that is
 * not netCDF-4/HDF5.
 * @returns ::NC_ELATEDEF Too late to change settings for this variable.
 * @returns ::NC_ENOTINDEFINE Not in define mode.
 * @returns ::NC_EINVAL Invalid input
 * @author Dennis Heimbigner
 */
int
nc_var_filter_remove(int ncid, int varid, unsigned int filterid)
{
    NC_VAR_INFO_T *var = NULL;
    int stat;

    /* Get pointer to the var. */
    if ((stat = nc4_hdf5_find_grp_h5_var(ncid, varid, NULL, NULL, &var)))
        return stat;
    assert(var);

    stat = NC4_hdf5_filter_remove(var,filterid);

    return stat;
}
#endif

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
 * not netCDF-4/HDF5.
 * @returns ::NC_ELATEDEF Too late to change settings for this variable.
 * @returns ::NC_ENOTINDEFINE Not in define mode.
 * @returns ::NC_EINVAL Invalid input
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
NC4_def_var_fletcher32(int ncid, int varid, int fletcher32)
{
    return nc_def_var_extra(ncid, varid, NULL, NULL, NULL, &fletcher32,
                            NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}

/**
 * @internal Define chunking stuff for a var. This is called by
 * nc_def_var_chunking(). Chunking is required in any dataset with one
 * or more unlimited dimensions in HDF5, or any dataset using a
 * filter.
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param storage Pointer to storage setting.
 * @param chunksizesp Array of chunksizes.
 *
 * @returns ::NC_NOERR No error.
 * @returns ::NC_EBADID Bad ncid.
 * @returns ::NC_ENOTVAR Invalid variable ID.
 * @returns ::NC_ENOTNC4 Attempting netcdf-4 operation on file that is
 * not netCDF-4/HDF5.
 * @returns ::NC_ELATEDEF Too late to change settings for this variable.
 * @returns ::NC_ENOTINDEFINE Not in define mode.
 * @returns ::NC_EINVAL Invalid input
 * @returns ::NC_EBADCHUNK Bad chunksize.
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
NC4_def_var_chunking(int ncid, int varid, int storage, const size_t *chunksizesp)
{
    return nc_def_var_extra(ncid, varid, NULL, NULL, NULL, NULL,
                            &storage, chunksizesp, NULL, NULL, NULL, NULL, NULL);
}

/**
 * @internal Define chunking stuff for a var. This is called by
 * the fortran API.
 * Note: see libsrc4/nc4cache.c for definition when HDF5 is disabled
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param storage Pointer to storage setting.
 * @param chunksizesp Array of chunksizes.
 *
 * @returns ::NC_NOERR No error.
 * @returns ::NC_EBADID Bad ncid.
 * @returns ::NC_ENOTVAR Invalid variable ID.
 * @returns ::NC_ENOTNC4 Attempting netcdf-4 operation on file that is
 * not netCDF-4/HDF5.
 * @returns ::NC_ELATEDEF Too late to change settings for this variable.
 * @returns ::NC_ENOTINDEFINE Not in define mode.
 * @returns ::NC_EINVAL Invalid input
 * @returns ::NC_EBADCHUNK Bad chunksize.
 * @author Ed Hartnett
 */
int
nc_def_var_chunking_ints(int ncid, int varid, int storage, int *chunksizesp)
{
    NC_VAR_INFO_T *var = NULL;
    size_t *cs = NULL;
    int i, retval;

    /* Get pointer to the var. */
    if ((retval = nc4_hdf5_find_grp_h5_var(ncid, varid, NULL, NULL, &var)))
        return retval;
    assert(var);

    /* Allocate space for the size_t copy of the chunksizes array. */
    if (var->ndims)
        if (!(cs = malloc(var->ndims * sizeof(size_t))))
            return NC_ENOMEM;

    /* Copy to size_t array. */
    for (i = 0; i < var->ndims; i++)
        cs[i] = (size_t)chunksizesp[i];

    retval = nc_def_var_extra(ncid, varid, NULL, NULL, NULL, NULL,
                              &storage, cs, NULL, NULL, NULL, NULL, NULL);

    if (var->ndims)
        free(cs);
    return retval;
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
 * not netCDF-4/HDF5.
 * @returns ::NC_ESTRICTNC3 Attempting netcdf-4 operation on strict nc3
 * netcdf-4 file.
 * @returns ::NC_ELATEDEF Too late to change settings for this variable.
 * @returns ::NC_ENOTINDEFINE Not in define mode.
 * @returns ::NC_EPERM File is read only.
 * @returns ::NC_EINVAL Invalid input
 * @author Ed Hartnett
 */
int
NC4_def_var_fill(int ncid, int varid, int no_fill, const void *fill_value)
{
    return nc_def_var_extra(ncid, varid, NULL, NULL, NULL, NULL, NULL,
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
 * not netCDF-4/HDF5.
 * @returns ::NC_ESTRICTNC3 Attempting netcdf-4 operation on strict nc3
 * netcdf-4 file.
 * @returns ::NC_ELATEDEF Too late to change settings for this variable.
 * @returns ::NC_ENOTINDEFINE Not in define mode.
 * @returns ::NC_EPERM File is read only.
 * @returns ::NC_EINVAL Invalid input
 * @author Ed Hartnett
 */
int
NC4_def_var_endian(int ncid, int varid, int endianness)
{
    return nc_def_var_extra(ncid, varid, NULL, NULL, NULL, NULL, NULL,
                            NULL, NULL, NULL, &endianness, NULL, NULL);
}

/**
 * @internal Rename a var to "bubba," for example. This is called by
 * nc_rename_var() for netCDF-4 files. This results in complexities
 * when coordinate variables are involved.

 * Whenever a var has the same name as a dim, and also uses that dim
 * as its first dimension, then that var is aid to be a coordinate
 * variable for that dimensions. Coordinate variables are represented
 * in the HDF5 by making them dimscales. Dimensions without coordinate
 * vars are represented by datasets which are dimscales, but have a
 * special attribute marking them as dimscales without associated
 * coordinate variables.
 *
 * When a var is renamed, we must detect whether it has become a
 * coordinate var (by being renamed to the same name as a dim that is
 * also its first dimension), or whether it is no longer a coordinate
 * var. These cause flags to be set in NC_VAR_INFO_T which are used at
 * enddef time to make changes in the HDF5 file.
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
 * @author Ed Hartnett
 */
int
NC4_rename_var(int ncid, int varid, const char *name)
{
    NC_GRP_INFO_T *grp;
    NC_HDF5_GRP_INFO_T *hdf5_grp;
    NC_FILE_INFO_T *h5;
    NC_VAR_INFO_T *var;
    NC_HDF5_VAR_INFO_T *hdf5_var;
    NC_DIM_INFO_T *other_dim;
    int use_secret_name = 0;
    int retval = NC_NOERR;

    if (!name)
        return NC_EINVAL;

    LOG((2, "%s: ncid 0x%x varid %d name %s", __func__, ncid, varid, name));

    /* Find info for this file and group, and set pointer to each. */
    if ((retval = nc4_find_grp_h5(ncid, &grp, &h5)))
        return retval;
    assert(h5 && grp && grp->format_grp_info);

    /* Get HDF5-specific group info. */
    hdf5_grp = (NC_HDF5_GRP_INFO_T *)grp->format_grp_info;

    /* Is the new name too long? */
    if (strlen(name) > NC_MAX_NAME)
        return NC_EMAXNAME;

    /* Trying to write to a read-only file? No way, Jose! */
    if (h5->no_write)
        return NC_EPERM;

    /* Check name validity, if strict nc3 rules are in effect for this
     * file. */
    if ((retval = NC_check_name(name)))
        return retval;

    /* Get the variable wrt varid */
    if (!(var = (NC_VAR_INFO_T *)ncindexith(grp->vars, (size_t)varid)))
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

    /* Is there another dim with this name, for which this var will not
     * be a coord var? If so, we have to create a dim without a
     * variable for the old name. */
    if ((other_dim = (NC_DIM_INFO_T *)ncindexlookup(grp->dim, name)) &&
        strcmp(name, var->dim[0]->hdr.name))
    {
        /* Create a dim without var dataset for old dim. */
        if ((retval = nc4_create_dim_wo_var(other_dim)))
            return retval;

        /* Give this var a secret HDF5 name so it can co-exist in file
         * with dim wp var dataset. Base the secret name on the new var
         * name. */
        if ((retval = give_var_secret_name(var, name)))
            return retval;
        use_secret_name++;
    }

    hdf5_var = (NC_HDF5_VAR_INFO_T*)var->format_var_info;
    assert(hdf5_var != NULL);

    /* Change the HDF5 file, if this var has already been created
       there. */
    if (var->created)
    {
        char *hdf5_name; /* Dataset will be renamed to this. */
        hdf5_name = use_secret_name ? var->alt_name: (char *)name;

        /* Do we need to read var metadata? */
        if (!var->meta_read)
            if ((retval = nc4_get_var_meta(var)))
                return retval;

        if (var->ndims)
        {
            NC_HDF5_DIM_INFO_T *hdf5_d0;
            hdf5_d0 = (NC_HDF5_DIM_INFO_T *)var->dim[0]->format_dim_info;

            /* Is there an existing dimscale-only dataset of this name? If
             * so, it must be deleted. */
            if (hdf5_d0->hdf_dimscaleid)
            {
                if ((retval = delete_dimscale_dataset(grp, var->dim[0]->hdr.id,
                                                      var->dim[0])))
                    return retval;
            }
        }

        LOG((3, "Moving dataset %s to %s", var->hdr.name, name));
        if (H5Lmove(hdf5_grp->hdf_grpid, var->hdr.name, hdf5_grp->hdf_grpid,
                    hdf5_name, H5P_DEFAULT, H5P_DEFAULT) < 0)
            return NC_EHDFERR;

        /* Rename all the vars in this file with a varid greater than
         * this var. Varids are assigned based on dataset creation time,
         * and we have just changed that for this var. We must do the
         * same for all vars with a > varid, so that the creation order
         * will continue to be correct. */
        for (size_t v = (size_t)var->hdr.id + 1; v < ncindexsize(grp->vars); v++)
        {
            NC_VAR_INFO_T *my_var;
            my_var = (NC_VAR_INFO_T *)ncindexith(grp->vars, v);
            assert(my_var);

            LOG((3, "mandatory rename of %s to same name", my_var->hdr.name));

            /* Rename to temp name. */
            if (H5Lmove(hdf5_grp->hdf_grpid, my_var->hdr.name, hdf5_grp->hdf_grpid,
                        NC_TEMP_NAME, H5P_DEFAULT, H5P_DEFAULT) < 0)
                return NC_EHDFERR;

            /* Rename to real name. */
            if (H5Lmove(hdf5_grp->hdf_grpid, NC_TEMP_NAME, hdf5_grp->hdf_grpid,
                        my_var->hdr.name, H5P_DEFAULT, H5P_DEFAULT) < 0)
                return NC_EHDFERR;
        }
    }

    /* Now change the name in our metadata. */
    free(var->hdr.name);
    if (!(var->hdr.name = strdup(name)))
        return NC_ENOMEM;
    LOG((3, "var is now %s", var->hdr.name));

    /* rebuild index. */
    if (!ncindexrebuild(grp->vars))
        return NC_EINTERNAL;

    /* Check if this was a coordinate variable previously, but names
     * are different now */
    if (hdf5_var->dimscale && strcmp(var->hdr.name, var->dim[0]->hdr.name))
    {
        /* Break up the coordinate variable */
        if ((retval = nc4_break_coord_var(grp, var, var->dim[0])))
            return retval;
    }

    /* Check if this should become a coordinate variable. */
    if (!hdf5_var->dimscale)
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
            if ((retval = nc4_find_dim(grp, var->dimids[0], &dim, &dim_grp)))
                return retval;
            if (!strcmp(dim->hdr.name, name) && dim_grp == grp)
            {
                /* Reform the coordinate variable. */
                if ((retval = nc4_reform_coord_var(grp, var, dim)))
                    return retval;
                var->became_coord_var = NC_TRUE;
            }
        }
    }

    return retval;
}

/**
 * @internal Write an array of data to a variable. This is called by
 * nc_put_vara() and other nc_put_vara_* functions, for netCDF-4
 * files.
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param startp Array of start indices. This array must be same size
 * as variable's number of dimensions.
 * @param countp Array of counts. This array must be same size as
 * variable's number of dimensions.
 * @param op pointer that gets the data.
 * @param memtype The type of these data in memory.
 *
 * @returns ::NC_NOERR for success
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
NC4_put_vara(int ncid, int varid, const size_t *startp,
             const size_t *countp, const void *op, int memtype)
{
    return NC4_put_vars(ncid, varid, startp, countp, NULL, op, memtype);
}

/**
 * @internal Read an array of values. This is called by nc_get_vara()
 * for netCDF-4 files, as well as all the other nc_get_vara_*
 * functions.
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param startp Array of start indices. This array must be same size
 * as variable's number of dimensions.
 * @param countp Array of counts.
 * @param ip pointer that gets the data.
 * @param memtype The type of these data after it is read into memory.
 *
 * @returns ::NC_NOERR for success
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
NC4_get_vara(int ncid, int varid, const size_t *startp,
             const size_t *countp, void *ip, int memtype)
{
    return NC4_get_vars(ncid, varid, startp, countp, NULL, ip, memtype);
}

/**
 * @internal Do some common check for NC4_put_vars and
 * NC4_get_vars. These checks have to be done when both reading and
 * writing data.
 *
 * @param mem_nc_type Pointer to type of data in memory.
 * @param var Pointer to var info struct.
 * @param h5 Pointer to HDF5 file info struct.
 *
 * @return ::NC_NOERR No error.
 * @author Ed Hartnett
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
        if ((retval = nc4_enddef_netcdf4_file(h5)))
            return retval;
    }

    return NC_NOERR;
}

#ifdef LOGGING
/**
 * @intarnal Print some debug info about dimensions to the log.
 */
static void
log_dim_info(NC_VAR_INFO_T *var, hsize_t *fdims, hsize_t *fmaxdims,
             hsize_t *start, hsize_t *count)
{
    int d2;

    /* Print some debugging info... */
    LOG((4, "%s: var name %s ndims %d", __func__, var->hdr.name, var->ndims));
    LOG((4, "File space, and requested:"));
    for (d2 = 0; d2 < var->ndims; d2++)
    {
        LOG((4, "fdims[%d]=%Ld fmaxdims[%d]=%Ld", d2, fdims[d2], d2,
             fmaxdims[d2]));
        LOG((4, "start[%d]=%Ld  count[%d]=%Ld", d2, start[d2], d2, count[d2]));
    }
}
#endif /* LOGGING */

#ifdef USE_PARALLEL4
/**
 * @internal Set the parallel access for a var (collective
 * vs. independent).
 *
 * @param h5 Pointer to HDF5 file info struct.
 * @param var Pointer to var info struct.
 * @param xfer_plistid H5FD_MPIO_COLLECTIVE or H5FD_MPIO_INDEPENDENT.
 *
 * @returns NC_NOERR No error.
 * @author Ed Hartnett
 */
static int
set_par_access(NC_FILE_INFO_T *h5, NC_VAR_INFO_T *var, hid_t xfer_plistid)
{
    /* If netcdf is built with parallel I/O, then parallel access can
     * be used, and, if this file was opened or created for parallel
     * access, we need to set the transfer mode. */
    if (h5->parallel)
    {
        H5FD_mpio_xfer_t hdf5_xfer_mode;

        /* Decide on collective or independent. */
        hdf5_xfer_mode = (var->parallel_access != NC_INDEPENDENT) ?
            H5FD_MPIO_COLLECTIVE : H5FD_MPIO_INDEPENDENT;

        /* Set the mode in the transfer property list. */
        if (H5Pset_dxpl_mpio(xfer_plistid, hdf5_xfer_mode) < 0)
            return NC_EPARINIT;

        LOG((4, "%s: %d H5FD_MPIO_COLLECTIVE: %d H5FD_MPIO_INDEPENDENT: %d",
             __func__, (int)hdf5_xfer_mode, H5FD_MPIO_COLLECTIVE,
             H5FD_MPIO_INDEPENDENT));
    }
    return NC_NOERR;
}
#endif /* USE_PARALLEL4 */

/**
 * @internal Write a strided array of data to a variable. This is
 * called by nc_put_vars() and other nc_put_vars_* functions, for
 * netCDF-4 files. Also the nc_put_vara() calls end up calling this
 * with a NULL stride parameter.
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param startp Array of start indices. Must always be provided by
 * caller for non-scalar vars. This array must be same size
 * as variable's number of dimensions.
 * @param countp Array of counts. Will default to counts of full
 * dimension size if NULL. This array must be same size
 * as variable's number of dimensions.
 * @param stridep Array of strides. Will default to strides of 1 if
 * NULL. This array must be same size as variable's number of dimensions.
 * @param data The data to be written.
 * @param mem_nc_type The type of the data in memory.
 *
 * @returns ::NC_NOERR No error.
 * @returns ::NC_EBADID Bad ncid.
 * @returns ::NC_ENOTVAR Var not found.
 * @returns ::NC_EHDFERR HDF5 function returned error.
 * @returns ::NC_EINVALCOORDS Incorrect start.
 * @returns ::NC_EEDGE Incorrect start/count.
 * @returns ::NC_ENOMEM Out of memory.
 * @returns ::NC_EMPI MPI library error (parallel only)
 * @returns ::NC_ECANTEXTEND Can't extend dimension for write.
 * @returns ::NC_ERANGE Data conversion error.
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
NC4_put_vars(int ncid, int varid, const size_t *startp, const size_t *countp,
             const ptrdiff_t *stridep, const void *data, nc_type mem_nc_type)
{
    NC_GRP_INFO_T *grp;
    NC_FILE_INFO_T *h5;
    NC_VAR_INFO_T *var;
    NC_DIM_INFO_T *dim;
    NC_HDF5_VAR_INFO_T *hdf5_var;
    herr_t herr;
    hid_t file_spaceid = 0, mem_spaceid = 0, xfer_plistid = 0;
    long long unsigned xtend_size[NC_MAX_VAR_DIMS];
    hsize_t fdims[NC_MAX_VAR_DIMS], fmaxdims[NC_MAX_VAR_DIMS];
    hsize_t start[NC_MAX_VAR_DIMS], count[NC_MAX_VAR_DIMS];
    hsize_t stride[NC_MAX_VAR_DIMS], ones[NC_MAX_VAR_DIMS];
    int need_to_extend = 0;
#ifdef USE_PARALLEL4
    int extend_possible = 0;
#endif
    int retval, range_error = 0, i, d2;
    void *bufr = NULL;
    int need_to_convert = 0;
    int zero_count = 0; /* true if a count is zero */
    size_t len = 1;

    /* Find info for this file, group, and var. */
    if ((retval = nc4_hdf5_find_grp_h5_var(ncid, varid, &h5, &grp, &var)))
        return retval;
    assert(h5 && grp && var && var->hdr.id == varid && var->format_var_info);

    /* Get the HDF5-specific var info. */
    hdf5_var = (NC_HDF5_VAR_INFO_T *)var->format_var_info;

    /* Cannot convert to user-defined types. */
    if (mem_nc_type >= NC_FIRSTUSERTYPEID)
        mem_nc_type = NC_NAT;

    LOG((3, "%s: var->hdr.name %s mem_nc_type %d", __func__,
         var->hdr.name, mem_nc_type));

    /* Check some stuff about the type and the file. If the file must
     * be switched from define mode, it happens here. */
    if ((retval = check_for_vara(&mem_nc_type, var, h5)))
        return retval;
    assert(hdf5_var->hdf_datasetid && (!var->ndims || (startp && countp)));

    /* Verify that all the variable's filters are available */
    if(hdf5_var->flags & NC_HDF5_VAR_FILTER_MISSING) {
	unsigned id = 0;
	NC4_hdf5_find_missing_filter(var, &id);
	LOG((0,"missing filter: variable=%s id=%u",var->hdr.name,id));
        return NC_ENOFILTER;
    }

    /* Convert from size_t and ptrdiff_t to hssize_t, and hsize_t. */
    /* Also do sanity checks */
    for (i = 0; i < var->ndims; i++)
    {
        /* Check for non-positive stride. */
        if (stridep && stridep[i] <= 0)
            return NC_ESTRIDE;

        start[i] = startp[i];
        count[i] = countp ? countp[i] : var->dim[i]->len;
        stride[i] = stridep ? (hsize_t)stridep[i] : 1;
        ones[i] = 1;
	LOG((4, "start[%d] %ld count[%d] %ld stride[%d] %ld", i, start[i], i, count[i], i, stride[i]));

        /* Check to see if any counts are zero. */
        if (!count[i])
            zero_count++;
    }

    /* Get file space of data. */
    if ((file_spaceid = H5Dget_space(hdf5_var->hdf_datasetid)) < 0)
        BAIL(NC_EHDFERR);

    /* Get the sizes of all the dims and put them in fdims. */
    if (H5Sget_simple_extent_dims(file_spaceid, fdims, fmaxdims) < 0)
        BAIL(NC_EHDFERR);

#ifdef LOGGING
    log_dim_info(var, fdims, fmaxdims, start, count);
#endif

    /* Check dimension bounds. Remember that unlimited dimensions can
     * put data beyond their current length. */
    for (d2 = 0; d2 < var->ndims; d2++)
    {
        hsize_t endindex = start[d2] + stride[d2] * (count[d2] - 1); /* last index written */
        dim = var->dim[d2];
        assert(dim && dim->hdr.id == var->dimids[d2]);
        if (count[d2] == 0)
            endindex = start[d2]; /* fixup for zero read count */
        if (!dim->unlimited)
        {
            /* Allow start to equal dim size if count is zero. */
            if (start[d2] > (hssize_t)fdims[d2] ||
                (start[d2] == (hssize_t)fdims[d2] && count[d2] > 0))
                BAIL_QUIET(NC_EINVALCOORDS);
            if (!zero_count && endindex >= fdims[d2])
                BAIL_QUIET(NC_EEDGE);
        }
    }

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
        if (stridep == NULL)
            herr = H5Sselect_hyperslab(file_spaceid, H5S_SELECT_SET, start,
                                       NULL, ones, count);
        else
            herr = H5Sselect_hyperslab(file_spaceid, H5S_SELECT_SET, start,
                                       stride, count, NULL);
        if (herr < 0)
            BAIL(NC_EHDFERR);

        /* Create a space for the memory, just big enough to hold the slab
           we want. */
        if ((mem_spaceid = H5Screate_simple((int)var->ndims, count, NULL)) < 0)
            BAIL(NC_EHDFERR);
    }

    /* Are we going to convert any data? (No converting of compound or
     * opaque types.) We also need to call this code if we are doing
     * quantization. */
    if ((mem_nc_type != var->type_info->hdr.id &&
	 mem_nc_type != NC_COMPOUND && mem_nc_type != NC_OPAQUE) ||
	var->quantize_mode)
    {
        size_t file_type_size;

        /* We must convert - allocate a buffer. */
        need_to_convert++;
        if (var->ndims)
            for (d2=0; d2<var->ndims; d2++)
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
        if (len > 0)
            if (!(bufr = malloc(len * file_type_size)))
                BAIL(NC_ENOMEM);
    }
    else
        bufr = (void *)data;

    /* Create the data transfer property list. */
    if ((xfer_plistid = H5Pcreate(H5P_DATASET_XFER)) < 0)
        BAIL(NC_EHDFERR);

#ifdef USE_PARALLEL4
    /* Set up parallel I/O, if needed. */
    if ((retval = set_par_access(h5, var, xfer_plistid)))
        BAIL(retval);
#endif

    /* Does the dataset have to be extended? If it's already extended
       to the required size, it will do no harm to reextend it to that
       size. */
    if (var->ndims)
    {
        for (d2 = 0; d2 < var->ndims; d2++)
        {
            hsize_t endindex = start[d2] + stride[d2] * (count[d2] - 1); /* last index written */
            if (count[d2] == 0)
                endindex = start[d2];
            dim = var->dim[d2];
            assert(dim && dim->hdr.id == var->dimids[d2]);
            if (dim->unlimited)
            {
#ifdef USE_PARALLEL4
                extend_possible = 1;
#endif
                if (!zero_count && endindex >= fdims[d2])
                {
                    xtend_size[d2] = (long long unsigned)(endindex + 1);
                    need_to_extend++;
                    dim->extended = NC_TRUE;
                }
                else
                    xtend_size[d2] = (long long unsigned)fdims[d2];
            }
            else
            {
                xtend_size[d2] = (long long unsigned)dim->len;
            }
        }

#ifdef USE_PARALLEL4
        /* Check if anyone wants to extend. */
        if (extend_possible && h5->parallel &&
            NC_COLLECTIVE == var->parallel_access)
        {
            /* Form consensus opinion among all processes about whether
             * to perform collective I/O.  */
            if (MPI_SUCCESS != MPI_Allreduce(MPI_IN_PLACE, &need_to_extend, 1,
                                             MPI_INT, MPI_BOR, h5->comm))
                BAIL(NC_EMPI);
        }
#endif /* USE_PARALLEL4 */

        /* If we need to extend it, we also need a new file_spaceid
           to reflect the new size of the space. */
        if (need_to_extend)
        {
            LOG((4, "extending dataset"));
#ifdef USE_PARALLEL4
            if (h5->parallel)
            {
                if (NC_COLLECTIVE != var->parallel_access)
                    BAIL(NC_ECANTEXTEND);

                /* Reach consensus about dimension sizes to extend to */
                if (MPI_SUCCESS != MPI_Allreduce(MPI_IN_PLACE, xtend_size, var->ndims,
                                                 MPI_UNSIGNED_LONG_LONG, MPI_MAX,
                                                 h5->comm))
                    BAIL(NC_EMPI);
            }
#endif /* USE_PARALLEL4 */
            /* Convert xtend_size back to hsize_t for use with
             * H5Dset_extent. */
            for (d2 = 0; d2 < var->ndims; d2++)
                fdims[d2] = (hsize_t)xtend_size[d2];

            if (H5Dset_extent(hdf5_var->hdf_datasetid, fdims) < 0)
                BAIL(NC_EHDFERR);
            if (file_spaceid > 0 && H5Sclose(file_spaceid) < 0)
                BAIL2(NC_EHDFERR);
            if ((file_spaceid = H5Dget_space(hdf5_var->hdf_datasetid)) < 0)
                BAIL(NC_EHDFERR);

            if (stridep == NULL)
                herr = H5Sselect_hyperslab(file_spaceid, H5S_SELECT_SET,
                                           start, NULL, ones, count);
            else
                herr = H5Sselect_hyperslab(file_spaceid, H5S_SELECT_SET,
                                           start, stride, count, NULL);
            if (herr < 0)
                BAIL(NC_EHDFERR);
        }
    }

    /* Do we need to convert the data? */
    if (need_to_convert)
    {
        if ((retval = nc4_convert_type(data, bufr, mem_nc_type, var->type_info->hdr.id,
                                       len, &range_error, var->fill_value,
                                       (h5->cmode & NC_CLASSIC_MODEL), var->quantize_mode,
				       var->nsd)))
            BAIL(retval);
    }

    /* Write the data. At last! */
    LOG((4, "about to H5Dwrite datasetid 0x%x mem_spaceid 0x%x "
         "file_spaceid 0x%x", hdf5_var->hdf_datasetid, mem_spaceid, file_spaceid));
    if (H5Dwrite(hdf5_var->hdf_datasetid,
                 ((NC_HDF5_TYPE_INFO_T *)var->type_info->format_type_info)->hdf_typeid,
                 mem_spaceid, file_spaceid, xfer_plistid, bufr) < 0)
        BAIL(NC_EHDFERR);

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
    if (file_spaceid > 0 && H5Sclose(file_spaceid) < 0)
        BAIL2(NC_EHDFERR);
    if (mem_spaceid > 0 && H5Sclose(mem_spaceid) < 0)
        BAIL2(NC_EHDFERR);
    if (xfer_plistid && (H5Pclose(xfer_plistid) < 0))
        BAIL2(NC_EPARINIT);
    if (need_to_convert && bufr) free(bufr);

    /* If there was an error return it, otherwise return any potential
       range error value. If none, return NC_NOERR as usual.*/
    if (retval)
        return retval;
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
 * non-scalar vars. This array must be same size as variable's number
 * of dimensions.
 * @param countp Array of counts. Will default to counts of extent of
 * dimension if NULL. This array must be same size as variable's
 * number of dimensions.
 * @param stridep Array of strides. Will default to strides of 1 if
 * NULL. This array must be same size as variable's number of dimensions.
 * @param data The data to be written.
 * @param mem_nc_type The type of the data in memory. (Convert to this
 * type from file type.)
 *
 * @returns ::NC_NOERR No error.
 * @returns ::NC_EBADID Bad ncid.
 * @returns ::NC_ENOTVAR Var not found.
 * @returns ::NC_EHDFERR HDF5 function returned error.
 * @returns ::NC_EINVALCOORDS Incorrect start.
 * @returns ::NC_EEDGE Incorrect start/count.
 * @returns ::NC_ENOMEM Out of memory.
 * @returns ::NC_EMPI MPI library error (parallel only)
 * @returns ::NC_ECANTEXTEND Can't extend dimension for write.
 * @returns ::NC_ERANGE Data conversion error.
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
NC4_get_vars(int ncid, int varid, const size_t *startp, const size_t *countp,
             const ptrdiff_t *stridep, void *data, nc_type mem_nc_type)
{
    NC_GRP_INFO_T *grp;
    NC_FILE_INFO_T *h5;
    NC_VAR_INFO_T *var;
    NC_HDF5_VAR_INFO_T *hdf5_var;
    NC_DIM_INFO_T *dim;
    NC_HDF5_TYPE_INFO_T *hdf5_type;
    hid_t file_spaceid = 0, mem_spaceid = 0;
    hid_t xfer_plistid = 0;
    size_t file_type_size;
    hsize_t count[NC_MAX_VAR_DIMS];
    hsize_t fdims[NC_MAX_VAR_DIMS], fmaxdims[NC_MAX_VAR_DIMS];
    hsize_t start[NC_MAX_VAR_DIMS];
    hsize_t stride[NC_MAX_VAR_DIMS], ones[NC_MAX_VAR_DIMS];
    void *fillvalue = NULL;
    int no_read = 0, provide_fill = 0;
    hssize_t fill_value_size[NC_MAX_VAR_DIMS];
    int scalar = 0, retval, range_error = 0, i, d2;
    void *bufr = NULL;
    int need_to_convert = 0;
    size_t len = 1;
    int fixedlengthstring = 0;
    hsize_t fstring_len = 0;
    size_t fstring_count = 1;

    /* Find info for this file, group, and var. */
    if ((retval = nc4_hdf5_find_grp_h5_var(ncid, varid, &h5, &grp, &var)))
        return retval;
    assert(h5 && grp && var && var->hdr.id == varid && var->format_var_info &&
           var->type_info && var->type_info->size &&
           var->type_info->format_type_info);

    /* Get the HDF5-specific var and type info. */
    hdf5_var = (NC_HDF5_VAR_INFO_T *)var->format_var_info;
    hdf5_type = (NC_HDF5_TYPE_INFO_T *)var->type_info->format_type_info;

    LOG((3, "%s: var->hdr.name %s mem_nc_type %d", __func__,
         var->hdr.name, mem_nc_type));

    /* Check some stuff about the type and the file. Also end define
     * mode, if needed. */
    if ((retval = check_for_vara(&mem_nc_type, var, h5)))
        return retval;
    assert(hdf5_var->hdf_datasetid && (!var->ndims || (startp && countp)));

    /* Verify that all the variable's filters are available */
    if(hdf5_var->flags & NC_HDF5_VAR_FILTER_MISSING) {
	unsigned id = 0;
	NC4_hdf5_find_missing_filter(var, &id);
	LOG((0,"missing filter: variable=%s id=%u",var->hdr.name,id));
        return NC_ENOFILTER;
    }

    /* Convert from size_t and ptrdiff_t to hsize_t. Also do sanity
     * checks. */
    for (i = 0; i < var->ndims; i++)
    {
        /* If any of the stride values are non-positive, fail. */
        if (stridep && stridep[i] <= 0)
            return NC_ESTRIDE;

        start[i] = startp[i];
        count[i] = countp[i];
        stride[i] = stridep ? (hsize_t)stridep[i] : 1;
        ones[i] = 1;

        /* if any of the count values are zero don't actually read. */
        if (count[i] == 0)
            no_read++;
    }

    /* Get file space of data. */
    if ((file_spaceid = H5Dget_space(hdf5_var->hdf_datasetid)) < 0)
        BAIL(NC_EHDFERR);

    /* Check to ensure the user selection is
     * valid. H5Sget_simple_extent_dims gets the sizes of all the dims
     * and put them in fdims. */
    if (H5Sget_simple_extent_dims(file_spaceid, fdims, fmaxdims) < 0)
        BAIL(NC_EHDFERR);

#ifdef LOGGING
    log_dim_info(var, fdims, fmaxdims, start, count);
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
        if (var->ndims)
            for (d2 = 0; d2 < var->ndims; d2++)
                len *= countp[d2];
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
     * get data beyond the length of the dataset, but within the
     * lengths of the unlimited dimension(s). */
    for (d2 = 0; d2 < var->ndims; d2++)
    {
        hsize_t endindex = start[d2] + stride[d2] * (count[d2] - 1); /* last index read */
        dim = var->dim[d2];
        assert(dim && dim->hdr.id == var->dimids[d2]);
        if (count[d2] == 0)
            endindex = start[d2]; /* fixup for zero read count */
        if (dim->unlimited)
        {
            size_t ulen;

            /* We can't go beyond the largest current extent of
               the unlimited dim. */
            if ((retval = HDF5_inq_dim(ncid, dim->hdr.id, NULL, &ulen)))
                BAIL(retval);

            /* Check for out of bound requests. */
            /* Allow start to equal dim size if count is zero. */
            if (start[d2] > (hssize_t)ulen ||
                (start[d2] == (hssize_t)ulen && count[d2] > 0))
                BAIL_QUIET(NC_EINVALCOORDS);
            if (count[d2] && endindex >= ulen)
                BAIL_QUIET(NC_EEDGE);

            /* Things get a little tricky here. If we're getting a GET
               request beyond the end of this var's current length in
               an unlimited dimension, we'll later need to return the
               fill value for the variable. */
            if (!no_read)
            {
                if (start[d2] >= (hssize_t)fdims[d2])
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
            if (start[d2] > (hssize_t)fdims[d2] ||
                (start[d2] == (hssize_t)fdims[d2] && count[d2] > 0))
                BAIL_QUIET(NC_EINVALCOORDS);
            if (count[d2] && endindex >= fdims[d2])
                BAIL_QUIET(NC_EEDGE);

            /* Set the fill value boundary */
            fill_value_size[d2] = count[d2];
        }
    }

    if (!no_read)
    {
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
            herr_t herr;
            if (stridep == NULL)
                herr = H5Sselect_hyperslab(file_spaceid, H5S_SELECT_SET,
                                           start, NULL, ones, count);
            else
                herr = H5Sselect_hyperslab(file_spaceid, H5S_SELECT_SET,
                                           start, stride, count, NULL);
            if (herr < 0)
                BAIL(NC_EHDFERR);

            /* Create a space for the memory, just big enough to hold the slab
               we want. */
            if ((mem_spaceid = H5Screate_simple((int)var->ndims, count, NULL)) < 0)
                BAIL(NC_EHDFERR);
        }

        /* Fix bug when reading HDF5 files with variable of type
         * fixed-length string.  We need to make it look like a
         * variable-length string, because that's all netCDF-4 data
         * model supports, lacking anonymous dimensions.  So
         * variable-length strings are in allocated memory that user has
         * to free, which we allocate here. */
        if (var->type_info->nc_type_class == NC_STRING &&
            H5Tget_size(hdf5_type->hdf_typeid) > 1 &&
            !H5Tis_variable_str(hdf5_type->hdf_typeid))
        {
	    size_t k;

            if ((fstring_len = H5Tget_size(hdf5_type->hdf_typeid)) == 0)
                BAIL(NC_EHDFERR);
	    /* Compute the total number of strings to read */
            if (var->ndims) {
                for (k = 0; k < var->ndims; k++) {
                    fstring_count *= countp[k];
		}
	    }
	    /* Allocate space for the all the strings */
            if (!(bufr = malloc(fstring_len*fstring_count)))
                    BAIL(NC_ENOMEM);
	    fixedlengthstring = 1;
	}

        /* Create the data transfer property list. */
        if ((xfer_plistid = H5Pcreate(H5P_DATASET_XFER)) < 0)
            BAIL(NC_EHDFERR);

#ifdef USE_PARALLEL4
        /* Set up parallel I/O, if needed. */
        if ((retval = set_par_access(h5, var, xfer_plistid)))
            BAIL(retval);
#endif

        /* Read this hyperslab into memory. */
        LOG((5, "About to H5Dread some data..."));
        if (H5Dread(hdf5_var->hdf_datasetid,
                    ((NC_HDF5_TYPE_INFO_T *)var->type_info->format_type_info)->native_hdf_typeid,
                    mem_spaceid, file_spaceid, xfer_plistid, bufr) < 0)
            BAIL(NC_EHDFERR);
    } /* endif ! no_read */
    else
    {
#ifdef USE_PARALLEL4 /* Start block contributed by HDF group. */
        /* For collective IO read, some processes may not have any element for reading.
           Collective requires all processes to participate, so we use H5Sselect_none
           for these processes. */
        if (var->parallel_access == NC_COLLECTIVE)
        {
            /* Create the data transfer property list. */
            if ((xfer_plistid = H5Pcreate(H5P_DATASET_XFER)) < 0)
                BAIL(NC_EHDFERR);

            if ((retval = set_par_access(h5, var, xfer_plistid)))
                BAIL(retval);

            if (H5Sselect_none(file_spaceid) < 0)
                BAIL(NC_EHDFERR);

            /* Since no element will be selected, we just get the memory
             * space the same as the file space. */
            if ((mem_spaceid = H5Dget_space(hdf5_var->hdf_datasetid)) < 0)
                BAIL(NC_EHDFERR);
            if (H5Sselect_none(mem_spaceid) < 0)
                BAIL(NC_EHDFERR);

            /* Read this hyperslab into memory. */
            LOG((5, "About to H5Dread some data..."));
            if (H5Dread(hdf5_var->hdf_datasetid,
                        ((NC_HDF5_TYPE_INFO_T *)var->type_info->format_type_info)->native_hdf_typeid,
                        mem_spaceid, file_spaceid, xfer_plistid, bufr) < 0)
                BAIL(NC_EHDFERR);
        }
#endif /* USE_PARALLEL4 */
    }

    /* If we read a sequence of fixed length strings, then we need to convert to char* in memory */
    if(fixedlengthstring) {
	size_t k;
	char** strvec = (char**)data;
	char* p;
	for(k=0;k < fstring_count;k++) {
	    char* eol;
	    if((p = (char*)malloc(1+fstring_len))==NULL) BAIL(NC_ENOMEM);
	    memcpy(p,((char*)bufr)+(k*fstring_len),fstring_len);
	    eol = p + fstring_len;
	    *eol = '\0';
	    strvec[k] = p; p = NULL;
	}
	free(bufr);
	bufr = NULL;
    }

    /* Now we need to fake up any further data that was asked for,
       using the fill values instead. First skip past the data we
       just read, if any. */
    if (!scalar && provide_fill)
    {
        void *filldata;
        size_t real_data_size = 0;
        size_t fill_len;

        /* Skip past the real data we've already read. */
        if (!no_read)
            for (real_data_size = file_type_size, d2 = 0; d2 < var->ndims; d2++)
                real_data_size *= count[d2];

        /* Get the fill value from the HDF5 variable. Memory will be
         * allocated. */
        if (nc4_get_fill_value(h5, var, &fillvalue) < 0)
            BAIL(NC_EHDFERR);

        /* How many fill values do we need? */
        for (fill_len = 1, d2 = 0; d2 < var->ndims; d2++)
            fill_len *= (fill_value_size[d2] ? fill_value_size[d2] : 1);

        /* Copy the fill value into the rest of the data buffer. */
        filldata = (char *)bufr + real_data_size;
        for (i = 0; i < fill_len; i++)
        {

	    {
		/* Copy one instance of the fill_value */
		if((retval = NC_copy_data(h5->controller,var->type_info->hdr.id,fillvalue,1,filldata)))
		    BAIL(retval);
	    }
            filldata = (char *)filldata + file_type_size;
	}        
    }

    /* Convert data type if needed. */
    if (need_to_convert)
    {
        if ((retval = nc4_convert_type(bufr, data, var->type_info->hdr.id, mem_nc_type,
				       len, &range_error, var->fill_value,
				       (h5->cmode & NC_CLASSIC_MODEL), var->quantize_mode, var->nsd)))
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
    if(fixedlengthstring && bufr) free(bufr);
    if (file_spaceid > 0)
        if (H5Sclose(file_spaceid) < 0)
            BAIL2(NC_EHDFERR);
    if (mem_spaceid > 0)
        if (H5Sclose(mem_spaceid) < 0)
            BAIL2(NC_EHDFERR);
    if (xfer_plistid > 0)
        if (H5Pclose(xfer_plistid) < 0)
            BAIL2(NC_EHDFERR);
    if (need_to_convert && bufr)
        free(bufr);
    if (fillvalue)
    {
        if (var->type_info->nc_type_class == NC_VLEN)
            nc_free_vlen((nc_vlen_t *)fillvalue);
        else if (var->type_info->nc_type_class == NC_STRING && *(char **)fillvalue)
            free(*(char **)fillvalue);
        free(fillvalue);
    }

    /* If there was an error return it, otherwise return any potential
       range error value. If none, return NC_NOERR as usual.*/
    if (retval)
        return retval;
    if (range_error)
        return NC_ERANGE;
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
 * @param storagep Gets storage setting.
 * @param chunksizesp Gets chunksizes.
 * @param no_fill Gets fill mode.
 * @param fill_valuep Gets fill value.
 * @param endiannessp Gets one of ::NC_ENDIAN_BIG ::NC_ENDIAN_LITTLE
 * ::NC_ENDIAN_NATIVE
 * @param idp Pointer to memory to store filter id.
 * @param nparamsp Pointer to memory to store filter parameter count.
 * @param params Pointer to vector of unsigned integers into which
 * to store filter parameters.
 * @param quantize_modep Gets quantization mode.
 * @param nsdp Gets number of significant digits, if quantization is in use.
 *
 * @returns ::NC_NOERR No error.
 * @returns ::NC_EBADID Bad ncid.
 * @returns ::NC_ENOTVAR Bad varid.
 * @returns ::NC_ENOMEM Out of memory.
 * @returns ::NC_EINVAL Invalid input.
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
NC4_HDF5_inq_var_all(int ncid, int varid, char *name, nc_type *xtypep,
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

    LOG((2, "%s: ncid 0x%x varid %d", __func__, ncid, varid));

    /* Find the file, group, and var info, and do lazy att read if
     * needed. */
    if ((retval = nc4_hdf5_find_grp_var_att(ncid, varid, NULL, 0, 0, NULL,
                                            &h5, &grp, &var, NULL)))
        return retval;
    assert(grp && h5);

    /* Now that lazy atts have been read, use the libsrc4 function to
     * get the answers. */
    return NC4_inq_var_all(ncid, varid, name, xtypep, ndimsp, dimidsp, nattsp,
                           shufflep, unused4, unused5, fletcher32p,
                           storagep, chunksizesp, no_fill, fill_valuep,
                           endiannessp, unused1, unused2, unused3);
}

/**
 * @internal Set chunk cache size for a variable. This is the internal
 * function called by nc_set_var_chunk_cache().
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param size Size in bytes to set cache.
 * @param nelems Number of elements in cache.
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
NC4_HDF5_set_var_chunk_cache(int ncid, int varid, size_t size, size_t nelems,
                             float preemption)
{
    NC_GRP_INFO_T *grp;
    NC_FILE_INFO_T *h5;
    NC_VAR_INFO_T *var;
    int retval;

    /* Check input for validity. */
    if (preemption < 0 || preemption > 1)
        return NC_EINVAL;

    /* Find info for this file and group, and set pointer to each. */
    if ((retval = nc4_find_nc_grp_h5(ncid, NULL, &grp, &h5)))
        return retval;
    assert(grp && h5);

    /* Find the var. */
    if (!(var = (NC_VAR_INFO_T *)ncindexith(grp->vars, (size_t)varid)))
        return NC_ENOTVAR;
    assert(var && var->hdr.id == varid);

    /* Set the values. */
    var->chunkcache.size = size;
    var->chunkcache.nelems = nelems;
    var->chunkcache.preemption = preemption;

    /* Reopen the dataset to bring new settings into effect. */
    if ((retval = nc4_reopen_dataset(grp, var)))
        return retval;
    return NC_NOERR;
}

/**
 * @internal A wrapper for NC4_set_var_chunk_cache(), we need this
 * version for fortran. Negative values leave settings as they are.
 * Note: see libsrc4/nc4cache.c for definition when HDF5 is disabled
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param size Size in MB to set cache.
 * @param nelems Number of elements in cache.
 * @param preemption Controls cache swapping.
 *
 * @returns ::NC_NOERR for success
 * @author Ed Hartnett
 */
int
nc_set_var_chunk_cache_ints(int ncid, int varid, int size, int nelems,
                            int preemption)
{
    size_t real_size = H5D_CHUNK_CACHE_NBYTES_DEFAULT;
    size_t real_nelems = H5D_CHUNK_CACHE_NSLOTS_DEFAULT;
    float real_preemption = CHUNK_CACHE_PREEMPTION;

    LOG((1, "%s: ncid 0x%x varid %d size %d nelems %d preemption %d",
	 __func__, ncid, varid, size, nelems, preemption));
    
    if (size >= 0)
        real_size = ((size_t) size) * MEGABYTE;

    if (nelems >= 0)
        real_nelems = (size_t)nelems;

    if (preemption >= 0)
        real_preemption = (float)(preemption / 100.);

    return NC4_HDF5_set_var_chunk_cache(ncid, varid, real_size, real_nelems,
                                        real_preemption);
}
