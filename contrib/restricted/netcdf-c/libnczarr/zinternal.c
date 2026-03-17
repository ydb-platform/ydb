/* Copyright 2003-2018, University Corporation for Atmospheric
 * Research. See the COPYRIGHT file for copying and redistribution
 * conditions.
 */

/**
 * @file @internal Internal netcdf-4 functions.
 *
 * This file contains functions internal to the netcdf4 library. None of
 * the functions in this file are exposed in the exetnal API. These
 * functions all relate to the manipulation of netcdf-4's in-memory
 * buffer of metadata information, i.e. the linked list of NC
 * structs.
 *
 * @author Dennis Heimbigner, Ed Hartnett
 */

#include "zincludes.h"
#include "zfilter.h"

/* Forward */

#ifdef LOGGING
/* This is the severity level of messages which will be logged. Use
   severity 0 for errors, 1 for important log messages, 2 for less
   important, etc. */
extern int nc_log_level;
#endif /* LOGGING */

#ifdef LOOK
/**
 * @internal Provide a wrapper for H5Eset_auto
 * @param func Pointer to func.
 * @param client_data Client data.
 *
 * @return 0 for success
 */
static herr_t
set_auto(void* func, void *client_data)
{
#ifdef DEBUGH5
    return H5Eset_auto2(H5E_DEFAULT,(H5E_auto2_t)h5catch,client_data);
#else
    return H5Eset_auto2(H5E_DEFAULT,(H5E_auto2_t)func,client_data);
#endif
}
#endif

/**
 * @internal Provide a function to do any necessary initialization of
 * the ZARR library.
 */
int
NCZ_initialize_internal(void)
{
    int stat = NC_NOERR;
    char* dimsep = NULL;
    NCglobalstate* ngs = NULL;

    ncz_initialized = 1;
    ngs = NC_getglobalstate();
    if(ngs != NULL) {
        /* Defaults */
	ngs->zarr.dimension_separator = DFALT_DIM_SEPARATOR;
        dimsep = NC_rclookup("ZARR.DIMENSION_SEPARATOR",NULL,NULL);
        if(dimsep != NULL) {
            /* Verify its value */
	    if(dimsep != NULL && strlen(dimsep) == 1 && islegaldimsep(dimsep[0]))
		ngs->zarr.dimension_separator = dimsep[0];
        }    
    }

    return stat;
}

/**
 * @internal Provide a function to do any necessary finalization of
 * the ZARR library.
 */
int
NCZ_finalize_internal(void)
{
    /* Reclaim global resources */
    ncz_initialized = 0;
#ifdef NETCDF_ENABLE_NCZARR_FILTERS
    NCZ_filter_finalize();
#endif
#ifdef NETCDF_ENABLE_S3
    NCZ_s3finalize();
#endif
    return NC_NOERR;
}

/**
 * @internal Given a varid, return the maximum length of a dimension
 * using dimid.
 *
 * @param grp Pointer to group info struct.
 * @param varid Variable ID.
 * @param dimid Dimension ID.
 * @param maxlen Pointer that gets the max length.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner, Ed Hartnett
 */
static int
find_var_dim_max_length(NC_GRP_INFO_T *grp, int varid, int dimid,
                        size_t *maxlen)
{
    NC_VAR_INFO_T *var;
    int retval = NC_NOERR;

    *maxlen = 0;

    /* Find this var. */
    var = (NC_VAR_INFO_T*)ncindexith(grp->vars,(size_t)varid);
    if (!var) return NC_ENOTVAR;
    assert(var->hdr.id == varid);

    /* If the var hasn't been created yet, its size is 0. */
    if (!var->created)
    {
        *maxlen = 0;
    }
    else
    {
        /* Get the number of records in the dataset. */
#ifdef LOOK
#if 0
not needed        if ((retval = ncz_open_var_grp2(grp, var->hdr.id, &datasetid)))
            BAIL(retval);
#endif
        if ((spaceid = H5Dget_space(datasetid)) < 0)
            BAIL(NC_EHDFERR);
        /* If it's a scalar dataset, it has length one. */
        if (H5Sget_simple_extent_type(spaceid) == H5S_SCALAR)
        {
            *maxlen = (var->dimids && var->dimids[0] == dimid) ? 1 : 0;
        }
        else
        {
            /* Check to make sure ndims is right, then get the len of each
               dim in the space. */
            if ((dataset_ndims = H5Sget_simple_extent_ndims(spaceid)) < 0)
                BAIL(NC_EHDFERR);
            if (dataset_ndims != var->ndims)
                BAIL(NC_EHDFERR);
            if (!(h5dimlen = malloc(dataset_ndims * sizeof(hsize_t))))
                BAIL(NC_ENOMEM);
            if (!(h5dimlenmax = malloc(dataset_ndims * sizeof(hsize_t))))
                BAIL(NC_ENOMEM);
            if ((dataset_ndims = H5Sget_simple_extent_dims(spaceid,
                                                           h5dimlen, h5dimlenmax)) < 0)
                BAIL(NC_EHDFERR);
            LOG((5, "find_var_dim_max_length: varid %d len %d max: %d",
                 varid, (int)h5dimlen[0], (int)h5dimlenmax[0]));
            for (d=0; d<dataset_ndims; d++) {
                if (var->dimids[d] == dimid) {
                    *maxlen = *maxlen > h5dimlen[d] ? *maxlen : h5dimlen[d];
                }
            }
        }
#endif /*LOOK*/
    }

#ifdef LOOK
exit:
    if (spaceid > 0 && H5Sclose(spaceid) < 0)
        BAIL2(NC_EHDFERR);
    if (h5dimlen) free(h5dimlen);
    if (h5dimlenmax) free(h5dimlenmax);
#endif
    return retval;
}

#ifdef LOOK
/**
 * @internal Search for type with a given HDF type id.
 *
 * @param h5 File
 * @param target_hdf_typeid ZARR type ID to find.
 *
 * @return Pointer to type info struct, or NULL if not found.
 * @author Dennis Heimbigner, Ed Hartnett
 */
NC_TYPE_INFO_T *
ncz_rec_find_hdf_type(NC_FILE_INFO_T *h5, hid_t target_hdf_typeid)
{
    NC_TYPE_INFO_T *type;
    int i;

    assert(h5);

    for (i = 0; i < nclistlength(h5->alltypes); i++)
    {
        type = (NC_TYPE_INFO_T*)nclistget(h5->alltypes, i);
        if(type == NULL) continue;

#ifdef LOOK
        /* Select the ZARR typeid to use. */
        hdf_typeid = ncz_type->native_hdf_typeid ?
            ncz_type->native_hdf_typeid : ncz_type->hdf_typeid;

        /* Is this the type we are searching for? */
        if ((equal = H5Tequal(hdf_typeid, target_hdf_typeid)) < 0)
            return NULL;
        if (equal)
            return type;
#endif
    }
    /* Can't find it. Fate, why do you mock me? */
    return NULL;
}
#endif

/**
 * @internal Find the actual length of a dim by checking the length of
 * that dim in all variables that use it, in grp or children. **len
 * must be initialized to zero before this function is called.
 *
 * @param grp Pointer to group info struct.
 * @param dimid Dimension ID.
 * @param len Pointer to pointer that gets length.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
ncz_find_dim_len(NC_GRP_INFO_T *grp, int dimid, size_t **len)
{
    NC_VAR_INFO_T *var;
    int retval;
    size_t i;

    assert(grp && len);
    LOG((3, "%s: grp->name %s dimid %d", __func__, grp->hdr.name, dimid));

    /* If there are any groups, call this function recursively on
     * them. */
    for (i = 0; i < ncindexsize(grp->children); i++) {
        if ((retval = ncz_find_dim_len((NC_GRP_INFO_T*)ncindexith(grp->children, i),
                                       dimid, len)))
            return retval;
    }
    /* For all variables in this group, find the ones that use this
     * dimension, and remember the max length. */
    for (i = 0; i < ncindexsize(grp->vars); i++)
    {
        size_t mylen;
        var = (NC_VAR_INFO_T *)ncindexith(grp->vars, i);
        assert(var);

        /* Find max length of dim in this variable... */
        if ((retval = find_var_dim_max_length(grp, var->hdr.id, dimid, &mylen)))
            return retval;

        **len = **len > mylen ? **len : mylen;
    }

    return NC_NOERR;
}

#if 0
/**
 * @internal Close ZARR resources for global atts in a group.
 *
 * @param grp Pointer to group info struct.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EHDFERR ZARR error.
 * @author Dennis Heimbigner, Ed Hartnett
 */

static int
close_gatts(NC_GRP_INFO_T *grp)
{
    NC_ATT_INFO_T *att;
    int a;

    for (a = 0; a < ncindexsize(grp->att); a++)
    {
        att = (NC_ATT_INFO_T *)ncindexith(grp->att, a);
        assert(att && att->format_att_info);

#ifdef LOOK
        /* Close the ZARR typeid. */
        if (ncz_att->native_hdf_typeid &&
            H5Tclose(ncz_att->native_hdf_typeid) < 0)
            return NC_EHDFERR;
#endif
    }
    return NC_NOERR;
}
#endif /*0*/

#if 0
/**
 * @internal Close ZARR resources for vars in a group.
 *
 * @param grp Pointer to group info struct.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EHDFERR ZARR error.
 * @author Dennis Heimbigner, Ed Hartnett
 */
static int
close_vars(NC_GRP_INFO_T *grp)
{
    NC_VAR_INFO_T *var;
    NC_ATT_INFO_T *att;
    int a, i;

    for (i = 0; i < ncindexsize(grp->vars); i++)
    {
        var = (NC_VAR_INFO_T *)ncindexith(grp->vars, i);
        assert(var && var->format_var_info);

        /* Close the ZARR dataset associated with this var. */
#ifdef LOOK
        if (ncz_var->hdf_datasetid)
#endif
        {
#ifdef LOOK
            LOG((3, "closing ZARR dataset %lld", ncz_var->hdf_datasetid));
            if (H5Dclose(ncz_var->hdf_datasetid) < 0)
                return NC_EHDFERR;
#endif
            if (var->fill_value)
            {
                if (var->type_info)
                {
		    int stat = NC_NOERR;
		    if((stat = NC_reclaim_data(grp->nc4_info,var->type_info->hdr.id,var->fill_value,1)))
		        return stat;
		    nullfree(var->fill_value);
                }
            }
        }

#ifdef LOOK
        /* Delete any ZARR dimscale objid information. */
        if (ncz_var->dimscale_ncz_objids)
            free(ncz_var->dimscale_ncz_objids);
#endif

        for (a = 0; a < ncindexsize(var->att); a++)
        {
            att = (NC_ATT_INFO_T *)ncindexith(var->att, a);
            assert(att && att->format_att_info);

#ifdef LOOK
            /* Close the ZARR typeid if one is open. */
            if (ncz_att->native_hdf_typeid &&
                H5Tclose(ncz_att->native_hdf_typeid) < 0)
                return NC_EHDFERR;
#endif
        }

	/* Reclaim filters */
	if(var->filters != NULL) {
	    (void)NCZ_filter_freelists(var);
	}
	var->filters = NULL;

    }

    return NC_NOERR;
}
#endif /*0*/

#if 0
/**
 * @internal Close ZARR resources for dims in a group.
 *
 * @param grp Pointer to group info struct.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EHDFERR ZARR error.
 * @author Dennis Heimbigner, Ed Hartnett
 */
static int
close_dims(NC_GRP_INFO_T *grp)
{
    NC_DIM_INFO_T *dim;
    size_t i;

    for (i = 0; i < ncindexsize(grp->dim); i++)
    {
        dim = (NC_DIM_INFO_T *)ncindexith(grp->dim, i);
        assert(dim && dim->format_dim_info);

#ifdef LOOK
        /* If this is a dim without a coordinate variable, then close
         * the ZARR DIM_WITHOUT_VARIABLE dataset associated with this
         * dim. */
        if (ncz_dim->hdf_dimscaleid && H5Dclose(ncz_dim->hdf_dimscaleid) < 0)
            return NC_EHDFERR;
#endif
    }

    return NC_NOERR;
}
#endif /*0*/

#if 0
/**
 * @internal Close ZARR resources for types in a group.  Set values to
 * 0 after closing types. Because of type reference counters, these
 * closes can be called multiple times.
 *
 * @param grp Pointer to group info struct.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EHDFERR ZARR error.
 * @author Dennis Heimbigner, Ed Hartnett
 */
static int
close_types(NC_GRP_INFO_T *grp)
{
    size_t i;

    for (i = 0; i < ncindexsize(grp->type); i++)
    {
        NC_TYPE_INFO_T *type;

        type = (NC_TYPE_INFO_T *)ncindexith(grp->type, i);
        assert(type && type->format_type_info);

#ifdef LOOK
        /* Close any open user-defined ZARR typeids. */
        if (ncz_type->hdf_typeid && H5Tclose(ncz_type->hdf_typeid) < 0)
            return NC_EHDFERR;
        ncz_type->hdf_typeid = 0;
        if (ncz_type->native_hdf_typeid &&
            H5Tclose(ncz_type->native_hdf_typeid) < 0)
            return NC_EHDFERR;
        ncz_type->native_hdf_typeid = 0;
#endif
    }

    return NC_NOERR;
}
#endif /*0*/

#if 0
/**
 * @internal Recursively free ZARR objects for a group (and everything
 * it contains).
 *
 * @param grp Pointer to group info struct.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EHDFERR ZARR error.
 * @author Dennis Heimbigner, Ed Hartnett
 */
static int
ncz_rec_grp_NCZ_del(NC_GRP_INFO_T *grp)
{
    size_t i;
    int retval;

    assert(grp && grp->format_grp_info);
    LOG((3, "%s: grp->name %s", __func__, grp->hdr.name));

    /* Recursively call this function for each child, if any, stopping
     * if there is an error. */
    for (i = 0; i < ncindexsize(grp->children); i++)
        if ((retval = ncz_rec_grp_NCZ_del((NC_GRP_INFO_T *)ncindexith(grp->children,
                                                                       i))))
            return retval;

    /* Close ZARR resources associated with global attributes. */
    if ((retval = close_gatts(grp)))
        return retval;

    /* Close ZARR resources associated with vars. */
    if ((retval = close_vars(grp)))
        return retval;

    /* Close ZARR resources associated with dims. */
    if ((retval = close_dims(grp)))
        return retval;

    /* Close ZARR resources associated with types. */
    if ((retval = close_types(grp)))
        return retval;

    /* Close the ZARR group. */
    LOG((4, "%s: closing group %s", __func__, grp->hdr.name));
#ifdef LOOK
    if (ncz_grp->hdf_grpid && H5Gclose(ncz_grp->hdf_grpid) < 0)
        return NC_EHDFERR;
#endif

    return NC_NOERR;
}
#endif /*0*/

/**
 * @internal Given an ncid and varid, get pointers to the group and var
 * metadata. Lazy var metadata reads are done as needed.
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param h5 Pointer that gets pointer to the NC_FILE_INFO_T struct
 * for this file. Ignored if NULL.
 * @param grp Pointer that gets pointer to group info. Ignored if
 * NULL.
 * @param var Pointer that gets pointer to var info. Ignored if NULL.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_ENOTVAR Variable not found.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
ncz_find_grp_file_var(int ncid, int varid, NC_FILE_INFO_T **h5,
                         NC_GRP_INFO_T **grp, NC_VAR_INFO_T **var)
{
    NC_FILE_INFO_T *my_h5;
    NC_VAR_INFO_T *my_var;
    int retval;

    /* Delegate to libsrc4 */
    if((retval = nc4_find_grp_h5_var(ncid,varid,&my_h5,grp,&my_var))) return retval;

    /* Do we need to read var metadata? */
    if (!my_var->meta_read && my_var->created)
        if ((retval = ncz_get_var_meta(my_h5, my_var)))
            return retval;
    if (var) *var = my_var;
    if (h5) *h5 = my_h5;
    return NC_NOERR;
}

/**
 * @internal Given an ncid, varid, and attribute name, return
 * normalized name and (optionally) pointers to the file, group,
 * var, and att info structs.
 * Lazy reads of attributes and variable metadata are done as needed.
 *
 * @param ncid File/group ID.
 * @param varid Variable ID.
 * @param name Name to of attribute.
 * @param attnum Number of attribute.
 * @param use_name If true, use the name to get the
 * attribute. Otherwise use the attnum.
 * @param norm_name Pointer to storage of size NC_MAX_NAME + 1,
 * which will get the normalized name, if use_name is true. Ignored if
 * NULL.
 * @param h5 Pointer to pointer that gets file info struct. Ignored if
 * NULL.
 * @param grp Pointer to pointer that gets group info struct. Ignored
 * if NULL.
 * @param h5 Pointer to pointer that gets variable info
 * struct. Ignored if NULL.
 * @param att Pointer to pointer that gets attribute info
 * struct. Ignored if NULL.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOTVAR Variable not found.
 * @return ::NC_ENOTATT Attribute not found.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
ncz_find_grp_var_att(int ncid, int varid, const char *name, int attnum,
                          int use_name, char *norm_name, NC_FILE_INFO_T **h5,
                          NC_GRP_INFO_T **grp, NC_VAR_INFO_T **var,
                          NC_ATT_INFO_T **att)
{
    NC_FILE_INFO_T *my_h5;
    NC_GRP_INFO_T *my_grp;
    NC_VAR_INFO_T *my_var = NULL;
    NC_ATT_INFO_T *my_att;
    char my_norm_name[NC_MAX_NAME + 1] = "";
    NCindex *attlist = NULL;
    int retval;

    LOG((4, "%s: ncid %d varid %d attnum %d use_name %d", __func__, ncid, varid,
         attnum, use_name));

    /* Don't need to provide name unless getting att pointer and using
     * use_name. */
    assert(!att || ((use_name && name) || !use_name));

    /* Find info for this file, group, and h5 info. */
    if ((retval = nc4_find_nc_grp_h5(ncid, NULL, &my_grp, &my_h5)))
        return retval;
    assert(my_grp && my_h5);

    /* Read the attributes for this var, if any */
    switch (retval = ncz_getattlist(my_grp, varid, &my_var, &attlist)) {
    case NC_NOERR: assert(attlist); break;
    case NC_EEMPTY: retval = NC_NOERR; attlist = NULL; break; /* variable has no attributes */
    default: return retval; /* significant error */
    }

    /* Need a name if use_name is true. */
    if (use_name && !name)
        return NC_EBADNAME;

    /* Normalize the name. */
    if (use_name)
        if ((retval = nc4_normalize_name(name, my_norm_name)))
            return retval;

    /* Now find the attribute by name or number. */
    if (att)
    {
        my_att = use_name ? (NC_ATT_INFO_T *)ncindexlookup(attlist, my_norm_name) :
            (NC_ATT_INFO_T *)ncindexith(attlist, (size_t)attnum);
        if (!my_att)
            return NC_ENOTATT;
    }

    /* Give the people what they want. */
    if (norm_name)
        strncpy(norm_name, my_norm_name, NC_MAX_NAME);
    if (h5)
        *h5 = my_h5;
    if (grp)
        *grp = my_grp;
    if (var)
        *var = my_var;
    if (att)
        *att = my_att;

    return retval;
}

/**
 * @internal Ensure that either var->no_fill || var->fill_value != NULL.
 * Side effects: set as default if necessary and build _FillValue attribute.
 *
 * @param h5 Pointer to file info struct.
 * @param var Pointer to variable info struct.
 *
 * @returns NC_NOERR No error.
 * @returns NC_ENOMEM Out of memory.
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
NCZ_ensure_fill_value(NC_VAR_INFO_T *var)
{
    size_t size;
    int retval = NC_NOERR;
    NC_FILE_INFO_T *h5 = var->container->nc4_info;

    if(var->no_fill)
        return NC_NOERR;

#if 0 /*LOOK*/
    /* Find out how much space we need for this type's fill value. */
    if (var->type_info->nc_type_class == NC_VLEN)
        size = sizeof(nc_vlen_t);
    else if (var->type_info->nc_type_class == NC_STRING)
        size = sizeof(char *);
    else
#endif

    if ((retval = nc4_get_typelen_mem(h5, var->type_info->hdr.id, &size))) goto done;
    assert(size);

    /* If the user has set a fill_value for this var, use, otherwise find the default fill value. */

    if (var->fill_value == NULL) {
	/* initialize the fill_value to the default */
	/* Allocate the fill_value space. */
        if((var->fill_value = calloc(1, size))==NULL)
	    {retval = NC_ENOMEM; goto done;}
        if((retval = nc4_get_default_fill_value(var->type_info, var->fill_value))) {
            /* Note: release memory, but don't return error on failure */
	    (void)NCZ_reclaim_fill_value(var);
	    retval = NC_NOERR;
	    goto done;
        }
    }
    assert(var->fill_value != NULL);

    LOG((4, "Found a fill value for var %s", var->hdr.name));
#if 0 /*LOOK*/
	/* Need to copy both vlen and a single basetype */
        if (var->type_info->nc_type_class == NC_VLEN)
        {
            nc_vlen_t *in_vlen = (nc_vlen_t *)(var->fill_value);
	    nc_vlen-t *fv_vlen = (nc_vlen_t *)fill;
            size_t basetypesize = 0;

            if((retval=nc4_get_typelen_mem(h5, var->type_info->u.v.base_nc_typeid, &basetypesize)))
                return retval;

            fv_vlen->len = in_vlen->len;
            if (!(fv_vlen->p = malloc(basetypesize * in_vlen->len)))
            {
                free(*fillp);
                *fillp = NULL;
                return NC_ENOMEM;
            }
            memcpy(fv_vlen->p, in_vlen->p, in_vlen->len * basetypesize);
        }
        else if (var->type_info->nc_type_class == NC_STRING)
        {
            if (*(char **)var->fill_value)
                if (!(**(char ***)fillp = strdup(*(char **)var->fill_value)))
                {
                    free(*fillp);
                    *fillp = NULL;
                    return NC_ENOMEM;
                }
        }
#endif /*0*/
done:
    return retval;
}

#ifdef LOGGING
/* We will need to check against nc log level from nc4internal.c. */
extern int nc_log_level;

/**
 * @internal This is like nc_set_log_level(), but will also turn on
 * ZARR internal logging, in addition to netCDF logging. This should
 * never be called by the user. It is called in open/create when the
 * nc logging level has changed.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_set_log_level()
{
    /* If the user wants to completely turn off logging, turn off NCZ
       logging too. Now I truly can't think of what to do if this
       fails, so just ignore the return code. */
    if (nc_log_level == NC_TURN_OFF_LOGGING)
    {
#ifdef LOOK
        set_auto(NULL, NULL);
#endif
        LOG((1, "NCZ error messages turned off!"));
    }
    else
    {
#ifdef LOOK
        if (set_auto((H5E_auto_t)&H5Eprint1, stderr) < 0)
            LOG((0, "H5Eset_auto failed!"));
#endif
        LOG((1, "NCZ error messages turned on."));
    }

    return NC_NOERR;
}
#endif /* LOGGING */

/**
 * @internal Get the format (i.e. NC_FORMAT_NETCDF4 pr
 * NC_FORMAT_NETCDF4_CLASSIC) of an open netCDF-4 file.
 *
 * @param ncid File ID (ignored).
 * @param formatp Pointer that gets the constant indicating format.

 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @author Ed Hartnett
 */
int
NCZ_inq_format(int ncid, int *formatp)
{
    int stat = NC_NOERR;

    ZTRACE(0,"ncid=%d formatp=%p",ncid,formatp);
    stat = NC4_inq_format(ncid,formatp);
    return ZUNTRACEX(stat,"formatp=%d",(formatp?-1:*formatp));
}

/**
 * @internal Return the extended format (i.e. the dispatch model),
 * plus the mode associated with an open file.
 *
 * @param ncid File ID (ignored).
 * @param formatp a pointer that gets the extended format. Note that
 * this is not the same as the format provided by nc_inq_format(). The
 * extended format indicates the dispatch layer model. NetCDF-4 files
 * will always get NC_FORMATX_NC4.
 * @param modep a pointer that gets the open/create mode associated with
 * this file. Ignored if NULL.

 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @author Dennis Heimbigner
 */
int
NCZ_inq_format_extended(int ncid, int *formatp, int *modep)
{
    NC *nc;
    int retval;

    LOG((2, "%s: ncid 0x%x", __func__, ncid));

    if ((retval = nc4_find_nc_grp_h5(ncid, &nc, NULL, NULL)))
        return NC_EBADID;

    if(modep)
        *modep = nc->mode|NC_NETCDF4;

    if (formatp)
        *formatp = NC_FORMATX_NCZARR;

    return NC_NOERR;
}
