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
 * @author Ed Hartnett
 */

#include "config.h"
#include <stddef.h>
#include "netcdf.h"
#include "ncplugins.h"
#include "hdf5internal.h"
#include "hdf5err.h" /* For BAIL2 */
#ifdef _WIN32
#include <windows.h>
#endif

#undef DEBUGH5

#ifdef DEBUGH5
/**
 * @internal Provide a catchable error reporting function
 *
 * @param ignored Ignored.
 *
 * @return 0 for success.
 */
static herr_t
h5catch(void* ignored)
{
    H5Eprint1(NULL);
    return 0;
}
#endif

#ifdef LOGGING
/* This is the severity level of messages which will be logged. Use
   severity 0 for errors, 1 for important log messages, 2 for less
   important, etc. */
extern int nc_log_level;

#endif /* LOGGING */

int nc4_hdf5_initialized = 0; /**< True if initialization has happened. */

/**
 * @internal Provide a wrapper for H5Eset_auto.
 *
 * If preprocessor symbol DEBUGH5 is set (at the top of this file),
 * then error messages will be pronted by the h5catch() function. If
 * not, a NULL will be passed as the second argument to H5eset_auto2()
 * and error messages will not be printed by HDF5.
 *
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

/**
 * @internal Provide a function to do any necessary initialization of
 * the HDF5 library.
 */
void
nc4_hdf5_initialize(void)
{
    if (set_auto(NULL, NULL) < 0)
        LOG((0, "Couldn't turn off HDF5 error messages!"));
    LOG((1, "HDF5 error messages have been turned off."));
    NC4_hdf5_filter_initialize();
    nc4_hdf5_initialized = 1;
}

/**
 * @internal Provide a function to do any necessary finalization of
 * the HDF5 library.
 */
void
nc4_hdf5_finalize(void)
{
    /* Reclaim global resources */
    NC4_provenance_finalize();
    NC4_hdf5_filter_finalize();
    nc4_hdf5_initialized = 0;
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
 * @author Ed Hartnett
 */
static int
find_var_dim_max_length(NC_GRP_INFO_T *grp, int varid, int dimid,
                        size_t *maxlen)
{
    hid_t datasetid = 0, spaceid = 0;
    NC_VAR_INFO_T *var;
    hsize_t *h5dimlen = NULL, *h5dimlenmax = NULL;
    int d, dataset_ndims = 0;
    int retval = NC_NOERR;

    *maxlen = 0;

    LOG((3, "find_var_dim_max_length varid %d dimid %d", varid, dimid));    

    /* Find this var. */
    var = (NC_VAR_INFO_T*)ncindexith(grp->vars, (size_t)varid);
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
        if ((retval = nc4_open_var_grp2(grp, var->hdr.id, &datasetid)))
            BAIL(retval);
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
            if (!(h5dimlen = malloc((size_t)dataset_ndims * sizeof(hsize_t))))
                BAIL(NC_ENOMEM);
            if (!(h5dimlenmax = malloc((size_t)dataset_ndims * sizeof(hsize_t))))
                BAIL(NC_ENOMEM);
            if ((dataset_ndims = H5Sget_simple_extent_dims(spaceid,
                                                           h5dimlen, h5dimlenmax)) < 0)
                BAIL(NC_EHDFERR);
            LOG((5, "find_var_dim_max_length: varid %d len %d max: %d",
                 varid, (int)h5dimlen[0], (int)h5dimlenmax[0]));
            for (d=0; d<dataset_ndims; d++)
                if (var->dimids[d] == dimid)
                    *maxlen = *maxlen > h5dimlen[d] ? *maxlen : h5dimlen[d];

#ifdef USE_PARALLEL
	    /* If we are doing parallel I/O in collective mode (with
	     * either pnetcdf or HDF5), then communicate with all
	     * other tasks in the collective and find out which has
	     * the max value for the dimension size. */
	    assert(grp->nc4_info);
	    LOG((3, "before Allreduce *maxlen %ld grp->nc4_info->parallel %d var->parallel_access %d",
		 *maxlen, grp->nc4_info->parallel, var->parallel_access));
	    if (grp->nc4_info->parallel && var->parallel_access == NC_COLLECTIVE)
	    {
		if ((MPI_SUCCESS != MPI_Allreduce(MPI_IN_PLACE, maxlen, 1,
						  MPI_UNSIGNED_LONG_LONG, MPI_MAX,
						  grp->nc4_info->comm)))
		    BAIL(NC_EMPI);
		LOG((3, "after Allreduce *maxlen %ld", *maxlen));
	    }
#endif /* USE_PARALLEL */
        }
    }

exit:
    if (spaceid > 0 && H5Sclose(spaceid) < 0)
        BAIL2(NC_EHDFERR);
    if (h5dimlen) free(h5dimlen);
    if (h5dimlenmax) free(h5dimlenmax);
    return retval;
}

/**
 * @internal Search for type with a given HDF type id.
 *
 * @param h5 File
 * @param target_hdf_typeid HDF5 type ID to find.
 *
 * @return Pointer to type info struct, or NULL if not found.
 * @author Ed Hartnett
 */
NC_TYPE_INFO_T *
nc4_rec_find_hdf_type(NC_FILE_INFO_T *h5, hid_t target_hdf_typeid)
{
    NC_TYPE_INFO_T *type;
    htri_t equal;
    size_t i;

    assert(h5);

    for (i = 0; i < nclistlength(h5->alltypes); i++)
    {
        NC_HDF5_TYPE_INFO_T *hdf5_type;
        hid_t hdf_typeid;

        type = (NC_TYPE_INFO_T*)nclistget(h5->alltypes, i);
        if(type == NULL) continue;

        /* Get HDF5-specific type info. */
        hdf5_type = (NC_HDF5_TYPE_INFO_T *)type->format_type_info;

        /* Select the HDF5 typeid to use. */
        hdf_typeid = hdf5_type->native_hdf_typeid ?
            hdf5_type->native_hdf_typeid : hdf5_type->hdf_typeid;

        /* Is this the type we are searching for? */
        if ((equal = H5Tequal(hdf_typeid, target_hdf_typeid)) < 0)
            return NULL;
        if (equal)
            return type;
    }
    /* Can't find it. Fate, why do you mock me? */
    return NULL;
}

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
 * @author Ed Hartnett
 */
int
nc4_find_dim_len(NC_GRP_INFO_T *grp, int dimid, size_t **len)
{
    NC_VAR_INFO_T *var;
    int retval;

    assert(grp && len);
    LOG((3, "%s: grp->name %s dimid %d", __func__, grp->hdr.name, dimid));

    /* If there are any groups, call this function recursively on
     * them. */
    for (size_t i = 0; i < ncindexsize(grp->children); i++)
        if ((retval = nc4_find_dim_len((NC_GRP_INFO_T*)ncindexith(grp->children, i),
                                       dimid, len)))
            return retval;

    /* For all variables in this group, find the ones that use this
     * dimension, and remember the max length. */
    for (size_t i = 0; i < ncindexsize(grp->vars); i++)
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

/**
 * @internal Break a coordinate variable to separate the dimension and
 * the variable.
 *
 * This is called from nc_rename_dim() and nc_rename_var(). In some
 * renames, the coord variable must stay, but it is no longer a coord
 * variable. This function changes a coord var into an ordinary
 * variable.
 *
 * @param grp Pointer to group info struct.
 * @param coord_var Pointer to variable info struct.
 * @param dim Pointer to dimension info struct.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_ENOMEM Out of memory.
 * @author Quincey Koziol, Ed Hartnett
 */
int
nc4_break_coord_var(NC_GRP_INFO_T *grp, NC_VAR_INFO_T *coord_var,
                    NC_DIM_INFO_T *dim)
{
    int retval;
    NC_HDF5_VAR_INFO_T* coord_h5var = (NC_HDF5_VAR_INFO_T*)coord_var->format_var_info;

    /* Sanity checks */
    assert(grp && coord_var && dim && dim->coord_var == coord_var &&
           coord_var->dim[0] == dim && coord_var->dimids[0] == dim->hdr.id &&
           !((NC_HDF5_DIM_INFO_T *)(dim->format_dim_info))->hdf_dimscaleid);
    LOG((3, "%s dim %s was associated with var %s, but now has different name",
         __func__, dim->hdr.name, coord_var->hdr.name));

    /* If we're replacing an existing dimscale dataset, go to
     * every var in the file and detach this dimension scale. */
    if ((retval = rec_detach_scales(grp->nc4_info->root_grp,
                                    dim->hdr.id,
                                    ((NC_HDF5_VAR_INFO_T *)(coord_var->format_var_info))->hdf_datasetid)))
        return retval;

    /* Allow attached dimscales to be tracked on the [former]
     * coordinate variable */
    if (coord_var->ndims)
    {
        /* Coordinate variables shouldn't have dimscales attached. */
        assert(!coord_h5var->dimscale_attached);

        /* Allocate space for tracking them */
        if (!(coord_h5var->dimscale_attached = calloc(coord_var->ndims,
                                                    sizeof(nc_bool_t))))
            return NC_ENOMEM;
    }

    /* Detach dimension from variable */
    coord_h5var->dimscale = NC_FALSE;
    dim->coord_var = NULL;

    /* Set state transition indicators */
    coord_var->was_coord_var = NC_TRUE;
    coord_var->became_coord_var = NC_FALSE;

    return NC_NOERR;
}

/**
 * @internal Delete an existing dimscale-only dataset.
 *
 * A dimscale-only HDF5 dataset is created when a dim is defined
 * without an accompanying coordinate variable.
 *
 * Sometimes, during renames, or late creation of variables, an
 * existing, dimscale-only dataset must be removed. This means
 * detaching all variables that use the dataset, then closing and
 * unlinking it.
 *
 * @param grp The grp of the dimscale-only dataset to be deleted, or a
 * higher group in the hierarchy (ex. root group).
 * @param dimid id of the dimension
 * @param dim Pointer to the dim with the dimscale-only dataset to be
 * deleted.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EHDFERR HDF5 error.
 * @author Ed Hartnett
 */
int
delete_dimscale_dataset(NC_GRP_INFO_T *grp, int dimid, NC_DIM_INFO_T *dim)
{
    NC_HDF5_DIM_INFO_T *hdf5_dim;
    NC_HDF5_GRP_INFO_T *hdf5_grp;
    int retval;

    assert(grp && grp->format_grp_info && dim && dim->format_dim_info);
    LOG((2, "%s: deleting dimscale dataset %s dimid %d", __func__, dim->hdr.name,
         dimid));

    /* Get HDF5 specific grp and dim info. */
    hdf5_dim = (NC_HDF5_DIM_INFO_T *)dim->format_dim_info;
    hdf5_grp = (NC_HDF5_GRP_INFO_T *)grp->format_grp_info;

    /* Detach dimscale from any variables using it */
    if ((retval = rec_detach_scales(grp, dimid, hdf5_dim->hdf_dimscaleid)) < 0)
        return retval;

    /* Close the HDF5 dataset */
    if (H5Dclose(hdf5_dim->hdf_dimscaleid) < 0)
        return NC_EHDFERR;
    hdf5_dim->hdf_dimscaleid = 0;

    /* Now delete the dataset. */
    if (H5Gunlink(hdf5_grp->hdf_grpid, dim->hdr.name) < 0)
        return NC_EHDFERR;

    return NC_NOERR;
}

/**
 * @internal Reform a coordinate variable from a dimension and a
 * variable.
 *
 * @param grp Pointer to group info struct.
 * @param var Pointer to variable info struct.
 * @param dim Pointer to dimension info struct.
 *
 * @return ::NC_NOERR No error.
 * @author Quincey Koziol, Ed Hartnett
 */
int
nc4_reform_coord_var(NC_GRP_INFO_T *grp, NC_VAR_INFO_T *var, NC_DIM_INFO_T *dim)
{
    NC_HDF5_DIM_INFO_T *hdf5_dim;
    NC_HDF5_GRP_INFO_T *hdf5_grp;
    NC_HDF5_VAR_INFO_T *hdf5_var;
    int need_to_reattach_scales = 0;
    int retval = NC_NOERR;

    assert(grp && grp->format_grp_info && var && var->format_var_info &&
           dim && dim->format_dim_info);
    LOG((3, "%s: dim->hdr.name %s var->hdr.name %s", __func__, dim->hdr.name,
         var->hdr.name));

    /* Get HDF5-specific dim, group, and var info. */
    hdf5_dim = (NC_HDF5_DIM_INFO_T *)dim->format_dim_info;
    hdf5_grp = (NC_HDF5_GRP_INFO_T *)grp->format_grp_info;
    hdf5_var = (NC_HDF5_VAR_INFO_T *)var->format_var_info;

    /* Detach dimscales from the [new] coordinate variable. */
    if (hdf5_var->dimscale_attached)
    {
        int dims_detached = 0;
        int finished = 0;

        /* Loop over all dimensions for variable. */
        for (unsigned int d = 0; d < var->ndims && !finished; d++)
        {
            /* Is there a dimscale attached to this axis? */
            if (hdf5_var->dimscale_attached[d])
            {
                NC_GRP_INFO_T *g;

                for (g = grp; g && !finished; g = g->parent)
                {
                    NC_DIM_INFO_T *dim1;
                    NC_HDF5_DIM_INFO_T *hdf5_dim1;

                    for (size_t k = 0; k < ncindexsize(g->dim); k++)
                    {
                        dim1 = (NC_DIM_INFO_T *)ncindexith(g->dim, k);
                        assert(dim1 && dim1->format_dim_info);
                        hdf5_dim1 = (NC_HDF5_DIM_INFO_T *)dim1->format_dim_info;

                        if (var->dimids[d] == dim1->hdr.id)
                        {
                            hid_t dim_datasetid;  /* Dataset ID for dimension */

                            /* Find dataset ID for dimension */
                            if (dim1->coord_var)
                                dim_datasetid = ((NC_HDF5_VAR_INFO_T *)
                                                 (dim1->coord_var->format_var_info))->hdf_datasetid;
                            else
                                dim_datasetid = hdf5_dim1->hdf_dimscaleid;

                            /* dim_datasetid may be 0 in some cases when
                             * renames of dims and vars are happening. In
                             * this case, the scale has already been
                             * detached. */
                            if (dim_datasetid > 0)
                            {
                                LOG((3, "detaching scale from %s", var->hdr.name));
                                if (H5DSdetach_scale(hdf5_var->hdf_datasetid,
                                                     dim_datasetid, d) < 0)
                                    BAIL(NC_EHDFERR);
                            }
                            hdf5_var->dimscale_attached[d] = NC_FALSE;
                            if (dims_detached++ == var->ndims)
                                finished++;
                        }
                    }
                }
            }
        } /* next variable dimension */

        /* Release & reset the array tracking attached dimscales. */
        free(hdf5_var->dimscale_attached);
        hdf5_var->dimscale_attached = NULL;
        need_to_reattach_scales++;
    }

    /* Use variable's dataset ID for the dimscale ID. */
    if (hdf5_dim->hdf_dimscaleid && grp != NULL)
    {
        LOG((3, "closing and unlinking dimscale dataset %s", dim->hdr.name));
        if (H5Dclose(hdf5_dim->hdf_dimscaleid) < 0)
            BAIL(NC_EHDFERR);
        hdf5_dim->hdf_dimscaleid = 0;

        /* Now delete the dimscale's dataset (it will be recreated
           later, if necessary). */
        if (H5Gunlink(hdf5_grp->hdf_grpid, dim->hdr.name) < 0)
            return NC_EDIMMETA;
    }

    /* Attach variable to dimension. */
    hdf5_var->dimscale = NC_TRUE;
    dim->coord_var = var;

    /* Check if this variable used to be a coord. var */
    if (need_to_reattach_scales || (var->was_coord_var && grp != NULL))
    {
        /* Reattach the scale everywhere it is used. (Recall that netCDF
         * dimscales are always 1-D) */
        if ((retval = rec_reattach_scales(grp->nc4_info->root_grp,
                                          var->dimids[0], hdf5_var->hdf_datasetid)))
            return retval;

        /* Set state transition indicator (cancels earlier
         * transition). */
        var->was_coord_var = NC_FALSE;
    }

    /* Set state transition indicator */
    var->became_coord_var = NC_TRUE;

exit:
    return retval;
}

/**
 * @internal Close HDF5 resources for global atts in a group.
 *
 * @param grp Pointer to group info struct.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EHDFERR HDF5 error.
 * @author Ed Hartnett
 */
static int
close_gatts(NC_GRP_INFO_T *grp)
{
    NC_ATT_INFO_T *att;

    for (size_t a = 0; a < ncindexsize(grp->att); a++)
    {
        att = (NC_ATT_INFO_T *)ncindexith(grp->att, a);
        assert(att && att->format_att_info);
        nc4_HDF5_close_att(att);
    }
    return NC_NOERR;
}

/**
 * @internal Close HDF5 resources for a single attribute
 *
 * @param att Pointer to att info struct.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EHDFERR HDF5 error.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
nc4_HDF5_close_att(NC_ATT_INFO_T *att)
{
        NC_HDF5_ATT_INFO_T *hdf5_att;

        assert(att && att->format_att_info);
        hdf5_att = (NC_HDF5_ATT_INFO_T *)att->format_att_info;

        /* Close the HDF5 typeid. */
        if (hdf5_att->native_hdf_typeid &&
            H5Tclose(hdf5_att->native_hdf_typeid) < 0)
            return NC_EHDFERR;

	nullfree(hdf5_att);
	att->format_att_info = NULL;	
	return NC_NOERR;
}

/**
 * @internal Close HDF5 resources for vars in a group.
 *
 * @param grp Pointer to group info struct.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EHDFERR HDF5 error.
 * @author Ed Hartnett
 */
static int
close_vars(NC_GRP_INFO_T *grp)
{
    NC_VAR_INFO_T *var;
    NC_HDF5_VAR_INFO_T *hdf5_var;
    NC_ATT_INFO_T *att;

    for (size_t i = 0; i < ncindexsize(grp->vars); i++)
    {
        var = (NC_VAR_INFO_T *)ncindexith(grp->vars, i);
        assert(var && var->format_var_info);
        hdf5_var = (NC_HDF5_VAR_INFO_T *)var->format_var_info;

        /* Close the HDF5 dataset associated with this var. */
        if (hdf5_var->hdf_datasetid)
        {
            LOG((3, "closing HDF5 dataset %lld", hdf5_var->hdf_datasetid));
            if (H5Dclose(hdf5_var->hdf_datasetid) < 0)
                return NC_EHDFERR;

            if (var->fill_value)
            {
                if (var->type_info)
                {
		    int stat = NC_NOERR;
		    if((stat = NC_reclaim_data(grp->nc4_info->controller,var->type_info->hdr.id,var->fill_value,1)))
		        return stat;
		    nullfree(var->fill_value);
                }
		var->fill_value = NULL;
            }
        }

        /* Free the HDF5 typeids. */
        if (var->type_info->rc == 1)
        {
	    if(var->type_info->hdr.id <= NC_STRING)
		/* This was a constructed atomic type; free its info */ 
		nc4_HDF5_close_type(var->type_info);
        }

        for (size_t a = 0; a < ncindexsize(var->att); a++)
        {
            att = (NC_ATT_INFO_T *)ncindexith(var->att, a);
            assert(att && att->format_att_info);
	    nc4_HDF5_close_att(att);
        }

        /* Delete any HDF5 dimscale objid information. */
        if (hdf5_var->dimscale_hdf5_objids)
            free(hdf5_var->dimscale_hdf5_objids);
	/* Delete information about the attachment status of dimscales. */
	if (hdf5_var->dimscale_attached)
	    free(hdf5_var->dimscale_attached);
	nullfree(hdf5_var);

	/* Reclaim filters */
	if(var->filters != NULL) {
	    (void)NC4_hdf5_filter_freelist(var);
	}
	var->filters = NULL;
    }

    return NC_NOERR;
}

/**
 * @internal Close HDF5 resources for dims in a group.
 *
 * @param grp Pointer to group info struct.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EHDFERR HDF5 error.
 * @author Ed Hartnett
 */
static int
close_dims(NC_GRP_INFO_T *grp)
{
    NC_DIM_INFO_T *dim;

    for (size_t i = 0; i < ncindexsize(grp->dim); i++)
    {
        NC_HDF5_DIM_INFO_T *hdf5_dim;

        dim = (NC_DIM_INFO_T *)ncindexith(grp->dim, i);
        assert(dim && dim->format_dim_info);
        hdf5_dim = (NC_HDF5_DIM_INFO_T *)dim->format_dim_info;

        /* If this is a dim without a coordinate variable, then close
         * the HDF5 DIM_WITHOUT_VARIABLE dataset associated with this
         * dim. */
        if (hdf5_dim->hdf_dimscaleid && H5Dclose(hdf5_dim->hdf_dimscaleid) < 0)
            return NC_EHDFERR;
	nullfree(hdf5_dim);
    }

    return NC_NOERR;
}

/**
 * @internal Close HDF5 resources for types in a group.  Set values to
 * 0 after closing types. Because of type reference counters, these
 * closes can be called multiple times.
 *
 * @param grp Pointer to group info struct.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EHDFERR HDF5 error.
 * @author Ed Hartnett, Dennis Heimbigner
 */
static int
close_types(NC_GRP_INFO_T *grp)
{
    for (size_t i = 0; i < ncindexsize(grp->type); i++)
    {
        NC_TYPE_INFO_T *type;

        type = (NC_TYPE_INFO_T *)ncindexith(grp->type, i);
        assert(type && type->format_type_info);
	nc4_HDF5_close_type(type);
    }

    return NC_NOERR;
}

/**
 * @internal Close a single type instance
 *
 * @param type Pointer to type info struct.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EHDFERR HDF5 error.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
nc4_HDF5_close_type(NC_TYPE_INFO_T* type)
{
        NC_HDF5_TYPE_INFO_T *hdf5_type;

        assert(type && type->format_type_info);

        /* Get HDF5-specific type info. */
        hdf5_type = (NC_HDF5_TYPE_INFO_T *)type->format_type_info;

        /* Close any open user-defined HDF5 typeids. */
        if (hdf5_type->hdf_typeid && H5Tclose(hdf5_type->hdf_typeid) < 0)
            return NC_EHDFERR;
        hdf5_type->hdf_typeid = 0;
        if (hdf5_type->native_hdf_typeid &&
            H5Tclose(hdf5_type->native_hdf_typeid) < 0)
            return NC_EHDFERR;
        hdf5_type->native_hdf_typeid = 0;
	nullfree(hdf5_type);

    return NC_NOERR;
}

/**
 * @internal Recursively free HDF5 objects for a group (and everything
 * it contains).
 *
 * @param grp Pointer to group info struct.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EHDFERR HDF5 error.
 * @author Ed Hartnett
 */
int
nc4_rec_grp_HDF5_del(NC_GRP_INFO_T *grp)
{
    NC_HDF5_GRP_INFO_T *hdf5_grp;
    int retval;

    assert(grp && grp->format_grp_info);
    LOG((3, "%s: grp->name %s", __func__, grp->hdr.name));

    hdf5_grp = (NC_HDF5_GRP_INFO_T *)grp->format_grp_info;

    /* Recursively call this function for each child, if any, stopping
     * if there is an error. */
    for (size_t i = 0; i < ncindexsize(grp->children); i++)
        if ((retval = nc4_rec_grp_HDF5_del((NC_GRP_INFO_T *)ncindexith(grp->children,
                                                                       i))))
            return retval;

    /* Close HDF5 resources associated with global attributes. */
    if ((retval = close_gatts(grp)))
        return retval;

    /* Close HDF5 resources associated with vars. */
    if ((retval = close_vars(grp)))
        return retval;

    /* Close HDF5 resources associated with dims. */
    if ((retval = close_dims(grp)))
        return retval;

    /* Close HDF5 resources associated with types. */
    if ((retval = close_types(grp)))
        return retval;

    /* Close the HDF5 group. */
    LOG((4, "%s: closing group %s", __func__, grp->hdr.name));
    if (hdf5_grp->hdf_grpid && H5Gclose(hdf5_grp->hdf_grpid) < 0)
        return NC_EHDFERR;

    nullfree(hdf5_grp);

    return NC_NOERR;
}

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
 * @author Ed Hartnett
 */
int
nc4_hdf5_find_grp_h5_var(int ncid, int varid, NC_FILE_INFO_T **h5,
                         NC_GRP_INFO_T **grp, NC_VAR_INFO_T **var)
{
    NC_VAR_INFO_T *my_var;
    int retval;
    /* Delegate to libsrc4 */
    if((retval = nc4_find_grp_h5_var(ncid,varid,h5,grp,&my_var))) return retval;
    /* Do we need to read var metadata? (hdf5 specific) */
    if (!my_var->meta_read && my_var->created)
        if ((retval = nc4_get_var_meta(my_var)))
            return retval;
    if (var) *var = my_var;
    return NC_NOERR;
}

/**
 * @internal Given an ncid, varid, and attribute name, return
 * normalized name and pointers to the file, group, var, and att info
 * structs. Lazy reads of attributes and variable metadata are done as
 * needed.
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
 * @author Ed Hartnett
 */
int
nc4_hdf5_find_grp_var_att(int ncid, int varid, const char *name, int attnum,
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

    /* Get either the global or a variable attribute list. */
    if (varid == NC_GLOBAL)
    {
        /* Do we need to read the atts? */
        if (!my_grp->atts_read)
            if ((retval = nc4_read_atts(my_grp, NULL)))
                return retval;

        attlist = my_grp->att;
    }
    else
    {
        if (!(my_var = (NC_VAR_INFO_T *)ncindexith(my_grp->vars, (size_t)varid)))
            return NC_ENOTVAR;

        /* Do we need to read the var attributes? */
        if (!my_var->atts_read)
            if ((retval = nc4_read_atts(my_grp, my_var)))
                return retval;

        /* Do we need to read var metadata? */
        if (!my_var->meta_read && my_var->created)
            if ((retval = nc4_get_var_meta(my_var)))
                return retval;

        attlist = my_var->att;
    }
    assert(attlist);

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
    {
        strncpy(norm_name, my_norm_name, NC_MAX_NAME);
        norm_name[NC_MAX_NAME] = 0;
    }
    if (h5)
        *h5 = my_h5;
    if (grp)
        *grp = my_grp;
    if (var)
        *var = my_var;
    if (att)
        *att = my_att;

    return NC_NOERR;
}

/**
 * @internal Get the file chunk cache settings from HDF5.
 *
 * @param ncid File ID of a NetCDF/HDF5 file.
 * @param sizep Pointer that gets size in bytes to set cache. Ignored
 * if NULL.
 * @param nelemsp Pointer that gets number of elements to hold in
 * cache. Ignored if NULL.
 * @param preemptionp Pointer that gets preemption strategy (between 0
 * and 1). Ignored if NULL.
 *
 * @return ::NC_NOERR No error.
 * @author Ed Hartnett
 */
int
nc4_hdf5_get_chunk_cache(int ncid, size_t *sizep, size_t *nelemsp,
		     float *preemptionp)
{
    NC_FILE_INFO_T *h5;
    NC_HDF5_FILE_INFO_T *hdf5_info;
    hid_t plistid;
    double dpreemption;
    int retval;
    
    /* Find info for this file, group, and h5 info. */
    if ((retval = nc4_find_nc_grp_h5(ncid, NULL, NULL, &h5)))
        return retval;
    assert(h5 && h5->format_file_info);
    hdf5_info = (NC_HDF5_FILE_INFO_T *)h5->format_file_info;

    /* Get the file access property list. */
    if ((plistid = H5Fget_access_plist(hdf5_info->hdfid)) < 0)
	return NC_EHDFERR;

    /* Get the chunk cache values from HDF5 for this property list. */
    if (H5Pget_cache(plistid, NULL, nelemsp, sizep, &dpreemption) < 0)
	return NC_EHDFERR;
    if (preemptionp)
	*preemptionp = (float)dpreemption;

    return NC_NOERR;
}

#ifdef LOGGING
/* We will need to check against nc log level from nc4internal.c. */
extern int nc_log_level;

/**
 * @internal This is like nc_set_log_level(), but will also turn on
 * HDF5 internal logging, in addition to netCDF logging. This should
 * never be called by the user. It is called in open/create when the
 * nc logging level has changed.
 *
 * @return ::NC_NOERR No error.
 * @author Ed Hartnett
 */
int
hdf5_set_log_level()
{
    /* If the user wants to completely turn off logging, turn off HDF5
       logging too. Now I truly can't think of what to do if this
       fails, so just ignore the return code. */
    if (nc_log_level == NC_TURN_OFF_LOGGING)
    {
        set_auto(NULL, NULL);
        LOG((1, "HDF5 error messages turned off!"));
    }
    else
    {
        if (set_auto((H5E_auto_t)&H5Eprint1, stderr) < 0)
            LOG((0, "H5Eset_auto failed!"));
        LOG((1, "HDF5 error messages turned on."));
    }

    return NC_NOERR;
}

void
nc_log_hdf5(void)
{
#ifdef USE_HDF5
    H5Eprint1(NULL);
#endif /* USE_HDF5 */
}

#endif /* LOGGING */

#if 0
#ifdef _WIN32

/**
 * Converts the filename from ANSI to UTF-8 if HDF5 >= 1.10.6. 
 * nc4_hdf5_free_pathbuf must be called to free pb.
 *
 * @param pb Pointer that conversion information is stored.
 * @param path The filename to be converted.
 *
 * @return The converted filename if succeeded. NULL if failed.
 */
const char *
nc4_ndf5_ansi_to_utf8(pathbuf_t *pb, const char *path)
{
    const uint UTF8_MAJNUM = 1;
    const uint UTF8_MINNUM = 10;
    const uint UTF8_RELNUM = 6;
    static enum {UNDEF, ANSI, UTF8} hdf5_encoding = UNDEF;
    wchar_t wbuf[MAX_PATH];
    wchar_t *ws = NULL;
    char *ns = NULL;
    int n;

    if (hdf5_encoding == UNDEF) {
#ifdef HDF5_UTF8_PATHS
	hdf5_encoding = UTF8;
#else
	hdf5_encoding = ANSI;
#endif
    }
    if (hdf5_encoding == ANSI) {
        pb->ptr = NULL;
        return path;
    }

    n = MultiByteToWideChar(CP_ACP, 0, path, -1, NULL, 0);
    if (!n) {
        errno = EILSEQ;
        goto done;
    }
    ws = n <= _countof(wbuf) ? wbuf : malloc(sizeof *ws * n);
    if (!ws)
        goto done;
    if (!MultiByteToWideChar(CP_ACP, 0, path, -1, ws, n)) {
        errno = EILSEQ;
        goto done;
    }

    n = WideCharToMultiByte(CP_UTF8, 0, ws, -1, NULL, 0, NULL, NULL);
    if (!n) {
        errno = EILSEQ;
        goto done;
    }
    ns = n <= sizeof pb->buffer ? pb->buffer : malloc(n);
    if (!ns)
        goto done;
    if (!WideCharToMultiByte(CP_UTF8, 0, ws, -1, ns, n, NULL, NULL)) {
        if (ns != pb->buffer)
            free(ns);
        ns = NULL;
        errno = EILSEQ;
        goto done;
    }

done:
    if (ws != wbuf)
        free (ws);

    pb->ptr = ns;
    return ns;
}

/**
 * Free the conversion information used by nc4_ndf5_ansi_to_utf8.
 *
 * @param pb Pointer that hold conversion information to be freed.
 */
void
nc4_hdf5_free_pathbuf(pathbuf_t *pb)
{
    if (pb->ptr && pb->ptr != pb->buffer)
        free(pb->ptr);
}

#endif /* _WIN32 */
#endif /*0*/
