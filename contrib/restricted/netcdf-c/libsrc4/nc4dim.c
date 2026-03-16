/* Copyright 2003-2018, University Corporation for Atmospheric
 * Research. See the COPYRIGHT file for copying and redistribution
 * conditions. */
/**
 * @file
 * @internal This file is part of netcdf-4, a netCDF-like interface
 * for HDF5, or a HDF5 backend for netCDF, depending on your point of
 * view.
 *
 * This file handles the nc4 dimension functions.
 *
 * @author Ed Hartnett
 */

#include "nc4internal.h"
#include "nc4dispatch.h"

/**
 * @internal Netcdf-4 files might have more than one unlimited
 * dimension, but return the first one anyway.
 *
 * @note that this code is inconsistent with nc_inq
 *
 * @param ncid File and group ID.
 * @param unlimdimidp Pointer that gets ID of first unlimited
 * dimension, or -1.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @author Ed Hartnett
 */
int
NC4_inq_unlimdim(int ncid, int *unlimdimidp)
{
    NC_GRP_INFO_T *grp, *g;
    NC_FILE_INFO_T *h5;
    NC_DIM_INFO_T *dim;
    int found = 0;
    int retval;

    LOG((2, "%s: called", __func__));

    if ((retval = nc4_find_grp_h5(ncid, &grp, &h5)))
        return retval;
    assert(h5 && grp);

    if (unlimdimidp)
    {
        /* According to netcdf-3 manual, return -1 if there is no unlimited
           dimension. */
        *unlimdimidp = -1;
        for (g = grp; g && !found; g = g->parent)
        {
            for(size_t i=0;i<ncindexsize(grp->dim);i++)
            {
                dim = (NC_DIM_INFO_T*)ncindexith(grp->dim,i);
                if(dim == NULL) continue;
                if (dim->unlimited)
                {
                    *unlimdimidp = dim->hdr.id;
                    found++;
                    break;
                }
            }
        }
    }

    return NC_NOERR;
}

/**
 * @internal Given dim name, find its id.
 * Fully qualified names are legal
 * @param ncid File and group ID.
 * @param name Name of the dimension to find.
 * @param idp Pointer that gets dimension ID.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_EBADDIM Dimension not found.
 * @return ::NC_EINVAL Invalid input. Name must be provided.
 * @author Ed Hartnett
 */
int
NC4_inq_dimid(int ncid, const char *name, int *idp)
{
    NC *nc = NULL;
    NC_GRP_INFO_T *grp = NULL;
    NC_GRP_INFO_T *g = NULL;
    NC_FILE_INFO_T *h5 = NULL;
    NC_DIM_INFO_T *dim = NULL;
    char norm_name[NC_MAX_NAME + 1];
    int retval = NC_NOERR;;
    int found = 0;

    LOG((2, "%s: ncid 0x%x name %s", __func__, ncid, name));

    /* Check input. */
    if (!name)
        {retval = NC_EINVAL; goto done;}

    /* If the first char is a /, this is a fully-qualified
     * name. Otherwise, this had better be a local name (i.e. no / in
     * the middle). */
    if (name[0] != '/' && strstr(name, "/"))
        {retval = NC_EINVAL; goto done;}

    /* Find metadata for this file. */
    if ((retval = nc4_find_nc_grp_h5(ncid, &nc, &grp, &h5)))
        goto done;
    assert(h5 && nc && grp);

    /* Normalize name. */
    if ((retval = nc4_normalize_name(name, norm_name)))
        goto done;;

    /* If this is a fqn, then walk the sequence of parent groups to the last group
       and see if that group has a dimension of the right name */
    if(name[0] == '/') { /* FQN */
	int rootncid = (grp->nc4_info->root_grp->hdr.id | grp->nc4_info->controller->ext_ncid);
	int parent = 0;
	char* lastname = strrchr(norm_name,'/'); /* break off the last segment: the type name */
	if(lastname == norm_name)
	    {retval = NC_EINVAL; goto done;}
	*lastname++ = '\0'; /* break off the lastsegment */
	if((retval = NC4_inq_grp_full_ncid(rootncid,norm_name,&parent))) 
	    goto done;
	/* Get parent info */
	if((retval=nc4_find_nc4_grp(parent,&grp)))
	    goto done;
	/* See if dim exists in this group */
        dim = (NC_DIM_INFO_T*)ncindexlookup(grp->dim,lastname);
	if(dim == NULL) 	
	    {retval = NC_EBADTYPE; goto done;}
	goto done;
    }

    /* check for a name match in this group and its parents */
    found = 0;
    for (g = grp; g ; g = g->parent) {
        dim = (NC_DIM_INFO_T*)ncindexlookup(g->dim,norm_name);
        if(dim != NULL) {found = 1; break;}
    }
    if(!found)
        {retval = NC_EBADDIM; goto done;}

done:
     if(retval == NC_NOERR) {
         assert(dim != NULL);
         if (idp)
            *idp = dim->hdr.id;
    }
    return retval;
}

/**
 * @internal Returns an array of unlimited dimension ids.The user can
 * get the number of unlimited dimensions by first calling this with
 * NULL for the second pointer.
 *
 * @param ncid File and group ID.
 * @param nunlimdimsp Pointer that gets the number of unlimited
 * dimensions. Ignored if NULL.
 * @param unlimdimidsp Pointer that gets array of unlimited dimension
 * ID. Ignored if NULL.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
NC4_inq_unlimdims(int ncid, int *nunlimdimsp, int *unlimdimidsp)
{
    NC_DIM_INFO_T *dim;
    NC_GRP_INFO_T *grp;
    NC *nc;
    NC_FILE_INFO_T *h5;
    int num_unlim = 0;
    int retval;

    LOG((2, "%s: ncid 0x%x", __func__, ncid));

    /* Find info for this file and group, and set pointer to each. */
    if ((retval = nc4_find_nc_grp_h5(ncid, &nc, &grp, &h5)))
        return retval;
    assert(h5 && nc && grp);

    /* Get our dim info. */
    assert(h5);
    {
        for(size_t i=0;i<ncindexsize(grp->dim);i++)
        {
            dim = (NC_DIM_INFO_T*)ncindexith(grp->dim,i);
            if(dim == NULL) continue;
            if (dim->unlimited)
            {
                if (unlimdimidsp)
                    unlimdimidsp[num_unlim] = dim->hdr.id;
                num_unlim++;
            }
        }
    }

    /* Give the number if the user wants it. */
    if (nunlimdimsp)
        *nunlimdimsp = num_unlim;

    return NC_NOERR;
}
