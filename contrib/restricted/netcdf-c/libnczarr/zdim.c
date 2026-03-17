/* Copyright 2003-2019, University Corporation for Atmospheric
 * Research. See the COPYRIGHT file for copying and redistribution
 * conditions. */

/**
 * @file @internal This file is part of netcdf-4, a netCDF-like
 * interface for NCZ, or a ZARR backend for netCDF, depending on your
 * point of view.
 *
 * This file includes the ZARR code to deal with dimensions.
 *
 * @author Dennis Heimbigner, Ed Hartnett
 */

#include "zincludes.h"

/**
 * @internal Dimensions are defined in attributes attached to the
 * appropriate group in the data file.
 *
 * @param ncid File and group ID.
 * @param name Name of the new dimension.
 * @param len Length of the new dimension.
 * @param idp Pointer that gets the ID of the new dimension. Ignored
 * if NULL.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_EMAXNAME Name is too long.
 * @return ::NC_EBADNAME Name breaks netCDF name rules.
 * @return ::NC_EINVAL Invalid input.
 * @return ::NC_EPERM Read-only file.
 * @return ::NC_EUNLIMIT Only one unlimited dim for classic model.
 * @return ::NC_ENOTINDEFINE Not in define mode.
 * @return ::NC_EDIMSIZE Dim length too large.
 * @return ::NC_ENAMEINUSE Name already in use in group.
 * @return ::NC_ENOMEM Out of memory.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_def_dim(int ncid, const char *name, size_t len, int *idp)
{
    NC *nc;
    NC_GRP_INFO_T *grp;
    NC_FILE_INFO_T *h5;
    NC_DIM_INFO_T *dim;
    char norm_name[NC_MAX_NAME + 1];
    int stat = NC_NOERR;

    LOG((2, "%s: ncid 0x%x name %s len %d", __func__, ncid, name,
         (int)len));

    /* Find our global metadata structure. */
    if ((stat = nc4_find_nc_grp_h5(ncid, &nc, &grp, &h5)))
        return stat;
    assert(h5 && nc && grp);

    /* If the file is read-only, return an error. */
    if (h5->no_write)
        return NC_EPERM;

    /* Check some stuff if strict nc3 rules are in effect. */
    if (h5->cmode & NC_CLASSIC_MODEL)
    {
#ifdef LOOK
        /* Only one limited dimension for strict nc3. */
        if (len == NC_UNLIMITED) {
	    int i;
            for(i=0;i<ncindexsize(grp->dim);i++) {
                dim = (NC_DIM_INFO_T*)ncindexith(grp->dim,i);
                if(dim == NULL) continue;
                if (dim->unlimited)
                    return NC_EUNLIMIT;
            }
        }
        /* Must be in define mode for stict nc3. */
        if (!(h5->flags & NC_INDEF))
            return NC_ENOTINDEFINE;
#endif
    }

    /* Make sure this is a valid netcdf name. */
    if ((stat = nc4_check_name(name, norm_name)))
        return stat;

    /* Since unlimited is supported, len >= 0 */
    if(len < 0)
        return NC_EDIMSIZE;

    /* For classic model: dim length has to fit in a 32-bit unsigned
     * int, as permitted for 64-bit offset format. */
    if (h5->cmode & NC_CLASSIC_MODEL)
        if(len > X_UINT_MAX) /* Backward compat */
            return NC_EDIMSIZE;

    /* Make sure the name is not already in use. */
    dim = (NC_DIM_INFO_T*)ncindexlookup(grp->dim,norm_name);
    if(dim != NULL)
        return NC_ENAMEINUSE;

    /* If it's not in define mode, enter define mode. Do this only
     * after checking all input data, so we only enter define mode if
     * input is good. */
    if (!(h5->flags & NC_INDEF))
        if ((stat = NCZ_redef(ncid)))
            return stat;

    /* Add a dimension to the list. The ID must come from the file
     * information, since dimids are visible in more than one group. */
    if ((stat = nc4_dim_list_add(grp, norm_name, len, -1, &dim)))
        return stat;

    {
        NCZ_DIM_INFO_T* diminfo = NULL;
        /* Create struct for NCZ-specific dim info. */
        if (!(diminfo = calloc(1, sizeof(NCZ_DIM_INFO_T))))
            return NC_ENOMEM;
        dim->format_dim_info = diminfo;
        diminfo->common.file = h5;
    }    

    /* Pass back the dimid. */
    if (idp)
        *idp = dim->hdr.id;

    return stat;
}

/**
 * @internal Find out name and len of a dim. For an unlimited
 * dimension, the length is the largest length so far written. If the
 * name of lenp pointers are NULL, they will be ignored.
 *
 * @param ncid File and group ID.
 * @param dimid Dimension ID.
 * @param name Pointer that gets name of the dimension.
 * @param lenp Pointer that gets length of dimension.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_EDIMSIZE Dimension length too large.
 * @return ::NC_EBADDIM Dimension not found.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_inq_dim(int ncid, int dimid, char *name, size_t *lenp)
{
    NC *nc;
    NC_FILE_INFO_T *h5;
    NC_GRP_INFO_T *grp, *dim_grp;
    NC_DIM_INFO_T *dim;
    int stat = NC_NOERR;

    LOG((2, "%s: ncid 0x%x dimid %d", __func__, ncid, dimid));

    /* Find our global metadata structure. */
    if ((stat = nc4_find_nc_grp_h5(ncid, &nc, &grp, &h5)))
        return stat;
    assert(h5 && nc && grp);

    /* Find the dimension and its home group. */
    if ((stat = nc4_find_dim(grp, dimid, &dim, &dim_grp)))
        return stat;
    assert(dim);

    /* Return the dimension name, if the caller wants it. */
    if (name && dim->hdr.name)
        strcpy(name, dim->hdr.name);

    /* Return the dimension length, if the caller wants it. */
    if (lenp)
    {
#ifdef LOOK
        if (dim->unlimited)
        {
            /* Since this is an unlimited dimension, go to the file
               and see how many records there are. Take the max number
               of records from all the vars that share this
               dimension. */
            *lenp = 0;
            if ((stat = ncz_find_dim_len(dim_grp, dimid, &lenp)))
                return stat;
        }
        else
#endif
        {
            if (dim->too_long)
            {
                stat = NC_EDIMSIZE;
                *lenp = NC_MAX_UINT;
            }
            else
                *lenp = dim->len;
        }
    }

    return stat;
}

/**
 * @internal Rename a dimension, for those who like to prevaricate.
 *
 * @note If we're not in define mode, new name must be of equal or
 * less size, if strict nc3 rules are in effect for this file. But we
 * don't check this because reproducing the exact classic behavior
 * would be too difficult. See github issue #1340.
 *
 * @param ncid File and group ID.
 * @param dimid Dimension ID.
 * @param name New dimension name.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_EHDFERR ZARR returned error.
 * @return ::NC_ENOMEM Out of memory.
 * @return ::NC_EINVAL Name must be provided.
 * @return ::NC_ENAMEINUSE Name is already in use in group.
 * @return ::NC_EMAXNAME Name is too long.
 * @return ::NC_EBADDIM Dimension not found.
 * @return ::NC_EBADNAME Name breaks netCDF name rules.
 * @return ::NC_EDIMMETA Unable to delete ZARR dataset.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_rename_dim(int ncid, int dimid, const char *name)
{
    NC_GRP_INFO_T *grp;
    NC_DIM_INFO_T *dim;
    NC_FILE_INFO_T *h5;
    char norm_name[NC_MAX_NAME + 1];
    int stat;

    /* Note: name is new name */
    if (!name)
        return NC_EINVAL;

    LOG((2, "%s: ncid 0x%x dimid %d name %s", __func__, ncid,
         dimid, name));

    /* Find info for this file and group, and set pointer to each. */
    if ((stat = nc4_find_grp_h5(ncid, &grp, &h5)))
        return stat;
    assert(h5 && grp);

    /* Trying to write to a read-only file? No way, Jose! */
    if (h5->no_write)
        return NC_EPERM;

    /* Make sure this is a valid netcdf name. */
    if ((stat = nc4_check_name(name, norm_name)))
        return stat;

    /* Get the original dim. */
    if ((stat = nc4_find_dim(grp, dimid, &dim, NULL)))
        return stat;
    assert(dim && dim->format_dim_info);

    /* Check if new name is in use. */
    if (ncindexlookup(grp->dim, norm_name))
        return NC_ENAMEINUSE;

    /* Give the dimension its new name in metadata. UTF8 normalization
     * has been done. */
    assert(dim->hdr.name);
    free(dim->hdr.name);
    if (!(dim->hdr.name = strdup(norm_name)))
        return NC_ENOMEM;
    LOG((3, "dim is now named %s", dim->hdr.name));

    /* rebuild index. */
    if (!ncindexrebuild(grp->dim))
        return NC_EINTERNAL;

    return NC_NOERR;
}
