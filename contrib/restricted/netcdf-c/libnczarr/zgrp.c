/* Copyright 2005-2018, University Corporation for Atmospheric
 * Research. See COPYRIGHT file for copying and redistribution
 * conditions. */

/**
 * @file @internal This file is part of netcdf-4, a netCDF-like
 * interface for NCZ, or a ZARR backend for netCDF, depending on your
 * point of view.
 *
 * This file handles ZARR groups.
 *
 * @author Dennis Heimbigner, Ed Hartnett
 */

#include "zincludes.h"

/**
 * @internal Create a group. Its ncid is returned in the new_ncid
 * pointer.
 *
 * @param parent_ncid Parent group.
 * @param name Name of new group.
 * @param new_ncid Pointer that gets ncid for new group.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ESTRICTNC3 Classic model in use for this file.
 * @return ::NC_ENOTNC4 Not a netCDF-4 file.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_def_grp(int parent_ncid, const char *name, int *new_ncid)
{
    NC_GRP_INFO_T *grp, *g;
    NC_FILE_INFO_T *h5;
    char norm_name[NC_MAX_NAME + 1];
    int stat;

    LOG((2, "%s: parent_ncid 0x%x name %s", __func__, parent_ncid, name));

    /* Find info for this file and group, and set pointer to each. */
    if ((stat = nc4_find_grp_h5(parent_ncid, &grp, &h5)))
        return stat;
    assert(h5);

    /* Check and normalize the name. */
    if ((stat = nc4_check_name(name, norm_name)))
        return stat;

    /* Check that this name is not in use as a var, grp, or type. */
    if ((stat = nc4_check_dup_name(grp, norm_name)))
        return stat;

    /* No groups in netcdf-3! */
    if (h5->cmode & NC_CLASSIC_MODEL)
        return NC_ESTRICTNC3;

    /* If it's not in define mode, switch to define mode. */
    if (!(h5->flags & NC_INDEF))
        if ((stat = NCZ_redef(parent_ncid)))
            return stat;

    /* Update internal lists to reflect new group. The actual NCZ
     * group creation will be done when metadata is written by a
     * sync. */
    if ((stat = nc4_grp_list_add(h5, grp, norm_name, &g)))
        return stat;
    if (!(g->format_grp_info = calloc(1, sizeof(NCZ_GRP_INFO_T))))
        return NC_ENOMEM;
    ((NCZ_GRP_INFO_T*)g->format_grp_info)->common.file = h5;

    /* For new groups, there are no atts to read from file. */
    g->atts_read = 1;

    /* Return the ncid to the user. */
    if (new_ncid)
        *new_ncid = grp->nc4_info->controller->ext_ncid | g->hdr.id;

    return NC_NOERR;
}

/**
 * @internal Rename a group.
 *
 * @param grpid Group ID.
 * @param name New name for group.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOTNC4 Not a netCDF-4 file.
 * @return ::NC_EPERM File opened read-only.
 * @return ::NC_EBADGRPID Renaming root forbidden.
 * @return ::NC_EHDFERR ZARR function returned error.
 * @return ::NC_ENOMEM Out of memory.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_rename_grp(int grpid, const char *name)
{
    NC_GRP_INFO_T *grp;
    NC_FILE_INFO_T *h5;
    char norm_name[NC_MAX_NAME + 1];
    int stat;

    LOG((2, "nc_rename_grp: grpid 0x%x name %s", grpid, name));

    /* Find info for this file and group, and set pointer to each. */
    if ((stat = nc4_find_grp_h5(grpid, &grp, &h5)))
        return stat;
    assert(h5 && grp && grp->format_grp_info);

    if (h5->no_write)
        return NC_EPERM; /* attempt to write to a read-only file */

    /* Do not allow renaming the root group */
    if (grp->parent == NULL)
        return NC_EBADGRPID;

    /* Check and normalize the name. */
    if ((stat = nc4_check_name(name, norm_name)))
        return stat;

    /* Check that this name is not in use as a var, grp, or type in the
     * parent group (i.e. the group that grp is in). */
    if ((stat = nc4_check_dup_name(grp->parent, norm_name)))
        return stat;

    /* If it's not in define mode, switch to define mode. */
    if (!(h5->flags & NC_INDEF))
        if ((stat = NCZ_redef(grpid)))
            return stat;

#ifdef LOOK
    /* Rename the group, if it exists in the file */
    if (zgrp->hdf_grpid)
    {
        ZGRP_INFO_T *parent_zgrp;
        parent_zgrp = (ZGRP_INFO_T *)grp->parent->format_grp_info;

        /* Close the group */
        if (H5Gclose(zgrp->hdf_grpid) < 0)
            return NC_EHDFERR;
        zgrp->hdf_grpid = 0;

        /* Attempt to rename & re-open the group, if the parent group is open */
        if (parent_zgrp->hdf_grpid)
        {
            /* Rename the group. */
            if (H5Lmove(parent_zgrp->hdf_grpid, grp->hdr.name,
                        parent_zgrp->hdf_grpid, name, H5P_DEFAULT,
                        H5P_DEFAULT) < 0)
                return NC_EHDFERR;

            /* Reopen the group, with the new name. */
            if ((zgrp->hdf_grpid = H5Gopen2(parent_zgrp->hdf_grpid, name,
                                                H5P_DEFAULT)) < 0)
                return NC_EHDFERR;
        }
    }
#endif

    /* Give the group its new name in metadata. UTF8 normalization
     * has been done. */
    free(grp->hdr.name);
    if (!(grp->hdr.name = strdup(norm_name)))
        return NC_ENOMEM;

    /* rebuild index. */
    if(!ncindexrebuild(grp->parent->children))
        return NC_EINTERNAL;

    return NC_NOERR;
}
