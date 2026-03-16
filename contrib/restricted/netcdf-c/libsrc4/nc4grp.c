/* Copyright 2005-2018, University Corporation for Atmospheric
 * Research. See COPYRIGHT file for copying and redistribution
 * conditions. */
/**
 * @file
 * @internal This file is part of netcdf-4, a netCDF-like interface
 * for HDF5, or a HDF5 backend for netCDF, depending on your point of
 * view.
 *
 * This file handles groups.
 *
 * @author Ed Hartnett
 */

#include "nc4internal.h"
#include "nc4dispatch.h"
/**
 * @internal Given an ncid and group name (NULL gets root group),
 * return the ncid of that group.
 *
 * @param ncid File and group ID.
 * @param name Pointer that gets name.
 * @param grp_ncid Pointer that gets group ncid.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOTNC4 Not a netCDF-4 file.
 * @return ::NC_ENOGRP Group not found.
 * @author Ed Hartnett
 */
int
NC4_inq_ncid(int ncid, const char *name, int *grp_ncid)
{
    NC_GRP_INFO_T *grp, *g;
    NC_FILE_INFO_T *h5;
    char norm_name[NC_MAX_NAME + 1];
    int retval;

    LOG((2, "nc_inq_ncid: ncid 0x%x name %s", ncid, name));

    /* Find info for this file and group, and set pointer to each. */
    if ((retval = nc4_find_grp_h5(ncid, &grp, &h5)))
        return retval;
    assert(h5);

    /* Short circuit the case of name == NULL => return the root group */
    if(name == NULL) {
	if(grp_ncid) {
	    NC_FILE_INFO_T* file = grp->nc4_info;
            *grp_ncid = file->controller->ext_ncid | file->root_grp->hdr.id;
	}	
	return NC_NOERR;
    }

    /* Normalize name. */
    if ((retval = nc4_check_name(name, norm_name)))
        return retval;

    g = (NC_GRP_INFO_T*)ncindexlookup(grp->children,norm_name);
    if(g != NULL)
    {
        if (grp_ncid)
            *grp_ncid = grp->nc4_info->controller->ext_ncid | g->hdr.id;
        return NC_NOERR;
    }

    /* If we got here, we didn't find the named group. */
    return NC_ENOGRP;
}

/**
 * @internal Given a location id, return the number of groups it
 * contains, and an array of their locids.
 *
 * @param ncid File and group ID.
 * @param numgrps Pointer that gets number of groups. Ignored if NULL.
 * @param ncids Pointer that gets array of ncids. Ignored if NULL.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @author Ed Hartnett
 */
int
NC4_inq_grps(int ncid, int *numgrps, int *ncids)
{
    NC_GRP_INFO_T *grp, *g;
    NC_FILE_INFO_T *h5;
    int num = 0;
    int retval;

    LOG((2, "nc_inq_grps: ncid 0x%x", ncid));

    /* Find info for this file and group, and set pointer to each. */
    if ((retval = nc4_find_grp_h5(ncid, &grp, &h5)))
        return retval;
    assert(h5);

    /* Count the number of groups in this group. */
    for(size_t i=0;i<ncindexsize(grp->children);i++)
    {
        g = (NC_GRP_INFO_T*)ncindexith(grp->children,i);
        if(g == NULL) continue;
        if (ncids)
        {
            /* Combine the nc_grpid in a bitwise or with the ext_ncid,
             * which allows the returned ncid to carry both file and
             * group information. */
            *ncids = g->hdr.id | g->nc4_info->controller->ext_ncid;
            ncids++;
        }
        num++;
    }

    if (numgrps)
        *numgrps = num;

    return NC_NOERR;
}

/**
 * @internal Given locid, find name of group. (Root group is named
 * "/".)
 *
 * @param ncid File and group ID.
 * @param name Pointer that gets name.

 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @author Ed Hartnett
 */
int
NC4_inq_grpname(int ncid, char *name)
{
    NC_GRP_INFO_T *grp;
    NC_FILE_INFO_T *h5;
    int retval;

    LOG((2, "nc_inq_grpname: ncid 0x%x", ncid));

    /* Find info for this file and group, and set pointer to each. */
    if ((retval = nc4_find_grp_h5(ncid, &grp, &h5)))
        return retval;
    assert(h5);

    /* Copy the name. */
    if (name)
        strcpy(name, grp->hdr.name);

    return NC_NOERR;
}

/**
 * @internal Find the full path name to the group represented by
 * ncid. Either pointer argument may be NULL; pass a NULL for the
 * third parameter to get the length of the full path name. The length
 * will not include room for a null pointer.
 *
 * @param ncid File and group ID.
 * @param lenp Pointer that gets length of full name.
 * @param full_name Pointer that gets name.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOMEM Out of memory.
 * @author Ed Hartnett
 */
int
NC4_inq_grpname_full(int ncid, size_t *lenp, char *full_name)
{
    char *name, grp_name[NC_MAX_NAME + 1];
    int g, id = ncid, parent_id, *gid;
    int i, ret = NC_NOERR;

    /* How many generations? */
    for (g = 0; !NC4_inq_grp_parent(id, &parent_id); g++, id = parent_id)
        ;

    /* Allocate storage. */
    if (!(name = malloc((size_t)(g + 1) * (NC_MAX_NAME + 1) + 1)))
        return NC_ENOMEM;
    if (!(gid = malloc((size_t)(g + 1) * sizeof(int))))
    {
        free(name);
        return NC_ENOMEM;
    }
    assert(name && gid);

    /* Always start with a "/" for the root group. */
    strcpy(name, NC_GROUP_NAME);

    /* Get the ncids for all generations. */
    gid[0] = ncid;
    for (i = 1; i < g && !ret; i++)
        ret = NC4_inq_grp_parent(gid[i - 1], &gid[i]);

    /* Assemble the full name. */
    for (i = g - 1; !ret && i >= 0 && !ret; i--)
    {
        if ((ret = NC4_inq_grpname(gid[i], grp_name)))
            break;
        strcat(name, grp_name);
        if (i)
            strcat(name, "/");
    }

    /* Give the user the length of the name, if he wants it. */
    if (!ret && lenp)
        *lenp = strlen(name);

    /* Give the user the name, if he wants it. */
    if (!ret && full_name)
        strcpy(full_name, name);

    free(gid);
    free(name);

    return ret;
}

/**
 * @internal Find the parent ncid of a group. For the root group,
 * return NC_ENOGRP error.  *Now* I know what kind of tinfoil hat
 * wearing nut job would call this function with a NULL pointer for
 * parent_ncid - Russ Rew!!
 *
 * @param ncid File and group ID.
 * @param parent_ncid Pointer that gets the ncid of parent group.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOGRP Root has no parent.
 * @author Ed Hartnett
 */
int
NC4_inq_grp_parent(int ncid, int *parent_ncid)
{
    NC_GRP_INFO_T *grp;
    NC_FILE_INFO_T *h5;
    int retval;

    LOG((2, "nc_inq_grp_parent: ncid 0x%x", ncid));

    /* Find info for this file and group. */
    if ((retval = nc4_find_grp_h5(ncid, &grp, &h5)))
        return retval;
    assert(h5);

    /* Set the parent ncid, if there is one. */
    if (grp->parent)
    {
        if (parent_ncid)
            *parent_ncid = grp->nc4_info->controller->ext_ncid | grp->parent->hdr.id;
    }
    else
        return NC_ENOGRP;

    return NC_NOERR;
}

/**
 * @internal Given a full name and ncid, find group ncid.
 *
 * @param ncid File and group ID.
 * @param full_name Full name of group.
 * @param grp_ncid Pointer that gets ncid of group.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOGRP Group not found.
 * @return ::NC_ENOMEM Out of memory.
 * @return ::NC_EINVAL Name is required.
 * @author Ed Hartnett
 */
int
NC4_inq_grp_full_ncid(int ncid, const char *full_name, int *grp_ncid)
{
    NC_GRP_INFO_T *grp;
    NC_FILE_INFO_T *h5;
    int id1 = ncid, id2;
    char *cp, *full_name_cpy;
    int ret;

    if (!full_name)
        return NC_EINVAL;

    /* Find info for this file and group, and set pointer to each. */
    if ((ret = nc4_find_grp_h5(ncid, &grp, &h5)))
        return ret;
    assert(h5);

    /* Copy full_name because strtok messes with the value it works
     * with, and we don't want to mess up full_name. */
    if (!(full_name_cpy = strdup(full_name)))
        return NC_ENOMEM;

    /* Get the first part of the name. */
    if (!(cp = strtok(full_name_cpy, "/")))
    {
        /* If "/" is passed, and this is the root group, return the root
         * group id. */
        if (!grp->parent)
            id2 = ncid;
        else
        {
            free(full_name_cpy);
            return NC_ENOGRP;
        }
    }
    else
    {
        /* Keep parsing the string. */
        for (; cp; id1 = id2)
        {
            if ((ret = NC4_inq_ncid(id1, cp, &id2)))
            {
                free(full_name_cpy);
                return ret;
            }
            cp = strtok(NULL, "/");
        }
    }

    /* Give the user the requested value. */
    if (grp_ncid)
        *grp_ncid = id2;

    free(full_name_cpy);

    return NC_NOERR;
}

/**
 * @internal Get a list of ids for all the variables in a group.
 *
 * @param ncid File and group ID.
 * @param nvars Pointer that gets number of vars in group.
 * @param varids Pointer that gets array of var IDs.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @author Ed Hartnett
 */
int
NC4_inq_varids(int ncid, int *nvars, int *varids)
{
    NC_GRP_INFO_T *grp;
    NC_FILE_INFO_T *h5;
    NC_VAR_INFO_T *var;
    int num_vars = 0;
    int retval;

    LOG((2, "nc_inq_varids: ncid 0x%x", ncid));

    /* Find info for this file and group, and set pointer to each. */
    if ((retval = nc4_find_grp_h5(ncid, &grp, &h5)))
        return retval;
    assert(h5);

    /* This is a netCDF-4 group. Round up them doggies and count
     * 'em. The list is in correct (i.e. creation) order. */
    for (size_t i=0; i < ncindexsize(grp->vars); i++)
    {
        var = (NC_VAR_INFO_T*)ncindexith(grp->vars,i);
        if (!var) continue;
        if (varids)
            varids[num_vars] = var->hdr.id;
        num_vars++;
    }

    /* If the user wants to know how many vars in the group, tell
     * him. */
    if (nvars)
        *nvars = num_vars;

    return NC_NOERR;
}

/**
 * @internal This is the comparison function used for sorting dim
 * ids. Integer comparison: returns negative if b > a and positive if
 * a > b.
 *
 * @param a A pointer to an item to compare to b.
 * @param b A pointer to an item to compare to a.
 *
 * @return a - b
 * @author Ed Hartnett
 */
int int_cmp(const void *a, const void *b)
{
    const int *ia = (const int *)a;
    const int *ib = (const int *)b;
    return *ia  - *ib;
}

/**
 * @internal Find all dimids for a location. This finds all dimensions
 * in a group, with or without any of its parents, depending on last
 * parameter.
 *
 * @param ncid File and group ID.
 * @param ndims Pointer that gets number of dimensions available in group.
 * @param dimids Pointer that gets dim IDs.
 * @param include_parents If non-zero, include dimensions from parent
 * groups.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @author Ed Hartnett
 */
int
NC4_inq_dimids(int ncid, int *ndims, int *dimids, int include_parents)
{
    NC_GRP_INFO_T *grp, *g;
    NC_FILE_INFO_T *h5;
    NC_DIM_INFO_T *dim;
    int num = 0;
    int retval;

    LOG((2, "nc_inq_dimids: ncid 0x%x include_parents: %d", ncid,
         include_parents));

    /* Find info for this file and group, and set pointer to each. */
    if ((retval = nc4_find_grp_h5(ncid, &grp, &h5)))
        return retval;
    assert(h5);

    /* First count them. */
    num = ncindexcount(grp->dim);
    if (include_parents) {
        for (g = grp->parent; g; g = g->parent)
            num += ncindexcount(g->dim);
    }

    /* If the user wants the dimension ids, get them. */
    if (dimids)
    {
        int n = 0;

        /* Get dimension ids from this group. */
        for(size_t i=0;i<ncindexsize(grp->dim);i++) {
            dim = (NC_DIM_INFO_T*)ncindexith(grp->dim,i);
            if(dim == NULL) continue;
            dimids[n++] = dim->hdr.id;
        }

        /* Get dimension ids from parent groups. */
        if (include_parents)
            for (g = grp->parent; g; g = g->parent) {
                for(size_t i=0;i<ncindexsize(g->dim);i++) {
                    dim = (NC_DIM_INFO_T*)ncindexith(g->dim,i);
                    if(dim == NULL) continue;
                    dimids[n++] = dim->hdr.id;
                }
            }
        qsort(dimids, (size_t)num, sizeof(int), int_cmp);
    }

    /* If the user wants the number of dims, give it. */
    if (ndims)
        *ndims = num;

    return NC_NOERR;
}
