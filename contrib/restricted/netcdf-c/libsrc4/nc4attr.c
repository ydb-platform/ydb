/* Copyright 2003-2018, University Corporation for Atmospheric
 * Research. See COPYRIGHT file for copying and redistribution
 * conditions. */
/**
 * @file
 *
 * @internal This file is part of netcdf-4, a netCDF-like interface
 * for HDF5, or a HDF5 backend for netCDF, depending on your point of
 * view.
 *
 * This file handles the nc4 attribute functions.
 *
 * Remember that with atts, type conversion can take place when
 * writing them, and when reading them.
 *
 * @author Ed Hartnett
 */

#include "nc.h"
#include "nc4internal.h"
#include "nc4dispatch.h"
#include "ncdispatch.h"

/**
 * @internal Get or put attribute metadata from our linked list of
 * file info. Always locate the attribute by name, never by attnum.
 * The mem_type is ignored if data=NULL.
 *
 * @param h5 File object
 * @param grp Group object
 * @param var Variable object
 * @param name Name of attribute. Must already be normalized.
 * @param xtype Pointer that gets (file) type of attribute. Ignored if
 * NULL.
 * @param mem_type The type of attribute data in memory.
 * @param lenp Pointer that gets length of attribute array. Ignored if
 * NULL.
 * @param attnum Pointer that gets the index number of this
 * attribute. Ignored if NULL.
 * @param data Pointer that gets attribute data. Ignored if NULL.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @author Ed Hartnett
 */
int
nc4_get_att_ptrs(NC_FILE_INFO_T *h5, NC_GRP_INFO_T *grp, NC_VAR_INFO_T *var,
                 const char *name, nc_type *xtype, nc_type mem_type,
                 size_t *lenp, int *attnum, void *data)
{
    NC_ATT_INFO_T *att = NULL;
    int my_attnum = -1;
    int need_to_convert = 0;
    int range_error = NC_NOERR;
    void *bufr = NULL;
    size_t type_size;
    int varid;
    int retval;

    LOG((3, "%s: mem_type %d", __func__, mem_type));

    /* Get the varid, or NC_GLOBAL. */
    varid = var ? var->hdr.id : NC_GLOBAL;

    if (attnum)
        my_attnum = *attnum;

    if (name == NULL)
        BAIL(NC_EBADNAME);

    /* Find the attribute, if it exists. */
    if ((retval = nc4_find_grp_att(grp, varid, name, my_attnum, &att)))
        return retval;

    /* If mem_type is NC_NAT, it means we want to use the attribute's
     * file type as the mem type as well. */
    if (mem_type == NC_NAT)
        mem_type = att->nc_typeid;

    /* If the attribute is NC_CHAR, and the mem_type isn't, or vice
     * versa, that's a freakish attempt to convert text to
     * numbers. Some pervert out there is trying to pull a fast one!
     * Send him an NC_ECHAR error. */
    if (data && att->len)
        if ((att->nc_typeid == NC_CHAR && mem_type != NC_CHAR) ||
            (att->nc_typeid != NC_CHAR && mem_type == NC_CHAR))
            BAIL(NC_ECHAR); /* take that, you freak! */

    /* Copy the info. */
    if (lenp)
        *lenp = att->len;
    if (xtype)
        *xtype = att->nc_typeid;
    if (attnum) {
        *attnum = att->hdr.id;
    }

    /* Zero len attributes are easy to read! */
    if (!att->len)
        BAIL(NC_NOERR);

    /* Later on, we will need to know the size of this type. */
    if ((retval = nc4_get_typelen_mem(h5, mem_type, &type_size)))
        BAIL(retval);

    /* We may have to convert data. Treat NC_CHAR the same as
     * NC_UBYTE. If the mem_type is NAT, don't try any conversion - use
     * the attribute's type. */
    if (data && att->len && mem_type != att->nc_typeid &&
        mem_type != NC_NAT &&
        !(mem_type == NC_CHAR &&
          (att->nc_typeid == NC_UBYTE || att->nc_typeid == NC_BYTE)))
    {
        if (!(bufr = malloc((size_t)(att->len) * type_size)))
            BAIL(NC_ENOMEM);
        need_to_convert++;
        if ((retval = nc4_convert_type(att->data, bufr, att->nc_typeid,
                                       mem_type, (size_t)att->len, &range_error,
                                       NULL, (h5->cmode & NC_CLASSIC_MODEL),
				       NC_NOQUANTIZE, 0)))
            BAIL(retval);

        /* For strict netcdf-3 rules, ignore erange errors between UBYTE
         * and BYTE types. */
        if ((h5->cmode & NC_CLASSIC_MODEL) &&
            (att->nc_typeid == NC_UBYTE || att->nc_typeid == NC_BYTE) &&
            (mem_type == NC_UBYTE || mem_type == NC_BYTE) &&
            range_error)
            range_error = 0;
    }
    else
    {
        bufr = att->data;
    }

    /* If the caller wants data, copy it for him. If he hasn't
       allocated enough memory for it, he will burn in segmentation
       fault hell, writhing with the agony of undiscovered memory
       bugs! */
    if (data)
    {
	{
	    if((retval = NC_copy_data(h5->controller,mem_type,bufr,att->len,data)))
	        BAIL(retval);
	}
    }

exit:
    if (need_to_convert)
        free(bufr);
    if (range_error)
        retval = NC_ERANGE;
    return retval;
}

/**
 * @internal Get or put attribute metadata from our linked list of
 * file info. Always locate the attribute by name, never by attnum.
 * The mem_type is ignored if data=NULL.
 *
 * @param ncid File and group ID.
 * @param varid Variable ID.
 * @param name Name of attribute.
 * @param xtype Pointer that gets (file) type of attribute. Ignored if
 * NULL.
 * @param mem_type The type of attribute data in memory.
 * @param lenp Pointer that gets length of attribute array. Ignored if
 * NULL.
 * @param attnum Pointer that gets the index number of this
 * attribute. Ignored if NULL.
 * @param data Pointer that gets attribute data. Ignored if NULL.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @author Ed Hartnett
 */
int
nc4_get_att(int ncid, int varid, const char *name, nc_type *xtype,
            nc_type mem_type, size_t *lenp, int *attnum, void *data)
{
    NC_FILE_INFO_T *h5;
    NC_GRP_INFO_T *grp;
    NC_VAR_INFO_T *var = NULL;
    char norm_name[NC_MAX_NAME + 1];
    int retval;

    LOG((3, "%s: ncid 0x%x varid %d mem_type %d", __func__, ncid,
         varid, mem_type));

    /* Find info for this file, group, and h5 info. */
    if ((retval = nc4_find_grp_h5(ncid, &grp, &h5)))
        return retval;
    assert(h5 && grp);

    /* Check varid */
    if (varid != NC_GLOBAL)
    {
        if (!(var = (NC_VAR_INFO_T*)ncindexith(grp->vars,(size_t)varid)))
            return NC_ENOTVAR;
        assert(var->hdr.id == varid);
    }

    /* Name is required. */
    if (!name)
        return NC_EBADNAME;

    /* Normalize name. */
    if ((retval = nc4_normalize_name(name, norm_name)))
        return retval;

    return nc4_get_att_ptrs(h5, grp, var, norm_name, xtype, mem_type, lenp,
                            attnum, data);
}

/**
 * @internal Learn about an att. All the nc4 nc_inq_ functions just
 * call nc4_get_att to get the metadata on an attribute.
 *
 * @param ncid File and group ID.
 * @param varid Variable ID.
 * @param name Name of attribute.
 * @param xtypep Pointer that gets type of attribute.
 * @param lenp Pointer that gets length of attribute data array.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @author Ed Hartnett
 */
int
NC4_inq_att(int ncid, int varid, const char *name, nc_type *xtypep,
            size_t *lenp)
{
    LOG((2, "%s: ncid 0x%x varid %d name %s", __func__, ncid, varid, name));
    return nc4_get_att(ncid, varid, name, xtypep, NC_NAT, lenp, NULL, NULL);
}

/**
 * @internal Learn an attnum, given a name.
 *
 * @param ncid File and group ID.
 * @param varid Variable ID.
 * @param name Name of attribute.
 * @param attnump Pointer that gets the attribute index number.
 *
 * @return ::NC_NOERR No error.
 * @author Ed Hartnett
 */
int
NC4_inq_attid(int ncid, int varid, const char *name, int *attnump)
{
    LOG((2, "%s: ncid 0x%x varid %d name %s", __func__, ncid, varid, name));
    return nc4_get_att(ncid, varid, name, NULL, NC_NAT, NULL, attnump, NULL);
}

/**
 * @internal Given an attnum, find the att's name.
 *
 * @param ncid File and group ID.
 * @param varid Variable ID.
 * @param attnum The index number of the attribute.
 * @param name Pointer that gets name of attribute.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @author Ed Hartnett
 */
int
NC4_inq_attname(int ncid, int varid, int attnum, char *name)
{
    NC_ATT_INFO_T *att;
    int retval;

    LOG((2, "nc_inq_attname: ncid 0x%x varid %d attnum %d", ncid, varid,
         attnum));

    /* Find the attribute metadata. */
    if ((retval = nc4_find_nc_att(ncid, varid, NULL, attnum, &att)))
        return retval;

    /* Get the name. */
    if (name)
        strcpy(name, att->hdr.name);

    return NC_NOERR;
}

/**
 * @internal Get an attribute.
 *
 * @param ncid File and group ID.
 * @param varid Variable ID.
 * @param name Name of attribute.
 * @param value Pointer that gets attribute data.
 * @param memtype The type the data should be converted to as it is read.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @author Ed Hartnett
 */
int
NC4_get_att(int ncid, int varid, const char *name, void *value, nc_type memtype)
{
    return nc4_get_att(ncid, varid, name, NULL, memtype, NULL, NULL, value);
}
