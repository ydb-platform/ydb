/* Copyright 2018, University Corporation for Atmospheric
 * Research. See the COPYRIGHT file for copying and redistribution
 * conditions. */
/**
 * @file
 * @internal This file is part of netcdf-4, a netCDF-like interface
 * for HDF5, or a HDF5 backend for netCDF, depending on your point of
 * view.
 *
 * This file contains functions internal to the netcdf4 library. None of
 * the functions in this file are exposed in the exetnal API. These
 * functions handle the HDF interface.
 *
 * @author Ed Hartnett, Dennis Heimbigner, Ward Fisher
 */

#include "config.h"
#include "netcdf.h"
#include "nc4internal.h"
#include "ncdispatch.h"
#include "hdf5internal.h"
#include "hdf5err.h" /* For BAIL2 */
#include "hdf5debug.h"
#include <math.h>
#include <stddef.h>

#ifdef HAVE_INTTYPES_H
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#endif

#define NC_HDF5_MAX_NAME 1024 /**< @internal Max size of HDF5 name. */

/**
 * @internal Flag attributes in a linked list as dirty.
 *
 * @param attlist List of attributes, may be NULL.
 *
 * @return NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
flag_atts_dirty(NCindex *attlist) {

    NC_ATT_INFO_T *att = NULL;

    if(attlist == NULL) {
        return NC_NOERR;
    }

    for(size_t i=0;i<ncindexsize(attlist);i++) {
        att = (NC_ATT_INFO_T*)ncindexith(attlist,i);
        if(att == NULL) continue;
        att->dirty = NC_TRUE;
    }

    return NC_NOERR;
}

/**
 * @internal This function is needed to handle one special case: what
 * if the user defines a dim, writes metadata, then goes back into
 * define mode and adds a coordinate var for the already existing
 * dim. In that case, I need to recreate the dim's dimension scale
 * dataset, and then I need to go to every var in the file which uses
 * that dimension, and attach the new dimension scale.
 *
 * @param grp Pointer to group info struct.
 * @param dimid Dimension ID.
 * @param dimscaleid HDF5 dimension scale ID.
 *
 * @returns NC_NOERR No error.
 * @returns NC_EDIMSCALE HDF5 returned an error when trying to reattach a dimension scale.
 * @author Ed Hartnett
 */
int
rec_reattach_scales(NC_GRP_INFO_T *grp, int dimid, hid_t dimscaleid)
{
    NC_VAR_INFO_T *var;
    NC_GRP_INFO_T *child_grp;
    size_t i;
    int retval;

    assert(grp && grp->hdr.name && dimid >= 0 && dimscaleid >= 0);
    LOG((3, "%s: grp->hdr.name %s", __func__, grp->hdr.name));

    /* If there are any child groups, attach dimscale there, if needed. */
    for (i = 0; i < ncindexsize(grp->children); i++)
    {
        child_grp = (NC_GRP_INFO_T*)ncindexith(grp->children, i);
        assert(child_grp);
        if ((retval = rec_reattach_scales(child_grp, dimid, dimscaleid)))
            return retval;
    }

    /* Find any vars that use this dimension id. */
    for (i = 0; i < ncindexsize(grp->vars); i++)
    {
        NC_HDF5_VAR_INFO_T *hdf5_var;

        var = (NC_VAR_INFO_T*)ncindexith(grp->vars,i);
        assert(var && var->format_var_info);

	hdf5_var = (NC_HDF5_VAR_INFO_T*)var->format_var_info;	
	assert(hdf5_var != NULL);
        for (unsigned int d = 0; d < var->ndims; d++)
        {
            if (var->dimids[d] == dimid && !hdf5_var->dimscale)
            {
                LOG((2, "%s: attaching scale for dimid %d to var %s",
                     __func__, var->dimids[d], var->hdr.name));
                if (var->created)
                {
                    if (H5DSattach_scale(hdf5_var->hdf_datasetid,
                                         dimscaleid, d) < 0)
                        return NC_EDIMSCALE;
                    hdf5_var->dimscale_attached[d] = NC_TRUE;
                }
            }
        }
    }
    return NC_NOERR;
}

/**
 * @internal This function is needed to handle one special case: what
 * if the user defines a dim, writes metadata, then goes back into
 * define mode and adds a coordinate var for the already existing
 * dim. In that case, I need to recreate the dim's dimension scale
 * dataset, and then I need to go to every var in the file which uses
 * that dimension, and attach the new dimension scale.
 *
 * @param grp Pointer to group info struct.
 * @param dimid Dimension ID.
 * @param dimscaleid HDF5 dimension scale ID.
 *
 * @returns NC_NOERR No error.
 * @returns NC_EDIMSCALE HDF5 returned an error when trying to detach a dimension scale.
 * @author Ed Hartnett
 */
int
rec_detach_scales(NC_GRP_INFO_T *grp, int dimid, hid_t dimscaleid)
{
    NC_VAR_INFO_T *var;
    NC_GRP_INFO_T *child_grp;
    size_t i;
    int retval;

    assert(grp && grp->hdr.name && dimid >= 0 && dimscaleid >= 0);
    LOG((3, "%s: grp->hdr.name %s", __func__, grp->hdr.name));

    /* If there are any child groups, detach dimscale there, if needed. */
    for(i=0;i<ncindexsize(grp->children);i++) {
        child_grp = (NC_GRP_INFO_T*)ncindexith(grp->children,i);
        if(child_grp == NULL) continue;
        if ((retval = rec_detach_scales(child_grp, dimid, dimscaleid)))
            return retval;
    }

    /* Find any vars that use this dimension id. */
    for (i = 0; i < ncindexsize(grp->vars); i++)
    {
        NC_HDF5_VAR_INFO_T *hdf5_var;
        var = (NC_VAR_INFO_T*)ncindexith(grp->vars, i);
        assert(var && var->format_var_info);
        hdf5_var = (NC_HDF5_VAR_INFO_T *)var->format_var_info;

        for (unsigned int d = 0; d < var->ndims; d++)
        {
            if (var->dimids[d] == dimid && !hdf5_var->dimscale)
            {
                LOG((2, "%s: detaching scale for dimid %d to var %s",
                     __func__, var->dimids[d], var->hdr.name));
                if (var->created)
                {
                    if (hdf5_var->dimscale_attached && hdf5_var->dimscale_attached[d])
                    {
                        if (H5DSdetach_scale(hdf5_var->hdf_datasetid,
                                             dimscaleid, d) < 0)
                            return NC_EDIMSCALE;
                        hdf5_var->dimscale_attached[d] = NC_FALSE;
                    }
                }
            }
        }
    }
    return NC_NOERR;
}

/**
 * @internal Open a HDF5 dataset and leave it open.
 *
 * @param grp Pointer to group info struct.
 * @param varid Variable ID.
 * @param dataset Pointer that gets the HDF5 dataset ID.
 *
 * @returns NC_NOERR No error.
 * @returns NC_EHDFERR HDF5 returned an error.
 * @author Ed Hartnett
 */
int
nc4_open_var_grp2(NC_GRP_INFO_T *grp, int varid, hid_t *dataset)
{
    NC_VAR_INFO_T *var;
    NC_HDF5_VAR_INFO_T *hdf5_var;

    assert(grp && grp->format_grp_info && dataset);

    /* Find the requested varid. */
    if (!(var = (NC_VAR_INFO_T *)ncindexith(grp->vars, (size_t)varid)))
        return NC_ENOTVAR;
    assert(var && var->hdr.id == varid && var->format_var_info);
    hdf5_var = (NC_HDF5_VAR_INFO_T *)var->format_var_info;

    /* Open this dataset if necessary. */
    if (!hdf5_var->hdf_datasetid)
    {
        NC_HDF5_GRP_INFO_T *hdf5_grp;
        hdf5_grp = (NC_HDF5_GRP_INFO_T *)grp->format_grp_info;

        if ((hdf5_var->hdf_datasetid = H5Dopen2(hdf5_grp->hdf_grpid,
                                                var->hdr.name, H5P_DEFAULT)) < 0)
            return NC_ENOTVAR;
    }

    *dataset = hdf5_var->hdf_datasetid;

    return NC_NOERR;
}

/**
 * @internal Given a netcdf type, return appropriate HDF typeid.  (All
 * hdf_typeid's returned from this routine must be H5Tclosed by the
 * caller).
 *
 * @param h5 Pointer to HDF5 file info struct.
 * @param xtype NetCDF type ID.
 * @param hdf_typeid Pointer that gets the HDF5 type ID.
 * @param endianness Desired endianness in HDF5 type.
 *
 * @return NC_NOERR No error.
 * @return NC_ECHAR Conversions of NC_CHAR forbidden.
 * @return NC_EVARMETA HDF5 returning error creating datatype.
 * @return NC_EHDFERR HDF5 returning error.
 * @return NC_EBADTYE Type not found.
 * @author Ed Hartnett
 */
int
nc4_get_hdf_typeid(NC_FILE_INFO_T *h5, nc_type xtype,
                   hid_t *hdf_typeid, int endianness)
{
    NC_TYPE_INFO_T *type;
    hid_t typeid = 0;
    int retval = NC_NOERR;

    assert(hdf_typeid && h5);

    *hdf_typeid = -1;

    /* Determine an appropriate HDF5 datatype */
    if (xtype == NC_NAT)
        return NC_EBADTYPE;
    else if (xtype == NC_CHAR || xtype == NC_STRING)
    {
        /* NC_CHAR & NC_STRING types create a new HDF5 datatype */
        if (xtype == NC_CHAR)
        {
            if ((typeid = H5Tcopy(H5T_C_S1)) < 0)
                return NC_EHDFERR;
            if (H5Tset_strpad(typeid, H5T_STR_NULLTERM) < 0)
                BAIL(NC_EVARMETA);
            if(H5Tset_cset(typeid, H5T_CSET_ASCII) < 0)
                BAIL(NC_EVARMETA);

            /* Take ownership of the newly created HDF5 datatype */
            *hdf_typeid = typeid;
            typeid = 0;
        }
        else
        {
            if ((typeid = H5Tcopy(H5T_C_S1)) < 0)
                return NC_EHDFERR;
            if (H5Tset_size(typeid, H5T_VARIABLE) < 0)
                BAIL(NC_EVARMETA);
            if(H5Tset_cset(typeid, H5T_CSET_UTF8) < 0)
                BAIL(NC_EVARMETA);

            /* Take ownership of the newly created HDF5 datatype */
            *hdf_typeid = typeid;
            typeid = 0;
        }
    }
    else
    {
        /* All other types use an existing HDF5 datatype */
        switch (xtype)
        {
        case NC_BYTE: /* signed 1 byte integer */
            if (endianness == NC_ENDIAN_LITTLE)
                typeid = H5T_STD_I8LE;
            else if (endianness == NC_ENDIAN_BIG)
                typeid = H5T_STD_I8BE;
            else
                typeid = H5T_NATIVE_SCHAR;
            break;

        case NC_SHORT: /* signed 2 byte integer */
            if (endianness == NC_ENDIAN_LITTLE)
                typeid = H5T_STD_I16LE;
            else if (endianness == NC_ENDIAN_BIG)
                typeid = H5T_STD_I16BE;
            else
                typeid = H5T_NATIVE_SHORT;
            break;

        case NC_INT:
            if (endianness == NC_ENDIAN_LITTLE)
                typeid = H5T_STD_I32LE;
            else if (endianness == NC_ENDIAN_BIG)
                typeid = H5T_STD_I32BE;
            else
                typeid = H5T_NATIVE_INT;
            break;

        case NC_UBYTE:
            if (endianness == NC_ENDIAN_LITTLE)
                typeid = H5T_STD_U8LE;
            else if (endianness == NC_ENDIAN_BIG)
                typeid = H5T_STD_U8BE;
            else
                typeid = H5T_NATIVE_UCHAR;
            break;

        case NC_USHORT:
            if (endianness == NC_ENDIAN_LITTLE)
                typeid = H5T_STD_U16LE;
            else if (endianness == NC_ENDIAN_BIG)
                typeid = H5T_STD_U16BE;
            else
                typeid = H5T_NATIVE_USHORT;
            break;

        case NC_UINT:
            if (endianness == NC_ENDIAN_LITTLE)
                typeid = H5T_STD_U32LE;
            else if (endianness == NC_ENDIAN_BIG)
                typeid = H5T_STD_U32BE;
            else
                typeid = H5T_NATIVE_UINT;
            break;

        case NC_INT64:
            if (endianness == NC_ENDIAN_LITTLE)
                typeid = H5T_STD_I64LE;
            else if (endianness == NC_ENDIAN_BIG)
                typeid = H5T_STD_I64BE;
            else
                typeid = H5T_NATIVE_LLONG;
            break;

        case NC_UINT64:
            if (endianness == NC_ENDIAN_LITTLE)
                typeid = H5T_STD_U64LE;
            else if (endianness == NC_ENDIAN_BIG)
                typeid = H5T_STD_U64BE;
            else
                typeid = H5T_NATIVE_ULLONG;
            break;

        case NC_FLOAT:
            if (endianness == NC_ENDIAN_LITTLE)
                typeid = H5T_IEEE_F32LE;
            else if (endianness == NC_ENDIAN_BIG)
                typeid = H5T_IEEE_F32BE;
            else
                typeid = H5T_NATIVE_FLOAT;
            break;

        case NC_DOUBLE:
            if (endianness == NC_ENDIAN_LITTLE)
                typeid = H5T_IEEE_F64LE;
            else if (endianness == NC_ENDIAN_BIG)
                typeid = H5T_IEEE_F64BE;
            else
                typeid = H5T_NATIVE_DOUBLE;
            break;

        default:
            /* Maybe this is a user defined type? */
            if (nc4_find_type(h5, xtype, &type))
                return NC_EBADTYPE;
            if (!type)
                return NC_EBADTYPE;
            typeid = ((NC_HDF5_TYPE_INFO_T *)type->format_type_info)->hdf_typeid;
            break;
        }
        assert(typeid);

        /* Copy the HDF5 datatype, so the function operates uniformly */
        if ((*hdf_typeid = H5Tcopy(typeid)) < 0)
            return NC_EHDFERR;
        typeid = 0;
    }
    assert(*hdf_typeid != -1);

exit:
    if (typeid > 0 && H5Tclose(typeid) < 0)
        BAIL2(NC_EHDFERR);
    return retval;
}

/**
 * @internal Write an attribute.
 *
 * @param grp Pointer to group info struct.
 * @param varid Variable ID or NC_GLOBAL.
 * @param att Pointer to attribute info struct.
 *
 * @returns ::NC_NOERR No error.
 * @returns ::NC_ENOTVAR Variable not found.
 * @returns ::NC_EPERM Read-only file.
 * @returns ::NC_EHDFERR HDF5 returned error.
 * @returns ::NC_EATTMETA HDF5 returned error with attribute calls.
 * @author Ed Hartnett
 */
static int
put_att_grpa(NC_GRP_INFO_T *grp, int varid, NC_ATT_INFO_T *att)
{
    NC_HDF5_GRP_INFO_T *hdf5_grp;
    hid_t datasetid = 0, locid;
    hid_t attid = 0, spaceid = 0, file_typeid = 0;
    hid_t existing_att_typeid = 0, existing_attid = 0, existing_spaceid = 0;
    hsize_t dims[1]; /* netcdf attributes always 1-D. */
    htri_t attr_exists;
    void *data;
    int phoney_data = 99;
    int retval = NC_NOERR;

    assert(att->hdr.name && grp && grp->format_grp_info);
    LOG((3, "%s: varid %d att->hdr.id %d att->hdr.name %s att->nc_typeid %d "
         "att->len %d", __func__, varid, att->hdr.id, att->hdr.name,
         att->nc_typeid, att->len));

    /* Get HDF5-specific group info. */
    hdf5_grp = (NC_HDF5_GRP_INFO_T *)grp->format_grp_info;

    /* If the file is read-only, return an error. */
    if (grp->nc4_info->no_write)
        BAIL(NC_EPERM);

    /* Get the hid to attach the attribute to, or read it from. */
    if (varid == NC_GLOBAL)
        locid = hdf5_grp->hdf_grpid;
    else
    {
        if ((retval = nc4_open_var_grp2(grp, varid, &datasetid)))
            BAIL(retval);
        locid = datasetid;
    }

    /* Get the length ready, and find the HDF type we'll be
     * writing. */
    dims[0] = (hsize_t)att->len;
    if ((retval = nc4_get_hdf_typeid(grp->nc4_info, att->nc_typeid,
                                     &file_typeid, 0)))
        BAIL(retval);

    /* Even if the length is zero, HDF5 won't let me write with a
     * NULL pointer. So if the length of the att is zero, point to
     * some phoney data (which won't be written anyway.)*/
    if (!dims[0])
        data = &phoney_data;
    else
        data = att->data;

    /* NC_CHAR types require some extra work. The space ID is set to
     * scalar, and the type is told how long the string is. If it's
     * really zero length, set the size to 1. (The fact that it's
     * really zero will be marked by the NULL dataspace, but HDF5
     * doesn't allow me to set the size of the type to zero.)*/
    if (att->nc_typeid == NC_CHAR)
    {
        size_t string_size = dims[0];
        if (!string_size)
        {
            string_size = 1;
            if ((spaceid = H5Screate(H5S_NULL)) < 0)
                BAIL(NC_EATTMETA);
        }
        else
        {
            if ((spaceid = H5Screate(H5S_SCALAR)) < 0)
                BAIL(NC_EATTMETA);
        }
        if (H5Tset_size(file_typeid, string_size) < 0)
            BAIL(NC_EATTMETA);
        if (H5Tset_strpad(file_typeid, H5T_STR_NULLTERM) < 0)
            BAIL(NC_EATTMETA);
    }
    else
    {
        if (!att->len)
        {
            if ((spaceid = H5Screate(H5S_NULL)) < 0)
                BAIL(NC_EATTMETA);
        }
        else
        {
            if ((spaceid = H5Screate_simple(1, dims, NULL)) < 0)
                BAIL(NC_EATTMETA);
        }
    }

    /* Does the att exist already? */
    if ((attr_exists = H5Aexists(locid, att->hdr.name)) < 0)
        BAIL(NC_EHDFERR);
    if (attr_exists)
    {
        /* Open the existing attribute. */
        if ((existing_attid = H5Aopen(locid, att->hdr.name, H5P_DEFAULT)) < 0)
            BAIL(NC_EATTMETA);

        /* Get the HDF5 datatype of the existing attribute. */
        if ((existing_att_typeid = H5Aget_type(existing_attid)) < 0)
            BAIL(NC_EATTMETA);

        /* Get the HDF5 dataspace of the existing attribute. */
        if ((existing_spaceid = H5Aget_space(existing_attid)) < 0)
            BAIL(NC_EATTMETA);

        /* Is new attribute the same datatype and size as previous?
         * For text attributes the size is embedded in the datatype, not the dataspace.
         * H5Sextent_equal will also do the right thing with NULL dataspace cases. */
        if (!H5Tequal(file_typeid, existing_att_typeid) ||
            (!H5Sextent_equal(spaceid, existing_spaceid)))
        {
            /* The attribute exists but we cannot reuse it. */

            /* Delete the attribute. */
            if (H5Adelete(locid, att->hdr.name) < 0)
                BAIL(NC_EHDFERR);

            /* Re-create the attribute with the type and length
               reflecting the new value (or values). */
            if ((attid = H5Acreate1(locid, att->hdr.name, file_typeid, spaceid,
				    H5P_DEFAULT)) < 0)
                BAIL(NC_EATTMETA);

            /* Write the values, (even if length is zero). */
            if (H5Awrite(attid, file_typeid, data) < 0)
                BAIL(NC_EATTMETA);
        }
        else
        {
            /* The attribute exists and we can reuse it. */

            /* Write the values, reusing the existing attribute. */
            if (H5Awrite(existing_attid, file_typeid, data) < 0)
                BAIL(NC_EATTMETA);
        }
    }
    else
    {
        /* The attribute does not exist yet. */

        /* Create the attribute. */
        if ((attid = H5Acreate1(locid, att->hdr.name, file_typeid, spaceid,
                               H5P_DEFAULT)) < 0)
            BAIL(NC_EATTMETA);

        /* Write the values, (even if length is zero). */
        if (H5Awrite(attid, file_typeid, data) < 0)
            BAIL(NC_EATTMETA);
    }

exit:
    if (file_typeid && H5Tclose(file_typeid))
        BAIL2(NC_EHDFERR);
    if (attid > 0 && H5Aclose(attid) < 0)
        BAIL2(NC_EHDFERR);
    if (existing_att_typeid && H5Tclose(existing_att_typeid))
        BAIL2(NC_EHDFERR);
    if (existing_attid > 0 && H5Aclose(existing_attid) < 0)
        BAIL2(NC_EHDFERR);
    if (spaceid > 0 && H5Sclose(spaceid) < 0)
        BAIL2(NC_EHDFERR);
    if (existing_spaceid > 0 && H5Sclose(existing_spaceid) < 0)
        BAIL2(NC_EHDFERR);
    return retval;
}

/**
 * @internal Write all the dirty atts in an attlist.
 *
 * @param attlist Pointer to the list if attributes.
 * @param varid Variable ID.
 * @param grp Pointer to group info struct.
 *
 * @returns NC_NOERR No error.
 * @returns NC_EHDFERR HDF5 returned an error.
 * @author Ed Hartnett
 */
static int
write_attlist(NCindex *attlist, int varid, NC_GRP_INFO_T *grp)
{
    NC_ATT_INFO_T *att;
    int retval;

    for(size_t i = 0; i < ncindexsize(attlist); i++)
    {
        att = (NC_ATT_INFO_T *)ncindexith(attlist, i);
        assert(att);
        if (att->dirty)
        {
            LOG((4, "%s: writing att %s to varid %d", __func__, att->hdr.name, varid));
            if ((retval = put_att_grpa(grp, varid, att)))
                return retval;
            att->dirty = NC_FALSE;
            att->created = NC_TRUE;
        }
    }
    return NC_NOERR;
}

/**
 * @internal HDF5 dimension scales cannot themselves have scales
 * attached. This is a problem for multidimensional coordinate
 * variables. So this function writes a special attribute for such a
 * variable, which has the ids of all the dimensions for that
 * coordinate variable.
 *
 * @param var Pointer to var info struct.
 *
 * @returns NC_NOERR No error.
 * @returns NC_EHDFERR HDF5 returned an error.
 * @author Ed Hartnett
 */
static int
write_coord_dimids(NC_VAR_INFO_T *var)
{
    NC_HDF5_VAR_INFO_T *hdf5_var;
    hsize_t coords_len[1];
    hid_t c_spaceid = -1, c_attid = -1;
    int retval = NC_NOERR;

    assert(var && var->format_var_info);

    /* Get HDF5-specific var info. */
    hdf5_var = (NC_HDF5_VAR_INFO_T *)var->format_var_info;

    /* Set up space for attribute. */
    coords_len[0] = var->ndims;
    if ((c_spaceid = H5Screate_simple(1, coords_len, coords_len)) < 0)
        BAIL(NC_EHDFERR);

    /* Create the attribute. */
    if ((c_attid = H5Acreate1(hdf5_var->hdf_datasetid, COORDINATES,
                             H5T_NATIVE_INT, c_spaceid, H5P_DEFAULT)) < 0)
        BAIL(NC_EHDFERR);

    /* Write our attribute. */
    if (H5Awrite(c_attid, H5T_NATIVE_INT, var->dimids) < 0)
        BAIL(NC_EHDFERR);

exit:
    if (c_spaceid >= 0 && H5Sclose(c_spaceid) < 0)
        BAIL2(NC_EHDFERR);
    if (c_attid >= 0 && H5Aclose(c_attid) < 0)
        BAIL2(NC_EHDFERR);
    return retval;
}

/**
 * @internal When nc_def_var_quantize() is used, a new attribute is
 * added to the var, containing the quantize information.
 *
 * @param var Pointer to var info struct.
 *
 * @returns NC_NOERR No error.
 * @returns NC_EHDFERR HDF5 returned an error.
 * @author Ed Hartnett
 */
static int
write_quantize_att(NC_VAR_INFO_T *var)
{
    NC_HDF5_VAR_INFO_T *hdf5_var;
    hsize_t len = 1;
    hid_t c_spaceid = -1, c_attid = -1;
    char att_name[NC_MAX_NAME + 1];
    int retval = NC_NOERR;

    assert(var && var->format_var_info);

    /* Get HDF5-specific var info. */
    hdf5_var = (NC_HDF5_VAR_INFO_T *)var->format_var_info;

    /* Different quantize algorithms get different attribute names. */
    switch (var->quantize_mode)
    {
	case NC_QUANTIZE_BITGROOM:
	    snprintf(att_name, sizeof(att_name), "%s", NC_QUANTIZE_BITGROOM_ATT_NAME);
	    break;
	case NC_QUANTIZE_GRANULARBR:
	    snprintf(att_name, sizeof(att_name), "%s", NC_QUANTIZE_GRANULARBR_ATT_NAME);
	    break;
	case NC_QUANTIZE_BITROUND:
	    snprintf(att_name, sizeof(att_name), "%s", NC_QUANTIZE_BITROUND_ATT_NAME);
	   break;
        default:
	    return NC_EINVAL;
    }

    /* Set up space for attribute. */
    if ((c_spaceid = H5Screate_simple(1, &len, &len)) < 0)
        BAIL(NC_EHDFERR);

    /* Create the attribute. */
    if ((c_attid = H5Acreate1(hdf5_var->hdf_datasetid, att_name,
                             H5T_NATIVE_INT, c_spaceid, H5P_DEFAULT)) < 0)
        BAIL(NC_EHDFERR);

    /* Write our attribute. */
    if (H5Awrite(c_attid, H5T_NATIVE_INT, &var->nsd) < 0)
        BAIL(NC_EHDFERR);

exit:
    if (c_spaceid >= 0 && H5Sclose(c_spaceid) < 0)
        BAIL2(NC_EHDFERR);
    if (c_attid >= 0 && H5Aclose(c_attid) < 0)
        BAIL2(NC_EHDFERR);
    return retval;
}

/**
 * @internal Write a special attribute for the netCDF-4 dimension ID.
 *
 * @param datasetid HDF5 datasset ID.
 * @param dimid NetCDF dimension ID.
 *
 * @returns NC_NOERR No error.
 * @returns NC_EHDFERR HDF5 returned an error.
 * @author Ed Hartnett
 */
static int
write_netcdf4_dimid(hid_t datasetid, int dimid)
{
    hid_t dimid_spaceid = -1, dimid_attid = -1;
    htri_t attr_exists;
    int retval = NC_NOERR;

    /* Create the space. */
    if ((dimid_spaceid = H5Screate(H5S_SCALAR)) < 0)
        BAIL(NC_EHDFERR);

    /* Does the attribute already exist? If so, don't try to create it. */
    if ((attr_exists = H5Aexists(datasetid, NC_DIMID_ATT_NAME)) < 0)
        BAIL(NC_EHDFERR);
    if (attr_exists)
        dimid_attid = H5Aopen_by_name(datasetid, ".", NC_DIMID_ATT_NAME,
                                      H5P_DEFAULT, H5P_DEFAULT);
    else
        /* Create the attribute if needed. */
        dimid_attid = H5Acreate1(datasetid, NC_DIMID_ATT_NAME,
                                H5T_NATIVE_INT, dimid_spaceid, H5P_DEFAULT);
    if (dimid_attid  < 0)
        BAIL(NC_EHDFERR);


    /* Write it. */
    LOG((4, "%s: writing secret dimid %d", __func__, dimid));
    if (H5Awrite(dimid_attid, H5T_NATIVE_INT, &dimid) < 0)
        BAIL(NC_EHDFERR);

exit:
    /* Close stuff*/
    if (dimid_spaceid >= 0 && H5Sclose(dimid_spaceid) < 0)
        BAIL2(NC_EHDFERR);
    if (dimid_attid >= 0 && H5Aclose(dimid_attid) < 0)
        BAIL2(NC_EHDFERR);

    return retval;
}

/**
 * @internal This function creates the HDF5 dataset for a variable.
 *
 * @param grp Pointer to group info struct.
 * @param var Pointer to variable info struct.
 * @param write_dimid True to write dimid.
 *
 * @return ::NC_NOERR
 * @returns NC_ECHAR Conversions of NC_CHAR forbidden.
 * @returns NC_EVARMETA HDF5 returning error creating datatype.
 * @returns NC_EHDFERR HDF5 returning error.
 * @returns NC_EBADTYE Type not found.
 * @author Ed Hartnett
 */
static int
var_create_dataset(NC_GRP_INFO_T *grp, NC_VAR_INFO_T *var, nc_bool_t write_dimid)
{
    NC_HDF5_GRP_INFO_T *hdf5_grp;
    NC_HDF5_VAR_INFO_T *hdf5_var;
    hid_t plistid = 0, access_plistid = 0, typeid = 0, spaceid = 0;
    hsize_t chunksize[H5S_MAX_RANK], dimsize[H5S_MAX_RANK], maxdimsize[H5S_MAX_RANK];
    int d;
    void *fillp = NULL;
    NC_DIM_INFO_T *dim = NULL;
    char *name_to_use;
    int retval;
    unsigned int* params = NULL;

    assert(grp && grp->format_grp_info && var && var->format_var_info);

    LOG((3, "%s:: name %s", __func__, var->hdr.name));

    /* Get HDF5-specific group and var info. */
    hdf5_grp = (NC_HDF5_GRP_INFO_T *)grp->format_grp_info;
    hdf5_var = (NC_HDF5_VAR_INFO_T *)var->format_var_info;

    /* Scalar or not, we need a creation property list. */
    if ((plistid = H5Pcreate(H5P_DATASET_CREATE)) < 0)
        BAIL(NC_EHDFERR);
    if ((access_plistid = H5Pcreate(H5P_DATASET_ACCESS)) < 0)
        BAIL(NC_EHDFERR);

    /* Turn off object tracking times in HDF5. */
    if (H5Pset_obj_track_times(plistid, 0) < 0)
        BAIL(NC_EHDFERR);

    /* Find the HDF5 type of the dataset. */
    if ((retval = nc4_get_hdf_typeid(grp->nc4_info, var->type_info->hdr.id, &typeid,
                                     var->type_info->endianness)))
        BAIL(retval);

    /* Figure out what fill value to set, if any. */
    if (var->no_fill)
    {
        /* Required to truly turn HDF5 fill values off */
        if (H5Pset_fill_time(plistid, H5D_FILL_TIME_NEVER) < 0)
            BAIL(NC_EHDFERR);
    }
    else
    {
        if ((retval = nc4_get_fill_value(grp->nc4_info, var, &fillp)))
            BAIL(retval);

        /* If there is a fill value, set it. */
        if (fillp)
        {
            if (var->type_info->nc_type_class == NC_STRING)
            {
                if (H5Pset_fill_value(plistid, typeid, fillp) < 0)
                    BAIL(NC_EHDFERR);
            }
            else
            {
                /* The fill value set in HDF5 must always be presented as
                 * a native type, even if the endianness for this dataset
                 * is non-native. HDF5 will translate the fill value to
                 * the target endiannesss. */
                hid_t fill_typeid = 0;

                if ((retval = nc4_get_hdf_typeid(grp->nc4_info, var->type_info->hdr.id, &fill_typeid,
                                                 NC_ENDIAN_NATIVE)))
                    BAIL(retval);
                if (H5Pset_fill_value(plistid, fill_typeid, fillp) < 0)
                {
                    if (H5Tclose(fill_typeid) < 0)
                        BAIL(NC_EHDFERR);
                    BAIL(NC_EHDFERR);
                }
                if (H5Tclose(fill_typeid) < 0)
                    BAIL(NC_EHDFERR);
            }
        }
    }

    /* If the user wants to compress the data, using either zlib
     * (a.k.a deflate) or szip, or another filter, set that up now.
     * Szip and zip can be turned on
     * either directly with nc_def_var_szip/deflate(), or using
     * nc_def_var_filter(). If the user
     * has specified a filter, it will be applied here. */
    if(var->filters != NULL) {
	size_t j;
	NClist* filters = (NClist*)var->filters;
	for(j=0;j<nclistlength(filters);j++) {
	    struct NC_HDF5_Filter* fi = (struct NC_HDF5_Filter*)nclistget(filters,j);
	    if(fi->filterid == H5Z_FILTER_FLETCHER32) {
	        if(H5Pset_fletcher32(plistid) < 0)
                    BAIL(NC_EHDFERR);
	    } else if(fi->filterid == H5Z_FILTER_SHUFFLE) {
	        if(H5Pset_shuffle(plistid) < 0)
                    BAIL(NC_EHDFERR);
            } else if(fi->filterid == H5Z_FILTER_DEFLATE) {/* Handle zip case here */
                if(fi->nparams != 1)
                    BAIL(NC_EFILTER);
                unsigned int level = fi->params[0];
                if(H5Pset_deflate(plistid, level) < 0)
                    BAIL(NC_EFILTER);
            } else if(fi->filterid == H5Z_FILTER_SZIP) {/* Handle szip case here */
                if(fi->nparams != 2)
                    BAIL(NC_EFILTER);
                unsigned int options_mask = fi->params[0];
                unsigned int bits_per_pixel = fi->params[1];
                if(H5Pset_szip(plistid, options_mask, bits_per_pixel) < 0)
                    BAIL(NC_EFILTER);
            } else {
                herr_t code = H5Pset_filter(plistid, fi->filterid,
                                            H5Z_FLAG_OPTIONAL, /* always make optional so filters on vlens are ignored */
                                           fi->nparams, fi->params);
		if(code < 0)
                    BAIL(NC_EFILTER);
	    }
        }
    }

    /* If ndims non-zero, get info for all dimensions. We look up the
       dimids and get the len of each dimension. We need this to create
       the space for the dataset. In netCDF a dimension length of zero
       means an unlimited dimension. */
    if (var->ndims)
    {
        int unlimdim = 0;

        /* Check to see if any unlimited dimensions are used in this var. */
        for (d = 0; d < var->ndims; d++) {
            dim = var->dim[d];
            assert(dim && dim->hdr.id == var->dimids[d]);
            if (dim->unlimited)
                unlimdim++;
        }

        /* If there are no unlimited dims, and no filters, and the user
         * has not specified chunksizes, use contiguous variable for
         * better performance. */
        if (nclistlength((NClist*)var->filters) == 0 &&
            (var->chunksizes == NULL || !var->chunksizes[0]) && !unlimdim)
	    var->storage = NC_CONTIGUOUS;

        /* Gather current & maximum dimension sizes, along with chunk
         * sizes. */
        for (d = 0; d < var->ndims; d++)
        {
            dim = var->dim[d];
            assert(dim && dim->hdr.id == var->dimids[d]);
            dimsize[d] = dim->unlimited ? NC_HDF5_UNLIMITED_DIMSIZE : dim->len;
            maxdimsize[d] = dim->unlimited ? H5S_UNLIMITED : (hsize_t)dim->len;
            if (var->storage == NC_CHUNKED)
            {
                if (var->chunksizes[d])
                    chunksize[d] = var->chunksizes[d];
                else
                {
                    size_t type_size;
                    if (var->type_info->nc_type_class == NC_STRING)
                        type_size = sizeof(char *);
                    else
                        type_size = var->type_info->size;

                    /* Unlimited dim always gets chunksize of 1. */
                    if (dim->unlimited)
                        chunksize[d] = 1;
                    else
                        chunksize[d] = (hsize_t)pow(DEFAULT_CHUNK_SIZE/(double)type_size,
                                                    1/(double)((int)var->ndims - unlimdim));

                    /* If the chunksize is greater than the dim
                     * length, make it the dim length. */
                    if (!dim->unlimited && chunksize[d] > dim->len)
                        chunksize[d] = dim->len;

                    /* Remember the computed chunksize */
                    var->chunksizes[d] = chunksize[d];
                }
            }
        }

        /* Create the dataspace. */
        if ((spaceid = H5Screate_simple((int)var->ndims, dimsize, maxdimsize)) < 0)
            BAIL(NC_EHDFERR);
    }
    else
    {
        if ((spaceid = H5Screate(H5S_SCALAR)) < 0)
            BAIL(NC_EHDFERR);
    }

    /* Set the var storage to contiguous, compact, or chunked. Don't
     * try to set chunking for scalar vars, they will default to
     * contiguous if not set to compact. */
    if (var->storage == NC_CONTIGUOUS)
    {
        if (H5Pset_layout(plistid, H5D_CONTIGUOUS) < 0)
            BAIL(NC_EHDFERR);
    }
    else if (var->storage == NC_COMPACT)
    {
        if (H5Pset_layout(plistid, H5D_COMPACT) < 0)
            BAIL(NC_EHDFERR);
    }
    else if (var->ndims)
    {
        if (H5Pset_chunk(plistid, (int)var->ndims, chunksize) < 0)
            BAIL(NC_EHDFERR);
    }

    /* Turn on creation order tracking. */
    if (!grp->nc4_info->no_attr_create_order) {
      if (H5Pset_attr_creation_order(plistid, H5P_CRT_ORDER_TRACKED|
				     H5P_CRT_ORDER_INDEXED) < 0)
        BAIL(NC_EHDFERR);
    }

    /* Set per-var chunk cache, for chunked datasets. */
    if (var->storage == NC_CHUNKED && var->chunkcache.size)
        if (H5Pset_chunk_cache(access_plistid, var->chunkcache.nelems,
                               var->chunkcache.size, var->chunkcache.preemption) < 0)
            BAIL(NC_EHDFERR);

    /* At long last, create the dataset. */
    name_to_use = var->alt_name ? var->alt_name : var->hdr.name;
    LOG((4, "%s: about to H5Dcreate2 dataset %s of type 0x%x", __func__,
         name_to_use, typeid));
    if ((hdf5_var->hdf_datasetid = H5Dcreate2(hdf5_grp->hdf_grpid, name_to_use, typeid,
                                              spaceid, H5P_DEFAULT, plistid, access_plistid)) < 0)
        BAIL(NC_EHDFERR);
    var->created = NC_TRUE;
    var->is_new_var = NC_FALSE;

    /* Always write the hidden coordinates attribute, which lists the
     * dimids of this var. When present, this speeds opens. When not
     * present, dimscale matching is used. */
    if (var->ndims)
        if ((retval = write_coord_dimids(var)))
            BAIL(retval);

    /* If this is a dimscale, mark it as such in the HDF5 file. Also
     * find the dimension info and store the dataset id of the dimscale
     * dataset. */
    if (hdf5_var->dimscale)
    {
        if (H5DSset_scale(hdf5_var->hdf_datasetid, var->hdr.name) < 0)
            BAIL(NC_EHDFERR);

        /* If this is a multidimensional coordinate variable, write a
         * coordinates attribute. */
        /* if (var->ndims > 1) */
        /*    if ((retval = write_coord_dimids(var))) */
        /*       BAIL(retval); */

        /* If desired, write the netCDF dimid. */
        if (write_dimid)
            if ((retval = write_netcdf4_dimid(hdf5_var->hdf_datasetid, var->dimids[0])))
                BAIL(retval);
    }

    /* If quantization is in use, write an attribute indicating it, a
     * single integer which is the number of significant digits 
     * (NSD, for BitGroom and Granular BitRound) or number of significant bits
     * (NSB, for BitRound). */
    if (var->quantize_mode)
	if ((retval = write_quantize_att(var)))
	    BAIL(retval);

    /* Write attributes for this var. */
    if ((retval = write_attlist(var->att, var->hdr.id, grp)))
        BAIL(retval);

    /* The file is now up-to-date with all settings for this var. */
    var->attr_dirty = NC_FALSE;

exit:
    nullfree(params);
    if (typeid > 0 && H5Tclose(typeid) < 0)
        BAIL2(NC_EHDFERR);
    if (plistid > 0 && H5Pclose(plistid) < 0)
        BAIL2(NC_EHDFERR);
    if (access_plistid > 0 && H5Pclose(access_plistid) < 0)
        BAIL2(NC_EHDFERR);
    if (spaceid > 0 && H5Sclose(spaceid) < 0)
        BAIL2(NC_EHDFERR);
    if (fillp)
    {
        if (var->type_info->nc_type_class == NC_VLEN)
            nc_free_vlen((nc_vlen_t *)fillp);
        else if (var->type_info->nc_type_class == NC_STRING && *(char **)fillp)
            free(*(char **)fillp);
        free(fillp);
    }

    return retval;
}

/**
 * @internal Adjust the chunk cache of a var for better
 * performance.
 *
 * @note For contiguous and compact storage vars, or when parallel I/O
 * is in use, this function will do nothing and return ::NC_NOERR;
 *
 * @param grp Pointer to group info struct.
 * @param var Pointer to var info struct.
 *
 * @return ::NC_NOERR No error.
 * @author Ed Hartnett
 */
int
nc4_adjust_var_cache(NC_GRP_INFO_T *grp, NC_VAR_INFO_T *var)
{
    size_t chunk_size_bytes = 1;
    int d;
    int retval;

    /* Nothing to be done for contiguous or compact data. */
    if (var->storage != NC_CHUNKED)
        return NC_NOERR;

#ifdef USE_PARALLEL4
    /* Don't set cache for files using parallel I/O. */
    if (grp->nc4_info->parallel)
        return NC_NOERR;
#endif

    /* How many bytes in the chunk? */
    for (d = 0; d < var->ndims; d++)
        chunk_size_bytes *= var->chunksizes[d];
    if (var->type_info->size)
        chunk_size_bytes *= var->type_info->size;
    else
        chunk_size_bytes *= sizeof(char *);

    /* If the chunk cache is too small, and the user has not changed
     * the default value of the chunk cache size, then increase the
     * size of the cache. */
    if (var->chunkcache.size == CHUNK_CACHE_SIZE)
        if (chunk_size_bytes > var->chunkcache.size)
        {
            var->chunkcache.size = chunk_size_bytes * DEFAULT_CHUNKS_IN_CACHE;
            if (var->chunkcache.size > DEFAULT_CHUNK_CACHE_SIZE)
                var->chunkcache.size = DEFAULT_CHUNK_CACHE_SIZE;
            if ((retval = nc4_reopen_dataset(grp, var)))
                return retval;
        }

    return NC_NOERR;
}

/**
 * @internal Create a HDF5 defined type from a NC_TYPE_INFO_T struct,
 * and commit it to the file.
 *
 * @param grp Pointer to group info struct.
 * @param type Pointer to type info struct.
 *
 * @return NC_NOERR No error.
 * @return NC_EHDFERR HDF5 error.
 * @return NC_ECHAR Conversions of NC_CHAR forbidden.
 * @return NC_EVARMETA HDF5 returning error creating datatype.
 * @return NC_EHDFERR HDF5 returning error.
 * @return NC_EBADTYE Type not found.
 * @author Ed Hartnett
 */
static int
commit_type(NC_GRP_INFO_T *grp, NC_TYPE_INFO_T *type)
{
    NC_HDF5_GRP_INFO_T *hdf5_grp;
    NC_HDF5_TYPE_INFO_T *hdf5_type;
    hid_t base_hdf_typeid;
    int retval;

    assert(grp && grp->format_grp_info && type && type->format_type_info);

    /* Get HDF5-specific group and type info. */
    hdf5_grp = (NC_HDF5_GRP_INFO_T *)grp->format_grp_info;
    hdf5_type = (NC_HDF5_TYPE_INFO_T *)type->format_type_info;

    /* Did we already record this type? */
    if (type->committed)
        return NC_NOERR;

    /* Is this a compound type? */
    if (type->nc_type_class == NC_COMPOUND)
    {
        NC_FIELD_INFO_T *field;
        hid_t hdf_base_typeid, hdf_typeid;
        size_t i;

        if ((hdf5_type->hdf_typeid = H5Tcreate(H5T_COMPOUND, type->size)) < 0)
            return NC_EHDFERR;
        LOG((4, "creating compound type %s hdf_typeid 0x%x", type->hdr.name,
             hdf5_type->hdf_typeid));

        for(i=0;i<nclistlength(type->u.c.field);i++)
        {
            field = (NC_FIELD_INFO_T *)nclistget(type->u.c.field, i);
            assert(field);
            if ((retval = nc4_get_hdf_typeid(grp->nc4_info, field->nc_typeid,
                                             &hdf_base_typeid, type->endianness)))
                return retval;

            /* If this is an array, create a special array type. */
            if (field->ndims)
            {
                int d;
                hsize_t dims[NC_MAX_VAR_DIMS];

                for (d = 0; d < field->ndims; d++)
                    dims[d] = (hsize_t)field->dim_size[d];
                if ((hdf_typeid = H5Tarray_create1(hdf_base_typeid, field->ndims,
						   dims, NULL)) < 0)
                {
                    if (H5Tclose(hdf_base_typeid) < 0)
                        return NC_EHDFERR;
                    return NC_EHDFERR;
                }
                if (H5Tclose(hdf_base_typeid) < 0)
                    return NC_EHDFERR;
            }
            else
                hdf_typeid = hdf_base_typeid;
            LOG((4, "inserting field %s offset %d hdf_typeid 0x%x", field->hdr.name,
                 field->offset, hdf_typeid));
            if (H5Tinsert(hdf5_type->hdf_typeid, field->hdr.name, field->offset,
                          hdf_typeid) < 0)
                return NC_EHDFERR;
            if (H5Tclose(hdf_typeid) < 0)
                return NC_EHDFERR;
        }
    }
    else if (type->nc_type_class == NC_VLEN)
    {
        /* Find the HDF typeid of the base type of this vlen. */
        if ((retval = nc4_get_hdf_typeid(grp->nc4_info, type->u.v.base_nc_typeid,
                                         &base_hdf_typeid, type->endianness)))
            return retval;

        /* Create a vlen type. */
        if ((hdf5_type->hdf_typeid = H5Tvlen_create(base_hdf_typeid)) < 0)
            return NC_EHDFERR;
    }
    else if (type->nc_type_class == NC_OPAQUE)
    {
        /* Create the opaque type. */
        if ((hdf5_type->hdf_typeid = H5Tcreate(H5T_OPAQUE, type->size)) < 0)
            return NC_EHDFERR;
    }
    else if (type->nc_type_class == NC_ENUM)
    {
        NC_ENUM_MEMBER_INFO_T *enum_m;
        size_t i;

        if (nclistlength(type->u.e.enum_member) == 0)
            return NC_EINVAL;

        /* Find the HDF typeid of the base type of this enum. */
        if ((retval = nc4_get_hdf_typeid(grp->nc4_info, type->u.e.base_nc_typeid,
                                         &base_hdf_typeid, type->endianness)))
            return retval;

        /* Create an enum type. */
        if ((hdf5_type->hdf_typeid =  H5Tenum_create(base_hdf_typeid)) < 0)
            return NC_EHDFERR;

        /* Add all the members to the HDF5 type. */
        for(i=0;i<nclistlength(type->u.e.enum_member);i++) {
            enum_m = (NC_ENUM_MEMBER_INFO_T*)nclistget(type->u.e.enum_member,i);
            if (H5Tenum_insert(hdf5_type->hdf_typeid, enum_m->name, enum_m->value) < 0)
                return NC_EHDFERR;
        }
    }
    else
    {
        LOG((0, "Unknown class: %d", type->nc_type_class));
        return NC_EBADTYPE;
    }

    /* Commit the type. */
    if (H5Tcommit1(hdf5_grp->hdf_grpid, type->hdr.name, hdf5_type->hdf_typeid) < 0)
        return NC_EHDFERR;
    type->committed = NC_TRUE;
    LOG((4, "just committed type %s, HDF typeid: 0x%x", type->hdr.name,
         hdf5_type->hdf_typeid));

    /* Later we will always use the native typeid. In this case, it is
     * a copy of the same type pointed to by hdf_typeid, but it's
     * easier to maintain a copy. */
    if ((hdf5_type->native_hdf_typeid = H5Tget_native_type(hdf5_type->hdf_typeid,
                                                           H5T_DIR_DEFAULT)) < 0)
        return NC_EHDFERR;

    return NC_NOERR;
}

/**
 * @internal Write an attribute, with value 1, to indicate that strict
 * NC3 rules apply to this file.
 *
 * @param hdf_grpid HDF5 group ID.
 *
 * @returns NC_NOERR No error.
 * @returns NC_EHDFERR HDF5 returned an error.
 * @author Ed Hartnett
 */
static int
write_nc3_strict_att(hid_t hdf_grpid)
{
    hid_t attid = 0, spaceid = 0;
    int one = 1;
    int retval = NC_NOERR;
    htri_t attr_exists;

    /* If the attribute already exists, call that a success and return
     * NC_NOERR. */
    if ((attr_exists = H5Aexists(hdf_grpid, NC3_STRICT_ATT_NAME)) < 0)
        return NC_EHDFERR;
    if (attr_exists)
        return NC_NOERR;

    /* Create the attribute to mark this as a file that needs to obey
     * strict netcdf-3 rules. */
    if ((spaceid = H5Screate(H5S_SCALAR)) < 0)
        BAIL(NC_EFILEMETA);
    if ((attid = H5Acreate1(hdf_grpid, NC3_STRICT_ATT_NAME,
                           H5T_NATIVE_INT, spaceid, H5P_DEFAULT)) < 0)
        BAIL(NC_EFILEMETA);
    if (H5Awrite(attid, H5T_NATIVE_INT, &one) < 0)
        BAIL(NC_EFILEMETA);

exit:
    if (spaceid > 0 && (H5Sclose(spaceid) < 0))
        BAIL2(NC_EFILEMETA);
    if (attid > 0 && (H5Aclose(attid) < 0))
        BAIL2(NC_EFILEMETA);
    return retval;
}

/**
 * @internal Create a HDF5 group that is not the root group. HDF5
 * properties are set in the group to ensure that objects and
 * attributes are kept in creation order, instead of alphebetical
 * order (the HDF5 default).
 *
 * @param grp Pointer to group info struct.
 *
 * @return NC_NOERR No error.
 * @return NC_EHDFERR HDF5 error.
 * @author Ed Hartnett
 */
static int
create_group(NC_GRP_INFO_T *grp)
{
    NC_HDF5_GRP_INFO_T *hdf5_grp, *parent_hdf5_grp;
    hid_t gcpl_id = -1;
    int retval = NC_NOERR;;

    assert(grp && grp->format_grp_info && grp->parent &&
           grp->parent->format_grp_info);

    /* Get HDF5 specific group info for group and parent. */
    hdf5_grp = (NC_HDF5_GRP_INFO_T *)grp->format_grp_info;
    parent_hdf5_grp = (NC_HDF5_GRP_INFO_T *)grp->parent->format_grp_info;
    assert(parent_hdf5_grp->hdf_grpid);

    /* Create group, with link_creation_order set in the group
     * creation property list. */
    if ((gcpl_id = H5Pcreate(H5P_GROUP_CREATE)) < 0)
        BAIL(NC_EHDFERR);

    /* Set track_times to be FALSE. */
    if (H5Pset_obj_track_times(gcpl_id, 0) < 0)
        BAIL(NC_EHDFERR);

    /* Tell HDF5 to keep track of objects in creation order. */
    if (H5Pset_link_creation_order(gcpl_id, H5P_CRT_ORDER_TRACKED|H5P_CRT_ORDER_INDEXED) < 0)
        BAIL(NC_EHDFERR);

    /* Tell HDF5 to keep track of attributes in creation order. */
    if (!grp->nc4_info->no_attr_create_order) {
      if (H5Pset_attr_creation_order(gcpl_id, H5P_CRT_ORDER_TRACKED|H5P_CRT_ORDER_INDEXED) < 0)
        BAIL(NC_EHDFERR);
    }

    /* Create the group. */
    if ((hdf5_grp->hdf_grpid = H5Gcreate2(parent_hdf5_grp->hdf_grpid, grp->hdr.name,
                                          H5P_DEFAULT, gcpl_id, H5P_DEFAULT)) < 0)
        BAIL(NC_EHDFERR);

exit:
    if (gcpl_id > -1 && H5Pclose(gcpl_id) < 0)
        BAIL2(NC_EHDFERR);
    if (retval)
        if (hdf5_grp->hdf_grpid > 0 && H5Gclose(hdf5_grp->hdf_grpid) < 0)
            BAIL2(NC_EHDFERR);
    return retval;
}

/**
 * @internal After all the datasets of the file have been read, it's
 * time to sort the wheat from the chaff. Which of the datasets are
 * netCDF dimensions, and which are coordinate variables, and which
 * are non-coordinate variables.
 *
 * @param grp Pointer to group info struct.
 *
 * @return ::NC_NOERR No error.
 * @returns NC_EDIMSCALE HDF5 returned an error when trying to attach a dimension scale.
 * @author Ed Hartnett
 */
static int
attach_dimscales(NC_GRP_INFO_T *grp)
{
    NC_VAR_INFO_T *var;
    NC_HDF5_VAR_INFO_T *hdf5_var;

    /* Attach dimension scales. */
    for (size_t v = 0; v < ncindexsize(grp->vars); v++)
    {
        /* Get pointer to var and HDF5-specific var info. */
        var = (NC_VAR_INFO_T *)ncindexith(grp->vars, v);
        assert(var && var->format_var_info);
        hdf5_var = (NC_HDF5_VAR_INFO_T *)var->format_var_info;

        /* Scales themselves do not attach. But I really wish they
         * would. */
        if (hdf5_var->dimscale)
            continue;

        /* Find the scale for each dimension, if any, and attach it. */
        for (unsigned int d = 0; d < var->ndims; d++)
        {
            /* Is there a dimscale for this dimension? */
            if (hdf5_var->dimscale_attached)
            {
                if (!hdf5_var->dimscale_attached[d])
                {
                    hid_t dsid;  /* Dataset ID for dimension */
                    assert(var->dim[d] && var->dim[d]->hdr.id == var->dimids[d] &&
                           var->dim[d]->format_dim_info);

                    LOG((2, "%s: attaching scale for dimid %d to var %s",
                         __func__, var->dimids[d], var->hdr.name));

                    /* Find dataset ID for dimension */
                    if (var->dim[d]->coord_var)
                        dsid = ((NC_HDF5_VAR_INFO_T *)(var->dim[d]->coord_var->format_var_info))->hdf_datasetid;
                    else
                        dsid = ((NC_HDF5_DIM_INFO_T *)var->dim[d]->format_dim_info)->hdf_dimscaleid;

                    if (dsid <= 0)
                        return NC_EDIMSCALE;

                    /* Attach the scale. */
                    if (H5DSattach_scale(hdf5_var->hdf_datasetid, dsid, d) < 0)
                        return NC_EDIMSCALE;
                    hdf5_var->dimscale_attached[d] = NC_TRUE;
                }
            }
        }
    }

    return NC_NOERR;
}

/**
 * @internal Does a variable exist?
 *
 * @param grpid HDF5 group ID.
 * @param name Name of variable.
 * @param exists Pointer that gets 1 of the variable exists, 0 otherwise.
 *
 * @return ::NC_NOERR No error.
 * @author Ed Hartnett
 */
static int
var_exists(hid_t grpid, char *name, nc_bool_t *exists)
{
    htri_t link_exists;

    /* Reset the boolean */
    *exists = NC_FALSE;

    /* Check if the object name exists in the group */
    if ((link_exists = H5Lexists(grpid, name, H5P_DEFAULT)) < 0)
        return NC_EHDFERR;
    if (link_exists)
    {
        H5G_stat_t statbuf;

        /* Get info about the object */
        if (H5Gget_objinfo(grpid, name, 1, &statbuf) < 0)
            return NC_EHDFERR;

        if (H5G_DATASET == statbuf.type)
            *exists = NC_TRUE;
    }

    return NC_NOERR;
}

/**
 * @internal Convert a coordinate variable HDF5 dataset into one that
 * is not a coordinate variable. This happens during renaming of vars
 * and dims. This function removes the HDF5 NAME and CLASS attributes
 * associated with dimension scales, and also the NC_DIMID_ATT_NAME
 * attribute which may be present, and, if it does, holds the dimid of
 * the coordinate variable.
 *
 * @param hdf_datasetid The HDF5 dataset ID of the coordinate variable
 * dataset.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EHDFERR HDF5 error.
 * @author Ed Hartnett
 */
static int
remove_coord_atts(hid_t hdf_datasetid)
{
    htri_t attr_exists;

    /* If the variable dataset has an optional NC_DIMID_ATT_NAME
     * attribute, delete it. */
    if ((attr_exists = H5Aexists(hdf_datasetid, NC_DIMID_ATT_NAME)) < 0)
        return NC_EHDFERR;
    if (attr_exists)
    {
        if (H5Adelete(hdf_datasetid, NC_DIMID_ATT_NAME) < 0)
            return NC_EHDFERR;
    }

    /* Remove the dimension scale 'CLASS' & 'NAME' attributes. */
    if ((attr_exists = H5Aexists(hdf_datasetid,
                                 HDF5_DIMSCALE_CLASS_ATT_NAME)) < 0)
        return NC_EHDFERR;
    if (attr_exists)
    {
        if (H5Adelete(hdf_datasetid, HDF5_DIMSCALE_CLASS_ATT_NAME) < 0)
            return NC_EHDFERR;
    }
    if ((attr_exists = H5Aexists(hdf_datasetid,
                                 HDF5_DIMSCALE_NAME_ATT_NAME)) < 0)
        return NC_EHDFERR;
    if (attr_exists)
    {
        if (H5Adelete(hdf_datasetid, HDF5_DIMSCALE_NAME_ATT_NAME) < 0)
            return NC_EHDFERR;
    }
    return NC_NOERR;
}

/**
 * @internal This function writes a variable. The principle difficulty
 * comes from the possibility that this is a coordinate variable, and
 * was already written to the file as a dimension-only dimscale. If
 * this occurs, then it must be deleted and recreated.
 *
 * @param var Pointer to variable info struct.
 * @param grp Pointer to group info struct.
 * @param write_dimid
 *
 * @returns NC_NOERR No error.
 * @returns NC_EHDFERR HDF5 returned an error.
 * @author Ed Hartnett, Quincey Koziol
 */
static int
write_var(NC_VAR_INFO_T *var, NC_GRP_INFO_T *grp, nc_bool_t write_dimid)
{
    NC_HDF5_GRP_INFO_T *hdf5_grp;
    NC_HDF5_VAR_INFO_T *hdf5_var;
    nc_bool_t replace_existing_var = NC_FALSE;
    int retval;

    assert(var && var->format_var_info && grp && grp->format_grp_info);

    LOG((4, "%s: writing var %s", __func__, var->hdr.name));

    /* Get HDF5-specific group and var info. */
    hdf5_grp = (NC_HDF5_GRP_INFO_T *)grp->format_grp_info;
    hdf5_var = (NC_HDF5_VAR_INFO_T *)var->format_var_info;

    /* If the variable has already been created & the fill value changed,
     * indicate that the existing variable should be replaced. */
    if (var->created && var->fill_val_changed)
    {
        replace_existing_var = NC_TRUE;
        var->fill_val_changed = NC_FALSE;
        /* If the variable is going to be replaced, we need to flag any
           other attributes associated with the variable as 'dirty', or
           else *only* the fill value attribute will be copied over and
           the rest will be lost.  See
           https://github.com/Unidata/netcdf-c/issues/239 */
        flag_atts_dirty(var->att);
    }

    /* Is this a coordinate var that has already been created in
     * the HDF5 file as a dimscale dataset? Check for dims with the
     * same name in this group. If there is one, check to see if
     * this object exists in the HDF group. */
    if (var->became_coord_var)
    {
        if ((NC_DIM_INFO_T *)ncindexlookup(grp->dim, var->hdr.name))
        {
            nc_bool_t exists;

            if ((retval = var_exists(hdf5_grp->hdf_grpid, var->hdr.name, &exists)))
                return retval;
            if (exists)
            {
                /* Indicate that the variable already exists, and should
                 * be replaced. */
                replace_existing_var = NC_TRUE;
                flag_atts_dirty(var->att);
            }
        }
    }

    /* Check dims if the variable will be replaced, so that the
     * dimensions will be de-attached and re-attached correctly. */
    if (replace_existing_var)
    {
        NC_DIM_INFO_T *d1;

        /* Is there a dim with this var's name? */
        if ((d1 = (NC_DIM_INFO_T *)ncindexlookup(grp->dim, var->hdr.name)))
        {
            nc_bool_t exists;
            assert(d1->format_dim_info && d1->hdr.name);

            if ((retval = var_exists(hdf5_grp->hdf_grpid, var->hdr.name, &exists)))
                return retval;
            if (exists)
            {
                hid_t dsid;

                /* Find dataset ID for dimension */
                if (d1->coord_var)
                    dsid = ((NC_HDF5_VAR_INFO_T *)d1->coord_var->format_var_info)->hdf_datasetid;
                else
                    dsid = ((NC_HDF5_DIM_INFO_T *)d1->format_dim_info)->hdf_dimscaleid;
                assert(dsid > 0);

                /* If we're replacing an existing dimscale dataset, go to
                 * every var in the file and detach this dimension scale,
                 * because we have to delete it. */
                if ((retval = rec_detach_scales(grp->nc4_info->root_grp,
                                                var->dimids[0], dsid)))
                    return retval;
            }
        }
    }

    /* If this is not a dimension scale, remove any attached scales,
     * and delete dimscale attributes from the var. */
    if (var->was_coord_var && hdf5_var->dimscale_attached)
    {
        /* If the variable already exists in the file, Remove any dimension scale
         * attributes from it, if they exist. */
        if (var->created)
            if ((retval = remove_coord_atts(hdf5_var->hdf_datasetid)))
                return retval;

        /* If this is a regular var, detach all its dim scales. */
        for (unsigned int d = 0; d < var->ndims; d++)
        {
            if (hdf5_var->dimscale_attached[d])
            {
                hid_t dsid;  /* Dataset ID for dimension */
                assert(var->dim[d] && var->dim[d]->hdr.id == var->dimids[d] &&
                       var->dim[d]->format_dim_info);

                /* Find dataset ID for dimension */
                if (var->dim[d]->coord_var)
                    dsid = ((NC_HDF5_VAR_INFO_T *)var->dim[d]->coord_var->format_var_info)->hdf_datasetid;
                else
                    dsid = ((NC_HDF5_DIM_INFO_T *)var->dim[d]->format_dim_info)->hdf_dimscaleid;
                assert(dsid > 0);

                /* Detach this dim scale. */
                if (H5DSdetach_scale(hdf5_var->hdf_datasetid, dsid, d) < 0)
                    return NC_EHDFERR;
                hdf5_var->dimscale_attached[d] = NC_FALSE;
            }
        }
    }

    /* Delete the HDF5 dataset that is to be replaced. */
    if (replace_existing_var)
    {
        /* Free the HDF5 dataset id. */
        if (hdf5_var->hdf_datasetid && H5Dclose(hdf5_var->hdf_datasetid) < 0)
            return NC_EHDFERR;
        hdf5_var->hdf_datasetid = 0;

        /* Now delete the variable. */
        if (H5Gunlink(hdf5_grp->hdf_grpid, var->hdr.name) < 0)
            return NC_EDIMMETA;
    }

    /* Create the dataset. */
    if (var->is_new_var || replace_existing_var)
    {
        if ((retval = var_create_dataset(grp, var, write_dimid)))
            return retval;
    }
    else
    {
        if (write_dimid && var->ndims)
            if ((retval = write_netcdf4_dimid(hdf5_var->hdf_datasetid,
                                              var->dimids[0])))
                return retval;
    }

    if (replace_existing_var)
    {
        /* If this is a dimension scale, reattach the scale everywhere it
         * is used. (Recall that netCDF dimscales are always 1-D). */
        if(hdf5_var->dimscale)
        {
            if ((retval = rec_reattach_scales(grp->nc4_info->root_grp,
                                              var->dimids[0], hdf5_var->hdf_datasetid)))
                return retval;
        }
        /* If it's not a dimension scale, clear the dimscale attached flags,
         * so the dimensions are re-attached. */
        else
        {
            if (hdf5_var->dimscale_attached)
                memset(hdf5_var->dimscale_attached, 0, sizeof(nc_bool_t) * var->ndims);
        }
    }

    /* Clear coord. var state transition flags */
    var->was_coord_var = NC_FALSE;
    var->became_coord_var = NC_FALSE;

    /* Now check the attributes for this var. */
    if (var->attr_dirty)
    {
        /* Write attributes for this var. */
        if ((retval = write_attlist(var->att, var->hdr.id, grp)))
            return retval;
        var->attr_dirty = NC_FALSE;
    }

    return NC_NOERR;
}

/**
 * @internal Write a HDF5 dataset which is a dimension without a
 * coordinate variable. This is a special 1-D dataset.
 *
 * @param dim Pointer to dim info struct.
 *
 * @returns ::NC_NOERR No error.
 * @returns ::NC_EPERM Read-only file.
 * @returns ::NC_EHDFERR HDF5 returned error.
 * @author Ed Hartnett
 */
int
nc4_create_dim_wo_var(NC_DIM_INFO_T *dim)
{
    NC_HDF5_DIM_INFO_T *hdf5_dim;
    NC_HDF5_GRP_INFO_T *hdf5_grp;
    hid_t spaceid = -1, create_propid = -1;
    hsize_t dims[1], max_dims[1], chunk_dims[1] = {1};
    char dimscale_wo_var[NC_MAX_NAME];
    int retval = NC_NOERR;

    LOG((4, "%s: creating dim %s", __func__, dim->hdr.name));

    /* Sanity check */
    assert(!dim->coord_var);

    /* Get HDF5-specific dim and group info. */
    hdf5_grp = (NC_HDF5_GRP_INFO_T *)dim->container->format_grp_info;
    hdf5_dim = (NC_HDF5_DIM_INFO_T *)dim->format_dim_info;

    /* Create a property list. */
    if ((create_propid = H5Pcreate(H5P_DATASET_CREATE)) < 0)
        BAIL(NC_EHDFERR);

    /* Turn off recording of times associated with this object. */
    if (H5Pset_obj_track_times(create_propid, 0) < 0)
        BAIL(NC_EHDFERR);

    /* Set size of dataset to size of dimension. */
    dims[0] = dim->len;
    max_dims[0] = dim->len;

    /* If this dimension scale is unlimited (i.e. it's an unlimited
     * dimension), then set up chunking, with a chunksize of 1. */
    if (dim->unlimited)
    {
        max_dims[0] = H5S_UNLIMITED;
        if (H5Pset_chunk(create_propid, 1, chunk_dims) < 0)
            BAIL(NC_EHDFERR);
    }

    /* Set up space. */
    if ((spaceid = H5Screate_simple(1, dims, max_dims)) < 0)
        BAIL(NC_EHDFERR);

    /* Turn on creation-order tracking. */
    if (!dim->container->nc4_info->no_attr_create_order) {
      if (H5Pset_attr_creation_order(create_propid, H5P_CRT_ORDER_TRACKED|
				     H5P_CRT_ORDER_INDEXED) < 0)
        BAIL(NC_EHDFERR);
    }
    /* Create the dataset that will be the dimension scale. */
    LOG((4, "%s: about to H5Dcreate1 a dimscale dataset %s", __func__,
         dim->hdr.name));
    if ((hdf5_dim->hdf_dimscaleid = H5Dcreate2(hdf5_grp->hdf_grpid, dim->hdr.name,
                                               H5T_IEEE_F32BE, spaceid,
                                               H5P_DEFAULT, create_propid,
                                               H5P_DEFAULT)) < 0)
        BAIL(NC_EHDFERR);

    /* Indicate that this is a scale. Also indicate that not
     * be shown to the user as a variable. It is hidden. It is
     * a DIM WITHOUT A VARIABLE! */
    snprintf(dimscale_wo_var, sizeof(dimscale_wo_var), "%s%10d", DIM_WITHOUT_VARIABLE, (int)dim->len);
    if (H5DSset_scale(hdf5_dim->hdf_dimscaleid, dimscale_wo_var) < 0)
        BAIL(NC_EHDFERR);

    /* Since this dimension was created out of order, we cannot rely on
     * it getting the correct dimid on file open. We must assign it
     * explicitly. */
    if ((retval = write_netcdf4_dimid(hdf5_dim->hdf_dimscaleid, dim->hdr.id)))
        BAIL(retval);

exit:
    if (spaceid > 0 && H5Sclose(spaceid) < 0)
        BAIL2(NC_EHDFERR);
    if (create_propid > 0 && H5Pclose(create_propid) < 0)
        BAIL2(NC_EHDFERR);
    return retval;
}

/**
 * @internal Write a dimension.
 *
 * @param dim Pointer to dim info struct.
 * @param grp Pointer to group info struct.
 * @param write_dimid
 *
 * @returns ::NC_NOERR No error.
 * @returns ::NC_EPERM Read-only file.
 * @returns ::NC_EHDFERR HDF5 returned error.
 * @author Ed Hartnett
 */
static int
write_dim(NC_DIM_INFO_T *dim, NC_GRP_INFO_T *grp, nc_bool_t write_dimid)
{
    NC_HDF5_DIM_INFO_T *hdf5_dim;
    int retval = NC_NOERR;

    assert(dim && dim->format_dim_info && grp && grp->format_grp_info);

    /* Get HDF5-specific dim and group info. */
    hdf5_dim = (NC_HDF5_DIM_INFO_T *)dim->format_dim_info;

    /* If there's no dimscale dataset for this dim, create one,
     * and mark that it should be hidden from netCDF as a
     * variable. (That is, it should appear as a dimension
     * without an associated variable.) */
    if (!hdf5_dim->hdf_dimscaleid)
        if ((retval = nc4_create_dim_wo_var(dim)))
            BAIL(retval);

    /* Did we extend an unlimited dimension? */
    if (dim->extended)
    {
        NC_VAR_INFO_T *v1 = NULL;

        assert(dim->unlimited);

        /* If this is a dimension with an associated coordinate var,
         * then update the length of that coord var. */
        v1 = dim->coord_var;
        if (v1)
        {
            NC_HDF5_VAR_INFO_T *hdf5_v1;
            hsize_t *new_size;
            int d1;

            hdf5_v1 = (NC_HDF5_VAR_INFO_T *)v1->format_var_info;

            /* Extend the dimension scale dataset to reflect the new
             * length of the dimension. */
            if (!(new_size = malloc(v1->ndims * sizeof(hsize_t))))
                BAIL(NC_ENOMEM);
            for (d1 = 0; d1 < v1->ndims; d1++)
            {
                assert(v1->dim[d1] && v1->dim[d1]->hdr.id == v1->dimids[d1]);
                new_size[d1] = v1->dim[d1]->len;
            }
            if (H5Dset_extent(hdf5_v1->hdf_datasetid, new_size) < 0)
                BAIL(NC_EHDFERR);
            free(new_size);
        }
    }

    /* If desired, write the secret dimid. This will be used instead of
     * the dimid that the dimension would otherwise receive based on
     * creation order. This can be necessary when dims and their
     * coordinate variables were created in different order. */
    if (write_dimid && hdf5_dim->hdf_dimscaleid)
        if ((retval = write_netcdf4_dimid(hdf5_dim->hdf_dimscaleid, dim->hdr.id)))
            BAIL(retval);

exit:

    return retval;
}

/**
 * @internal Recursively write all the metadata in a group. Groups and
 * types have all already been written. Propagate bad coordinate
 * order to subgroups, if detected.
 *
 * @param grp Pointer to group info struct.
 * @param bad_coord_order 1 if there is a bad coordinate order.
 *
 * @returns NC_NOERR No error.
 * @returns NC_EHDFERR HDF5 returned an error.
 * @author Ed Hartnett
 */
int
nc4_rec_write_metadata(NC_GRP_INFO_T *grp, nc_bool_t bad_coord_order)
{
    NC_DIM_INFO_T *dim = NULL;
    NC_VAR_INFO_T *var = NULL;
    NC_GRP_INFO_T *child_grp = NULL;
    int coord_varid = -1;
    size_t var_index = 0;
    size_t dim_index = 0;
    int retval;

    assert(grp && grp->hdr.name &&
           ((NC_HDF5_GRP_INFO_T *)(grp->format_grp_info))->hdf_grpid);
    LOG((3, "%s: grp->hdr.name %s, bad_coord_order %d", __func__, grp->hdr.name,
         bad_coord_order));

    /* Write global attributes for this group. */
    if ((retval = write_attlist(grp->att, NC_GLOBAL, grp)))
        return retval;

    /* Set the pointers to the beginning of the list of dims & vars in this
     * group. */
    dim = (NC_DIM_INFO_T *)ncindexith(grp->dim, dim_index);
    var = (NC_VAR_INFO_T *)ncindexith(grp->vars, var_index);

    /* Because of HDF5 ordering the dims and vars have to be stored in
     * this way to ensure that the dims and coordinate vars come out in
     * the correct order. */
    while (dim || var)
    {
        nc_bool_t found_coord, wrote_coord;

        /* Write non-coord dims in order, stopping at the first one that
         * has an associated coord var. */
        for (found_coord = NC_FALSE; dim && !found_coord; )
        {
            if (!dim->coord_var)
            {
                if ((retval = write_dim(dim, grp, bad_coord_order)))
                    return retval;
            }
            else
            {
                coord_varid = dim->coord_var->hdr.id;
                found_coord = NC_TRUE;
            }
            dim = (NC_DIM_INFO_T *)ncindexith(grp->dim, ++dim_index);
        }

        /* Write each var. When we get to the coord var we are waiting
         * for (if any), then we break after writing it. */
        for (wrote_coord = NC_FALSE; var && !wrote_coord; )
        {
            if ((retval = write_var(var, grp, bad_coord_order)))
                return retval;
            if (found_coord && var->hdr.id == coord_varid)
                wrote_coord = NC_TRUE;
            var = (NC_VAR_INFO_T *)ncindexith(grp->vars, ++var_index);
        }
    } /* end while */

    /* Attach dimscales to vars in this group. Unless directed not to. */
    if (!grp->nc4_info->no_dimscale_attach) {
        if ((retval = attach_dimscales(grp)))
            return retval;
    }

    /* If there are any child groups, write their metadata. */
    for (size_t i = 0; i < ncindexsize(grp->children); i++)
    {
        child_grp = (NC_GRP_INFO_T *)ncindexith(grp->children, i);
        assert(child_grp);
        if ((retval = nc4_rec_write_metadata(child_grp, bad_coord_order)))
            return retval;
    }
    return NC_NOERR;
}

/**
 * @internal Recursively write all groups and types.
 *
 * @param grp Pointer to group info struct.
 *
 * @returns NC_NOERR No error.
 * @returns NC_EHDFERR HDF5 returned an error.
 * @author Ed Hartnett
 */
int
nc4_rec_write_groups_types(NC_GRP_INFO_T *grp)
{
    NC_GRP_INFO_T *child_grp;
    NC_HDF5_GRP_INFO_T *hdf5_grp;
    NC_TYPE_INFO_T *type;
    int retval;
    size_t i;

    assert(grp && grp->hdr.name && grp->format_grp_info);
    LOG((3, "%s: grp->hdr.name %s", __func__, grp->hdr.name));

    /* Get HDF5-specific group info. */
    hdf5_grp = (NC_HDF5_GRP_INFO_T *)grp->format_grp_info;

    /* Create the group in the HDF5 file if it doesn't exist. */
    if (!hdf5_grp->hdf_grpid)
        if ((retval = create_group(grp)))
            return retval;

    /* If this is the root group of a file with strict NC3 rules, write
     * an attribute. But don't leave the attribute open. */
    if (!grp->parent && (grp->nc4_info->cmode & NC_CLASSIC_MODEL))
        if ((retval = write_nc3_strict_att(hdf5_grp->hdf_grpid)))
            return retval;

    /* If there are any user-defined types, write them now. */
    for(i=0;i<ncindexsize(grp->type);i++) {
        type = (NC_TYPE_INFO_T *)ncindexith(grp->type, i);
        assert(type);
        if ((retval = commit_type(grp, type)))
            return retval;
    }

    /* If there are any child groups, write their groups and types. */
    for(i=0;i<ncindexsize(grp->children);i++) {
        if((child_grp = (NC_GRP_INFO_T*)ncindexith(grp->children,i)) == NULL) continue;
        if ((retval = nc4_rec_write_groups_types(child_grp)))
            return retval;
    }
    return NC_NOERR;
}

/**
 * @internal In our first pass through the data, we may have
 * encountered variables before encountering their dimscales, so go
 * through the vars in this file and make sure we've got a dimid for
 * each.
 *
 * @param grp Pointer to group info struct.
 *
 * @returns NC_NOERR No error.
 * @returns NC_EHDFERR HDF5 returned an error.
 * @returns NC_ENOMEM Out of memory.
 * @author Ed Hartnett
 */
int
nc4_rec_match_dimscales(NC_GRP_INFO_T *grp)
{
    NC_GRP_INFO_T *g;
    NC_VAR_INFO_T *var;
    NC_DIM_INFO_T *dim;
    int retval = NC_NOERR;
    size_t i;

    assert(grp && grp->hdr.name);
    LOG((4, "%s: grp->hdr.name %s", __func__, grp->hdr.name));

    /* Perform var dimscale match for child groups. */
    for (i = 0; i < ncindexsize(grp->children); i++)
    {
        g = (NC_GRP_INFO_T*)ncindexith(grp->children, i);
        assert(g);
        if ((retval = nc4_rec_match_dimscales(g)))
            return retval;
    }

    /* Check all the vars in this group. If they have dimscale info,
     * try and find a dimension for them. */
    for (i = 0; i < ncindexsize(grp->vars); i++)
    {
        NC_HDF5_VAR_INFO_T *hdf5_var;
        int d;

        /* Get pointer to var and to the HDF5-specific var info. */
        var = (NC_VAR_INFO_T*)ncindexith(grp->vars, i);
        assert(var && var->format_var_info);
        hdf5_var = (NC_HDF5_VAR_INFO_T *)var->format_var_info;

        /* Check all vars and see if dim[i] != NULL if dimids[i] valid. */
        /* This loop is very odd. Under normal circumstances, var->dimid[d] is zero
           (from the initial calloc) which is a legitimate dimid. The code does not
           distinguish this case from the dimscale case where the id might actually
           be defined.
           The original nc4_find_dim searched up the group tree looking for the given
           dimid in one of the dim lists associated with each ancestor group.
           I changed nc4_fnd_dim to use the dimid directly using h5->alldims.
           However, here that is incorrect because it will find the dimid 0 always
           (if any dimensions were defined). Except that when dimscale dimids have
           been defined, one or more of the values in var->dimids will have a
           legitimate value.
           The solution I choose is to modify nc4_var_list_add to initialize dimids to
           illegal values (-1). This is another example of the problems with dimscales.
        */
        const size_t ndims = var->ndims;
        for (size_t d = 0; d < ndims; d++)
        {
            if (var->dim[d] == NULL) {
                nc4_find_dim(grp, var->dimids[d], &var->dim[d], NULL);
            }
            /*       assert(var->dim[d] && var->dim[d]->hdr.id == var->dimids[d]); */
        }

        /* Skip dimension scale variables */
        if (!hdf5_var->dimscale)
        {
            /* Are there dimscales for this variable? */
            if (hdf5_var->dimscale_hdf5_objids)
            {
                for (size_t d = 0; d < var->ndims; d++)
                {
                    nc_bool_t finished = NC_FALSE;
                    LOG((5, "%s: var %s has dimscale info...", __func__, var->hdr.name));

                    /* Check this and parent groups. */
                    for (g = grp; g && !finished; g = g->parent)
                    {
                        /* Check all dims in this group. */
                        for (size_t j = 0; j < ncindexsize(g->dim); j++)
                        {
                            /* Get the HDF5 specific dim info. */
                            NC_HDF5_DIM_INFO_T *hdf5_dim;
                            dim = (NC_DIM_INFO_T *)ncindexith(g->dim, j);
                            assert(dim && dim->format_dim_info);
                            hdf5_dim = (NC_HDF5_DIM_INFO_T *)dim->format_dim_info;

                            /* Check for exact match of fileno/objid arrays
                             * to find identical objects in HDF5 file. */
#if H5_VERSION_GE(1,12,0)
                            int token_cmp;
                            if (H5Otoken_cmp(hdf5_var->hdf_datasetid, &hdf5_var->dimscale_hdf5_objids[d].token, &hdf5_dim->hdf5_objid.token, &token_cmp) < 0)
                                return NC_EHDFERR;

                            if (hdf5_var->dimscale_hdf5_objids[d].fileno == hdf5_dim->hdf5_objid.fileno &&
                                token_cmp == 0)
#else
                            if (hdf5_var->dimscale_hdf5_objids[d].fileno[0] == hdf5_dim->hdf5_objid.fileno[0] &&
                                hdf5_var->dimscale_hdf5_objids[d].objno[0] == hdf5_dim->hdf5_objid.objno[0] &&
                                hdf5_var->dimscale_hdf5_objids[d].fileno[1] == hdf5_dim->hdf5_objid.fileno[1] &&
                                hdf5_var->dimscale_hdf5_objids[d].objno[1] == hdf5_dim->hdf5_objid.objno[1])
#endif
                            {
                                LOG((4, "%s: for dimension %d, found dim %s", __func__,
                                     d, dim->hdr.name));
                                var->dimids[d] = dim->hdr.id;
                                var->dim[d] = dim;
                                finished = NC_TRUE;
                                break;
                            }
                        } /* next dim */
                    } /* next grp */
                    LOG((5, "%s: dimid for this dimscale is %d", __func__,
                         var->type_info->hdr.id));
                } /* next var->dim */
            }
            /* No dimscales for this var! Invent phony dimensions. */
            else
            {
                hid_t spaceid = 0;
                hsize_t *h5dimlen = NULL, *h5dimlenmax = NULL;
                int dataset_ndims;

                /* Find the space information for this dimension. */
                if ((spaceid = H5Dget_space(hdf5_var->hdf_datasetid)) < 0)
                    return NC_EHDFERR;

                /* Get the len of each dim in the space. */
                if (var->ndims)
                {
                    if (!(h5dimlen = malloc(var->ndims * sizeof(hsize_t))))
                        return NC_ENOMEM;
                    if (!(h5dimlenmax = malloc(var->ndims * sizeof(hsize_t))))
                    {
                        free(h5dimlen);
                        return NC_ENOMEM;
                    }
                    if ((dataset_ndims = H5Sget_simple_extent_dims(spaceid, h5dimlen,
                                                                   h5dimlenmax)) < 0) {
                        free(h5dimlenmax);
                        free(h5dimlen);
                        return NC_EHDFERR;
                    }
                    if (dataset_ndims != var->ndims) {
                        free(h5dimlenmax);
                        free(h5dimlen);
                        return NC_EHDFERR;
                    }
                }
                else
                {
                    /* Make sure it's scalar. */
                    if (H5Sget_simple_extent_type(spaceid) != H5S_SCALAR)
                        return NC_EHDFERR;
                }

                /* Release the space object. */
                if (H5Sclose(spaceid) < 0) {
                    free(h5dimlen);
                    free(h5dimlenmax);
                    return NC_EHDFERR;
                }

                /* Create a phony dimension for each dimension in the
                 * dataset, unless there already is one the correct
                 * size. */
                for (d = 0; d < var->ndims; d++)
                {
                    nc_bool_t match = NC_FALSE;
                    /* Is there already a phony dimension of the correct size? */
                    for(size_t k=0;k<ncindexsize(grp->dim);k++) {
                        if((dim = (NC_DIM_INFO_T*)ncindexith(grp->dim,k)) == NULL) continue;
                        if ((dim->len == h5dimlen[d]) &&
                            ((h5dimlenmax[d] == H5S_UNLIMITED && dim->unlimited) ||
                             (h5dimlenmax[d] != H5S_UNLIMITED && !dim->unlimited)))
                        {match = NC_TRUE; break;}
                    }

                    /* Didn't find a phony dim? Then create one. */
                    if (match == NC_FALSE)
                    {
                        char phony_dim_name[NC_MAX_NAME + 1];
                        snprintf(phony_dim_name, sizeof(phony_dim_name), "phony_dim_%d", grp->nc4_info->next_dimid);
                        LOG((3, "%s: creating phony dim for var %s", __func__, var->hdr.name));
                        if ((retval = nc4_dim_list_add(grp, phony_dim_name, h5dimlen[d], -1, &dim)))
                        {
                            free(h5dimlenmax);
                            free(h5dimlen);
                            return retval;
                        }
                        /* Create struct for HDF5-specific dim info. */
                        if (!(dim->format_dim_info = calloc(1, sizeof(NC_HDF5_DIM_INFO_T))))
                            return NC_ENOMEM;
                        if (h5dimlenmax[d] == H5S_UNLIMITED)
                            dim->unlimited = NC_TRUE;
                    }

                    /* The variable must remember the dimid. */
                    var->dimids[d] = dim->hdr.id;
                    var->dim[d] = dim;
                } /* next dim */

                /* Free the memory we malloced. */
                free(h5dimlen);
                free(h5dimlenmax);
            }
        }
    }

    return retval;
}

/**
 * @internal Report information about an open HDF5 object. This is
 * called on any still-open objects when a HDF5 file close is
 * attempted.
 *
 * @param uselog If true, send output to LOG not stderr.
 * @param id HDF5 ID of open object.
 * @param type Type of HDF5 object, file, dataset, etc.
 *
 * @author Dennis Heimbigner
 */
void
reportobject(int uselog, hid_t id, unsigned int type)
{
    char name[NC_HDF5_MAX_NAME];
    ssize_t len;
    const char* typename = NULL;
    long long printid = (long long)id;

    len = H5Iget_name(id, name, NC_HDF5_MAX_NAME);
    if(len < 0) return;
    name[len] = '\0';

    switch (type) {
    case H5F_OBJ_FILE: typename = "File"; break;
    case H5F_OBJ_DATASET: typename = "Dataset"; break;
    case H5F_OBJ_GROUP: typename = "Group"; break;
    case H5F_OBJ_DATATYPE: typename = "Datatype"; break;
    case H5F_OBJ_ATTR:
        typename = "Attribute";
        len = H5Aget_name(id, NC_HDF5_MAX_NAME, name);
        if(len < 0) len = 0;
        name[len] = '\0';
        break;
    default: typename = "<unknown>"; break;
    }
#ifdef LOGGING
    if(uselog) {
        LOG((0,"Type = %s(%lld) name='%s'",typename,printid,name));
    } else
#endif
    {
        fprintf(stderr,"Type = %s(%lld) name='%s'",typename,printid,name);
    }

}

/**
 * @internal
 *
 * @param uselog
 * @param fid HDF5 ID.
 * @param ntypes Number of types.
 * @param otypes Pointer that gets number of open types.
 *
 * @author Dennis Heimbigner
 */
static void
reportopenobjectsT(int uselog, hid_t fid, int ntypes, unsigned int* otypes)
{
    int t,i;
    ssize_t ocount;
    hid_t* idlist = NULL;

    /* Always report somehow */
#ifdef LOGGING
    if(uselog)
        LOG((0,"\nReport: open objects on %lld",(long long)fid));
    else
#endif
        fprintf(stdout,"\nReport: open objects on %lld\n",(long long)fid);
    size_t maxobjs = (size_t)H5Fget_obj_count(fid,H5F_OBJ_ALL);
    if(idlist != NULL) free(idlist);
    idlist = (hid_t*)malloc(sizeof(hid_t)*maxobjs);
    for(t=0;t<ntypes;t++) {
        unsigned int ot = otypes[t];
        ocount = H5Fget_obj_ids(fid,ot,maxobjs,idlist);
        for(i=0;i<ocount;i++) {
            hid_t o = idlist[i];
            reportobject(uselog,o,ot);
        }
    }
    if(idlist != NULL) free(idlist);
}

/**
 * @internal Report open objects.
 *
 * @param uselog
 * @param fid HDF5 file ID.
 *
 * @author Dennit Heimbigner
 */
void
reportopenobjects(int uselog, hid_t fid)
{
    unsigned int OTYPES[5] = {H5F_OBJ_FILE, H5F_OBJ_DATASET, H5F_OBJ_GROUP,
                              H5F_OBJ_DATATYPE, H5F_OBJ_ATTR};

    reportopenobjectsT(uselog, fid ,5, OTYPES);
}

/**
 * @internal Report open objects given a pointer to NC_FILE_INFO_T object
 *
 * @param h5 file object
 *
 * @author Dennis Heimbigner
 */
void
showopenobjects5(NC_FILE_INFO_T* h5)
{
    NC_HDF5_FILE_INFO_T *hdf5_info;

    assert(h5 && h5->format_file_info);
    hdf5_info = (NC_HDF5_FILE_INFO_T *)h5->format_file_info;

    fprintf(stderr,"===== begin showopenobjects =====\n");
    reportopenobjects(0,hdf5_info->hdfid);
    fprintf(stderr,"===== end showopenobjects =====\n");
    fflush(stderr);
}

/**
 * @internal Report open objects given an ncid
 * Defined to support user or gdb level call.
 *
 * @param ncid file id
 *
 * @author Dennis Heimbigner
 */
void
showopenobjects(int ncid)
{
    NC_FILE_INFO_T* h5 = NULL;

    /* Find our metadata for this file. */
    if (nc4_find_nc_grp_h5(ncid, NULL, NULL, &h5) != NC_NOERR)
        fprintf(stderr,"failed\n");
    else
        showopenobjects5(h5);
    fflush(stderr);
}

/**
 * @internal Get HDF5 library version.
 *
 * @param major Pointer that gets major version number.
 * @param minor Pointer that gets minor version number.
 * @param release Pointer that gets release version number.
 *
 * @returns NC_NOERR No error.
 * @returns NC_EHDFERR HDF5 returned error.
 * @author Dennis Heimbigner
 */
int
NC4_hdf5get_libversion(unsigned* major,unsigned* minor,unsigned* release)
{
    if(H5get_libversion(major,minor,release) < 0)
        return NC_EHDFERR;
    return NC_NOERR;
}

/**
 * @internal Get HDF5 superblock version.
 *
 * @param h5 Pointer to HDF5 file info struct.
 * @param idp Pointer that gets superblock version.
 *
 * @returns NC_NOERR No error.
 * @returns NC_EHDFERR HDF5 returned error.
 * @author Dennis Heimbigner
 */
int
NC4_hdf5get_superblock(struct NC_FILE_INFO* h5, int* idp)
{
    NC_HDF5_FILE_INFO_T *hdf5_info;
    int stat = NC_NOERR;
    unsigned super;
    hid_t plist = -1;

    assert(h5 && h5->format_file_info);
    hdf5_info = (NC_HDF5_FILE_INFO_T *)h5->format_file_info;

    if((plist = H5Fget_create_plist(hdf5_info->hdfid)) < 0)
    {stat = NC_EHDFERR; goto done;}
    if(H5Pget_version(plist, &super, NULL, NULL, NULL) < 0)
    {stat = NC_EHDFERR; goto done;}
    if(idp) *idp = (int)super;
done:
    if(plist >= 0) H5Pclose(plist);
    return stat;
}

static int NC4_root_att_exists(NC_FILE_INFO_T*, const char* aname);
static int NC4_walk(hid_t, int*);

/**
 * @internal Determine whether file is netCDF-4.
 *
 * We define a file as being from netcdf-4 if any of the following
 * are true:
 * 1. NCPROPS attribute exists in root group
 * 2. NC3_STRICT_ATT_NAME exists in root group
 * 3. any of NC_ATT_REFERENCE_LIST, NC_ATT_CLASS,
 * NC_ATT_DIMENSION_LIST, NC_ATT_NAME,
 * NC_ATT_COORDINATES, NC_DIMID_ATT_NAME
 * exist anywhere in the file; note that this
 * requires walking the file.

 * @note WARNINGS:
 *   1. False negatives are possible for a small subset of netcdf-4
 *      created files; especially if the file looks like a simple
        netcdf classic file.
 *   2. Deliberate falsification in the file can be used to cause
 *      a false positive.
 *
 * @param h5 Pointer to HDF5 file info struct.
 *
 * @returns NC_NOERR No error.
 * @author Dennis Heimbigner.
 */
int
NC4_isnetcdf4(struct NC_FILE_INFO* h5)
{
    int stat;
    int isnc4 = 0;
    int exists;
    int count;

    /* Look for NC3_STRICT_ATT_NAME */
    exists = NC4_root_att_exists(h5,NC3_STRICT_ATT_NAME);
    if(exists)
        {isnc4 = 1; goto done;}
    /* Look for _NCProperties */
    exists = NC4_root_att_exists(h5,NCPROPS);
    if(exists)
        {isnc4 = 1; goto done;}
    /* attribute did not exist */
    /* => last resort: walk the HDF5 file looking for markers */
    count = 0;
    stat = NC4_walk(((NC_HDF5_GRP_INFO_T *)(h5->root_grp->format_grp_info))->hdf_grpid,
                    &count);
    if(stat != NC_NOERR)
        isnc4 = 0;
    else /* Threshold is at least two matches */
        isnc4 = (count >= 2);

done:
    return isnc4;
}

/**
 * @internal See if the named root attribute exists.
 *
 * @param h5 Pointer to HDF5 file info struct.
 * @param aname attribute name for which to look.
 *
 * @returns -1 if error || 1 if exists || 0 otherwise
 * @author Dennis Heimbigner.
 */
static int
NC4_root_att_exists(NC_FILE_INFO_T *h5, const char* aname)
{
    hid_t grpid = -1;
    htri_t attr_exists;
    
    /* Get root group ID. */
    grpid = ((NC_HDF5_GRP_INFO_T *)(h5->root_grp->format_grp_info))->hdf_grpid;

    /* See if the NC3_STRICT_ATT_NAME attribute exists */
    if ((attr_exists = H5Aexists(grpid, aname))<0)
        return -1;
    return (attr_exists?1:0);
}

/**
 * @internal Walk group struct.
 *
 * @param gid HDF5 ID of starting group.
 * @param countp Pointer that gets count.
 *
 * @returns NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
NC4_walk(hid_t gid, int* countp)
{
    int ncstat = NC_NOERR;
    int j,na;
    ssize_t len;
    hsize_t nobj;
    herr_t err;
    int otype;
    hid_t grpid, dsid;
    char name[NC_HDF5_MAX_NAME];

    /* walk group members of interest */
    err = H5Gget_num_objs(gid, &nobj);
    if(err < 0) return err;

    for(hsize_t i = 0; i < nobj; i++) {
        /* Get name & kind of object in the group */
        len = H5Gget_objname_by_idx(gid,i,name,(size_t)NC_HDF5_MAX_NAME);
        if(len < 0) return (int)len;

        otype =  H5Gget_objtype_by_idx(gid, i);
        switch(otype) {
        case H5G_GROUP:
	    grpid = H5Gopen1(gid,name);
            NC4_walk(grpid,countp);
            H5Gclose(grpid);
            break;
        case H5G_DATASET: /* variables */
            /* Check for phony_dim */
            if(strcmp(name,"phony_dim")==0)
                *countp = *countp + 1;
            dsid = H5Dopen1(gid,name);
            na = H5Aget_num_attrs(dsid);
            for(j = 0; j < na; j++) {
                hid_t aid =  H5Aopen_idx(dsid,(unsigned int)    j);
                if(aid >= 0) {
                    const NC_reservedatt* ra;
                    ssize_t len = H5Aget_name(aid, NC_HDF5_MAX_NAME, name);
                    if(len < 0) return (int)len;
                    /* Is this a netcdf-4 marker attribute */
                    ra = NC_findreserved(name);
                    if(ra != NULL)
                        *countp = *countp + 1;
                }
                H5Aclose(aid);
            }
            H5Dclose(dsid);
            break;
        default:/* ignore */
            break;
        }
    }
    return ncstat;
}

