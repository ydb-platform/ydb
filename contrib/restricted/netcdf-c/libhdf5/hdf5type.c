/* Copyright 2005-2018, University Corporation for Atmospheric
 * Research. See the COPYRIGHT file for copying and redistribution
 * conditions. */
/**
 * @file @internal This file is part of netcdf-4, a netCDF-like
 * interface for HDF5, or a HDF5 backend for netCDF, depending on your
 * point of view.
 *
 * This file handles the nc4 user-defined type functions
 * (i.e. compound and opaque types).
 *
 * @author Ed Hartnett
 */

#include "config.h"
#include "hdf5internal.h"
#include <stddef.h>

/**
 * @internal Determine if two types are equal.
 *
 * @param ncid1 First file/group ID.
 * @param typeid1 First type ID.
 * @param ncid2 Second file/group ID.
 * @param typeid2 Second type ID.
 * @param equalp Pointer that will get 1 if the two types are equal.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_EBADTYPE Type not found.
 * @return ::NC_EINVAL Invalid type.
 * @author Ed Hartnett
 */
extern int
NC4_inq_type_equal(int ncid1, nc_type typeid1, int ncid2,
                   nc_type typeid2, int *equalp)
{
    NC_GRP_INFO_T *grpone, *grptwo;
    NC_TYPE_INFO_T *type1, *type2;
    int retval;

    LOG((2, "nc_inq_type_equal: ncid1 0x%x typeid1 %d ncid2 0x%x typeid2 %d",
         ncid1, typeid1, ncid2, typeid2));

    /* Check input. */
    if(equalp == NULL) return NC_NOERR;

    if (typeid1 <= NC_NAT || typeid2 <= NC_NAT)
        return NC_EINVAL;

    /* If one is atomic, and the other user-defined, the types are not
     * equal. */
    if ((typeid1 <= NC_STRING && typeid2 > NC_STRING) ||
        (typeid2 <= NC_STRING && typeid1 > NC_STRING))
    {
        *equalp = 0;
        return NC_NOERR;
    }

    /* If both are atomic types, the answer is easy. */
    if (typeid1 <= NUM_ATOMIC_TYPES)
    {
        if (typeid1 == typeid2)
            *equalp = 1;
        else
            *equalp = 0;
        return NC_NOERR;
    }

    /* Not atomic types - so find type1 and type2 information. */
    if ((retval = nc4_find_nc4_grp(ncid1, &grpone)))
        return retval;
    if (!(type1 = nclistget(grpone->nc4_info->alltypes, (size_t)typeid1)))
        return NC_EBADTYPE;
    if ((retval = nc4_find_nc4_grp(ncid2, &grptwo)))
        return retval;
    if (!(type2 = nclistget(grptwo->nc4_info->alltypes, (size_t)typeid2)))
        return NC_EBADTYPE;

    /* Are the two types equal? */
    {
        hid_t hid1, hid2;

        /* Get the HDF5 types from the HDF5-specific type info. */
        assert(type1->format_type_info && type2->format_type_info);
        hid1 = ((NC_HDF5_TYPE_INFO_T *)type1->format_type_info)->native_hdf_typeid;
        hid2 = ((NC_HDF5_TYPE_INFO_T *)type2->format_type_info)->native_hdf_typeid;

        /* Ask HDF5 if the types are equal. */
        if ((retval = H5Tequal(hid1, hid2)) < 0)
            return NC_EHDFERR;
        *equalp = 1 ? retval : 0;
    }

    return NC_NOERR;
}

/**
 * @internal This internal function adds a new user defined type to
 * the metadata of a group of an open file.
 *
 * @param ncid File and group ID.
 * @param size Size in bytes of new type.
 * @param name Name of new type.
 * @param base_typeid Base type ID.
 * @param type_class NC_VLEN, NC_ENUM, or NC_STRING
 * @param typeidp Pointer that gets new type ID.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOTNC4 User types in netCDF-4 files only.
 * @return ::NC_EINVAL Bad size.
 * @return ::NC_EMAXNAME Name is too long.
 * @return ::NC_EBADNAME Name breaks netCDF name rules.
 * @return ::NC_ESTRICTNC3 Cannot define user types in classic model.
 * @author Ed Hartnett
 */
static int
add_user_type(int ncid, size_t size, const char *name, nc_type base_typeid,
              nc_type type_class, nc_type *typeidp)
{
    NC_FILE_INFO_T *h5;
    NC_GRP_INFO_T *grp;
    NC_TYPE_INFO_T *type;
    NC_HDF5_TYPE_INFO_T *hdf5_type;
    char norm_name[NC_MAX_NAME + 1];
    int retval;

    /* Check and normalize the name. */
    if ((retval = nc4_check_name(name, norm_name)))
        return retval;

    LOG((2, "%s: ncid 0x%x size %d name %s base_typeid %d ",
         __FUNCTION__, ncid, size, norm_name, base_typeid));

    /* Find group metadata. */
    if ((retval = nc4_find_grp_h5(ncid, &grp, &h5)))
        return retval;
    assert(h5 && grp);

    /* User types cannot be defined with classic model flag. */
    if (h5->cmode & NC_CLASSIC_MODEL)
        return NC_ESTRICTNC3;

    /* Turn on define mode if it is not on. */
    if (!(h5->cmode & NC_INDEF))
        if ((retval = NC4_redef(ncid)))
            return retval;

    /* No size is provided for vlens; use the size of nc_vlen_t */
    if (type_class == NC_VLEN)
	size = sizeof(nc_vlen_t);

    /* No size is provided for enums, get it from the base type. */
    else if(type_class == NC_ENUM)
    {
        if ((retval = nc4_get_typelen_mem(grp->nc4_info, base_typeid, &size)))
            return retval;
    }

    /* Else better be defined */
    else if (size <= 0)
        return NC_EINVAL;

    /* Check that this name is not in use as a var, grp, or type. */
    if ((retval = nc4_check_dup_name(grp, norm_name)))
        return retval;

    /* Add to our list of types. */
    if ((retval = nc4_type_list_add(grp, size, norm_name, &type)))
        return retval;

    /* Allocate storage for HDF5-specific type info. */
    if (!(hdf5_type = calloc(1, sizeof(NC_HDF5_TYPE_INFO_T))))
        return NC_ENOMEM;
    type->format_type_info = hdf5_type;

    /* Remember info about this type. */
    type->nc_type_class = type_class;
    switch(type_class) {
    case NC_ENUM:
        type->u.e.base_nc_typeid = base_typeid;
        type->u.e.enum_member = nclistnew();
	break;
    case NC_OPAQUE:
	break;
    case NC_VLEN:
        type->u.v.base_nc_typeid = base_typeid;
	break;
    case NC_COMPOUND:
        type->u.c.field = nclistnew();
	break;
    default: break;
    }
    if((retval=NC4_set_varsize(type))) return retval;

    /* Return the typeid to the user. */
    if (typeidp)
        *typeidp = type->hdr.id;

    return NC_NOERR;
}

/**
 * @internal Create a compound type.
 *
 * @param ncid File and group ID.
 * @param size Gets size in bytes of one element of type.
 * @param name Name of the type.
 * @param typeidp Gets the type ID.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOTNC4 User types in netCDF-4 files only.
 * @return ::NC_EINVAL Bad size.
 * @return ::NC_EMAXNAME Name is too long.
 * @return ::NC_EBADNAME Name breaks netCDF name rules.
 * @return ::NC_ESTRICTNC3 Cannot define user types in classic model.
 * @author Ed Hartnett
 */
int
NC4_def_compound(int ncid, size_t size, const char *name, nc_type *typeidp)
{
    return add_user_type(ncid, size, name, 0, NC_COMPOUND, typeidp);
}

/**
 * @internal Insert a named field into a compound type.
 *
 * @param ncid File and group ID.
 * @param typeid1 Type ID.
 * @param name Name of the type.
 * @param offset Offset of field.
 * @param field_typeid Field type ID.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_EMAXNAME Name is too long.
 * @return ::NC_EBADNAME Name breaks netCDF name rules.
 * @author Ed Hartnett
 */
int
NC4_insert_compound(int ncid, nc_type typeid1, const char *name, size_t offset,
                    nc_type field_typeid)
{
    return nc_insert_array_compound(ncid, typeid1, name, offset,
                                    field_typeid, 0, NULL);
}

/**
 * @internal Insert a named array into a compound type.
 *
 * @param ncid File and group ID.
 * @param typeid1 Type ID.
 * @param name Name of the array field.
 * @param offset Offset in bytes.
 * @param field_typeid Type of field.
 * @param ndims Number of dims for field.
 * @param dim_sizesp Array of dim sizes.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_EMAXNAME Name is too long.
 * @return ::NC_EBADNAME Name breaks netCDF name rules.
 * @author Ed Hartnett
 */
extern int
NC4_insert_array_compound(int ncid, int typeid1, const char *name,
                          size_t offset, nc_type field_typeid,
                          int ndims, const int *dim_sizesp)
{
    NC_GRP_INFO_T *grp;
    NC_TYPE_INFO_T *type;
    char norm_name[NC_MAX_NAME + 1];
    int retval;

    LOG((2, "nc_insert_array_compound: ncid 0x%x, typeid %d name %s "
         "offset %d field_typeid %d ndims %d", ncid, typeid1,
         name, offset, field_typeid, ndims));

    /* Check and normalize the name. */
    if ((retval = nc4_check_name(name, norm_name)))
        return retval;

    /* Find file metadata. */
    if ((retval = nc4_find_nc4_grp(ncid, &grp)))
        return retval;

    /* Find type metadata. */
    if ((retval = nc4_find_type(grp->nc4_info, typeid1, &type)))
        return retval;

    /* Did the user give us a good compound type typeid? */
    if (!type || type->nc_type_class != NC_COMPOUND)
        return NC_EBADTYPE;

    /* If this type has already been written to the file, you can't
     * change it. */
    if (type->committed)
        return NC_ETYPDEFINED;

    /* Insert new field into this type's list of fields. */
    if ((retval = nc4_field_list_add(type, norm_name, offset, field_typeid,
                                     ndims, dim_sizesp)))
        return retval;

    /* See if this changes from fixed size to variable size */
    if((retval=NC4_recheck_varsize(type,field_typeid))) return retval;
    return NC_NOERR;
}

/* Opaque type. */

/**
 * @internal Create an opaque type. Provide a size and a name.
 *
 * @param ncid File and group ID.
 * @param datum_size Size in bytes of a datum.
 * @param name Name of new vlen type.
 * @param typeidp Pointer that gets new type ID.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOTNC4 User types in netCDF-4 files only.
 * @return ::NC_EINVAL Bad size.
 * @return ::NC_EMAXNAME Name is too long.
 * @return ::NC_EBADNAME Name breaks netCDF name rules.
 * @return ::NC_ESTRICTNC3 Cannot define user types in classic model.
 * @author Ed Hartnett
 */
int
NC4_def_opaque(int ncid, size_t datum_size, const char *name,
               nc_type *typeidp)
{
    return add_user_type(ncid, datum_size, name, 0, NC_OPAQUE, typeidp);
}


/**
 * @internal Define a variable length type.
 *
 * @param ncid File and group ID.
 * @param name Name of new vlen type.
 * @param base_typeid Base type of vlen.
 * @param typeidp Pointer that gets new type ID.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOTNC4 User types in netCDF-4 files only.
 * @return ::NC_EMAXNAME Name is too long.
 * @return ::NC_EBADNAME Name breaks netCDF name rules.
 * @return ::NC_ESTRICTNC3 Cannot define user types in classic model.
 * @author Ed Hartnett
 */
int
NC4_def_vlen(int ncid, const char *name, nc_type base_typeid,
             nc_type *typeidp)
{
    return add_user_type(ncid, 0, name, base_typeid, NC_VLEN, typeidp);
}

/**
 * @internal Create an enum type. Provide a base type and a name. At
 * the moment only ints are accepted as base types.
 *
 * @param ncid File and group ID.
 * @param base_typeid Base type of vlen.
 * @param name Name of new vlen type.
 * @param typeidp Pointer that gets new type ID.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOTNC4 User types in netCDF-4 files only.
 * @return ::NC_EMAXNAME Name is too long.
 * @return ::NC_EBADNAME Name breaks netCDF name rules.
 * @return ::NC_ESTRICTNC3 Cannot define user types in classic model.
 * @author Ed Hartnett
 */
int
NC4_def_enum(int ncid, nc_type base_typeid, const char *name,
             nc_type *typeidp)
{
    return add_user_type(ncid, 0, name, base_typeid, NC_ENUM, typeidp);
}

/**
 * @internal Insert a identifier value into an enum type. The value
 * must fit within the size of the enum type, the identifier size must
 * be <= NC_MAX_NAME.
 *
 * @param ncid File and group ID.
 * @param typeid1 Type ID.
 * @param identifier Name of this enum value.
 * @param value Value of enum.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_EBADTYPE Type not found.
 * @return ::NC_ETYPDEFINED Type already defined.
 * @author Ed Hartnett
 */
int
NC4_insert_enum(int ncid, nc_type typeid1, const char *identifier,
                const void *value)
{
    NC_GRP_INFO_T *grp;
    NC_TYPE_INFO_T *type;
    char norm_name[NC_MAX_NAME + 1];
    int retval;

    LOG((2, "nc_insert_enum: ncid 0x%x, typeid %d identifier %s value %d", ncid,
         typeid1, identifier, value));

    /* Check and normalize the name. */
    if ((retval = nc4_check_name(identifier, norm_name)))
        return retval;

    /* Find file metadata. */
    if ((retval = nc4_find_nc4_grp(ncid, &grp)))
        return retval;

    /* Find type metadata. */
    if ((retval = nc4_find_type(grp->nc4_info, typeid1, &type)))
        return retval;

    /* Did the user give us a good enum typeid? */
    if (!type || type->nc_type_class != NC_ENUM)
        return NC_EBADTYPE;

    /* If this type has already been written to the file, you can't
     * change it. */
    if (type->committed)
        return NC_ETYPDEFINED;

    /* Insert new field into this type's list of fields. */
    if ((retval = nc4_enum_member_add(type, type->size,
                                      norm_name, value)))
        return retval;

    return NC_NOERR;
}

/**
 * @internal Insert one element into an already allocated vlen array
 * element.
 *
 * @param ncid File and group ID.
 * @param typeid1 Type ID.
 * @param vlen_element The VLEN element to insert.
 * @param len Length of element in bytes.
 * @param data Element data.
 *
 * @return ::NC_NOERR No error.
 * @author Ed Hartnett
 */
int
NC4_put_vlen_element(int ncid, int typeid1, void *vlen_element,
                     size_t len, const void *data)
{
    nc_vlen_t *tmp = (nc_vlen_t*)vlen_element;
    tmp->len = len;
    tmp->p = (void *)data;
    return NC_NOERR;
}

/**
 * @internal Insert one element into an already allocated vlen array
 * element.
 *
 * @param ncid File and group ID.
 * @param typeid1 Type ID.
 * @param vlen_element The VLEN element to insert.
 * @param len Length of element in bytes.
 * @param data Element data.
 *
 * @return ::NC_NOERR No error.
 * @author Ed Hartnett
 */
int
NC4_get_vlen_element(int ncid, int typeid1, const void *vlen_element,
                     size_t *len, void *data)
{
    const nc_vlen_t *tmp = (nc_vlen_t*)vlen_element;
    const size_t type_size = 4;

    *len = tmp->len;
    memcpy(data, tmp->p, tmp->len * type_size);
    return NC_NOERR;
}
