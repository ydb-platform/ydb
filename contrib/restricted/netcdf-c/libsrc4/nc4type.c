/* Copyright 2005, University Corporation for Atmospheric Research. See
 * the COPYRIGHT file for copying and redistribution conditions. */
/**
 * @file
 * @internal This file is part of netcdf-4, a netCDF-like interface
 * for HDF5, or a HDF5 backend for netCDF, depending on your point of
 * view.
 *
 * This file handles the nc4 user-defined type functions
 * (i.e. compound and opaque types).
 *
 * @author Ed Hartnett
 */
#include "nc4internal.h"
#include "nc4dispatch.h"
#include <stddef.h>

/**
 * @internal Find all user-defined types for a location. This finds
 * all user-defined types in a group.
 *
 * @param ncid File and group ID.
 * @param ntypes Pointer that gets the number of user-defined
 * types. Ignored if NULL
 * @param typeids Array that gets the typeids. Ignored if NULL.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @author Ed Hartnett
 */
int
NC4_inq_typeids(int ncid, int *ntypes, int *typeids)
{
    NC_GRP_INFO_T *grp;
    NC_FILE_INFO_T *h5;
    NC_TYPE_INFO_T *type;
    int num = 0;
    int retval;

    LOG((2, "nc_inq_typeids: ncid 0x%x", ncid));

    /* Find info for this file and group, and set pointer to each. */
    if ((retval = nc4_find_grp_h5(ncid, &grp, &h5)))
        return retval;
    assert(h5 && grp);

    /* Count types. */
    if (grp->type) {
        for(size_t i=0;i<ncindexsize(grp->type);i++)
        {
            if((type = (NC_TYPE_INFO_T*)ncindexith(grp->type,i)) == NULL) continue;
            if (typeids)
                typeids[num] = type->hdr.id;
            num++;
        }
    }

    /* Give the count to the user. */
    if (ntypes)
        *ntypes = num;

    return NC_NOERR;
}

/**
 * @internal Get the name and size of a type.
 * For VLEN the base type len is returned.
 *
 * @param ncid File and group ID.
 * @param typeid1 Type ID.
 * @param name Gets the name of the type.
 * @param size Gets the size of one element of the type in bytes.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_EBADTYPE Type not found.
 * @author Ed Hartnett
 */
int
NC4_inq_type(int ncid, nc_type typeid1, char *name, size_t *size)
{
    NC_GRP_INFO_T *grp;
    NC_TYPE_INFO_T *type;

    int retval;

    LOG((2, "nc_inq_type: ncid 0x%x typeid %d", ncid, typeid1));

    /* If this is an atomic type, the answer is easy. */
    if (typeid1 < NUM_ATOMIC_TYPES)
    {
	if((retval=NC4_inq_atomic_type(typeid1, name, size))) return retval;
        return NC_NOERR;
    }

    /* Not an atomic type - so find group. */
    if ((retval = nc4_find_nc4_grp(ncid, &grp)))
        return retval;

    /* Find this type. */
    if (!(type = nclistget(grp->nc4_info->alltypes, (size_t)typeid1)))
        return NC_EBADTYPE;

    if (name)
        strcpy(name, type->hdr.name);

    if (size)
    {
        if (type->nc_type_class == NC_VLEN)
            *size = sizeof(nc_vlen_t);
        else
            *size = type->size;
    }

    return NC_NOERR;
}

/**
 * @internal Find info about any user defined type.
 *
 * @param ncid File and group ID.
 * @param typeid1 Type ID.
 * @param name Gets name of the type.
 * @param size Gets size in bytes of one element of type.
 * @param base_nc_typep Gets the base nc_type.
 * @param nfieldsp Gets the number of fields.
 * @param classp Gets the type class (NC_COMPOUND, NC_ENUM, NC_VLEN).
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_EBADTYPE Type not found.
 * @author Ed Hartnett
 */
int
NC4_inq_user_type(int ncid, nc_type typeid1, char *name, size_t *size,
                  nc_type *base_nc_typep, size_t *nfieldsp, int *classp)
{
    NC_GRP_INFO_T *grp;
    NC_TYPE_INFO_T *type;
    int retval;

    LOG((2, "nc_inq_user_type: ncid 0x%x typeid %d", ncid, typeid1));

    /* Find group metadata. */
    if ((retval = nc4_find_nc4_grp(ncid, &grp)))
        return retval;

    /* Find this type. */
    if (!(type = nclistget(grp->nc4_info->alltypes, (size_t)typeid1)))
        return NC_EBADTYPE;

    /* Count the number of fields. */
    if (nfieldsp)
    {
        if (type->nc_type_class == NC_COMPOUND)
            *nfieldsp = nclistlength(type->u.c.field);
        else if (type->nc_type_class == NC_ENUM)
            *nfieldsp = nclistlength(type->u.e.enum_member);
        else
            *nfieldsp = 0;
    }

    /* Fill in size and name info, if desired. */
    if (size)
    {
        if (type->nc_type_class == NC_VLEN)
            *size = sizeof(nc_vlen_t);
        else if (type->nc_type_class == NC_STRING) {
	    if((retval=NC4_inq_type(ncid,typeid1,NULL,size))) return retval;
        } else
            *size = type->size;
    }
    if (name)
        strcpy(name, type->hdr.name);

    /* VLENS and ENUMs have a base type - that is, they type they are
     * arrays of or enums of. */
    if (base_nc_typep)
    {
        if (type->nc_type_class == NC_ENUM)
            *base_nc_typep = type->u.e.base_nc_typeid;
        else if (type->nc_type_class == NC_VLEN)
            *base_nc_typep = type->u.v.base_nc_typeid;
        else
            *base_nc_typep = NC_NAT;
    }

    /* If the user wants it, tell whether this is a compound, opaque,
     * vlen, enum, or string class of type. */
    if (classp)
        *classp = type->nc_type_class;

    return NC_NOERR;
}

/**
 * @internal Given the ncid, typeid and fieldid, get info about the
 * field.
 *
 * @param ncid File and group ID.
 * @param typeid1 Type ID.
 * @param fieldid Field ID.
 * @param name Gets name of field.
 * @param offsetp Gets offset of field.
 * @param field_typeidp Gets field type ID.
 * @param ndimsp Gets number of dims for this field.
 * @param dim_sizesp Gets the dim sizes for this field.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @author Ed Hartnett
 */
int
NC4_inq_compound_field(int ncid, nc_type typeid1, int fieldid, char *name,
                       size_t *offsetp, nc_type *field_typeidp, int *ndimsp,
                       int *dim_sizesp)
{
    NC_GRP_INFO_T *grp;
    NC_TYPE_INFO_T *type;
    NC_FIELD_INFO_T *field;
    int d, retval;

    /* Find file metadata. */
    if ((retval = nc4_find_nc4_grp(ncid, &grp)))
        return retval;

    /* Find this type. */
    if (!(type = nclistget(grp->nc4_info->alltypes, (size_t)typeid1)))
        return NC_EBADTYPE;

    /* Find the field. */
    if (!(field = nclistget(type->u.c.field, (size_t)fieldid)))
        return NC_EBADFIELD;

    if (name)
        strcpy(name, field->hdr.name);
    if (offsetp)
        *offsetp = field->offset;
    if (field_typeidp)
        *field_typeidp = field->nc_typeid;
    if (ndimsp)
        *ndimsp = field->ndims;
    if (dim_sizesp)
        for (d = 0; d < field->ndims; d++)
            dim_sizesp[d] = field->dim_size[d];

    return NC_NOERR;
}

/**
 * @internal Given the typeid and the name, get the fieldid.
 *
 * @param ncid File and group ID.
 * @param typeid1 Type ID.
 * @param name Name of field.
 * @param fieldidp Pointer that gets new field ID.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_EBADTYPE Type not found.
 * @return ::NC_EBADFIELD Field not found.
 * @author Ed Hartnett
 */
int
NC4_inq_compound_fieldindex(int ncid, nc_type typeid1, const char *name, int *fieldidp)
{
    NC_FILE_INFO_T *h5;
    NC_TYPE_INFO_T *type;
    NC_FIELD_INFO_T *field;
    char norm_name[NC_MAX_NAME + 1];
    int retval;
    size_t i;

    LOG((2, "nc_inq_compound_fieldindex: ncid 0x%x typeid %d name %s",
         ncid, typeid1, name));

    /* Find file metadata. */
    if ((retval = nc4_find_grp_h5(ncid, NULL, &h5)))
        return retval;

    /* Find the type. */
    if ((retval = nc4_find_type(h5, typeid1, &type)))
        return retval;

    /* Did the user give us a good compound type typeid? */
    if (!type || type->nc_type_class != NC_COMPOUND)
        return NC_EBADTYPE;

    /* Normalize name. */
    if ((retval = nc4_normalize_name(name, norm_name)))
        return retval;

    /* Find the field with this name. */
    for (i = 0; i < nclistlength(type->u.c.field); i++)
    {
        field = nclistget(type->u.c.field, i);
        assert(field);
        if (!strcmp(field->hdr.name, norm_name))
            break;
        field = NULL; /* because this is the indicator of not found */
    }

    if (!field)
        return NC_EBADFIELD;

    if (fieldidp)
        *fieldidp = field->hdr.id;
    return NC_NOERR;
}

/**
 * @internal Get enum name from enum value. Name size will be <= NC_MAX_NAME.
 * If the value is not a legitimate enum identifier and the value is zero
 * (the default HDF5 enum fill value), then return the identifier "_UNDEFINED".
 *
 * @param ncid File and group ID.
 * @param xtype Type ID.
 * @param value Value of enum.
 * @param identifier Gets the identifier for this enum value.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_EBADTYPE Type not found.
 * @return ::NC_EINVAL Invalid type data or no matching enum value is found
 * @author Ed Hartnett
 */
int
NC4_inq_enum_ident(int ncid, nc_type xtype, long long value, char *identifier)
{
    NC_GRP_INFO_T *grp;
    NC_TYPE_INFO_T *type;
    NC_ENUM_MEMBER_INFO_T *enum_member;
    long long ll_val;
    size_t i;
    int retval;
    int found;

    LOG((3, "nc_inq_enum_ident: xtype %d value %d\n", xtype, value));

    /* Find group metadata. */
    if ((retval = nc4_find_nc4_grp(ncid, &grp)))
        return retval;

    /* Find this type. */
    if (!(type = nclistget(grp->nc4_info->alltypes, (size_t)xtype)))
        return NC_EBADTYPE;

    /* Complain if they are confused about the type. */
    if (type->nc_type_class != NC_ENUM)
        return NC_EBADTYPE;

    /* Move to the desired enum member in the list. */
    for (found = 0, i = 0; i < nclistlength(type->u.e.enum_member); i++)
    {
        enum_member = nclistget(type->u.e.enum_member, i);
        assert(enum_member);
        switch (type->u.e.base_nc_typeid)
        {
        case NC_BYTE:
            ll_val = *(char *)enum_member->value;
            break;
        case NC_UBYTE:
            ll_val = *(unsigned char *)enum_member->value;
            break;
        case NC_SHORT:
            ll_val = *(short *)enum_member->value;
            break;
        case NC_USHORT:
            ll_val = *(unsigned short *)enum_member->value;
            break;
        case NC_INT:
            ll_val = *(int *)enum_member->value;
            break;
        case NC_UINT:
            ll_val = *(unsigned int *)enum_member->value;
            break;
        case NC_INT64:
        case NC_UINT64:
            ll_val = *(long long *)enum_member->value;
            break;
        default:
            return NC_EINVAL;
        }
        LOG((4, "ll_val=%d", ll_val));
        if (ll_val == value)
        {
            if (identifier)
                strcpy(identifier, enum_member->name);
            found = 1;
            break;
        }
    }

    /* If we didn't find it, life sucks for us. :-( */
    if(!found) {
        if(value == 0) /* Special case for HDF5 default Fill Value*/
	    strcpy(identifier, NC_UNDEFINED_ENUM_IDENT);
        else
            return NC_EINVAL;
    }
    
    return NC_NOERR;
}

/**
 * @internal Get information about an enum member: an identifier and
 * value. Identifier size will be <= NC_MAX_NAME.
 *
 * @param ncid File and group ID.
 * @param typeid1 Type ID.
 * @param idx Enum member index.
 * @param identifier Gets the identifier.
 * @param value Gets the enum value.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_EBADTYPE Type not found.
 * @return ::NC_EINVAL Bad idx.
 * @author Ed Hartnett
 */
int
NC4_inq_enum_member(int ncid, nc_type typeid1, int idx, char *identifier,
                    void *value)
{
    NC_GRP_INFO_T *grp;
    NC_TYPE_INFO_T *type;
    NC_ENUM_MEMBER_INFO_T *enum_member;
    int retval;

    LOG((2, "nc_inq_enum_member: ncid 0x%x typeid %d", ncid, typeid1));

    /* Find group metadata. */
    if ((retval = nc4_find_nc4_grp(ncid, &grp)))
        return retval;

    /* Find this type. */
    if (!(type = nclistget(grp->nc4_info->alltypes, (size_t)typeid1)))
        return NC_EBADTYPE;

    /* Complain if they are confused about the type. */
    if (type->nc_type_class != NC_ENUM)
        return NC_EBADTYPE;

    /* Move to the desired enum member in the list. */
    if (!(enum_member = nclistget(type->u.e.enum_member, (size_t)idx)))
        return NC_EINVAL;

    /* Give the people what they want. */
    if (identifier)
        strcpy(identifier, enum_member->name);
    if (value)
        memcpy(value, enum_member->value, type->size);

    return NC_NOERR;
}

/**
 * @internal Get the id of a type from the name.
 *
 * @param ncid File and group ID.
 * @param name Name of type; might be fully qualified.
 * @param typeidp Pointer that will get the type ID.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_ENOMEM Out of memory.
 * @return ::NC_EINVAL Bad size.
 * @return ::NC_ENOTNC4 User types in netCDF-4 files only.
 * @return ::NC_EBADTYPE Type not found.
 * @author Ed Hartnett
 */
EXTERNL int
NC4_inq_typeid(int ncid, const char *name, nc_type *typeidp)
{
    NC_GRP_INFO_T *grp;
    NC_GRP_INFO_T *grptwo;
    NC_FILE_INFO_T *h5;
    NC_TYPE_INFO_T *type = NULL;
    char *norm_name = NULL;
    int i, retval = NC_NOERR;

    /* Handle atomic types. */
    for (i = 0; i < NUM_ATOMIC_TYPES; i++)
        if (!strcmp(name, nc4_atomic_name[i]))
        {
            if (typeidp)
                *typeidp = i;
            goto done;
        }

    /* Find info for this file and group, and set pointer to each. */
    if ((retval = nc4_find_grp_h5(ncid, &grp, &h5)))
        goto done;
    assert(h5 && grp);

    /* If the first char is a /, this is a fully-qualified
     * name. Otherwise, this had better be a local name (i.e. no / in
     * the middle). */
    if (name[0] != '/' && strstr(name, "/"))
        {retval = NC_EINVAL; goto done;}

    /* Normalize name. */
    if (!(norm_name = (char*)malloc(strlen(name) + 1)))
        {retval = NC_ENOMEM; goto done;}
    if ((retval = nc4_normalize_name(name, norm_name)))
	goto done;

    /* If this is a fqn, then walk the sequence of parent groups to the last group
       and see if that group has a type of the right name */
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
	/* See if type exists in this group */
        type = (NC_TYPE_INFO_T*)ncindexlookup(grp->type,lastname);
	if(type == NULL) 	
	    {retval = NC_EBADTYPE; goto done;}
	goto done;
    }

    /* Is the type in this group? If not, search parents. */
    for (grptwo = grp; grptwo; grptwo = grptwo->parent) {
        type = (NC_TYPE_INFO_T*)ncindexlookup(grptwo->type,norm_name);
        if(type)
        {
            if (typeidp)
                *typeidp = type->hdr.id;
            break;
        }
    }

    /* Still didn't find type? Search file recursively, starting at the
     * root group. */
    if (!type)
        if ((type = nc4_rec_find_named_type(grp->nc4_info->root_grp, norm_name)))
            if (typeidp)
                *typeidp = type->hdr.id;

    /* OK, I give up already! */
    if (!type)
        {retval = NC_EBADTYPE; goto done;}

done:
    nullfree(norm_name);
    return retval;
}

/**
 * @internal Get the class of a type
 *
 * @param h5 Pointer to the HDF5 file info struct.
 * @param xtype NetCDF type ID.
 * @param type_class Pointer that gets class of type, NC_INT,
 * NC_FLOAT, NC_CHAR, or NC_STRING, NC_ENUM, NC_VLEN, NC_COMPOUND, or
 * NC_OPAQUE.
 *
 * @return ::NC_NOERR No error.
 * @author Ed Hartnett
 */
int
nc4_get_typeclass(const NC_FILE_INFO_T *h5, nc_type xtype, int *type_class)
{
    int retval = NC_NOERR;

    LOG((4, "%s xtype: %d", __func__, xtype));
    assert(type_class);

    /* If this is an atomic type, the answer is easy. */
    if (xtype <= NC_STRING)
    {
        switch (xtype)
        {
        case NC_BYTE:
        case NC_UBYTE:
        case NC_SHORT:
        case NC_USHORT:
        case NC_INT:
        case NC_UINT:
        case NC_INT64:
        case NC_UINT64:
            /* NC_INT is class used for all integral types */
            *type_class = NC_INT;
            break;

        case NC_FLOAT:
        case NC_DOUBLE:
            /* NC_FLOAT is class used for all floating-point types */
            *type_class = NC_FLOAT;
            break;

        case NC_CHAR:
            *type_class = NC_CHAR;
            break;

        case NC_STRING:
            *type_class = NC_STRING;
            break;

        default:
            BAIL(NC_EBADTYPE);
        }
    }
    else
    {
        NC_TYPE_INFO_T *type;

        /* See if it's a used-defined type */
        if ((retval = nc4_find_type(h5, xtype, &type)))
            BAIL(retval);
        if (!type)
            BAIL(NC_EBADTYPE);

        *type_class = type->nc_type_class;
    }

exit:
    return retval;
}

/**
 * @internal return 1 if type is fixed size; 0 otherwise.
 *
 * @param ncid file id
 * @param xtype type id
 * @param fixedsizep pointer into which 1/0 is stored
 *
 * @return ::NC_NOERR
 * @return ::NC_EBADTYPE if bad type
 * @author Dennis Heimbigner
 */
int
NC4_inq_type_fixed_size(int ncid, nc_type xtype, int* fixedsizep)
{
    int stat = NC_NOERR;   
    int f = 0;
    NC_FILE_INFO_T* h5 = NULL;
    NC_TYPE_INFO_T* typ = NULL;

    if(xtype < NC_STRING) {f = 1; goto done;}
    if(xtype == NC_STRING) {f = 0; goto done;}

#ifdef USE_NETCDF4
    /* Must be user type */
    if ((stat = nc4_find_grp_h5(ncid, NULL, &h5)))
        goto done;
    if((stat = nc4_find_type(h5,xtype,&typ))) goto done;
    f = !typ->varsized;
#endif
done:
    if(fixedsizep) *fixedsizep = f;
    return stat;
}


/**
For types with one or more subtypes (e.g. basetype or
fieldtype), determine the varsizedness of the type based on the
basetype.  The idea is to inform the code of the fact that
parenttype has addedtype "inserted" into it.
@param parenttype
@param subtype
*/

int
NC4_recheck_varsize(NC_TYPE_INFO_T* parenttype, nc_type subtype)
{
    int stat = NC_NOERR;
    NC_FILE_INFO_T* file = NULL;
    NC_TYPE_INFO_T* utype = NULL;
    if(subtype < NC_STRING) goto done; /* will not change the "variable-sizedness" of parenttype */
    if(subtype == NC_STRING) {parenttype->varsized = 1; goto done;}
    /* Get the inferred user-type */
    file = parenttype->container->nc4_info;
    if((stat = nc4_find_type(file,subtype,&utype))) goto done;
    switch (utype->nc_type_class) {
    case NC_OPAQUE: case NC_ENUM: break; /* no change */
    case NC_VLEN: parenttype->varsized = 1; break;
    case NC_COMPOUND: if(utype->varsized) parenttype->varsized = 1; break;
    }
    
done:
    return stat;
}

/**
When creating a type, mark it as variable-sized if known for sure.
@param typ
*/

int
NC4_set_varsize(NC_TYPE_INFO_T* typ)
{
    int stat = NC_NOERR;
    if(typ->hdr.id < NC_STRING) goto done; /* will not change the "variable-sizedness" of typ */
    if(typ->hdr.id == NC_STRING) {typ->varsized = 1; goto done;}
    switch (typ->nc_type_class) {
    case NC_OPAQUE: case NC_ENUM: break; /* no change */
    case NC_VLEN: typ->varsized = 1; break;
    case NC_COMPOUND: typ->varsized = 0; break; /* until proven otherwise */
    }
done:
    return stat;
}

/**
 * Test if a variable's type is fixed sized or not.
 * @param var - to test
 * @return 0 if fixed size, 1 otherwise.
 */
int
NC4_var_varsized(NC_VAR_INFO_T* var)
{
    NC_TYPE_INFO_T* vtype = NULL;
    
    /* Check the variable type */
    vtype = var->type_info;
    if(vtype->hdr.id < NC_STRING) return 0;
    if(vtype->hdr.id == NC_STRING) return 1;
    return vtype->varsized;
}

