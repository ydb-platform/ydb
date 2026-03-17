/* Copyright 2003-2018, University Corporation for Atmospheric
 * Research. See COPYRIGHT file for copying and redistribution
 * conditions. */
/**
 * @file
 * @internal This file handles HDF5 attributes.
 *
 * @author Ed Hartnett
 */

#include "config.h"
#include "hdf5internal.h"

/**
 * @internal Get the attribute list for either a varid or NC_GLOBAL
 *
 * @param grp Group
 * @param varid Variable ID | NC_BLOGAL
 * @param varp Pointer that gets pointer to NC_VAR_INFO_T
 * instance. Ignored if NULL.
 * @param attlist Pointer that gets pointer to attribute list.
 *
 * @return NC_NOERR No error.
 * @author Dennis Heimbigner, Ed Hartnett
 * [Candidate for moving to libsrc4]
 */
static int
getattlist(NC_GRP_INFO_T *grp, int varid, NC_VAR_INFO_T **varp,
           NCindex **attlist)
{
    int retval;

    assert(grp && attlist);

    if (varid == NC_GLOBAL)
    {
        /* Do we need to read the atts? */
        if (!grp->atts_read)
            if ((retval = nc4_read_atts(grp, NULL)))
                return retval;

        if (varp)
            *varp = NULL;
        *attlist = grp->att;
    }
    else
    {
        NC_VAR_INFO_T *var;

        if (!(var = (NC_VAR_INFO_T *)ncindexith(grp->vars, (size_t)varid)))
            return NC_ENOTVAR;
        assert(var->hdr.id == varid);

        /* Do we need to read the atts? */
        if (!var->atts_read)
            if ((retval = nc4_read_atts(grp, var)))
                return retval;

        if (varp)
            *varp = var;
        *attlist = var->att;
    }
    return NC_NOERR;
}

/**
 * @internal Get one of three special attributes, NCPROPS,
 * ISNETCDF4ATT, and SUPERBLOCKATT. These atts are not all really in
 * the file, they are constructed on the fly.
 *
 * @param h5 Pointer to HDF5 file info struct.
 * @param name Name of attribute.
 * @param filetypep Pointer that gets type of the attribute data in
 * file.
 * @param mem_type Type of attribute data in memory.
 * @param lenp Pointer that gets length of attribute array.
 * @param attnump Pointer that gets the attribute number.
 * @param data Attribute data.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ERANGE Data conversion out of range.
 * @author Dennis Heimbigner
 */
int
nc4_get_att_special(NC_FILE_INFO_T* h5, const char* name,
                    nc_type* filetypep, nc_type mem_type, size_t* lenp,
                    int* attnump, void* data)
{
    /* Fail if asking for att id */
    if(attnump)
        return NC_EATTMETA;

    if(strcmp(name,NCPROPS)==0) {
        if(h5->provenance.ncproperties == NULL)
            return NC_ENOTATT;
        if(mem_type == NC_NAT) mem_type = NC_CHAR;
        if(mem_type != NC_CHAR)
            return NC_ECHAR;
        if(filetypep) *filetypep = NC_CHAR;
        size_t len = strlen(h5->provenance.ncproperties);
        if(lenp) *lenp = len;
        if(data) strncpy((char*)data,h5->provenance.ncproperties,len+1);
    } else if(strcmp(name,ISNETCDF4ATT)==0
              || strcmp(name,SUPERBLOCKATT)==0) {
        unsigned long long iv = 0;
        if(filetypep) *filetypep = NC_INT;
        if(lenp) *lenp = 1;
        if(strcmp(name,SUPERBLOCKATT)==0)
            iv = (unsigned long long)h5->provenance.superblockversion;
        else /* strcmp(name,ISNETCDF4ATT)==0 */
            iv = (unsigned long long)NC4_isnetcdf4(h5);
        if(mem_type == NC_NAT) mem_type = NC_INT;
        if(data)
            switch (mem_type) {
            case NC_BYTE: *((char*)data) = (char)iv; break;
            case NC_SHORT: *((short*)data) = (short)iv; break;
            case NC_INT: *((int*)data) = (int)iv; break;
            case NC_UBYTE: *((unsigned char*)data) = (unsigned char)iv; break;
            case NC_USHORT: *((unsigned short*)data) = (unsigned short)iv; break;
            case NC_UINT: *((unsigned int*)data) = (unsigned int)iv; break;
            case NC_INT64: *((long long*)data) = (long long)iv; break;
            case NC_UINT64: *((unsigned long long*)data) = (unsigned long long)iv; break;
            default:
                return NC_ERANGE;
            }
    }
    return NC_NOERR;
}

/**
 * @internal I think all atts should be named the exact same thing, to
 * avoid confusion!
 *
 * @param ncid File and group ID.
 * @param varid Variable ID.
 * @param name Name of attribute.
 * @param newname New name for attribute.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_EMAXNAME New name too long.
 * @return ::NC_EPERM File is read-only.
 * @return ::NC_ENAMEINUSE New name already in use.
 * @return ::NC_ENOTINDEFINE Classic model file not in define mode.
 * @return ::NC_EHDFERR HDF error.
 * @return ::NC_ENOMEM Out of memory.
 * @return ::NC_EINTERNAL Could not rebuild list.
 * @author Ed Hartnett
 */
int
NC4_HDF5_rename_att(int ncid, int varid, const char *name, const char *newname)
{
    NC_GRP_INFO_T *grp;
    NC_FILE_INFO_T *h5;
    NC_VAR_INFO_T *var = NULL;
    NC_ATT_INFO_T *att;
    NCindex *list;
    char norm_newname[NC_MAX_NAME + 1], norm_name[NC_MAX_NAME + 1];
    hid_t datasetid = 0;
    int retval = NC_NOERR;

    if (!name || !newname)
        return NC_EINVAL;

    LOG((2, "nc_rename_att: ncid 0x%x varid %d name %s newname %s",
         ncid, varid, name, newname));

    /* If the new name is too long, that's an error. */
    if (strlen(newname) > NC_MAX_NAME)
        return NC_EMAXNAME;

    /* Find info for this file, group, and h5 info. */
    if ((retval = nc4_find_grp_h5(ncid, &grp, &h5)))
        return retval;
    assert(h5 && grp);

    /* If the file is read-only, return an error. */
    if (h5->no_write)
        return NC_EPERM;

    /* Check and normalize the name. */
    if ((retval = nc4_check_name(newname, norm_newname)))
        return retval;

    /* Get the list of attributes. */
    if ((retval = getattlist(grp, varid, &var, &list)))
        return retval;

    /* Is new name in use? */
    att = (NC_ATT_INFO_T*)ncindexlookup(list,norm_newname);
    if(att != NULL)
        return NC_ENAMEINUSE;

    /* Normalize name and find the attribute. */
    if ((retval = nc4_normalize_name(name, norm_name)))
        return retval;

    att = (NC_ATT_INFO_T*)ncindexlookup(list,norm_name);
    if (!att)
        return NC_ENOTATT;

    /* If we're not in define mode, new name must be of equal or
       less size, if complying with strict NC3 rules. */
    if (!(h5->flags & NC_INDEF) && strlen(norm_newname) > strlen(att->hdr.name) &&
        (h5->cmode & NC_CLASSIC_MODEL))
        return NC_ENOTINDEFINE;

    /* Delete the original attribute, if it exists in the HDF5 file. */
    if (att->created)
    {
        if (varid == NC_GLOBAL)
        {
            if (H5Adelete(((NC_HDF5_GRP_INFO_T *)(grp->format_grp_info))->hdf_grpid,
                          att->hdr.name) < 0)
                return NC_EHDFERR;
        }
        else
        {
            if ((retval = nc4_open_var_grp2(grp, varid, &datasetid)))
                return retval;
            if (H5Adelete(datasetid, att->hdr.name) < 0)
                return NC_EHDFERR;
        }
        att->created = NC_FALSE;
    }

    /* Copy the new name into our metadata. */
    if(att->hdr.name) free(att->hdr.name);
    if (!(att->hdr.name = strdup(norm_newname)))
        return NC_ENOMEM;

    att->dirty = NC_TRUE;

    /* Rehash the attribute list so that the new name is used */
    if(!ncindexrebuild(list))
        return NC_EINTERNAL;

    /* Mark attributes on variable dirty, so they get written */
    if(var)
        var->attr_dirty = NC_TRUE;

    return retval;
}

/**
 * @internal Delete an att. Rub it out. Push the button on
 * it. Liquidate it. Bump it off. Take it for a one-way
 * ride. Terminate it.
 *
 * @param ncid File and group ID.
 * @param varid Variable ID.
 * @param name Name of attribute to delete.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOTATT Attribute not found.
 * @return ::NC_EINVAL No name provided.
 * @return ::NC_EPERM File is read only.
 * @return ::NC_ENOTINDEFINE Classic model not in define mode.
 * @return ::NC_EINTERNAL Could not rebuild list.
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
NC4_HDF5_del_att(int ncid, int varid, const char *name)
{
    NC_GRP_INFO_T *grp;
    NC_VAR_INFO_T *var;
    NC_FILE_INFO_T *h5;
    NC_ATT_INFO_T *att;
    NCindex* attlist = NULL;
    hid_t locid = 0;
    int retval;

    /* Name must be provided. */
    if (!name)
        return NC_EINVAL;

    LOG((2, "nc_del_att: ncid 0x%x varid %d name %s", ncid, varid, name));

    /* Find info for this file, group, and h5 info. */
    if ((retval = nc4_find_grp_h5(ncid, &grp, &h5)))
        return retval;
    assert(h5 && grp);

    /* If the file is read-only, return an error. */
    if (h5->no_write)
        return NC_EPERM;

    /* If file is not in define mode, return error for classic model
     * files, otherwise switch to define mode. */
    if (!(h5->flags & NC_INDEF))
    {
        if (h5->cmode & NC_CLASSIC_MODEL)
            return NC_ENOTINDEFINE;
        if ((retval = NC4_redef(ncid)))
            return retval;
    }

    /* Get either the global or a variable attribute list. */
    if ((retval = getattlist(grp, varid, &var, &attlist)))
        return retval;

    /* Determine the location id in the HDF5 file. */
    if (varid == NC_GLOBAL)
        locid = ((NC_HDF5_GRP_INFO_T *)(grp->format_grp_info))->hdf_grpid;
    else if (var->created)
        locid = ((NC_HDF5_VAR_INFO_T *)(var->format_var_info))->hdf_datasetid;

    /* Now find the attribute by name. */
    if (!(att = (NC_ATT_INFO_T*)ncindexlookup(attlist, name)))
        return NC_ENOTATT;

    /* Reclaim the content of the attribute */
    if(att->data) 
	if((retval = NC_reclaim_data_all(h5->controller,att->nc_typeid,att->data,att->len))) return retval;
    att->data = NULL;
    att->len = 0;

    /* Delete it from the HDF5 file, if it's been created. */
    if (att->created)
    {
        assert(locid);
        if (H5Adelete(locid, att->hdr.name) < 0)
            return NC_EATTMETA;
    }

    int deletedid = att->hdr.id;

    /* reclaim associated HDF5 info */
    if((retval=nc4_HDF5_close_att(att))) return retval;

    /* Remove this attribute in this list */
    if ((retval = nc4_att_list_del(attlist, att)))
        return retval;

    /* Renumber all attributes with higher indices. */
    for (size_t i = 0; i < ncindexsize(attlist); i++)
    {
        NC_ATT_INFO_T *a;
        if (!(a = (NC_ATT_INFO_T *)ncindexith(attlist, i)))
            continue;
        if (a->hdr.id > deletedid)
            a->hdr.id--;
    }

    /* Rebuild the index. */
    if (!ncindexrebuild(attlist))
        return NC_EINTERNAL;

    return NC_NOERR;
}

/**
 * @internal This will return the length of a netcdf atomic data type
 * in bytes.
 *
 * @param type A netcdf atomic type.
 *
 * @return Type size in bytes, or -1 if type not found.
 * @author Ed Hartnett
 */
static int
nc4typelen(nc_type type)
{
    switch(type){
    case NC_BYTE:
    case NC_CHAR:
    case NC_UBYTE:
        return 1;
    case NC_USHORT:
    case NC_SHORT:
        return 2;
    case NC_FLOAT:
    case NC_INT:
    case NC_UINT:
        return 4;
    case NC_DOUBLE:
    case NC_INT64:
    case NC_UINT64:
        return 8;
    }
    return -1;
}

/**
 * @internal
 * Write an attribute to a netCDF-4/HDF5 file, converting
 * data type if necessary.
 *
 * @param ncid File and group ID.
 * @param varid Variable ID.
 * @param name Name of attribute.
 * @param file_type Type of the attribute data in file.
 * @param len Number of elements in attribute array.
 * @param data Attribute data.
 * @param mem_type Type of data in memory.
 * @param force write even if the attribute is special
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EINVAL Invalid parameters.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOTVAR Variable not found.
 * @return ::NC_EBADNAME Name contains illegal characters.
 * @return ::NC_ENAMEINUSE Name already in use.
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
nc4_put_att(NC_GRP_INFO_T* grp, int varid, const char *name, nc_type file_type,
            size_t len, const void *data, nc_type mem_type, int force)
{
    NC* nc;
    NC_FILE_INFO_T *h5;
    NC_VAR_INFO_T *var = NULL;
    NCindex* attlist = NULL;
    NC_ATT_INFO_T* att;
    char norm_name[NC_MAX_NAME + 1];
    nc_bool_t new_att = NC_FALSE;
    int retval = NC_NOERR, range_error = 0;
    size_t type_size;
    int ret;
    int ncid;
    void* copy = NULL;
    /* Save the old att data and length and old fillvalue in case we need to rollback on error */
    struct Save {
	size_t len;
	void* data;
        nc_type type; /* In case we change the type of the attribute */
    } attsave = {0,NULL,-1};
    struct Save fillsave = {0,NULL,-1};

    h5 = grp->nc4_info;
    nc = h5->controller;
    assert(nc && grp && h5);

    ncid = nc->ext_ncid | grp->hdr.id;

    /* Find att, if it exists. (Must check varid first or nc_test will
     * break.) This also does lazy att reads if needed. */
    if ((ret = getattlist(grp, varid, &var, &attlist)))
        return ret;

    /* The length needs to be positive (cast needed for braindead
       systems with signed size_t). */
    if((unsigned long) len > X_INT_MAX)
        return NC_EINVAL;

    /* Check name before LOG statement. */
    if (!name || strlen(name) > NC_MAX_NAME)
        return NC_EBADNAME;

    LOG((1, "%s: ncid 0x%x varid %d name %s file_type %d mem_type %d len %d",
         __func__,ncid, varid, name, file_type, mem_type, len));

    /* If len is not zero, then there must be some data. */
    if (len && !data)
        return NC_EINVAL;

    /* If the file is read-only, return an error. */
    if (h5->no_write)
        return NC_EPERM;

    /* Check and normalize the name. */
    if ((retval = nc4_check_name(name, norm_name)))
        return retval;

    /* Check that a reserved att name is not being used improperly */
    const NC_reservedatt* ra = NC_findreserved(name);
    if(ra != NULL && !force) {
        /* case 1: grp=root, varid==NC_GLOBAL, flags & READONLYFLAG */
        if (nc->ext_ncid == ncid && varid == NC_GLOBAL && grp->parent == NULL
            && (ra->flags & READONLYFLAG))
            return NC_ENAMEINUSE;
        /* case 2: grp=NA, varid!=NC_GLOBAL, flags & HIDDENATTRFLAG */
        if (varid != NC_GLOBAL && (ra->flags & (HIDDENATTRFLAG|READONLYFLAG)))
            return NC_ENAMEINUSE;
    }

    /* See if there is already an attribute with this name. */
    att = (NC_ATT_INFO_T*)ncindexlookup(attlist,norm_name);

    if (!att)
    {
        /* If this is a new att, require define mode. */
        if (!(h5->flags & NC_INDEF))
        {
            if (h5->cmode & NC_CLASSIC_MODEL)
                return NC_ENOTINDEFINE;
            if ((retval = NC4_redef(ncid)))
                BAIL(retval);
        }
        new_att = NC_TRUE;
    }
    else
    {
        /* For an existing att, if we're not in define mode, the len
           must not be greater than the existing len for classic model. */
        if (!(h5->flags & NC_INDEF) &&
            len * nc4typelen(file_type) > (size_t)att->len * nc4typelen(att->nc_typeid))
        {
            if (h5->cmode & NC_CLASSIC_MODEL)
                return NC_ENOTINDEFINE;
            if ((retval = NC4_redef(ncid)))
                BAIL(retval);
        }
    }

    /* We must have two valid types to continue. */
    if (file_type == NC_NAT || mem_type == NC_NAT)
        return NC_EBADTYPE;

    /* No character conversions are allowed. */
    if (file_type != mem_type &&
        (file_type == NC_CHAR || mem_type == NC_CHAR ||
         file_type == NC_STRING || mem_type == NC_STRING))
        return NC_ECHAR;

    /* For classic mode file, only allow atts with classic types to be
     * created. */
    if (h5->cmode & NC_CLASSIC_MODEL && file_type > NC_DOUBLE)
        return NC_ESTRICTNC3;

    /* Add to the end of the attribute list, if this att doesn't
       already exist. */
    if (new_att)
    {
        LOG((3, "adding attribute %s to the list...", norm_name));
        if ((ret = nc4_att_list_add(attlist, norm_name, &att)))
            BAIL(ret);

        /* Allocate storage for the HDF5 specific att info. */
        if (!(att->format_att_info = calloc(1, sizeof(NC_HDF5_ATT_INFO_T))))
            BAIL(NC_ENOMEM);

	if(varid == NC_GLOBAL)
	    att->container = (NC_OBJ*)grp;
	else
	    att->container = (NC_OBJ*)var;
    }

    /* Now fill in the metadata. */
    att->dirty = NC_TRUE;

    /* When we reclaim existing data, make sure to use the right type */ 
    if(new_att) attsave.type = file_type; else attsave.type = att->nc_typeid;
    att->nc_typeid = file_type;

    /* Get information about this (possibly new) type. */
    if ((retval = nc4_get_typelen_mem(h5, file_type, &type_size)))
        return retval;

    if (att->data)
    {
	assert(attsave.data == NULL);
	attsave.data = att->data;
	attsave.len = att->len;
        att->data = NULL;
    }

    /* If this is the _FillValue attribute, then we will also have to
     * copy the value to the fill_vlue pointer of the NC_VAR_INFO_T
     * struct for this var. (But ignore a global _FillValue
     * attribute).
     * Since fill mismatch is no longer required, we need to convert the
     * att's type to the vars's type as part of storing.
     */
    if (!strcmp(att->hdr.name, NC_FillValue) && varid != NC_GLOBAL)
    {
        /* Fill value must have exactly one value */
        if (len != 1)
            BAIL(NC_EINVAL);

        /* If we already wrote to the dataset, then return an error. */
        if (var->written_to)
           BAIL(NC_ELATEFILL);

        /* Get the length of the variable's data type. */
        if ((retval = nc4_get_typelen_mem(grp->nc4_info, var->type_info->hdr.id,
                                          &type_size)))
            BAIL(retval);

        /* Fill value must be same type and have exactly one value */
        if (att->nc_typeid != var->type_info->hdr.id)
            return NC_EBADTYPE;

        /* Already set a fill value? Now I'll have to free the old
         * one. Make up your damn mind, would you? */
        if (var->fill_value)
        {
	    /* reclaim later */
	    fillsave.data = var->fill_value;
	    fillsave.type = var->type_info->hdr.id;
	    fillsave.len = 1;
	    var->fill_value = NULL;
        }

        /* Determine the size of the fill value in bytes. */

	{
	    nc_type var_type = var->type_info->hdr.id;
   	    size_t var_type_size = var->type_info->size;
	    /* The old code used the var's type as opposed to the att's type; normally same,
	       but not required. Now we need to convert from the att's type to the var's type.
	       Note that we use mem_type rather than file_type because our data is in the form
	       of the memory data. When we later capture the memory data for the actual
	       attribute, we will use file_type as the target of the conversion. */
	    if(mem_type != var_type && mem_type < NC_STRING && var_type < NC_STRING) {
		/* Need to convert from memory data into copy buffer */
		if((copy = malloc(len*var_type_size))==NULL) BAIL(NC_ENOMEM);
                if ((retval = nc4_convert_type(data, copy, mem_type, var_type,
                                               len, &range_error, NULL,
                                               (h5->cmode & NC_CLASSIC_MODEL),
					       NC_NOQUANTIZE, 0)))
                    BAIL(retval);
	    } else { /* no conversion */
		/* Still need a copy of the input data */
		copy = NULL;
	        if((retval = NC_copy_data_all(h5->controller, mem_type, data, 1, &copy)))
		    BAIL(retval);
	    }
	    var->fill_value = copy;
	    copy = NULL;
	}
        /* Indicate that the fill value was changed, if the variable has already
         * been created in the file, so the dataset gets deleted and re-created. */
        if (var->created)
            var->fill_val_changed = NC_TRUE;
    }

    /* Copy the attribute data, if there is any. */
    if (len)
    {
        nc_type type_class;    /* Class of attribute's type */

        /* Get class for this type. */
        if ((retval = nc4_get_typeclass(h5, file_type, &type_class)))
            BAIL(retval);

        assert(data);
        {
	    /* Allocate top level of the copy */
	    if (!(copy = malloc(len * type_size)))
                BAIL(NC_ENOMEM);
	    /* Special case conversion from memory to file type */
	    if(mem_type != file_type && mem_type < NC_STRING && file_type < NC_STRING) {
                if ((retval = nc4_convert_type(data, copy, mem_type, file_type,
                                               len, &range_error, NULL,
                                               (h5->cmode & NC_CLASSIC_MODEL),
					       NC_NOQUANTIZE, 0)))
                    BAIL(retval);
	    } else if(mem_type == file_type) { /* General case: no conversion */
	        if((retval = NC_copy_data(h5->controller,file_type,data,len,copy)))
		    BAIL(retval);
	    } else
	    	BAIL(NC_EURL);
	    /* Store it */
	    att->data = copy; copy = NULL;
	}
    }
    att->dirty = NC_TRUE;
    att->created = NC_FALSE;
    att->len = len;

    /* Mark attributes on variable dirty, so they get written */
    if(var)
        var->attr_dirty = NC_TRUE;
    /* Reclaim saved data */
    if(attsave.data != NULL) {
        assert(attsave.len > 0);
        (void)NC_reclaim_data_all(h5->controller,attsave.type,attsave.data,attsave.len);
	attsave.len = 0; attsave.data = NULL;
    }
    if(fillsave.data != NULL) {
        assert(fillsave.len > 0);
        (void)NC_reclaim_data_all(h5->controller,fillsave.type,fillsave.data,fillsave.len);
	fillsave.len = 0; fillsave.data = NULL;
    }

exit:
    if(copy)
        (void)NC_reclaim_data_all(h5->controller,file_type,copy,len);
    if(retval) {
	/* Rollback */
        if(attsave.data != NULL) {
            assert(attsave.len > 0);
	    if(att->data)
                (void)NC_reclaim_data_all(h5->controller,attsave.type,att->data,att->len);
	    att->len = attsave.len; att->data = attsave.data;
        }
        if(fillsave.data != NULL) {
            assert(fillsave.len > 0);
	    if(att->data)
                (void)NC_reclaim_data_all(h5->controller,fillsave.type,var->fill_value,1);
	    var->fill_value = fillsave.data;
        }
    }    
    /* If there was an error return it, otherwise return any potential
       range error value. If none, return NC_NOERR as usual.*/
    if (range_error)
        return NC_ERANGE;
    if (retval)
        return retval;
    return NC_NOERR;
}

/**
 * @internal Write an attribute to a netCDF-4/HDF5 file, converting
 * data type if necessary.
 *
 * @param ncid File and group ID.
 * @param varid Variable ID.
 * @param name Name of attribute.
 * @param file_type Type of the attribute data in file.
 * @param len Number of elements in attribute array.
 * @param data Attribute data.
 * @param mem_type Type of data in memory.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EINVAL Invalid parameters.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOTVAR Variable not found.
 * @return ::NC_EBADNAME Name contains illegal characters.
 * @return ::NC_ENAMEINUSE Name already in use.
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
NC4_HDF5_put_att(int ncid, int varid, const char *name, nc_type file_type,
                 size_t len, const void *data, nc_type mem_type)
{
    NC_FILE_INFO_T *h5;
    NC_GRP_INFO_T *grp;
    int ret;

    /* Find info for this file, group, and h5 info. */
    if ((ret = nc4_find_nc_grp_h5(ncid, NULL, &grp, &h5)))
        return ret;
    assert(grp && h5);

    return nc4_put_att(grp, varid, name, file_type, len, data, mem_type, 0);
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
NC4_HDF5_inq_att(int ncid, int varid, const char *name, nc_type *xtypep,
                 size_t *lenp)
{
    NC_FILE_INFO_T *h5;
    NC_GRP_INFO_T *grp;
    NC_VAR_INFO_T *var = NULL;
    char norm_name[NC_MAX_NAME + 1];
    int retval;

    LOG((2, "%s: ncid 0x%x varid %d", __func__, ncid, varid));

    /* Find the file, group, and var info, and do lazy att read if
     * needed. */
    if ((retval = nc4_hdf5_find_grp_var_att(ncid, varid, name, 0, 1, norm_name,
                                            &h5, &grp, &var, NULL)))
        return retval;

    /* If this is one of the reserved atts, use nc_get_att_special. */
    if (!var)
    {
        const NC_reservedatt *ra = NC_findreserved(norm_name);
        if (ra  && ra->flags & NAMEONLYFLAG)
            return nc4_get_att_special(h5, norm_name, xtypep, NC_NAT, lenp, NULL,
                                       NULL);
    }

    return nc4_get_att_ptrs(h5, grp, var, norm_name, xtypep, NC_NAT,
                            lenp, NULL, NULL);
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
NC4_HDF5_inq_attid(int ncid, int varid, const char *name, int *attnump)
{
    NC_FILE_INFO_T *h5;
    NC_GRP_INFO_T *grp;
    NC_VAR_INFO_T *var = NULL;
    char norm_name[NC_MAX_NAME + 1];
    int retval;

    LOG((2, "%s: ncid 0x%x varid %d", __func__, ncid, varid));

    /* Find the file, group, and var info, and do lazy att read if
     * needed. */
    if ((retval = nc4_hdf5_find_grp_var_att(ncid, varid, name, 0, 1, norm_name,
                                            &h5, &grp, &var, NULL)))
        return retval;

    /* If this is one of the reserved atts, use nc_get_att_special. */
    if (!var)
    {
        const NC_reservedatt *ra = NC_findreserved(norm_name);
        if (ra  && ra->flags & NAMEONLYFLAG)
            return nc4_get_att_special(h5, norm_name, NULL, NC_NAT, NULL, attnump,
                                       NULL);
    }

    return nc4_get_att_ptrs(h5, grp, var, norm_name, NULL, NC_NAT,
                            NULL, attnump, NULL);
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
NC4_HDF5_inq_attname(int ncid, int varid, int attnum, char *name)
{
    NC_ATT_INFO_T *att;
    int retval;

    LOG((2, "%s: ncid 0x%x varid %d", __func__, ncid, varid));

    /* Find the file, group, and var info, and do lazy att read if
     * needed. */
    if ((retval = nc4_hdf5_find_grp_var_att(ncid, varid, NULL, attnum, 0, NULL,
                                            NULL, NULL, NULL, &att)))
        return retval;
    assert(att);

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
 * @param memtype The type the data should be converted to as it is
 * read.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @author Ed Hartnett
 */
int
NC4_HDF5_get_att(int ncid, int varid, const char *name, void *value,
                 nc_type memtype)
{
    NC_FILE_INFO_T *h5;
    NC_GRP_INFO_T *grp;
    NC_VAR_INFO_T *var = NULL;
    char norm_name[NC_MAX_NAME + 1];
    int retval;

    LOG((2, "%s: ncid 0x%x varid %d", __func__, ncid, varid));

    /* Find the file, group, and var info, and do lazy att read if
     * needed. */
    if ((retval = nc4_hdf5_find_grp_var_att(ncid, varid, name, 0, 1, norm_name,
                                            &h5, &grp, &var, NULL)))
        return retval;

    /* If this is one of the reserved atts, use nc_get_att_special. */
    if (!var)
    {
        const NC_reservedatt *ra = NC_findreserved(norm_name);
        if (ra  && ra->flags & NAMEONLYFLAG)
            return nc4_get_att_special(h5, norm_name, NULL, NC_NAT, NULL, NULL,
                                       value);
    }

    return nc4_get_att_ptrs(h5, grp, var, norm_name, NULL, memtype,
                            NULL, NULL, value);
}
