/* Copyright 2003-2018, University Corporation for Atmospheric
 * Research. See COPYRIGHT file for copying and redistribution
 * conditions. */

/**
 * @file
 * @internal This file handles ZARR attributes.
 *
 * @author Dennis Heimbigner, Ed Hartnett
 */

#include "zincludes.h"
#include "zfilter.h"

#undef ADEBUG

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
int
ncz_getattlist(NC_GRP_INFO_T *grp, int varid, NC_VAR_INFO_T **varp, NCindex **attlist)
{
    int retval;
    NC_FILE_INFO_T* file = grp->nc4_info;
    NCZ_FILE_INFO_T* zinfo = file->format_file_info;

    assert(grp && attlist && file && zinfo);

    if (varid == NC_GLOBAL)
    {
        /* Do we need to read the atts? */
        if (!grp->atts_read)
            if ((retval = ncz_read_atts(file, (NC_OBJ*)grp)))
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
            if ((retval = ncz_read_atts(file, (NC_OBJ*)var)))
                return retval;

        if (varp)
            *varp = var;
        *attlist = var->att;
    }
    return NC_NOERR;
}

/**
 * @internal Get one of the special attributes:
 * See the reserved attribute table in libsrc4/nc4internal.c.
 * The special attributes are the ones marked with NAMEONLYFLAG.
 * For example: NCPROPS, ISNETCDF4ATT, and SUPERBLOCKATT, and CODECS.
 * These atts are not all really in the file, they are constructed on the fly.
 *
 * @param h5 Pointer to ZARR file info struct.
 * @param var Pointer to var info struct; NULL signals global.
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
ncz_get_att_special(NC_FILE_INFO_T* h5, NC_VAR_INFO_T* var, const char* name,
                    nc_type* filetypep, nc_type mem_type, size_t* lenp,
                    int* attnump, void* data)
{
    int stat = NC_NOERR;
    
    /* Fail if asking for att id */
    if(attnump)
        {stat = NC_EATTMETA; goto done;}

    /* Handle the per-var case(s) first */
    if(var != NULL) {
#ifdef NETCDF_ENABLE_NCZARR_FILTERS
        if(strcmp(name,NC_ATT_CODECS)==0) {	
            NClist* filters = (NClist*)var->filters;

            if(mem_type == NC_NAT) mem_type = NC_CHAR;
            if(mem_type != NC_CHAR)
                {stat = NC_ECHAR; goto done;}
            if(filetypep) *filetypep = NC_CHAR;
	    if(lenp) *lenp = 0;
	    if(filters == NULL) goto done;	  
 	    if((stat = NCZ_codec_attr(var,lenp,data))) goto done;
	}
#endif
	goto done;
    }

    /* The global reserved attributes */
    if(strcmp(name,NCPROPS)==0) {
        size_t len;
        if(h5->provenance.ncproperties == NULL)
            {stat = NC_ENOTATT; goto done;}
        if(mem_type == NC_NAT) mem_type = NC_CHAR;
        if(mem_type != NC_CHAR)
            {stat = NC_ECHAR; goto done;}
        if(filetypep) *filetypep = NC_CHAR;
	len = strlen(h5->provenance.ncproperties);
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
            iv = (unsigned long long)NCZ_isnetcdf4(h5);
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
                {stat = NC_ERANGE; goto done;}
            }
    }
done:
    return stat;

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
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_rename_att(int ncid, int varid, const char *name, const char *newname)
{
    NC_GRP_INFO_T *grp;
    NC_FILE_INFO_T *h5;
    NC_VAR_INFO_T *var = NULL;
    NC_ATT_INFO_T *att;
    NCindex *list;
    char norm_newname[NC_MAX_NAME + 1], norm_name[NC_MAX_NAME + 1];
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
    if ((retval = ncz_getattlist(grp, varid, &var, &list)))
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
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_del_att(int ncid, int varid, const char *name)
{
    NC_GRP_INFO_T *grp;
    NC_VAR_INFO_T *var;
    NC_FILE_INFO_T *h5;
    NC_ATT_INFO_T *att;
    NCindex* attlist = NULL;
    size_t i;
    int deletedid;
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
        if ((retval = NCZ_redef(ncid)))
            return retval;
    }

    /* Get either the global or a variable attribute list. */
    if ((retval = ncz_getattlist(grp, varid, &var, &attlist)))
        return retval;

#ifdef LOOK
    /* Determine the location id in the ZARR file. */
    if (varid == NC_GLOBAL)
        locid = ((NCZ_GRP_INFO_T *)(grp->format_grp_info))->hdf_grpid;
    else if (var->created)
        locid = ((NCZ_VAR_INFO_T *)(var->format_var_info))->hdf_datasetid;
#endif

    /* Now find the attribute by name. */
    if (!(att = (NC_ATT_INFO_T*)ncindexlookup(attlist, name)))
        return NC_ENOTATT;

    /* Reclaim the content of the attribute */
    if(att->data) {
	if((retval = NC_reclaim_data_all(h5->controller,att->nc_typeid,att->data,att->len))) return retval;
    }
    att->data = NULL;
    att->len = 0;

    /* Delete it from the ZARR file, if it's been created. */
    if (att->created)
    {
#ifdef LOOK
        assert(locid);
        if (H5Adelete(locid, att->hdr.name) < 0)
            return NC_EATTMETA;
#endif
    }

    deletedid = att->hdr.id;

    /* reclaim associated NCZarr info */
    {
	NCZ_ATT_INFO_T* za = (NCZ_ATT_INFO_T*)att->format_att_info;
	nullfree(za);
    }

    /* Remove this attribute in this list */
    if ((retval = nc4_att_list_del(attlist, att)))
        return retval;

    /* Renumber all attributes with higher indices. */
    for (i = 0; i < ncindexsize(attlist); i++)
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
 * @author Dennis Heimbigner, Ed Hartnett
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
 * Write an attribute to a netCDF-4/NCZ file, converting
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
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
ncz_put_att(NC_GRP_INFO_T* grp, int varid, const char *name, nc_type file_type,
            size_t len, const void *data, nc_type mem_type, int force)
{
    NC* nc;
    NC_FILE_INFO_T *h5 = NULL;
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
    if ((ret = ncz_getattlist(grp, varid, &var, &attlist)))
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
        if (varid != NC_GLOBAL && (ra->flags & HIDDENATTRFLAG))
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
            if ((retval = NCZ_redef(ncid)))
                BAIL(retval);
        }
        new_att = NC_TRUE;
    }
    else
    {
        /* For an existing att, if we're not in define mode, the len
           must not be greater than the existing len for classic model. */
        if (!(h5->flags & NC_INDEF) &&
            len * (size_t)nc4typelen(file_type) > (size_t)att->len * (size_t)nc4typelen(att->nc_typeid))
        {
            if (h5->cmode & NC_CLASSIC_MODEL)
                return NC_ENOTINDEFINE;
            if ((retval = NCZ_redef(ncid)))
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

        /* Allocate storage for the ZARR specific att info. */
        if (!(att->format_att_info = calloc(1, sizeof(NCZ_ATT_INFO_T))))
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
     * copy the value to the fill_value pointer of the NC_VAR_INFO_T
     * struct for this var. (But ignore a global _FillValue
     * attribute). Also kill the cache fillchunk as no longer valid */
    if (!strcmp(att->hdr.name, NC_FillValue) && varid != NC_GLOBAL)
    {
        /* Fill value must have exactly one value */
        if (len != 1)
            return NC_EINVAL;

        /* If we already wrote to the dataset, then return an error. */
        if (var->written_to)
            return NC_ELATEFILL;

        /* Get the length of the variable data type. */
        if ((retval = nc4_get_typelen_mem(grp->nc4_info, var->type_info->hdr.id,
                                          &type_size)))
            return retval;

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
        /* Reclaim any existing fill_chunk */
        if((retval = NCZ_reclaim_fill_chunk(((NCZ_VAR_INFO_T*)var->format_var_info)->cache))) BAIL(retval);
    }

    /* Copy the attribute data, if there is any. */
    if (len)
    {
        nc_type type_class;    /* Class of attribute's type */

        /* Get class for this type. */
        if ((retval = nc4_get_typeclass(h5, file_type, &type_class)))
            return retval;

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

    /* If this is a maxstrlen attribute, then we will also have to
     * sync the value to NCZ_VAR_INFO_T or NCZ_FILE_INFO_T structure */
    {
	if(strcmp(att->hdr.name,NC_NCZARR_DEFAULT_MAXSTRLEN_ATTR)==0 && varid == NC_GLOBAL && len == 1) {
	    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)h5->format_file_info;
	    if((retval = nc4_convert_type(att->data, &zfile->default_maxstrlen, file_type, NC_INT,
				      len, &range_error, NULL, NC_CLASSIC_MODEL, NC_NOQUANTIZE, 0)))
	        BAIL(retval);
	} else if(strcmp(att->hdr.name,NC_NCZARR_MAXSTRLEN_ATTR)==0 && varid != NC_GLOBAL && len == 1) {
	    NCZ_VAR_INFO_T* zvar = (NCZ_VAR_INFO_T*)var->format_var_info;
	if((retval = nc4_convert_type(att->data, &zvar->maxstrlen, file_type, NC_INT,
				      len, &range_error, NULL, NC_CLASSIC_MODEL, NC_NOQUANTIZE, 0)))
	        BAIL(retval);
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
 * @internal Write an attribute to a netCDF-4/NCZ file, converting
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
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_put_att(int ncid, int varid, const char *name, nc_type file_type,
                 size_t len, const void *data, nc_type mem_type)
{
    NC_FILE_INFO_T *h5;
    NC_GRP_INFO_T *grp;
    int ret;

    /* Find info for this file, group, and h5 info. */
    if ((ret = nc4_find_grp_h5(ncid, &grp, &h5)))
        return ret;
    assert(grp && h5);

    return ncz_put_att(grp, varid, name, file_type, len, data, mem_type, 0);
}

/**
 * @internal Learn about an att. All the nc4 nc_inq_ functions just
 * call ncz_get_att to get the metadata on an attribute.
 *
 * @param ncid File and group ID.
 * @param varid Variable ID.
 * @param name Name of attribute.
 * @param xtypep Pointer that gets type of attribute.
 * @param lenp Pointer that gets length of attribute data array.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_inq_att(int ncid, int varid, const char *name, nc_type *xtypep,
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
    if ((retval = ncz_find_grp_var_att(ncid, varid, name, 0, 1, norm_name,
                                            &h5, &grp, &var, NULL)))
        return retval;

    /* If this is one of the reserved atts, use nc_get_att_special. */
    {
        const NC_reservedatt *ra = NC_findreserved(norm_name);
        if (ra  && ra->flags & NAMEONLYFLAG)
            return ncz_get_att_special(h5, var, norm_name, xtypep, NC_NAT, lenp, NULL,
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
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_inq_attid(int ncid, int varid, const char *name, int *attnump)
{
    NC_FILE_INFO_T *h5;
    NC_GRP_INFO_T *grp;
    NC_VAR_INFO_T *var = NULL;
    char norm_name[NC_MAX_NAME + 1];
    int retval;

    LOG((2, "%s: ncid 0x%x varid %d", __func__, ncid, varid));

    /* Find the file, group, and var info, and do lazy att read if
     * needed. */
    if ((retval = ncz_find_grp_var_att(ncid, varid, name, 0, 1, norm_name,
                                            &h5, &grp, &var, NULL)))
        return retval;

    /* If this is one of the reserved atts, use nc_get_att_special. */
    {
        const NC_reservedatt *ra = NC_findreserved(norm_name);
        if (ra  && ra->flags & NAMEONLYFLAG)
            return ncz_get_att_special(h5, var, norm_name, NULL, NC_NAT, NULL, attnump,
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
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_inq_attname(int ncid, int varid, int attnum, char *name)
{
    NC_ATT_INFO_T *att;
    int retval = NC_NOERR;

    ZTRACE(1,"ncid=%d varid=%d attnum=%d",ncid,varid,attnum);
    LOG((2, "%s: ncid 0x%x varid %d", __func__, ncid, varid));

    /* Find the file, group, and var info, and do lazy att read if
     * needed. */
    if ((retval = ncz_find_grp_var_att(ncid, varid, NULL, attnum, 0, NULL,
                                            NULL, NULL, NULL, &att)))
	goto done;
    assert(att);

    /* Get the name. */
    if (name)
        strcpy(name, att->hdr.name);
done:
    return ZUNTRACEX(retval,"name=%s",(retval?"":name));
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
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_get_att(int ncid, int varid, const char *name, void *value,
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
    if ((retval = ncz_find_grp_var_att(ncid, varid, name, 0, 1, norm_name,
                                            &h5, &grp, &var, NULL)))
        return retval;

    /* If this is one of the reserved global atts, use nc_get_att_special. */
    {
        const NC_reservedatt *ra = NC_findreserved(norm_name);
        if (ra  && ra->flags & NAMEONLYFLAG)
            return ncz_get_att_special(h5, var, norm_name, NULL, NC_NAT, NULL, NULL,
                                       value);
    }

    return nc4_get_att_ptrs(h5, grp, var, norm_name, NULL, memtype,
                            NULL, NULL, value);
}

#if 0
static int
ncz_del_attr(NC_FILE_INFO_T* file, NC_OBJ* container, const char* name)
{
    int i,stat = NC_NOERR;

    ZTRACE();

    if(container->sort == NCGRP)
	stat = ncz_getattlist((NC_GRP_INFO_T*)container,NC_GLOBAL,NULL,&attlist);
    else
	stat = ncz_getattlist((NC_VAR_INFO_T*)container,NC_GLOBAL,NULL,&attlist);

	goto done;

    /* Iterate over the attributes to locate the matching attribute */
    for(i=0;i<nclistlength(jattrs->dict);i+=2) {
	NCjson* key = nclistget(jattrs->dict,i);
	assert(key->sort == NCJ_STRING);
	if(strcmp(key->value,name)==0) {
	    /* Remove and reclaim */
	    NCjson* value = nclistget(jattrs->dict,i+1);
	    nclistremove(jattrs->dict,i);
	    nclistremove(jattrs->dict,i+1);
	    NCJreclaim(key);
	    NCJreclaim(value);
	    break;
	}    
    }
    /* Write the json back out */
    if((stat = ncz_unload_jatts(zinfo, container, jattrs, jtypes)))
	goto done;

done:
    NCJreclaim(jattrs);
    NCJreclaim(jtypes);
    return stat;
}
#endif

/* If we do not have a _FillValue, then go ahead and create it */
int
ncz_create_fillvalue(NC_VAR_INFO_T* var)
{
    int stat = NC_NOERR;
    size_t i;
    NC_ATT_INFO_T* fv = NULL;

    /* Have the var's attributes been read? */
    if(!var->atts_read) goto done; /* above my pay grade */

    /* Is FillValue warranted? */
    if(!var->no_fill && var->fill_value != NULL) {
        /* Make sure _FillValue does not exist */
	for(i=0;i<ncindexsize(var->att);i++) {
	    fv = (NC_ATT_INFO_T*)ncindexith(var->att,i);
	    if(strcmp(fv->hdr.name,NC_FillValue)==0) break;
	    fv = NULL;
        }
	if(fv == NULL) {
	    /* Create it */
	    if((stat = ncz_makeattr((NC_OBJ*)var,var->att,NC_FillValue,var->type_info->hdr.id,1,var->fill_value,&fv)))
	    goto done;
	}
    }
done:
    return THROW(stat);
}

/* Create an attribute; This is an abbreviated form
   of ncz_put_att above */
int
ncz_makeattr(NC_OBJ* container, NCindex* attlist, const char* name, nc_type typeid, size_t len, void* values, NC_ATT_INFO_T** attp)
{
    int stat = NC_NOERR;
    NC_ATT_INFO_T* att = NULL;
    NCZ_ATT_INFO_T* zatt = NULL;
    void* clone = NULL;
    size_t typesize, clonesize;
    NC_GRP_INFO_T* grp = (container->sort == NCGRP ? (NC_GRP_INFO_T*)container
                                                   : ((NC_VAR_INFO_T*)container)->container);

    /* Duplicate the values */
    if ((stat = nc4_get_typelen_mem(grp->nc4_info, typeid, &typesize))) goto done;
    clonesize = len*typesize;
    if((clone = malloc(clonesize))==NULL) {stat = NC_ENOMEM; goto done;}
    if((stat = NC_copy_data(grp->nc4_info->controller, typeid, values, len, clone))) goto done;
    if((stat=nc4_att_list_add(attlist,name,&att)))
	goto done;
    if((zatt = calloc(1,sizeof(NCZ_ATT_INFO_T))) == NULL)
	{stat = NC_ENOMEM; goto done;}
    if(container->sort == NCGRP) {
        zatt->common.file = ((NC_GRP_INFO_T*)container)->nc4_info;
    } else if(container->sort == NCVAR) {
        zatt->common.file = ((NC_VAR_INFO_T*)container)->container->nc4_info;
    } else
	abort();
    att->container = container;
    att->format_att_info = zatt;
    /* Fill in the attribute's type and value  */
    att->nc_typeid = typeid;
    att->len = len;
    att->data = clone; clone = NULL;
    att->dirty = NC_TRUE;
    if(attp) {*attp = att; att = NULL;}

done:
    nullfree(clone);
    if(stat) {
	if(att) nc4_att_list_del(attlist,att);
	nullfree(zatt);
    }
    return THROW(stat);
}

