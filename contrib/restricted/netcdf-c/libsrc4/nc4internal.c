/* Copyright 2003-2018, University Corporation for Atmospheric
 * Research. See the COPYRIGHT file for copying and redistribution
 * conditions.
 */
/**
 * @file
 * @internal
 * Internal netcdf-4 functions.
 *
 * This file contains functions internal to the netcdf4 library. None of
 * the functions in this file are exposed in the exetnal API. These
 * functions all relate to the manipulation of netcdf-4's in-memory
 * buffer of metadata information, i.e. the linked list of NC
 * structs.
 *
 * @author Ed Hartnett, Dennis Heimbigner, Ward Fisher
 */
#include "config.h"
#include <stdarg.h>
#include <stddef.h>
#include "netcdf.h"
#include "netcdf_filter.h"
#include "netcdf_meta.h"
#include "nc4internal.h"
#include "nc.h" /* from libsrc */
#include "ncdispatch.h" /* from libdispatch */
#include "ncutf8.h"
#include "ncrc.h"
#include "nc4internal.h"

/** @internal Number of reserved attributes. These attributes are
 * hidden from the netcdf user, but exist in the implementation
 * datasets to help netcdf read the dataset.
 * Moved here from hdf5file.c.
 * These tables need to capture all reserved attributes
 * across all possible dispatchers
*/

/** @internal List of reserved attributes.
    WARNING: This list will be sorted in (strcmp) sorted order for binary search. 
    So order here does not matter; the table will be modified by sorting.
*/
static NC_reservedatt NC_reserved[] = {
    {NC_ATT_CLASS, READONLYFLAG|HIDDENATTRFLAG},			/*CLASS*/
    {NC_ATT_DIMENSION_LIST, READONLYFLAG|HIDDENATTRFLAG},		/*DIMENSION_LIST*/
    {NC_ATT_NAME, READONLYFLAG|HIDDENATTRFLAG},				/*NAME*/
    {NC_ATT_REFERENCE_LIST, READONLYFLAG|HIDDENATTRFLAG},		/*REFERENCE_LIST*/
    {NC_XARRAY_DIMS, READONLYFLAG|HIDDENATTRFLAG},			/*_ARRAY_DIMENSIONS*/
    {NC_ATT_CODECS, VARFLAG|READONLYFLAG|NAMEONLYFLAG},			/*_Codecs*/
    {NC_ATT_FORMAT, READONLYFLAG},					/*_Format*/
    {ISNETCDF4ATT, READONLYFLAG|NAMEONLYFLAG|VIRTUALFLAG},		/*_IsNetcdf4*/
    {NCPROPS,READONLYFLAG|NAMEONLYFLAG|HIDDENATTRFLAG},			/*_NCProperties*/
    {NC_ATT_COORDINATES, READONLYFLAG|HIDDENATTRFLAG},			/*_Netcdf4Coordinates*/
    {NC_ATT_DIMID_NAME, READONLYFLAG|HIDDENATTRFLAG},			/*_Netcdf4Dimid*/
    {SUPERBLOCKATT, READONLYFLAG|NAMEONLYFLAG|VIRTUALFLAG},		/*_SuperblockVersion*/
    {NC_ATT_NC3_STRICT_NAME, READONLYFLAG},				/*_nc3_strict*/
    {NC_NCZARR_ATTR, READONLYFLAG|HIDDENATTRFLAG},			/*_nczarr_attr */
    {NC_NCZARR_GROUP, READONLYFLAG|HIDDENATTRFLAG},			/*_nczarr_group */
    {NC_NCZARR_ARRAY, READONLYFLAG|HIDDENATTRFLAG},			/*_nczarr_array */
    {NC_NCZARR_SUPERBLOCK, READONLYFLAG|HIDDENATTRFLAG},		/*_nczarr_superblock */
};
#define NRESERVED (sizeof(NC_reserved) / sizeof(NC_reservedatt))  /*|NC_reservedatt*/

/*Forward */
static int NC4_move_in_NCList(NC* nc, int new_id);
static int bincmp(const void* arg1, const void* arg2);
static int sortcmp(const void* arg1, const void* arg2);

#if LOGGING
/* This is the severity level of messages which will be logged. Use
   severity 0 for errors, 1 for important log messages, 2 for less
   important, etc. */
int nc_log_level = NC_TURN_OFF_LOGGING;


#if NC_HAS_PARALLEL4
/* File pointer for the parallel I/O log file. */
FILE *LOG_FILE = NULL;
#endif /* NC_HAS_PARALLEL4 */

/* This function prints out a message, if the severity of
 * the message is lower than the global nc_log_level. To use it, do
 * something like this:
 *
 * nc_log(0, "this computer will explode in %d seconds", i);
 *
 * After the first arg (the severity), use the rest like a normal
 * printf statement. Output will appear on stderr for sequential
 * builds, and in a file nc4_log_R.log for each process for a parallel
 * build, where R is the rank of the process.
 *
 * Ed Hartnett
 */
void
nc_log(int severity, const char *fmt, ...)
{
    va_list argp;
    int t;
    FILE *f = stderr;

    const char* envv = NULL;
    envv = getenv("NC4LOGGING");
    if(envv != NULL) {
        nc_log_level = atoi(envv);
    }

    /* If the severity is greater than the log level, we don't print
     * this message. */
    if (severity > nc_log_level)
        return;

#if NC_HAS_PARALLEL4
    /* For parallel I/O build, if MPI has been initialized, instead of
     * printing logging output to stderr, it goes to a file for each
     * process. */
    {
        int mpi_initialized;
        int mpierr;

        /* Check to see if MPI has been initialized. */
        if ((mpierr = MPI_Initialized(&mpi_initialized)))
            return;

        /* If MPI has been initialized use a log file. */
        assert(LOG_FILE);
        if (mpi_initialized)
            f = LOG_FILE;
    }
#endif /* NC_HAS_PARALLEL4 */

    /* If the severity is zero, this is an error. Otherwise insert that
       many tabs before the message. */
    if (!severity)
        fprintf(f, "ERROR: ");
    for (t = 0; t < severity; t++)
        fprintf(f, "\t");

    /* Print out the variable list of args with vprintf. */
    va_start(argp, fmt);
    vfprintf(f, fmt, argp);
    va_end(argp);

    /* Put on a final linefeed. */
    fprintf(f, "\n");
    fflush(f);
}
#endif /* LOGGING */

/**
 * @internal Check and normalize and name.
 *
 * @param name Name to normalize.
 * @param norm_name The normalized name.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EMAXNAME Name too long.
 * @return ::NC_EINVAL NULL given for name.
 * @return ::NC_ENOMEM Out of memory.
 * @author Dennis Heimbigner
 */
int
nc4_check_name(const char *name, char *norm_name)
{
    char *temp;
    int retval;

    assert(norm_name);

    /* Check for NULL. */
    if (!name)
        return NC_EINVAL;

    /* Make sure this is a valid netcdf name. This should be done
     * before the name is normalized, because it gives better error
     * codes for bad utf8 strings. */
    if ((retval = NC_check_name(name)))
        return retval;

    /* Normalize the name. */
    if ((retval = nc_utf8_normalize((const unsigned char *)name,
                                    (unsigned char **)&temp)))
        return retval;

    /* Check length of normalized name. */
    if (strlen(temp) > NC_MAX_NAME)
    {
        free(temp);
        return NC_EMAXNAME;
    }

    /* Copy the normalized name. */
    strcpy(norm_name, temp);
    free(temp);

    return NC_NOERR;
}

/**
 * @internal Add a file to the list of libsrc4 open files. This is
 * used by dispatch layers that wish to use the libsrc4 metadata
 * model, but don't know about struct NC. This is the same as
 * nc4_nc4f_list_add(), except it takes an ncid instead of an NC *,
 * and also passes back the dispatchdata pointer.
 *
 * @param ncid The (already-assigned) ncid of the file (aka ext_ncid).
 * @param path The file name of the new file.
 * @param mode The mode flag.
 * @param dispatchdata Void * that gets pointer to dispatch data,
 * which is the NC_FILE_INFO_T struct allocated for this file and its
 * metadata. Ignored if NULL. (This is passed as a void to allow
 * external user-defined formats to use this function.)
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID No NC struct with this ext_ncid.
 * @return ::NC_ENOMEM Out of memory.
 * @author Ed Hartnett
 */
int
nc4_file_list_add(int ncid, const char *path, int mode, void **dispatchdata)
{
    NC *nc;
    int ret;

    /* Find NC pointer for this file. */
    if ((ret = NC_check_id(ncid, &nc)))
        return ret;

    /* Add necessary structs to hold netcdf-4 file data. This is where
     * the NC_FILE_INFO_T struct is allocated for the file. */
    if ((ret = nc4_nc4f_list_add(nc, path, mode)))
        return ret;

    /* If the user wants a pointer to the NC_FILE_INFO_T, then provide
     * it. */
    if (dispatchdata)
        *dispatchdata = nc->dispatchdata;

    return NC_NOERR;
}

/**
 * @internal Change the ncid of an open file. This is needed for PIO
 * integration.
 *
 * @param ncid The ncid of the file (aka ext_ncid).
 * @param new_ncid_index The new ncid index to use (i.e. the first two bytes
 * of the ncid).
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID No NC struct with this ext_ncid.
 * @return ::NC_ENOMEM Out of memory.
 * @author Ed Hartnett
 */
int
nc4_file_change_ncid(int ncid, unsigned short new_ncid_index)
{
    NC *nc;
    int ret;

    LOG((2, "%s: ncid %d new_ncid_index %d", __func__, ncid, new_ncid_index));

    /* Find NC pointer for this file. */
    if ((ret = NC_check_id(ncid, &nc)))
        return ret;

    /* Move it in the list. It will fail if list spot is already
     * occupied. */
    LOG((3, "moving nc->ext_ncid %d nc->ext_ncid >> ID_SHIFT %d",
         nc->ext_ncid, nc->ext_ncid >> ID_SHIFT));
    if (NC4_move_in_NCList(nc, new_ncid_index))
        return NC_EIO;
    LOG((3, "moved to new_ncid_index %d new nc->ext_ncid %d", new_ncid_index,
         nc->ext_ncid));

    return NC_NOERR;
}

/**
 * @internal Get info about a file on the list of libsrc4 open
 * files. This is used by dispatch layers that wish to use the libsrc4
 * metadata model, but don't know about struct NC.
 *
 * @param ncid The ncid of the file (aka ext_ncid).
 * @param path A pointer that gets file name (< NC_MAX_NAME). Ignored
 * if NULL.
 * @param mode A pointer that gets the mode flag. Ignored if NULL.
 * @param dispatchdata Void * that gets pointer to dispatch data,
 * which is the NC_FILE_INFO_T struct allocated for this file and its
 * metadata. Ignored if NULL. (This is passed as a void to allow
 * external user-defined formats to use this function.)
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID No NC struct with this ext_ncid.
 * @return ::NC_ENOMEM Out of memory.
 * @author Ed Hartnett
 */
int
nc4_file_list_get(int ncid, char **path, int *mode, void **dispatchdata)
{
    NC *nc;
    int ret;

    /* Find NC pointer for this file. */
    if ((ret = NC_check_id(ncid, &nc)))
        return ret;

    /* If the user wants path, give it. */
    if (path)
        strncpy(*path, nc->path, NC_MAX_NAME);

    /* If the user wants mode, give it. */
    if (mode)
        *mode = nc->mode;

    /* If the user wants dispatchdata, give it. */
    if (dispatchdata)
        *dispatchdata = nc->dispatchdata;

    return NC_NOERR;
}

/**
 * @internal Given an NC pointer, add the necessary stuff for a
 * netcdf-4 file. This allocates the NC_FILE_INFO_T struct for the
 * file, which is used by libhdf5 and libhdf4 (and perhaps other
 * future dispatch layers) to hold the metadata for the file.
 *
 * @param nc Pointer to file's NC struct.
 * @param path The file name of the new file.
 * @param mode The mode flag.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_ENOMEM Out of memory.
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
nc4_nc4f_list_add(NC *nc, const char *path, int mode)
{
    NC_FILE_INFO_T *h5;
    int retval;

    assert(nc && !NC4_DATA(nc) && path);

    /* We need to malloc and initialize the substructure
       NC_FILE_INFO_T. */
    if (!(h5 = calloc(1, sizeof(NC_FILE_INFO_T))))
        return NC_ENOMEM;
    nc->dispatchdata = h5;
    h5->controller = nc;

    h5->hdr.sort = NCFIL;
    h5->hdr.name = strdup(path);    
    h5->hdr.id = nc->ext_ncid;

    /* Hang on to cmode, and note that we're in define mode. */
    h5->cmode = mode | NC_INDEF;

    /* The next_typeid needs to be set beyond the end of our atomic
     * types. */
    h5->next_typeid = NC_FIRSTUSERTYPEID;

    /* Initialize lists for dimensions, types, and groups. */
    h5->alldims = nclistnew();
    h5->alltypes = nclistnew();
    h5->allgroups = nclistnew();

    /* There's always at least one open group - the root
     * group. Allocate space for one group's worth of information. Set
     * its grp id, name, and allocate associated empty lists. */
    if ((retval = nc4_grp_list_add(h5, NULL, NC_GROUP_NAME, &h5->root_grp)))
        return retval;

    return NC_NOERR;
}

/**
 * @internal Given an ncid, find the relevant group and return a
 * pointer to it.
 *
 * @param ncid File and group ID.
 * @param grp Pointer that gets pointer to group info struct. Ignored
 * if NULL.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_ENOTNC4 Not a netCDF-4 file.
 * @author Ed Hartnett
 */
int
nc4_find_nc4_grp(int ncid, NC_GRP_INFO_T **grp)
{
    return nc4_find_nc_grp_h5(ncid, NULL, grp, NULL);
}

/**
 * @internal Given an ncid, find the relevant group and return a
 * pointer to it, also set a pointer to the nc4_info struct of the
 * related file.
 *
 * @param ncid File and group ID.
 * @param grp Pointer that gets pointer to group info struct. Ignored
 * if NULL.
 * @param h5 Pointer that gets pointer to file info struct. Ignored if
 * NULL.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @author Ed Hartnett
 */
int
nc4_find_grp_h5(int ncid, NC_GRP_INFO_T **grp, NC_FILE_INFO_T **h5)
{
    return nc4_find_nc_grp_h5(ncid, NULL, grp, h5);
}

/**
 * @internal Find info for this file and group, and set pointers.
 *
 * @param ncid File and group ID.
 * @param nc Pointer that gets a pointer to the file's NC
 * struct. Ignored if NULL.
 * @param grp Pointer that gets a pointer to the group
 * struct. Ignored if NULL.
 * @param h5 Pointer that gets HDF5 file struct. Ignored if NULL.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
nc4_find_nc_grp_h5(int ncid, NC **nc, NC_GRP_INFO_T **grp, NC_FILE_INFO_T **h5)
{
    NC_GRP_INFO_T *my_grp = NULL;
    NC_FILE_INFO_T *my_h5 = NULL;
    NC *my_nc;
    int retval;
    size_t index;

    /* Look up file metadata. */
    if ((retval = NC_check_id(ncid, &my_nc)))
        return retval;
    my_h5 = my_nc->dispatchdata;
    assert(my_h5 && my_h5->root_grp);

    /* If we can't find it, the grp id part of ncid is bad. */
    index =  (ncid & GRP_ID_MASK);
    if (!(my_grp = nclistget(my_h5->allgroups,index)))
        return NC_EBADID;

    /* Return pointers to caller, if desired. */
    if (nc)
        *nc = my_nc;
    if (h5)
        *h5 = my_h5;
    if (grp)
        *grp = my_grp;

    return NC_NOERR;
}

/**
 * @internal Given an ncid and varid, get pointers to the group and var
 * metadata.
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param h5 Pointer that gets pointer to the NC_FILE_INFO_T struct
 * for this file. Ignored if NULL.
 * @param grp Pointer that gets pointer to group info. Ignored if
 * NULL.
 * @param var Pointer that gets pointer to var info. Ignored if NULL.
 *
 * @return ::NC_NOERR No error.
 * @author Ed Hartnett
 */
int
nc4_find_grp_h5_var(int ncid, int varid, NC_FILE_INFO_T **h5, NC_GRP_INFO_T **grp,
                    NC_VAR_INFO_T **var)
{
    NC_FILE_INFO_T *my_h5;
    NC_GRP_INFO_T *my_grp;
    NC_VAR_INFO_T *my_var;
    int retval;

    /* Look up file and group metadata. */
    if ((retval = nc4_find_grp_h5(ncid, &my_grp, &my_h5)))
        return retval;
    assert(my_grp && my_h5);

    /* Find the var. */
    if (!(my_var = (NC_VAR_INFO_T *)ncindexith(my_grp->vars, (size_t)varid)))
        return NC_ENOTVAR;
    assert(my_var && my_var->hdr.id == varid);

    /* Return pointers that caller wants. */
    if (h5)
        *h5 = my_h5;
    if (grp)
        *grp = my_grp;
    if (var)
        *var = my_var;

    return NC_NOERR;
}

/**
 * @internal Find a dim in the file.
 *
 * @param grp Pointer to group info struct.
 * @param dimid Dimension ID to find.
 * @param dim Pointer that gets pointer to dim info if found.
 * @param dim_grp Pointer that gets pointer to group info of group
 * that contains dimension. Ignored if NULL.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADDIM Dimension not found.
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
nc4_find_dim(NC_GRP_INFO_T *grp, int dimid, NC_DIM_INFO_T **dim,
             NC_GRP_INFO_T **dim_grp)
{
    assert(grp && grp->nc4_info && dim);
    LOG((4, "%s: dimid %d", __func__, dimid));

    /* Find the dim info. */
    if (!((*dim) = nclistget(grp->nc4_info->alldims, (size_t)dimid)))
        return NC_EBADDIM;

    /* Give the caller the group the dimension is in. */
    if (dim_grp)
        *dim_grp = (*dim)->container;

    return NC_NOERR;
}

/**
 * @internal Find a var (by name) in a grp.
 *
 * @param grp Pointer to group info.
 * @param name Name of var to find.
 * @param var Pointer that gets pointer to var info struct, if found.
 *
 * @return ::NC_NOERR No error.
 * @author Ed Hartnett
 */
int
nc4_find_var(NC_GRP_INFO_T *grp, const char *name, NC_VAR_INFO_T **var)
{
    assert(grp && var && name);

    /* Find the var info. */
    *var = (NC_VAR_INFO_T*)ncindexlookup(grp->vars,name);
    return NC_NOERR;
}

/**
 * @internal Locate netCDF type by name.
 *
 * @param start_grp Pointer to starting group info.
 * @param name Name of type to find.
 *
 * @return Pointer to type info, or NULL if not found.
 * @author Ed Hartnett, Dennis Heimbigner
 */
NC_TYPE_INFO_T *
nc4_rec_find_named_type(NC_GRP_INFO_T *start_grp, char *name)
{
    NC_GRP_INFO_T *g;
    NC_TYPE_INFO_T *type, *res;

    assert(start_grp);

    /* Does this group have the type we are searching for? */
    type  = (NC_TYPE_INFO_T*)ncindexlookup(start_grp->type,name);
    if(type != NULL)
        return type;

    /* Search subgroups. */
    for(size_t i=0;i<ncindexsize(start_grp->children);i++) {
        g = (NC_GRP_INFO_T*)ncindexith(start_grp->children,i);
        if(g == NULL) continue;
        if ((res = nc4_rec_find_named_type(g, name)))
            return res;
    }
    /* Can't find it. Oh, woe is me! */
    return NULL;
}

/**
 * @internal Use a netCDF typeid to find a type in a type_list.
 *
 * @param h5 Pointer to HDF5 file info struct.
 * @param typeid The netCDF type ID.
 * @param type Pointer to pointer to the list of type info structs.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EINVAL Invalid input.
 * @author Ed Hartnett
 */
int
nc4_find_type(const NC_FILE_INFO_T *h5, nc_type typeid, NC_TYPE_INFO_T **type)
{
    /* Check inputs. */
    assert(h5);
    if (typeid < 0 || !type)
        return NC_EINVAL;
    *type = NULL;

    /* Atomic types don't have associated NC_TYPE_INFO_T struct, just
     * return NOERR. */
    if (typeid <= NC_STRING)
        return NC_NOERR;

    /* Find the type. */
    if (!(*type = nclistget(h5->alltypes, (size_t)typeid)))
        return NC_EBADTYPID;

    return NC_NOERR;
}

/**
 * @internal Given a group, find an att. If name is provided, use that,
 * otherwise use the attnum.
 *
 * @param grp Pointer to group info struct.
 * @param varid Variable ID.
 * @param name Name to of attribute.
 * @param attnum Number of attribute.
 * @param att Pointer to pointer that gets attribute info struct.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_ENOTVAR Variable not found.
 * @return ::NC_ENOTATT Attribute not found.
 * @author Ed Hartnett
 */
int
nc4_find_grp_att(NC_GRP_INFO_T *grp, int varid, const char *name, int attnum,
                 NC_ATT_INFO_T **att)
{
    NC_VAR_INFO_T *var;
    NC_ATT_INFO_T *my_att;
    NCindex *attlist = NULL;

    assert(grp && grp->hdr.name && att);

    LOG((4, "%s: grp->name %s varid %d attnum %d", __func__, grp->hdr.name,
         varid, attnum));

    /* Get either the global or a variable attribute list. */
    if (varid == NC_GLOBAL)
    {
        attlist = grp->att;
    }
    else
    {
        var = (NC_VAR_INFO_T*)ncindexith(grp->vars,(size_t)varid);
        if (!var) return NC_ENOTVAR;

        attlist = var->att;
    }
    assert(attlist);

    /* Now find the attribute by name or number. If a name is provided,
     * ignore the attnum. */
    if (name)
        my_att = (NC_ATT_INFO_T *)ncindexlookup(attlist, name);
    else
        my_att = (NC_ATT_INFO_T *)ncindexith(attlist, (size_t)attnum);

    if (!my_att)
        return NC_ENOTATT;

    *att = my_att;
    return NC_NOERR;
}

/**
 * @internal Given an ncid, varid, and name or attnum, find and return
 * pointer to NC_ATT_INFO_T metadata.
 *
 * @param ncid File and group ID.
 * @param varid Variable ID.
 * @param name Name to of attribute.
 * @param attnum Number of attribute.
 * @param att Pointer to pointer that gets attribute info struct.

 *
 * @return ::NC_NOERR No error.
 * @return ::NC_ENOTVAR Variable not found.
 * @return ::NC_ENOTATT Attribute not found.
 * @author Ed Hartnett
 */
int
nc4_find_nc_att(int ncid, int varid, const char *name, int attnum,
                NC_ATT_INFO_T **att)
{
    NC_GRP_INFO_T *grp;
    int retval;

    LOG((4, "nc4_find_nc_att: ncid 0x%x varid %d name %s attnum %d",
         ncid, varid, name, attnum));

    /* Find info for this file and group, and set pointer to each. */
    if ((retval = nc4_find_grp_h5(ncid, &grp, NULL)))
        return retval;
    assert(grp);

    return nc4_find_grp_att(grp, varid, name, attnum, att);
}

/**
 * @internal Add NC_OBJ to allXXX lists in a file
 *
 * @param file Pointer to the containing file
 * @param obj Pointer to object to add.
 *
 * @author Dennis Heimbigner
 */
static void
obj_track(NC_FILE_INFO_T* file, NC_OBJ* obj)
{
    NClist* list = NULL;
    /* record the object in the file  */
    switch (obj->sort) {
    case NCDIM: list = file->alldims; break;
    case NCTYP: list = file->alltypes; break;
    case NCGRP: list = file->allgroups; break;
    default:
        assert(NC_FALSE);
    }
    /* Insert at the appropriate point in the list */
    nclistset(list,(size_t)obj->id,obj);
}

/**
 * @internal Create a new variable and insert into relevant
 * lists. Dimensionality info need not be known.
 *
 * @param grp the containing group
 * @param name the name for the new variable
 * @param var Pointer in which to return a pointer to the new var.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_ENOMEM Out of memory.
 * @author Ed Hartnett
 */
int
nc4_var_list_add2(NC_GRP_INFO_T *grp, const char *name, NC_VAR_INFO_T **var)
{
    NC_VAR_INFO_T *new_var = NULL;
    NCglobalstate* gs = NC_getglobalstate();

    /* Allocate storage for new variable. */
    if (!(new_var = calloc(1, sizeof(NC_VAR_INFO_T))))
        return NC_ENOMEM;
    new_var->hdr.sort = NCVAR;
    new_var->container = grp;

    memset(&new_var->chunkcache,0,sizeof(struct ChunkCache));

    /* These are the HDF5-1.8.4 defaults. */
    new_var->chunkcache.size = gs->chunkcache.size;
    new_var->chunkcache.nelems = gs->chunkcache.nelems;
    new_var->chunkcache.preemption = gs->chunkcache.preemption;

    /* Now fill in the values in the var info structure. */
    new_var->hdr.id = (int)ncindexsize(grp->vars);
    if (!(new_var->hdr.name = strdup(name))) {
      if(new_var)
        free(new_var);
      return NC_ENOMEM;
    }

    /* Create an indexed list for the attributes. */
    new_var->att = ncindexnew(0);

    /* Officially track it */
    ncindexadd(grp->vars, (NC_OBJ *)new_var);

    /* Set the var pointer, if one was given */
    if (var)
        *var = new_var;

    return NC_NOERR;
}

/**
 * @internal Set the number of dims in an NC_VAR_INFO_T struct.
 *
 * @param var Pointer to the var.
 * @param ndims Number of dimensions for this var.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_ENOMEM Out of memory.
 * @author Ed Hartnett
 */
int
nc4_var_set_ndims(NC_VAR_INFO_T *var, int ndims)
{
    assert(var);

    /* Remember the number of dimensions. */
    var->ndims = (size_t)ndims;

    /* Allocate space for dimension information. */
    if (ndims)
    {
      if (!(var->dim = calloc((size_t)ndims, sizeof(NC_DIM_INFO_T *))))
            return NC_ENOMEM;
      if (!(var->dimids = calloc((size_t)ndims, sizeof(int))))
            return NC_ENOMEM;

        /* Initialize dimids to illegal values (-1). See the comment
           in nc4_rec_match_dimscales(). */
      memset(var->dimids, -1, (size_t)ndims * sizeof(int));
    }

    return NC_NOERR;
}

/**
 * @internal Create a new variable and insert int relevant list.
 *
 * @param grp the containing group
 * @param name the name for the new variable
 * @param ndims the rank of the new variable
 * @param var Pointer in which to return a pointer to the new var.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_ENOMEM Out of memory.
 * @author Ed Hartnett
 */
int
nc4_var_list_add(NC_GRP_INFO_T* grp, const char* name, int ndims,
                 NC_VAR_INFO_T **var)
{
    int retval;

    if ((retval = nc4_var_list_add2(grp, name, var)))
        return retval;
    if ((retval = nc4_var_set_ndims(*var, ndims)))
        return retval;

    return NC_NOERR;
}

/**
 * @internal Add a dimension to the dimension list for a group.
 *
 * @param grp container for the dim
 * @param name for the dim
 * @param len for the dim
 * @param assignedid override dimid if >= 0
 * @param dim Pointer to pointer that gets the new dim info struct.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_ENOMEM Out of memory.
 * @author Ed Hartnett
 */
int
nc4_dim_list_add(NC_GRP_INFO_T *grp, const char *name, size_t len,
                 int assignedid, NC_DIM_INFO_T **dim)
{
    NC_DIM_INFO_T *new_dim = NULL;

    assert(grp && name);

    /* Allocate memory for dim metadata. */
    if (!(new_dim = calloc(1, sizeof(NC_DIM_INFO_T))))
        return NC_ENOMEM;

    new_dim->hdr.sort = NCDIM;

    /* Assign the dimension ID. */
    if (assignedid >= 0)
        new_dim->hdr.id = assignedid;
    else
        new_dim->hdr.id = grp->nc4_info->next_dimid++;

    /* Remember the name and create a hash. */
    if (!(new_dim->hdr.name = strdup(name))) {
      if(new_dim)
        free(new_dim);

      return NC_ENOMEM;
    }

    /* Is dimension unlimited? */
    new_dim->len = len;
    if (len == NC_UNLIMITED)
        new_dim->unlimited = NC_TRUE;

    /* Remember the containing group. */
    new_dim->container = grp;

    /* Add object to dimension list for this group. */
    ncindexadd(grp->dim, (NC_OBJ *)new_dim);
    obj_track(grp->nc4_info, (NC_OBJ *)new_dim);

    /* Set the dim pointer, if one was given */
    if (dim)
        *dim = new_dim;

    return NC_NOERR;
}

/**
 * @internal Add to an attribute list.
 *
 * @param list NCindex of att info structs.
 * @param name name of the new attribute
 * @param att Pointer to pointer that gets the new att info
 * struct. Ignored if NULL.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_ENOMEM Out of memory.
 * @author Ed Hartnett
 */
int
nc4_att_list_add(NCindex *list, const char *name, NC_ATT_INFO_T **att)
{
    NC_ATT_INFO_T *new_att = NULL;

    LOG((3, "%s: name %s ", __func__, name));

    if (!(new_att = calloc(1, sizeof(NC_ATT_INFO_T))))
        return NC_ENOMEM;
    new_att->hdr.sort = NCATT;

    /* Fill in the information we know. */
    new_att->hdr.id = (int)ncindexsize(list);
    if (!(new_att->hdr.name = strdup(name))) {
      if(new_att)
        free(new_att);
      return NC_ENOMEM;
    }

    /* Add object to list as specified by its number */
    ncindexadd(list, (NC_OBJ *)new_att);

    /* Set the attribute pointer, if one was given */
    if (att)
        *att = new_att;

    return NC_NOERR;
}

/**
 * @internal Add a group to a group list.
 *
 * @param h5 Pointer to the file info.
 * @param parent Pointer to the parent group. Will be NULL when adding
 * the root group.
 * @param name Name of the group.
 * @param grp Pointer to pointer that gets new group info
 * struct. Ignored if NULL.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_ENOMEM Out of memory.
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
nc4_grp_list_add(NC_FILE_INFO_T *h5, NC_GRP_INFO_T *parent, char *name,
                 NC_GRP_INFO_T **grp)
{
    NC_GRP_INFO_T *new_grp;

    /* Check inputs. */
    assert(h5 && name);
    LOG((3, "%s: name %s ", __func__, name));

    /* Get the memory to store this groups info. */
    if (!(new_grp = calloc(1, sizeof(NC_GRP_INFO_T))))
        return NC_ENOMEM;

    /* Fill in this group's information. */
    new_grp->hdr.sort = NCGRP;
    new_grp->nc4_info = h5;
    new_grp->parent = parent;

    /* Assign the group ID. The root group will get id 0. */
    new_grp->hdr.id = h5->next_nc_grpid++;
    assert(parent || !new_grp->hdr.id);

    /* Handle the group name. */
    if (!(new_grp->hdr.name = strdup(name)))
    {
        free(new_grp);
        return NC_ENOMEM;
    }

    /* Set up new indexed lists for stuff this group can contain. */
    new_grp->children = ncindexnew(0);
    new_grp->dim = ncindexnew(0);
    new_grp->att = ncindexnew(0);
    new_grp->type = ncindexnew(0);
    new_grp->vars = ncindexnew(0);

    /* Add object to lists */
    if (parent)
        ncindexadd(parent->children, (NC_OBJ *)new_grp);
    obj_track(h5, (NC_OBJ *)new_grp);

    /* Set the group pointer, if one was given */
    if (grp)
        *grp = new_grp;

    return NC_NOERR;
}

/**
 * @internal Names for groups, variables, and types must not be the
 * same. This function checks that a proposed name is not already in
 * use. Normalzation of UTF8 strings should happen before this
 * function is called.
 *
 * @param grp Pointer to group info struct.
 * @param name Name to check.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_ENAMEINUSE Name is in use.
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
nc4_check_dup_name(NC_GRP_INFO_T *grp, char *name)
{
    NC_TYPE_INFO_T *type;
    NC_GRP_INFO_T *g;
    NC_VAR_INFO_T *var;

    /* Any types of this name? */
    type = (NC_TYPE_INFO_T*)ncindexlookup(grp->type,name);
    if(type != NULL)
        return NC_ENAMEINUSE;

    /* Any child groups of this name? */
    g = (NC_GRP_INFO_T*)ncindexlookup(grp->children,name);
    if(g != NULL)
        return NC_ENAMEINUSE;

    /* Any variables of this name? */
    var = (NC_VAR_INFO_T*)ncindexlookup(grp->vars,name);
    if(var != NULL)
        return NC_ENAMEINUSE;

    return NC_NOERR;
}

/**
 * @internal Create a type, but do not add to various lists nor
 * increment its ref count
 *
 * @param size Size of type in bytes.
 * @param name Name of type.
 * @param assignedid if >= 0 then override the default type id.
 * @param type Pointer that gets pointer to new type info struct.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_ENOMEM Out of memory.
 * @author Ed Hartnett, Ward Fisher
 */
int
nc4_type_new(size_t size, const char *name, int assignedid,
             NC_TYPE_INFO_T **type)
{
    NC_TYPE_INFO_T *new_type;

    LOG((4, "%s: size %d name %s assignedid %d", __func__, size, name, assignedid));

    /* Check inputs. */
    assert(type);

    /* Allocate memory for the type */
    if (!(new_type = calloc(1, sizeof(NC_TYPE_INFO_T))))
        return NC_ENOMEM;
    new_type->hdr.sort = NCTYP;
    new_type->hdr.id = assignedid;

    /* Remember info about this type. */
    new_type->size = size;
    if (!(new_type->hdr.name = strdup(name))) {
        free(new_type);
        return NC_ENOMEM;
    }

    /* Return a pointer to the new type. */
    *type = new_type;

    return NC_NOERR;
}

/**
 * @internal Add to the type list.
 *
 * @param grp Pointer to group info struct.
 * @param size Size of type in bytes.
 * @param name Name of type.
 * @param type Pointer that gets pointer to new type info
 * struct.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_ENOMEM Out of memory.
 * @author Ed Hartnett
 */
int
nc4_type_list_add(NC_GRP_INFO_T *grp, size_t size, const char *name,
                  NC_TYPE_INFO_T **type)
{
    NC_TYPE_INFO_T *new_type;
    int retval;

    /* Check inputs. */
    assert(grp && name && type);
    LOG((4, "%s: size %d name %s", __func__, size, name));

    /* Create the new TYPE_INFO struct. */
    if ((retval = nc4_type_new(size, name, grp->nc4_info->next_typeid,
                               &new_type)))
        return retval;
    grp->nc4_info->next_typeid++;

    /* Increment the ref. count on the type */
    new_type->rc++;

    /* Add object to lists */
    ncindexadd(grp->type, (NC_OBJ *)new_type);
    obj_track(grp->nc4_info,(NC_OBJ*)new_type);

    /* back link */
    new_type->container = grp;

    /* Return a pointer to the new type. */
    *type = new_type;

    return NC_NOERR;
}

/**
 * @internal Add to the compound field list.
 *
 * @param parent parent type
 * @param name Name of the field.
 * @param offset Offset in bytes.
 * @param xtype The netCDF type of the field.
 * @param ndims The number of dimensions of the field.
 * @param dim_sizesp An array of dim sizes for the field.
 *
 * @return ::NC_NOERR No error.
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
nc4_field_list_add(NC_TYPE_INFO_T *parent, const char *name,
                   size_t offset, nc_type xtype, int ndims,
                   const int *dim_sizesp)
{
    NC_FIELD_INFO_T *field;

    /* Name has already been checked and UTF8 normalized. */
    if (!name)
        return NC_EINVAL;

    /* Allocate storage for this field information. */
    if (!(field = calloc(1, sizeof(NC_FIELD_INFO_T))))
        return NC_ENOMEM;
    field->hdr.sort = NCFLD;

    /* Store the information about this field. */
    if (!(field->hdr.name = strdup(name)))
    {
        free(field);
        return NC_ENOMEM;
    }
    field->nc_typeid = xtype;
    field->offset = offset;
    field->ndims = ndims;
    if (ndims)
    {
        int i;
        if (!(field->dim_size = malloc((size_t)ndims * sizeof(int))))
        {
            free(field->hdr.name);
            free(field);
            return NC_ENOMEM;
        }
        for (i = 0; i < ndims; i++)
            field->dim_size[i] = dim_sizesp[i];
    }

    /* Add object to lists */
    field->hdr.id = (int)nclistlength(parent->u.c.field);
    nclistpush(parent->u.c.field,field);

    return NC_NOERR;
}

/**
 * @internal Add a member to an enum type.
 *
 * @param parent Containing NC_TYPE_INFO_T object
 * @param size Size in bytes of new member.
 * @param name Name of the member.
 * @param value Value to associate with member.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_ENOMEM Out of memory.
 * @author Ed Hartnett
 */
int
nc4_enum_member_add(NC_TYPE_INFO_T *parent, size_t size,
                    const char *name, const void *value)
{
    NC_ENUM_MEMBER_INFO_T *member;

    /* Name has already been checked. */
    assert(name && size > 0 && value);
    LOG((4, "%s: size %d name %s", __func__, size, name));

    /* Allocate storage for this field information. */
    if (!(member = calloc(1, sizeof(NC_ENUM_MEMBER_INFO_T))))
        return NC_ENOMEM;
    if (!(member->value = malloc(size))) {
        free(member);
        return NC_ENOMEM;
    }
    if (!(member->name = strdup(name))) {
        free(member->value);
        free(member);
        return NC_ENOMEM;
    }

    /* Store the value for this member. */
    memcpy(member->value, value, size);

    /* Add object to list */
    nclistpush(parent->u.e.enum_member,member);

    return NC_NOERR;
}

/**
 * @internal Free up a field
 *
 * @param field Pointer to field info of field to delete.
 *
 * @author Ed Hartnett
 */
static void
field_free(NC_FIELD_INFO_T *field)
{
    /* Free some stuff. */
    if (field->hdr.name)
        free(field->hdr.name);
    if (field->dim_size)
        free(field->dim_size);

    /* Nc_Free the memory. */
    free(field);
}

/**
 * @internal Free allocated space for type information.
 *
 * @param type Pointer to type info struct.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EHDFERR HDF5 error.
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
nc4_type_free(NC_TYPE_INFO_T *type)
{
    size_t i;

    assert(type && type->rc && type->hdr.name);

    /* Decrement the ref. count on the type */
    type->rc--;

    /* Release the type, if the ref. count drops to zero */
    if (type->rc == 0)
    {
        LOG((4, "%s: deleting type %s", __func__, type->hdr.name));

        /* Free the name. */
        free(type->hdr.name);

        /* Enums and compound types have lists of fields to clean up. */
        switch (type->nc_type_class)
        {
        case NC_COMPOUND:
        {
            NC_FIELD_INFO_T *field;

            /* Delete all the fields in this type (there will be some if its a
             * compound). */
            for(i=0;i<nclistlength(type->u.c.field);i++) {
                field = nclistget(type->u.c.field,i);
                field_free(field);
            }
            nclistfree(type->u.c.field);
        }
        break;

        case NC_ENUM:
        {
            NC_ENUM_MEMBER_INFO_T *enum_member;

            /* Delete all the enum_members, if any. */
            for(i=0;i<nclistlength(type->u.e.enum_member);i++) {
                enum_member = nclistget(type->u.e.enum_member,i);
                free(enum_member->value);
                free(enum_member->name);
                free(enum_member);
            }
            nclistfree(type->u.e.enum_member);
        }
        break;

        default:
            break;
        }

        /* Release the memory. */
        free(type);
    }

    return NC_NOERR;
}

/**
 * @internal Free memory of an attribute object
 *
 * @param att Pointer to attribute info struct.
 *
 * @return ::NC_NOERR No error.
 * @author Ed Hartnett
 */
int
nc4_att_free(NC_ATT_INFO_T *att)
{
    int stat = NC_NOERR;

    assert(att);
    LOG((3, "%s: name %s ", __func__, att->hdr.name));

    /* Free the name. */
    if (att->hdr.name)
        free(att->hdr.name);

    if (att->data) {
	NC_OBJ* parent;
	NC_FILE_INFO_T* h5 = NULL;

	/* Locate relevant objects */
	parent = att->container;
	if(parent->sort == NCVAR) parent = (NC_OBJ*)(((NC_VAR_INFO_T*)parent)->container);
	assert(parent->sort == NCGRP);
	h5 = ((NC_GRP_INFO_T*)parent)->nc4_info;
	/* Reclaim the attribute data */
	if((stat = NC_reclaim_data(h5->controller,att->nc_typeid,att->data,att->len))) goto done;
	free(att->data); /* reclaim top level */
	att->data = NULL;
    }

done:
    free(att);
    return stat;
}

/**
 * @internal Delete a var, and free the memory. All HDF5 objects for
 * the var must be closed before this is called.
 *
 * @param var Pointer to the var info struct of var to delete.
 *
 * @return ::NC_NOERR No error.
 * @author Ed Hartnett, Dennis Heimbigner
 */
static int
var_free(NC_VAR_INFO_T *var)
{
    int retval;

    assert(var);
    LOG((4, "%s: deleting var %s", __func__, var->hdr.name));

    /* First delete all the attributes attached to this var. */
    for (size_t i = 0; i < ncindexsize(var->att); i++)
        if ((retval = nc4_att_free((NC_ATT_INFO_T *)ncindexith(var->att, i))))
            return retval;
    ncindexfree(var->att);

    /* Free some things that may be allocated. */
    if (var->chunksizes)
        free(var->chunksizes);

    if (var->alt_name)
        free(var->alt_name);

    if (var->dimids)
        free(var->dimids);

    if (var->dim)
        free(var->dim);

    memset(&var->chunkcache,0,sizeof(struct ChunkCache));

    /* Delete any fill value allocation. */
    if (var->fill_value) {
	int tid = var->type_info->hdr.id;
        if((retval = NC_reclaim_data_all(var->container->nc4_info->controller, tid, var->fill_value, 1))) return retval;
	var->fill_value = NULL;
    }

    /* Release type information */
    if (var->type_info)
        if ((retval = nc4_type_free(var->type_info)))
            return retval;

    /* Do this last because debugging may need it */
    if (var->hdr.name)
        free(var->hdr.name);

    /* Delete the var. */
    free(var);

    return NC_NOERR;
}

/**
 * @internal  Delete a var, and free the memory.
 *
 * @param grp Pointer to the strct for the containing group.
 * @param var Pointer to the var info struct of var to delete.
 *
 * @return ::NC_NOERR No error.
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
nc4_var_list_del(NC_GRP_INFO_T *grp, NC_VAR_INFO_T *var)
{
    int i;

    assert(var && grp);

    /* Remove from lists */
    i = ncindexfind(grp->vars, (NC_OBJ *)var);
    if (i >= 0)
        ncindexidel(grp->vars, (size_t)i);

    return var_free(var);
}

/**
 * @internal Free a dim
 *
 * @param dim Pointer to dim info struct of type to delete.
 *
 * @return ::NC_NOERR No error.
 * @author Ed Hartnett, Ward Fisher
 */
static int
dim_free(NC_DIM_INFO_T *dim)
{
    assert(dim);
    LOG((4, "%s: deleting dim %s", __func__, dim->hdr.name));

    /* Free memory allocated for names. */
    if (dim->hdr.name)
        free(dim->hdr.name);

    free(dim);
    return NC_NOERR;
}

/**
 * @internal Free a dim and unlist it
 *
 * @param grp Pointer to dim's containing group
 * @param dim Pointer to dim info struct of type to delete.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
int
nc4_dim_list_del(NC_GRP_INFO_T *grp, NC_DIM_INFO_T *dim)
{
    if (grp && dim)
    {
        int pos = ncindexfind(grp->dim, (NC_OBJ *)dim);
        if(pos >= 0)
            ncindexidel(grp->dim, (size_t)pos);
    }

    return dim_free(dim);
}

/**
 * @internal Recursively delete the data for a group (and everything
 * it contains) in our internal metadata store.
 *
 * @param grp Pointer to group info struct.
 *
 * @return ::NC_NOERR No error.
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
nc4_rec_grp_del(NC_GRP_INFO_T *grp)
{
    int retval;

    assert(grp);
    LOG((3, "%s: grp->name %s", __func__, grp->hdr.name));

    /* Recursively call this function for each child, if any, stopping
     * if there is an error. */
    for (size_t i = 0; i < ncindexsize(grp->children); i++)
        if ((retval = nc4_rec_grp_del((NC_GRP_INFO_T *)ncindexith(grp->children,
                                                                  i))))
            return retval;
    ncindexfree(grp->children);

    /* Free attributes */
    for (size_t i = 0; i < ncindexsize(grp->att); i++)
        if ((retval = nc4_att_free((NC_ATT_INFO_T *)ncindexith(grp->att, i))))
            return retval;
    ncindexfree(grp->att);

    /* Delete all vars. */
    for (size_t i = 0; i < ncindexsize(grp->vars); i++) {
        NC_VAR_INFO_T* v = (NC_VAR_INFO_T *)ncindexith(grp->vars, i);
        if ((retval = var_free(v)))
            return retval;
    }
    ncindexfree(grp->vars);

    /* Delete all dims, and free the list of dims. */
    for (size_t i = 0; i < ncindexsize(grp->dim); i++)
        if ((retval = dim_free((NC_DIM_INFO_T *)ncindexith(grp->dim, i))))
            return retval;
    ncindexfree(grp->dim);

    /* Delete all types. */
    for (size_t i = 0; i < ncindexsize(grp->type); i++)
        if ((retval = nc4_type_free((NC_TYPE_INFO_T *)ncindexith(grp->type, i))))
            return retval;
    ncindexfree(grp->type);

    /* Free the name. */
    free(grp->hdr.name);

    /* Free up this group */
    free(grp);

    return NC_NOERR;
}

/**
 * @internal Recursively delete the data for a group (and everything
 * it contains) in our internal metadata store.
 *
 * @param grp Pointer to group info struct.
 *
 * @return ::NC_NOERR No error.
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
nc4_rec_grp_del_att_data(NC_GRP_INFO_T *grp)
{
    int retval;

    assert(grp);
    LOG((3, "%s: grp->name %s", __func__, grp->hdr.name));

    /* Recursively call this function for each child, if any, stopping
     * if there is an error. */
    for (size_t i = 0; i < ncindexsize(grp->children); i++)
        if ((retval = nc4_rec_grp_del_att_data((NC_GRP_INFO_T *)ncindexith(grp->children, i))))
            return retval;

    /* Free attribute data in this group */
    for (size_t i = 0; i < ncindexsize(grp->att); i++) {
        NC_ATT_INFO_T * att = (NC_ATT_INFO_T*)ncindexith(grp->att, i);
        if((retval = NC_reclaim_data_all(grp->nc4_info->controller,att->nc_typeid,att->data,att->len)))
            return retval;
	att->data = NULL;
	att->len = 0;
	att->dirty = 0;
    }

    /* Delete att data from all contained vars in this group */
    for (size_t i = 0; i < ncindexsize(grp->vars); i++) {
	NC_VAR_INFO_T* v = (NC_VAR_INFO_T *)ncindexith(grp->vars, i);
	for(size_t j=0;j<ncindexsize(v->att);j++) {
	    NC_ATT_INFO_T* att = (NC_ATT_INFO_T*)ncindexith(v->att, j);
   	    if((retval = NC_reclaim_data_all(grp->nc4_info->controller,att->nc_typeid,att->data,att->len)))
	        return retval;
	    att->data = NULL;
	    att->len = 0;
	    att->dirty = 0;
	}
    }

    return NC_NOERR;
}

/**
 * @internal Remove a NC_ATT_INFO_T from an index.
 * This will nc_free the memory too.
 *
 * @param list Pointer to pointer of list.
 * @param att Pointer to attribute info struct.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
int
nc4_att_list_del(NCindex *list, NC_ATT_INFO_T *att)
{
    assert(att && list);
    ncindexidel(list, (size_t)((NC_OBJ *)att)->id);
    return nc4_att_free(att);
}

/**
 * @internal Free all resources and memory associated with a
 * NC_FILE_INFO_T. This is the same as nc4_nc4f_list_del(), except it
 * takes ncid. This function allows external dispatch layers, like
 * PIO, to manipulate the file list without needing to know about
 * internal netcdf structures.
 *
 * @param ncid The ncid of the file to release.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @author Ed Hartnett
 */
int
nc4_file_list_del(int ncid)
{
    NC_FILE_INFO_T *h5;
    int retval;

    /* Find our metadata for this file. */
    if ((retval = nc4_find_grp_h5(ncid, NULL, &h5)))
        return retval;
    assert(h5);

    /* Delete the file resources. */
    if ((retval = nc4_nc4f_list_del(h5)))
        return retval;

    return NC_NOERR;
}

/**
 * @internal Free all resources and memory associated with a
 * NC_FILE_INFO_T.
 *
 * @param h5 Pointer to NC_FILE_INFO_T to be freed.
 *
 * @return ::NC_NOERR No error.
 * @author Ed Hartnett
 */
int
nc4_nc4f_list_del(NC_FILE_INFO_T *h5)
{
    int retval;

    assert(h5);

    /* Order is important here. We must delete the attribute contents
       before deleting any metadata because nc_reclaim_data depends
       on the existence of the type info.
    */

    /* Delete all the attribute data contents in each group and variable. */
    if ((retval = nc4_rec_grp_del_att_data(h5->root_grp)))
        return retval;

    /* Delete all the list contents for vars, dims, and atts, in each
     * group. */
    if ((retval = nc4_rec_grp_del(h5->root_grp)))
        return retval;

    /* Cleanup these (extra) lists of all dims, groups, and types. */
    nclistfree(h5->alldims);
    nclistfree(h5->allgroups);
    nclistfree(h5->alltypes);

    /* Free the NC_FILE_INFO_T struct. */
    nullfree(h5->hdr.name);
    free(h5);

    return NC_NOERR;
}

/**
 * @internal Normalize a UTF8 name. Put the result in norm_name, which
 * can be NC_MAX_NAME + 1 in size. This function makes sure the free()
 * gets called on the return from utf8proc_NFC, and also ensures that
 * the name is not too long.
 *
 * @param name Name to normalize.
 * @param norm_name The normalized name.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EMAXNAME Name too long.
 * @author Dennis Heimbigner
 */
int
nc4_normalize_name(const char *name, char *norm_name)
{
    char *temp_name;
    int stat = nc_utf8_normalize((const unsigned char *)name,(unsigned char **)&temp_name);
    if(stat != NC_NOERR)
        return stat;
    if (strlen(temp_name) > NC_MAX_NAME)
    {
        free(temp_name);
        return NC_EMAXNAME;
    }
    strcpy(norm_name, temp_name);
    free(temp_name);
    return NC_NOERR;
}

#ifdef NETCDF_ENABLE_SET_LOG_LEVEL

/**
 * Initialize parallel I/O logging. For parallel I/O builds, open log
 * file, if not opened yet, or increment ref count if already open.
 *
 * @author Ed Hartnett
 */
int
nc4_init_logging(void)
{
    int ret = NC_NOERR;

#if LOGGING
#if NC_HAS_PARALLEL4
    if (!LOG_FILE && nc_log_level >= 0)
    {
        char log_filename[NC_MAX_NAME];
        int my_rank = 0;
        int mpierr;
        int mpi_initialized;

        /* If MPI has been initialized find the rank. */
        if ((mpierr = MPI_Initialized(&mpi_initialized)))
            return NC_EMPI;
        if (mpi_initialized)
        {
            if ((mpierr = MPI_Comm_rank(MPI_COMM_WORLD, &my_rank)))
                return NC_EMPI;
        }

        /* Create a filename with the rank in it. */
        snprintf(log_filename, sizeof(log_filename), "nc4_log_%d.log", my_rank);

        /* Open a file for this rank to log messages. */
        if (!(LOG_FILE = fopen(log_filename, "w")))
            return NC_EINTERNAL;
    }
#endif /* NC_HAS_PARALLEL4 */
#endif /* LOGGING */

    return ret;
}

/**
 * Finalize logging - close parallel I/O log files, if open. This does
 * nothing if logging is not enabled.
 *
 * @author Ed Hartnett
 */
void
nc4_finalize_logging(void)
{
#if LOGGING
#if NC_HAS_PARALLEL4
    if (LOG_FILE)
    {
        fclose(LOG_FILE);
        LOG_FILE = NULL;
    }
#endif /* NC_HAS_PARALLEL4 */
#endif /* LOGGING */
}

/**
 * Use this to set the global log level. 
 * 
 * Set it to NC_TURN_OFF_LOGGING (-1) to turn off all logging. Set it
 * to 0 to show only errors, and to higher numbers to show more and
 * more logging details. If logging is not enabled when building
 * netCDF, this function will do nothing.
 *
 * @param new_level The new logging level.
 *
 * @return ::NC_NOERR No error.
 * @author Ed Hartnett
 */
int
nc_set_log_level(int new_level)
{
#if LOGGING
    /* Remember the new level. */
    nc_log_level = new_level;

#if NC_HAS_PARALLEL4
    /* For parallel I/O builds, call the log init/finalize functions
     * as needed, to open and close the log files. */
    if (new_level >= 0)
    {
        if (!LOG_FILE)
            nc4_init_logging();
    }
    else
        nc4_finalize_logging();
#endif /* NC_HAS_PARALLEL4 */
    
    LOG((1, "log_level changed to %d", nc_log_level));
#endif /* LOGGING */
    
    return NC_NOERR;
}
#endif /* NETCDF_ENABLE_SET_LOG_LEVEL */

#if LOGGING
#define MAX_NESTS 10
/**
 * @internal Recursively print the metadata of a group.
 *
 * @param grp Pointer to group info struct.
 * @param tab_count Number of tabs.
 *
 * @return ::NC_NOERR No error.
 * @author Ed Hartnett, Dennis Heimbigner
 */
static int
rec_print_metadata(NC_GRP_INFO_T *grp, int tab_count)
{
    NC_ATT_INFO_T *att;
    NC_VAR_INFO_T *var;
    NC_DIM_INFO_T *dim;
    NC_TYPE_INFO_T *type;
    NC_FIELD_INFO_T *field;
    char tabs[MAX_NESTS+1] = "";
    char temp_string[10];
    int t, retval, d, i;

    /* Come up with a number of tabs relative to the group. */
    for (t = 0; t < tab_count && t < MAX_NESTS; t++)
        tabs[t] = '\t';
    tabs[t] = '\0';

    LOG((2, "%s GROUP - %s nc_grpid: %d nvars: %d natts: %d",
         tabs, grp->hdr.name, grp->hdr.id, ncindexsize(grp->vars), ncindexsize(grp->att)));

    for (i = 0; i < ncindexsize(grp->att); i++)
    {
        att = (NC_ATT_INFO_T *)ncindexith(grp->att, i);
        assert(att);
        LOG((2, "%s GROUP ATTRIBUTE - attnum: %d name: %s type: %d len: %d",
             tabs, att->hdr.id, att->hdr.name, att->nc_typeid, att->len));
    }

    for (i = 0; i < ncindexsize(grp->dim); i++)
    {
        dim = (NC_DIM_INFO_T *)ncindexith(grp->dim, i);
        assert(dim);
        LOG((2, "%s DIMENSION - dimid: %d name: %s len: %d unlimited: %d",
             tabs, dim->hdr.id, dim->hdr.name, dim->len, dim->unlimited));
    }

    for (i = 0; i < ncindexsize(grp->vars); i++)
    {
        int j;
        char storage_str[NC_MAX_NAME] = "";
        char *dims_string = NULL;

        var = (NC_VAR_INFO_T*)ncindexith(grp->vars,i);
        assert(var);
        if (var->ndims > 0)
        {
            if (!(dims_string = malloc(sizeof(char) * var->ndims * 4)))
                return NC_ENOMEM;
            strcpy(dims_string, "");
            for (d = 0; d < var->ndims; d++)
            {
                snprintf(temp_string, sizeof(temp_string), " %d", var->dimids[d]);
                strcat(dims_string, temp_string);
            }
        }
        if (!var->meta_read)
            strcat(storage_str, "unknown");
        else if (var->storage == NC_CONTIGUOUS)
            strcat(storage_str, "contiguous");
        else if (var->storage == NC_COMPACT)
            strcat(storage_str, "compact");
        else if (var->storage == NC_CHUNKED)
            strcat(storage_str, "chunked");
        else if (var->storage == NC_VIRTUAL)
            strcat(storage_str, "virtual");
        else
            strcat(storage_str, "unknown");
        LOG((2, "%s VARIABLE - varid: %d name: %s ndims: %d "
             "dimids:%s storage: %s", tabs, var->hdr.id, var->hdr.name,
             var->ndims,
             (dims_string ? dims_string : " -"), storage_str));
        for (j = 0; j < ncindexsize(var->att); j++)
        {
            att = (NC_ATT_INFO_T *)ncindexith(var->att, j);
            assert(att);
            LOG((2, "%s VAR ATTRIBUTE - attnum: %d name: %s type: %d len: %d",
                 tabs, att->hdr.id, att->hdr.name, att->nc_typeid, att->len));
        }
        if (dims_string)
            free(dims_string);
    }

    for (i = 0; i < ncindexsize(grp->type); i++)
    {
        type = (NC_TYPE_INFO_T*)ncindexith(grp->type, i);
        assert(type);
        LOG((2, "%s TYPE - nc_typeid: %d size: %d committed: %d name: %s",
             tabs, type->hdr.id, type->size, (int)type->committed, type->hdr.name));
        /* Is this a compound type? */
        if (type->nc_type_class == NC_COMPOUND)
        {
            int j;
            LOG((3, "compound type"));
            for (j = 0; j < nclistlength(type->u.c.field); j++)
            {
                field = (NC_FIELD_INFO_T *)nclistget(type->u.c.field, j);
                LOG((4, "field %s offset %d nctype %d ndims %d", field->hdr.name,
                     field->offset, field->nc_typeid, field->ndims));
            }
        }
        else if (type->nc_type_class == NC_VLEN)
        {
            LOG((3, "VLEN type"));
            LOG((4, "base_nc_type: %d", type->u.v.base_nc_typeid));
        }
        else if (type->nc_type_class == NC_OPAQUE)
            LOG((3, "Opaque type"));
        else if (type->nc_type_class == NC_ENUM)
        {
            LOG((3, "Enum type"));
            LOG((4, "base_nc_type: %d", type->u.e.base_nc_typeid));
        }
        else
        {
            LOG((0, "Unknown class: %d", type->nc_type_class));
            return NC_EBADTYPE;
        }
    }

    /* Call self for each child of this group. */
    for (i = 0; i < ncindexsize(grp->children); i++)
        if ((retval = rec_print_metadata((NC_GRP_INFO_T *)ncindexith(grp->children, i),
                                         tab_count + 1)))
            return retval;

    return NC_NOERR;
}

/**
 * @internal Print out the internal metadata for a file. This is
 * useful to check that netCDF is working! Nonetheless, this function
 * will print nothing if logging is not set to at least two.
 *
 * @param Pointer to the file info struct.
 *
 * @return ::NC_NOERR No error.
 * @author Ed Hartnett
 */
int
log_metadata_nc(NC_FILE_INFO_T *h5)
{
    LOG((2, "*** NetCDF-4 Internal Metadata: int_ncid 0x%x ext_ncid 0x%x",
         h5->root_grp->nc4_info->controller->int_ncid,
         h5->root_grp->nc4_info->controller->ext_ncid));
    if (!h5)
    {
        LOG((2, "This is a netCDF-3 file."));
        return NC_NOERR;
    }
    LOG((2, "FILE - path: %s cmode: 0x%x parallel: %d redef: %d "
         "fill_mode: %d no_write: %d next_nc_grpid: %d", h5->root_grp->nc4_info->controller->path,
         h5->cmode, (int)h5->parallel, (int)h5->redef, h5->fill_mode, (int)h5->no_write,
         h5->next_nc_grpid));
    if(nc_log_level >= 2)
        return rec_print_metadata(h5->root_grp, 0);
    return NC_NOERR;
}

#endif /*LOGGING */

/**
 * @internal Show the in-memory metadata for a netcdf file. This
 * function does nothing unless netCDF was built with
 * the configure option --enable-logging.
 *
 * @param ncid File and group ID.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @author Ed Hartnett
 */
int
NC4_show_metadata(int ncid)
{
    int retval = NC_NOERR;
#if LOGGING
    NC_FILE_INFO_T *h5;
    int old_log_level = nc_log_level;

    /* Find file metadata. */
    if ((retval = nc4_find_grp_h5(ncid, NULL, &h5)))
        return retval;

    /* Log level must be 2 to see metadata. */
    nc_log_level = 2;
    retval = log_metadata_nc(h5);
    nc_log_level = old_log_level;
#endif /*LOGGING*/
    return retval;
}

/**
 * @internal Define a binary searcher for reserved attributes
 * @param name for which to search
 * @return pointer to the matching NC_reservedatt structure.
 * @return NULL if not found.
 * @author Dennis Heimbigner
 */
const NC_reservedatt*
NC_findreserved(const char* name)
{
#if 0
    int n = NRESERVED;
    int L = 0;
    int R = (n - 1);

    for(;;) {
        if(L > R) break;
        int m = (L + R) / 2;
        const NC_reservedatt* p = &NC_reserved[m];
        int cmp = strcmp(p->name,name);
	if(cmp == 0) return p;
        if(cmp < 0)
            L = (m + 1);
        else /*cmp > 0*/
            R = (m - 1);
    }
    return NULL;
#else
    return (const NC_reservedatt*)bsearch(name,NC_reserved,NRESERVED,sizeof(NC_reservedatt),bincmp);
#endif
}

/* Ed Hartness requires this function */
static int
NC4_move_in_NCList(NC* nc, int new_id)
{
    int stat = move_in_NCList(nc,new_id);
    if(stat == NC_NOERR) {
        /* Synchronize header */
        if(nc->dispatchdata)
	    ((NC_OBJ*)nc->dispatchdata)->id = nc->ext_ncid;
    }
    return stat;
}

static int
sortcmp(const void* arg1, const void* arg2)
{
    NC_reservedatt* r1 = (NC_reservedatt*)arg1;
    NC_reservedatt* r2 = (NC_reservedatt*)arg2;
    return strcmp(r1->name,r2->name);
}

static int
bincmp(const void* arg1, const void* arg2)
{
    const char* name = (const char*)arg1;
    NC_reservedatt* ra = (NC_reservedatt*)arg2;
    return strcmp(name,ra->name);
}

void
NC_initialize_reserved(void)
{
    /* Guarantee the reserved attribute list is sorted */
    qsort((void*)NC_reserved,NRESERVED,sizeof(NC_reservedatt),sortcmp);
}
