/* Copyright 2003-2018, University Corporation for Atmospheric
 * Research. See COPYRIGHT file for copying and redistribution
 * conditions. */
/**
 * @file
 * @internal This file contains functions that are used in file
 * opens.
 *
 * @author Dennis Heimbigner, Ed Hartnett
 */

#include "zincludes.h"
#include "ncmodel.h"

#define NUM_TYPES 12 /**< Number of netCDF atomic types. */
#define CD_NELEMS_ZLIB 1 /**< Number of parameters needed for filter. */

/** @internal These flags may not be set for open mode. */
static const int ILLEGAL_OPEN_FLAGS = (NC_MMAP|NC_DISKLESS|NC_64BIT_OFFSET|NC_CDF5);

/* Forward */

/**
 * @internal Check for the attribute that indicates that netcdf
 * classic model is in use.
 *
 * @param root_grp pointer to the group info for the root group of the
 * @param is_classic store 1 if this is a classic file.
 * file.
 *
 * @return NC_NOERR No error.
 * @author Dennis Heimbigner, Ed Hartnett
 */
static int
check_for_classic_model(NC_GRP_INFO_T *root_grp, int *is_classic)
{
    int attr_exists = 0;
    /* Check inputs. */
    assert(root_grp && root_grp->format_grp_info && !root_grp->parent
           && is_classic);

    *is_classic = attr_exists ? 1 : 0;

    return NC_NOERR;
}

/**
 * @internal Open a netcdf-4 file. Things have already been kicked off
 * in ncfunc.c in nc_open, but here the netCDF-4 part of opening a
 * file is handled.
 *
 * @param path The file name of the new file.
 * @param mode The open mode flag.
 * @param fraglist uri fragment list in envv form
 * @param nc Pointer to NC file info.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_ENOMEM Out of memory.
 * @return ::NC_EINTERNAL Internal list error.
 * @return ::NC_EHDFERR HDF error.
 * @return ::NC_EMPI MPI error for parallel.
 * @return ::NC_EPARINIT Parallel I/O initialization error.
 * @return ::NC_EINMEMMORY Memory file error.
 * @author Dennis Heimbigner, Ed Hartnett
 */
static int
ncz_open_file(const char *path, int mode, NClist* controls, int ncid)
{
    int stat = NC_NOERR;
    NC_FILE_INFO_T *h5 = NULL;
    int is_classic;
    NC* nc = NULL;

    LOG((3, "%s: path %s mode %d", __func__, path, mode));
    assert(path);

    ZTRACE(2,"path=%s,mode=%d,ncid=%d,controls=%s)",path,mode,ncid,(controls?nczprint_envv(controls):"null"));

    /* Convert ncid to an NC* structure pointer */
    if((stat = NC_check_id(ncid,&nc))) goto exit;

    /* Add necessary structs to hold netcdf-4 file data;
       will define the NC_FILE_INFO_T for the file
       and the NC_GRP_INFO_T for the root group. */
    if ((stat = nc4_nc4f_list_add(nc, path, mode)))
        goto exit;
    h5 = (NC_FILE_INFO_T*)nc->dispatchdata;
    assert(h5 && h5->root_grp);

    h5->mem.inmemory = ((mode & NC_INMEMORY) == NC_INMEMORY);
    h5->mem.diskless = ((mode & NC_DISKLESS) == NC_DISKLESS);
    h5->mem.persist = ((mode & NC_PERSIST) == NC_PERSIST);

    /* Does the mode specify that this file is read-only? */
    if ((mode & NC_WRITE) == 0)
	h5->no_write = NC_TRUE;

    /* Setup zarr state */
    if((stat = ncz_open_dataset(h5,controls)))
	goto exit;

    /* Now read in all the metadata. Some types
     * information may be difficult to resolve here, if, for example, a
     * dataset of user-defined type is encountered before the
     * definition of that type. */
    if((stat = ncz_read_file(h5)))
       goto exit;

    /* We must read in the attributes of the root group to get
       e.g. provenance and classic model attribute */
    if((stat = ncz_read_atts(h5,(NC_OBJ*)h5->root_grp))) goto exit;

    /* Check for classic model attribute. */
    if ((stat = check_for_classic_model(h5->root_grp, &is_classic)))
       goto exit;
    if (is_classic)
       h5->cmode |= NC_CLASSIC_MODEL;

#ifdef LOGGING
    /* This will print out the names, types, lens, etc of the vars and
       atts in the file, if the logging level is 2 or greater. */
    log_metadata_nc(h5);
#endif

exit:
    if (stat && h5)
	ncz_closeorabort(h5, NULL, 1); /*  treat like abort*/
    return ZUNTRACE(stat);
}

/**
 * @internal Open a netCDF-4 file.
 *
 * @param path The file name of the new file.
 * @param mode The open mode flag.
 * @param basepe Ignored by this function.
 * @param chunksizehintp Ignored by this function.
 * @param parameters pointer to struct holding extra data (e.g. for parallel I/O)
 * layer. Ignored if NULL.
 * @param dispatch Pointer to the dispatch table for this file.
 * @param nc_file Pointer to an instance of NC.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EINVAL Invalid inputs.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_open(const char *path, int mode, int basepe, size_t *chunksizehintp,
         void *parameters, const NC_Dispatch *dispatch, int ncid)
{
    int stat = NC_NOERR;
    NCURI* uri = NULL;

    ZTRACE(0,"path=%s,mode=%d,ncid=%d)",path,mode,ncid);

    NC_UNUSED(parameters);

    assert(path && dispatch);

    LOG((1, "%s: path %s mode %d ",
         __func__, path, mode));

    /* Check the mode for validity */
    if (mode & ILLEGAL_OPEN_FLAGS)
        {stat = NC_EINVAL; goto done;}

    if((mode & NC_DISKLESS) && (mode & NC_INMEMORY))
        {stat = NC_EINVAL; goto done;}

    /* If this is our first file, initialize NCZ. */
    if (!ncz_initialized) NCZ_initialize();

#ifdef LOGGING
    /* If nc logging level has changed, see if we need to turn on
     * NCZ's error messages. */
    NCZ_set_log_level();
#endif /* LOGGING */

    /* Get the controls */
    ncuriparse(path,&uri);
    if(uri == NULL) goto done;

    /* Open the file. */
    if((stat = ncz_open_file(path, mode, ncurifragmentparams(uri), ncid)))
	goto done;

done:
    ncurifree(uri);
    return ZUNTRACE(stat);
}

