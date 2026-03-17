/* Copyright 2003-2018, University Corporation for Atmospheric
 * Research. See COPYRIGHT file for copying and redistribution
 * conditions. */

/**
 * @file
 * @internal The netCDF-4 file functions relating to file creation.
 *
 * @author Dennis Heimbigner, Ed Hartnett
 */

#include "zincludes.h"

/** @internal These flags may not be set for create. */
static const int ILLEGAL_CREATE_FLAGS = (NC_NOWRITE|NC_MMAP|NC_DISKLESS|NC_64BIT_OFFSET|NC_CDF5);

/**
 * @internal Create a netCDF-4/NCZ file.
 *
 * @param path The file name of the new file.
 * @param cmode The creation mode flag.
 * @param initialsz The proposed initial file size (advisory)
 * @param nc Pointer to an instance of NC.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EINVAL Invalid input (check cmode).
 * @return ::NC_EEXIST File exists and NC_NOCLOBBER used.
 * @return ::NC_EHDFERR ZARR returned error.
 * @ingroup netcdf4
 * @author Dennis Heimbigner, Ed Hartnett
 */
static int
ncz_create_file(const char *path, int cmode, size_t initialsz, NClist* controls, int ncid)
{
    int retval = NC_NOERR;
    NC_FILE_INFO_T* h5 = NULL;

    assert(path);
    LOG((3, "%s: path %s mode 0x%x", __func__, path, cmode));

    /* Add necessary structs to hold netcdf-4 file data. */
    if ((retval = nc4_file_list_add(ncid, path, cmode, (void**)&h5)))
        BAIL(retval);
    assert(h5 && h5->root_grp);
    h5->root_grp->atts_read = 1;

    h5->mem.inmemory = ((cmode & NC_INMEMORY) == NC_INMEMORY);
    h5->mem.diskless = ((cmode & NC_DISKLESS) == NC_DISKLESS);
    h5->mem.persist = ((cmode & NC_PERSIST) == NC_PERSIST);

    /* Do format specific setup */

    if((retval = ncz_create_dataset(h5,h5->root_grp,controls)))
	BAIL(retval);

    /* Define mode gets turned on automatically on create. */
    h5->flags |= NC_INDEF;

    /* Set provenance. */
    if ((retval = NCZ_new_provenance(h5)))
        BAIL(retval);

    return NC_NOERR;

exit: /*failure exit*/
    if(retval && h5)
        ncz_closeorabort(h5, NULL, 1); /* treat like abort */
    return retval;
}

/**
 * @internal Create a netCDF-4/NCZ file.
 *
 * @param path The file name of the new file.
 * @param cmode The creation mode flag.
 * @param initialsz Ignored by this function.
 * @param basepe Ignored by this function.
 * @param chunksizehintp Ignored by this function.
 * @param parameters pointer to struct holding extra data (e.g. for
 * parallel I/O) layer. Ignored if NULL.
 * @param dispatch Pointer to the dispatch table for this file.
 * @param nc_file Pointer to an instance of NC.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EINVAL Invalid input (check cmode).
 * @ingroup netcdf4
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_create(const char* path, int cmode, size_t initialsz, int basepe,
           size_t *chunksizehintp, void *parameters,
           const NC_Dispatch *dispatch, int ncid)
{
    int stat = NC_NOERR;
    NCURI* uri = NULL;

    ZTRACE(0,"path=%s,cmode=%d,initialsz=%ld,ncid=%d)",path,cmode,initialsz,ncid);
    
    NC_UNUSED(parameters);

    assert(path);

    LOG((1, "%s: path %s cmode 0x%x ncid %d",
         __func__, path, cmode ,ncid));

    /* If this is our first file, initialize */
    if (!ncz_initialized) NCZ_initialize();

#ifdef LOGGING
    /* If nc logging level has changed, see if we need to turn on
     * NCZ's error messages. */
    NCZ_set_log_level();
#endif /* LOGGING */

    /* Check the cmode for validity. */
    if((cmode & ILLEGAL_CREATE_FLAGS) != 0)
        {stat = NC_EINVAL; goto done;}

    /* Turn on NC_WRITE */
    cmode |= NC_WRITE;
    
    /* Get the controls */
    ncuriparse(path,&uri);
    if(uri == NULL) goto done;

    /* Create the file */
   stat = ncz_create_file(path, cmode, initialsz, ncurifragmentparams(uri), ncid);

done:
    ncurifree(uri);
    return ZUNTRACE(stat);
}
