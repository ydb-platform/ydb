/*
 *      Copyright 2022, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */
/**
 * @file
 * @internal
 *
 * This file contains functions that work with the NC struct. There is
 * an NC struct for every open netCDF file.
 *
 * @author Glenn Davis
 */

#include "config.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include "ncdispatch.h"

#ifndef nulldup
 #define nulldup(x) ((x)?strdup(x):(x))
#endif

/** This is the default create format for nc_create and nc__create. */
static int default_create_format = NC_FORMAT_CLASSIC;

/**
 * Find the NC struct for an open file, using the ncid.
 *
 * @param ncid The ncid to find.
 * @param ncpp Pointer that gets a pointer to the NC.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID ncid not found.
 * @author Glenn Davis, Dennis Heimbigner
 */
int
NC_check_id(int ncid, NC** ncpp)
{
    NC* nc = find_in_NCList(ncid);
    if(nc == NULL) return NC_EBADID;
    if(ncpp) *ncpp = nc;
    return NC_NOERR;
}

/**
 * Free an NC struct and its related resources. Before this is done,
 * be sure to remove the NC from the open file list with
 * del_from_NCList().
 *
 * @param ncp Pointer to the NC struct to be freed.
 *
 * @author Glenn Davis, Dennis Heimbigner
 */
void
free_NC(NC *ncp)
{
    if(ncp == NULL)
        return;
    if(ncp->path)
        free(ncp->path);
    /* We assume caller has already cleaned up ncp->dispatchdata */
    free(ncp);
}

/**
 * Create and initialize a new NC struct. The ncid is assigned later.
 *
 * @param dispatcher An pointer to the NC_Dispatch table that should
 * be used by this NC.
 * @param path The name of the file.
 * @param mode The open or create mode.
 * @param model An NCmodel instance, provided by NC_infermodel().
 * @param ncpp A pointer that gets a pointer to the newlly allocacted
 * and initialized NC struct.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_ENOMEM Out of memory.
 * @author Glenn Davis, Dennis Heimbigner
 */
int
new_NC(const NC_Dispatch* dispatcher, const char* path, int mode, NC** ncpp)
{
    NC *ncp = (NC*)calloc(1,sizeof(NC));
    if(ncp == NULL) return NC_ENOMEM;
    ncp->dispatch = dispatcher;
    ncp->path = nulldup(path);
    ncp->mode = mode;
    if(ncp->path == NULL) { /* fail */
        free_NC(ncp);
        return NC_ENOMEM;
    }
    if(ncpp) {
        *ncpp = ncp;
    } else {
        free_NC(ncp);
    }
    return NC_NOERR;
}

/**
 * This function sets a default create flag that will be logically
 *  or'd to whatever flags are passed into nc_create for all future
 *  calls to nc_create.
 *
 * @param format The format that should become the default.
 * @param old_formatp Pointer that gets the previous default. Ignored
 * if NULL.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_ENOTBUILD Requested format not built with this install.
 * @return ::NC_EINVAL Invalid input.
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
nc_set_default_format(int format, int *old_formatp)
{
    /* Return existing format if desired. */
    if (old_formatp)
        *old_formatp = default_create_format;

    /* Make sure only valid format is set. */
#ifndef NETCDF_ENABLE_CDF5
    if (format == NC_FORMAT_CDF5)
        return NC_ENOTBUILT;
#endif
#ifdef USE_HDF5
    if (format != NC_FORMAT_CLASSIC && format != NC_FORMAT_64BIT_OFFSET &&
        format != NC_FORMAT_NETCDF4 && format != NC_FORMAT_NETCDF4_CLASSIC &&
        format != NC_FORMAT_CDF5)
        return NC_EINVAL;
#else
    if (format == NC_FORMAT_NETCDF4 || format == NC_FORMAT_NETCDF4_CLASSIC)
        return NC_ENOTBUILT;
    if (format != NC_FORMAT_CLASSIC && format != NC_FORMAT_64BIT_OFFSET &&
        format != NC_FORMAT_CDF5)
        return NC_EINVAL;
#endif
    default_create_format = format;
    return NC_NOERR;
}

/**
 * Get the current default format.
 *
 * @return the default format.
 * @author Ed Hartnett
 */
int
nc_get_default_format(void)
{
    return default_create_format;
}
