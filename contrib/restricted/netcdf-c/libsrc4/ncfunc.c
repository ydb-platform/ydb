/**
 * @internal
 *
 * Copyright 2018, University Corporation for Atmospheric
 * Research. See netcdf-4/docs/COPYRIGHT file for copying and
 * redistribution conditions.
 *
 * This file is part of netcdf-4, a netCDF-like interface for HDF5, or a
 * HDF5 backend for netCDF, depending on your point of view.
 *
 * This file handles the inq_format functions.
 *
 * @author Ed Hartnett, Dennis Heimbigner
 */

#include "nc4internal.h"
#include "nc4dispatch.h"

/**
 * @internal Get the format (i.e. NC_FORMAT_NETCDF4 pr
 * NC_FORMAT_NETCDF4_CLASSIC) of an open netCDF-4 file.
 *
 * @param ncid File ID (ignored).
 * @param formatp Pointer that gets the constant indicating format.

 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @author Ed Hartnett
 */
int
NC4_inq_format(int ncid, int *formatp)
{
    NC_FILE_INFO_T *nc4_info;
    int retval;

    LOG((2, "nc_inq_format: ncid 0x%x", ncid));

    if (!formatp)
        return NC_NOERR;

    /* Find the file metadata. */
    if ((retval = nc4_find_nc_grp_h5(ncid, NULL, NULL, &nc4_info)))
        return retval;

    /* Check if classic NC3 rules are in effect for this file. */
    if (nc4_info->cmode & NC_CLASSIC_MODEL)
        *formatp = NC_FORMAT_NETCDF4_CLASSIC;
    else
        *formatp = NC_FORMAT_NETCDF4;

    return NC_NOERR;
}

/**
 * @internal Return the extended format (i.e. the dispatch model),
 * plus the mode associated with an open file.
 *
 * @param ncid File ID (ignored).
 * @param formatp a pointer that gets the extended format. Note that
 * this is not the same as the format provided by nc_inq_format(). The
 * extended format indicates the dispatch layer model. NetCDF-4 files
 * will always get NC_FORMATX_NC4.
 * @param modep a pointer that gets the open/create mode associated with
 * this file. Ignored if NULL.

 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @author Dennis Heimbigner
 */
int
NC4_inq_format_extended(int ncid, int *formatp, int *modep)
{
    NC *nc;
    int retval;

    LOG((2, "%s: ncid 0x%x", __func__, ncid));

    if ((retval = nc4_find_nc_grp_h5(ncid, &nc, NULL, NULL)))
        return NC_EBADID;

    if(modep)
        *modep = nc->mode|NC_NETCDF4;

    if (formatp)
        *formatp = NC_FORMATX_NC_HDF5;

    return NC_NOERR;
}
