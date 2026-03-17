/* Copyright 2018, UCAR/Unidata See netcdf/COPYRIGHT file for copying
 * and redistribution conditions.*/
/**
 * @file @internal This file handles the  *_varm()
 * functions for dispatch layers that need to return ::NC_ENOTNC3.
 *
 * @author Ed Hartnett
*/

#include "nc4dispatch.h"

/**
 * @internal This function only does anything for netcdf-3 files.
 *
 * @param ncid Ignored.
 * @param varid Ignored.
 * @param start Ignored.
 * @param edges Ignored.
 * @param stride Ignored.
 * @param imapp Ignored.
 * @param value0 Ignored.
 * @param memtype Ignored.
 *
 * @return ::NC_ENOTNC3 Not a netCDF classic format file.
 * @author Ed Hartnett
 */
int
NC_NOTNC3_get_varm(int ncid, int varid, const size_t *start,
                   const size_t *edges, const ptrdiff_t *stride,
                   const ptrdiff_t *imapp, void *value0, nc_type memtype)
{
    return NC_ENOTNC4;
}

/**
 * @internal This function only does anything for netcdf-3 files.
 *
 * @param ncid Ignored.
 * @param varid Ignored.
 * @param start Ignored.
 * @param edges Ignored.
 * @param stride Ignored.
 * @param imapp Ignored.
 * @param value0 Ignored.
 * @param memtype Ignored.
 *
 * @return ::NC_ENOTNC3 Not a netCDF classic format file.
 * @author Ed Hartnett
 */
int
NC_NOTNC3_put_varm(int ncid, int varid, const size_t * start,
                   const size_t *edges, const ptrdiff_t *stride,
                   const ptrdiff_t *imapp, const void *value0,
                   nc_type memtype)
{
    return NC_ENOTNC4;
}
