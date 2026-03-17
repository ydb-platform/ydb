/* Copyright 2022, University Corporation for Atmospheric Research.
 * See COPYRIGHT file for copying and redistribution conditions. */
/**
 * @file
 * @internal This function selects the best HDF5 file format options
 * to create netCDF-4 files that can be read and written by older
 * library versions.
 *
 * Format compatibility is transient, not baked in to an HDF5 file
 * at creation time.  Therefore the desired compatibility options
 * must be selected every time a file is opened for writing.
 *
 * This function should be called before every call to create a new
 * netCDF-4 file, or to open an existing netCDF-4 file for writing.
 * This function has no effect when opening a file for read only.
 *
 * This function should work correctly with all HDF5 library versions
 * from 1.8.0 through 1.13.0 and beyond, with no further changes.
 * This assumes that HDF5 versioning controls remain consistent
 * into the future.
 *
 * The basic functionality is to select the traditional HDF5 v1.8
 * format compatibility, whenever possible.  The less desirable
 * v1.6 compatibility is selected in a few strange cases when it is
 * not possible to select v1.8.
 *
 * Files created or updated with v1.10 and higher compatibility are
 * not legal netCDF-4 format, as of 2022 January.  They are not
 * readable by any netCDF library version linked with any HDF5 v1.8
 * or older library version.  However, it is possible for advanced
 * or experimental software to deliberately override these default
 * format settings, to create advanced format files for special
 * purposes.
 *
 * Files created with v1.6  compatibility have superblock version 0.
 * Files created with v1.8  compatibility have superblock version 2.
 * Files created with v1.10 compatibility have superblock version 3,
 * and are avoided by default.  Et cetera.
 *
 * The superblock version is locked in when a file is first created.
 * It is then possible to get a mix of v1.6 and v1.8 internal
 * object versions, when an existing netCDF-4 file is modified by
 * a different software version than the one that originally
 * created the file.  Mixed-object files of this nature are common
 * and do not suffer any serious problems.
 *
 * Also note that the THIRD function argument to H5Pset_libver_bounds
 * below, is set to "high = H5F_LIBVER_LATEST" in all cases.  This
 * setting does not affect object versions used by default in
 * netCDF-4.  Its purpose is for new or experimental code, to enable
 * the possible application of new HDF5 features such as SWMR,
 * without additional complication of this format control function.
 *
 * See netcdf-c github issues #250, #951, and PR #2176 for more
 * details about the rationale and evolution of netCDF-4 format
 * compatibility.
 *
 * See HDF5 documentation for H5Pset_libver_bounds and related RFC's,
 * for more details about HDF5 file object versioning.
 *
 * @author Dave Allured, NOAA/PSL/CIRES @date 2022 January 21
 */

#include "config.h"
#include "hdf5internal.h"

/**
 * @internal Function to set HDF5 file access options for backward
 * format compatibility.  Call this before every call to H5Fcreate
 * or H5Fopen.
 *
 * @param fapl_id Identifier for valid file access property list to
 *                be used in the next call to H5Fcreate or H5Fopen.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EHDFERR HDF5 returned error.
 *
 * @author Dave Allured, NOAA/PSL/CIRES @date 2022 January 11
 */
int
hdf5set_format_compatibility(hid_t fapl_id)
{
#if H5_VERSION_GE(1,10,2)
    /* lib versions 1.10.2 and higher */
    if (H5Pset_libver_bounds(fapl_id, H5F_LIBVER_V18, H5F_LIBVER_LATEST) < 0)

#else
#if H5_VERSION_GE(1,10,0)
    /* lib versions 1.10.0, 1.10.1 */
    if (H5Pset_libver_bounds(fapl_id, H5F_LIBVER_EARLIEST, H5F_LIBVER_LATEST) < 0)

#else
    /* all HDF5 1.8 lib versions */
    if (H5Pset_libver_bounds(fapl_id, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST) < 0)
#endif
#endif
        return NC_EHDFERR;    /* failure exit */

    return NC_NOERR;          /* normal exit */
}
