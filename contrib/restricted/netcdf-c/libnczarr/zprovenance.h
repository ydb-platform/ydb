/* Copyright 2018-2018 University Corporation for Atmospheric
   Research/Unidata. */
/**
 * @file
 * @internal Contains information for creating provenance
 * info and/or displaying provenance info.
 *
 * It has come to pass that we can't guarantee that this information is
 * contained only within netcdf4 files.  As a result, we need
 * to make printing these hidden attributes available to
 * netcdf3 as well.
 *
 *
 * For netcdf4 files, capture state information about the following:
 * - Global: netcdf library version
 * - Global: hdf5 library version
 * - Global: any parameters specified by the --with-ncproperties option for ./configure
 * - Per file: superblock version
 * - Per File: was it created by netcdf-4?
 * - Per file: _NCProperties attribute
 *
 * @author Dennis Heimbigner, Ward Fisher
 *
 * [This file is too hdf5 specific, need to clean so we can use with Zarr]
*/

#ifndef ZPROVENANCE_H
#define ZPROVENANCE_H

#include "nc_provenance.h"

#define NCPZARRLIB "zarr"

/* Forward */
struct NC_FILE_INFO;

/**************************************************/
/**
   For netcdf4 files, capture state information about the following:
   1. Global: netcdf library version
   2. Global: hdf5 library version
   3. Per file: superblock version
   4. Per File: was it created by netcdf-4?
   5. Per file: _NCProperties attribute
*/

/* Provenance Management (moved from nc4internal.h) */

/* Initialize the provenance global state */
extern int NCZ_provenance_init(void);

/* Finalize the provenance global state */
extern int NCZ_provenance_finalize(void);

/* Read and store the provenance from an existing file;
   Note: signature differs from libhdf5 because
   this will get called when the root group attributes are read
*/
extern int NCZ_read_provenance(struct NC_FILE_INFO* file, const char* name, const char* value);

/* Write the provenance into a newly created file */
extern int NCZ_write_provenance(struct NC_FILE_INFO* file);

/* Create the provenance for a newly created file */
extern int NCZ_new_provenance(struct NC_FILE_INFO* file);

/* Clean up the provenance info in a file */
extern int NCZ_clear_provenance(NC4_Provenance* prov);

#endif /* ZPROVENANCE_H */
