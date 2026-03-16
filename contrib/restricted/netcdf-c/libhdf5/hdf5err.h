/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/
#ifndef HDF5ERR_H
#define HDF5ERR_H

#ifdef LOGGING

/* BAIL2 was moved here from nc_logging.h because
   it invokes nc_log_hdf5, and so is HDF5 specific.
   Also moved nc_log_hdf5 into hdf5internal.c
*/

/* To log based on error code, and set retval. */
/* Note that this can only be used in libhdf5 because of
   the call to nc_log_hdf5 */
#define BAIL2(e) \
   do { \
      retval = e; \
      LOG((0, "file %s, line %d.\n%s", __FILE__, __LINE__, nc_strerror(e))); \
      nc_log_hdf5(); \
   } while (0) 

#else /* LOGGING */

#define BAIL2(e) \
   do { \
      retval = e; \
   } while (0)

#endif /*LOGGING*/

#endif /*HDF5ERR_H*/
