/** \file
Error messages and library version.

These functions return the library version, and error messages.

Copyright 2018 University Corporation for Atmospheric
Research/Unidata. See COPYRIGHT file for more info.
*/

#include "config.h"
#include "ncdispatch.h"
#ifdef USE_PNETCDF
#error #include <pnetcdf.h>  /* for ncmpi_strerror() */
#endif

/** @internal The version string for the library, used by
 * nc_inq_libvers(). */
static const char nc_libvers[] = PACKAGE_VERSION " of "__DATE__" "__TIME__" $";

/**
Return the library version.

\returns short string that contains the version information for the
library.
 */
const char *
nc_inq_libvers(void)
{
   return nc_libvers;
}

/*! NetCDF Error Handling

\addtogroup error NetCDF Error Handling

NetCDF functions return a non-zero status codes on error.

Each netCDF function returns an integer status value. If the returned
status value indicates an error, you may handle it in any way desired,
from printing an associated error message and exiting to ignoring the
error indication and proceeding (not recommended!). For simplicity,
the examples in this guide check the error status and call a separate
function, handle_err(), to handle any errors. One possible definition
of handle_err() can be found within the documentation of
nc_strerror().

The nc_strerror() function is available to convert a returned integer
error status into an error message string.

Occasionally, low-level I/O errors may occur in a layer below the
netCDF library. For example, if a write operation causes you to exceed
disk quotas or to attempt to write to a device that is no longer
available, you may get an error from a layer below the netCDF library,
but the resulting write error will still be reflected in the returned
status value.

*/

/** \{ */

/*! Given an error number, return an error message.

This function returns a static reference to an error message string
corresponding to an integer netCDF error status or to a system error
number, presumably returned by a previous call to some other netCDF
function. The error codes are defined in netcdf.h.

\param ncerr1 error number

\returns short string containing error message.

Here is an example of a simple error handling function that uses
nc_strerror() to print the error message corresponding to the netCDF
error status returned from any netCDF function call and then exit:

\code
     #include <netcdf.h>
        ...
     void handle_error(int status) {
     if (status != NC_NOERR) {
        fprintf(stderr, "%s\n", nc_strerror(status));
        exit(-1);
        }
     }
\endcode
*/
const char *nc_strerror(int ncerr1)
{
   /* System error? */
   if(NC_ISSYSERR(ncerr1))
   {
      const char *cp = (const char *) strerror(ncerr1);
      if(cp == NULL)
	 return "Unknown Error";
      return cp;
   }

   /* If we're here, this is a netcdf error code. */
   switch(ncerr1)
   {
      case NC_NOERR:
	 return "No error";
      case NC_EBADID:
	 return "NetCDF: Not a valid ID";
      case NC_ENFILE:
	 return "NetCDF: Too many files open";
      case NC_EEXIST:
	 return "NetCDF: File exists && NC_NOCLOBBER";
      case NC_EINVAL:
	 return "NetCDF: Invalid argument";
      case NC_EPERM:
	 return "NetCDF: Write to read only";
      case NC_ENOTINDEFINE:
	 return "NetCDF: Operation not allowed in data mode";
      case NC_EINDEFINE:
	 return "NetCDF: Operation not allowed in define mode";
      case NC_EINVALCOORDS:
	 return "NetCDF: Index exceeds dimension bound";
      case NC_EMAXDIMS:
	 return "NetCDF: NC_MAX_DIMS exceeded"; /* not enforced after 4.5.0 */
      case NC_ENAMEINUSE:
	 return "NetCDF: String match to name in use";
      case NC_ENOTATT:
	 return "NetCDF: Attribute not found";
      case NC_EMAXATTS:
	 return "NetCDF: NC_MAX_ATTRS exceeded"; /* not enforced after 4.5.0 */
      case NC_EBADTYPE:
	 return "NetCDF: Not a valid data type or _FillValue type mismatch";
      case NC_EBADDIM:
	 return "NetCDF: Invalid dimension ID or name";
      case NC_EUNLIMPOS:
	 return "NetCDF: NC_UNLIMITED in the wrong index";
      case NC_EMAXVARS:	 return "NetCDF: NC_MAX_VARS exceeded"; /* not enforced after 4.5.0 */
      case NC_ENOTVAR:
	 return "NetCDF: Variable not found";
      case NC_EGLOBAL:
	 return "NetCDF: Action prohibited on NC_GLOBAL varid";
      case NC_ENOTNC:
	 return "NetCDF: Unknown file format";
      case NC_ESTS:
	 return "NetCDF: In Fortran, string too short";
      case NC_EMAXNAME:
	 return "NetCDF: NC_MAX_NAME exceeded";
      case NC_EUNLIMIT:
	 return "NetCDF: NC_UNLIMITED size already in use";
      case NC_ENORECVARS:
	 return "NetCDF: nc_rec op when there are no record vars";
      case NC_ECHAR:
	 return "NetCDF: Attempt to convert between text & numbers";
      case NC_EEDGE:
	 return "NetCDF: Start+count exceeds dimension bound";
      case NC_ESTRIDE:
	 return "NetCDF: Illegal stride";
      case NC_EBADNAME:
	 return "NetCDF: Name contains illegal characters";
      case NC_ERANGE:
	 return "NetCDF: Numeric conversion not representable";
      case NC_ENOMEM:
	 return "NetCDF: Memory allocation (malloc) failure";
      case NC_EVARSIZE:
	 return "NetCDF: One or more variable sizes violate format constraints";
      case NC_EDIMSIZE:
	 return "NetCDF: Invalid dimension size";
      case NC_ETRUNC:
	 return "NetCDF: File likely truncated or possibly corrupted";
      case NC_EAXISTYPE:
	 return "NetCDF: Illegal axis type";
      case NC_EDAP:
	 return "NetCDF: DAP failure";
      case NC_ECURL:
	 return "NetCDF: libcurl failure";
      case NC_EIO:
	 return "NetCDF: I/O failure";
      case NC_ENODATA:
	 return "NetCDF: Variable has no data";
      case NC_EDAPSVC:
	 return "NetCDF: DAP server error";
      case NC_EDAS:
	 return "NetCDF: Malformed or inaccessible DAP DAS";
      case NC_EDDS:
	 return "NetCDF: Malformed or inaccessible DAP2 DDS or DAP4 DMR response";
      case NC_EDATADDS:
	 return "NetCDF: Malformed or inaccessible DAP2 DATADDS or DAP4 DAP response";
      case NC_EDAPURL:
	 return "NetCDF: Malformed URL";
      case NC_EDAPCONSTRAINT:
	 return "NetCDF: Malformed or unexpected Constraint";
      case NC_ETRANSLATION:
	 return "NetCDF: Untranslatable construct";
      case NC_EACCESS:
	 return "NetCDF: Access failure";
      case NC_EAUTH:
	 return "NetCDF: Authorization failure";
      case NC_ENOTFOUND:
	 return "NetCDF: file not found";
      case NC_ECANTREMOVE:
	 return "NetCDF: cannot delete file";
      case NC_EINTERNAL:
	 return "NetCDF: internal library error; Please contact Unidata support";
      case NC_EPNETCDF:
	 return "NetCDF: PnetCDF error";
      case NC_EHDFERR:
	 return "NetCDF: HDF error";
      case NC_ECANTREAD:
	 return "NetCDF: Can't read file";
      case NC_ECANTWRITE:
	 return "NetCDF: Can't write file";
      case NC_ECANTCREATE:
	 return "NetCDF: Can't create file";
      case NC_EFILEMETA:
	 return "NetCDF: Can't add HDF5 file metadata";
      case NC_EDIMMETA:
	 return "NetCDF: Can't define dimensional metadata";
      case NC_EATTMETA:
	 return "NetCDF: Can't open HDF5 attribute";
      case NC_EVARMETA:
	 return "NetCDF: Problem with variable metadata.";
      case NC_ENOCOMPOUND:
	 return "NetCDF: Can't create HDF5 compound type";
      case NC_EATTEXISTS:
	 return "NetCDF: Attempt to create attribute that already exists";
      case NC_ENOTNC4:
	 return "NetCDF: Attempting netcdf-4 operation on netcdf-3 file";
      case NC_ESTRICTNC3:
	 return "NetCDF: Attempting netcdf-4 operation on strict nc3 netcdf-4 file";
      case NC_ENOTNC3:
	 return "NetCDF: Attempting netcdf-3 operation on netcdf-4 file";
      case NC_ENOPAR:
	 return "NetCDF: Parallel operation on file opened for non-parallel access";
      case NC_EPARINIT:
	 return "NetCDF: Error initializing for parallel access";
      case NC_EBADGRPID:
	 return "NetCDF: Bad group ID";
      case NC_EBADTYPID:
	 return "NetCDF: Bad type ID";
      case NC_ETYPDEFINED:
	 return "NetCDF: Type has already been defined and may not be edited";
      case NC_EBADFIELD:
	 return "NetCDF: Bad field ID";
      case NC_EBADCLASS:
	 return "NetCDF: Bad class";
      case NC_EMAPTYPE:
	 return "NetCDF: Mapped access for atomic types only";
      case NC_ELATEFILL:
	 return "NetCDF: Attempt to define fill value when data already exists.";
      case NC_ELATEDEF:
	 return "NetCDF: Attempt to define var properties, like deflate, after enddef.";
      case NC_EDIMSCALE:
	 return "NetCDF: Problem with HDF5 dimscales.";
      case NC_ENOGRP:
	 return "NetCDF: No group found.";
      case NC_ESTORAGE:
	 return "NetCDF: Cannot specify both contiguous and chunking.";
      case NC_EBADCHUNK:
	 return "NetCDF: Bad chunk sizes.";
      case NC_ENOTBUILT:
	 return "NetCDF: Attempt to use feature that was not turned on "
	    "when netCDF was built.";
      case NC_EDISKLESS:
	 return "NetCDF: Error in using diskless access";
      case NC_EFILTER:
	 return "NetCDF: Filter error: bad id or parameters or duplicate filter";
      case NC_ENOFILTER:
	 return "NetCDF: Filter error: undefined filter encountered";
      case NC_ECANTEXTEND:
	return "NetCDF: Attempt to extend dataset during NC_INDEPENDENT I/O operation. Use nc_var_par_access to set mode NC_COLLECTIVE before extending variable.";
      case NC_EMPI: return "NetCDF: MPI operation failed.";
      case NC_ERCFILE:
	return "NetCDF: RC File Failure.";
      case NC_ENULLPAD:
       return "NetCDF: File fails strict Null-Byte Header check.";
      case NC_EINMEMORY:
       return "NetCDF: In-memory File operation failed.";
      case NC_ENCZARR:
	 return "NetCDF: NCZarr error";
      case NC_ES3:
	 return "NetCDF: S3 error";
      case NC_EEMPTY:
	 return "NetCDF: Attempt to read empty NCZarr map key";
      case NC_EOBJECT:
	 return "NetCDF: Some object exists when it should not";
      case NC_ENOOBJECT:
	 return "NetCDF: Some object not found";
      case NC_EPLUGIN:
	 return "NetCDF: Unclassified failure in accessing a dynamically loaded plugin";
     default:
#ifdef USE_PNETCDF
        /* The behavior of ncmpi_strerror here is to return
           NULL, not a string.  This causes problems in (at least)
           the fortran interface. */
        return (ncmpi_strerror(ncerr1) ?
                ncmpi_strerror(ncerr1) :
                "Unknown Error");
#else
	 return "Unknown Error";
#endif
   }
}

/** \} */
