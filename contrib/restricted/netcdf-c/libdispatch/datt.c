/*Copyright 2018 University Corporation for Atmospheric
  Research/Unidata. See copyright file for more info. */
/**
 * @file
 *
 * These functions in this file rename and delete attributes.
*/

#include "config.h"
#include "ncdispatch.h"

/**
 * @defgroup attributes Attributes
 *
 * Attributes hold metadata about data and files.
 *
 * @image html ncatts.png "Attributes store metadata."
 *
 * Attributes may be associated with a netCDF variable to specify such
 * properties as units, special values, maximum and minimum valid
 * values, scaling factors, and offsets.
 *
 * It is also possible to have attributes that are not associated with
 * any variable. These are called global attributes and are identified
 * by using ::NC_GLOBAL as a variable pseudo-ID. Global attributes are
 * related to the netCDF dataset as a whole and may be used for
 * purposes such as providing a title or processing history for a
 * netCDF dataset. In netCDF-4/HDF5 files, global attributes are
 * associated with a hierarchical group.
 *
 * An attribute is designated by its variable ID and name. When an
 * attribute name is not known, it may be designated by its variable
 * ID and number in order to determine its name, using the function
 * nc_inq_attname().
 *
 * Operations supported on attributes are:
 * - Create an attribute, given its variable ID, name, data type,
 *   length, and value.
 * - Get attribute's data type and length from its variable ID and
     name.
 * - Get attribute's value from its variable ID and name.
 * - Copy attribute from one netCDF variable to another.
 * - Get name of attribute from its number.
 * - Rename an attribute.
 * - Delete an attribute.
*/

/** @{*/

/**
 * @name Deleting and Renaming Attributes
 *
 * Functions to delete or rename an attribute. */
/**@{*/  /* Start doxygen member group. */

/**
 * Rename an attribute.
 *
 * The function nc_rename_att() changes the name of an attribute. In
 * classic formats, if the new name is longer than the original name,
 * the netCDF dataset must be in define mode. In netCDF-4/HDF5 files,
 * attributes may be renamed at any time. You cannot rename an
 * attribute to have the same name as another attribute of the same
 * variable.
 *
 * @param ncid NetCDF or group ID, from a previous call to nc_open(),
 * nc_create(), nc_def_grp(), or associated inquiry functions such as
 * nc_inq_ncid().
 * @param varid Variable ID of the attribute's variable, or
 * ::NC_GLOBAL for a global attribute.
 * @param name Attribute \ref object_name.
 * @param newname The new attribute \ref object_name.
 *
 * <h1>Example</h1>
 *
 * Here is an example using nc_rename_att() to rename the variable
 * attribute units to Units for a variable rh in an existing netCDF
 * dataset named foo.nc:
 *
 @code
     #include <netcdf.h>
        ...
     int  status;
     int  ncid;
     int  rh_id;
        ...
     status = nc_open("foo.nc", NC_NOWRITE, &ncid);
     if (status != NC_NOERR) handle_error(status);
        ...
     status = nc_inq_varid (ncid, "rh", &rh_id);
     if (status != NC_NOERR) handle_error(status);
        ...
     status = nc_rename_att(ncid, rh_id, "units", "Units");
     if (status != NC_NOERR) handle_error(status);
 @endcode

 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOTVAR Bad varid.
 * @return ::NC_EBADNAME Bad name.
 * @return ::NC_EMAXNAME New name too long.
 * @return ::NC_EINVAL Name or new name not provided.
 * @return ::NC_ENAMEINUSE Name already in use.
 * @return ::NC_EPERM File was opened read only.
 * @return ::NC_ENOTINDEFINE File is not in define mode.
 * @return ::NC_ENOTATT Attribute not found.
 * @return ::NC_EHDFERR Failure at HDF5 layer.
 * @return ::NC_ENOMEM Out of memory.
 *
 * @author Glenn Davis, Ed Hartnett, Dennis Heimbigner
 */
int
nc_rename_att(int ncid, int varid, const char *name, const char *newname)
{
   NC* ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   TRACE(nc_rename_att);
   return ncp->dispatch->rename_att(ncid, varid, name, newname);
}

/**
 * Delete an attribute.
 *
 * The function nc_del_att() deletes a netCDF attribute from an open
 * netCDF dataset. For classic netCDF formats, the dataset must be in
 * define mode to delete an attribute. In netCDF-4/HDF5 files,
 * attributes may be deleted at any time.
 *
 * @param ncid NetCDF or group ID, from a previous call to nc_open(),
 * nc_create(), nc_def_grp(), or associated inquiry functions such as
 * nc_inq_ncid().
 * @param varid Variable ID of the attribute's variable, or
 * ::NC_GLOBAL for a global attribute.
 * @param name Attribute name.
 *
 * <h1>Example</h1>
 *
 * Here is an example using nc_del_att() to delete the variable
 * attribute Units for a variable rh in an existing netCDF dataset
 * named foo.nc:

@code
     #include <netcdf.h>
        ...
     int  status;
     int  ncid;
     int  rh_id;
        ...
     status = nc_open("foo.nc", NC_WRITE, &ncid);
     if (status != NC_NOERR) handle_error(status);
        ...
     status = nc_inq_varid (ncid, "rh", &rh_id);
     if (status != NC_NOERR) handle_error(status);
        ...
     status = nc_redef(ncid);
     if (status != NC_NOERR) handle_error(status);
     status = nc_del_att(ncid, rh_id, "Units");
     if (status != NC_NOERR) handle_error(status);
     status = nc_enddef(ncid);
     if (status != NC_NOERR) handle_error(status);
@endcode

 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOTVAR Bad varid.
 * @return ::NC_EBADNAME Bad name.
 * @return ::NC_EINVAL Name not provided.
 * @return ::NC_EPERM File was opened read only.
 * @return ::NC_ENOTINDEFINE File is not in define mode.
 * @return ::NC_ENOTATT Attribute not found.
 * @return ::NC_EATTMETA Failure at HDF5 layer.
 *
 * @author Glenn Davis, Ed Hartnett, Dennis Heimbigner
 */
int
nc_del_att(int ncid, int varid, const char *name)
{
   NC* ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   TRACE(nc_del_att);
   stat = ncp->dispatch->del_att(ncid, varid, name);
   return stat;
}
/**@}*/  /* End doxygen member group. */

/**@}*/
