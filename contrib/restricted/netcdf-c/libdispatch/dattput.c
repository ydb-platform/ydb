/* Copyright 2018 University Corporation for Atmospheric
   Research/Unidata. See copyright file for more info.  */
/**
 * @file
 * Functions to write attributes.
 *
 * These functions write attributes.
 */
#include "ncdispatch.h"

/**
 * @anchor writing_attributes
 * @name Writing Attributes
 *
 * Functions to write attributes.
 *
 * For netCDF classic formats, attributes are defined when the dataset
 * is first created, while the netCDF dataset is in define
 * mode. Additional attributes may be added later by reentering define
 * mode. For netCDF-4/HDF5 netCDF files, attributes may be defined at
 * any time.
 *
 * In classic format files, the data type, length, and value of an
 * attribute may be changed even when in data mode, as long as the
 * changed attribute requires no more space than the attribute as
 * originally defined. In netCDF-4/HDF5 files, attribute name, length,
 * and value may be changed at any time.
 *
 * Attribute data conversion automatically takes place when the type
 * of the data does not match the xtype argument. All attribute data
 * values are converted to xtype before being written to the file.
 *
 * If writing a new attribute, or if the space required to store
 * the attribute is greater than before, the netCDF dataset must be in
 * define mode for classic formats (or netCDF-4/HDF5 with
 * NC_CLASSIC_MODEL).
 *
 * @note With netCDF-4 files, nc_put_att will notice if you are
 * writing a _FillValue attribute, and will tell the HDF5 layer to use
 * the specified fill value for that variable.  With either classic or
 * netCDF-4 files, a _FillValue attribute will be checked for
 * validity, to make sure it has only one value and that its type
 * matches the type of the associated variable.
 *
*/

/**@{*/  /* Start doxygen member group. */
/**
 * @ingroup attributes
 * Write a string attribute.
 *
 * The function nc_put_att_string adds or changes a variable attribute
 * or global attribute of an open netCDF dataset. The string type is
 * only available in netCDF-4/HDF5 files, when ::NC_CLASSIC_MODEL has
 * not been used in nc_create().
 *
 * Also see @ref writing_attributes "Writing Attributes"
 *
 * @param ncid NetCDF file or group ID.
 * @param varid Variable ID, or ::NC_GLOBAL for a global attribute.
 * @param name Attribute @ref object_name.
 * @param len Number of values provided for the attribute.
 * @param value Pointer to one or more values.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EINVAL Invalid or global _FillValue.
 * @return ::NC_ENOTVAR Couldn't find varid.
 * @return ::NC_EBADTYPE Fill value and var must be same type.
 * @return ::NC_ENOMEM Out of memory
 * @return ::NC_ELATEFILL Too late to set fill value.
 *
 * @author Ed Hartnett, Dennis Heimbigner
*/
int
nc_put_att_string(int ncid, int varid, const char *name,
		  size_t len, const char** value)
{
    NC* ncp;
    int stat = NC_check_id(ncid, &ncp);
    if(stat != NC_NOERR) return stat;
    return ncp->dispatch->put_att(ncid, varid, name, NC_STRING,
				  len, (void*)value, NC_STRING);
}

/**
 * @ingroup attributes
 * Write a text attribute.
 *
 * Add or change a text attribute. If this attribute is new, or if the
 * space required to store the attribute is greater than before, the
 * netCDF dataset must be in define mode for classic formats (or
 * netCDF-4/HDF5 with NC_CLASSIC_MODEL).
 *
 * Type conversion is not available with text attributes.
 *
 * Also see @ref writing_attributes "Writing Attributes"
 *
 * @note Whether or not this length includes the NULL character in C
 * char arrays is a user decision. If the NULL character is not
 * written, then all C programs must add the NULL character after
 * reading a text attribute.
 *
 * @param ncid NetCDF file or group ID.
 * @param varid Variable ID, or ::NC_GLOBAL for a global attribute.
 * @param name Attribute @ref object_name.
 * @param len The length of the text array.
 * @param value Pointer to the start of the character array.
 *
 * @section nc_put_att_text_example Example
 *
 * Here is an example using nc_put_att_double() to add a variable
 * attribute named valid_range for a netCDF variable named rh and
 * nc_put_att_text() to add a global attribute named title to an
 * existing netCDF dataset named foo.nc:
 *
@code
     #include <netcdf.h>
        ...
     int  status;
     int  ncid;
     int  rh_id;
     static double rh_range[] = {0.0, 100.0};
     static char title[] = "example netCDF dataset";
        ...
     status = nc_open("foo.nc", NC_WRITE, &ncid);
     if (status != NC_NOERR) handle_error(status);
        ...
     status = nc_redef(ncid);
     if (status != NC_NOERR) handle_error(status);
     status = nc_inq_varid (ncid, "rh", &rh_id);
     if (status != NC_NOERR) handle_error(status);
        ...
     status = nc_put_att_double (ncid, rh_id, "valid_range",
                                 NC_DOUBLE, 2, rh_range);
     if (status != NC_NOERR) handle_error(status);
     status = nc_put_att_text (ncid, NC_GLOBAL, "title",
                               strlen(title), title)
     if (status != NC_NOERR) handle_error(status);
        ...
     status = nc_enddef(ncid);
     if (status != NC_NOERR) handle_error(status);
@endcode

 * @return ::NC_NOERR No error.
 * @return ::NC_EINVAL Invalid or global _FillValue.
 * @return ::NC_ENOTVAR Couldn't find varid.
 * @return ::NC_EBADTYPE Fill value and var must be same type.
 * @return ::NC_ENOMEM Out of memory
 * @return ::NC_ELATEFILL Too late to set fill value.
 *
 * @author Glenn Davis, Ed Hartnett, Dennis Heimbigner
*/
int nc_put_att_text(int ncid, int varid, const char *name,
		size_t len, const char *value)
{
   NC* ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   return ncp->dispatch->put_att(ncid, varid, name, NC_CHAR, len,
				 (void *)value, NC_CHAR);
}

/**
 * @ingroup attributes
 * Write an attribute of any type.
 *
 * Also see @ref writing_attributes "Writing Attributes"
 *
 * @param ncid NetCDF file or group ID.
 * @param varid Variable ID, or ::NC_GLOBAL for a global attribute.
 * @param name Attribute @ref object_name.
 * @param xtype The type of attribute to write. Data will be converted
 * to this type.
 * @param len Number of values provided for the attribute.
 * @param value Pointer to one or more values.
 *
 * @section nc_put_att_double_example Example
 *
 * Here is an example using nc_put_att_double() to add a variable
 * attribute named valid_range for a netCDF variable named rh and
 * nc_put_att_text() to add a global attribute named title to an
 * existing netCDF dataset named foo.nc:
 *
@code
     #include <netcdf.h>
        ...
     int  status;
     int  ncid;
     int  rh_id;
     static double rh_range[] = {0.0, 100.0};
     static char title[] = "example netCDF dataset";
        ...
     status = nc_open("foo.nc", NC_WRITE, &ncid);
     if (status != NC_NOERR) handle_error(status);
        ...
     status = nc_redef(ncid);
     if (status != NC_NOERR) handle_error(status);
     status = nc_inq_varid (ncid, "rh", &rh_id);
     if (status != NC_NOERR) handle_error(status);
        ...
     status = nc_put_att_double (ncid, rh_id, "valid_range",
                                 NC_DOUBLE, 2, rh_range);
     if (status != NC_NOERR) handle_error(status);
     status = nc_put_att_text (ncid, NC_GLOBAL, "title",
                               strlen(title), title)
     if (status != NC_NOERR) handle_error(status);
        ...
     status = nc_enddef(ncid);
     if (status != NC_NOERR) handle_error(status);
@endcode

 * @return ::NC_NOERR No error.
 * @return ::NC_EINVAL Invalid or global _FillValue.
 * @return ::NC_ENOTVAR Couldn't find varid.
 * @return ::NC_EBADTYPE Fill value and var must be same type.
 * @return ::NC_ENOMEM Out of memory
 * @return ::NC_ELATEFILL Too late to set fill value.
 *
 * @author Glenn Davis, Ed Hartnett, Dennis Heimbigner
*/
int
nc_put_att(int ncid, int varid, const char *name, nc_type xtype,
	   size_t len, const void *value)
{
   NC* ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   return ncp->dispatch->put_att(ncid, varid, name, xtype, len,
				 value, xtype);
}

/**
 * @ingroup attributes
 * Write an attribute of type signed char.
 *
 * Also see @ref writing_attributes "Writing Attributes"
 *
 * @param ncid NetCDF file or group ID.
 * @param varid Variable ID, or ::NC_GLOBAL for a global attribute.
 * @param name Attribute @ref object_name.
 * @param xtype The type of attribute to write. Data will be converted
 * to this type.
 * @param len Number of values provided for the attribute.
 * @param value Pointer to one or more values.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EINVAL Invalid or global _FillValue.
 * @return ::NC_ENOTVAR Couldn't find varid.
 * @return ::NC_EBADTYPE Fill value and var must be same type.
 * @return ::NC_ENOMEM Out of memory
 * @return ::NC_ELATEFILL Too late to set fill value.
 *
 * @author Glenn Davis, Ed Hartnett, Dennis Heimbigner
*/
int
nc_put_att_schar(int ncid, int varid, const char *name,
		 nc_type xtype, size_t len, const signed char *value)
{
   NC *ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   return ncp->dispatch->put_att(ncid, varid, name, xtype, len,
				 (void *)value, NC_BYTE);
}

/**
 * @ingroup attributes
 * Write an attribute of type unsigned char.
 *
 * Also see @ref writing_attributes "Writing Attributes"
 *
 * @param ncid NetCDF file or group ID.
 * @param varid Variable ID, or ::NC_GLOBAL for a global attribute.
 * @param name Attribute @ref object_name.
 * @param xtype The type of attribute to write. Data will be converted
 * to this type.
 * @param len Number of values provided for the attribute.
 * @param value Pointer to one or more values.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EINVAL Invalid or global _FillValue.
 * @return ::NC_ENOTVAR Couldn't find varid.
 * @return ::NC_EBADTYPE Fill value and var must be same type.
 * @return ::NC_ENOMEM Out of memory
 * @return ::NC_ELATEFILL Too late to set fill value.
 *
 * @author Glenn Davis, Ed Hartnett, Dennis Heimbigner
*/
int
nc_put_att_uchar(int ncid, int varid, const char *name,
		 nc_type xtype, size_t len, const unsigned char *value)
{
   NC* ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   return ncp->dispatch->put_att(ncid, varid, name, xtype, len,
				 (void *)value, NC_UBYTE);
}

/**
 * @ingroup attributes
 * Write an attribute of type short.
 *
 * Also see @ref writing_attributes "Writing Attributes"
 *
 * @param ncid NetCDF file or group ID.
 * @param varid Variable ID, or ::NC_GLOBAL for a global attribute.
 * @param name Attribute @ref object_name.
 * @param xtype The type of attribute to write. Data will be converted
 * to this type.
 * @param len Number of values provided for the attribute.
 * @param value Pointer to one or more values.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EINVAL Invalid or global _FillValue.
 * @return ::NC_ENOTVAR Couldn't find varid.
 * @return ::NC_EBADTYPE Fill value and var must be same type.
 * @return ::NC_ENOMEM Out of memory
 * @return ::NC_ELATEFILL Too late to set fill value.
 *
 * @author Glenn Davis, Ed Hartnett, Dennis Heimbigner
*/
int
nc_put_att_short(int ncid, int varid, const char *name,
		 nc_type xtype, size_t len, const short *value)
{
   NC* ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   return ncp->dispatch->put_att(ncid, varid, name, xtype, len,
				 (void *)value, NC_SHORT);
}

/**
 * @ingroup attributes
 * Write an attribute of type int.
 *
 * Also see @ref writing_attributes "Writing Attributes"
 *
 * @param ncid NetCDF file or group ID.
 * @param varid Variable ID, or ::NC_GLOBAL for a global attribute.
 * @param name Attribute @ref object_name.
 * @param xtype The type of attribute to write. Data will be converted
 * to this type.
 * @param len Number of values provided for the attribute.
 * @param value Pointer to one or more values.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EINVAL Invalid or global _FillValue.
 * @return ::NC_ENOTVAR Couldn't find varid.
 * @return ::NC_EBADTYPE Fill value and var must be same type.
 * @return ::NC_ENOMEM Out of memory
 * @return ::NC_ELATEFILL Too late to set fill value.
 *
 * @author Glenn Davis, Ed Hartnett, Dennis Heimbigner
*/
int
nc_put_att_int(int ncid, int varid, const char *name,
	       nc_type xtype, size_t len, const int *value)
{
   NC* ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   return ncp->dispatch->put_att(ncid, varid, name, xtype, len,
				 (void *)value, NC_INT);
}

/**
 * @ingroup attributes
 * Write an attribute of type long.
 *
 * Also see @ref writing_attributes "Writing Attributes"
 *
 * @param ncid NetCDF file or group ID.
 * @param varid Variable ID, or ::NC_GLOBAL for a global attribute.
 * @param name Attribute @ref object_name.
 * @param xtype The type of attribute to write. Data will be converted
 * to this type.
 * @param len Number of values provided for the attribute.
 * @param value Pointer to one or more values.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EINVAL Invalid or global _FillValue.
 * @return ::NC_ENOTVAR Couldn't find varid.
 * @return ::NC_EBADTYPE Fill value and var must be same type.
 * @return ::NC_ENOMEM Out of memory
 * @return ::NC_ELATEFILL Too late to set fill value.
 *
 * @author Glenn Davis, Ed Hartnett, Dennis Heimbigner
*/
int
nc_put_att_long(int ncid, int varid, const char *name,
		nc_type xtype, size_t len, const long *value)
{
   NC* ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   return ncp->dispatch->put_att(ncid, varid, name, xtype, len,
				 (void *)value, longtype);
}

/**
 * @ingroup attributes
 * Write an attribute of type float.
 *
 * Also see @ref writing_attributes "Writing Attributes"
 *
 * @param ncid NetCDF file or group ID.
 * @param varid Variable ID, or ::NC_GLOBAL for a global attribute.
 * @param name Attribute @ref object_name.
 * @param xtype The type of attribute to write. Data will be converted
 * to this type.
 * @param len Number of values provided for the attribute.
 * @param value Pointer to one or more values.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EINVAL Invalid or global _FillValue.
 * @return ::NC_ENOTVAR Couldn't find varid.
 * @return ::NC_EBADTYPE Fill value and var must be same type.
 * @return ::NC_ENOMEM Out of memory
 * @return ::NC_ELATEFILL Too late to set fill value.
 *
 * @author Glenn Davis, Ed Hartnett, Dennis Heimbigner
*/
int
nc_put_att_float(int ncid, int varid, const char *name,
		 nc_type xtype, size_t len, const float *value)
{
   NC* ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   return ncp->dispatch->put_att(ncid, varid, name, xtype, len,
				 (void *)value, NC_FLOAT);
}

/**
 * @ingroup attributes
 * Write an attribute of type double.
 *
 * Also see @ref writing_attributes "Writing Attributes"
 *
 * @param ncid NetCDF file or group ID.
 * @param varid Variable ID, or ::NC_GLOBAL for a global attribute.
 * @param name Attribute @ref object_name.
 * @param xtype The type of attribute to write. Data will be converted
 * to this type.
 * @param len Number of values provided for the attribute.
 * @param value Pointer to one or more values.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EINVAL Invalid or global _FillValue.
 * @return ::NC_ENOTVAR Couldn't find varid.
 * @return ::NC_EBADTYPE Fill value and var must be same type.
 * @return ::NC_ENOMEM Out of memory
 * @return ::NC_ELATEFILL Too late to set fill value.
 *
 * @author Glenn Davis, Ed Hartnett, Dennis Heimbigner
*/
int
nc_put_att_double(int ncid, int varid, const char *name,
		  nc_type xtype, size_t len, const double *value)
{
   NC* ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   return ncp->dispatch->put_att(ncid, varid, name, xtype, len,
				 (void *)value, NC_DOUBLE);
}

/**
 * @ingroup attributes
 * Write an attribute of type unsigned char.
 *
 * Also see @ref writing_attributes "Writing Attributes"
 *
 * @param ncid NetCDF file or group ID.
 * @param varid Variable ID, or ::NC_GLOBAL for a global attribute.
 * @param name Attribute @ref object_name.
 * @param xtype The type of attribute to write. Data will be converted
 * to this type.
 * @param len Number of values provided for the attribute.
 * @param value Pointer to one or more values.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EINVAL Invalid or global _FillValue.
 * @return ::NC_ENOTVAR Couldn't find varid.
 * @return ::NC_EBADTYPE Fill value and var must be same type.
 * @return ::NC_ENOMEM Out of memory
 * @return ::NC_ELATEFILL Too late to set fill value.
 *
 * @author Ed Hartnett, Dennis Heimbigner
*/
int
nc_put_att_ubyte(int ncid, int varid, const char *name,
		 nc_type xtype, size_t len, const unsigned char *value)
{
   NC* ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   return ncp->dispatch->put_att(ncid, varid, name, xtype, len,
				 (void *)value, NC_UBYTE);
}

/**
 * @ingroup attributes
 * Write an attribute of type unsigned short.
 *
 * Also see @ref writing_attributes "Writing Attributes"
 *
 * @param ncid NetCDF file or group ID.
 * @param varid Variable ID, or ::NC_GLOBAL for a global attribute.
 * @param name Attribute @ref object_name.
 * @param xtype The type of attribute to write. Data will be converted
 * to this type.
 * @param len Number of values provided for the attribute.
 * @param value Pointer to one or more values.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EINVAL Invalid or global _FillValue.
 * @return ::NC_ENOTVAR Couldn't find varid.
 * @return ::NC_EBADTYPE Fill value and var must be same type.
 * @return ::NC_ENOMEM Out of memory
 * @return ::NC_ELATEFILL Too late to set fill value.
 *
 * @author Ed Hartnett, Dennis Heimbigner
*/
int
nc_put_att_ushort(int ncid, int varid, const char *name,
		  nc_type xtype, size_t len, const unsigned short *value)
{
   NC* ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   return ncp->dispatch->put_att(ncid, varid, name, xtype, len,
				 (void *)value, NC_USHORT);
}

/**
 * @ingroup attributes
 * Write an attribute of type unsigned int.
 *
 * Also see @ref writing_attributes "Writing Attributes"
 *
 * @param ncid NetCDF file or group ID.
 * @param varid Variable ID, or ::NC_GLOBAL for a global attribute.
 * @param name Attribute @ref object_name.
 * @param xtype The type of attribute to write. Data will be converted
 * to this type.
 * @param len Number of values provided for the attribute.
 * @param value Pointer to one or more values.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EINVAL Invalid or global _FillValue.
 * @return ::NC_ENOTVAR Couldn't find varid.
 * @return ::NC_EBADTYPE Fill value and var must be same type.
 * @return ::NC_ENOMEM Out of memory
 * @return ::NC_ELATEFILL Too late to set fill value.
 *
 * @author Ed Hartnett, Dennis Heimbigner
*/
int
nc_put_att_uint(int ncid, int varid, const char *name,
		nc_type xtype, size_t len, const unsigned int *value)
{
   NC* ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   return ncp->dispatch->put_att(ncid, varid, name, xtype, len,
				 (void *)value, NC_UINT);
}

/**
 * @ingroup attributes
 * Write an attribute of type long long.
 *
 * Also see @ref writing_attributes "Writing Attributes"
 *
 * @param ncid NetCDF file or group ID.
 * @param varid Variable ID, or ::NC_GLOBAL for a global attribute.
 * @param name Attribute @ref object_name.
 * @param xtype The type of attribute to write. Data will be converted
 * to this type.
 * @param len Number of values provided for the attribute.
 * @param value Pointer to one or more values.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EINVAL Invalid or global _FillValue.
 * @return ::NC_ENOTVAR Couldn't find varid.
 * @return ::NC_EBADTYPE Fill value and var must be same type.
 * @return ::NC_ENOMEM Out of memory
 * @return ::NC_ELATEFILL Too late to set fill value.
 *
 * @author Ed Hartnett, Dennis Heimbigner
*/
int
nc_put_att_longlong(int ncid, int varid, const char *name,
		    nc_type xtype, size_t len,
		    const long long *value)
{
   NC* ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   return ncp->dispatch->put_att(ncid, varid, name, xtype, len,
				 (void *)value, NC_INT64);
}

/**
 * @ingroup attributes
 * Write an attribute of type unsigned long long.
 *
 * Also see @ref writing_attributes "Writing Attributes"
 *
 * @param ncid NetCDF file or group ID.
 * @param varid Variable ID, or ::NC_GLOBAL for a global attribute.
 * @param name Attribute @ref object_name.
 * @param xtype The type of attribute to write. Data will be converted
 * to this type.
 * @param len Number of values provided for the attribute.
 * @param value Pointer to one or more values.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EINVAL Invalid or global _FillValue.
 * @return ::NC_ENOTVAR Couldn't find varid.
 * @return ::NC_EBADTYPE Fill value and var must be same type.
 * @return ::NC_ENOMEM Out of memory
 * @return ::NC_ELATEFILL Too late to set fill value.
 *
 * @author Ed Hartnett, Dennis Heimbigner
*/
int
nc_put_att_ulonglong(int ncid, int varid, const char *name,
		     nc_type xtype, size_t len,
		     const unsigned long long *value)
{
   NC* ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   return ncp->dispatch->put_att(ncid, varid, name, xtype, len,
				 (void *)value, NC_UINT64);
}

/**@}*/  /* End doxygen member group. */
