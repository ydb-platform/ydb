/* Copyright 2018 University Corporation for Atmospheric
   Research/Unidata. See copyright file for more info.  */
/**
 * @file
 * Attribute functions
 *
 * These functions in this file read attributes.
 */

#include "ncdispatch.h"

/**
 * @anchor getting_attributes
 * @name Getting Attributes
 *
 * Functions to get the values of attributes.
 *
 * For classic format files, the netCDF library reads all attributes
 * into memory when the file is opened with nc_open().
 *
 * For netCDF-4/HDF5 files, since version 4.7.2, attributes are not
 * read on file open. Instead, when the first read of a variable
 * attribute is done, all attributes for that variable are
 * read. Subsequent access to other attributes of that variable will
 * not incur a disk read. Similarly, when the first NC_GLOBAL
 * attribute is read in a group, all NC_GLOBAL attributes for that
 * group will be read.
 *
 * @note All elements attribute data array are returned, so you must
 * allocate enough space to hold them. If you don't know how much
 * space to reserve, call nc_inq_attlen() first to find out the length
 * of the attribute.
 *
 * <h1>Example</h1>
 *
 * Here is an example using nc_get_att_double() to determine the
 * values of a variable attribute named valid_range for a netCDF
 * variable named rh from a netCDF dataset named foo.nc.
 *
 * In this example, it is assumed that we don't know how many values
 * will be returned, but that we do know the types of the
 * attributes. Hence, to allocate enough space to store them, we must
 * first inquire about the length of the attributes.

@code
     #include <netcdf.h>
        ...
     int  status;
     int  ncid;
     int  rh_id;
     int  vr_len;
     double *vr_val;

        ...
     status = nc_open("foo.nc", NC_NOWRITE, &ncid);
     if (status != NC_NOERR) handle_error(status);
        ...
     status = nc_inq_varid (ncid, "rh", &rh_id);
     if (status != NC_NOERR) handle_error(status);
        ...
     status = nc_inq_attlen (ncid, rh_id, "valid_range", &vr_len);
     if (status != NC_NOERR) handle_error(status);

     vr_val = (double *) malloc(vr_len * sizeof(double));

     status = nc_get_att_double(ncid, rh_id, "valid_range", vr_val);
     if (status != NC_NOERR) handle_error(status);
        ...
@endcode
 */
/**@{*/  /* Start doxygen member group. */

/**
 * @ingroup attributes
 * Get an attribute of any type.
 *
 * The nc_get_att() function works for any type of attribute, and must
 * be used to get attributes of user-defined type. We recommend that
 * the type safe versions of this function be used for atomic data
 * types.
 *
 * Also see @ref getting_attributes "Getting Attributes"
 *
 * @param ncid NetCDF file or group ID.
 * @param varid Variable ID, or ::NC_GLOBAL for a global attribute.
 * @param name Attribute name.
 * @param value Pointer that will get array of attribute value(s). Use
 * nc_inq_attlen() to learn length.
 *
 * @note See documentation for nc_get_att_string() regarding a special
 * case where memory must be explicitly released.
 *
 * <h1>Example</h1>
 *
 * Here is an example using nc_get_att() from nc_test4/tst_vl.c
 * creates a VLEN attribute, then uses nc_get_att() to read it.
 *
@code
#define FILE_NAME "tst_vl.nc"
#define VLEN_NAME "vlen_name"
#define ATT_NAME "att_name"

      int ncid, typeid;
      nc_vlen_t data[DIM_LEN], data_in[DIM_LEN];
      ...

      if (nc_create(FILE_NAME, NC_NETCDF4, &ncid)) ERR;
      if (nc_def_vlen(ncid, VLEN_NAME, NC_INT, &typeid)) ERR;
      ...
      if (nc_put_att(ncid, NC_GLOBAL, ATT_NAME, typeid, DIM_LEN, data)) ERR;
      if (nc_close(ncid)) ERR;

      ...
      if (nc_open(FILE_NAME, NC_NOWRITE, &ncid)) ERR;
      if (nc_get_att(ncid, NC_GLOBAL, ATT_NAME, data_in)) ERR;
      ...
      if (nc_close(ncid)) ERR;
@endcode

 * @return ::NC_NOERR for success.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOTVAR Bad varid.
 * @return ::NC_EBADNAME Bad name. See \ref object_name.
 * @return ::NC_EINVAL Invalid parameters.
 * @return ::NC_ENOTATT Can't find attribute.
 * @return ::NC_ECHAR Can't convert to or from NC_CHAR.
 * @return ::NC_ENOMEM Out of memory.
 * @return ::NC_ERANGE Data conversion went out of range.
 *
 * @author Glenn Davis, Ed Hartnett, Dennis Heimbigner
*/
int
nc_get_att(int ncid, int varid, const char *name, void *value)
{
   NC* ncp;
   int stat = NC_NOERR;
   nc_type xtype;

   if ((stat = NC_check_id(ncid, &ncp)))
      return stat;

   /* Need to get the type */
   if ((stat = nc_inq_atttype(ncid, varid, name, &xtype)))
      return stat;

   TRACE(nc_get_att);
   return ncp->dispatch->get_att(ncid, varid, name, value, xtype);
}

/**
 * @ingroup attributes
 * Get a text attribute.
 *
 * This function gets a text attribute from the netCDF
 * file. Type conversions are not permitted.
 *
 * Also see @ref getting_attributes "Getting Attributes"
 *
 * @param ncid NetCDF file or group ID.
 * @param varid Variable ID, or ::NC_GLOBAL for a global attribute.
 * @param name Attribute name.
 * @param value Pointer that will get array of attribute value(s). Use
 * nc_inq_attlen() to learn length.
 *
 * @note The handling of NULL terminators is not specified by
 * netCDF. C programs can write attributes with or without NULL
 * terminators. It is up to the reader to know whether NULL
 * terminators have been used, and, if not, to add a NULL terminator
 * when reading text attributes.
 *
 * <h1>Example</h1>
 *
 * Here is an example using nc_get_att_text() to read a global
 * attribute named title in an existing netCDF dataset named foo.nc.
 *
 * In this example we learn the length of the attribute, so that an
 * array may be allocated, adding 1 in case a NULL terminator is
 * needed. We then take the precaution of setting the last element of
 * the array to 0, to NULL terminate the string. If a NULL terminator
 * was written with this attribute, strlen(title) will show the
 * correct length (the number of chars before the first NULL
 * terminator).

@code
     #include <netcdf.h>
        ...
     int  status;
     int  ncid;
     int  rh_id;
     int  t_len;
     char *title;

        ...
     status = nc_open("foo.nc", NC_NOWRITE, &ncid);
     if (status != NC_NOERR) handle_error(status);
        ...
     status = nc_inq_varid (ncid, "rh", &rh_id);
     if (status != NC_NOERR) handle_error(status);
        ...
     status = nc_inq_attlen (ncid, NC_GLOBAL, "title", &t_len);
     if (status != NC_NOERR) handle_error(status);

     title = (char *) malloc(t_len + 1);
     status = nc_get_att_text(ncid, NC_GLOBAL, "title", title);
     if (status != NC_NOERR) handle_error(status);
     title[t_len] = '\0';
        ...
@endcode
 *
 * @return ::NC_NOERR for success.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOTVAR Bad varid.
 * @return ::NC_EBADNAME Bad name. See \ref object_name.
 * @return ::NC_EINVAL Invalid parameters.
 * @return ::NC_ENOTATT Can't find attribute.
 * @return ::NC_ECHAR Can't convert to or from NC_CHAR.
 * @return ::NC_ENOMEM Out of memory.
 * @return ::NC_ERANGE Data conversion went out of range.
 * @author Glenn Davis, Ed Hartnett, Dennis Heimbigner
*/
int
nc_get_att_text(int ncid, int varid, const char *name, char *value)
{
   NC* ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   TRACE(nc_get_att_text);
   return ncp->dispatch->get_att(ncid, varid, name, (void *)value, NC_CHAR);
}

/**
 * @ingroup attributes
 * Get an attribute of an signed char type.
 *
 * Also see @ref getting_attributes "Getting Attributes"
 *
 * @param ncid NetCDF file or group ID.
 * @param varid Variable ID, or ::NC_GLOBAL for a global attribute.
 * @param name Attribute name.
 * @param value Pointer that will get array of attribute value(s). Use
 * nc_inq_attlen() to learn length.
 *
 * @return ::NC_NOERR for success.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOTVAR Bad varid.
 * @return ::NC_EBADNAME Bad name. See \ref object_name.
 * @return ::NC_EINVAL Invalid parameters.
 * @return ::NC_ENOTATT Can't find attribute.
 * @return ::NC_ECHAR Can't convert to or from NC_CHAR.
 * @return ::NC_ENOMEM Out of memory.
 * @return ::NC_ERANGE Data conversion went out of range.
 * @author Glenn Davis, Ed Hartnett, Dennis Heimbigner
 */
int
nc_get_att_schar(int ncid, int varid, const char *name, signed char *value)
{
   NC* ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   TRACE(nc_get_att_schar);
   return ncp->dispatch->get_att(ncid, varid, name, (void *)value, NC_BYTE);
}

/**
 * @ingroup attributes
 * Get an attribute of an signed char type.
 *
 * Also see @ref getting_attributes "Getting Attributes"
 *
 * @param ncid NetCDF file or group ID.
 * @param varid Variable ID, or ::NC_GLOBAL for a global attribute.
 * @param name Attribute name.
 * @param value Pointer that will get array of attribute value(s). Use
 * nc_inq_attlen() to learn length.
 *
 * @return ::NC_NOERR for success.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOTVAR Bad varid.
 * @return ::NC_EBADNAME Bad name. See \ref object_name.
 * @return ::NC_EINVAL Invalid parameters.
 * @return ::NC_ENOTATT Can't find attribute.
 * @return ::NC_ECHAR Can't convert to or from NC_CHAR.
 * @return ::NC_ENOMEM Out of memory.
 * @return ::NC_ERANGE Data conversion went out of range.
 * @author Glenn Davis, Ed Hartnett, Dennis Heimbigner
 */
int
nc_get_att_uchar(int ncid, int varid, const char *name, unsigned char *value)
{
   NC* ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   TRACE(nc_get_att_uchar);
   return ncp->dispatch->get_att(ncid, varid, name, (void *)value, NC_UBYTE);
}

/**
 * @ingroup attributes
 * Get an attribute array of type short.
 *
 * Also see @ref getting_attributes "Getting Attributes"
 *
 * @param ncid NetCDF file or group ID.
 * @param varid Variable ID, or ::NC_GLOBAL for a global attribute.
 * @param name Attribute name.
 * @param value Pointer that will get array of attribute value(s). Use
 * nc_inq_attlen() to learn length.
 *
 * @return ::NC_NOERR for success.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOTVAR Bad varid.
 * @return ::NC_EBADNAME Bad name. See \ref object_name.
 * @return ::NC_EINVAL Invalid parameters.
 * @return ::NC_ENOTATT Can't find attribute.
 * @return ::NC_ECHAR Can't convert to or from NC_CHAR.
 * @return ::NC_ENOMEM Out of memory.
 * @return ::NC_ERANGE Data conversion went out of range.
 * @author Glenn Davis, Ed Hartnett, Dennis Heimbigner
 */
int
nc_get_att_short(int ncid, int varid, const char *name, short *value)
{
   NC* ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   TRACE(nc_get_att_short);
   return ncp->dispatch->get_att(ncid, varid, name, (void *)value, NC_SHORT);
}

/**
 * @ingroup attributes
 * Get an attribute array of type int.
 *
 * Also see @ref getting_attributes "Getting Attributes"
 *
 * @param ncid NetCDF file or group ID.
 * @param varid Variable ID, or ::NC_GLOBAL for a global attribute.
 * @param name Attribute name.
 * @param value Pointer that will get array of attribute value(s). Use
 * nc_inq_attlen() to learn length.
 *
 * @return ::NC_NOERR for success.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOTVAR Bad varid.
 * @return ::NC_EBADNAME Bad name. See \ref object_name.
 * @return ::NC_EINVAL Invalid parameters.
 * @return ::NC_ENOTATT Can't find attribute.
 * @return ::NC_ECHAR Can't convert to or from NC_CHAR.
 * @return ::NC_ENOMEM Out of memory.
 * @return ::NC_ERANGE Data conversion went out of range.
 * @author Glenn Davis, Ed Hartnett, Dennis Heimbigner
 */
int
nc_get_att_int(int ncid, int varid, const char *name, int *value)
{
   NC* ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   TRACE(nc_get_att_int);
   return ncp->dispatch->get_att(ncid, varid, name, (void *)value, NC_INT);
}

/**
 * @ingroup attributes
 * Get an attribute array of type long.
 *
 * Also see @ref getting_attributes "Getting Attributes"
 *
 * @param ncid NetCDF file or group ID.
 * @param varid Variable ID, or ::NC_GLOBAL for a global attribute.
 * @param name Attribute name.
 * @param value Pointer that will get array of attribute value(s). Use
 * nc_inq_attlen() to learn length.
 *
 * @return ::NC_NOERR for success.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOTVAR Bad varid.
 * @return ::NC_EBADNAME Bad name. See \ref object_name.
 * @return ::NC_EINVAL Invalid parameters.
 * @return ::NC_ENOTATT Can't find attribute.
 * @return ::NC_ECHAR Can't convert to or from NC_CHAR.
 * @return ::NC_ENOMEM Out of memory.
 * @return ::NC_ERANGE Data conversion went out of range.
 * @author Glenn Davis, Ed Hartnett, Dennis Heimbigner
 */
int
nc_get_att_long(int ncid, int varid, const char *name, long *value)
{
   NC* ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   TRACE(nc_get_att_long);
   return ncp->dispatch->get_att(ncid, varid, name, (void *)value, longtype);
}

/**
 * @ingroup attributes
 * Get an attribute array of type float.
 *
 * Also see @ref getting_attributes "Getting Attributes"
 *
 * @param ncid NetCDF file or group ID.
 * @param varid Variable ID, or ::NC_GLOBAL for a global attribute.
 * @param name Attribute name.
 * @param value Pointer that will get array of attribute value(s). Use
 * nc_inq_attlen() to learn length.
 *
 * @return ::NC_NOERR for success.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOTVAR Bad varid.
 * @return ::NC_EBADNAME Bad name. See \ref object_name.
 * @return ::NC_EINVAL Invalid parameters.
 * @return ::NC_ENOTATT Can't find attribute.
 * @return ::NC_ECHAR Can't convert to or from NC_CHAR.
 * @return ::NC_ENOMEM Out of memory.
 * @return ::NC_ERANGE Data conversion went out of range.
 * @author Glenn Davis, Ed Hartnett, Dennis Heimbigner
 */
int
nc_get_att_float(int ncid, int varid, const char *name, float *value)
{
   NC* ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   TRACE(nc_get_att_float);
   return ncp->dispatch->get_att(ncid, varid, name, (void *)value, NC_FLOAT);
}

/**
 * @ingroup attributes
 * Get an attribute array of type double.
 *
 * Also see @ref getting_attributes "Getting Attributes"
 *
 * @param ncid NetCDF file or group ID.
 * @param varid Variable ID, or ::NC_GLOBAL for a global attribute.
 * @param name Attribute name.
 * @param value Pointer that will get array of attribute value(s). Use
 * nc_inq_attlen() to learn length.
 *
 * @return ::NC_NOERR for success.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOTVAR Bad varid.
 * @return ::NC_EBADNAME Bad name. See \ref object_name.
 * @return ::NC_EINVAL Invalid parameters.
 * @return ::NC_ENOTATT Can't find attribute.
 * @return ::NC_ECHAR Can't convert to or from NC_CHAR.
 * @return ::NC_ENOMEM Out of memory.
 * @return ::NC_ERANGE Data conversion went out of range.
 * @author Glenn Davis, Ed Hartnett, Dennis Heimbigner
 */
int
nc_get_att_double(int ncid, int varid, const char *name, double *value)
{
   NC* ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   TRACE(nc_get_att_double);
   return ncp->dispatch->get_att(ncid, varid, name, (void *)value, NC_DOUBLE);
}

/**
 * @ingroup attributes
 * Get an attribute array of type unsigned char.
 *
 * Also see @ref getting_attributes "Getting Attributes"
 *
 * @param ncid NetCDF file or group ID.
 * @param varid Variable ID, or ::NC_GLOBAL for a global attribute.
 * @param name Attribute name.
 * @param value Pointer that will get array of attribute value(s). Use
 * nc_inq_attlen() to learn length.
 *
 * @return ::NC_NOERR for success.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOTVAR Bad varid.
 * @return ::NC_EBADNAME Bad name. See \ref object_name.
 * @return ::NC_EINVAL Invalid parameters.
 * @return ::NC_ENOTATT Can't find attribute.
 * @return ::NC_ECHAR Can't convert to or from NC_CHAR.
 * @return ::NC_ENOMEM Out of memory.
 * @return ::NC_ERANGE Data conversion went out of range.
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
nc_get_att_ubyte(int ncid, int varid, const char *name, unsigned char *value)
{
   NC* ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   TRACE(nc_get_att_ubyte);
   return ncp->dispatch->get_att(ncid, varid, name, (void *)value, NC_UBYTE);
}

/**
 * @ingroup attributes
 * Get an attribute array of type unsigned short.
 *
 * Also see @ref getting_attributes "Getting Attributes"
 *
 * @param ncid NetCDF file or group ID.
 * @param varid Variable ID, or ::NC_GLOBAL for a global attribute.
 * @param name Attribute name.
 * @param value Pointer that will get array of attribute value(s). Use
 * nc_inq_attlen() to learn length.
 *
 * @return ::NC_NOERR for success.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOTVAR Bad varid.
 * @return ::NC_EBADNAME Bad name. See \ref object_name.
 * @return ::NC_EINVAL Invalid parameters.
 * @return ::NC_ENOTATT Can't find attribute.
 * @return ::NC_ECHAR Can't convert to or from NC_CHAR.
 * @return ::NC_ENOMEM Out of memory.
 * @return ::NC_ERANGE Data conversion went out of range.
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
nc_get_att_ushort(int ncid, int varid, const char *name, unsigned short *value)
{
   NC* ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   TRACE(nc_get_att_ushort);
   return ncp->dispatch->get_att(ncid, varid, name, (void *)value, NC_USHORT);
}

/**
 * @ingroup attributes
 * Get an attribute array of type unsigned int.
 *
 * Also see @ref getting_attributes "Getting Attributes"
 *
 * @param ncid NetCDF file or group ID.
 * @param varid Variable ID, or ::NC_GLOBAL for a global attribute.
 * @param name Attribute name.
 * @param value Pointer that will get array of attribute value(s). Use
 * nc_inq_attlen() to learn length.
 *
 * @return ::NC_NOERR for success.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOTVAR Bad varid.
 * @return ::NC_EBADNAME Bad name. See \ref object_name.
 * @return ::NC_EINVAL Invalid parameters.
 * @return ::NC_ENOTATT Can't find attribute.
 * @return ::NC_ECHAR Can't convert to or from NC_CHAR.
 * @return ::NC_ENOMEM Out of memory.
 * @return ::NC_ERANGE Data conversion went out of range.
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
nc_get_att_uint(int ncid, int varid, const char *name, unsigned int *value)
{
   NC* ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   TRACE(nc_get_att_uint);
   return ncp->dispatch->get_att(ncid, varid, name, (void *)value, NC_UINT);
}

/**
 * @ingroup attributes
 * Get an attribute array of type long long.
 *
 * Also see @ref getting_attributes "Getting Attributes"
 *
 * @param ncid NetCDF file or group ID.
 * @param varid Variable ID, or ::NC_GLOBAL for a global attribute.
 * @param name Attribute name.
 * @param value Pointer that will get array of attribute value(s). Use
 * nc_inq_attlen() to learn length.
 *
 * @return ::NC_NOERR for success.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOTVAR Bad varid.
 * @return ::NC_EBADNAME Bad name. See \ref object_name.
 * @return ::NC_EINVAL Invalid parameters.
 * @return ::NC_ENOTATT Can't find attribute.
 * @return ::NC_ECHAR Can't convert to or from NC_CHAR.
 * @return ::NC_ENOMEM Out of memory.
 * @return ::NC_ERANGE Data conversion went out of range.
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
nc_get_att_longlong(int ncid, int varid, const char *name, long long *value)
{
   NC* ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   TRACE(nc_get_att_longlong);
   return ncp->dispatch->get_att(ncid, varid, name, (void *)value, NC_INT64);
}

/**
 * @ingroup attributes
 * Get an attribute array of type unsigned long long.
 *
 * Also see @ref getting_attributes "Getting Attributes"
 *
 * @param ncid NetCDF file or group ID.
 * @param varid Variable ID, or ::NC_GLOBAL for a global attribute.
 * @param name Attribute name.
 * @param value Pointer that will get array of attribute value(s). Use
 * nc_inq_attlen() to learn length.
 *
 * @return ::NC_NOERR for success.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOTVAR Bad varid.
 * @return ::NC_EBADNAME Bad name. See \ref object_name.
 * @return ::NC_EINVAL Invalid parameters.
 * @return ::NC_ENOTATT Can't find attribute.
 * @return ::NC_ECHAR Can't convert to or from NC_CHAR.
 * @return ::NC_ENOMEM Out of memory.
 * @return ::NC_ERANGE Data conversion went out of range.
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
nc_get_att_ulonglong(int ncid, int varid, const char *name, unsigned long long *value)
{
   NC *ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   TRACE(nc_get_att_ulonglong);
   return ncp->dispatch->get_att(ncid, varid, name, (void *)value, NC_UINT64);
}

/**
 * @ingroup attributes
 * Get an attribute array of type string.
 *
 * This function gets an attribute from netCDF file. The nc_get_att()
 * function works with any type of data including user defined types,
 * but this function will retrieve attributes which are of type
 * variable-length string.
 *
 * Also see @ref getting_attributes "Getting Attributes"
 *
 * @note Note that unlike most other nc_get_att functions,
 * nc_get_att_string() allocates a chunk of memory which is returned
 * to the calling function.  This chunk of memory must be specifically
 * deallocated with nc_free_string() to avoid any memory leaks.  Also
 * note that you must still preallocate the memory needed for the
 * array of pointers passed to nc_get_att_string().
 *
 * @param ncid NetCDF file or group ID.
 * @param varid Variable ID, or ::NC_GLOBAL for a global attribute.
 * @param name Attribute name.
 * @param value Pointer that will get array of attribute value(s). Use
 * nc_inq_attlen() to learn length.
 *
 * @section nc_get_att_string_example Example
 *
@code{.c}
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <netcdf.h>

void check(int stat) {
  if (stat != NC_NOERR) {
    printf("NetCDF error: %s\n", nc_strerror(stat));
    exit(1);
  }
}

int main(int argc, char ** argv) {
  int stat = 0;

  int ncid = 0;
  stat = nc_open("test.nc", NC_NOWRITE, &ncid); check(stat);

  int varid = 0;
  stat = nc_inq_varid(ncid, "variable", &varid); check(stat);

  size_t attlen = 0;
  stat = nc_inq_attlen(ncid, varid, "attribute", &attlen); check(stat);

  char **string_attr = (char**)malloc(attlen * sizeof(char*));
  memset(string_attr, 0, attlen * sizeof(char*));

  stat = nc_get_att_string(ncid, varid, "attribute", string_attr); check(stat);

  for (size_t k = 0; k < attlen; ++k) {
    printf("variable:attribute[%d] = %s\n", k, string_attr[k]);
  }

  stat = nc_free_string(attlen, string_attr); check(stat);

  free(string_attr);

  stat = nc_close(ncid); check(stat);

  return 0;
}
@endcode

 * @return ::NC_NOERR for success.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOTVAR Bad varid.
 * @return ::NC_EBADNAME Bad name. See \ref object_name.
 * @return ::NC_EINVAL Invalid parameters.
 * @return ::NC_ENOTATT Can't find attribute.
 * @return ::NC_ECHAR Can't convert to or from NC_CHAR.
 * @return ::NC_ENOMEM Out of memory.
 * @return ::NC_ERANGE Data conversion went out of range.
 *
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
nc_get_att_string(int ncid, int varid, const char *name, char **value)
{
    NC *ncp;
    int stat = NC_check_id(ncid, &ncp);
    if(stat != NC_NOERR) return stat;
    TRACE(nc_get_att_string);
    return ncp->dispatch->get_att(ncid,varid,name,(void*)value, NC_STRING);
}
/**@}*/  /* End doxygen member group. */
