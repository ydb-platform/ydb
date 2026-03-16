/* Copyright 2018 University Corporation for Atmospheric
Research/Unidata. See \ref copyright file for more info.  */
/**
 * @file
 * Attribute inquiry functions
 *
 * These functions find out about attributes.
 */
#include "ncdispatch.h"

/**
 * @name Learning about Attributes
 *
 * Functions to learn about the attributes in a file. */
/** \{ */ /* All these functions are part of this named group... */

/**
 * @ingroup attributes
 * Return information about a netCDF attribute.
 *
 * The function nc_inq_att returns the attribute's type and length.
 *
 * @param ncid NetCDF or group ID, from a previous call to nc_open(),
 * nc_create(), nc_def_grp(), or associated inquiry functions such as
 * nc_inq_ncid().
 *
 * @param varid Variable ID of the attribute's variable, or
 * ::NC_GLOBAL for a global attribute.
 *
 * @param name Pointer to the location for the returned attribute \ref
 * object_name. \ref ignored_if_null.
 *
 * @param xtypep Pointer to location for returned attribute [data
 * type](https://docs.unidata.ucar.edu/nug/current/md_types.html#data_type). \ref
 * ignored_if_null.
 *
 * @param lenp Pointer to location for returned number of values
 * currently stored in the attribute. For attributes of type
 * ::NC_CHAR, you should not assume that this includes a trailing zero
 * byte; it doesn't if the attribute was stored without a trailing
 * zero byte, for example from a FORTRAN program. Before using the
 * value as a C string, make sure it is null-terminated. \ref
 * ignored_if_null.
 *
 * @section nc_inq_att_example Example
 *
 * Here is an example using nc_inq_att() to find out the type and
 * length of a variable attribute named valid_range for a netCDF
 * variable named rh and a global attribute named title in an existing
 * netCDF dataset named foo.nc:
 *
@code
     #include <netcdf.h>
        ...
     int  status;
     int  ncid;
     int  rh_id;
     nc_type vr_type, t_type;
     size_t  vr_len, t_len;

        ...
     status = nc_open("foo.nc", NC_NOWRITE, &ncid);
     if (status != NC_NOERR) handle_error(status);
        ...
     status = nc_inq_varid (ncid, "rh", &rh_id);
     if (status != NC_NOERR) handle_error(status);
        ...
     status = nc_inq_att (ncid, rh_id, "valid_range", &vr_type, &vr_len);
     if (status != NC_NOERR) handle_error(status);
     status = nc_inq_att (ncid, NC_GLOBAL, "title", &t_type, &t_len);
     if (status != NC_NOERR) handle_error(status);
@endcode
 *
 * @return ::NC_NOERR no error.
 * @return ::NC_EBADID bad ncid.
 * @return ::NC_ENOTVAR bad varid.
 * @return ::NC_EBADGRPID bad group ID.
 * @return ::NC_EBADNAME bad name.
 * @return ::NC_ENOTATT attribute not found.
 * @return ::NC_ECHAR illegal conversion to or from NC_CHAR.
 * @return ::NC_ENOMEM out of memory.
 * @return ::NC_ERANGE range error when converting data.
 * @author Glenn Davis, Ed Hartnett, Dennis Heimbigner
*/
int
nc_inq_att(int ncid, int varid, const char *name, nc_type *xtypep,
	   size_t *lenp)
{
   NC* ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   return ncp->dispatch->inq_att(ncid, varid, name, xtypep, lenp);
}

/**
 * @ingroup attributes
 * Find an attribute ID.
 *
 * @param ncid NetCDF or group ID, from a previous call to nc_open(),
 * nc_create(), nc_def_grp(), or associated inquiry functions such as
 * nc_inq_ncid().
 *
 * @param varid Variable ID of the attribute's variable, or
 * ::NC_GLOBAL for a global attribute.
 *
 * @param name Attribute \ref object_name.
 *
 * @param idp Pointer to location for returned attribute number that
 * specifies which attribute this is for this variable (or which
 * global attribute). If you already know the attribute name, knowing
 * its number is not very useful, because accessing information about
 * an attribute requires its name.
 *
 * @section nc_inq_attid_example Example
 *
 * Here is an example using nc_inq_attid() from
 * nc_test4/tst_vars2.c. In this example three attributes are created
 * in a file. Then it is re-opened, and their IDs are checked. They
 * will be 0, 1, and 2, in the order that the attributes were written
 * to the file.
 *
@code
     #include <netcdf.h>
     ...
     printf("**** testing fill value with three other attributes...");
     {
#define NUM_LEADERS 3
         char leader[NUM_LEADERS][NC_MAX_NAME + 1] = {"hair_length_of_strategoi",
                                                      "hair_length_of_Miltiades",
                                                      "hair_length_of_Darius_I"};
         short hair_length[NUM_LEADERS] = {3, 11, 4};
         short short_in;
         int a;

         if (nc_create(FILE_NAME, cmode, &ncid)) ERR;
         if (nc_def_dim(ncid, DIM1_NAME, DIM1_LEN, &dimids[0])) ERR;
         if (nc_def_var(ncid, VAR_NAME, NC_BYTE, NUM_DIMS, dimids, &varid)) ERR;
         for (a = 0; a < NUM_LEADERS; a++)
            if (nc_put_att_short(ncid, varid, leader[a], NC_SHORT, 1, &hair_length[a])) ERR;
         if (nc_put_att_schar(ncid, varid, _FillValue, NC_BYTE, 1, &fill_value)) ERR;
         if (nc_close(ncid)) ERR;

         if (nc_open(FILE_NAME, NC_NOWRITE, &ncid)) ERR;
         for (a = 0; a < NUM_LEADERS; a++)
         {
            ...
            if (nc_inq_attid(ncid, 0, leader[a], &attnum_in)) ERR;
            if (attnum_in != a) ERR;
         }

         if (nc_close(ncid)) ERR;
@endcode
 *
 * @return ::NC_NOERR no error.
 * @return ::NC_EBADID bad ncid.
 * @return ::NC_ENOTVAR bad varid.
 * @return ::NC_EBADGRPID bad group ID.
 * @return ::NC_EBADNAME bad name.
 * @return ::NC_ENOTATT attribute not found.
 * @return ::NC_ENOMEM out of memory.
 * @author Glenn Davis, Ed Hartnett, Dennis Heimbigner
*/
int
nc_inq_attid(int ncid, int varid, const char *name, int *idp)
{
   NC* ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   return ncp->dispatch->inq_attid(ncid, varid, name, idp);
}

/**
 * @ingroup attributes
 * Find the name of an attribute.
 *
 * @param ncid NetCDF or group ID, from a previous call to nc_open(),
 * nc_create(), nc_def_grp(), or associated inquiry functions such as
 * nc_inq_ncid().
 *
 * @param varid Variable ID of the attribute's variable, or
 * ::NC_GLOBAL for a global attribute.
 *
 * @param attnum Attribute number. The attributes for each variable
 * are numbered from 0 (the first attribute) to natts-1, where natts
 * is the number of attributes for the variable, as returned from a
 * call to nc_inq_varnatts().
 *
 * @param name Pointer to the location for the returned attribute \ref
 * object_name.
 *
 * @section nc_inq_attname_example Example
 *
 * Here is an example from nc_test4/tst_atts3.c a variable of every
 * type is added to a file, with names from the 'names' array. Then
 * the file is re-opened, and the names of the attributes are checked
 * in a for loop.
 *
@code
     #include <netcdf.h>
     ...
#define NUM_ATTS 8
#define ATT_MAX_NAME 25
int
tst_att_ordering(int cmode)
{
   int ncid;
   char name[NUM_ATTS][ATT_MAX_NAME + 1] = {"Gc", "Gb", "Gs", "Gi", "Gf",
					    "Gd", "Gatt-name-dashes", "Gatt.name.dots"};
   int len[NUM_ATTS] = {0, 2, 3, 3, 3, 3, 1, 1};
   signed char b[2] = {-128, 127};
   short s[3] = {-32768, 0, 32767};
   int i[3] = {42, 0, -42};
   float f[3] = {42.0, -42.0, 42.0};
   double d[3] = {420.0, -420.0, 420.0};
   int att_name_dashes = -1, att_name_dots = -2;
   char name_in[NC_MAX_NAME];
   int j;

   if (nc_create(FILE_NAME, cmode, &ncid)) ERR;
   ...
   if (nc_put_att_text(ncid, NC_GLOBAL, name[0], len[0], NULL)) ERR;
   if (nc_put_att_schar(ncid, NC_GLOBAL, name[1], NC_BYTE, len[1], b)) ERR;
   if (nc_put_att_short(ncid, NC_GLOBAL, name[2], NC_SHORT, len[2], s)) ERR;
   if (nc_put_att_int(ncid, NC_GLOBAL, name[3], NC_INT, len[3], i)) ERR;
   if (nc_put_att_float(ncid, NC_GLOBAL, name[4], NC_FLOAT, len[4], f)) ERR;
   if (nc_put_att_double(ncid, NC_GLOBAL, name[5], NC_DOUBLE, len[5], d)) ERR;
   if (nc_put_att_int(ncid, NC_GLOBAL, name[6], NC_INT, len[6], &att_name_dashes)) ERR;
   if (nc_put_att_int(ncid, NC_GLOBAL, name[7], NC_INT, len[7], &att_name_dots)) ERR;
   if (nc_close(ncid)) ERR;

   ...
   if (nc_open(FILE_NAME, 0, &ncid)) ERR;
   for (j = 0; j < NUM_ATTS; j++)
   {
      if (nc_inq_attname(ncid, NC_GLOBAL, j, name_in)) ERR;
      if (strcmp(name_in, name[j])) ERR;
   }

   if (nc_close(ncid)) ERR;

@endcode
 *
 * @return ::NC_NOERR no error.
 * @return ::NC_EBADID bad ncid.
 * @return ::NC_ENOTVAR bad varid.
 * @return ::NC_EBADGRPID bad group ID.
 * @return ::NC_EBADNAME bad name.
 * @return ::NC_ENOTATT attribute not found.
 * @return ::NC_ECHAR illegal conversion to or from NC_CHAR.
 * @return ::NC_ENOMEM out of memory.
 * @return ::NC_ERANGE range error when converting data.
 * @author Glenn Davis, Ed Hartnett, Dennis Heimbigner
*/
int
nc_inq_attname(int ncid, int varid, int attnum, char *name)
{
   NC* ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   return ncp->dispatch->inq_attname(ncid, varid, attnum, name);
}

/**
 * @ingroup attributes
 * Find number of global or group attributes.
 *
 * @param ncid NetCDF or group ID, from a previous call to nc_open(),
 * nc_create(), nc_def_grp(), or associated inquiry functions such as
 * nc_inq_ncid().
 *
 * @param nattsp Pointer where number of global or group attributes
 * will be written. \ref ignored_if_null.
 *
 * @section nc_inq_natts_example Example
 *
 * Here is an example from nc_test4/tst_vars.c:
 *
@code
     #include <netcdf.h>
     ...
int
check_4D_example(char *file_name, int expected_format)
{
   int ncid;
   int format, ndims_in, nvars_in, natts_in;
   ...

   if (nc_open(file_name, 0, &ncid)) ERR;
   ...
   if (nc_inq_natts(ncid, &natts_in)) ERR;
   if (natts_in != 0) ERR;
@endcode

 * @return ::NC_NOERR no error.
 * @return ::NC_EBADID bad ncid.
 * @return ::NC_EBADGRPID bad group ID.
 * @author Glenn Davis, Ed Hartnett, Dennis Heimbigner
*/
int
nc_inq_natts(int ncid, int *nattsp)
{
   NC* ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   if(nattsp == NULL) return NC_NOERR;
   return ncp->dispatch->inq(ncid, NULL, NULL, nattsp, NULL);
}

/**
 * @ingroup attributes
 * Find the type of an attribute.
 *
 * @param ncid NetCDF or group ID, from a previous call to nc_open(),
 * nc_create(), nc_def_grp(), or associated inquiry functions such as
 * nc_inq_ncid().
 *
 * @param varid Variable ID of the attribute's variable, or
 * ::NC_GLOBAL for a global or group attribute.
 *
 * @param name Attribute \ref object_name.
 *
 * @param xtypep Pointer to location for returned attribute [data
 * type](https://docs.unidata.ucar.edu/nug/current/md_types.html#data_type).
 *
 * @section nc_inq_atttype_example Example
 *
 * Here is an example from nc_test4/tst_h_refs.c. In this example, a
 * file with an integer attribute is open. It's type is confirmed to
 * be NC_INT.
 *
@code
     #include <netcdf.h>
     ...
    printf("*** Checking accessing file through netCDF-4 API...");
    {
	int ncid, varid, attid;
	nc_type type;

	if (nc_open(FILE_NAME, NC_NOWRITE, &ncid)) ERR;
        ...
	if (nc_inq_atttype(ncid, NC_GLOBAL, INT_ATT_NAME, &type)) ERR;
	if (type != NC_INT) ERR;

@endcode
 *
 * @return ::NC_NOERR no error.
 * @return ::NC_EBADID bad ncid.
 * @return ::NC_ENOTVAR bad varid.
 * @return ::NC_EBADGRPID bad group ID.
 * @return ::NC_EBADNAME bad name.
 * @return ::NC_ENOTATT attribute not found.
 * @return ::NC_ECHAR illegal conversion to or from NC_CHAR.
 * @return ::NC_ENOMEM out of memory.
 * @return ::NC_ERANGE range error when converting data.
 * @author Glenn Davis, Ed Hartnett, Dennis Heimbigner
*/
int
nc_inq_atttype(int ncid, int varid, const char *name, nc_type *xtypep)
{
   NC* ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   return ncp->dispatch->inq_att(ncid, varid, name, xtypep, NULL);
}

/**
 * @ingroup attributes
 * Find the length of an attribute.
 *
 * @param ncid NetCDF or group ID, from a previous call to nc_open(),
 * nc_create(), nc_def_grp(), or associated inquiry functions such as
 * nc_inq_ncid().
 *
 * @param varid Variable ID of the attribute's variable, or
 * ::NC_GLOBAL for a global or group attribute.
 *
 * @param name Attribute \ref object_name.
 *
 * @param lenp Pointer to location for returned number of values
 * currently stored in the attribute. Before using the value as a C
 * string, make sure it is null-terminated. \ref ignored_if_null.
 *
 * @section nc_inq_attlen_example Example
 *
 * Here is an example from nc_test4/tst_h_scalar.c which checks the
 * attributes of an already-open netCDF file. In this code, the length
 * of two attributes are checked, and found to be 1.
 *
@code
     #include <netcdf.h>
     ...
int
check_attrs(int ncid, int obj)
{
    int attid;
    int natts = 0;
    size_t len;
    nc_type type;
    char *vlstr;
    char fixstr[10];
    int x;

    ...
    if (nc_inq_attlen(ncid, obj, VSTR_ATT1_NAME, &len)) ERR_GOTO;
    if (len != 1) ERR_GOTO;
    ...
    if (nc_inq_attlen(ncid, obj, VSTR_ATT2_NAME, &len)) ERR_GOTO;
    if (len != 1) ERR_GOTO;

@endcode

 * @return ::NC_NOERR no error.
 * @return ::NC_EBADID bad ncid.
 * @return ::NC_ENOTVAR bad varid.
 * @return ::NC_EBADGRPID bad group ID.
 * @return ::NC_EBADNAME bad name.
 * @return ::NC_ENOTATT attribute not found.
 * @return ::NC_ECHAR illegal conversion to or from NC_CHAR.
 * @return ::NC_ENOMEM out of memory.
 * @return ::NC_ERANGE range error when converting data.
 * @author Glenn Davis, Ed Hartnett, Dennis Heimbigner
*/
int
nc_inq_attlen(int ncid, int varid, const char *name, size_t *lenp)
{
   NC* ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   return ncp->dispatch->inq_att(ncid, varid, name, NULL, lenp);
}

/*! \} */  /* End of named group ...*/
