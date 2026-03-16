/*! \file
  Functions for User-Defined Types

  Copyright 2018 University Corporation for Atmospheric
  Research/Unidata. See \ref copyright file for more info. */

#include "ncdispatch.h"

/** \defgroup user_types User-Defined Types

User defined types allow for more complex data structures.

NetCDF-4 has added support for four different user defined data
types. User defined type may only be used in files created with the
::NC_NETCDF4 and without ::NC_CLASSIC_MODEL.
- compound type: like a C struct, a compound type is a collection of
types, including other user defined types, in one package.
- variable length array type: used to store ragged arrays.
- opaque type: This type has only a size per element, and no other
  type information.
- enum type: Like an enumeration in C, this type lets you assign text
  values to integer values, and store the integer values.

Users may construct user defined type with the various nc_def_*
functions described in this section. They may learn about user defined
types by using the nc_inq_ functions defined in this section.

Once types are constructed, define variables of the new type with
nc_def_var (see nc_def_var). Write to them with nc_put_var1,
nc_put_var, nc_put_vara, or nc_put_vars. Read data of user-defined
type with nc_get_var1, nc_get_var, nc_get_vara, or nc_get_vars (see
\ref variables).

Create attributes of the new type with nc_put_att (see nc_put_att_
type). Read attributes of the new type with nc_get_att (see
\ref attributes).
*/

/** \{ */


/** 
\ingroup user_types
Learn if two types are equal.

\note User-defined types in netCDF-4/HDF5 files must be committed to
the file before nc_inq_type_equal() will work on the type. For
uncommitted user-defined types, nc_inq_type_equal() will return
::NC_EHDFERR. Commit types to the file with a call to nc_enddef().

\param ncid1 \ref ncid of first typeid.
\param typeid1 First typeid.
\param ncid2 \ref ncid of second typeid.
\param typeid2 Second typeid.
\param equal Pointer to int. A non-zero value will be copied here if
the two types are equal, a zero if they are not equal.

\returns ::NC_NOERR No error.
\returns ::NC_EBADID Bad \ref ncid.
\returns ::NC_EBADTYPE Bad type id.
\returns ::NC_ENOTNC4 Not an netCDF-4 file, or classic model enabled.
\returns ::NC_EHDFERR An error was reported by the HDF5 layer. This
will occur if either of the types have not been committed to the file
(with an nc_enddef()).

\author Dennis Heimbigner, Ward Fisher, Ed Hartnett
 */
int
nc_inq_type_equal(int ncid1, nc_type typeid1, int ncid2,
		  nc_type typeid2, int *equal)
{
    NC* ncp1;
    int stat = NC_check_id(ncid1,&ncp1);
    if(stat != NC_NOERR) return stat;
    return ncp1->dispatch->inq_type_equal(ncid1,typeid1,ncid2,typeid2,equal);
}

/** \name Learning about User-Defined Types

    Functions to learn about any kind of user-defined type. */
/*! \{ */ /* All these functions are part of this named group... */

/** \ingroup user_types

Find a type by name. Given a group ID and a type name, find the ID of
the type. If the type is not found in the group, then the parents are
searched. If still not found, the entire file is searched.

\param ncid \ref ncid
\param name \ref object_name of type to search for.
\param typeidp Typeid of named type will be copied here, if it is
found.

\returns ::NC_NOERR No error.
\returns ::NC_EBADID Bad \ref ncid.
\returns ::NC_EBADTYPE Bad type id.
\returns ::NC_ENOTNC4 Not an netCDF-4 file, or classic model enabled.
\returns ::NC_EHDFERR An error was reported by the HDF5 layer.
\author Ed Hartnett, Dennis Heimbigner
 */
int
nc_inq_typeid(int ncid, const char *name, nc_type *typeidp)
{
    NC* ncp;
    int stat = NC_check_id(ncid,&ncp);
    if(stat != NC_NOERR) return stat;
    return ncp->dispatch->inq_typeid(ncid,name,typeidp);
}

/** \ingroup user_types
Learn about a user defined type.

Given an ncid and a typeid, get the information about a user defined
type. This function will work on any user defined type, whether
compound, opaque, enumeration, or variable length array.

\param ncid \ref ncid

\param xtype The typeid

\param name The \ref object_name will be copied here. \ref
ignored_if_null.

\param size the (in-memory) size of the type in bytes will be copied
here. VLEN type size is the size of nc_vlen_t. String size is returned
as the size of a character pointer. The size may be used to malloc
space for the data, no matter what the type. \ref ignored_if_null.

\param base_nc_typep The base type will be copied here for enum and
VLEN types. \ref ignored_if_null.

\param nfieldsp The number of fields will be copied here for enum and
compound types. \ref ignored_if_null.

\param classp Return the class of the user defined type, ::NC_VLEN,
::NC_OPAQUE, ::NC_ENUM, or ::NC_COMPOUND. \ref ignored_if_null.

\returns ::NC_NOERR No error.
\returns ::NC_EBADID Bad \ref ncid.
\returns ::NC_EBADTYPE Bad type id.
\returns ::NC_ENOTNC4 Not an netCDF-4 file, or classic model enabled.
\returns ::NC_EHDFERR An error was reported by the HDF5 layer.
\author Ed Hartnett, Dennis Heimbigner
 */
int
nc_inq_user_type(int ncid, nc_type xtype, char *name, size_t *size,
		 nc_type *base_nc_typep, size_t *nfieldsp, int *classp)
{
    NC *ncp;
    int stat = NC_check_id(ncid,&ncp);
    if(stat != NC_NOERR) return stat;
    return ncp->dispatch->inq_user_type(ncid, xtype, name, size,
					base_nc_typep, nfieldsp, classp);
}
/*! \} */  /* End of named group ...*/

/** \} */
