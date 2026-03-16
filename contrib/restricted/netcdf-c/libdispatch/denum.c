/*! \file
  Functions for Enum Types

  Copyright 2018 University Corporation for Atmospheric
  Research/Unidata. See \ref copyright file for more info. */

#include "ncdispatch.h"

/** \name Enum Types
    Functions to create and learn about enum types. */
/*! \{ */ /* All these functions are part of this named group... */

/** \ingroup user_types
Create an enum type. Provide an ncid, a name, and a base integer type.

After calling this function, fill out the type with repeated calls to
nc_insert_enum(). Call nc_insert_enum() once for each value you wish
to make part of the enumeration.

\param ncid \ref ncid

\param base_typeid The base integer type for this enum. Must be one
of: ::NC_BYTE, ::NC_UBYTE, ::NC_SHORT, ::NC_USHORT, ::NC_INT,
::NC_UINT, ::NC_INT64, ::NC_UINT64.

\param name \ref object_name of new type.

\param typeidp A pointer to an nc_type. The typeid of the new type
will be placed there.

\returns ::NC_NOERR No error.
\returns ::NC_EBADID Bad \ref ncid.
\returns ::NC_EBADTYPE Bad type id.
\returns ::NC_ENOTNC4 Not an netCDF-4 file, or classic model enabled.
\returns ::NC_EHDFERR An error was reported by the HDF5 layer.
\returns ::NC_ENAMEINUSE That name is in use.
\returns ::NC_EMAXNAME Name exceeds max length NC_MAX_NAME.
\returns ::NC_EBADNAME Name contains illegal characters.
\returns ::NC_EPERM Attempt to write to a read-only file.
\returns ::NC_ENOTINDEFINE Not in define mode. 
 */
int
nc_def_enum(int ncid, nc_type base_typeid, const char *name, nc_type *typeidp)
{
    NC* ncp;
    int stat = NC_check_id(ncid,&ncp);
    if(stat != NC_NOERR) return stat;
    return ncp->dispatch->def_enum(ncid,base_typeid,name,typeidp);
}

/** \ingroup user_types
Insert a named member into a enum type. 

\param ncid \ref ncid
\param xtype
\param name The identifier (\ref object_name) of the new member. 
\param value The value that is to be associated with this member. 

\returns ::NC_NOERR No error.
\returns ::NC_EBADID Bad \ref ncid.
\returns ::NC_EBADTYPE Bad type id.
\returns ::NC_ENOTNC4 Not an netCDF-4 file, or classic model enabled.
\returns ::NC_EHDFERR An error was reported by the HDF5 layer.
\returns ::NC_ENAMEINUSE That name is in use.
\returns ::NC_EMAXNAME Name exceeds max length NC_MAX_NAME.
\returns ::NC_EBADNAME Name contains illegal characters.
\returns ::NC_EPERM Attempt to write to a read-only file.
\returns ::NC_ENOTINDEFINE Not in define mode. 
 */
int
nc_insert_enum(int ncid, nc_type xtype, const char *name, 
	       const void *value)
{
    NC *ncp;
    int stat = NC_check_id(ncid, &ncp);
    if(stat != NC_NOERR) return stat;
    return ncp->dispatch->insert_enum(ncid, xtype, name,
				      value);
}

/** \ingroup user_types
Learn about a user-define enumeration type. 

\param ncid \ref ncid

\param xtype Typeid to inquire about.

\param name \ref object_name of type will be copied here. \ref
ignored_if_null.

\param base_nc_typep Typeid if the base type of the enum.\ref
ignored_if_null.

\param base_sizep Pointer that will get the size in bytes of the base
type. \ref ignored_if_null.

\param num_membersp Pointer that will get the number of members
defined for this enum type. \ref ignored_if_null.

\returns ::NC_NOERR No error.
\returns ::NC_EBADID Bad \ref ncid.
\returns ::NC_EBADTYPE Bad type id.
\returns ::NC_ENOTNC4 Not an netCDF-4 file, or classic model enabled.
\returns ::NC_EHDFERR An error was reported by the HDF5 layer.
 */
int
nc_inq_enum(int ncid, nc_type xtype, char *name, nc_type *base_nc_typep, 
	    size_t *base_sizep, size_t *num_membersp)
{
    int class = 0;
    int stat = nc_inq_user_type(ncid, xtype, name, base_sizep, 
				base_nc_typep, num_membersp, &class);
    if(stat != NC_NOERR) return stat;
    if(class != NC_ENUM) stat = NC_EBADTYPE;
    return stat;
}

/** \ingroup user_types
Learn about a about a member of an enum type. 

\param ncid \ref ncid

\param xtype Typeid of the enum type.

\param idx Index to the member to inquire about.

\param name The identifier (\ref object_name) of this member will be
copied here. \ref ignored_if_null.

\param value The value of this member will be copied here. \ref
ignored_if_null.

\returns ::NC_NOERR No error.
\returns ::NC_EBADID Bad \ref ncid.
\returns ::NC_EBADTYPE Bad type id.
\returns ::NC_ENOTNC4 Not an netCDF-4 file, or classic model enabled.
\returns ::NC_EHDFERR An error was reported by the HDF5 layer.
 */
int
nc_inq_enum_member(int ncid, nc_type xtype, int idx, char *name, 
		   void *value)
{
    NC *ncp;
    int stat = NC_check_id(ncid, &ncp);
    if(stat != NC_NOERR) return stat;
    return ncp->dispatch->inq_enum_member(ncid, xtype, idx, name, value);
}

/** \ingroup user_types
Get the name which is associated with an enum member value. 

\param ncid
\param xtype Typeid of the enum type.
\param value Value of interest.
\param identifier The identifier (\ref object_name) of this value will be copied here; ignored_if_null.

\returns ::NC_NOERR No error.
\returns ::NC_EBADID Bad \ref ncid.
\returns ::NC_EBADTYPE Bad type id.
\returns ::NC_ENOTNC4 Not an netCDF-4 file, or classic model enabled.
\returns ::NC_EHDFERR An error was reported by the HDF5 layer.
 */
int
nc_inq_enum_ident(int ncid, nc_type xtype, long long value, 
		  char *identifier)
{
    NC* ncp;
    int stat = NC_check_id(ncid,&ncp);
    if(stat != NC_NOERR) return stat;
    return ncp->dispatch->inq_enum_ident(ncid,xtype,value,identifier);
}
/*! \} */  /* End of named group ...*/
