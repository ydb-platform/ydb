/*! \file
  Functions for Compound Types

  Copyright 2018 University Corporation for Atmospheric
  Research/Unidata. See \ref copyright file for more info. */

#include "ncdispatch.h"

/** \name Compound Types
    Functions to create and learn about compound types. */
/*! \{ */ /* All these functions are part of this named group... */


/** \ingroup user_types

Create a compound type. Provide an ncid, a name, and a total size (in
bytes) of one element of the completed compound type.

After calling this function, fill out the type with repeated calls to
nc_insert_compound(). Call nc_insert_compound() once for each field
you wish to insert into the compound type.

\param ncid \ref ncid
\param size The size, in bytes, of the compound type.
\param name \ref object_name of the created type.
\param typeidp The type ID of the new type is copied here.

\returns ::NC_NOERR No error.
\returns ::NC_EBADID Bad \ref ncid.
\returns ::NC_EBADTYPE Bad type id.
\returns ::NC_ENAMEINUSE That name is in use.
\returns ::NC_EMAXNAME Name exceeds max length NC_MAX_NAME.
\returns ::NC_EBADNAME Name contains illegal characters.
\returns ::NC_ESTRICTNC3 Attempting a netCDF-4 operation on a netCDF-3 file.
\returns ::NC_ENOTNC4 This file was created with the strict netcdf-3 flag, therefore netcdf-4 operations are not allowed. (see nc_open).
\returns ::NC_EHDFERR An error was reported by the HDF5 layer.
\returns ::NC_EPERM Attempt to write to a read-only file.
\returns ::NC_ENOTINDEFINE Not in define mode.

\section nc_def_compound_example Example

\code
struct s1
{
int i1;
int i2;
};
struct s1 data[DIM_LEN], data_in[DIM_LEN];

if (nc_create(FILE_NAME, NC_NETCDF4, &ncid)) ERR;
if (nc_def_compound(ncid, sizeof(struct s1), SVC_REC, &typeid)) ERR;
if (nc_insert_compound(ncid, typeid, BATTLES_WITH_KLINGONS,
HOFFSET(struct s1, i1), NC_INT)) ERR;
if (nc_insert_compound(ncid, typeid, DATES_WITH_ALIENS,
HOFFSET(struct s1, i2), NC_INT)) ERR;
if (nc_def_dim(ncid, STARDATE, DIM_LEN, &dimid)) ERR;
if (nc_def_var(ncid, SERVICE_RECORD, typeid, 1, dimids, &varid)) ERR;
if (nc_put_var(ncid, varid, data)) ERR;
if (nc_close(ncid)) ERR;
\endcode
*/
int
nc_def_compound(int ncid, size_t size, const char *name,
		nc_type *typeidp)
{
   NC* ncp;
   int stat = NC_check_id(ncid,&ncp);
   if(stat != NC_NOERR) return stat;
   return ncp->dispatch->def_compound(ncid,size,name,typeidp);
}

/** \ingroup user_types
Insert a named field into a compound type.

\param ncid \ref ncid

\param xtype The typeid for this compound type, as returned by
nc_def_compound(), or nc_inq_var().

\param name The \ref object_name of the new field.

\param offset Offset in byte from the beginning of the compound type
for this field.

\param field_typeid The type of the field to be inserted.

\returns ::NC_NOERR No error.
\returns ::NC_EBADID Bad \ref ncid.
\returns ::NC_EBADTYPE Bad type id.
\returns ::NC_ENAMEINUSE That name is in use.
\returns ::NC_EMAXNAME Name exceeds max length NC_MAX_NAME.
\returns ::NC_EBADNAME Name contains illegal characters.
\returns ::NC_ENOTNC4 Not an netCDF-4 file, or classic model enabled.
\returns ::NC_EHDFERR An error was reported by the HDF5 layer.
\returns ::NC_EPERM Attempt to write to a read-only file.
\returns ::NC_ENOTINDEFINE Not in define mode.
*/
int
nc_insert_compound(int ncid, nc_type xtype, const char *name,
		   size_t offset, nc_type field_typeid)
{
   NC *ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   return ncp->dispatch->insert_compound(ncid, xtype, name,
					 offset, field_typeid);
}

/** \ingroup user_types
Insert a named array field into a compound type.

\param ncid \ref ncid

\param xtype The typeid for this compound type, as returned by
nc_def_compound(), or nc_inq_var().

\param name The \ref object_name of the new field.

\param offset Offset in byte from the beginning of the compound type
for this field.

\param field_typeid The type of the field to be inserted.

 \param ndims Number of dimensions in array.

 \param dim_sizes Array of dimension sizes.

\returns ::NC_NOERR No error.
\returns ::NC_EBADID Bad \ref ncid.
\returns ::NC_EBADTYPE Bad type id.
\returns ::NC_ENAMEINUSE That name is in use.
\returns ::NC_EMAXNAME Name exceeds max length NC_MAX_NAME.
\returns ::NC_EBADNAME Name contains illegal characters.
\returns ::NC_ESTRICTNC3 Attempting a netCDF-4 operation on a netCDF-3 file.
\returns ::NC_ENOTNC4 Not an netCDF-4 file, or classic model enabled.
\returns ::NC_EHDFERR An error was reported by the HDF5 layer.
\returns ::NC_EPERM Attempt to write to a read-only file.
\returns ::NC_ENOTINDEFINE Not in define mode.
*/
int
nc_insert_array_compound(int ncid, nc_type xtype, const char *name,
			 size_t offset, nc_type field_typeid,
			 int ndims, const int *dim_sizes)
{
   NC* ncp;
   int stat = NC_check_id(ncid,&ncp);
   if(stat != NC_NOERR) return stat;
   return ncp->dispatch->insert_array_compound(ncid,xtype,name,offset,field_typeid,ndims,dim_sizes);
}

/**  \ingroup user_types
Learn about a compound type. Get the number of fields, len, and
name of a compound type.

\param ncid \ref ncid

\param xtype The typeid for this compound type, as returned by
nc_def_compound(), or nc_inq_var().

\param name Returned \ref object_name of compound type. \ref
ignored_if_null.

\param sizep Returned size of compound type in bytes. \ref ignored_if_null.

\param nfieldsp The number of fields in the compound type will be
placed here. \ref ignored_if_null.

\returns ::NC_NOERR No error.
\returns ::NC_EBADID Bad \ref ncid.
\returns ::NC_EBADTYPE Bad type id.
\returns ::NC_ENOTNC4 Not an netCDF-4 file, or classic model enabled.
\returns ::NC_EHDFERR An error was reported by the HDF5 layer.
 */
int
nc_inq_compound(int ncid, nc_type xtype, char *name,
		size_t *sizep, size_t *nfieldsp)
{
   int class = 0;
   int stat = nc_inq_user_type(ncid,xtype,name,sizep,NULL,nfieldsp,&class);
   if(stat != NC_NOERR) return stat;
   if(class != NC_COMPOUND) stat = NC_EBADTYPE;
   return stat;
}

/**  \ingroup user_types
Learn the name of a compound type.

\param ncid \ref ncid

\param xtype The typeid for this compound type, as returned by
nc_def_compound(), or nc_inq_var().

\param name Returned \ref object_name of compound type. \ref
ignored_if_null.

\returns ::NC_NOERR No error.
\returns ::NC_EBADID Bad \ref ncid.
\returns ::NC_EBADTYPE Bad type id.
\returns ::NC_ENOTNC4 Not an netCDF-4 file, or classic model enabled.
\returns ::NC_EHDFERR An error was reported by the HDF5 layer.
 */
int
nc_inq_compound_name(int ncid, nc_type xtype, char *name)
{
   return nc_inq_compound(ncid,xtype,name,NULL,NULL);
}

/**  \ingroup user_types
Learn the size of a compound type.

\param ncid \ref ncid

\param xtype The typeid for this compound type, as returned by
nc_def_compound(), or nc_inq_var().

\param sizep Returned size of compound type in bytes. \ref
ignored_if_null.

\returns ::NC_NOERR No error.
\returns ::NC_EBADID Bad \ref ncid.
\returns ::NC_EBADTYPE Bad type id.
\returns ::NC_ENOTNC4 Not an netCDF-4 file, or classic model enabled.
\returns ::NC_EHDFERR An error was reported by the HDF5 layer.
 */
int
nc_inq_compound_size(int ncid, nc_type xtype, size_t *sizep)
{
   return nc_inq_compound(ncid,xtype,NULL,sizep,NULL);
}

/**  \ingroup user_types
Learn the number of fields in a compound type.

\param ncid \ref ncid

\param xtype The typeid for this compound type, as returned by
nc_def_compound(), or nc_inq_var().

\param nfieldsp The number of fields in the compound type will be
placed here. \ref ignored_if_null.

\returns ::NC_NOERR No error.
\returns ::NC_EBADID Bad \ref ncid.
\returns ::NC_EBADTYPE Bad type id.
\returns ::NC_ENOTNC4 Not an netCDF-4 file, or classic model enabled.
\returns ::NC_EHDFERR An error was reported by the HDF5 layer.
 */
int
nc_inq_compound_nfields(int ncid, nc_type xtype, size_t *nfieldsp)
{
   return nc_inq_compound(ncid,xtype,NULL,NULL,nfieldsp);
}

/**  \ingroup user_types
Get information about one of the fields of a compound type.

\param ncid \ref ncid

\param xtype The typeid for this compound type, as returned by
nc_def_compound(), or nc_inq_var().

\param fieldid A zero-based index number specifying a field in the
compound type.

\param name Returned \ref object_name of the field. \ref
ignored_if_null.

\param offsetp A pointer which will get the offset of the field. \ref
ignored_if_null.

\param field_typeidp A pointer which will get the typeid of the
field. \ref ignored_if_null.

\param ndimsp A pointer which will get the number of dimensions of the
field. \ref ignored_if_null.

\param dim_sizesp A pointer which will get the dimension sizes of the
field. \ref ignored_if_null.

\returns ::NC_NOERR No error.
\returns ::NC_EBADID Bad \ref ncid.
\returns ::NC_EBADTYPE Bad type id.
\returns ::NC_ENOTNC4 Not an netCDF-4 file, or classic model enabled.
\returns ::NC_EHDFERR An error was reported by the HDF5 layer.
 */
int
nc_inq_compound_field(int ncid, nc_type xtype, int fieldid,
		      char *name, size_t *offsetp,
		      nc_type *field_typeidp, int *ndimsp,
		      int *dim_sizesp)
{
   NC* ncp;
   int stat = NC_check_id(ncid,&ncp);
   if(stat != NC_NOERR) return stat;
   return ncp->dispatch->inq_compound_field(ncid, xtype, fieldid,
					    name, offsetp, field_typeidp,
					    ndimsp, dim_sizesp);
}

/**  \ingroup user_types
Get information about one of the fields of a compound type.

\param ncid \ref ncid

\param xtype The typeid for this compound type, as returned by
nc_def_compound(), or nc_inq_var().

\param fieldid A zero-based index number specifying a field in the
compound type.

\param name Returned \ref object_name of the field. \ref
ignored_if_null.

\returns ::NC_NOERR No error.
\returns ::NC_EBADID Bad \ref ncid.
\returns ::NC_EBADTYPE Bad type id.
\returns ::NC_ENOTNC4 Not an netCDF-4 file, or classic model enabled.
\returns ::NC_EHDFERR An error was reported by the HDF5 layer.
 */
int
nc_inq_compound_fieldname(int ncid, nc_type xtype, int fieldid,
			  char *name)
{
   NC* ncp;
   int stat = NC_check_id(ncid,&ncp);
   if(stat != NC_NOERR) return stat;
   return ncp->dispatch->inq_compound_field(ncid, xtype, fieldid,
					    name, NULL, NULL, NULL,
					    NULL);
}

/**  \ingroup user_types
Get information about one of the fields of a compound type.

\param ncid \ref ncid

\param xtype The typeid for this compound type, as returned by
nc_def_compound(), or nc_inq_var().

\param fieldid A zero-based index number specifying a field in the
compound type.

\param offsetp A pointer which will get the offset of the field. \ref
ignored_if_null.

\returns ::NC_NOERR No error.
\returns ::NC_EBADID Bad \ref ncid.
\returns ::NC_EBADTYPE Bad type id.
\returns ::NC_ENOTNC4 Not an netCDF-4 file, or classic model enabled.
\returns ::NC_EHDFERR An error was reported by the HDF5 layer.
 */
int
nc_inq_compound_fieldoffset(int ncid, nc_type xtype, int fieldid,
			    size_t *offsetp)
{
   NC* ncp;
   int stat = NC_check_id(ncid,&ncp);
   if(stat != NC_NOERR) return stat;
   return ncp->dispatch->inq_compound_field(ncid,xtype,fieldid,NULL,offsetp,NULL,NULL,NULL);
}

/**  \ingroup user_types
Get information about one of the fields of a compound type.

\param ncid \ref ncid

\param xtype The typeid for this compound type, as returned by
nc_def_compound(), or nc_inq_var().

\param fieldid A zero-based index number specifying a field in the
compound type.

\param field_typeidp A pointer which will get the typeid of the
field. \ref ignored_if_null.

\returns ::NC_NOERR No error.
\returns ::NC_EBADID Bad \ref ncid.
\returns ::NC_EBADTYPE Bad type id.
\returns ::NC_ENOTNC4 Not an netCDF-4 file, or classic model enabled.
\returns ::NC_EHDFERR An error was reported by the HDF5 layer.
 */
int
nc_inq_compound_fieldtype(int ncid, nc_type xtype, int fieldid,
			  nc_type *field_typeidp)
{
   NC* ncp;
   int stat = NC_check_id(ncid,&ncp);
   if(stat != NC_NOERR) return stat;
   return ncp->dispatch->inq_compound_field(ncid,xtype,fieldid,NULL,NULL,field_typeidp,NULL,NULL);
}

/**  \ingroup user_types
Get information about one of the fields of a compound type.

\param ncid \ref ncid

\param xtype The typeid for this compound type, as returned by
nc_def_compound(), or nc_inq_var().

\param fieldid A zero-based index number specifying a field in the
compound type.

\param ndimsp A pointer which will get the number of dimensions of the
field. \ref ignored_if_null.

\returns ::NC_NOERR No error.
\returns ::NC_EBADID Bad \ref ncid.
\returns ::NC_EBADTYPE Bad type id.
\returns ::NC_ENOTNC4 Not an netCDF-4 file, or classic model enabled.
\returns ::NC_EHDFERR An error was reported by the HDF5 layer.
 */
int
nc_inq_compound_fieldndims(int ncid, nc_type xtype, int fieldid,
			   int *ndimsp)
{
   NC* ncp;
   int stat = NC_check_id(ncid,&ncp);
   if(stat != NC_NOERR) return stat;
   return ncp->dispatch->inq_compound_field(ncid,xtype,fieldid,NULL,NULL,NULL,ndimsp,NULL);
}

/**  \ingroup user_types
Get information about one of the fields of a compound type.

\param ncid \ref ncid

\param xtype The typeid for this compound type, as returned by
nc_def_compound(), or nc_inq_var().

\param fieldid A zero-based index number specifying a field in the
compound type.

\param dim_sizesp A pointer which will get the dimension sizes of the
field. \ref ignored_if_null.

\returns ::NC_NOERR No error.
\returns ::NC_EBADID Bad \ref ncid.
\returns ::NC_EBADTYPE Bad type id.
\returns ::NC_ENOTNC4 Not an netCDF-4 file, or classic model enabled.
\returns ::NC_EHDFERR An error was reported by the HDF5 layer.
 */
int
nc_inq_compound_fielddim_sizes(int ncid, nc_type xtype, int fieldid,
			       int *dim_sizesp)
{
   NC *ncp;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   return ncp->dispatch->inq_compound_field(ncid, xtype, fieldid,
					    NULL, NULL, NULL, NULL,
					    dim_sizesp);
}

/**  \ingroup user_types
Learn the Index of a Named Field in a Compound Type. Get the index
 * of a field in a compound type from the name.

\param ncid \ref ncid

\param xtype The typeid for this compound type, as returned by
nc_def_compound(), or nc_inq_var().

\param name \ref object_name of the field. \ref ignored_if_null.

\param fieldidp A pointer which will get the index of the named
field. \ref ignored_if_null.

\returns ::NC_NOERR No error.
\returns ::NC_EBADID Bad \ref ncid.
\returns ::NC_EBADTYPE Bad type id.
\returns ::NC_ENOTNC4 Not an netCDF-4 file, or classic model enabled.
\returns ::NC_EHDFERR An error was reported by the HDF5 layer.
 */
int
nc_inq_compound_fieldindex(int ncid, nc_type xtype, const char *name,
			   int *fieldidp)
{
   NC* ncp;
   int stat = NC_check_id(ncid,&ncp);
   if(stat != NC_NOERR) return stat;
   return ncp->dispatch->inq_compound_fieldindex(ncid,xtype,name,fieldidp);
}
/*! \} */  /* End of named group ...*/
