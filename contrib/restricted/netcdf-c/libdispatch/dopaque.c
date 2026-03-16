/*! \file
  Functions for Opaque Types

  Copyright 2018 University Corporation for Atmospheric
  Research/Unidata. See \ref copyright file for more info. */

#include "ncdispatch.h"

/** \name Opaque Types
    Functions to create and learn about opaque types. */
/*! \{ */ /* All these functions are part of this named group... */

/** \ingroup user_types
Create an opaque type. Provide a size and a name.

\param ncid \ref ncid
\param size The size of each opaque object in bytes.
\param name \ref object_name of the new type.
\param xtypep Pointer where the new typeid for this type is returned.

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
nc_def_opaque(int ncid, size_t size, const char *name, nc_type *xtypep)
{
    NC* ncp;
    int stat = NC_check_id(ncid,&ncp);
    if(stat != NC_NOERR) return stat;
    return ncp->dispatch->def_opaque(ncid,size,name,xtypep);
}

/** \ingroup user_types
Learn about an opaque type.

\param ncid \ref ncid

\param xtype Typeid to inquire about.

\param name The \ref object_name of this type will be
copied here. \ref ignored_if_null.

\param sizep The size of the type will be copied here. \ref
ignored_if_null.

\returns ::NC_NOERR No error.
\returns ::NC_EBADID Bad \ref ncid.
\returns ::NC_EBADTYPE Bad type id.
\returns ::NC_ENOTNC4 Not an netCDF-4 file, or classic model enabled.
\returns ::NC_EHDFERR An error was reported by the HDF5 layer.
 */
int
nc_inq_opaque(int ncid, nc_type xtype, char *name, size_t *sizep)
{
    int class = 0;
    int stat = nc_inq_user_type(ncid,xtype,name,sizep,NULL,NULL,&class);
    if(stat != NC_NOERR) return stat;
    if(class != NC_OPAQUE) stat = NC_EBADTYPE;
    return stat;
}

/*! \} */  /* End of named group ...*/
