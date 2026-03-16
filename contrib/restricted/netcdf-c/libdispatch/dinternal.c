/*! \internal
Public functions from dispatch table.

Copyright 2018 University Corporation for Atmospheric
Research/Unidata. See COPYRIGHT file for more info.
*/

#include "ncdispatch.h"

/**
Certain functions are in the dispatch table,
but not in the netcdf.h API. These need to
be exposed for use in delegation such as
in libdap2.
*/

int
NCDISPATCH_inq_var_all(int ncid, int varid, char *name, nc_type *xtypep,
               int *ndimsp, int *dimidsp, int *nattsp,
               int *shufflep, int *deflatep, int *deflate_levelp,
               int *fletcher32p, int *contiguousp, size_t *chunksizesp,
               int *no_fill, void *fill_valuep, int *endiannessp,
	       unsigned int* idp, size_t* nparamsp, unsigned int* params
               )
{
   NC* ncp;
   int stat = NC_check_id(ncid,&ncp);
   if(stat != NC_NOERR) return stat;
   return ncp->dispatch->inq_var_all(
      ncid, varid, name, xtypep,
      ndimsp, dimidsp, nattsp,
      shufflep, deflatep, deflate_levelp, fletcher32p,
      contiguousp, chunksizesp,
      no_fill, fill_valuep,
      endiannessp,
      idp, nparamsp, params);
}

int
NCDISPATCH_get_att(int ncid, int varid, const char* name, void* value, nc_type t)
{
   NC* ncp;
   int stat = NC_check_id(ncid,&ncp);
   if(stat != NC_NOERR) return stat;
   return ncp->dispatch->get_att(ncid,varid,name,value,t);
}

/*! \} */  /* End of named group ...*/
