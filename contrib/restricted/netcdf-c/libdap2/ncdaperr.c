/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "dapincludes.h"

NCerror
ocerrtoncerr(OCerror ocerr)
{
    if(ocerr > 0) return ocerr; /* really a system error*/
    switch (ocerr) {
    case OC_NOERR:	  return NC_NOERR;
    case OC_EBADID:	  return NC_EBADID;
    case OC_ECHAR:	  return NC_ECHAR;
    case OC_EDIMSIZE:	  return NC_EDIMSIZE;
    case OC_EEDGE:	  return NC_EEDGE;
    case OC_EINVAL:	  return NC_EINVAL;
    case OC_EINVALCOORDS: return NC_EINVALCOORDS;
    case OC_ENOMEM:	  return NC_ENOMEM;
    case OC_ENOTVAR:	  return NC_ENOTVAR;
    case OC_EPERM:	  return NC_EPERM;
    case OC_ESTRIDE:	  return NC_ESTRIDE;
    case OC_EDAP:	  return NC_EDAP;
    case OC_EXDR:	  return NC_EDAP;
    case OC_ECURL:	  return NC_EIO;
    case OC_EBADURL:	  return NC_EDAPURL;
    case OC_EBADVAR:	  return NC_EDAP;
    case OC_EOPEN:	  return NC_EIO;
    case OC_EIO:	  return NC_EIO;
    case OC_ENODATA:	  return NC_ENODATA;
    case OC_EDAPSVC:	  return NC_EDAPSVC;
    case OC_ENAMEINUSE:	  return NC_ENAMEINUSE;
    case OC_EDAS:	  return NC_EDAS;
    case OC_EDDS:	  return NC_EDDS;
    case OC_EDATADDS:	  return NC_EDATADDS;
    case OC_ERCFILE:	  return NC_EDAP;
    case OC_ENOFILE:	  return NC_ECANTREAD;
    case OC_EAUTH:	  return NC_EAUTH;
    case OC_EACCESS:	  return NC_EACCESS;
    default: break;
    }
    return NC_EDAP; /* default;*/
}
