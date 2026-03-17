/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT dap for more information. */

/*
Internal library debugging interface
(undocumented)
*/

#ifndef OCX_H
#define OCX_H

/* Declaration modifiers for DLL support (MSC et al) */
#if defined(DLL_NETCDF) /* define when library is a DLL */
#  if defined(DLL_EXPORT) /* define when building the library */
#   define MSC_EXTRA __declspec(dllexport)
#  else
#   define MSC_EXTRA __declspec(dllimport)
#  endif
#  include <io.h>
#else
#define MSC_EXTRA  /**< Needed for DLL build. */
#endif  /* defined(DLL_NETCDF) */

#define EXTERNL MSC_EXTRA extern /**< Needed for DLL build. */

/**************************************************/
/* Flags defining the structure of an OCdata object */

/* Must be consistent with ocutil.c.ocdtmodestring */
typedef unsigned int OCDT;
#define OCDT_FIELD     ((OCDT)(1)) /* field of a container */
#define OCDT_ELEMENT   ((OCDT)(2)) /* element of a structure array */
#define OCDT_RECORD    ((OCDT)(4)) /* record of a sequence */
#define OCDT_ARRAY     ((OCDT)(8)) /* is structure array */
#define OCDT_SEQUENCE  ((OCDT)(16)) /* is sequence */
#define OCDT_ATOMIC    ((OCDT)(32)) /* is atomic leaf */

/* Return mode for this data */
extern OCerror oc_data_mode(OClink, OCdatanode, OCDT* modep);

EXTERNL OCerror oc_dds_dd(OClink, OCddsnode, int);
EXTERNL OCerror oc_dds_ddnode(OClink, OCddsnode);
EXTERNL OCerror oc_data_ddpath(OClink, OCdatanode, char**);
EXTERNL OCerror oc_data_ddtree(OClink, OCdatanode root);

#endif /*OCX_H*/
