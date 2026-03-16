/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *   $Header$
 *********************************************************************/

#ifndef NCEXTERNL_H
#define NCEXTERNL_H

#if defined(DLL_NETCDF) /* define when library is a DLL */
#  if defined(DLL_EXPORT) /* define when building the library */
#   define MSC_EXTRA __declspec(dllexport)
#  else
#   define MSC_EXTRA __declspec(dllimport)
#  endif
#else
#  define MSC_EXTRA
#endif	/* defined(DLL_NETCDF) */
#ifndef EXTERNL
# define EXTERNL MSC_EXTRA extern
#endif

#endif /*NCEXTERNL_H*/
