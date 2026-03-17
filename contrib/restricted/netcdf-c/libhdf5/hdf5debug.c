/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/
#include "config.h"
#include <stdarg.h>
#include <stdio.h>
#ifdef HAVE_EXECINFO_H
#include <execinfo.h>
#endif

#include "nclog.h"
#include "hdf5debug.h"

#ifdef H5CATCH

int
nch5breakpoint(int err)
{
    return ncbreakpoint(err);
}

int
nch5throw(int err, int line)
{
    if(err == 0) return err;
    fprintf(stderr,">>> hdf5throw: line=%d err=%d\n",line,err); fflush(stderr);
    return nch5breakpoint(err);
}
#endif

