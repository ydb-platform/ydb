/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/
#ifndef NCH5DEBUG_H
#define NCH5DEBUG_H

/* Warning: significant performance impact */
#undef H5CATCH

#undef H5BACKTRACE

#ifdef H5CATCH
/* Place breakpoint to catch errors close to where they occur*/
/* Warning: do not evaluate e more than once */
#define THROW(e) nch5throw(e,__LINE__)
#define THROWCHK(e) (void)nch5throw(e)
extern int nch5breakpoint(int err);
extern int nch5throw(int err, int line);
#else
#define THROW(e) (e)
#define THROWCHK(e)
#endif

#endif /*NCH5DEBUG_H*/

