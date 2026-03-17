/*********************************************************************
 *   Copyright 2018, University Corporation for Atmospheric Research
 *   See netcdf/README file for copying and redistribution conditions.
 *   Russ Rew
 *********************************************************************/
#ifndef _ISNAN_H
#define _ISNAN_H

#include "config.h"

#ifndef NO_FLOAT_H
#include <float.h>		/* for DBL_MAX */
#endif /* NO_FLOAT_H */
#include <math.h>

#ifndef NAN
#error "NAN undefined"
#endif
#ifndef INFINITY
#error "INFINITY undefined"
#endif
#ifndef NANF
#define NANF ((float)NAN)
#endif
#ifndef INFINITYF
#define INFINITYF ((float)INFINITY)
#endif
#if ! (defined(isinf) || HAVE_DECL_ISINF)
#define isinf(x) (DBL_MAX/((double)(x))==0.0)
#endif /* !HAVE_DECL_ISINF */
#if ! (defined(isnan) || HAVE_DECL_ISNAN)
#define isnan(x) ((x)!=(x))
#endif /* !HAVE_DECL_ISNAN */
#if ! (defined(isfinite) || HAVE_DECL_ISFINITE)
#define isfinite(x) (!(isinf(x)||isnan(x)))
#endif /* !HAVE_DECL_ISFINITE */

#endif /* _ISNAN_H */
