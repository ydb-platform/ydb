#include <stdio.h>
#include <math.h>

#include "astro.h"

static void ecleq_aux (int sw, double mj, double x, double y,
    double *p, double *q);

#define	EQtoECL	1
#define	ECLtoEQ	(-1)


/* given the modified Julian date, mj, and an equitorial ra and dec, each in
 * radians, find the corresponding geocentric ecliptic latitude, *lt, and
 * longititude, *lg, also each in radians.
 * correction for the effect on the angle of the obliquity due to nutation is
 * not included.
 */
void
eq_ecl (double mj, double ra, double dec, double *lt, double *lg)
{
	ecleq_aux (EQtoECL, mj, ra, dec, lg, lt);
}

/* given the modified Julian date, mj, and a geocentric ecliptic latitude,
 * *lt, and longititude, *lg, each in radians, find the corresponding
 * equitorial ra and dec, also each in radians.
 * correction for the effect on the angle of the obliquity due to nutation is
 * not included.
 */
void
ecl_eq (double mj, double lt, double lg, double *ra, double *dec)
{
	ecleq_aux (ECLtoEQ, mj, lg, lt, ra, dec);
}

static void
ecleq_aux (
int sw,			/* +1 for eq to ecliptic, -1 for vv. */
double mj,
double x, double y,	/* sw==1: x==ra, y==dec.  sw==-1: x==lg, y==lt. */
double *p, double *q)	/* sw==1: p==lg, q==lt. sw==-1: p==ra, q==dec. */
{
	static double lastmj = -10000;	/* last mj calculated */
	static double seps, ceps;	/* sin and cos of mean obliquity */
	double sx, cx, sy, cy, ty, sq;

	if (mj != lastmj) {
	    double eps;
	    obliquity (mj, &eps);		/* mean obliquity for date */
    	    seps = sin(eps);
	    ceps = cos(eps);
	    lastmj = mj;
	}

	sy = sin(y);
	cy = cos(y);				/* always non-negative */
        if (fabs(cy)<1e-20) cy = 1e-20;		/* insure > 0 */
        ty = sy/cy;
	cx = cos(x);
	sx = sin(x);
        sq = (sy*ceps)-(cy*seps*sx*sw);
	if (sq < -1) sq = -1;
	if (sq >  1) sq =  1;
        *q = asin(sq);
        *p = atan(((sx*ceps)+(ty*seps*sw))/cx);
        if (cx<0) *p += PI;		/* account for atan quad ambiguity */
	range (p, 2*PI);
}

