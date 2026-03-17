/* code to convert between equitorial and galactic coordinates */

#include <stdio.h>
#include <math.h>

#include "astro.h"

static void galeq_aux (int sw, double x, double y, double *p, double *q);
static void galeq_init (void);

#define	EQtoGAL	1
#define	GALtoEQ	(-1)
#define	SMALL	(1e-20)

static double an = degrad(32.93192);    /* G lng of asc node on equator */
static double gpr = degrad(192.85948);  /* RA of North Gal Pole, 2000 */
static double gpd = degrad(27.12825);   /* Dec of  " */
static double cgpd, sgpd;		/* cos() and sin() of gpd */
static double mj2000;			/* mj of 2000 */
static int before;			/* whether these have been set yet */

/* given ra and dec, each in radians, for the given epoch, find the
 * corresponding galactic latitude, *lt, and longititude, *lg, also each in
 * radians.
 */
void
eq_gal (double mj, double ra, double dec, double *lt, double *lg)
{
	galeq_init();
	precess (mj, mj2000, &ra, &dec);
	galeq_aux (EQtoGAL, ra, dec, lg, lt);
}

/* given galactic latitude, lt, and longititude, lg, each in radians, find
 * the corresponding equitorial ra and dec, also each in radians, at the 
 * given epoch.
 */
void
gal_eq (double mj, double lt, double lg, double *ra, double *dec)
{
	galeq_init();
	galeq_aux (GALtoEQ, lg, lt, ra, dec);
	precess (mj2000, mj, ra, dec);
}

static void
galeq_aux (
int sw,			/* +1 for eq to gal, -1 for vv. */
double x, double y,	/* sw==1: x==ra, y==dec.  sw==-1: x==lg, y==lt. */
double *p, double *q)	/* sw==1: p==lg, q==lt. sw==-1: p==ra, q==dec. */
{
	double sy, cy, a, ca, sa, b, sq, c, d; 

	cy = cos(y);
	sy = sin(y);
	a = x - an;
	if (sw == EQtoGAL)
	    a = x - gpr;
	ca = cos(a);
	sa = sin(a);
	b = sa;
	if (sw == EQtoGAL)
	    b = ca;
	sq = (cy*cgpd*b) + (sy*sgpd);
	*q = asin (sq);

	if (sw == GALtoEQ) {
	    c = cy*ca;
	    d = (sy*cgpd) - (cy*sgpd*sa);
	    if (fabs(d) < SMALL)
		d = SMALL;
	    *p = atan (c/d) + gpr;
	} else {
	    c = sy - (sq*sgpd);
	    d = cy*sa*cgpd;
	    if (fabs(d) < SMALL)
		d = SMALL;
	    *p = atan (c/d) + an;
	}

	if (d < 0) *p += PI;
	if (*p < 0) *p += 2*PI;
	if (*p > 2*PI) *p -= 2*PI;
}

/* set up the definitions */
static void
galeq_init()
{
	if (!before) {
	    cgpd = cos (gpd);
	    sgpd = sin (gpd);
	    mj2000 = J2000;
	    before = 1;
	}
}

