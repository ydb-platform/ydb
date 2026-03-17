#include <stdio.h>
#include <math.h>

#include "astro.h"

static void unrefractLT15 (double pr, double tr, double aa, double *ta);
static void unrefractGE15 (double pr, double tr, double aa, double *ta);

void
unrefract (double pr, double tr, double aa, double *ta)
{
#define	LTLIM	14.5
#define	GELIM	15.5

	double aadeg = raddeg(aa);

	if (aadeg < LTLIM)
	    unrefractLT15 (pr, tr, aa, ta);
	else if (aadeg >= GELIM)
	    unrefractGE15 (pr, tr, aa, ta);
	else {
	    /* smooth blend -- important for inverse */
	    double taLT, taGE, p;

	    unrefractLT15 (pr, tr, aa, &taLT);
	    unrefractGE15 (pr, tr, aa, &taGE);
	    p = (aadeg - LTLIM)/(GELIM - LTLIM);
	    *ta = taLT + (taGE - taLT)*p;
	}
}

static void
unrefractGE15 (double pr, double tr, double aa, double *ta)
{
	double r;
	
	r = 7.888888e-5*pr/((273+tr)*tan(aa));
	*ta  =  aa - r;
}

static void
unrefractLT15 (double pr, double tr, double aa, double *ta)
{
	double aadeg = raddeg(aa);
	double r, a, b;

	a = ((2e-5*aadeg+1.96e-2)*aadeg+1.594e-1)*pr;
	b = (273+tr)*((8.45e-2*aadeg+5.05e-1)*aadeg+1);
	r = degrad(a/b);

	*ta  =  (aa < 0 && r < 0) ? aa : aa - r;	/* 0 below ~5 degs */
}

/* correct the true altitude, ta, for refraction to the apparent altitude, aa,
 * each in radians, given the local atmospheric pressure, pr, in mbars, and
 * the temperature, tr, in degrees C.
 */
void
refract (double pr, double tr, double ta, double *aa)
{
#define	MAXRERR	degrad(0.1/3600.)	/* desired accuracy, rads */

	double d, t, t0, a;

	if (isnan (ta)) {
	    *aa = ta;
	    return;
	}

	/* first guess of error is to go backwards.
	 * make use that we know delta-apparent is always < delta-true.
	 */
	unrefract (pr, tr, ta, &t);
	d = 0.8*(ta - t);
	t0 = t;
	a = ta;

	/* use secant method to discover a value that unrefracts to ta.
	 * max=7 ave=2.4 loops in hundreds of test cases.
	 */
	while (1) {
	    a += d;
	    unrefract (pr, tr, a, &t);
	    if (fabs(ta-t) <= MAXRERR)
		break;
	    d *= -(ta - t)/(t0 - t);
	    t0 = t;
	}

	*aa = a;

#undef	MAXRERR
}

