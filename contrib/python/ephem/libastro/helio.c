#include <stdlib.h>
#include <math.h>
#include <math.h>

#include "astro.h"

/* given geocentric time jd and coords of a distant object at ra/dec (J2000),
 * find the difference in time between light arriving at earth vs the sun.
 * *hcp must be subtracted from geocentric jd to get heliocentric jd.
 * From RLM Oct 12, 1995.
 */
void
heliocorr (double jd, double ra, double dec, double *hcp)
{
	double e;	/* obliquity of ecliptic */
	double n;	/* day number */
	double g;	/* solar mean anomaly */
	double L;	/* solar ecliptic longitude */
	double l;	/* mean solar ecliptic longitude */
	double R;	/* sun distance, AU */
	double X, Y;	/* equatorial rectangular solar coords */
	double cdec, sdec;
	double cra, sra;

	/* following algorithm really wants EOD */
	precess (J2000, jd - MJD0, &ra, &dec);

	cdec = cos(dec);
	sdec = sin(dec);
	cra = cos(ra);
	sra = sin(ra);

	n = jd - 2451545.0;	/* use epoch 2000 */
	e = degrad(23.439 - 0.0000004*n);
	g = degrad(357.528) + degrad(0.9856003)*n;
	L = degrad(280.461) + degrad(0.9856474)*n;
	l = L + degrad(1.915)*sin(g) + degrad(0.02)*sin(2.0*g);
	R = 1.00014 - 0.01671*cos(g) - 0.00014*cos(2.0*g);
	X = R*cos(l);
	Y = R*cos(e)*sin(l);

#if 0
	    printf ("n=%g g=%g L=%g l=%g R=%g X=%g Y=%g\n",
				n, raddeg(g), raddeg(L), raddeg(l), R, X, Y);
#endif

	*hcp = 0.0057755 * (cdec*cra*X + (cdec*sra + tan(e)*sdec)*Y);
}

