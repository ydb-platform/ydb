#include <math.h>
#include <stdio.h>

#include "astro.h"

/* transformation from spherical to cartesian coordinates */
void
sphcart (
double l, double b, double r,		/* source: spherical coordinates */
double *x, double *y, double *z)	/* result: rectangular coordinates */
{
	double rcb = r * cos(b);

	*x = rcb * cos(l);
	*y = rcb * sin(l);
	*z = r * sin(b);
}

/* transformation from cartesian to spherical coordinates */
void
cartsph (
double x, double y, double z,		/* source: rectangular coordinates */
double *l, double *b, double *r)	/* result: spherical coordinates */
{
	double rho = x*x + y*y;

	if (rho > 0) {	/* standard case: off axis */
	    *l = atan2(y, x);
	    range (l, 2*PI);
	    *b = atan2(z, sqrt(rho));
	    *r = sqrt(rho + z*z);
	} else {		/* degenerate case; avoid math error */
	    *l = 0.0;
	    if (z == 0.0)
		*b = 0.0;
	    else
		*b = (z > 0.0) ? PI/2. : -PI/2.;
	    *r = fabs(z);
	}
}

