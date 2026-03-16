#include <stdio.h>
#include <math.h>
#include <stdlib.h>

#include "astro.h"

/*  RINGS OF SATURN by Olson, et al, BASIC Code from Sky & Telescope, May 1995.
 *  As converted from BASIC to C by pmartz@dsd.es.com (Paul Martz)
 *  Adapted to xephem by Elwood Charles Downey.
 */
void
satrings (
double sb, double sl, double sr,	/* Saturn hlat, hlong, sun dist */
double el, double er,			/* Earth hlong, sun dist */
double JD,				/* Julian date */
double *etiltp, double *stiltp)		/* tilt from earth and sun, rads south*/
{
	double t, i, om;
	double x, y, z;
	double la, be;
	double s, b, sp, bp;

	t = (JD-2451545.)/365250.;
	i = degrad(28.04922-.13*t+.0004*t*t);
	om = degrad(169.53+13.826*t+.04*t*t);

	x = sr*cos(sb)*cos(sl)-er*cos(el);
	y = sr*cos(sb)*sin(sl)-er*sin(el);
	z = sr*sin(sb);

	la = atan(y/x);
	if (x<0) la+=PI;
	be = atan(z/sqrt(x*x+y*y));

	s = sin(i)*cos(be)*sin(la-om)-cos(i)*sin(be);
	b = atan(s/sqrt(1.-s*s));
	sp = sin(i)*cos(sb)*sin(sl-om)-cos(i)*sin(sb);
	bp = atan(sp/sqrt(1.-sp*sp));

	*etiltp = b;
	*stiltp = bp;
}

