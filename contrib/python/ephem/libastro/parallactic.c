/* compure parallactic angle: angle formed by N pole - Object - Zenith
 */

#include <stdio.h>
#include <math.h>

#include "astro.h"

/* compute parallactic angle given latitude, object dec and alt.
 * all angles in rads.
 * N.B. always return >= 0, caller must determine sign and degenerate cases at
 *   pole or zenith.
 */
double
parallacticLDA (double lt, double dec, double alt)
{
	double ca = sin(lt);
	double cb = sin(dec);
	double sb = cos(dec);
	double cc = sin(alt);
	double sc = cos(alt);
	double cpa;

	/* given three sides find an angle */
	if (sb==0 || sc==0)
	    return (0);
	cpa = (ca - cb*cc)/(sb*sc);
	if (cpa < -1) cpa = -1;
	if (cpa >  1) cpa =  1;
	return (acos (cpa));
}

/* compute parallactic angle given latitude, object HA and Dec.
 * all angles in rads.
 * return value is between -PI and PI, sign is like HA, ie +west
 */
double
parallacticLHD (double lt, double ha, double dec)
{
	double A, b, cc, sc, B;

	A = ha;
	b = PI/2 - lt;
	cc = sin(dec);
	sc = cos(dec);
	solve_sphere (A, b, cc, sc, NULL, &B);

	if (B > PI)
	    B -= 2*PI;
	return (B);
}

