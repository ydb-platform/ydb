#include <stdio.h>
#include <math.h>

#include "astro.h"


/* given true ha and dec, tha and tdec, the geographical latitude, phi, the
 * height above sea-level (as a fraction of the earths radius, 6378.16km),
 * ht, and the geocentric distance rho in Earth radii(!), find the apparent
 * ha and dec, aha and adec allowing for parallax.
 * all angles in radians. ehp is the angle subtended at the body by the
 * earth's equator.
 */
void
ta_par (double tha, double tdec, double phi, double ht, double *rho,
double *aha, double *adec)
{
	static double last_phi = 1000.0, last_ht = -1000.0, xobs, zobs;
	double x, y, z;	/* obj cartesian coord, in Earth radii */

	/* avoid calcs involving the same phi and ht */
	if (phi != last_phi || ht != last_ht) {
	    double cphi, sphi, robs, e2 = (2 - 1/298.257)/298.257;
	    cphi = cos(phi);
	    sphi = sin(phi);
	    robs = 1/sqrt(1 - e2 * sphi * sphi);

	    /* observer coordinates: x to meridian, y east, z north */
	    xobs = (robs + ht) * cphi;
	    zobs = (robs*(1-e2) + ht) * sphi;
	    last_phi  =  phi;
	    last_ht  =  ht;
	}

	sphcart(-tha, tdec, *rho, &x, &y, &z);
	cartsph(x - xobs, y, z - zobs, aha, adec, rho);
	*aha *= -1;
	range (aha, 2*PI);
}

