#include <math.h>

#include "astro.h"

/* given a modified Julian date, mj, and a set of heliocentric parabolic
 * orbital elements referred to the epoch of date (mj):
 *   ep:   epoch of perihelion,
 *   inc:  inclination,
 *   ap:   argument of perihelion (equals the longitude of perihelion minus the
 *	   longitude of ascending node)
 *   qp:   perihelion distance,
 *   om:   longitude of ascending node;
 * find:
 *   lpd:  heliocentric longitude, 
 *   psi:  heliocentric latitude,
 *   rp:   distance from the sun to the planet, 
 *   rho:  distance from the Earth to the planet,
 *   lam:  geocentric ecliptic longitude, 
 *   bet:  geocentric ecliptic latitude,
 *         none are corrected for light time, ie, they are the true values for
 *	   the given instant.
 *
 * all angles are in radians, all distances in AU.
 * mutual perturbation corrections with other solar system objects are not
 * applied. corrections for nutation and abberation must be made by the caller.
 * The RA and DEC calculated from the fully-corrected ecliptic coordinates are
 * then the apparent geocentric coordinates. Further corrections can be made,
 * if required, for atmospheric refraction and geocentric parallax.
 */
void
comet (double mj, double ep, double inc, double ap, double qp, double om,
double *lpd, double *psi, double *rp, double *rho, double *lam, double *bet)
{
	double w, s, s2;
	double l, sl, cl, y;
	double spsi, cpsi;
	double rd, lsn, rsn;
	double lg, re, ll;
	double cll, sll;
	double nu;

#define	ERRLMT	0.0001
        w = ((mj-ep)*3.649116e-02)/(qp*sqrt(qp));
        s = w/3;
	for (;;) {
	    double d;
	    s2 = s*s;
	    d = (s2+3)*s-w;
	    if (fabs(d) <= ERRLMT)
		break;
	    s = ((2*s*s2)+w)/(3*(s2+1));
	}

        nu = 2*atan(s);
	*rp = qp*(1+s2);
	l = nu+ap;
        sl = sin(l);
	cl = cos(l);
	spsi = sl*sin(inc);
        *psi = asin(spsi);
	y = sl*cos(inc);
        *lpd = atan(y/cl)+om;
	cpsi = cos(*psi);
        if (cl<0) *lpd += PI;
	range (lpd, 2*PI);
        rd = *rp * cpsi;
	sunpos (mj, &lsn, &rsn, 0);
	lg = lsn+PI;
        re = rsn;
	ll = *lpd - lg;
        cll = cos(ll);
	sll = sin(ll);
        *rho = sqrt((re * re)+(*rp * *rp)-(2*re*rd*cll));
        if (rd<re) 
            *lam = atan((-1*rd*sll)/(re-(rd*cll)))+lg+PI;
	else
	    *lam = atan((re*sll)/(rd-(re*cll)))+*lpd;
	range (lam, 2*PI);
        *bet = atan((rd*spsi*sin(*lam-*lpd))/(cpsi*re*sll));
}

