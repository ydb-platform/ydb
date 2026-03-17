/* VSOP87 planetary theory
 *
 * currently uses version VSOP87D:
 * heliocentric spherical, mean ecliptic of date.
 *
 * calculation of rates (daily changes) is optional;
 * see header file for the necessary #define's
 *
 * rough orientation on calculation time, miliseconds
 * on an HP 715/75, all planets Mercury to Neptune, prec=0.0:
 *
 *      terms	with rates	without rates
 * 	3598	11		7.1
 *      31577	51		44
 *
 * with secular terms for JD 2232395.0  19/12/1399 0h TDB:
 *
 *	FULL PRECISION code (31577 terms), milliseconds
 *	prec	terms	rates	no rates
 *	1e-8	15086	62	36
 *	1e-7	10105	44	25
 *	1e-6	3725	20	13
 *	1e-5	1324	11	7.8
 *	1e-4	443	7.0	6.0
 *	1e-3	139	6.0	5.0
 *
 *	REDUCED PRECISION code (3598 terms), milliseconds
 *	prec	terms	rates	no rates
 *	1e-7	2463	9.9	5.5
 *	1e-6	1939	8.0	4.5
 *	1e-5	1131	4.9	2.9
 *	1e-4	443	2.2	1.5
 *	1e-3	139	1.0	0.9
 */

#include <math.h>

#include "astro.h"
#include "vsop87.h"

#define VSOP_A1000	365250.0	/* days per millenium */
#define VSOP_MAXALPHA	5		/* max degree of time */

/******************************************************************
 * adapted from BdL FORTRAN Code; stern
 *
 *    Reference : Bureau des Longitudes - PBGF9502
 *
 *    Object :  calculate a VSOP87 position for a given time.
 *
 *    Input :
 *
 *    mj       modified julian date, counted from J1900.0
 *             time scale : dynamical time TDB.
 *
 *    obj	object number as in astro.h, NB: not for pluto
 *
 *    prec     relative precision
 *
 *             if prec is equal to 0 then the precision is the precision
 *                p0 of the complete solution VSOP87.
 *                Mercury    p0 =  0.6 10**-8
 *                Venus      p0 =  2.5 10**-8
 *                Earth      p0 =  2.5 10**-8
 *                Mars       p0 = 10.0 10**-8
 *                Jupiter    p0 = 35.0 10**-8
 *                Saturn     p0 = 70.0 10**-8
 *                Uranus     p0 =  8.0 10**-8
 *                Neptune    p0 = 42.0 10**-8
 *
 *             if prec is not equal to 0, let us say in between p0 and
 *             10**-3, the precision is :
 *                for the positions :
 *                - prec*a0 au for the distances.
 *                - prec rad for the other variables.
 *                for the velocities :
 *                - prec*a0 au/day for the distances.
 *                - prec rad/day for the other variables.
 *                  a0 is the semi-major axis of the body.
 *
 *    Output :
 *
 *    ret[6]     array of the results (double).
 *
 *             for spherical coordinates :
 *                 1: longitude (rd)
 *                 2: latitude (rd)
 *                 3: radius (au)
 *		#if VSOP_GETRATE:
 *                 4: longitude velocity (rad/day)
 *                 5: latitude velocity (rad/day)
 *                 6: radius velocity (au/day)
 *
 *    return:     error index (int)
 *                 0: no error.
 *		   2: object out of range [MERCURY .. NEPTUNE, SUN]
 *		   3: precision out of range [0.0 .. 1e-3]
 ******************************************************************/
int
vsop87 (double mj, int obj, double prec, double *ret)
{
    static double (*vx_map[])[3] = {		/* data tables */
		vx_mercury, vx_venus, vx_mars, vx_jupiter,
		vx_saturn, vx_uranus, vx_neptune, 0, vx_earth,
	};
    static int (*vn_map[])[3] = {		/* indexes */
		vn_mercury, vn_venus, vn_mars, vn_jupiter,
		vn_saturn, vn_uranus, vn_neptune, 0, vn_earth,
	};
    static double a0[] = {	/* semimajor axes; for precision ctrl only */
	    0.39, 0.72, 1.5, 5.2, 9.6, 19.2, 30.1, 39.5, 1.0,
	};
    double (*vx_obj)[3] = vx_map[obj];		/* VSOP87 data and indexes */
    int (*vn_obj)[3] = vn_map[obj];

    double t[VSOP_MAXALPHA+1];			/* powers of time */
    double t_abs[VSOP_MAXALPHA+1];		/* powers of abs(time) */
    double q;					/* aux for precision control */
    int i, cooidx, alpha;			/* misc indexes */

    if (obj == PLUTO || obj > SUN)
	return (2);

    if (prec < 0.0 || prec > 1e-3)
	return(3);

    /* zero result array */
    for (i = 0; i < 6; ++i) ret[i] = 0.0;

    /* time and its powers */
    t[0] = 1.0;
    t[1] = (mj - J2000)/VSOP_A1000;
    for (i = 2; i <= VSOP_MAXALPHA; ++i) t[i] = t[i-1] * t[1];
    t_abs[0] = 1.0;
    for (i = 1; i <= VSOP_MAXALPHA; ++i) t_abs[i] = fabs(t[i]);

    /* precision control */
    q = -log10(prec + 1e-35) - 2;	/* decades below 1e-2 */
    q = VSOP_ASCALE * prec / 10.0 / q;	/* reduce threshold progressively
					 * for higher precision */

    /* do the term summation; first the spatial dimensions */
    for (cooidx = 0; cooidx < 3; ++cooidx) {

	/* then the powers of time */
	for (alpha = 0; vn_obj[alpha+1][cooidx] ; ++alpha) {
	    double p, term, termdot;

	    /* precision threshold */
	    p= alpha ? q/(t_abs[alpha] + alpha*t_abs[alpha-1]*1e-4 + 1e-35) : q;
#if VSOP_SPHERICAL
	    if (cooidx == 2)	/* scale by semimajor axis for radius */
#endif
		p *= a0[obj];

	    term = termdot = 0.0;
	    for (i = vn_obj[alpha][cooidx]; i < vn_obj[alpha+1][cooidx]; ++i) {
		double a, b, c, arg;

		a = vx_obj[i][0];
		if (a < p) continue;	/* ignore small terms */

		b = vx_obj[i][1];
		c = vx_obj[i][2];

		arg = b + c * t[1];
		term += a * cos(arg);
#if VSOP_GETRATE
		termdot += -c * a * sin(arg);
#endif
	    }

	    ret[cooidx] += t[alpha] * term;
#if VSOP_GETRATE
	    ret[cooidx + 3] += t[alpha] * termdot +
		    ((alpha > 0) ? alpha * t[alpha - 1] * term : 0.0);
#endif
	} /* alpha */
    } /* cooidx */

    for (i = 0; i < 6; ++i) ret[i] /= VSOP_ASCALE;

#if VSOP_SPHERICAL
    /* reduce longitude to 0..2pi */
    ret[0] -= floor(ret[0]/(2.*PI)) * (2.*PI);
#endif

#if VSOP_GETRATE
    /* convert millenium rate to day rate */
    for (i = 3; i < 6; ++i) ret[i] /= VSOP_A1000;
#endif

#if VSOP_SPHERICAL
    /* reduction from dynamical equinox of VSOP87 to FK5;
     */
    if (prec < 5e-7) {		/* 5e-7 rad = 0.1 arc seconds */
	double L1, c1, s1;
	L1 = ret[0] - degrad(13.97 * t[1] - 0.031 * t[2]);
	c1 = cos(L1); s1 = sin(L1);
	ret[0] += degrad(-0.09033 + 0.03916 * (c1 + s1) * tan(ret[1]))/3600.0;
	ret[1] += degrad(0.03916 * (c1 - s1))/3600.0;
    }
#endif

    return (0);
}

