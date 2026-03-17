#include <stdio.h>
#include <math.h>

#include "astro.h"

static void precess_hiprec (double mjd1, double mjd2, double *ra, double *dec);


#define	DCOS(x)		cos(degrad(x))
#define	DSIN(x)		sin(degrad(x))
#define	DASIN(x)	raddeg(asin(x))
#define	DATAN2(y,x)	raddeg(atan2((y),(x)))

/* corrects ra and dec, both in radians, for precession from epoch 1 to epoch 2.
 * the epochs are given by their modified JDs, mjd1 and mjd2, respectively.
 * N.B. ra and dec are modifed IN PLACE.
 */
void
precess (
double mjd1, double mjd2,	/* initial and final epoch modified JDs */
double *ra, double *dec)	/* ra/dec for mjd1 in, for mjd2 out */
{
	precess_hiprec (mjd1, mjd2, ra, dec);
}

/*
 * Copyright (c) 1990 by Craig Counterman. All rights reserved.
 *
 * This copy of the precess_hiprec() routine is, by permission of the
 * author, licensed under the same license as the PyEphem package in
 * which it is here included.
 *
 * Rigorous precession. From Astronomical Ephemeris 1989, p. B18
 *
 * 96-06-20 Hayo Hase <hase@wettzell.ifag.de>: theta_a corrected
 */
static void
precess_hiprec (
double mjd1, double mjd2,	/* initial and final epoch modified JDs */
double *ra, double *dec)	/* ra/dec for mjd1 in, for mjd2 out */
{
	static double last_mjd1 = -213.432, last_from;
	static double last_mjd2 = -213.432, last_to;
	double zeta_A, z_A, theta_A;
	double T;
	double A, B, C;
	double alpha, delta;
	double alpha_in, delta_in;
	double from_equinox, to_equinox;
	double alpha2000, delta2000;

	/* convert mjds to years;
	 * avoid the remarkably expensive calls to mjd_year()
	 */
	if (last_mjd1 == mjd1)
	    from_equinox = last_from;
	else {
	    mjd_year (mjd1, &from_equinox);
	    last_mjd1 = mjd1;
	    last_from = from_equinox;
	}
	if (last_mjd2 == mjd2)
	    to_equinox = last_to;
	else {
	    mjd_year (mjd2, &to_equinox);
	    last_mjd2 = mjd2;
	    last_to = to_equinox;
	}

	/* convert coords in rads to degs */
	alpha_in = raddeg(*ra);
	delta_in = raddeg(*dec);

	/* precession progresses about 1 arc second in .047 years */
	/* From from_equinox to 2000.0 */
	if (fabs (from_equinox-2000.0) > .02) {
	    T = (from_equinox - 2000.0)/100.0;
	    zeta_A  = 0.6406161* T + 0.0000839* T*T + 0.0000050* T*T*T;
	    z_A     = 0.6406161* T + 0.0003041* T*T + 0.0000051* T*T*T;
	    theta_A = 0.5567530* T - 0.0001185* T*T - 0.0000116* T*T*T;

	    A = DSIN(alpha_in - z_A) * DCOS(delta_in);
	    B = DCOS(alpha_in - z_A) * DCOS(theta_A) * DCOS(delta_in)
	      + DSIN(theta_A) * DSIN(delta_in);
	    C = -DCOS(alpha_in - z_A) * DSIN(theta_A) * DCOS(delta_in)
	      + DCOS(theta_A) * DSIN(delta_in);

	    alpha2000 = DATAN2(A,B) - zeta_A;
	    range (&alpha2000, 360.0);
	    delta2000 = DASIN(C);
	} else {
	    /* should get the same answer, but this could improve accruacy */
	    alpha2000 = alpha_in;
	    delta2000 = delta_in;
	};


	/* From 2000.0 to to_equinox */
	if (fabs (to_equinox - 2000.0) > .02) {
	    T = (to_equinox - 2000.0)/100.0;
	    zeta_A  = 0.6406161* T + 0.0000839* T*T + 0.0000050* T*T*T;
	    z_A     = 0.6406161* T + 0.0003041* T*T + 0.0000051* T*T*T;
	    theta_A = 0.5567530* T - 0.0001185* T*T - 0.0000116* T*T*T;

	    A = DSIN(alpha2000 + zeta_A) * DCOS(delta2000);
	    B = DCOS(alpha2000 + zeta_A) * DCOS(theta_A) * DCOS(delta2000)
	      - DSIN(theta_A) * DSIN(delta2000);
	    C = DCOS(alpha2000 + zeta_A) * DSIN(theta_A) * DCOS(delta2000)
	      + DCOS(theta_A) * DSIN(delta2000);

	    alpha = DATAN2(A,B) + z_A;
	    range(&alpha, 360.0);
	    delta = DASIN(C);
	} else {
	    /* should get the same answer, but this could improve accruacy */
	    alpha = alpha2000;
	    delta = delta2000;
	};

	*ra = degrad(alpha);
	*dec = degrad(delta);
}

#if 0
static void
precess_fast (
double mjd1, double mjd2,	/* initial and final epoch modified JDs */
double *ra, double *dec)	/* ra/dec for mjd1 in, for mjd2 out */
{
#define	N	degrad (20.0468/3600.0)
#define	M	hrrad (3.07234/3600.0)
	double nyrs;

	nyrs = (mjd2 - mjd1)/365.2425;
	*dec += N * cos(*ra) * nyrs;
	*ra += (M + (N * sin(*ra) * tan(*dec))) * nyrs;
	range (ra, 2.0*PI);
}
#endif

