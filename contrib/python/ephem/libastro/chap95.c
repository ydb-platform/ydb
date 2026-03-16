/* heliocentric rectangular equatorial coordinates of Jupiter to Pluto;
 * from Chapront's expansion of DE200/extension of DE200;  mean equator J2000.0
 *
 * calculation time (milliseconds) on an HP 715/75, Jupiter to Pluto:
 * (each coordinate component counted as 1 term,
 * secular terms included for JD 2448908.5 = 1992 Oct 13.0)
 *
 *      prec	terms	rates	no rates
 *	0.0	2256	5.1	4.6
 *
 *	1e-7	792	2.6	2.4	--> nominal precision rel. to DE200
 *	1e-6	535	2.1	2.0
 *	1e-5	350	1.8	1.6
 *	1e-4	199	1.5	1.4
 *	1e-3	96	1.2	1.1
 *
 *	no drop	2256	4.5	3.9	(code without test criterion)
 */

#include <math.h>

#include "astro.h"
#include "chap95.h"

#define CHAP_MAXTPOW	2	/* NB: valid for all 5 outer planets */

/* chap95()
 *
 * input:
 *	m	modified JD; days from J1900.0 = 2415020.0
 *
 *	prec	precision level, in radians.
 *		if (prec = 0.0), you get the full precision, namely
 *		a deviation of not more than 0.02 arc seconds (1e-7 rad)
 *		from the JPL DE200 integration, on which this expansion
 *		is based.
 *
 *	obj	object number as in astro.h (jupiter=3, saturn=4, ...)
 *
 * output:
 *	ret[6]	cartesian components of position and velocity
 *
 * return:
 *	0	Ok
 *	1	time out of range [CHAP_BEGIN .. CHAP_END]
 *	2	object out of range [JUPITER .. PLUTO]
 *	3	precision out of range [0.0 .. 1e-3]
 */
int
chap95 (double m, int obj, double prec, double *ret)
{
	static double a0[] = {		/* semimajor axes for precision ctrl */
	    0.39, 0.72, 1.5, 5.2, 9.6, 19.2, 30.1, 39.5, 1.0
	};
	double sum[CHAP_MAXTPOW+1][6];	/* [T^0, ..][X,Y,Z,X',Y',Z'] */
	double T, t;			/* time in centuries and years */
	double ca, sa, Nu;		/* aux vars for terms */
	double precT[CHAP_MAXTPOW+1];	/* T-augmented precision threshold */
	chap95_rec *rec;		/* term coeffs */
	int cooidx;

	/* check parameters */
	if (m < CHAP_BEGIN || m > CHAP_END)
		return (1);

	if (obj < JUPITER || obj > PLUTO)
		return (2);

	if (prec < 0.0 || prec > 1e-3)
		return (3);

	/* init the sums */
	zero_mem ((void *)sum, sizeof(sum));

	T = (m - J2000)/36525.0;	/* centuries since J2000.0 */

	/* modify precision treshold for
	 * a) term storing scale
	 * b) convert radians to au
	 * c) account for skipped terms (more terms needed for better prec)
	 *    threshold empirically established similar to VSOP; stern
	 * d) augment for secular terms
	 */
	precT[0] = prec * CHAP_SCALE				/* a) */
			* a0[obj]				/* b) */
			/ (10. * (-log10(prec + 1e-35) - 2));	/* c) */
	t = 1./(fabs(T) + 1e-35);				/* d) */
	precT[1] = precT[0]*t;
	precT[2] = precT[1]*t;

	t = T * 100.0;		/* YEARS since J2000.0 */

	ca = sa = Nu = 0.;	/* shut up compiler warning 'uninitialised' */

	switch (obj) {		/* set initial term record pointer */
	    case JUPITER:	rec = chap95_jupiter;	break;
	    case SATURN:	rec = chap95_saturn;	break;
	    case URANUS:	rec = chap95_uranus;	break;
	    case NEPTUNE:	rec = chap95_neptune;	break;
	    case PLUTO:		rec = chap95_pluto;	break;
	    default:
		return (2);	/* wrong object: severe internal trouble */
	}

	/* do the term summation into sum[T^n] slots */
	for (; rec->n >= 0; ++rec) {
	    double *amp;

	    /* NOTE:  The formula
	     * X = SUM[i=1,Records] T**n_i*(CX_i*cos(Nu_k*t)+SX_i*sin(Nu_k*t))
	     * could be rewritten as  SUM( ... A sin (B + C*t) )
	     * "saving" trigonometric calls.  However, e.g. for Pluto,
	     * there are only 65 distinct angles NU_k (130 trig calls).
	     * With that manipulation, EVERY arg_i would be different for X,
	     * Y and Z, which is 3*96 terms.  Hence, the formulation as
	     * given is good (optimal?).
	     */

	    for (cooidx = 0, amp = rec->amp; cooidx < 3; ++cooidx) {
		double C, S, term, termdot;
		short n;		/* fast access */

		C = *amp++;
		S = *amp++;
		n = rec->n;

		/* drop term if too small
		 * this is quite expensive:  17% of loop time
		 */
		if (fabs(C) + fabs(S) < precT[n])
			continue;

		if (n == 0 && cooidx == 0) {	/* new Nu only here */
		    double arg;

		    Nu = rec->Nu;
		    arg = Nu * t;
		    arg -= floor(arg/(2.*PI))*(2.*PI);
		    ca = cos(arg);	/* blast it - even for Nu = 0.0 */
		    sa = sin(arg);
		}

		term = C * ca + S * sa;
		sum[n][cooidx] += term;
#if CHAP_GETRATE
		termdot = (-C * sa + S * ca) * Nu;
		sum[n][cooidx+3] += termdot;
		if (n > 0) sum[n - 1][cooidx+3] += n/100.0 * term;
#endif
	    } /* cooidx */
	} /* records */

	/* apply powers of time and sum up */
	for (cooidx = 0; cooidx < 6; ++cooidx) {
	    ret[cooidx] = (sum[0][cooidx] +
			T * (sum[1][cooidx] +
			T * (sum[2][cooidx] )) )/CHAP_SCALE;
	}

	/* TEST: if the MAIN terms are dropped, get angular residue
	ret[0] = sqrt(ret[0]*ret[0] + ret[1]*ret[1] + ret[2]*ret[2])/a0[obj];
	*/

#if CHAP_GETRATE
	for (cooidx = 3; cooidx < 6; ++cooidx) {
	    ret[cooidx] /= 365.25;	/* yearly to daily rate */
	}
#endif

    return (0);
}

