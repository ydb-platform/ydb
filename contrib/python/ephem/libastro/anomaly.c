/* improved by rclark@lpl.arizona.edu (Richard Clark) */

#include <stdio.h>
#include <math.h>

#include "astro.h"


#define TWOPI   	(2*PI)
#define	STOPERR		(1e-8)

/* given the mean anomaly, ma, and the eccentricity, s, of elliptical motion,
 * find the true anomaly, *nu, and the eccentric anomaly, *ea.
 * all angles in radians.
 */
void
anomaly (double ma, double s, double *nu, double *ea)
{
        double m, fea, corr;

        if (s < 1.0) {
            /* elliptical */
            double dla;

            m = ma-TWOPI*(long)(ma/TWOPI);
            if (m > PI) m -= TWOPI;
            if (m < -PI) m += TWOPI;
            fea = m;

            for (;;) {
                dla = fea-(s*sin(fea))-m;
                if (fabs(dla)<STOPERR)
                    break;
                /* avoid runnaway corrections for e>.97 and M near 0*/
                corr = 1-(s*cos(fea));
                if (corr < .1) corr = .1;
                dla /= corr;
                fea -= dla;
            }
            *nu = 2*atan(sqrt((1+s)/(1-s))*tan(fea/2));
        } else {
            /* hyperbolic */
	    double fea1;

            m = fabs(ma);
            fea = m / (s-1.);
	    fea1 = pow(6*m/(s*s),1./3.);
            /* whichever is smaller is the better initial guess */
            if (fea1 < fea) fea = fea1;

	    corr = 1;
            while (fabs(corr) > STOPERR) {
		corr = (m - s * sinh(fea) + fea) / (s*cosh(fea) - 1);
		fea += corr;
            }
            if (ma < 0.) fea = -fea;
            *nu = 2*atan(sqrt((s+1)/(s-1))*tanh(fea/2));
        }
        *ea = fea;
}

