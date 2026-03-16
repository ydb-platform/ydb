#include <math.h>

#include "astro.h"

/* given apparent altitude find airmass.
 * R.H. Hardie, 1962, `Photoelectric Reductions', Chapter 8 of Astronomical
 * Techniques, W.A. Hiltner (Ed), Stars and Stellar Systems, II (University
 * of Chicago Press: Chicago), pp178-208. 
 */
void
airmass (
double aa,		/* apparent altitude, rads */
double *Xp)		/* airmasses */
{
	double sm1;	/* secant zenith angle, minus 1 */

	/* degenerate near or below horizon */
	if (aa < degrad(3.0))
	    aa = degrad(3.0);

	sm1 = 1.0/sin(aa) - 1.0;
	*Xp = 1.0 + sm1*(0.9981833 - sm1*(0.002875 + 0.0008083*sm1));
}

