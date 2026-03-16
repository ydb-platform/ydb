#include <math.h>

#include "astro.h"

#undef sqr
#define	sqr(x)		((x)*(x))

/* given a planet, the sun, the planet's eq pole position and a
 * position of a satellite (as eq x=+e y=+s z=front in planet radii) find x,y
 * position of shadow.
 * return 0 if ok else -1 if shadow not on planet
 */
int
plshadow (Obj *op, Obj *sop, double polera, double poledec, double x,
double y, double z, float *sxp, float *syp)
{
	/* equatorial to ecliptic sky-plane rotation */
	double sa = cos(op->s_dec) * cos(poledec) *
			(cos(op->s_ra)*sin(polera) - sin(op->s_ra)*cos(polera));
	double ca = sqrt (1.0 - sa*sa);

	/* rotate moon from equatorial to ecliptic */
	double ex =  x*ca + y*sa;
	double ey = -x*sa + y*ca;

	/* find angle subtended by earth-sun from planet */
	double a = asin (sin(op->s_hlong - sop->s_hlong)/op->s_edist);
	double b = asin (-sin(op->s_hlat)/op->s_edist);

	/* find displacement in sky plane */
	double x0 = ex - z*tan(a);
	double y0 = ey - z*tan(b);

	/* projection onto unit sphere */
	double x1 = x0 + (ex-x0)/sqrt(sqr(ex-x0)+sqr(z));
	double y1 = y0 + (ey-y0)/sqrt(sqr(ey-y0)+sqr(z));

	/* check behind or off edge */
	if (z < 0 || sqr(x1) + sqr(y1) > 1)
	    return (-1);

	/* rotate back to equatorial */
	*sxp = x1*ca - y1*sa;
	*syp = x1*sa + y1*ca;

	return (0);
}

