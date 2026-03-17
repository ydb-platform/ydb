/* misc handy functions.
 * every system has such, no?
 *  4/20/98 now_lst() always just returns apparent time
 */

#include "astro.h"

#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>

static union {
    unsigned char bytes[sizeof(double)];
    double value;
} _nan = {
#if (defined(__s390__) || defined(__s390x__) || defined(__zarch__))
    {0x7f, 0xf8, 0, 0, 0, 0, 0, 0}
#else
    {0, 0, 0, 0, 0, 0, 0xf8, 0x7f}
#endif
};

double ascii_strtod(const char *s00, char **se);  /* for PyEphem */

/* zero from loc for len bytes */
void
zero_mem (void *loc, unsigned len)
{
	(void) memset (loc, 0, len);
}

/* given min and max and an approximate number of divisions desired,
 * fill in ticks[] with nicely spaced values and return how many.
 * N.B. return value, and hence number of entries to ticks[], might be as
 *   much as 2 more than numdiv.
 */
int
tickmarks (double min, double max, int numdiv, double ticks[])
{
        static int factor[] = { 1, 2, 5 };
        double minscale;
        double delta;
	double lo;
        double v;
        int n;

        minscale = fabs(max - min);
        delta = minscale/numdiv;
        for (n=0; n < (int)(sizeof(factor)/sizeof(factor[0])); n++) {
	    double scale;
	    double x = delta/factor[n];
            if ((scale = (pow(10.0, ceil(log10(x)))*factor[n])) < minscale)
		minscale = scale;
	}
        delta = minscale;

        lo = floor(min/delta);
        for (n = 0; (v = delta*(lo+n)) < max+delta; )
	    ticks[n++] = v;

	return (n);
}

/* given an Obj *, return its type as a descriptive string.
 * if it's of type fixed then return its class description.
 * N.B. we return the address of static storage -- do not free or change.
 */
char *
obj_description (Obj *op)
{
	typedef struct {
	    char classcode;
	    char *desc;
	} CC;

#define	NFCM	((int)(sizeof(fixed_class_map)/sizeof(fixed_class_map[0])))
	static CC fixed_class_map[] = {
	    {'A', "Cluster of Galaxies"},
	    {'B', "Binary System"},
	    {'C', "Globular Cluster"},
	    {'D', "Double Star"},
	    {'F', "Diffuse Nebula"},
	    {'G', "Spiral Galaxy"},
	    {'H', "Spherical Galaxy"},
	    {'J', "Radio"},
	    {'K', "Dark Nebula"},
	    {'L', "Pulsar"},
	    {'M', "Multiple Star"},
	    {'N', "Bright Nebula"},
	    {'O', "Open Cluster"},
	    {'P', "Planetary Nebula"},
	    {'Q', "Quasar"},
	    {'R', "Supernova Remnant"},
	    {'S', "Star"},
	    {'T', "Star-like Object"},
	    {'U', "Cluster, with nebulosity"},
	    {'V', "Variable Star"},
	    {'Y', "Supernova"},
	};

#define	NBCM	((int)(sizeof(binary_class_map)/sizeof(binary_class_map[0])))
	static CC binary_class_map[] = {
	    {'a', "Astrometric binary"},
	    {'c', "Cataclysmic variable"},
	    {'e', "Eclipsing binary"},
	    {'x', "High-mass X-ray binary"},
	    {'y', "Low-mass X-ray binary"},
	    {'o', "Occultation binary"},
	    {'s', "Spectroscopic binary"},
	    {'t', "1-line spectral binary"},
	    {'u', "2-line spectral binary"},
	    {'v', "Spectrum binary"},
	    {'b', "Visual binary"},
	    {'d', "Visual binary, apparent"},
	    {'q', "Visual binary, optical"},
	    {'r', "Visual binary, physical"},
	    {'p', "Exoplanet"},
	};

	switch (op->o_type) {
	case FIXED:
	    if (op->f_class) {
		int i;
		for (i = 0; i < NFCM; i++)
		    if (fixed_class_map[i].classcode == op->f_class)
			return (fixed_class_map[i].desc);
	    }
	    return ("Fixed");
	case PARABOLIC:
	    return ("Solar - Parabolic");
	case HYPERBOLIC:
	    return ("Solar - Hyperbolic");
	case ELLIPTICAL:
	    return ("Solar - Elliptical");
	case BINARYSTAR:
	    if (op->f_class) {
		int i;
		for (i = 0; i < NFCM; i++)
		    if (binary_class_map[i].classcode == op->f_class)
			return (binary_class_map[i].desc);
	    }
	    return ("Binary system");
	case PLANET: {
	    static char nsstr[MAXNM + 9];
	    static Obj *biop;

	    if (op->pl_code == SUN)
		return ("Star");
	    if (op->pl_code == MOON)
		return ("Moon of Earth");
	    if (op->pl_moon == X_PLANET)
		return ("Planet");
	    if (!biop)
		getBuiltInObjs (&biop);
	    sprintf (nsstr, "Moon of %s", biop[op->pl_code].o_name);
	    return (nsstr);
	    }
	case EARTHSAT:
	    return ("Earth Sat");
	default:
	    printf ("obj_description: unknown type: 0x%x\n", op->o_type);
	    abort();
	    return (NULL);	/* for lint */
	}
}

/* given a Now *, find the local apparent sidereal time, in hours.
 */
void
now_lst (Now *np, double *lstp)
{
	static double last_mjd = -23243, last_lng = 121212, last_lst;
	double eps, lst, deps, dpsi;

	if (last_mjd == mjd && last_lng == lng) {
	    *lstp = last_lst;
	    return;
	}

	utc_gst (mjd_day(mjd), mjd_hr(mjd), &lst);
	lst += radhr(lng);

	obliquity(mjd, &eps);
	nutation(mjd, &deps, &dpsi);
	lst += radhr(dpsi*cos(eps+deps));

	range (&lst, 24.0);

	last_mjd = mjd;
	last_lng = lng;
	*lstp = last_lst = lst;
}

/* convert ra to ha, in range 0..2*PI
 * need dec too if not already apparent.
 */
void
radec2ha (Now *np, double ra, double dec, double *hap)
{
	double ha, lst;

	if (epoch != EOD)
	    as_ap (np, epoch, &ra, &dec);
	now_lst (np, &lst);
	ha = hrrad(lst) - ra;
	if (ha < 0)
	    ha += 2*PI;
	*hap = ha;
}

/* find Greenwich Hour Angle of the given object at the given time, 0..2*PI.
 */
void
gha (Now *np, Obj *op, double *ghap)
{
	Now n = *np;
	Obj o = *op;
	double tmp;

	n.n_epoch = EOD;
	n.n_lng = 0.0;
	n.n_lat = 0.0;
	obj_cir (&n, &o);
	now_lst (&n, &tmp);
	tmp = hrrad(tmp) - o.s_ra;
	if (tmp < 0)
	    tmp += 2*PI;
	*ghap = tmp;
}

/* given a circle and a line segment, find a segment of the line inside the 
 *   circle.
 * return 0 and the segment end points if one exists, else -1.
 * We use a parametric representation of the line:
 *   x = x1 + (x2-x1)*t and y = y1 + (y2-y1)*t, 0 < t < 1
 * and a centered representation of the circle:
 *   (x - xc)**2 + (y - yc)**2 = r**2
 * and solve for the t's that work, checking for usual conditions.
 */
int
lc (
int cx, int cy, int cw,			/* circle bbox corner and width */
int x1, int y1, int x2, int y2,		/* line segment endpoints */
int *sx1, int *sy1, int *sx2, int *sy2)	/* segment inside the circle */
{
	int dx = x2 - x1;
	int dy = y2 - y1;
	int r = cw/2;
	int xc = cx + r;
	int yc = cy + r;
	int A = x1 - xc;
	int B = y1 - yc;
	double a = dx*dx + dy*dy;	/* O(2 * 2**16 * 2**16) */
	double b = 2*(dx*A + dy*B);	/* O(4 * 2**16 * 2**16) */
	double c = A*A + B*B - r*r;	/* O(2 * 2**16 * 2**16) */
	double d = b*b - 4*a*c;		/* O(2**32 * 2**32) */
	double sqrtd;
	double t1, t2;

	if (d <= 0)
	    return (-1);	/* containing line is purely outside circle */

	sqrtd = sqrt(d);
	t1 = (-b - sqrtd)/(2.0*a);
	t2 = (-b + sqrtd)/(2.0*a);

	if (t1 >= 1.0 || t2 <= 0.0)
	    return (-1);	/* segment is purely outside circle */

	/* we know now that some part of the segment is inside,
	 * ie, t1 < 1 && t2 > 0
	 */

	if (t1 <= 0.0) {
	    /* (x1,y1) is inside circle */
	    *sx1 = x1;
	    *sy1 = y1;
	} else {
	    *sx1 = (int)(x1 + dx*t1);
	    *sy1 = (int)(y1 + dy*t1);
	}

	if (t2 >= 1.0) {
	    /* (x2,y2) is inside circle */
	    *sx2 = x2;
	    *sy2 = y2;
	} else {
	    *sx2 = (int)(x1 + dx*t2);
	    *sy2 = (int)(y1 + dy*t2);
	}

	return (0);
}

/* compute visual magnitude using the H/G parameters used in the Astro Almanac.
 * these are commonly used for asteroids.
 */
void
hg_mag (
double h, double g,
double rp,	/* sun-obj dist, AU */
double rho,	/* earth-obj dist, AU */
double rsn,	/* sun-earth dist, AU */
double *mp)
{
	double psi_t, Psi_1, Psi_2, beta;
	double c;
	double tb2;

	c = (rp*rp + rho*rho - rsn*rsn)/(2*rp*rho);
	if (c <= -1)
	    beta = PI;
	else if (c >= 1)
	    beta = 0;
	else
	    beta = acos(c);;
	tb2 = tan(beta/2.0);
	/* psi_t = exp(log(tan(beta/2.0))*0.63); */
	psi_t = pow (tb2, 0.63);
	Psi_1 = exp(-3.33*psi_t);
	/* psi_t = exp(log(tan(beta/2.0))*1.22); */
	psi_t = pow (tb2, 1.22);
	Psi_2 = exp(-1.87*psi_t);
	*mp = h + 5.0*log10(rp*rho);
	if (Psi_1 || Psi_2) *mp -= 2.5*log10((1-g)*Psi_1 + g*Psi_2);
}

/* given faintest desired mag, mag step magstp, image scale and object
 * magnitude and size, return diameter to draw object, in pixels, or 0 if
 * dimmer than fmag.
 */
int
magdiam (
int fmag,	/* faintest mag */
int magstp,	/* mag range per dot size */
double scale,	/* rads per pixel */
double mag,	/* magnitude */
double size)	/* rads, or 0 */
{
	int diam, sized;
	
	if (mag > fmag)
	    return (0);
	diam = (int)((fmag - mag)/magstp + 1);
	sized = (int)(size/scale + 0.5);
	if (sized > diam)
	    diam = sized;

	return (diam);
}

/* computer visual magnitude using the g/k parameters commonly used for comets.
 */
void
gk_mag (
double g, double k,
double rp,	/* sun-obj dist, AU */
double rho,	/* earth-obj dist, AU */
double *mp)
{
	*mp = g + 5.0*log10(rho) + 2.5*k*log10(rp);
}

/* given a string convert to floating point and return it as a double.
 * this is to isolate possible unportabilities associated with declaring atof().
 * it's worth it because atof() is often some 50% faster than sscanf ("%lf");
 */
double
atod (char *buf)
{
     if (*buf == '\0') return _nan.value;
     return (ascii_strtod(buf, NULL));
}

/* solve a spherical triangle:
 *           A
 *          /  \
 *         /    \
 *      c /      \ b
 *       /        \
 *      /          \
 *    B ____________ C
 *           a
 *
 * given A, b, c find B and a in range 0..B..2PI and 0..a..PI, respectively..
 * cap and Bp may be NULL if not interested in either one.
 * N.B. we pass in cos(c) and sin(c) because in many problems one of the sides
 *   remains constant for many values of A and b.
 */
void
solve_sphere (double A, double b, double cc, double sc, double *cap, double *Bp)
{
	double cb = cos(b), sb = sin(b);
	double sA, cA = cos(A);
	double x, y;
	double ca;
	double B;

	ca = cb*cc + sb*sc*cA;
	if (ca >  1.0) ca =  1.0;
	if (ca < -1.0) ca = -1.0;
	if (cap)
	    *cap = ca;

	if (!Bp)
	    return;

	if (sc < 1e-7)
	    B = cc < 0 ? A : PI-A;
	else {
	    sA = sin(A);
	    y = sA*sb*sc;
	    x = cb - ca*cc;
	    B = y ? (x ? atan2(y,x) : (y>0 ? PI/2 : -PI/2)) : (x>=0 ? 0 : PI);
	}

	*Bp = B;
	range (Bp, 2*PI);
}

/* #define WANT_MATHERR if your system supports it. it gives SGI fits.
 */
#undef WANT_MATHERR
#if defined(WANT_MATHERR)
/* attempt to do *something* reasonable when a math function blows.
 */
matherr (xp)
struct exception *xp;
{
	static char *names[8] = {
	    "acos", "asin", "atan2", "pow",
	    "exp", "log", "log10", "sqrt"
	};
	int i;

	/* catch-all */
	xp->retval = 0.0;

	for (i = 0; i < sizeof(names)/sizeof(names[0]); i++)
	    if (strcmp (xp->name, names[i]) == 0)
		switch (i) {
		case 0:	/* acos */
		    xp->retval = xp->arg1 >= 1.0 ? 0.0 : -PI;
		    break;
		case 1: /* asin */
		    xp->retval = xp->arg1 >= 1.0 ? PI/2 : -PI/2;
		    break;
		case 2: /* atan2 */
		    if (xp->arg1 == 0.0)
			xp->retval = xp->arg2 < 0.0 ? PI : 0.0;
		    else if (xp->arg2 == 0.0)
			xp->retval = xp->arg1 < 0.0 ? -PI/2 : PI/2;
		    else
			xp->retval = 0.0;
		    break;
		case 3: /* pow */
		/* FALLTHRU */
		case 4: /* exp */
		    xp->retval = xp->o_type == OVERFLOW ? 1e308 : 0.0;
		    break;
		case 5: /* log */
		/* FALLTHRU */
		case 6: /* log10 */
		    xp->retval = xp->arg1 <= 0.0 ? -1e308 : 0;
		    break;
		case 7: /* sqrt */
		    xp->retval = 0.0;
		    break;
		}

        return (1);     /* suppress default error handling */
}
#endif

/* given the difference in two RA's, in rads, return their difference,
 *   accounting for wrap at 2*PI. caller need *not* first force it into the
 *   range 0..2*PI.
 */
double
delra (double dra)
{
	double fdra = fmod(fabs(dra), 2*PI);

	if (fdra > PI)
	    fdra = 2*PI - fdra;
	return (fdra);
}

/* return 1 if object is considered to be "deep sky", else 0.
 * The only things deep-sky are fixed objects other than stars.
 */
int
is_deepsky (Obj *op)
{
	int deepsky = 0;

	if (is_type(op, FIXEDM)) {
	    switch (op->f_class) {
	    case 'T':
	    case 'B':
	    case 'D':
	    case 'M':
	    case 'S':
	    case 'V':
		break;
	    default:
		deepsky = 1;
		break;
	    }
	}

	return (deepsky);
}

