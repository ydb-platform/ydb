/* jupiter moon info */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <math.h>

#include "astro.h"
#include "bdl.h"

static int use_bdl (double jd, char *dir, MoonData md[J_NMOONS]);
static void moonradec (double jupsize, MoonData md[J_NMOONS]);
static void moonSVis (Obj *sop, Obj *jop, MoonData md[J_NMOONS]);
static void moonEVis (MoonData md[J_NMOONS]);
static void moonPShad (Obj *sop, Obj *jop, MoonData md[J_NMOONS]);
static void moonTrans (MoonData md[J_NMOONS]);

/* moon table and a few other goodies and when it was last computed */
static double mdmjd = -123456;
static MoonData jmd[J_NMOONS] = {
    {"Jupiter", NULL},
    {"Io", "I"},
    {"Europa", "II"},
    {"Ganymede", "III"},
    {"Callisto", "IV"}
};
static double sizemjd;	/* size at last mjd */
static double cmlImjd;	/* central meridian long sys I, at last mjd */
static double cmlIImjd;	/*    "                      II      " */

/* These values are from the Explanatory Supplement.
 * Precession degrades them gradually over time.
 */
#define POLE_RA   degrad(268.05)	 /* RA of Jupiter's north pole */
#define POLE_DEC  degrad(64.50)		/* Dec of Jupiter's north pole */


/* get jupiter info in md[0], moon info in md[1..J_NMOONS-1].
 * if !dir always use meeus model.
 * if !jop caller just wants md[] for names
 * N.B. we assume sop and jop are updated.
 */
void
jupiter_data (
double Mjd,		/* mjd */
char dir[],             /* dir in which to look for helper files */
Obj *sop,               /* Sun */
Obj *jop,		/* jupiter */
double *sizep,		/* jup angular diam, rads */
double *cmlI, double *cmlII,		/* central meridian longitude, rads */
double *polera, double *poledec,	/* pole location */
MoonData md[J_NMOONS])	/* return info */
{
        double JD;

	/* always copy back at least for name */
	memcpy (md, jmd, sizeof(jmd));

	/* pole */
	if (polera) *polera = POLE_RA;
	if (poledec) *poledec = POLE_DEC;

	/* nothing else if repeat call or just want names */
	if (Mjd == mdmjd || !jop) {
	    if (jop) {
		*sizep = sizemjd;
		*cmlI = cmlImjd;
		*cmlII = cmlIImjd;
	    }
	    return;
	}
	JD = Mjd + MJD0;

	/* planet in [0] */
	md[0].ra = jop->s_ra;
	md[0].dec = jop->s_dec;
	md[0].mag = get_mag(jop);
	md[0].x = 0;
	md[0].y = 0;
	md[0].z = 0;
	md[0].evis = 1;
	md[0].svis = 1;

	/* size is straight from jop */
	*sizep = degrad(jop->s_size/3600.0);

	/* mags from JPL ephemeris */
	md[1].mag = 5.7;
	md[2].mag = 5.8;
	md[3].mag = 5.3;
	md[4].mag = 6.7;

	/* get moon data from BDL if possible, else Meeus' model.
	 * always use Meeus for cml
	 */
	if (use_bdl (JD, dir, md) == 0)
	    meeus_jupiter (Mjd, cmlI, cmlII, NULL);
	else
	    meeus_jupiter (Mjd, cmlI, cmlII, md);

	/* set visibilities */
	moonSVis (sop, jop, md);
	moonPShad (sop, jop, md);
	moonEVis (md);
	moonTrans (md);

	/* fill in moon ra and dec */
	moonradec (*sizep, md);

	/* save */
	mdmjd = Mjd;
	sizemjd = *sizep;
	cmlImjd = *cmlI;
	cmlIImjd = *cmlII;
	memcpy (jmd, md, sizeof(jmd));
}

/* hunt for BDL file in dir[] and use if possible
 * return 0 if ok, else -1
 */
static int
use_bdl (
double JD,		/* julian date */
char dir[],		/* directory */
MoonData md[J_NMOONS])	/* fill md[1..NM-1].x/y/z for each moon */
{
#define	JUPRAU	.0004769108	/* jupiter radius, AU */
	double x[J_NMOONS], y[J_NMOONS], z[J_NMOONS];
	BDL_Dataset *dataset;
	int i;

	/* check ranges and appropriate data file */
	if (JD < 2451179.50000)		/* Jan 1 1999 UTC */
	    return (-1);
	if (JD < 2455562.5)		/* Jan 1 2011 UTC */
	    dataset = & jupiter_9910;
	else if (JD < 2459215.5)	/* Jan 1 2021 UTC */
            dataset = & jupiter_1020;
        else if (JD < 2466520.5)        /* Jan 1 2041 UTC */
            dataset = & jupiter_2040;
	else
	    return (-1);

	/* use it */
        do_bdl(dataset, JD, x, y, z);

	/* copy into md[1..NM-1] with our scale and sign conventions */
	for (i = 1; i < J_NMOONS; i++) {
	    md[i].x =  x[i-1]/JUPRAU;	/* we want jup radii +E */
	    md[i].y = -y[i-1]/JUPRAU;	/* we want jup radii +S */
	    md[i].z = -z[i-1]/JUPRAU;	/* we want jup radii +front */
	}

	/* ok */
	return (0);
}

/* compute location of GRS and Galilean moons.
 * if md == NULL, just to cml.
 * from "Astronomical Formulae for Calculators", 2nd ed, by Jean Meeus,
 *   Willmann-Bell, Richmond, Va., U.S.A. (c) 1982, chapters 35 and 36.
 */
void
meeus_jupiter(
double d,
double *cmlI, double *cmlII,	/* central meridian longitude, rads */
MoonData md[J_NMOONS])	/* fill in md[1..NM-1].x/y/z for each moon.
			 * N.B. md[0].ra/dec must already be set
			 */
{
#define	dsin(x)	sin(degrad(x))
#define	dcos(x)	cos(degrad(x))
	double A, B, Del, J, K, M, N, R, V;
	double cor_u1, cor_u2, cor_u3, cor_u4;
	double solc, tmp, G, H, psi, r, r1, r2, r3, r4;
	double u1, u2, u3, u4;
	double lam, Ds;
	double z1, z2, z3,  z4;
	double De, dsinDe;
	double theta, phi;
	double tvc, pvc;
	double salpha, calpha;
	int i;

	V = 134.63 + 0.00111587 * d;

	M = (358.47583 + 0.98560003*d);
	N = (225.32833 + 0.0830853*d) + 0.33 * dsin (V);

	J = 221.647 + 0.9025179*d - 0.33 * dsin(V);

	A = 1.916*dsin(M) + 0.02*dsin(2*M);
	B = 5.552*dsin(N) + 0.167*dsin(2*N);
	K = (J+A-B);
	R = 1.00014 - 0.01672 * dcos(M) - 0.00014 * dcos(2*M);
	r = 5.20867 - 0.25192 * dcos(N) - 0.00610 * dcos(2*N);
	Del = sqrt (R*R + r*r - 2*R*r*dcos(K));
	psi = raddeg (asin (R/Del*dsin(K)));

	*cmlI  = degrad(268.28 + 877.8169088*(d - Del/173) + psi - B);
	range (cmlI, 2*PI);
	*cmlII = degrad(290.28 + 870.1869088*(d - Del/173) + psi - B);
	range (cmlII, 2*PI);

	/* that's it if don't want moon info too */
	if (!md)
	    return;

	solc = (d - Del/173.);	/* speed of light correction */
	tmp = psi - B;

	u1 = 84.5506 + 203.4058630 * solc + tmp;
	u2 = 41.5015 + 101.2916323 * solc + tmp;
	u3 = 109.9770 + 50.2345169 * solc + tmp;
	u4 = 176.3586 + 21.4879802 * solc + tmp;

	G = 187.3 + 50.310674 * solc;
	H = 311.1 + 21.569229 * solc;
      
	cor_u1 =  0.472 * dsin (2*(u1-u2));
	cor_u2 =  1.073 * dsin (2*(u2-u3));
	cor_u3 =  0.174 * dsin (G);
	cor_u4 =  0.845 * dsin (H);
      
	r1 = 5.9061 - 0.0244 * dcos (2*(u1-u2));
	r2 = 9.3972 - 0.0889 * dcos (2*(u2-u3));
	r3 = 14.9894 - 0.0227 * dcos (G);
	r4 = 26.3649 - 0.1944 * dcos (H);

	md[1].x = -r1 * dsin (u1+cor_u1);
	md[2].x = -r2 * dsin (u2+cor_u2);
	md[3].x = -r3 * dsin (u3+cor_u3);
	md[4].x = -r4 * dsin (u4+cor_u4);

	lam = 238.05 + 0.083091*d + 0.33*dsin(V) + B;
	Ds = 3.07*dsin(lam + 44.5);
	De = Ds - 2.15*dsin(psi)*dcos(lam+24.)
		- 1.31*(r-Del)/Del*dsin(lam-99.4);
	dsinDe = dsin(De);

	z1 = r1 * dcos(u1+cor_u1);
	z2 = r2 * dcos(u2+cor_u2);
	z3 = r3 * dcos(u3+cor_u3);
	z4 = r4 * dcos(u4+cor_u4);

	md[1].y = z1*dsinDe;
	md[2].y = z2*dsinDe;
	md[3].y = z3*dsinDe;
	md[4].y = z4*dsinDe;

	/* compute sky transformation angle as triple vector product */
	tvc = PI/2.0 - md[0].dec;
	pvc = md[0].ra;
	theta = PI/2.0 - POLE_DEC;
	phi = POLE_RA;
	salpha = -sin(tvc)*sin(theta)*(cos(pvc)*sin(phi) - sin(pvc)*cos(phi));
	calpha = sqrt (1.0 - salpha*salpha);

	for (i = 0; i < J_NMOONS; i++) {
	    double tx =  md[i].x*calpha + md[i].y*salpha;
	    double ty = -md[i].x*salpha + md[i].y*calpha;
	    md[i].x = tx;
	    md[i].y = ty;
	}

	md[1].z = z1;
	md[2].z = z2;
	md[3].z = z3;
	md[4].z = z4;
}


/* given jupiter loc in md[0].ra/dec and size, and location of each moon in 
 * md[1..NM-1].x/y in jup radii, find ra/dec of each moon in md[1..NM-1].ra/dec.
 */
static void
moonradec (
double jupsize,		/* jup diameter, rads */
MoonData md[J_NMOONS])	/* fill in RA and Dec */
{
	double juprad = jupsize/2;
	double jupra = md[0].ra;
	double jupdec = md[0].dec;
	int i;

	for (i = 1; i < J_NMOONS; i++) {
	    double dra  = juprad * md[i].x;
	    double ddec = juprad * md[i].y;
	    md[i].ra  = jupra + dra;
	    md[i].dec = jupdec - ddec;
	}
}

/* set svis according to whether moon is in sun light */
static void
moonSVis(
Obj *sop,		/* SUN */
Obj *jop,		/* jupiter */
MoonData md[J_NMOONS])
{
	double esd = sop->s_edist;
	double eod = jop->s_edist;
	double sod = jop->s_sdist;
	double soa = degrad(jop->s_elong);
	double esa = asin(esd*sin(soa)/sod);
	double   h = sod*jop->s_hlat;
	double nod = h*(1./eod - 1./sod);
	double sca = cos(esa), ssa = sin(esa);
	int i;

	for (i = 1; i < J_NMOONS; i++) {
	    MoonData *mdp = &md[i];
	    double xp =  sca*mdp->x + ssa*mdp->z;
	    double yp =  mdp->y;
	    double zp = -ssa*mdp->x + sca*mdp->z;
	    double ca = cos(nod), sa = sin(nod);
	    double xpp = xp;
	    double ypp = ca*yp - sa*zp;
	    double zpp = sa*yp + ca*zp;
	    int outside = xpp*xpp + ypp*ypp > 1.0;
	    int infront = zpp > 0.0;
	    mdp->svis = outside || infront;
	}
}

/* set evis according to whether moon is geometrically visible from earth */
static void
moonEVis (MoonData md[J_NMOONS])
{
	int i;

	for (i = 1; i < J_NMOONS; i++) {
	    MoonData *mdp = &md[i];
	    int outside = mdp->x*mdp->x + mdp->y*mdp->y > 1.0;
	    int infront = mdp->z > 0.0;
	    mdp->evis = outside || infront;
	}
}

/* set pshad and sx,sy shadow info */
static void
moonPShad(
Obj *sop,		/* SUN */
Obj *jop,		/* jupiter */
MoonData md[J_NMOONS])
{
	int i;

	for (i = 1; i < J_NMOONS; i++) {
	    MoonData *mdp = &md[i];
	    mdp->pshad = !plshadow (jop, sop, POLE_RA, POLE_DEC, mdp->x,
					    mdp->y, mdp->z, &mdp->sx, &mdp->sy);
	}
}

/* set whether moons are transiting */
static void
moonTrans (MoonData md[J_NMOONS])
{
	int i;

	for (i = 1; i < J_NMOONS; i++) {
	    MoonData *mdp = &md[i];
	    mdp->trans = mdp->z > 0 && mdp->x*mdp->x + mdp->y*mdp->y < 1;
	}
}

