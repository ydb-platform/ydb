/* uranus moon info */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <math.h>

#include "astro.h"
#include "bdl.h"

static int use_bdl (double jd, char *dir, MoonData md[U_NMOONS]);
static void moonradec (double usize, MoonData md[U_NMOONS]);
static void moonSVis (Obj *sop, Obj *uop, MoonData md[U_NMOONS]);
static void moonEVis (MoonData md[U_NMOONS]);
static void moonPShad (Obj *sop, Obj *uop, MoonData md[U_NMOONS]);
static void moonTrans (MoonData md[U_NMOONS]);

/* moon table and a few other goodies and when it was last computed */
static double mdmjd = -123456;
static MoonData umd[U_NMOONS] = {
    {"Uranus", NULL},
    {"Ariel", "I"},
    {"Umbriel", "II"},
    {"Titania", "III"},
    {"Oberon", "IV"},
    {"Miranda", "V"},
};
static double sizemjd;	/* size at last mjd */

/* These values are from the Explanatory Supplement.
 * Precession degrades them gradually over time.
 */
#define POLE_RA         degrad(257.43)  /* RA of Uranus' north pole */
#define POLE_DEC        degrad(-15.10)  /* Dec of Uranus' north pole */


/* get uranus info in md[0], moon info in md[1..U_NMOONS-1].
 * if !uop caller just wants md[] for names
 * N.B. we assume sop and uop are updated.
 */
void
uranus_data (
double Mjd,		/* mjd */
char dir[],             /* dir in which to look for helper files */
Obj *sop,               /* Sun */
Obj *uop,		/* uranus */
double *sizep,		/* u angular diam, rads */
double *polera, double *poledec,      /* pole location */
MoonData md[U_NMOONS])	/* return info */
{
        double JD;

	/* always copy back at least for name */
	memcpy (md, umd, sizeof(umd));

	/* pole */
	if (polera) *polera = POLE_RA;
	if (poledec) *poledec = POLE_DEC;

	/* nothing else if repeat call or just want names */
	if (Mjd == mdmjd || !uop) {
	    if (uop) {
		*sizep = sizemjd;
	    }
	    return;
	}
	JD = Mjd + MJD0;

	/* planet in [0] */
	md[0].ra = uop->s_ra;
	md[0].dec = uop->s_dec;
	md[0].mag = get_mag(uop);
	md[0].x = 0;
	md[0].y = 0;
	md[0].z = 0;
	md[0].evis = 1;
	md[0].svis = 1;

	/* size is straight from uop */
	*sizep = degrad(uop->s_size/3600.0);

        /* from Pasachoff/Menzel */

	md[1].mag = 14.2;
	md[2].mag = 14.8;
	md[3].mag = 13.7;
	md[4].mag = 14.0;
	md[5].mag = 16.3;

	/* get moon x,y,z from BDL if possible */
	if (use_bdl (JD, dir, md) < 0) {
	    int i;
	    for (i = 1; i < U_NMOONS; i++)
		md[i].x = md[i].y = md[i].z = 0.0;
	    /*fprintf (stderr, "No uranus model available\n");*/
	}

	/* set visibilities */
	moonSVis (sop, uop, md);
	moonPShad (sop, uop, md);
	moonEVis (md);
	moonTrans (md);

	/* fill in moon ra and dec */
	moonradec (*sizep, md);

	/* save */
	mdmjd = Mjd;
	sizemjd = *sizep;
	memcpy (umd, md, sizeof(umd));
}

/* hunt for BDL file in dir[] and use if possible
 * return 0 if ok, else -1
 */
static int
use_bdl (
double JD,		/* julian date */
char dir[],		/* directory */
MoonData md[U_NMOONS])	/* fill md[1..NM-1].x/y/z for each moon */
{
#define URAU    .0001597        /* Uranus radius, AU */
	double x[U_NMOONS], y[U_NMOONS], z[U_NMOONS];
        BDL_Dataset *dataset;
	int i;

	/* check ranges and appropriate data file */
	if (JD < 2451179.50000)		/* Jan 1 1999 UTC */
	    return (-1);
	if (JD < 2455562.5)		/* Jan 1 2011 UTC */
            dataset = & uranus_9910;
	else if (JD < 2459215.5)	/* Jan 1 2021 UTC */
            dataset = & uranus_1020;
        else if (JD < 2466520.5)        /* Jan 1 2041 UTC */
            dataset = & uranus_2040;
	else
	    return (-1);

	/* use it */
        do_bdl(dataset, JD, x, y, z);

	/* copy into md[1..NM-1] with our scale and sign conventions */
	for (i = 1; i < U_NMOONS; i++) {
	    md[i].x =  x[i-1]/URAU;	/* we want u radii +E */
	    md[i].y = -y[i-1]/URAU;	/* we want u radii +S */
	    md[i].z = -z[i-1]/URAU;	/* we want u radii +front */
	}

	/* ok */
	return (0);
}

/* given uranus loc in md[0].ra/dec and size, and location of each moon in 
 * md[1..NM-1].x/y in ura radii, find ra/dec of each moon in md[1..NM-1].ra/dec.
 */
static void
moonradec (
double usize,		/* ura diameter, rads */
MoonData md[U_NMOONS])	/* fill in RA and Dec */
{
	double urad = usize/2;
	double ura = md[0].ra;
	double udec = md[0].dec;
	int i;

	for (i = 1; i < U_NMOONS; i++) {
	    double dra  = urad * md[i].x;
	    double ddec = urad * md[i].y;
	    md[i].ra  = ura + dra;
	    md[i].dec = udec - ddec;
	}
}

/* set svis according to whether moon is in sun light */
static void
moonSVis(
Obj *sop,		/* SUN */
Obj *uop,		/* uranus */
MoonData md[U_NMOONS])
{
	double esd = sop->s_edist;
	double eod = uop->s_edist;
	double sod = uop->s_sdist;
	double soa = degrad(uop->s_elong);
	double esa = asin(esd*sin(soa)/sod);
	double   h = sod*uop->s_hlat;
	double nod = h*(1./eod - 1./sod);
	double sca = cos(esa), ssa = sin(esa);
	int i;

	for (i = 1; i < U_NMOONS; i++) {
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
moonEVis (MoonData md[U_NMOONS])
{
	int i;

	for (i = 1; i < U_NMOONS; i++) {
	    MoonData *mdp = &md[i];
	    int outside = mdp->x*mdp->x + mdp->y*mdp->y > 1.0;
	    int infront = mdp->z > 0.0;
	    mdp->evis = outside || infront;
	}
}

/* set pshad and sx,sy shadow info */
static void
moonPShad(
Obj *sop,             /* SUN */
Obj *uop,             /* uranus */
MoonData md[U_NMOONS])
{
	int i;

	for (i = 1; i < U_NMOONS; i++) {
	    MoonData *mdp = &md[i];
	    mdp->pshad = !plshadow (uop, sop, POLE_RA, POLE_DEC, mdp->x,
					  mdp->y, mdp->z, &mdp->sx, &mdp->sy);
	}
}

/* set whether moons are transiting */
static void
moonTrans (MoonData md[U_NMOONS])
{
	int i;

	for (i = 1; i < U_NMOONS; i++) {
	    MoonData *mdp = &md[i];
	    mdp->trans = mdp->z > 0 && mdp->x*mdp->x + mdp->y*mdp->y < 1;
	}
}


