/* mars moon info */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <math.h>

#include "astro.h"
#include "bdl.h"

static int use_bdl (double JD, char *dir, MoonData md[M_NMOONS]);
static void moonradec (double msize, MoonData md[M_NMOONS]);
static void moonSVis (Obj *sop, Obj *mop, MoonData md[M_NMOONS]);
static void moonEVis (MoonData md[M_NMOONS]);
static void moonPShad (Obj *sop, Obj *mop, MoonData md[M_NMOONS]);
static void moonTrans (MoonData md[M_NMOONS]);

/* moon table and a few other goodies and when it was last computed */
static double mdmjd = -123456;
static MoonData mmd[M_NMOONS] = {
    {"Mars", NULL},
    {"Phobos", "I"},
    {"Deimos", "II"},
};
static double sizemjd;

/* These values are from the Explanatory Supplement.
 * Precession degrades them gradually over time.
 */
#define POLE_RA         degrad(317.61)
#define POLE_DEC        degrad(52.85)


/* get mars info in md[0], moon info in md[1..M_NMOONS-1].
 * if !dir always use bruton model.
 * if !mop caller just wants md[] for names
 * N.B. we assume sop and mop are updated.
 */
void
marsm_data (
double Mjd,			/* mjd */
char dir[],			/* dir in which to look for helper files */
Obj *sop,			/* Sun */
Obj *mop,			/* mars */
double *sizep,			/* mars's angular diam, rads */
double *polera, double *poledec,/* pole location */
MoonData md[M_NMOONS])		/* return info */
{
	double JD, dmag;

	/* always copy back at least for name */
	memcpy (md, mmd, sizeof(mmd));

	/* pole */
	if (polera) *polera = POLE_RA;
	if (poledec) *poledec = POLE_DEC;

	/* nothing else if repeat call or just want names */
	if (Mjd == mdmjd || !mop) {
	    if (mop) {
		*sizep = sizemjd;
	    }
	    return;
	}
	JD = Mjd + MJD0;

	/* planet in [0] */
	md[0].ra = mop->s_ra;
	md[0].dec = mop->s_dec;
	md[0].mag = get_mag(mop);
	md[0].x = 0;
	md[0].y = 0;
	md[0].z = 0;
	md[0].evis = 1;
	md[0].svis = 1;

	/* size is straight from mop */
	*sizep = degrad(mop->s_size/3600.0);

	/* from Pasachoff/Menzel: brightest @ .6 AU */
	dmag = 5.0*log10(mop->s_edist + 0.4);
	md[1].mag = 11.8 + dmag;
	md[2].mag = 12.9 + dmag;

	/* get moon x,y,z from BDL if possible */
	if (use_bdl (JD, dir, md) < 0) {
	    int i;
	    for (i = 1; i < M_NMOONS; i++)
		md[i].x = md[i].y = md[i].z = 0.0;
	    /*fprintf (stderr, "No mars model available\n");*/
	}

	/* set visibilities */
	moonSVis (sop, mop, md);
	moonPShad (sop, mop, md);
	moonEVis (md);
	moonTrans (md);

	/* fill in moon ra and dec */
	moonradec (*sizep, md);

	/* save */
	mdmjd = Mjd;
	sizemjd = *sizep;
	memcpy (mmd, md, sizeof(mmd));
}

/* hunt for BDL file in dir[] and use if possible.
 * return 0 if ok, else -1
 */
static int
use_bdl (
double JD,		/* julian date */
char dir[],		/* directory */
MoonData md[M_NMOONS])	/* fill md[1..NM-1].x/y/z for each moon */
{
#define MRAU    .00002269       /* Mars radius, AU */
	double x[M_NMOONS], y[M_NMOONS], z[M_NMOONS];
	BDL_Dataset *dataset;
	int i;

	/* check ranges and appropriate data file */
	if (JD < 2451179.50000)		/* Jan 1 1999 UTC */
	    return (-1);
	if (JD < 2455562.5)		/* Jan 1 2011 UTC */
            dataset = & mars_9910;
	else if (JD < 2459215.5)	/* Jan 1 2021 UTC */
	    dataset = & mars_1020;
        else if (JD < 2466520.5)        /* Jan 1 2041 UTC */
            dataset = & mars_2040;
	else
	    return (-1);

	/* use it */
        do_bdl(dataset, JD, x, y, z);

	/* copy into md[1..NM-1] with our scale and sign conventions */
	for (i = 1; i < M_NMOONS; i++) {
	    md[i].x =  x[i-1]/MRAU;	/* we want mars radii +E */
	    md[i].y = -y[i-1]/MRAU;	/* we want mars radii +S */
	    md[i].z = -z[i-1]/MRAU;	/* we want mars radii +front */
	}

	/* ok */
	return (0);
}

/* given mars loc in md[0].ra/dec and size, and location of each moon in 
 * md[1..NM-1].x/y in mars radii,find ra/dec of each moon in md[1..NM-1].ra/dec.
 */
static void
moonradec (
double msize,		/* mars diameter, rads */
MoonData md[M_NMOONS])	/* fill in RA and Dec */
{
	double mrad = msize/2;
	double mra = md[0].ra;
	double mdec = md[0].dec;
	int i;

	for (i = 1; i < M_NMOONS; i++) {
	    double dra  = mrad * md[i].x;
	    double ddec = mrad * md[i].y;
	    md[i].ra  = mra + dra;
	    md[i].dec = mdec - ddec;
	}
}

/* set svis according to whether moon is in sun light */
static void
moonSVis(
Obj *sop,		/* SUN */
Obj *mop,		/* mars */
MoonData md[M_NMOONS])
{
	double esd = sop->s_edist;
	double eod = mop->s_edist;
	double sod = mop->s_sdist;
	double soa = degrad(mop->s_elong);
	double esa = asin(esd*sin(soa)/sod);
	double   h = sod*mop->s_hlat;
	double nod = h*(1./eod - 1./sod);
	double sca = cos(esa), ssa = sin(esa);
	int i;

	for (i = 1; i < M_NMOONS; i++) {
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
moonEVis (MoonData md[M_NMOONS])
{
	int i;

	for (i = 1; i < M_NMOONS; i++) {
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
Obj *mop,             /* mars */
MoonData md[M_NMOONS])
{
	int i;

	for (i = 1; i < M_NMOONS; i++) {
	    MoonData *mdp = &md[i];
	    mdp->pshad = !plshadow (mop, sop, POLE_RA, POLE_DEC, mdp->x,
					  mdp->y, mdp->z, &mdp->sx, &mdp->sy);
	}
}

/* set whether moons are transiting */
static void
moonTrans (MoonData md[M_NMOONS])
{
	int i;

	for (i = 1; i < M_NMOONS; i++) {
	    MoonData *mdp = &md[i];
	    mdp->trans = mdp->z > 0 && mdp->x*mdp->x + mdp->y*mdp->y < 1;
	}
}


