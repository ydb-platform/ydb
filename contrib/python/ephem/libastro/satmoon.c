/* saturn moon info */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <math.h>

#include "astro.h"
#include "bdl.h"

static int use_bdl (double JD, char *dir, MoonData md[S_NMOONS]);
static void bruton_saturn (Obj *sop, double JD, MoonData md[S_NMOONS]);
static void moonradec (double satsize, MoonData md[S_NMOONS]);
static void moonSVis (Obj *eop, Obj *sop, MoonData md[S_NMOONS]);
static void moonEVis (MoonData md[S_NMOONS]);
static void moonPShad (Obj *eop, Obj *sop, MoonData md[S_NMOONS]);
static void moonTrans (MoonData md[S_NMOONS]);

/* moon table and a few other goodies and when it was last computed */
static double mdmjd = -123456;
static MoonData smd[S_NMOONS] = {
    {"Saturn",	NULL},
    {"Mimas",	"I"},
    {"Enceladus","II"},
    {"Tethys",	"III"},
    {"Dione",	"IV"},
    {"Rhea",	"V"},
    {"Titan",	"VI"},
    {"Hyperion","VII"},
    {"Iapetus",	"VIII"},
};
static double sizemjd;
static double etiltmjd;
static double stiltmjd;

/* These values are from the Explanatory Supplement.
 * Precession degrades them gradually over time.
 */
#define POLE_RA         degrad(40.58)   /* RA of Saturn's north pole */
#define POLE_DEC        degrad(83.54)   /* Dec of Saturn's north pole */


/* get saturn info in md[0], moon info in md[1..S_NMOONS-1].
 * if !dir always use bruton model.
 * if !sop caller just wants md[] for names
 * N.B. we assume eop and sop are updated.
 */
void
saturn_data (
double Mjd,			/* mjd */
char dir[],			/* dir in which to look for helper files */
Obj *eop,			/* earth == Sun */
Obj *sop,			/* saturn */
double *sizep,			/* saturn's angular diam, rads */
double *etiltp, double *stiltp,	/* earth and sun tilts -- +S */
double *polera, double *poledec,/* pole location */
MoonData md[S_NMOONS])		/* return info */
{
	double JD;

	/* always copy back at least for name */
	memcpy (md, smd, sizeof(smd));

	/* pole */
	if (polera) *polera = POLE_RA;
	if (poledec) *poledec = POLE_DEC;

	/* nothing else if repeat call or just want names */
	if (Mjd == mdmjd || !sop) {
	    if (sop) {
		*sizep = sizemjd;
		*etiltp = etiltmjd;
		*stiltp = stiltmjd;
	    }
	    return;
	}
	JD = Mjd + MJD0;

	/* planet in [0] */
	md[0].ra = sop->s_ra;
	md[0].dec = sop->s_dec;
	md[0].mag = get_mag(sop);
	md[0].x = 0;
	md[0].y = 0;
	md[0].z = 0;
	md[0].evis = 1;
	md[0].svis = 1;

	/* size is straight from sop */
	*sizep = degrad(sop->s_size/3600.0);

	/*  Visual Magnitude of the Satellites */

	md[1].mag = 13; md[2].mag = 11.8; md[3].mag = 10.3; md[4].mag = 10.2;
	md[5].mag = 9.8; md[6].mag = 8.4; md[7].mag = 14.3; md[8].mag = 11.2;

	/* get tilts from sky and tel code */
	satrings (sop->s_hlat, sop->s_hlong, sop->s_sdist, eop->s_hlong,
					    eop->s_edist, JD, etiltp, stiltp);

	/* get moon x,y,z from BDL if possible, else Bruton's model */
	if (use_bdl (JD, dir, md) < 0)
	    bruton_saturn (sop, JD, md);

	/* set visibilities */
	moonSVis (eop, sop, md);
	moonPShad (eop, sop, md);
	moonEVis (md);
	moonTrans (md);

	/* fill in moon ra and dec */
	moonradec (*sizep, md);

	/* save */
	mdmjd = Mjd;
	etiltmjd = *etiltp;
	stiltmjd = *stiltp;
	sizemjd = *sizep;
	memcpy (smd, md, sizeof(smd));
}

/* hunt for BDL file in dir[] and use if possible.
 * return 0 if ok, else -1
 */
static int
use_bdl (
double JD,		/* julian date */
char dir[],		/* directory */
MoonData md[S_NMOONS])	/* fill md[1..NM-1].x/y/z for each moon */
{
#define	SATRAU	.0004014253	/* saturn radius, AU */
	double x[S_NMOONS], y[S_NMOONS], z[S_NMOONS];
        BDL_Dataset *dataset;
	int i;

	/* check ranges and appropriate data file */
	if (JD < 2451179.50000)		/* Jan 1 1999 UTC */
	    return (-1);
	if (JD < 2455562.5)		/* Jan 1 2011 UTC */
            dataset = & saturne_9910;
	else if (JD < 2459215.5)	/* Jan 1 2021 UTC */
            dataset = & saturne_1020;
        else if (JD < 2466520.5)        /* Jan 1 2041 UTC */
            dataset = & saturne_2040;
	else
	    return (-1);

	/* use it */
        do_bdl(dataset, JD, x, y, z);

	/* copy into md[1..NM-1] with our scale and sign conventions */
	for (i = 1; i < S_NMOONS; i++) {
	    md[i].x =  x[i-1]/SATRAU;	/* we want sat radii +E */
	    md[i].y = -y[i-1]/SATRAU;	/* we want sat radii +S */
	    md[i].z = -z[i-1]/SATRAU;	/* we want sat radii +front */
	}

	/* ok */
	return (0);
}

/*  */
/*     SS2TXT.BAS                     Dan Bruton, astro@tamu.edu */
/*  */
/*       This is a text version of SATSAT2.BAS.  It is smaller, */
/*    making it easier to convert other languages (250 lines */
/*    compared to 850 lines). */
/*  */
/*       This BASIC program computes and displays the locations */
/*    of Saturn's Satellites for a given date and time.  See */
/*    "Practical Astronomy with your Calculator" by Peter */
/*    Duffett-Smith and the Astronomical Almanac for explanations */
/*    of some of the calculations here.  The code is included so */
/*    that users can make changes or convert to other languages. */
/*    This code was made using QBASIC (comes with DOS 5.0). */
/*  */
/*    ECD: merged with Sky and Tel, below, for better earth and sun ring tilt */
/*  */

/* ECD: BASICeze */
#define	FOR	for
#define	IF	if
#define	ELSE	else
#define	COS	cos
#define	SIN	sin
#define	TAN	tan
#define ATN	atan
#define ABS	fabs
#define SQR	sqrt

/* find saturn moon data from Bruton's model */
/* this originally computed +X:East +Y:North +Z:behind in [1..8] indeces.
 * and +tilt:front south, rads
 * then we adjust things in md[].x/y/z/mag to fit into our MoonData format.
 */
static void
bruton_saturn (
Obj *sop,		/* saturn */
double JD,		/* julian date */
MoonData md[S_NMOONS])	/* fill md[1..NM-1].x/y/z for each moon */
{
     /* ECD: code does not use [0].
      * ECD and why 11 here? seems like 9 would do
      */
     double SMA[11], U[11], U0[11], PD[11];
     double X[S_NMOONS], Y[S_NMOONS], Z[S_NMOONS];

     double P,TP,TE,EP,EE,RE0,RP0,RS;
     double JDE,LPE,LPP,LEE,LEP;
     double NN,ME,MP,VE,VP;
     double LE,LP,RE,RP,DT,II,F,F1;
     double RA,DECL;
     double TVA,PVA,TVC,PVC,DOT1,INC,TVB,PVB,DOT2,INCI;
     double TRIP,GAM,TEMPX,TEMPY,TEMPZ;
     int I;

     /* saturn */
     RA = sop->s_ra;
     DECL = sop->s_dec;

     /*  ******************************************************************** */
     /*  *                                                                  * */
     /*  *                        Constants                                 * */
     /*  *                                                                  * */
     /*  ******************************************************************** */
     P = PI / 180;
     /*  Orbital Rate of Saturn in Radians per Days */
     TP = 2 * PI / (29.45771 * 365.2422);
     /*  Orbital Rate of Earth in Radians per Day */
     TE = 2 * PI / (1.00004 * 365.2422);
     /*  Eccentricity of Saturn's Orbit */
     EP = .0556155;
     /*  Eccentricity of Earth's Orbit */
     EE = .016718;
     /*  Semimajor axis of Earth's and Saturn's orbit in Astronomical Units */
     RE0 = 1; RP0 = 9.554747;
     /*  Semimajor Axis of the Satellites' Orbit in Kilometers */
     SMA[1] = 185600; SMA[2] = 238100; SMA[3] = 294700; SMA[4] = 377500;
     SMA[5] = 527200; SMA[6] = 1221600; SMA[7] = 1483000; SMA[8] = 3560100;
     /*  Eccentricity of Satellites' Orbit [Program uses 0] */
     /*  Synodic Orbital Period of Moons in Days */
     PD[1] = .9425049;
     PD[2] = 1.3703731;
     PD[3] = 1.8880926;
     PD[4] = 2.7375218;
     PD[5] = 4.5191631;
     PD[6] = 15.9669028;
     PD[7] = 21.3174647;
     PD[8] = 79.9190206;	/* personal mail 1/14/95 */
     RS = 60330; /*  Radius of planet in kilometers */
    
     /*  ******************************************************************** */
     /*  *                                                                  * */
     /*  *                      Epoch Information                           * */
     /*  *                                                                  * */
     /*  ******************************************************************** */
     JDE = 2444238.5; /*  Epoch Jan 0.0 1980 = December 31,1979 0:0:0 UT */
     LPE = 165.322242 * P; /*  Longitude of Saturn at Epoch */
     LPP = 92.6653974 * P; /*  Longitude of Saturn`s Perihelion */
     LEE = 98.83354 * P; /*  Longitude of Earth at Epoch */
     LEP = 102.596403 * P; /*  Longitude of Earth's Perihelion */
     /*  U0[I] = Angle from inferior geocentric conjuction */
     /*          measured westward along the orbit at epoch */
     U0[1] = 18.2919 * P;
     U0[2] = 174.2135 * P;
     U0[3] = 172.8546 * P;
     U0[4] = 76.8438 * P;
     U0[5] = 37.2555 * P;
     U0[6] = 57.7005 * P;
     U0[7] = 266.6977 * P;
     U0[8] = 195.3513 * P;	/* from personal mail 1/14/1995 */
    
     /*  ******************************************************************** */
     /*  *                                                                  * */
     /*  *                    Orbit Calculations                            * */
     /*  *                                                                  * */
     /*  ******************************************************************** */
     /*  ****************** FIND MOON ORBITAL ANGLES ************************ */
     NN = JD - JDE; /*  NN = Number of days since epoch */
     ME = ((TE * NN) + LEE - LEP); /*  Mean Anomoly of Earth */
     MP = ((TP * NN) + LPE - LPP); /*  Mean Anomoly of Saturn */
     VE = ME; VP = MP; /*  True Anomolies - Solve Kepler's Equation */
     FOR (I = 1; I <= 3; I++) {
	 VE = VE - (VE - (EE * SIN(VE)) - ME) / (1 - (EE * COS(VE)));
	 VP = VP - (VP - (EP * SIN(VP)) - MP) / (1 - (EP * COS(VP)));
     }
     VE = 2 * ATN(SQR((1 + EE) / (1 - EE)) * TAN(VE / 2));
     IF (VE < 0) VE = (2 * PI) + VE;
     VP = 2 * ATN(SQR((1 + EP) / (1 - EP)) * TAN(VP / 2));
     IF (VP < 0) VP = (2 * PI) + VP;
     /*   Heliocentric Longitudes of Earth and Saturn */
     LE = VE + LEP; IF (LE > (2 * PI)) LE = LE - (2 * PI);
     LP = VP + LPP; IF (LP > (2 * PI)) LP = LP - (2 * PI);
     /*   Distances of Earth and Saturn from the Sun in AU's */
     RE = RE0 * (1 - EE * EE) / (1 + EE * COS(VE));
     RP = RP0 * (1 - EP * EP) / (1 + EP * COS(VP));
     /*   DT = Distance from Saturn to Earth in AU's - Law of Cosines */
     DT = SQR((RE * RE) + (RP * RP) - (2 * RE * RP * COS(LE - LP)));
     /*   II = Angle between Earth and Sun as seen from Saturn */
     II = RE * SIN(LE - LP) / DT;
     II = ATN(II / SQR(1 - II * II)); /*   ArcSIN and Law of Sines */
     /*    F = NN - (Light Time to Earth in days) */
     F = NN - (DT / 173.83);
     F1 = II + MP - VP;
     /*  U(I) = Angle from inferior geocentric conjuction measured westward */
     FOR (I = 1; I < S_NMOONS; I++) {
	U[I] = U0[I] + (F * 2 * PI / PD[I]) + F1;
	U[I] = ((U[I] / (2 * PI)) - (int)(U[I] / (2 * PI))) * 2 * PI;

     }

     /*  **************** FIND INCLINATION OF RINGS ************************* */
     /*  Use dot product of Earth-Saturn vector and Saturn's rotation axis */
     TVA = (90 - 83.51) * P; /*  Theta coordinate of Saturn's axis */
     PVA = 40.27 * P; /*  Phi coordinate of Saturn's axis */
     TVC = (PI / 2) - DECL;
     PVC = RA;
     DOT1 = SIN(TVA) * COS(PVA) * SIN(TVC) * COS(PVC);
     DOT1 = DOT1 + SIN(TVA) * SIN(PVA) * SIN(TVC) * SIN(PVC);
     DOT1 = DOT1 + COS(TVA) * COS(TVC);
     INC = ATN(SQR(1 - DOT1 * DOT1) / DOT1); /*    ArcCOS */
     IF (INC > 0) INC = (PI / 2) - INC; ELSE INC = -(PI / 2) - INC;

     /*  ************* FIND INCLINATION OF IAPETUS' ORBIT ******************* */
     /*  Use dot product of Earth-Saturn vector and Iapetus' orbit axis */
     /*  Vector B */
     TVB = (90 - 75.6) * P; /*  Theta coordinate of Iapetus' orbit axis (estimate) */
     PVB = 21.34 * 2 * PI / 24; /*  Phi coordinate of Iapetus' orbit axis (estimate) */
     DOT2 = SIN(TVB) * COS(PVB) * SIN(TVC) * COS(PVC);
     DOT2 = DOT2 + SIN(TVB) * SIN(PVB) * SIN(TVC) * SIN(PVC);
     DOT2 = DOT2 + COS(TVB) * COS(TVC);
     INCI = ATN(SQR(1 - DOT2 * DOT2) / DOT2); /*    ArcCOS */
     IF (INCI > 0) INCI = (PI / 2) - INCI; ELSE INCI = -(PI / 2) - INCI;

     /*  ************* FIND ROTATION ANGLE OF IAPETUS' ORBIT **************** */
     /*  Use inclination of Iapetus' orbit with respect to ring plane */
     /*  Triple Product */
     TRIP = SIN(TVC) * COS(PVC) * SIN(TVA) * SIN(PVA) * COS(TVB);
     TRIP = TRIP - SIN(TVC) * COS(PVC) * SIN(TVB) * SIN(PVB) * COS(TVA);
     TRIP = TRIP + SIN(TVC) * SIN(PVC) * SIN(TVB) * COS(PVB) * COS(TVA);
     TRIP = TRIP - SIN(TVC) * SIN(PVC) * SIN(TVA) * COS(PVA) * COS(TVB);
     TRIP = TRIP + COS(TVC) * SIN(TVA) * COS(PVA) * SIN(TVB) * SIN(PVB);
     TRIP = TRIP - COS(TVC) * SIN(TVB) * COS(PVB) * SIN(TVA) * SIN(PVA);
     GAM = -1 * ATN(TRIP / SQR(1 - TRIP * TRIP)); /*  ArcSIN */
    
     /*  ******************************************************************** */
     /*  *                                                                  * */
     /*  *                     Compute Moon Positions                       * */
     /*  *                                                                  * */
     /*  ******************************************************************** */
     FOR (I = 1; I < S_NMOONS - 1; I++) {
	 X[I] = -1 * SMA[I] * SIN(U[I]) / RS;
	 Z[I] = -1 * SMA[I] * COS(U[I]) / RS;	/* ECD */
	 Y[I] = SMA[I] * COS(U[I]) * SIN(INC) / RS;
     }
     /*  ************************* Iapetus' Orbit *************************** */
     TEMPX = -1 * SMA[8] * SIN(U[8]) / RS;
     TEMPZ = -1 * SMA[8] * COS(U[8]) / RS;
     TEMPY = SMA[8] * COS(U[8]) * SIN(INCI) / RS;
     X[8] = TEMPX * COS(GAM) + TEMPY * SIN(GAM); /*       Rotation */
     Z[8] = TEMPZ * COS(GAM) + TEMPY * SIN(GAM);
     Y[8] = -1 * TEMPX * SIN(GAM) + TEMPY * COS(GAM);
    
#ifdef SHOWALL
     /*  ******************************************************************** */
     /*  *                                                                  * */
     /*  *                          Show Results                            * */
     /*  *                                                                  * */
     /*  ******************************************************************** */
     printf ("                           Julian Date : %g\n", JD);
     printf ("             Right Ascension of Saturn : %g Hours\n", RA * 24 / (2 * PI));
     printf ("                 Declination of Saturn : %g\n", DECL / P);
     printf ("   Ring Inclination as seen from Earth : %g\n", -1 * INC / P);
     printf ("      Heliocentric Longitude of Saturn : %g\n", LP / P);
     printf ("       Heliocentric Longitude of Earth : %g\n", LE / P);
     printf ("                Sun-Saturn-Earth Angle : %g\n", II / P);
     printf ("     Distance between Saturn and Earth : %g AU = %g million miles\n", DT, (DT * 93));
     printf ("       Light time from Saturn to Earth : %g minutes\n", DT * 8.28);
     TEMP = 2 * ATN(TAN(165.6 * P / (2 * 3600)) / DT) * 3600 / P;
     printf ("                Angular Size of Saturn : %g arcsec\n", TEMP);
     printf ("  Major Angular Size of Saturn's Rings : %g arcsec\n", RS4 * TEMP / RS);
     printf ("  Minor Angular Size of Saturn's Rings : %g arcsec\n", ABS(RS4 * TEMP * SIN(INC) / RS));
#endif

     /* copy into md[1..S_NMOONS-1] with our sign conventions */
     for (I = 1; I < S_NMOONS; I++) {
	md[I].x =  X[I];	/* we want +E */
	md[I].y = -Y[I];	/* we want +S */
	md[I].z = -Z[I];	/* we want +front */
     }
}

/* given saturn loc in md[0].ra/dec and size, and location of each moon in 
 * md[1..NM-1].x/y in sat radii, find ra/dec of each moon in md[1..NM-1].ra/dec.
 */
static void
moonradec (
double satsize,		/* sat diameter, rads */
MoonData md[S_NMOONS])	/* fill in RA and Dec */
{
	double satrad = satsize/2;
	double satra = md[0].ra;
	double satdec = md[0].dec;
	int i;

	for (i = 1; i < S_NMOONS; i++) {
	    double dra  = satrad * md[i].x;
	    double ddec = satrad * md[i].y;
	    md[i].ra  = satra + dra;
	    md[i].dec = satdec - ddec;
	}
}

/* set svis according to whether moon is in sun light */
static void
moonSVis(
Obj *eop,		/* earth == SUN */
Obj *sop,		/* saturn */
MoonData md[S_NMOONS])
{
	double esd = eop->s_edist;
	double eod = sop->s_edist;
	double sod = sop->s_sdist;
	double soa = degrad(sop->s_elong);
	double esa = asin(esd*sin(soa)/sod);
	double   h = sod*sop->s_hlat;
	double nod = h*(1./eod - 1./sod);
	double sca = cos(esa), ssa = sin(esa);
	int i;

	for (i = 1; i < S_NMOONS; i++) {
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
moonEVis (MoonData md[S_NMOONS])
{
	int i;

	for (i = 1; i < S_NMOONS; i++) {
	    MoonData *mdp = &md[i];
	    int outside = mdp->x*mdp->x + mdp->y*mdp->y > 1.0;
	    int infront = mdp->z > 0.0;
	    mdp->evis = outside || infront;
	}
}

/* set pshad and sx,sy shadow info */
static void
moonPShad(
Obj *eop,             /* earth == SUN */
Obj *sop,             /* saturn */
MoonData md[S_NMOONS])
{
	int i;

	for (i = 1; i < S_NMOONS; i++) {
	    MoonData *mdp = &md[i];
	    mdp->pshad = !plshadow (sop, eop, POLE_RA, POLE_DEC, mdp->x,
					  mdp->y, mdp->z, &mdp->sx, &mdp->sy);
	}
}


/* set whether moons are transiting */
static void
moonTrans (MoonData md[S_NMOONS])
{
	int i;

	for (i = 1; i < S_NMOONS; i++) {
	    MoonData *mdp = &md[i];
	    mdp->trans = mdp->z > 0 && mdp->x*mdp->x + mdp->y*mdp->y < 1;
	}
}

