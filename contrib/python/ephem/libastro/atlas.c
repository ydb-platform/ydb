/* look up which star atlas an ra/dec is in.
 * Urano and Mill contributed by Atsuo Ohki <ohki@gssm.otsuka.tsukuba.ac.jp>
 * U2K by Robert Lane <roblane@alum.mit.edu>
 *
 * N.B. skylist.c assumes a certain max length for any returned string.
 */
 
#include <stdio.h>
#include <string.h>

#include "astro.h"

/* for Millennium Star Atlas */
static int msa_charts[] = {
    /* 90*/  2, /* 84*/  4, /* 78*/  8, /* 72*/ 10, /* 66*/ 12,
    /* 60*/ 14, /* 54*/ 16, /* 48*/ 20, /* 42*/ 20, /* 36*/ 22,
    /* 30*/ 22, /* 24*/ 24, /* 18*/ 24, /* 12*/ 24, /*  6*/ 24,
    /*  0*/ 24,
    /* -6*/ 24, /*-12*/ 24, /*-18*/ 24, /*-24*/ 24, /*-30*/ 22,
    /*-36*/ 22, /*-42*/ 20, /*-48*/ 20, /*-54*/ 16, /*-60*/ 14,
    /*-66*/ 12, /*-72*/ 10, /*-78*/  8, /*-84*/  4, /*-90*/  2
};

/*
 * find the chart number of Millennium Star Atlas and return pointer to static
 * string describing location.
 * 0 <= ra < 24;  -90 <= dec <= 90
 */
char *
msa_atlas(double ra, double dec)
{
	static char buf[512];
	int zone, band;
	int i, p;

	ra = radhr(ra);
	dec = raddeg(dec);
	buf[0] = 0;
	if (ra < 0.0 || 24.0 <= ra || dec < -90.0 || 90.0 < dec)
	    return (buf);
	zone = (int)(ra/8.0);
	band = -((int)(dec+((dec>=0)?3:-3))/6 - 15);
	for (p=0, i=0; i <= band; i++)
	    p += msa_charts[i];
	i = (int)((ra - 8.0*zone) / (8.0/msa_charts[band]));
	sprintf(buf, "V%d - P%3d", zone+1, p-i+zone*516);
	return (buf);
}

/* for original Uranometria */
static struct {
    double l;
    int n;
} um_zones[] = {
    /* 84 - 90 */ { 84.5,  2},
    /* 72 - 85 */ { 72.5, 12},
    /* 60 - 73 */ { 61.0, 20},
    /* 49 - 62 */ { 50.0, 24},
    /* 38 - 51 */ { 39.0, 30},
    /* 27 - 40 */ { 28.0, 36},
    /* 16 - 29 */ { 17.0, 45},
    /*  5 - 18 */ {  5.5, 45},
    /*  0 -  6 */ {  0.0, 45},
		  {  0.0,  0}
};

/*
 * find the chart number of Uranometria first edition and return pointer to
 * static string describing location.
 * 0 <= ra < 24;  -90 <= dec <= 90
 */
char *
um_atlas(double ra, double dec)
{
	static char buf[512];
	int band, south;
	int p;
	double w;

	ra = radhr(ra);
	dec = raddeg(dec);
	buf[0] = 0;
	if (ra < 0.0 || 24.0 <= ra || dec < -90.0 || 90.0 < dec)
	    return (buf);
	p = 0;
	if (dec < 0.0) {
	    dec = -dec;
	    south = 1;
	} else
	    south = 0;
	p = 1;
	for (band=0; um_zones[band].n; band++) {
	    if (um_zones[band].l <= dec)
		break; 
	    p += um_zones[band].n;
	}
	if (!um_zones[band].n)
	    return (buf);
	w = 24.0 / um_zones[band].n;
	if (band) {
	    ra += w/2.0;
	    if (ra >= 24.0)
		ra -= 24.0;
	}
	if (south && um_zones[band+1].n)
	    p = 475 - p - um_zones[band].n;
	if (south && band == 0) {
	    /* south pole part is mis-ordered! */
	    ra = 24.0 - ra;
	}
	sprintf(buf, "V%d - P%3d", south+1, p+(int)(ra/w));
	return (buf);
}

/* for Uranometria 2000.0 */
static struct {
    double lowDec; 	/* lower dec cutoff */
    int numZones;	/* number of panels (aka zones) */

} u2k_zones[] = { /* array of book layout info */
    /* 84 - 90 */ { 84.5,  1}, /* lower dec cutoff, # of panels in band */
    /* 73 - 85 */ { 73.5,  6},
    /* 62 - 74 */ { 62.0, 10},
    /* 51 - 63 */ { 51.0, 12},
    /* 40 - 52 */ { 40.0, 15},
    /* 29 - 41 */ { 29.0, 18},
    /* 17 - 30 */ { 17.0, 18},
    /*  5 - 18 */ {  5.5, 20},
    /*  0 -  6 */ {  0.0, 20},
                  {  0.0,  0} /*numZones value in this line is a stopper.*/
};

/* find the chart number of Uranometria 2000.0 and return pointer to static
 * string describing location.
 * 0 <= ra < 24;  -90 <= dec <= 90
 */
char *
u2k_atlas(double ra, double dec)
{
	static char buf[512];
	static char err[] = "???";
	int band; 		/* index to array */
	int south;		/* flag for volume 2*/
	int panel;		/* panel number */

	ra = radhr(ra);
	dec = raddeg(dec);
	buf[0] = 0;
	if (ra < 0.0 || 24.0 <= ra || dec < -90.0 || 90.0 < dec) {
	    strcpy (buf, err);
	    return (buf); /* range checking */
	}

	if (dec < 0.0) {
	    dec = -dec;
	    south = 1; /* South is mirror of North */
	} else
	    south = 0;

	panel = 1;
	band = 0;

	/* scan u2k_zones for the correct band: */
	while (u2k_zones[band].numZones != 0 && dec <= u2k_zones[band].lowDec ){
	    panel += u2k_zones[band].numZones; /*accumulate total panels */
	    band++ ;
	}

	if (!u2k_zones[band].numZones) { /* hit end of array with no match. */
	    strcpy (buf, err);
	    return (buf);
	}

	ra -= 12.0 / u2k_zones[band].numZones; /*offset by half-width of panel*/
	if (ra >= 24.0)			/* reality check. shouldn't happen. */
	    ra -= 24.0;

	if (ra < 0.0)			/* offset could give negative ra */
	    ra += 24.0;

	if (south && u2k_zones[band+1].numZones)
	    panel = 222 - panel - u2k_zones[band].numZones;

	/* resultant panel number is accumulated panels in prior bands plus
	 * ra's fraction of panels in dec's band. panel # goes up as ra goes
	 * down.
	 */
	sprintf(buf, "V%d - P%3d", south+1,
	panel+(int)(u2k_zones[band].numZones*(24.0 - ra)/24.0));

	return (buf);
}


