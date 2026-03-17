/* code to compute lunar sunrise position and local sun angle.
 */

#include <stdio.h>
#include <stdlib.h>
#include <math.h>

#include "astro.h"

static void Librations (double RAD, double LAMH, double BH, double OM,
    double F, double L, double L1, double *L0, double *B0);
static void Moon (double RAD, double T, double T2, double LAM0, double R,
    double M, double *F, double *L1, double *OM, double *LAM, double *B,
    double *DR, double *LAMH, double *BH);
static void Sun (double RAD, double T, double T2, double *L, double *M,
    double *R, double *LAM0);

/* given a Julian date and a lunar location, find selenographic colongitude of
 * rising sun, lunar latitude of subsolar point, illuminated fraction, and alt
 * of sun at the given location. Any pointer may be 0 if not interested.
 * From Bruning and Talcott, October 1995 _Astronomy_, page 76.
 * N.B. lunar coordinates use +E, but selenograhic colongs are +W.
 */
void
moon_colong (
double jd,	/* jd */
double lt,	/* lat of location on moon, rads +N +E */
double lg,	/* long of location on moon, rads +N +E */
double *cp,	/* selenographic colongitude (-lng of rising sun), rads */
double *kp,	/* illuminated fraction of surface from Earth */
double *ap,	/* sun altitude at location, rads */
double *sp)	/* lunar latitude of subsolar point, rads */
{
	double RAD = .0174533;
	double T;
	double T2;
	double L, M, R, LAM0;
	double F, L1, OM, LAM, B, DR, LAMH, BH;
	double L0, B0;
	double TEMP;
	double C0;
	double PSI;
	double NUM, DEN;
	double I, K;
	double THETA, ETA;
	double H;

	T = (jd - 2451545)/36525.0;
	T2 = T * T;

	Sun(RAD, T, T2, &L, &M, &R, &LAM0);
	Moon(RAD, T, T2, LAM0, R, M, &F, &L1, &OM, &LAM, &B, &DR, &LAMH, &BH);
	Librations(RAD, LAMH, BH, OM, F, L, L1, &L0, &B0);
	if (sp)
	    *sp = B0;

	TEMP = L0 / 360;
	L0 = ((TEMP) - (int)(TEMP)) * 360;
	if (L0 < 0) L0 = L0 + 360;
	if (L0 <= 90) C0 = 90 - L0; else C0 = 450 - L0;
	if (cp) {
	    *cp = degrad(C0);
	    range (cp, 2*PI);	/* prefer 0..360 +W */
	}

	if (kp) {
	    TEMP = cos(B * RAD) * cos(LAM - LAM0 * RAD);
	    PSI = acos(TEMP);
	    NUM = R * sin(PSI);
	    DEN = DR - R * TEMP;
	    I = atan(NUM / DEN);
	    if (NUM * DEN < 0) I = I + 3.14159;
	    if (NUM < 0) I = I + 3.14159;
	    K = (1 + cos(I)) / 2;
	    *kp = K;
	}

	if (ap) {
	    THETA = lt;
	    ETA = lg;
	    C0 = C0 * RAD;
	    TEMP = sin(B0) * sin(THETA) + cos(B0) * cos(THETA) * sin(C0+ETA);
	    H = asin(TEMP);
	    *ap = H;
	}
}

static void
Librations (double RAD, double LAMH, double BH, double OM, double F,
double L, double L1, double *L0, double *B0)
{ 
	double I, PSI, W, NUM, DEN, A, TEMP;

	/* inclination of lunar equator */
	I = 1.54242 * RAD;

	/* nutation in longitude, in arcseconds */
	PSI = -17.2 * sin(OM) - 1.32 * sin(2 * L) - .23 * sin(2 * L1) +
							    .21 * sin(2 * OM);
	PSI = PSI * RAD / 3600;

	/* optical librations */
	W = (LAMH - PSI) - OM;
	NUM = sin(W) * cos(BH) * cos(I) - sin(BH) * sin(I);
	DEN = cos(W) * cos(BH);
	A = atan(NUM / DEN);
	if (NUM * DEN < 0) A = A + 3.14159;
	if (NUM < 0) A = A + 3.14159;
	*L0 = (A - F) / RAD;
	TEMP = -sin(W) * cos(BH) * sin(I) - sin(BH) * cos(I);
	*B0 = asin(TEMP);
}

static void
Moon (double RAD, double T, double T2, double LAM0, double R, double M,
double *F, double *L1, double *OM, double *LAM, double *B, double *DR,
double *LAMH, double *BH)
{
	double T3, M1, D2, SUMR, SUML, DIST;

	T3 = T * T2; 

	/* argument of the latitude of the Moon */
	*F = (93.2721 + 483202 * T - .003403 * T2 - T3 / 3526000) * RAD;

	/* mean longitude of the Moon */
	*L1 = (218.316 + 481268. * T) * RAD;
	
	/* longitude of the ascending node of Moon's mean orbit */
	*OM = (125.045 - 1934.14 * T + .002071 * T2 + T3 / 450000) * RAD;
	
	/* Moon's mean anomaly */
	M1 = (134.963 + 477199 * T + .008997 * T2 + T3 / 69700) * RAD;
	
	/* mean elongation of the Moon */
	D2 = (297.85 + 445267 * T - .00163 * T2 + T3 / 545900) * 2 * RAD;

	/* Lunar distance */
	SUMR = -20954 * cos(M1) - 3699 * cos(D2 - M1) - 2956 * cos(D2);
	*DR = 385000 + SUMR;
	
	/* geocentric latitude */
	*B = 5.128 * sin(*F) + .2806 * sin(M1 + *F) + .2777 * sin(M1 - *F) +
							.1732 * sin(D2 - *F);
	SUML = 6.289 * sin(M1) + 1.274 * sin(D2 - M1) + .6583 * sin(D2) +
		.2136 * sin(2 * M1) - .1851 * sin(M) - .1143 * sin(2 * *F);
	*LAM = *L1 + SUML * RAD;
	DIST = *DR / R;
	*LAMH = (LAM0 + 180 + DIST * cos(*B) * sin(LAM0 * RAD - *LAM) / RAD)
									* RAD;
	*BH = DIST * *B * RAD;
}

static void
Sun (double RAD, double T, double T2, double *L, double *M, double *R,
double *LAM0)
{
	double T3, C, V, E, THETA, OM;

	T3 = T2 * T;

	/* mean longitude of the Sun */
	*L = 280.466 + 36000.8 * T;

	/* mean anomaly of the Sun */
	*M = 357.529 + 35999 * T - .0001536 * T2 + T3 / 24490000;
	*M = *M * RAD;

	/* correction for Sun's elliptical orbit */
	C = (1.915 - .004817 * T - .000014 * T2) * sin(*M) +
		    (.01999 - .000101 * T) * sin(2 * *M) + .00029 * sin(3 * *M);

	/* true anomaly of the Sun */
	V = *M + C * RAD;

	/* eccentricity of Earth's orbit */
	E = .01671 - .00004204 * T - .0000001236 * T2;

	/* Sun-Earth distance */
	*R = .99972 / (1 + E * cos(V)) * 145980000;

	/* true geometric longitude of the Sun */
	THETA = *L + C;

	/* apparent longitude of the Sun */
	OM = 125.04 - 1934.1 * T;
	*LAM0 = THETA - .00569 - .00478 * sin(OM * RAD);
}

#ifdef TESTCOLONG

/* insure 0 <= *v < r.
 */
void
range (v, r)
double *v, r;
{
	*v -= r*floor(*v/r);
}

/* To be sure the program is functioning properly, try the test case
 * 2449992.5 (1 Oct 1995): the colongitude should be 3.69 degrees.
 */
int
main (int ac, char *av[])
{
	double jd, lt, lg;
	double c, k, a;

	if (ac != 2) {
	    fprintf (stderr, "%s: JD\n", av[0]);
	    abort();
	}

	jd = atof(av[1]);

	printf ("Latitude of lunar feature: ");
	fscanf (stdin, "%lf", &lt);
	lt = degrad(lt);
	printf ("Longitude:                 ");
	fscanf (stdin, "%lf", &lg);
	lg = degrad(lg);

	moon_colong (jd, lt, lg, &c, &k, &a);

	printf ("Selenographic colongitude is %g\n", raddeg(c));
	printf ("The illuminated fraction of the Moon is %g\n", k);
	printf ("Altitude of Sun above feature is %g\n", raddeg(a));

	return (0);
}

#endif

