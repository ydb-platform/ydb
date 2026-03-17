/* nutation (in IAU (1980) expression) and abberation; stern
 * on an HP PA processor, this reproduces the Almanac nutation values
 * (given to 0.001") EXACTLY over 750 days (1995 and 1996)
 */
#include <stdio.h>
#include <math.h>

#include "astro.h"

#define NUT_SCALE	1e4
#define NUT_SERIES	106
#define NUT_MAXMUL	4
#define SECPERCIRC	(3600.*360.)

/* Delaunay arguments, in arc seconds; they differ slightly from ELP82B */
static double delaunay[5][4] = {
    {485866.733,  1717915922.633, 31.310,  0.064}, /* M', moon mean anom */
    {1287099.804, 129596581.224,  -0.577, -0.012}, /* M, sun mean anom */
    {335778.877,  1739527263.137, -13.257, 0.011}, /* F, moon arg lat */
    {1072261.307, 1602961601.328, -6.891,  0.019}, /* D, elong moon sun */
    {450160.280,  -6962890.539,   7.455,   0.008}, /* Om, moon l asc node */
};

/* multipliers for Delaunay arguments */
static short multarg[NUT_SERIES][5] = {
	/* bounds:  -2..3, -2..2, -2/0/2/4, -4..4, 0..2 */
    {0, 0, 0, 0, 1},
    {0, 0, 0, 0, 2},
    {-2, 0, 2, 0, 1},
    {2, 0, -2, 0, 0},
    {-2, 0, 2, 0, 2},
    {1, -1, 0, -1, 0},
    {0, -2, 2, -2, 1},
    {2, 0, -2, 0, 1},
    {0, 0, 2, -2, 2},
    {0, 1, 0, 0, 0},
    {0, 1, 2, -2, 2},
    {0, -1, 2, -2, 2},
    {0, 0, 2, -2, 1},
    {2, 0, 0, -2, 0},
    {0, 0, 2, -2, 0},
    {0, 2, 0, 0, 0},
    {0, 1, 0, 0, 1},
    {0, 2, 2, -2, 2},
    {0, -1, 0, 0, 1},
    {-2, 0, 0, 2, 1},
    {0, -1, 2, -2, 1},
    {2, 0, 0, -2, 1},
    {0, 1, 2, -2, 1},
    {1, 0, 0, -1, 0},
    {2, 1, 0, -2, 0},
    {0, 0, -2, 2, 1},
    {0, 1, -2, 2, 0},
    {0, 1, 0, 0, 2},
    {-1, 0, 0, 1, 1},
    {0, 1, 2, -2, 0},
    {0, 0, 2, 0, 2},
    {1, 0, 0, 0, 0},
    {0, 0, 2, 0, 1},
    {1, 0, 2, 0, 2},
    {1, 0, 0, -2, 0},
    {-1, 0, 2, 0, 2},
    {0, 0, 0, 2, 0},
    {1, 0, 0, 0, 1},
    {-1, 0, 0, 0, 1},
    {-1, 0, 2, 2, 2},
    {1, 0, 2, 0, 1},
    {0, 0, 2, 2, 2},
    {2, 0, 0, 0, 0},
    {1, 0, 2, -2, 2},
    {2, 0, 2, 0, 2},
    {0, 0, 2, 0, 0},
    {-1, 0, 2, 0, 1},
    {-1, 0, 0, 2, 1},
    {1, 0, 0, -2, 1},
    {-1, 0, 2, 2, 1},
    {1, 1, 0, -2, 0},
    {0, 1, 2, 0, 2},
    {0, -1, 2, 0, 2},
    {1, 0, 2, 2, 2},
    {1, 0, 0, 2, 0},
    {2, 0, 2, -2, 2},
    {0, 0, 0, 2, 1},
    {0, 0, 2, 2, 1},
    {1, 0, 2, -2, 1},
    {0, 0, 0, -2, 1},
    {1, -1, 0, 0, 0},
    {2, 0, 2, 0, 1},
    {0, 1, 0, -2, 0},
    {1, 0, -2, 0, 0},
    {0, 0, 0, 1, 0},
    {1, 1, 0, 0, 0},
    {1, 0, 2, 0, 0},
    {1, -1, 2, 0, 2},
    {-1, -1, 2, 2, 2},
    {-2, 0, 0, 0, 1},
    {3, 0, 2, 0, 2},
    {0, -1, 2, 2, 2},
    {1, 1, 2, 0, 2},
    {-1, 0, 2, -2, 1},
    {2, 0, 0, 0, 1},
    {1, 0, 0, 0, 2},
    {3, 0, 0, 0, 0},
    {0, 0, 2, 1, 2},
    {-1, 0, 0, 0, 2},
    {1, 0, 0, -4, 0},
    {-2, 0, 2, 2, 2},
    {-1, 0, 2, 4, 2},
    {2, 0, 0, -4, 0},
    {1, 1, 2, -2, 2},
    {1, 0, 2, 2, 1},
    {-2, 0, 2, 4, 2},
    {-1, 0, 4, 0, 2},
    {1, -1, 0, -2, 0},
    {2, 0, 2, -2, 1},
    {2, 0, 2, 2, 2},
    {1, 0, 0, 2, 1},
    {0, 0, 4, -2, 2},
    {3, 0, 2, -2, 2},
    {1, 0, 2, -2, 0},
    {0, 1, 2, 0, 1},
    {-1, -1, 0, 2, 1},
    {0, 0, -2, 0, 1},
    {0, 0, 2, -1, 2},
    {0, 1, 0, 2, 0},
    {1, 0, -2, -2, 0},
    {0, -1, 2, 0, 1},
    {1, 1, 0, -2, 1},
    {1, 0, -2, 2, 0},
    {2, 0, 0, 2, 0},
    {0, 0, 2, 4, 2},
    {0, 1, 0, 1, 0}
};

/* amplitudes which  have secular terms; in 1/NUT_SCALE arc seconds
 * {index, constant dPSI, T/10 in dPSI, constant in dEPS, T/10 in dEPS}
 */
static long ampsecul[][5] = {
    {0  ,-171996 ,-1742 ,92025 ,89},
    {1  ,2062    ,2     ,-895  ,5},
    {8  ,-13187  ,-16   ,5736  ,-31},
    {9  ,1426    ,-34   ,54    ,-1},
    {10 ,-517    ,12    ,224   ,-6},
    {11 ,217     ,-5    ,-95   ,3},
    {12 ,129     ,1     ,-70   ,0},
    {15 ,17      ,-1    ,0     ,0},
    {17 ,-16     ,1     ,7     ,0},
    {30 ,-2274   ,-2    ,977   ,-5},
    {31 ,712     ,1     ,-7    ,0},
    {32 ,-386    ,-4    ,200   ,0},
    {33 ,-301    ,0     ,129   ,-1},
    {37 ,63      ,1     ,-33   ,0},
    {38 ,-58     ,-1    ,32    ,0},
    /* termination */  { -1, }
};

/* amplitudes which only have constant terms; same unit as above
 * {dPSI, dEPS}
 * indexes which are already in ampsecul[][] are zeroed
 */
static short ampconst[NUT_SERIES][2] = {
    {0,0},
    {0,0},
    {46,-24},
    {11,0},
    {-3,1},
    {-3,0},
    {-2,1},
    {1,0},
    {0,0},
    {0,0},
    {0,0},
    {0,0},
    {0,0},
    {48,1},
    {-22,0},
    {0,0},
    {-15,9},
    {0,0},
    {-12,6},
    {-6,3},
    {-5,3},
    {4,-2},
    {4,-2},
    {-4,0},
    {1,0},
    {1,0},
    {-1,0},
    {1,0},
    {1,0},
    {-1,0},
    {0,0},
    {0,0},
    {0,0},
    {0,0},
    {-158,-1},
    {123,-53},
    {63,-2},
    {0,0},
    {0,0},
    {-59,26},
    {-51,27},
    {-38,16},
    {29,-1},
    {29,-12},
    {-31,13},
    {26,-1},
    {21,-10},
    {16,-8},
    {-13,7},
    {-10,5},
    {-7,0},
    {7,-3},
    {-7,3},
    {-8,3},
    {6,0},
    {6,-3},
    {-6,3},
    {-7,3},
    {6,-3},
    {-5,3},
    {5,0},
    {-5,3},
    {-4,0},
    {4,0},
    {-4,0},
    {-3,0},
    {3,0},
    {-3,1},
    {-3,1},
    {-2,1},
    {-3,1},
    {-3,1},
    {2,-1},
    {-2,1},
    {2,-1},
    {-2,1},
    {2,0},
    {2,-1},
    {1,-1},
    {-1,0},
    {1,-1},
    {-2,1},
    {-1,0},
    {1,-1},
    {-1,1},
    {-1,1},
    {1,0},
    {1,0},
    {1,-1},
    {-1,0},
    {-1,0},
    {1,0},
    {1,0},
    {-1,0},
    {1,0},
    {1,0},
    {-1,0},
    {-1,0},
    {-1,0},
    {-1,0},
    {-1,0},
    {-1,0},
    {-1,0},
    {1,0},
    {-1,0},
    {1,0}
};

/* given the modified JD, mj, find the nutation in obliquity, *deps, and
 * the nutation in longitude, *dpsi, each in radians.
 */
void
nutation (
double mj,
double *deps,	/* on input:  precision parameter in arc seconds */
double *dpsi)
{
	static double lastmj = -10000, lastdeps, lastdpsi;
	double T, T2, T3, T10;			/* jul cent since J2000 */
	double prec;				/* series precis in arc sec */
	int i, isecul;				/* index in term table */
	static double delcache[5][2*NUT_MAXMUL+1];
			/* cache for multiples of delaunay args
			 * [M',M,F,D,Om][-min*x, .. , 0, .., max*x]
			 * make static to have unfilled fields cleared on init
			 */

	if (mj == lastmj) {
	    *deps = lastdeps;
	    *dpsi = lastdpsi;
	    return;
	}

	prec = 0.0;

#if 0	/* this is if deps should contain a precision value */
	prec =* deps;
	if (prec < 0.0 || prec > 1.0)	/* accept only sane value */
		prec = 1.0;
#endif

	/* augment for abundance of small terms */
	prec *= NUT_SCALE/10;

	T = (mj - J2000)/36525.;
	T2 = T * T;
	T3 = T2 * T;
	T10 = T/10.;

	/* calculate delaunay args and place in cache */
	for (i = 0; i < 5; ++i) {
	    double x;
	    short j;

	    x = delaunay[i][0] +
		delaunay[i][1] * T +
		delaunay[i][2] * T2 +
		delaunay[i][3] * T3;

	    /* convert to radians */
	    x /= SECPERCIRC;
	    x -= floor(x);
	    x *= 2.*PI;

	    /* fill cache table */
	    for (j = 0; j <= 2*NUT_MAXMUL; ++j)
		delcache[i][j] = (j - NUT_MAXMUL) * x;
	}

	/* find dpsi and deps */
	lastdpsi = lastdeps = 0.;
	for (i = isecul = 0; i < NUT_SERIES ; ++i) {
	    double arg = 0., ampsin, ampcos;
	    short j;

	    if (ampconst[i][0] || ampconst[i][1]) {
		/* take non-secular terms from simple array */
		ampsin = ampconst[i][0];
		ampcos = ampconst[i][1];
	    } else {
		/* secular terms from different array */
		ampsin = ampsecul[isecul][1] + ampsecul[isecul][2] * T10;
		ampcos = ampsecul[isecul][3] + ampsecul[isecul][4] * T10;
		++isecul;
	    }

	    for (j = 0; j < 5; ++j)
		arg += delcache[j][NUT_MAXMUL + multarg[i][j]];

	    if (fabs(ampsin) >= prec)
		lastdpsi += ampsin * sin(arg);

	    if (fabs(ampcos) >= prec)
		lastdeps += ampcos * cos(arg);

	}

	/* convert to radians.
	 */
	lastdpsi = degrad(lastdpsi/3600./NUT_SCALE);
	lastdeps = degrad(lastdeps/3600./NUT_SCALE);

	lastmj = mj;
	*deps = lastdeps;
	*dpsi = lastdpsi;
}

/* given the modified JD, mj, correct, IN PLACE, the right ascension *ra
 * and declination *dec (both in radians) for nutation.
 */
void
nut_eq (double mj, double *ra, double *dec)
{
	static double lastmj = -10000;
	static double a[3][3];		/* rotation matrix */
	double xold, yold, zold, x, y, z;

	if (mj != lastmj) {
	    double epsilon, dpsi, deps;
	    double se, ce, sp, cp, sede, cede;

	    obliquity(mj, &epsilon);
	    nutation(mj, &deps, &dpsi);

	    /* the rotation matrix a applies the nutation correction to
	     * a vector of equatoreal coordinates Xeq to Xeq' by 3 subsequent
	     * rotations:  R1 - from equatoreal to ecliptic system by
	     * rotation of angle epsilon about x, R2 - rotate ecliptic
	     * system by -dpsi about its z, R3 - from ecliptic to equatoreal
	     * by rotation of angle -(epsilon + deps)
	     *
	     *	Xeq' = A * Xeq = R3 * R2 * R1 * Xeq
	     * 
	     *		[ 1       0          0    ]
	     * R1 =	[ 0   cos(eps)   sin(eps) ]
	     *		[ 0  - sin(eps)  cos(eps) ]
	     * 
	     *		[ cos(dpsi)  - sin(dpsi)  0 ]
	     * R2 =	[ sin(dpsi)   cos(dpsi)   0 ]
	     *		[      0           0      1 ]
	     * 
	     *		[ 1         0                 0         ]
	     * R3 =	[ 0  cos(eps + deps)  - sin(eps + deps) ]
	     *		[ 0  sin(eps + deps)   cos(eps + deps)  ]
	     * 
	     * for efficiency, here is a explicitely:
	     */
	    
	    se = sin(epsilon);
	    ce = cos(epsilon);
	    sp = sin(dpsi);
	    cp = cos(dpsi);
	    sede = sin(epsilon + deps);
	    cede = cos(epsilon + deps);

	    a[0][0] = cp;
	    a[0][1] = -sp*ce;
	    a[0][2] = -sp*se;

	    a[1][0] = cede*sp;
	    a[1][1] = cede*cp*ce+sede*se;
	    a[1][2] = cede*cp*se-sede*ce;

	    a[2][0] = sede*sp;
	    a[2][1] = sede*cp*ce-cede*se;
	    a[2][2] = sede*cp*se+cede*ce;

	    lastmj = mj;
	}

	sphcart(*ra, *dec, 1.0, &xold, &yold, &zold);
	x = a[0][0] * xold + a[0][1] * yold + a[0][2] * zold;
	y = a[1][0] * xold + a[1][1] * yold + a[1][2] * zold;
	z = a[2][0] * xold + a[2][1] * yold + a[2][2] * zold;
	cartsph(x, y, z, ra, dec, &zold);	/* radius should be 1.0 */
	if (*ra < 0.) *ra += 2.*PI;		/* make positive for display */
}

