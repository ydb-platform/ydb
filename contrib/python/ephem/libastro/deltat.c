/* DeltaT = Ephemeris Time - Universal Time
 *
 * Adapted 2011/4/14 from Stephen Moshier <moshier@world.std.com>,
 * cosmetic changes only.
 *
 * Compile as follows to create stand-alone test program:
 *   cc -DTEST_MAIN deltat.c libastro.a
 *
 * Tabulated values of deltaT, in hundredths of a second, are
 * from The Astronomical Almanac and current IERS reports.
 * A table of values for the pre-telescopic period was taken from
 * Morrison and Stephenson (2004).  The overall tabulated range is
 * -1000.0 through 2018.0.  Values at intermediate times are interpolated
 * from the tables.
 *
 * For dates earlier and later than the tabulated range, the program
 * calculates a polynomial extrapolation formula.
 *
 * Updated deltaT predictions can be obtained from this network archive,
 *    http://maia.usno.navy.mil
 * then appended to the dt[] table and update TABEND.
 *
 * Input is XEphem's MJD, output is ET-UT in seconds.
 *
 *
 * References:
 *
 * Morrison, L. V., and F. R. Stephenson, Historical values of the Earth's
 * clock error deltat T and the calculation of eclipses. Journal for the
 * History of Astronomy 35, 327-336 (2004)
 *
 * Stephenson, F. R., and L. V. Morrison, "Long-term changes
 * in the rotation of the Earth: 700 B.C. to A.D. 1980,"
 * Philosophical Transactions of the Royal Society of London
 * Series A 313, 47-70 (1984)
 *
 * Chapront-Touze, Michelle, and Jean Chapront, _Lunar Tables
 * and Programs from 4000 B.C. to A.D. 8000_, Willmann-Bell 1991
 *
 * Stephenson, F. R., and M. A. Houlden, _Atlas of Historical
 * Eclipse Maps_, Cambridge U. Press (1986)
 *
 */

#include <math.h>

#include "astro.h"

#define TABSTART 1620
#define TABEND 2018
#define TABSIZ (TABEND - TABSTART + 1)

/* Morrison and Stephenson (2004)
 * This table covers -1000 through 1700 in 100-year steps.
 * Values are in whole seconds.
 * Estimated standard error at -1000 is 640 seconds; at 1600, 20 seconds.
 * The first value in the table has been adjusted 28 sec for
 * continuity with their long-term quadratic extrapolation formula.
 * The last value in this table agrees with the AA table at 1700,
 * so there is no discontinuity at either endpoint.
 */
#define MS_SIZ 28
short m_s[MS_SIZ] = {
    /* -1000 to -100 */
    25428, 23700, 22000, 21000, 19040, 17190, 15530, 14080, 12790, 11640,

    /* 0 to 900 */
    10580, 9600, 8640, 7680, 6700, 5710, 4740, 3810, 2960, 2200,

    /* 1000 to 1700 */
    1570, 1090, 740, 490, 320, 200, 120, 9,
};


/* Entries prior to 1955 in the following table are from
 * the 2020 Astronomical Almanac and assume ndot = -26.0.
 * For dates prior to 1700, the above table is used instead of this one.
 */
short dt[TABSIZ] = {
    /* 1620.0 thru 1659.0 */
    12400, 11900, 11500, 11000, 10600, 10200, 9800, 9500, 9100, 8800,
    8500, 8200, 7900, 7700, 7400, 7200, 7000, 6700, 6500, 6300,
    6200, 6000, 5800, 5700, 5500, 5400, 5300, 5100, 5000, 4900,
    4800, 4700, 4600, 4500, 4400, 4300, 4200, 4100, 4000, 3800,

    /* 1660.0 thru 1699.0 */
    3700, 3600, 3500, 3400, 3300, 3200, 3100, 3000, 2800, 2700,
    2600, 2500, 2400, 2300, 2200, 2100, 2000, 1900, 1800, 1700,
    1600, 1500, 1400, 1400, 1300, 1200, 1200, 1100, 1100, 1000,
    1000, 1000, 900, 900, 900, 900, 900, 900, 900, 900,

    /* 1700.0 thru 1739.0 */
    900, 900, 900, 900, 900, 900, 900, 900, 1000, 1000,
    1000, 1000, 1000, 1000, 1000, 1000, 1000, 1100, 1100, 1100,
    1100, 1100, 1100, 1100, 1100, 1100, 1100, 1100, 1100, 1100,
    1100, 1100, 1100, 1100, 1200, 1200, 1200, 1200, 1200, 1200,

    /* 1740.0 thru 1779.0 */
    1200, 1200, 1200, 1200, 1300, 1300, 1300, 1300, 1300, 1300,
    1300, 1400, 1400, 1400, 1400, 1400, 1400, 1400, 1500, 1500,
    1500, 1500, 1500, 1500, 1500, 1600, 1600, 1600, 1600, 1600,
    1600, 1600, 1600, 1600, 1600, 1700, 1700, 1700, 1700, 1700,

    /* 1780.0 thru 1799.0 */
    1700, 1700, 1700, 1700, 1700, 1700, 1700, 1700, 1700, 1700,
    1700, 1700, 1600, 1600, 1600, 1600, 1500, 1500, 1400, 1400,

    /* 1800.0 thru 1819.0 */
    1370, 1340, 1310, 1290, 1270, 1260, 1250, 1250, 1250, 1250,
    1250, 1250, 1250, 1250, 1250, 1250, 1250, 1240, 1230, 1220,

    /* 1820.0 thru 1859.0 */
    1200, 1170, 1140, 1110, 1060, 1020, 960, 910, 860, 800,
    750, 700, 660, 630, 600, 580, 570, 560, 560, 560,
    570, 580, 590, 610, 620, 630, 650, 660, 680, 690,
    710, 720, 730, 740, 750, 760, 770, 770, 780, 780,

    /* 1860.0 thru 1899.0 */
    788, 782, 754, 697, 640, 602, 541, 410, 292, 182,
    161, 10, -102, -128, -269, -324, -364, -454, -471, -511,
    -540, -542, -520, -546, -546, -579, -563, -564, -580, -566,
    -587, -601, -619, -664, -644, -647, -609, -576, -466, -374,

    /* 1900.0 thru 1939.0 */
    -272, -154, -2, 124, 264, 386, 537, 614, 775, 913,
    1046, 1153, 1336, 1465, 1601, 1720, 1824, 1906, 2025, 2095,
    2116, 2225, 2241, 2303, 2349, 2362, 2386, 2449, 2434, 2408,
    2402, 2400, 2387, 2395, 2386, 2393, 2373, 2392, 2396, 2402,

    /* 1940.0 thru 1979.0 */
    2433, 2483, 2530, 2570, 2624, 2677, 2728, 2778, 2825, 2871,
    2915, 2957, 2997, 3036, 3072, 3107, 3135, 3168, 3218, 3268,
    3315, 3359, 3400, 3447, 3503, 3573, 3654, 3743, 3829, 3920,
    4018, 4117, 4223, 4337, 4449, 4548, 4646, 4752, 4853, 4959,

    /* 1980.0 thru 2018.0 */
    5054, 5138, 5217, 5296, 5379, 5434, 5487, 5532, 5582, 5630,
    5686, 5757, 5831, 5912, 5998, 6078, 6163, 6230, 6297, 6347,
    6383, 6409, 6430, 6447, 6457, 6469, 6485, 6515, 6546, 6578,
    6607, 6632, 6660, 6691, 6728, 6764, 6810, 6859, 6897,
};


/* Given MJD return DeltaT = ET - UT1 in seconds.  Describes the irregularities
 * of the Earth rotation rate in the ET time scale.
 */
double
deltat(double mj)
{
	static double ans, lastmj;
	double Y, p, B;
	int d[6];
	int i, iy, k;

	if (mj == lastmj)
	    return (ans);
	lastmj = mj;

	mjd_year (mj, &Y);

	if( Y > TABEND ) {
	    /* Extrapolate future values beyond the lookup table.  */
	    if (Y > (TABEND + 100.0)) {
		/* Morrison & Stephenson (2004) long-term curve fit.  */
		B = 0.01 * (Y - 1820.0);
		ans = 32.0 * B * B - 20.0;

	    } else {

		double a, b, c, d, m0, m1;

		/* Cubic interpolation between last tabulated value
		 * and long-term curve evaluated at 100 years later. 
		 */

		/* Last tabulated delta T value. */
		a = 0.01 * dt[TABSIZ-1];
		/* Approximate slope in past 10 years. */
		b = 0.001 * (dt[TABSIZ-1] - dt[TABSIZ - 11]);

		/* Long-term curve 100 years hence. */
		B = 0.01 * (TABEND + 100.0 - 1820.0);
		m0 = 32.0 * B*B - 20.0;
		/* Its slope. */
		m1 = 0.64 * B;

		/* Solve for remaining coefficients of an interpolation polynomial
		 * that agrees in value and slope at both ends of the 100-year
		 * interval.
		 */
		d = 2.0e-6 * (50.0 * (m1 + b) - m0 + a);
		c = 1.0e-4 * (m0 - a - 100.0 * b - 1.0e6 * d);

		/* Note, the polynomial coefficients do not depend on Y.
		 * A given tabulation and long-term formula
		 * determine the polynomial.
		 * Thus, for the IERS table ending at 2011.0, the coefficients are
		 * a = 66.32
		 * b = 0.223
		 * c = 0.03231376
		 * d = -0.0001607784
		 */

		/* Compute polynomial value at desired time. */
		p = Y - TABEND;
		ans = a + p * (b  + p * (c + p * d));
	    }

	    return (ans);
	}


	/* Use Morrison and Stephenson (2004) prior to the year 1700.  */
	if( Y < 1700.0 ) {
	    if (Y <= -1000.0) {
		/* Morrison and Stephenson long-term fit.  */
		B = 0.01 * (Y - 1820.0);
		ans = 32.0 * B * B - 20.0;

	    } else {

		/* Morrison and Stephenson recommend linear interpolation
		 * between tabulations.
		 */
		iy = Y;
		iy = (iy + 1000) / 100;  /* Integer index into the table. */
		B = -1000 + 100 * iy;    /* Starting year of tabulated interval.  */
		p = m_s[iy];
		ans = p + 0.01 * (Y - B) * (m_s[iy + 1] - p);
	    }

	    return (ans);
	}

	/* Besselian interpolation between tabulated values
	 * in the telescopic era.
	 * See AA page K11.
	 */

        /* Avoid Segmentation fault if year is NaN. */
        if (Y != Y)
            return 0.0;
	/* Index into the table.  */
	p = floor(Y);
	iy = (int) (p - TABSTART);
	/* Zeroth order estimate is value at start of year */
	ans = dt[iy];
	k = iy + 1;
	if( k >= TABSIZ )
	    goto done; /* No data, can't go on. */

	/* The fraction of tabulation interval */
	p = Y - p;

	/* First order interpolated value */
	ans += p*(dt[k] - dt[iy]);
	if( (iy-1 < 0) || (iy+2 >= TABSIZ) )
	    goto done; /* can't do second differences */

	/* Make table of first differences */
	k = iy - 2;
	for (i=0; i<5; i++) {
	    if( (k < 0) || (k+1 >= TABSIZ) )
		d[i] = 0;
	    else
		d[i] = dt[k+1] - dt[k];
	    k += 1;
	}

	/* Compute second differences */
	for( i=0; i<4; i++ )
	    d[i] = d[i+1] - d[i];
	B = 0.25*p*(p-1.0);
	ans += B*(d[1] + d[2]);
	if (iy+2 >= TABSIZ)
	    goto done;

	/* Compute third differences */
	for( i=0; i<3; i++ )
	    d[i] = d[i+1] - d[i];
	B = 2.0*B/3.0;
	ans += (p-0.5)*B*d[1];
	if ((iy-2 < 0) || (iy+3 > TABSIZ) )
	    goto done;

	/* Compute fourth differences */
	for( i=0; i<2; i++ )
	    d[i] = d[i+1] - d[i];
	B = 0.125*B*(p+1.0)*(p-2.0);
	ans += B*(d[0] + d[1]);

    done:

	ans *= 0.01;

#if 0 /* ndot = -26.0 assumed; no correction. */

	/* Astronomical Almanac table is corrected by adding the expression
	 *     -0.000091 (ndot + 26)(year-1955)^2  seconds
	 * to entries prior to 1955 (AA page K8), where ndot is the secular
	 * tidal term in the mean motion of the Moon.
	 *
	 * Entries after 1955 are referred to atomic time standards and
	 * are not affected by errors in Lunar or planetary theory.
	 */
	if( Y < 1955.0 )
		{
		B = (Y - 1955.0);
	#if 1
		ans += -0.000091 * (-25.8 + 26.0) * B * B;
	#else
		ans += -0.000091 * (-23.8946 + 26.0) * B * B;
	#endif
		}

#endif /* 0 */

	return( ans );
}


#ifdef TEST_MAIN

/* Exercise program.
 */
#include <stdio.h>
#include <stdlib.h>

int main(int ac, char *av[])
{
	double ans, mj, y = atof(av[1]);
	year_mjd (y, &mj);
	ans = deltat(mj);
	printf( "%.4lf\n", ans );
	return (0);
}
#endif
