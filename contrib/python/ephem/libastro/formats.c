#include "astro.h"
#include "preferences.h"

#include <stdio.h>
#include <math.h>
#include <ctype.h>
#include <stdlib.h>
#include <string.h>

double ascii_strtod(const char *s00, char **se);  /* for PyEphem */

/* sprint the variable a in sexagesimal format into out[].
 * w is the number of spaces for the whole part.
 * fracbase is the number of pieces a whole is to broken into; valid options:
 *	360000:	<w>:mm:ss.ss
 *	36000:	<w>:mm:ss.s
 *	3600:	<w>:mm:ss
 *	600:	<w>:mm.m
 *	60:	<w>:mm
 * return number of characters written to out, not counting final '\0'.
 */
int
fs_sexa (char *out, double a, int w, int fracbase)
{
	char *out0 = out;
	unsigned long n;
	int d;
	int f;
	int m;
	int s;
	int isneg;

	/* save whether it's negative but do all the rest with a positive */
	isneg = (a < 0);
	if (isneg)
	    a = -a;

	/* convert to an integral number of whole portions */
	n = (unsigned long)(a * fracbase + 0.5);
	d = n/fracbase;
	f = n%fracbase;

	/* form the whole part; "negative 0" is a special case */
	if (isneg && d == 0)
	    out += sprintf (out, "%*s-0", w-2, "");
	else
	    out += sprintf (out, "%*d", w, isneg ? -d : d);

	/* do the rest */
	switch (fracbase) {
	case 60:	/* dd:mm */
	    m = f/(fracbase/60);
	    out += sprintf (out, ":%02d", m);
	    break;
	case 600:	/* dd:mm.m */
	    out += sprintf (out, ":%02d.%1d", f/10, f%10);
	    break;
	case 3600:	/* dd:mm:ss */
	    m = f/(fracbase/60);
	    s = f%(fracbase/60);
	    out += sprintf (out, ":%02d:%02d", m, s);
	    break;
	case 36000:	/* dd:mm:ss.s*/
	    m = f/(fracbase/60);
	    s = f%(fracbase/60);
	    out += sprintf (out, ":%02d:%02d.%1d", m, s/10, s%10);
	    break;
	case 360000:	/* dd:mm:ss.ss */
	    m = f/(fracbase/60);
	    s = f%(fracbase/60);
	    out += sprintf (out, ":%02d:%02d.%02d", m, s/100, s%100);
	    break;
	default:
	    printf ("fs_sexa: unknown fracbase: %d\n", fracbase);
	    abort();
	}

	return (out - out0);
}

/* put the given modified Julian date, jd, in out[] according to the given
 * preference format.
 * return number of characters written to out, not counting final '\0'.
 */
int
fs_date (char out[], int format, double jd)
{
	char *out0 = out;
	int m, y;
	double d;

	mjd_cal (jd, &m, &d, &y);
	/* beware of %g rounding day up */
	if ((d < 1.0 && d - floor(d) >= .9999995)
				    || (d < 10.0 && d - floor(d) >= .999995)
				    || (d >= 10.0 && d - floor(d) >= .99995))
	    mjd_cal (mjd_day(jd+0.5), &m, &d, &y);

	switch (format) {
	case PREF_YMD:
	    out += sprintf (out, "%4d/%02d/%02.6g", y, m, d);
	    break;
	case PREF_DMY:
	    out += sprintf (out, "%2.6g/%02d/%-4d", d, m, y);
	    break;
	case PREF_MDY:
	    out += sprintf (out, "%2d/%02.6g/%-4d", m, d, y);
	    break;
	default:
	    printf ("fs_date: bad date pref: %d\n", format);
	    abort();
	}

	return (out - out0);
}


/* convert sexagesimal string str A:B:C to double.
 *   Any missing A, B or C will be assumed 0.
 *   optional - and + can be anywhere.
 * return 0 if ok, -1 if can't find a thing or A, B or C are invalid numbers.
 */
int
f_scansexa (
const char *str0,	/* input string */
double *dp)		/* cracked value, if return 0 */
{
	double a, b, c;
	char str[256];
	char *neg, *s, *end;
	int isneg, status;

	/* copy str0 so we can play with it */
	strncpy (str, str0, sizeof(str)-1);
	str[sizeof(str)-1] = '\0';

	/* note first negative (but not fooled by neg exponent) */
	isneg = 0;
	neg = strchr(str, '-');
	if (neg && (neg == str || (neg[-1] != 'E' && neg[-1] != 'e'))) {
	    *neg = ' ';
	    isneg = 1;
	}

        /* These three calls replace an old, locale-sensitive sscanf.
           Note that, per the semantics of the strtod call, if we run
           out of valid numbers to parse, then the last few values will
           just get zero. */
        status = 0;
        s = str;
        a = ascii_strtod(s, &end);
        if (end == s) { /* since a will be -1 */
             a = 0.0;
             /* don't fail if A is an empty string */
             if ((*end != ':') && (*end != '\0')) status = -1;
        }
        s = end;
        if (*s == ':') s++;
        b = ascii_strtod(s, &end);
        if (end == s) { /* since b will be -1 */
             b = 0.0;
             /* don't fail if B is an empty string */
             if ((*end != ':') && (*end != '\0')) status = -1;
        }
        s = end;
        if (*s == ':') s++;
        c = ascii_strtod(s, &end);
        if (end == s) { /* since c will be -1 */
             c = 0.0;
             /* don't fail if C is an empty string */
             if ((*end != ':') && (*end != '\0')) status = -1;
        }

	/* back to one value, restoring neg */
	*dp = a + b/60.0 + c/3600.0;
	if (isneg)
	    *dp *= -1;
	return status;
}

/* crack a floating date string, bp, of the form X/Y/Z determined by the
 *   PREF_DATE_FORMAT preference into its components. allow the day to be a
 *   floating point number,
 * the slashes may also be spaces or colons.
 * a lone component with a decimal point is considered a year.
 * Brandon Rhodes: 2011-11-24 supplemented this to allow dash separators.
 */
void
f_sscandate (
char *bp,
int pref,       /* one of PREF_X for PREF_DATE_FORMAT */
int *m,
double *d,
int *y)
{
        double X,Y,Z; /* the three components */
        char *s, *end;
	int n;

	X = Y = Z = 0.0;

        /* This replaces an old, locale-sensitive sscanf(). */
        X = ascii_strtod(bp, &end);
        if (bp == end) {
             n = 0;
             X = 0.0; /* X will be -1 */
        } else {
             s = end;
             if (*s == '-' || *s == '/' || *s == ':') s++;
             Y = ascii_strtod(s, &end);
             if (s == end) {
                  n = 1;
                  Y = 0.0; /* Y will be -1 */
             } else {
                  s = end;
                  if (*s == '-' || *s == '/' || *s == ':') s++;
                  Z = ascii_strtod(s, &end);
                  if (s == end) {
                       n = 2;
                       Z = 0.0; /* Z will be -1 */
                  } else {
                       n = 3;
                  }
             }
        }
        

	if (n == 1 && (strchr(bp, '.')
			|| (pref == PREF_MDY && (X < 1 || X > 12))
			|| (pref == PREF_DMY && (X < 1 || X > 31)))) {
	    double Mjd;
	    year_mjd (X, &Mjd);
	    mjd_cal (Mjd, m, d, y);
	} else {
	    switch (pref) {
	    case PREF_MDY:
		if (n > 0 && X != 0)
		    *m = (int)X;
		if (n > 1 && Y != 0)
		    *d = Y;
		if (n > 2 && Z != 0)
		    *y = (int)Z;
		break;
	    case PREF_YMD:
		if (n > 0 && X != 0)
		    *y = (int)X;
		if (n > 1 && Y != 0)
		    *m = (int)Y;
		if (n > 2 && Z != 0)
		    *d = Z;
		break;
	    case PREF_DMY:
		if (n > 0 && X != 0)
		    *d = X;
		if (n > 1 && Y != 0)
		    *m = (int)Y;
		if (n > 2 && Z != 0)
		    *y = (int)Z;
		break;
	    }
	}
}

