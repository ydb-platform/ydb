/* Convert radian argument to DMS ascii format */

#include <math.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>

#include "proj.h"
#include "proj_internal.h"

/*
** RES is fractional second figures
** RES60 = 60 * RES
** CONV = 180 * 3600 * RES / PI (radians to RES seconds)
*/
static double RES = 1000., RES60 = 60000., CONV = 206264806.24709635516;
static char format[50] = "%dd%d'%.3f\"%c";
static int dolong = 0;
void set_rtodms(int fract, int con_w) {
    int i;

    if (fract >= 0 && fract < 9) {
        RES = 1.;
        /* following not very elegant, but used infrequently */
        for (i = 0; i < fract; ++i)
            RES *= 10.;
        RES60 = RES * 60.;
        CONV = 180. * 3600. * RES / M_PI;
        if (!con_w)
            (void)snprintf(format, sizeof(format), "%%dd%%d'%%.%df\"%%c",
                           fract);
        else
            (void)snprintf(format, sizeof(format), "%%dd%%02d'%%0%d.%df\"%%c",
                           fract + 2 + (fract ? 1 : 0), fract);
        dolong = con_w;
    }
}
char *rtodms(char *s, size_t sizeof_s, double r, int pos, int neg) {
    int deg, min, sign;
    char *ss = s;
    double sec;
    size_t sizeof_ss = sizeof_s;

    if (r < 0) {
        r = -r;
        if (!pos) {
            if (sizeof_s == 1) {
                *s = 0;
                return s;
            }
            sizeof_ss--;
            *ss++ = '-';
            sign = 0;
        } else
            sign = neg;
    } else
        sign = pos;
    r = floor(r * CONV + .5);
    sec = fmod(r / RES, 60.);
    r = floor(r / RES60);
    min = (int)fmod(r, 60.);
    r = floor(r / 60.);
    deg = (int)r;

    if (dolong)
        (void)snprintf(ss, sizeof_ss, format, deg, min, sec, sign);
    else if (sec != 0.0) {
        char *p, *q;
        /* double prime + pos/neg suffix (if included) + NUL */
        size_t suffix_len = sign ? 3 : 2;

        (void)snprintf(ss, sizeof_ss, format, deg, min, sec, sign);
        /* Replace potential decimal comma by decimal point for non C locale */
        for (p = ss; *p != '\0'; ++p) {
            if (*p == ',') {
                *p = '.';
                break;
            }
        }
        if (suffix_len > strlen(ss))
            return s;
        for (q = p = ss + strlen(ss) - suffix_len; *p == '0'; --p)
            ;
        if (*p != '.')
            ++p;
        if (++q != p)
            (void)memmove(p, q, suffix_len);
    } else if (min)
        (void)snprintf(ss, sizeof_ss, "%dd%d'%c", deg, min, sign);
    else
        (void)snprintf(ss, sizeof_ss, "%dd%c", deg, sign);
    return s;
}
