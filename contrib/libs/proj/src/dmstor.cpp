/* Convert DMS string to radians */

#include <ctype.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>

#include "proj.h"
#include "proj_internal.h"

static double proj_strtod(char *nptr, char **endptr);

/* following should be sufficient for all but the ridiculous */
#define MAX_WORK 64
static const char *sym = "NnEeSsWw";
static const double vm[] = {DEG_TO_RAD, .0002908882086657216,
                            .0000048481368110953599};
/* byte sequence for Degree Sign U+00B0 in UTF-8. */
static constexpr char DEG_SIGN1 = '\xc2';
static constexpr char DEG_SIGN2 = '\xb0';

double dmstor(const char *is, char **rs) {
    return dmstor_ctx(pj_get_default_ctx(), is, rs);
}

double dmstor_ctx(PJ_CONTEXT *ctx, const char *is, char **rs) {
    int n, nl;
    char *s, work[MAX_WORK];
    const char *p;
    double v, tv;

    if (rs)
        *rs = (char *)is;
    /* copy string into work space */
    while (isspace(*is))
        ++is;
    n = MAX_WORK;
    s = work;
    p = (char *)is;

    /*
     * Copy characters into work until we hit a non-printable character or run
     * out of space in the buffer.  Make a special exception for the bytes of
     * the Degree Sign in UTF-8.
     *
     * It is possible that a really odd input (like lots of leading zeros)
     * could be truncated in copying into work.  But ...
     */
    while ((isgraph(static_cast<unsigned char>(*p)) || *p == DEG_SIGN1 ||
            *p == DEG_SIGN2) &&
           --n)
        *s++ = *p++;
    *s = '\0';
    int sign = *(s = work);
    if (sign == '+' || sign == '-')
        s++;
    else
        sign = '+';
    v = 0.;
    for (nl = 0; nl < 3; nl = n + 1) {
        if (!(isdigit(*s) || *s == '.'))
            break;
        if ((tv = proj_strtod(s, &s)) == HUGE_VAL)
            return tv;
        int adv = 1;

        if (*s == 'D' || *s == 'd' || *s == DEG_SIGN2) {
            /*
             * Accept \xb0 as a single-byte degree symbol. This byte is the
             * degree symbol in various single-byte encodings: multiple ISO
             * 8859 parts, several Windows code pages and others.
             */
            n = 0;
        } else if (*s == '\'') {
            n = 1;
        } else if (*s == '"') {
            n = 2;
        } else if (s[0] == DEG_SIGN1 && s[1] == DEG_SIGN2) {
            /* degree symbol in UTF-8 */
            n = 0;
            adv = 2;
        } else if (*s == 'r' || *s == 'R') {
            if (nl) {
                proj_context_errno_set(ctx,
                                       PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
                return HUGE_VAL;
            }
            ++s;
            v = tv;
            n = 4;
            continue;
        } else {
            v += tv * vm[nl];
            n = 4;
            continue;
        }

        if (n < nl) {
            proj_context_errno_set(ctx, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
            return HUGE_VAL;
        }
        v += tv * vm[n];
        s += adv;
    }
    /* postfix sign */
    if (*s && (p = strchr(sym, *s))) {
        sign = (p - sym) >= 4 ? '-' : '+';
        ++s;
    }
    if (sign == '-')
        v = -v;
    if (rs) /* return point of next char after valid string */
        *rs = (char *)is + (s - work);
    return v;
}

static double proj_strtod(char *nptr, char **endptr)

{
    char c, *cp = nptr;
    double result;

    /*
     * Scan for characters which cause problems with VC++ strtod()
     */
    while ((c = *cp) != '\0') {
        if (c == 'd' || c == 'D') {

            /*
             * Found one, so NUL it out, call strtod(),
             * then restore it and return
             */
            *cp = '\0';
            result = strtod(nptr, endptr);
            *cp = c;
            return result;
        }
        ++cp;
    }

    /* no offending characters, just handle normally */

    return pj_strtod(nptr, endptr);
}
