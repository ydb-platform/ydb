/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2013 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2014 Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/*
 * Buffer safe printf functions for portability to archaic platforms.
 */

#include "opal_config.h"

#include "opal/util/printf.h"
#include "opal/util/output.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>


/*
 * Make a good guess about how long a printf-style varargs formatted
 * string will be once all the % escapes are filled in.  We don't
 * handle every % escape here, but we handle enough, and then add a
 * fudge factor in at the end.
 */
static int guess_strlen(const char *fmt, va_list ap)
{
#if HAVE_VSNPRINTF
    char dummy[1];

    /* vsnprintf() returns the number of bytes that would have been
       copied if the provided buffer were infinite. */
    return 1 + vsnprintf(dummy, sizeof(dummy), fmt, ap);
#else
    char *sarg, carg;
    double darg;
    float farg;
    size_t i;
    int iarg;
    int len;
    long larg;

    /* Start off with a fudge factor of 128 to handle the % escapes that
       we aren't calculating here */

    len = (int)strlen(fmt) + 128;
    for (i = 0; i < strlen(fmt); ++i) {
        if ('%' == fmt[i] && i + 1 < strlen(fmt)
            && '%' != fmt[i + 1]) {
            ++i;
            switch (fmt[i]) {
            case 'c':
                carg = va_arg(ap, int);
                len += 1;  /* let's suppose it's a printable char */
                (void)carg;  /* prevent compiler from complaining about set but not used variables */
                break;
            case 's':
                sarg = va_arg(ap, char *);

                /* If there's an arg, get the strlen, otherwise we'll
                 * use (null) */

                if (NULL != sarg) {
                    len += (int)strlen(sarg);
                } else {
#if OPAL_ENABLE_DEBUG
                    opal_output(0, "OPAL DEBUG WARNING: Got a NULL argument to opal_vasprintf!\n");
#endif
                    len += 5;
                }
                break;

            case 'd':
            case 'i':
                iarg = va_arg(ap, int);
                /* Alloc for minus sign */
                if (iarg < 0)
                    ++len;
                /* Now get the log10 */
                do {
                    ++len;
                    iarg /= 10;
                } while (0 != iarg);
                break;

            case 'x':
            case 'X':
                iarg = va_arg(ap, int);
                /* Now get the log16 */
                do {
                    ++len;
                    iarg /= 16;
                } while (0 != iarg);
                break;

            case 'f':
                farg = (float)va_arg(ap, int);
                /* Alloc for minus sign */
                if (farg < 0) {
                    ++len;
                    farg = -farg;
                }
                /* Alloc for 3 decimal places + '.' */
                len += 4;
                /* Now get the log10 */
                do {
                    ++len;
                    farg /= 10.0;
                } while (0 != farg);
                break;

            case 'g':
                darg = va_arg(ap, int);
                /* Alloc for minus sign */
                if (darg < 0) {
                    ++len;
                    darg = -darg;
                }
                /* Alloc for 3 decimal places + '.' */
                len += 4;
                /* Now get the log10 */
                do {
                    ++len;
                    darg /= 10.0;
                } while (0 != darg);
                break;

            case 'l':
                /* Get %ld %lx %lX %lf */
                if (i + 1 < strlen(fmt)) {
                    ++i;
                    switch (fmt[i]) {
                    case 'x':
                    case 'X':
                        larg = va_arg(ap, int);
                        /* Now get the log16 */
                        do {
                            ++len;
                            larg /= 16;
                        } while (0 != larg);
                        break;

                    case 'f':
                        darg = va_arg(ap, int);
                        /* Alloc for minus sign */
                        if (darg < 0) {
                            ++len;
                            darg = -darg;
                        }
                        /* Alloc for 3 decimal places + '.' */
                        len += 4;
                        /* Now get the log10 */
                        do {
                            ++len;
                            darg /= 10.0;
                        } while (0 != darg);
                        break;

                    case 'd':
                    default:
                        larg = va_arg(ap, int);
                        /* Now get the log10 */
                        do {
                            ++len;
                            larg /= 10;
                        } while (0 != larg);
                        break;
                    }
                }

            default:
                break;
            }
        }
    }

    return len;
#endif
}


int opal_asprintf(char **ptr, const char *fmt, ...)
{
    int length;
    va_list ap;

    va_start(ap, fmt);
    length = opal_vasprintf(ptr, fmt, ap);
    va_end(ap);

    return length;
}


int opal_vasprintf(char **ptr, const char *fmt, va_list ap)
{
    int length;
    va_list ap2;

    /* va_list might have pointer to internal state and using
       it twice is a bad idea.  So make a copy for the second
       use.  Copy order taken from Autoconf docs. */
#if OPAL_HAVE_VA_COPY
    va_copy(ap2, ap);
#elif OPAL_HAVE_UNDERSCORE_VA_COPY
    __va_copy(ap2, ap);
#else
    memcpy (&ap2, &ap, sizeof(va_list));
#endif

    /* guess the size */
    length = guess_strlen(fmt, ap);

    /* allocate a buffer */
    *ptr = (char *) malloc((size_t) length + 1);
    if (NULL == *ptr) {
        errno = ENOMEM;
        va_end(ap2);
        return -1;
    }

    /* fill the buffer */
    length = vsprintf(*ptr, fmt, ap2);
#if OPAL_HAVE_VA_COPY || OPAL_HAVE_UNDERSCORE_VA_COPY
    va_end(ap2);
#endif  /* OPAL_HAVE_VA_COPY || OPAL_HAVE_UNDERSCORE_VA_COPY */

    /* realloc */
    *ptr = (char*) realloc(*ptr, (size_t) length + 1);
    if (NULL == *ptr) {
        errno = ENOMEM;
        return -1;
    }

    return length;
}


int opal_snprintf(char *str, size_t size, const char *fmt, ...)
{
    int length;
    va_list ap;

    va_start(ap, fmt);
    length = opal_vsnprintf(str, size, fmt, ap);
    va_end(ap);

    return length;
}


int opal_vsnprintf(char *str, size_t size, const char *fmt, va_list ap)
{
    int length;
    char *buf;

    length = opal_vasprintf(&buf, fmt, ap);
    if (length < 0) {
        return length;
    }

    /* return the length when given a null buffer (C99) */
    if (str) {
        if ((size_t) length < size) {
            strcpy(str, buf);
        } else {
            memcpy(str, buf, size - 1);
            str[size] = '\0';
        }
    }

    /* free allocated buffer */
    free(buf);

    return length;
}


#ifdef TEST

int main(int argc, char *argv[])
{
    char a[10];
    char b[173];
    char *s;
    int length;

    puts("test for NULL buffer in snprintf:");
    length = opal_snprintf(NULL, 0, "this is a string %d", 1004);
    printf("length = %d\n", length);

    puts("test of snprintf to an undersize buffer:");
    length = opal_snprintf(a, sizeof(a), "this is a string %d", 1004);
    printf("string = %s\n", a);
    printf("length = %d\n", length);
    printf("strlen = %d\n", (int) strlen(a));

    puts("test of snprintf to an oversize buffer:");
    length = opal_snprintf(b, sizeof(b), "this is a string %d", 1004);
    printf("string = %s\n", b);
    printf("length = %d\n", length);
    printf("strlen = %d\n", (int) strlen(b));

    puts("test of asprintf:");
    length = opal_asprintf(&s, "this is a string %d", 1004);
    printf("string = %s\n", s);
    printf("length = %d\n", length);
    printf("strlen = %d\n", (int) strlen(s));

    free(s);

    return 0;
}

#endif
