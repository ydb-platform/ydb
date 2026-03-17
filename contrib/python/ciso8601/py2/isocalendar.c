/* This file was originally taken from cPython's code base
 * (`Modules/_datetimemodule.c`) at commit
 * 27d8dc2c9d3de886a884f79f0621d4586c0e0f7a
 *
 * Below is a copy of the Python 3.11 code license
 * (from https://docs.python.org/3/license.html):
 *
 * PSF LICENSE AGREEMENT FOR PYTHON 3.11.0
 *
 * 1. This LICENSE AGREEMENT is between the Python Software Foundation ("PSF"),
 *    and the Individual or Organization ("Licensee") accessing and otherwise
 *    using Python 3.11.0 software in source or binary form and its associated
 *    documentation.
 *
 * 2. Subject to the terms and conditions of this License Agreement, PSF hereby
 *    grants Licensee a nonexclusive, royalty-free, world-wide license to
 *    reproduce, analyze, test, perform and/or display publicly, prepare
 *    derivative works, distribute, and otherwise use Python 3.11.0 alone or in
 *    any derivative version, provided, however, that PSF's License Agreement
 *    and PSF's notice of copyright, i.e., "Copyright © 2001-2022 Python
 *    Software Foundation; All Rights Reserved" are retained in Python 3.11.0
 *    alone or in any derivative version prepared by Licensee.
 *
 * 3. In the event Licensee prepares a derivative work that is based on or
 *    incorporates Python 3.11.0 or any part thereof, and wants to make the
 *    derivative work available to others as provided herein, then Licensee
 *    hereby agrees to include in any such work a brief summary of the changes
 *    made to Python 3.11.0.
 *
 * 4. PSF is making Python 3.11.0 available to Licensee on an "AS IS" basis.
 *    PSF MAKES NO REPRESENTATIONS OR WARRANTIES, EXPRESS OR IMPLIED.  BY WAY
 *    OF EXAMPLE, BUT NOT LIMITATION, PSF MAKES NO AND DISCLAIMS ANY
 *    REPRESENTATION OR WARRANTY OF MERCHANTABILITY OR FITNESS FOR ANY
 *    PARTICULAR PURPOSE OR THAT THE USE OF PYTHON 3.11.0 WILL NOT INFRINGE ANY
 *    THIRD PARTY RIGHTS.
 *
 * 5. PSF SHALL NOT BE LIABLE TO LICENSEE OR ANY OTHER USERS OF PYTHON 3.11.0
 *    FOR ANY INCIDENTAL, SPECIAL, OR CONSEQUENTIAL DAMAGES OR LOSS AS A RESULT
 *    OF MODIFYING, DISTRIBUTING, OR OTHERWISE USING PYTHON 3.11.0, OR ANY
 *    DERIVATIVE THEREOF, EVEN IF ADVISED OF THE POSSIBILITY THEREOF.
 *
 * 6. This License Agreement will automatically terminate upon a material
 *    breach of its terms and conditions.
 *
 * 7. Nothing in this License Agreement shall be deemed to create any
 *    relationship of agency, partnership, or joint venture between PSF and
 *    Licensee.  This License Agreement does not grant permission to use PSF
 *    trademarks or trade name in a trademark sense to endorse or promote
 *    products or services of Licensee, or any third party.
 *
 * 8. By copying, installing or otherwise using Python 3.11.0, Licensee agrees
 *    to be bound by the terms and conditions of this License Agreement.
 */

#include "isocalendar.h"

#include "Python.h"

/* ---------------------------------------------------------------------------
 * General calendrical helper functions
 */

/* For each month ordinal in 1..12, the number of days in that month,
 * and the number of days before that month in the same year.  These
 * are correct for non-leap years only.
 */
static const int _days_in_month[] = {
    0, /* unused; this vector uses 1-based indexing */
    31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31,
};

static const int _days_before_month[] = {
    0, /* unused; this vector uses 1-based indexing */
    0,  31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334,
    365  // Useful for month + 1 accesses for December
};

/* year -> 1 if leap year, else 0. */
static int
is_leap(int year)
{
    /* Cast year to unsigned.  The result is the same either way, but
     * C can generate faster code for unsigned mod than for signed
     * mod (especially for % 4 -- a good compiler should just grab
     * the last 2 bits when the LHS is unsigned).
     */
    const unsigned int ayear = (unsigned int)year;
    return ayear % 4 == 0 && (ayear % 100 != 0 || ayear % 400 == 0);
}

/* year, month -> number of days in that month in that year */
static int
days_in_month(int year, int month)
{
    assert(month >= 1);
    assert(month <= 12);
    if (month == 2 && is_leap(year))
        return 29;
    else
        return _days_in_month[month];
}

/* year, month -> number of days in year preceding first day of month */
static int
days_before_month(int year, int month)
{
    int days;

    assert(month >= 1);
    assert(month <= 12);
    days = _days_before_month[month];
    if (month > 2 && is_leap(year))
        ++days;
    return days;
}

/* year -> number of days before January 1st of year.  Remember that we
 * start with year 1, so days_before_year(1) == 0.
 */
static int
days_before_year(int year)
{
    int y = year - 1;
    /* This is incorrect if year <= 0; we really want the floor
     * here.  But so long as MINYEAR is 1, the smallest year this
     * can see is 1.
     */
    assert(year >= 1);
    return y * 365 + y / 4 - y / 100 + y / 400;
}

/* Number of days in 4, 100, and 400 year cycles.  That these have
 * the correct values is asserted in the module init function.
 */
#define DI4Y   1461   /* days_before_year(5); days in 4 years */
#define DI100Y 36524  /* days_before_year(101); days in 100 years */
#define DI400Y 146097 /* days_before_year(401); days in 400 years  */

/* ordinal -> year, month, day, considering 01-Jan-0001 as day 1. */
static void
ord_to_ymd(int ordinal, int *year, int *month, int *day)
{
    int n, n1, n4, n100, n400, leapyear, preceding;

    /* ordinal is a 1-based index, starting at 1-Jan-1.  The pattern of
     * leap years repeats exactly every 400 years.  The basic strategy is
     * to find the closest 400-year boundary at or before ordinal, then
     * work with the offset from that boundary to ordinal.  Life is much
     * clearer if we subtract 1 from ordinal first -- then the values
     * of ordinal at 400-year boundaries are exactly those divisible
     * by DI400Y:
     *
     *    D  M   Y            n              n-1
     *    -- --- ----        ----------     ----------------
     *    31 Dec -400        -DI400Y       -DI400Y -1
     *     1 Jan -399         -DI400Y +1   -DI400Y      400-year boundary
     *    ...
     *    30 Dec  000        -1             -2
     *    31 Dec  000         0             -1
     *     1 Jan  001         1              0          400-year boundary
     *     2 Jan  001         2              1
     *     3 Jan  001         3              2
     *    ...
     *    31 Dec  400         DI400Y        DI400Y -1
     *     1 Jan  401         DI400Y +1     DI400Y      400-year boundary
     */
    assert(ordinal >= 1);
    --ordinal;
    n400 = ordinal / DI400Y;
    n = ordinal % DI400Y;
    *year = n400 * 400 + 1;

    /* Now n is the (non-negative) offset, in days, from January 1 of
     * year, to the desired date.  Now compute how many 100-year cycles
     * precede n.
     * Note that it's possible for n100 to equal 4!  In that case 4 full
     * 100-year cycles precede the desired day, which implies the
     * desired day is December 31 at the end of a 400-year cycle.
     */
    n100 = n / DI100Y;
    n = n % DI100Y;

    /* Now compute how many 4-year cycles precede it. */
    n4 = n / DI4Y;
    n = n % DI4Y;

    /* And now how many single years.  Again n1 can be 4, and again
     * meaning that the desired day is December 31 at the end of the
     * 4-year cycle.
     */
    n1 = n / 365;
    n = n % 365;

    *year += n100 * 100 + n4 * 4 + n1;
    if (n1 == 4 || n100 == 4) {
        assert(n == 0);
        *year -= 1;
        *month = 12;
        *day = 31;
        return;
    }

    /* Now the year is correct, and n is the offset from January 1.  We
     * find the month via an estimate that's either exact or one too
     * large.
     */
    leapyear = n1 == 3 && (n4 != 24 || n100 == 3);
    assert(leapyear == is_leap(*year));
    *month = (n + 50) >> 5;
    preceding = (_days_before_month[*month] + (*month > 2 && leapyear));
    if (preceding > n) {
        /* estimate is too large */
        *month -= 1;
        preceding -= days_in_month(*year, *month);
    }
    n -= preceding;
    assert(0 <= n);
    assert(n < days_in_month(*year, *month));

    *day = n + 1;
}

/* year, month, day -> ordinal, considering 01-Jan-0001 as day 1. */
static int
ymd_to_ord(int year, int month, int day)
{
    return days_before_year(year) + days_before_month(year, month) + day;
}

/* Day of week, where Monday==0, ..., Sunday==6.  1/1/1 was a Monday. */
static int
weekday(int year, int month, int day)
{
    return (ymd_to_ord(year, month, day) + 6) % 7;
}

/* Ordinal of the Monday starting week 1 of the ISO year.  Week 1 is the
 * first calendar week containing a Thursday.
 */
static int
iso_week1_monday(int year)
{
    int first_day = ymd_to_ord(year, 1, 1); /* ord of 1/1 */
    /* 0 if 1/1 is a Monday, 1 if a Tue, etc. */
    int first_weekday = (first_day + 6) % 7;
    /* ordinal of closest Monday at or before 1/1 */
    int week1_monday = first_day - first_weekday;

    if (first_weekday > 3) /* if 1/1 was Fri, Sat, Sun */
        week1_monday += 7;
    return week1_monday;
}

int
iso_to_ymd(const int iso_year, const int iso_week, const int iso_day,
           int *year, int *month, int *day)
{
    if (iso_week <= 0 || iso_week >= 53) {
        int out_of_range = 1;
        if (iso_week == 53) {
            // ISO years have 53 weeks in it on years starting with a Thursday
            // and on leap years starting on Wednesday
            int first_weekday = weekday(iso_year, 1, 1);
            if (first_weekday == 3 ||
                (first_weekday == 2 && is_leap(iso_year))) {
                out_of_range = 0;
            }
        }

        if (out_of_range) {
            return -2;
        }
    }

    if (iso_day <= 0 || iso_day >= 8) {
        return -3;
    }

    // Convert (Y, W, D) to (Y, M, D) in-place
    int day_1 = iso_week1_monday(iso_year);

    int day_offset = (iso_week - 1) * 7 + iso_day - 1;

    ord_to_ymd(day_1 + day_offset, year, month, day);
    return 0;
}

int
ordinal_to_ymd(const int iso_year, int ordinal_day, int *year, int *month,
               int *day)
{
    if (ordinal_day < 1) {
        return -1;
    }

    /* January */
    if (ordinal_day <= _days_before_month[2]) {
        *year = iso_year;
        *month = 1;
        *day = ordinal_day - _days_before_month[1];
        return 0;
    }

    /* February */
    if (ordinal_day <= (_days_before_month[3] + (is_leap(iso_year) ? 1 : 0))) {
        *year = iso_year;
        *month = 2;
        *day = ordinal_day - _days_before_month[2];
        return 0;
    }

    if (is_leap(iso_year)) {
        ordinal_day -= 1;
    }

    /* March - December */
    for (int i = 3; i <= 12; i++) {
        if (ordinal_day <= _days_before_month[i + 1]) {
            *year = iso_year;
            *month = i;
            *day = ordinal_day - _days_before_month[i];
            return 0;
        }
    }

    return -2;
}
