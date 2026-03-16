/***********************************************************************

       gie - The Geospatial Integrity Investigation Environment

************************************************************************

The Geospatial Integrity Investigation Environment "gie" is a modest
regression testing environment for the PROJ.4 transformation library.

Its primary design goal was to be able to replace those thousands of
lines of regression testing code that are (at time of writing) part
of PROJ.4, while not requiring any other kind of tooling than the same
C compiler already employed for compiling the library.

The basic functionality of the gie command language is implemented
through just 3 command verbs:

operation,     which defines the PROJ.4 operation to test,
accept,        which defines the input coordinate to read, and
expect,        which defines the result to expect.

E.g:

operation  +proj=utm  +zone=32  +ellps=GRS80
accept     12  55
expect     691_875.632_14   6_098_907.825_05

Note that gie accepts the underscore ("_") as a thousands separator.
It is not required (in fact, it is entirely ignored by the input
routine), but it significantly improves the readability of the very
long strings of numbers typically required in projected coordinates.

By default, gie considers the EXPECTation met, if the result comes to
within 0.5 mm of the expected. This default can be changed using the
'tolerance' command verb (and yes, I know, linguistically speaking, both
"operation" and "tolerance" are nouns, not verbs). See the first
few hundred lines of the "builtins.gie" test file for more details of
the command verbs available (verbs of both the VERBal and NOUNistic
persuation).

--

But more importantly than being an acronym for "Geospatial Integrity
Investigation Environment", gie were also the initials, user id, and
USGS email address of Gerald Ian Evenden (1935--2016), the geospatial
visionary, who, already in the 1980s, started what was to become the
PROJ.4 of today.

Gerald's clear vision was that map projections are *just special
functions*. Some of them rather complex, most of them of two variables,
but all of them *just special functions*, and not particularly more
special than the sin(), cos(), tan(), and hypot() already available in
the C standard library.

And hence, according to Gerald, *they should not be particularly much
harder to use*, for a programmer, than the sin()s, tan()s and hypot()s
so readily available.

Gerald's ingenuity also showed in the implementation of the vision,
where he devised a comprehensive, yet simple, system of key-value
pairs for parameterising a map projection, and the highly flexible
PJ struct, storing run-time compiled versions of those key-value pairs,
hence making a map projection function call, pj_fwd(PJ, point), as easy
as a traditional function call like hypot(x,y).

While today, we may have more formally well defined metadata systems
(most prominent the OGC WKT2 representation), nothing comes close being
as easily readable ("human compatible") as Gerald's key-value system.
This system in particular, and the PROJ.4 system in general, was
Gerald's great gift to anyone using and/or communicating about geodata.

It is only reasonable to name a program, keeping an eye on the integrity
of the PROJ.4 system, in honour of Gerald.

So in honour, and hopefully also in the spirit, of Gerald Ian Evenden
(1935--2016), this is the Geospatial Integrity Investigation Environment.

************************************************************************

Thomas Knudsen, thokn@sdfe.dk, 2017-10-01/2017-10-08

************************************************************************

* Copyright (c) 2017 Thomas Knudsen
* Copyright (c) 2017, SDFE
*
* Permission is hereby granted, free of charge, to any person obtaining a
* copy of this software and associated documentation files (the "Software"),
* to deal in the Software without restriction, including without limitation
* the rights to use, copy, modify, merge, publish, distribute, sublicense,
* and/or sell copies of the Software, and to permit persons to whom the
* Software is furnished to do so, subject to the following conditions:
*
* The above copyright notice and this permission notice shall be included
* in all copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
* OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
* THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
* FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
* DEALINGS IN THE SOFTWARE.

***********************************************************************/

#include <ctype.h>
#include <errno.h>
#include <math.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "proj.h"
#include "proj_internal.h"
#include "proj_strtod.h"
#include <cmath> /* for isnan */
#include <math.h>

#include "optargpm.h"

/* Package for flexible format I/O - ffio */
typedef struct ffio {
    FILE *f;
    const char *const *tags;
    const char *tag;
    char *args;
    char *next_args;
    size_t n_tags;
    size_t args_size;
    size_t next_args_size;
    size_t argc;
    size_t lineno, next_lineno;
    size_t level;
    bool strict_mode;
} ffio;

static int get_inp(ffio *G);
static int skip_to_next_tag(ffio *G);
static int step_into_gie_block(ffio *G);
static int nextline(ffio *G);
static int at_end_delimiter(ffio *G);
static const char *at_tag(ffio *G);
static int at_decorative_element(ffio *G);
static ffio *ffio_destroy(ffio *G);
static ffio *ffio_create(const char *const *tags, size_t n_tags,
                         size_t max_record_size);

static const char *const gie_tags[] = {
    "<gie>",
    "operation",
    "crs_src",
    "crs_dst",
    "use_proj4_init_rules",
    "accept",
    "expect",
    "roundtrip",
    "banner",
    "verbose",
    "direction",
    "tolerance",
    "ignore",
    "require_grid",
    "echo",
    "skip",
    "</gie>",
    "<gie-strict>",
    "</gie-strict>",
};

static const size_t n_gie_tags = sizeof gie_tags / sizeof gie_tags[0];

int main(int argc, char **argv);

static int dispatch(const char *cmnd, const char *args);
static int errmsg(int errlev, const char *msg, ...);
static int errno_from_err_const(const char *err_const);
static int list_err_codes(void);
static int process_file(const char *fname);

static const char *column(const char *buf, int n);
static const char *err_const_from_errno(int err);

#define SKIP -1

#define MAX_OPERATION 10000

typedef struct {
    char operation[MAX_OPERATION + 1];
    char crs_dst[MAX_OPERATION + 1];
    char crs_src[MAX_OPERATION + 1];
    PJ *P;
    PJ_COORD a, b, e;
    PJ_DIRECTION dir;
    int verbosity;
    int skip;
    int op_id;
    int op_ok, op_ko, op_skip;
    int total_ok, total_ko, total_skip;
    int grand_ok, grand_ko, grand_skip;
    size_t operation_lineno;
    size_t dimensions_given, dimensions_given_at_last_accept;
    double tolerance;
    int use_proj4_init_rules;
    int ignore;
    int skip_test;
    const char *curr_file;
    FILE *fout;
} gie_ctx;

ffio *F = nullptr;

static gie_ctx T;
int tests = 0, succs = 0, succ_fails = 0, fail_fails = 0, succ_rtps = 0,
    fail_rtps = 0;

static const char delim[] = {"-------------------------------------------------"
                             "------------------------------\n"};

static const char usage[] = {
    "--------------------------------------------------------------------------"
    "------\n"
    "Usage: %s [-options]... infile...\n"
    "--------------------------------------------------------------------------"
    "------\n"
    "Options:\n"
    "--------------------------------------------------------------------------"
    "------\n"
    "    -h                Help: print this usage information\n"
    "    -o /path/to/file  Specify output file name\n"
    "    -v                Verbose: Provide non-essential informational "
    "output.\n"
    "                      Repeat -v for more verbosity (e.g. -vv)\n"
    "    -q                Quiet: Opposite of verbose. In quiet mode not even "
    "errors\n"
    "                      are reported. Only interaction is through the "
    "return code\n"
    "                      (0 on success, non-zero indicates number of FAILED "
    "tests)\n"
    "    -l                List the PROJ internal system error codes\n"
    "--------------------------------------------------------------------------"
    "------\n"
    "Long Options:\n"
    "--------------------------------------------------------------------------"
    "------\n"
    "    --output          Alias for -o\n"
    "    --verbose         Alias for -v\n"
    "    --help            Alias for -h\n"
    "    --list            Alias for -l\n"
    "    --version         Print version number\n"
    "--------------------------------------------------------------------------"
    "------\n"
    "Examples:\n"
    "--------------------------------------------------------------------------"
    "------\n"
    "1. Run all tests in file \"corner-cases.gie\", providing much extra "
    "information\n"
    "       gie -vvvv corner-cases.gie\n"
    "2. Run all tests in files \"foo\" and \"bar\", providing info on failures "
    "only\n"
    "       gie foo bar\n"
    "--------------------------------------------------------------------------"
    "------\n"};

int main(int argc, char **argv) {
    int i;
    const char *longflags[] = {"v=verbose", "q=quiet", "h=help",
                               "l=list",    "version", nullptr};
    const char *longkeys[] = {"o=output", nullptr};
    OPTARGS *o;

    memset(&T, 0, sizeof(T));
    T.dir = PJ_FWD;
    T.verbosity = 1;
    T.tolerance = 5e-4;
    T.ignore = 5555; /* Error code that will not be issued by proj_create() */
    T.use_proj4_init_rules = FALSE;

    /* coverity[tainted_data] */
    o = opt_parse(argc, argv, "hlvq", "o", longflags, longkeys);
    if (nullptr == o)
        return 1;

    if (opt_given(o, "h") || argc == 1) {
        printf(usage, o->progname);
        free(o);
        return 0;
    }

    if (opt_given(o, "version")) {
        fprintf(stdout, "%s: %s\n", o->progname, pj_get_release());
        free(o);
        return 0;
    }

    T.verbosity = opt_given(o, "q");
    if (T.verbosity)
        T.verbosity = -1;
    if (T.verbosity != -1)
        T.verbosity = opt_given(o, "v") + 1;

    T.fout = stdout;
    if (opt_given(o, "o"))
        T.fout = fopen(opt_arg(o, "output"), "rt");

    if (nullptr == T.fout) {
        fprintf(stderr, "%s: Cannot open '%s' for output\n", o->progname,
                opt_arg(o, "output"));
        free(o);
        return 1;
    }

    if (opt_given(o, "l")) {
        free(o);
        return list_err_codes();
    }

    if (0 == o->fargc) {
        if (T.verbosity == -1)
            return -1;
        fprintf(T.fout, "Nothing to do\n");
        free(o);
        return 0;
    }

    F = ffio_create(gie_tags, n_gie_tags, 1000);
    if (nullptr == F) {
        fprintf(stderr, "%s: No memory\n", o->progname);
        free(o);
        return 1;
    }

    for (i = 0; i < o->fargc; i++) {
        FILE *f = fopen(o->fargv[i], "rt");
        if (f == nullptr) {
            fprintf(stderr, "%sCannot open specified input file '%s' - bye!\n",
                    delim, o->fargv[i]);
            free(o);
            return 1;
        }
        fclose(f);
    }

    for (i = 0; i < o->fargc; i++)
        process_file(o->fargv[i]);

    if (T.verbosity > 0) {
        if (o->fargc > 1) {
            fprintf(
                T.fout,
                "%sGrand total: %d. Success: %d, Skipped: %d, Failure: %d\n",
                delim, T.grand_ok + T.grand_ko + T.grand_skip, T.grand_ok,
                T.grand_skip, T.grand_ko);
        }
        fprintf(T.fout, "%s", delim);
        if (T.verbosity > 1) {
            fprintf(T.fout,
                    "Failing roundtrips: %4d,    Succeeding roundtrips: %4d\n",
                    fail_rtps, succ_rtps);
            fprintf(T.fout,
                    "Failing failures:   %4d,    Succeeding failures:   %4d\n",
                    fail_fails, succ_fails);
            fprintf(
                T.fout,
                "Internal counters:                            %4.4d(%4.4d)\n",
                tests, succs);
            fprintf(T.fout, "%s", delim);
        }
    } else if (T.grand_ko)
        fprintf(T.fout, "Failures: %d", T.grand_ko);

    if (stdout != T.fout)
        fclose(T.fout);

    free(o);
    ffio_destroy(F);
    return T.grand_ko;
}

static int another_failure(void) {
    T.op_ko++;
    T.total_ko++;
    proj_errno_reset(T.P);
    return 0;
}

static int another_skip(void) {
    T.op_skip++;
    T.total_skip++;
    return 0;
}

static int another_success(void) {
    T.op_ok++;
    T.total_ok++;
    proj_errno_reset(T.P);
    return 0;
}

static int another_succeeding_failure(void) {
    succ_fails++;
    return another_success();
}

static int another_failing_failure(void) {
    fail_fails++;
    return another_failure();
}

static int another_succeeding_roundtrip(void) {
    succ_rtps++;
    return another_success();
}

static int another_failing_roundtrip(void) {
    fail_rtps++;
    return another_failure();
}

static int process_file(const char *fname) {
    F->lineno = F->next_lineno = F->level = 0;
    T.op_ok = T.total_ok = 0;
    T.op_ko = T.total_ko = 0;
    T.op_skip = T.total_skip = 0;

    if (T.skip) {
        proj_destroy(T.P);
        T.P = nullptr;
        return 0;
    }

    /* We have already tested in main that the file exists */
    F->f = fopen(fname, "rt");

    if (T.verbosity > 0)
        fprintf(T.fout, "%sReading file '%s'\n", delim, fname);
    T.curr_file = fname;

    while (get_inp(F)) {
        if (SKIP == dispatch(F->tag, F->args)) {
            proj_destroy(T.P);
            T.P = nullptr;
            return 0;
        }
    }

    fclose(F->f);
    F->lineno = F->next_lineno = 0;

    T.grand_ok += T.total_ok;
    T.grand_ko += T.total_ko;
    T.grand_skip += T.grand_skip;
    if (T.verbosity > 0) {
        fprintf(
            T.fout,
            "%stotal: %2d tests succeeded, %2d tests skipped, %2d tests %s\n",
            delim, T.total_ok, T.total_skip, T.total_ko,
            T.total_ko ? "FAILED!" : "failed.");
    }
    if (F->level == 0)
        return errmsg(-3, "File '%s':Missing '<gie>' cmnd - bye!\n", fname);
    if (F->level % 2) {
        if (F->strict_mode)
            return errmsg(-4, "File '%s':Missing '</gie-strict>' cmnd - bye!\n",
                          fname);
        else
            return errmsg(-4, "File '%s':Missing '</gie>' cmnd - bye!\n",
                          fname);
    }
    return 0;
}

/*****************************************************************************/
const char *column(const char *buf, int n) {
    /*****************************************************************************
    Return a pointer to the n'th column of buf. Column numbers start at 0.
    ******************************************************************************/
    int i;
    if (n <= 0)
        return buf;
    for (i = 0; i < n; i++) {
        while (isspace(*buf))
            buf++;
        if (i == n - 1)
            break;
        while ((0 != *buf) && !isspace(*buf))
            buf++;
    }
    return buf;
}

/*****************************************************************************/
static double strtod_scaled(const char *args, double default_scale) {
    /*****************************************************************************
    Interpret <args> as a numeric followed by a linear decadal prefix.
    Return the properly scaled numeric
    ******************************************************************************/
    const double GRS80_DEG = 111319.4908; /* deg-to-m at equator of GRS80 */

    char *endp;
    double s = proj_strtod(args, &endp);
    if (args == endp)
        return HUGE_VAL;

    const char *units = column(args, 2);

    if (0 == strcmp(units, "km"))
        s *= 1000;
    else if (0 == strcmp(units, "m"))
        s *= 1;
    else if (0 == strcmp(units, "dm"))
        s /= 10;
    else if (0 == strcmp(units, "cm"))
        s /= 100;
    else if (0 == strcmp(units, "mm"))
        s /= 1000;
    else if (0 == strcmp(units, "um"))
        s /= 1e6;
    else if (0 == strcmp(units, "nm"))
        s /= 1e9;
    else if (0 == strcmp(units, "rad"))
        s = GRS80_DEG * proj_todeg(s);
    else if (0 == strcmp(units, "deg"))
        s = GRS80_DEG * s;
    else
        s *= default_scale;
    return s;
}

static int banner(const char *args) {
    char dots[] = {"..."}, nodots[] = {""}, *thedots = nodots;
    if (strlen(args) > 70)
        thedots = dots;
    fprintf(T.fout, "%s%-70.70s%s\n", delim, args, thedots);
    return 0;
}

static int tolerance(const char *args) {
    T.tolerance = strtod_scaled(args, 1);
    if (HUGE_VAL == T.tolerance) {
        T.tolerance = 0.0005;
        return 1;
    }
    return 0;
}

static int use_proj4_init_rules(const char *args) {
    T.use_proj4_init_rules = strcmp(args, "true") == 0;
    return 0;
}

static int ignore(const char *args) {
    T.ignore = errno_from_err_const(column(args, 1));
    return 0;
}

static int require_grid(const char *args) {
    PJ_GRID_INFO grid_info;
    const char *grid_filename = column(args, 1);
    grid_info = proj_grid_info(grid_filename);
    if (strlen(grid_info.filename) == 0) {
        if (T.verbosity > 1) {
            fprintf(T.fout, "Test skipped because of missing grid %s\n",
                    grid_filename);
        }
        T.skip_test = 1;
    }
    return 0;
}

static int direction(const char *args) {
    const char *endp = args;
    while (isspace(*endp))
        endp++;
    switch (*endp) {
    case 'F':
    case 'f':
        T.dir = PJ_FWD;
        break;
    case 'I':
    case 'i':
    case 'R':
    case 'r':
        T.dir = PJ_INV;
        break;
    default:
        return 1;
    }

    return 0;
}

static void finish_previous_operation(const char *args) {
    if (T.verbosity > 1 && T.op_id > 1 && T.op_ok + T.op_ko)
        fprintf(T.fout,
                "%s     %d tests succeeded,  %d tests skipped, %d tests %s\n",
                delim, T.op_ok, T.op_skip, T.op_ko,
                T.op_ko ? "FAILED!" : "failed.");
    (void)args;
}

/*****************************************************************************/
static int operation(const char *args) {
    /*****************************************************************************
    Define the operation to apply to the input data (in ISO 19100 lingo,
    an operation is the general term describing something that can be
    either a conversion or a transformation)
    ******************************************************************************/
    T.op_id++;

    T.operation_lineno = F->lineno;

    strncpy(&(T.operation[0]), F->args, MAX_OPERATION);
    T.operation[MAX_OPERATION] = '\0';

    if (T.verbosity > 1) {
        finish_previous_operation(F->args);
        banner(args);
    }

    T.op_ok = 0;
    T.op_ko = 0;
    T.op_skip = 0;
    T.skip_test = 0;

    direction("forward");
    tolerance("0.5 mm");
    ignore("pjd_err_dont_skip");

    proj_errno_reset(T.P);

    if (T.P)
        proj_destroy(T.P);
    proj_errno_reset(nullptr);
    proj_context_use_proj4_init_rules(nullptr, T.use_proj4_init_rules);

    T.P = proj_create(nullptr, F->args);

    /* Checking that proj_create succeeds is first done at "expect" time, */
    /* since we want to support "expect"ing specific error codes */

    return 0;
}

static int crs_to_crs_operation() {
    T.op_id++;
    T.operation_lineno = F->lineno;

    if (T.verbosity > 1) {
        char buffer[80];
        finish_previous_operation(F->args);
        snprintf(buffer, 80, "%-36.36s -> %-36.36s", T.crs_src, T.crs_dst);
        banner(buffer);
    }

    T.op_ok = 0;
    T.op_ko = 0;
    T.op_skip = 0;
    T.skip_test = 0;

    direction("forward");
    tolerance("0.5 mm");
    ignore("pjd_err_dont_skip");

    proj_errno_reset(T.P);

    if (T.P)
        proj_destroy(T.P);
    proj_errno_reset(nullptr);
    proj_context_use_proj4_init_rules(nullptr, T.use_proj4_init_rules);

    T.P = proj_create_crs_to_crs(nullptr, T.crs_src, T.crs_dst, nullptr);

    strcpy(T.crs_src, "");
    strcpy(T.crs_dst, "");
    return 0;
}

static int crs_src(const char *args) {
    strncpy(&(T.crs_src[0]), F->args, MAX_OPERATION);
    T.crs_src[MAX_OPERATION] = '\0';
    (void)args;

    if (strcmp(T.crs_src, "") != 0 && strcmp(T.crs_dst, "") != 0) {
        crs_to_crs_operation();
    }

    return 0;
}

static int crs_dst(const char *args) {
    strncpy(&(T.crs_dst[0]), F->args, MAX_OPERATION);
    T.crs_dst[MAX_OPERATION] = '\0';
    (void)args;

    if (strcmp(T.crs_src, "") != 0 && strcmp(T.crs_dst, "") != 0) {
        crs_to_crs_operation();
    }

    return 0;
}

static PJ_COORD torad_coord(PJ *P, PJ_DIRECTION dir, PJ_COORD a) {
    size_t i, n;
    const char *axis = "enut";
    paralist *l = pj_param_exists(P->params, "axis");
    if (l && dir == PJ_INV)
        axis = l->param + strlen("axis=");
    n = strlen(axis);
    for (i = 0; i < n; i++)
        if (strchr("news", axis[i]))
            a.v[i] = proj_torad(a.v[i]);
    return a;
}

static PJ_COORD todeg_coord(PJ *P, PJ_DIRECTION dir, PJ_COORD a) {
    size_t i, n;
    const char *axis = "enut";
    paralist *l = pj_param_exists(P->params, "axis");
    if (l && dir == PJ_FWD)
        axis = l->param + strlen("axis=");
    n = strlen(axis);
    for (i = 0; i < n; i++)
        if (strchr("news", axis[i]))
            a.v[i] = proj_todeg(a.v[i]);
    return a;
}

/*****************************************************************************/
static PJ_COORD parse_coord(const char *args) {
    /*****************************************************************************
    Attempt to interpret args as a PJ_COORD.
    ******************************************************************************/
    int i;
    char *endp;
    char *dmsendp;
    const char *prev = args;
    PJ_COORD a = proj_coord(0, 0, 0, 0);

    T.dimensions_given = 0;
    for (i = 0; i < 4; i++) {
        /* proj_strtod doesn't read values like 123d45'678W so we need a bit */
        /* of help from proj_dmstor. proj_strtod effectively ignores what    */
        /* comes after "d", so we use that fact that when dms is larger than */
        /* d the value was stated in "dms" form.                             */
        /* This could be avoided if proj_dmstor used the same proj_strtod()  */
        /* as gie, but that is not the case (yet). When we remove projects.h */
        /* from the public API we can change that.                           */

        // Even Rouault: unsure about the above. Coordinates are not necessarily
        // geographic coordinates, and the roundtrip through radians for
        // big projected coordinates cause inaccuracies, that can cause
        // test failures when testing points at edge of grids.
        // For example 1501000.0 becomes 1501000.000000000233
        double d;
        while (*prev && isspace(*prev))
            ++prev;
        if (strncmp(prev, "HUGE_VAL", strlen("HUGE_VAL")) == 0) {
            d = HUGE_VAL;
            endp = const_cast<char *>(prev) + strlen("HUGE_VAL");
        } else {
            d = proj_strtod(prev, &endp);
        }
        if (!std::isnan(d) && *endp != '\0' && !isspace(*endp)) {
            double dms = PJ_TODEG(proj_dmstor(prev, &dmsendp));
            /* TODO: When projects.h is removed, call proj_dmstor() in all cases
             */
            if (d != dms && fabs(d) < fabs(dms) && fabs(dms) < fabs(d) + 1) {
                d = dms;
                endp = dmsendp;
            }
            /* A number like -81d00'00.000 will be parsed correctly by both */
            /* proj_strtod and proj_dmstor but only the latter will return  */
            /* the correct end-pointer.                                     */
            if (d == dms && endp != dmsendp)
                endp = dmsendp;
        }

        /* Break out if there were no more numerals */
        if (prev == endp)
            return i > 1 ? a : proj_coord_error();

        a.v[i] = d;
        prev = endp;
        T.dimensions_given++;
    }

    return a;
}

/*****************************************************************************/
static int accept(const char *args) {
    /*****************************************************************************
    Read ("ACCEPT") a 2, 3, or 4 dimensional input coordinate.
    ******************************************************************************/
    T.a = parse_coord(args);
    if (T.verbosity > 3)
        fprintf(T.fout, "#  %s\n", args);
    T.dimensions_given_at_last_accept = T.dimensions_given;
    return 0;
}

/*****************************************************************************/
static int roundtrip(const char *args) {
    /*****************************************************************************
    Check how far we go from the ACCEPTed point when doing successive
    back/forward transformation pairs.

    Without args, roundtrip defaults to 100 iterations:

      roundtrip

    With one arg, roundtrip will default to a tolerance of T.tolerance:

      roundtrip ntrips

    With two args:

      roundtrip ntrips tolerance

    Always returns 0.
    ******************************************************************************/
    int ntrips;
    double d, r, ans;
    char *endp;
    PJ_COORD coo;

    if (nullptr == T.P) {
        if (T.ignore == proj_errno(T.P))
            return another_skip();

        return another_failure();
    }

    ans = proj_strtod(args, &endp);
    if (endp == args) {
        /* Default to 100 iterations if not args. */
        ntrips = 100;
    } else {
        if (ans < 1.0 || ans > 1000000.0) {
            errmsg(2, "Invalid number of roundtrips: %lf\n", ans);
            return another_failing_roundtrip();
        }
        ntrips = (int)ans;
    }

    d = strtod_scaled(endp, 1);
    d = d == HUGE_VAL ? T.tolerance : d;

    /* input ("accepted") values - probably in degrees */
    coo = proj_angular_input(T.P, T.dir) ? torad_coord(T.P, T.dir, T.a) : T.a;

    r = proj_roundtrip(T.P, T.dir, ntrips, &coo);
    if ((std::isnan(r) && std::isnan(d)) || r <= d)
        return another_succeeding_roundtrip();

    if (T.verbosity > -1) {
        if (0 == T.op_ko && T.verbosity < 2)
            banner(T.operation);
        fprintf(T.fout, "%s", T.op_ko ? "     -----\n" : delim);
        fprintf(T.fout, "     FAILURE in %s(%d):\n",
                opt_strip_path(T.curr_file), (int)F->lineno);
        fprintf(T.fout,
                "     roundtrip deviation: %.6f mm, expected: %.6f mm\n",
                1000 * r, 1000 * d);
    }
    return another_failing_roundtrip();
}

static int expect_message(double d, const char *args) {
    another_failure();

    if (T.verbosity < 0)
        return 1;
    if (d > 1e6)
        d = 999999.999999;
    if (0 == T.op_ko && T.verbosity < 2)
        banner(T.operation);
    fprintf(T.fout, "%s", T.op_ko ? "     -----\n" : delim);

    fprintf(T.fout, "     FAILURE in %s(%d):\n", opt_strip_path(T.curr_file),
            (int)F->lineno);
    fprintf(T.fout, "     expected: %s\n", args);
    fprintf(T.fout, "     got:      %.12f   %.12f", T.b.xy.x, T.b.xy.y);
    if (T.b.xyzt.t != 0 || T.b.xyzt.z != 0)
        fprintf(T.fout, "   %.9f", T.b.xyz.z);
    if (T.b.xyzt.t != 0)
        fprintf(T.fout, "   %.9f", T.b.xyzt.t);
    fprintf(T.fout, "\n");
    fprintf(T.fout, "     deviation:  %.6f mm,  expected:  %.6f mm\n", 1000 * d,
            1000 * T.tolerance);
    return 1;
}

static int expect_message_cannot_parse(const char *args) {
    another_failure();
    if (T.verbosity > -1) {
        if (0 == T.op_ko && T.verbosity < 2)
            banner(T.operation);
        fprintf(T.fout, "%s", T.op_ko ? "     -----\n" : delim);
        fprintf(T.fout, "     FAILURE in %s(%d):\n     Too few args: %s\n",
                opt_strip_path(T.curr_file), (int)F->lineno, args);
    }
    return 1;
}

static int expect_failure_with_errno_message(int expected, int got) {
    another_failing_failure();

    if (T.verbosity < 0)
        return 1;
    if (0 == T.op_ko && T.verbosity < 2)
        banner(T.operation);
    fprintf(T.fout, "%s", T.op_ko ? "     -----\n" : delim);
    fprintf(T.fout, "     FAILURE in %s(%d):\n", opt_strip_path(T.curr_file),
            (int)F->lineno);
    fprintf(T.fout, "     got errno %s (%d): %s\n", err_const_from_errno(got),
            got, proj_errno_string(got));
    fprintf(T.fout, "     expected %s (%d):  %s",
            err_const_from_errno(expected), expected,
            proj_errno_string(expected));
    fprintf(T.fout, "\n");
    return 1;
}

/* For test purposes, we want to call a transformation of the same */
/* dimensionality as the number of dimensions given in accept */
static PJ_COORD expect_trans_n_dim(const PJ_COORD &ci) {
    if (4 == T.dimensions_given_at_last_accept)
        return proj_trans(T.P, T.dir, ci);

    if (3 == T.dimensions_given_at_last_accept)
        return pj_approx_3D_trans(T.P, T.dir, ci);

    return pj_approx_2D_trans(T.P, T.dir, ci);
}

/*****************************************************************************/
static int expect(const char *args) {
    /*****************************************************************************
    Tell GIE what to expect, when transforming the ACCEPTed input
    ******************************************************************************/
    PJ_COORD ci, co, ce;
    double d;
    int expect_failure = 0;
    int expect_failure_with_errno = 0;

    if (0 == strncmp(args, "failure", 7)) {
        expect_failure = 1;

        /* Option: Fail with an expected errno (syntax: expect failure errno
         * -33) */
        if (0 == strncmp(column(args, 2), "errno", 5))
            expect_failure_with_errno = errno_from_err_const(column(args, 3));
    }

    if (T.ignore == proj_errno(T.P))
        return another_skip();

    if (nullptr == T.P) {
        /* If we expect failure, and fail, then it's a success... */
        if (expect_failure) {
            /* Failed to fail correctly? */
            if (expect_failure_with_errno &&
                proj_errno(T.P) != expect_failure_with_errno)
                return expect_failure_with_errno_message(
                    expect_failure_with_errno, proj_errno(T.P));

            return another_succeeding_failure();
        }

        /* Otherwise, it's a true failure */
        banner(T.operation);
        errmsg(3,
               "%sInvalid operation definition in line no. %d:\n       %s "
               "(errno=%s/%d)\n",
               delim, (int)T.operation_lineno,
               proj_errno_string(proj_errno(T.P)),
               err_const_from_errno(proj_errno(T.P)), proj_errno(T.P));
        return another_failing_failure();
    }

    /* We may still successfully fail even if the proj_create succeeded */
    if (expect_failure) {
        proj_errno_reset(T.P);

        /* Try to carry out the operation - and expect failure */
        ci =
            proj_angular_input(T.P, T.dir) ? torad_coord(T.P, T.dir, T.a) : T.a;
        co = expect_trans_n_dim(ci);

        if (expect_failure_with_errno) {
            if (proj_errno(T.P) == expect_failure_with_errno)
                return another_succeeding_failure();
            // fprintf (T.fout, "errno=%d, expected=%d\n", proj_errno (T.P),
            // expect_failure_with_errno);
            banner(T.operation);
            errmsg(3, "%serrno=%s (%d), expected=%d at line %d\n", delim,
                   err_const_from_errno(proj_errno(T.P)), proj_errno(T.P),
                   expect_failure_with_errno, static_cast<int>(F->lineno));
            return another_failing_failure();
        }

        /* Succeeded in failing? - that's a success */
        if (co.xyz.x == HUGE_VAL)
            return another_succeeding_failure();

        /* Failed to fail? - that's a failure */
        banner(T.operation);
        errmsg(3, "%sFailed to fail. Operation definition in line no. %d\n",
               delim, (int)T.operation_lineno);
        return another_failing_failure();
    }

    if (T.verbosity > 3) {
        fprintf(T.fout, "%s\n", T.P->inverted ? "INVERTED" : "NOT INVERTED");
        fprintf(T.fout, "%s\n", T.dir == 1 ? "forward" : "reverse");
        fprintf(T.fout, "%s\n",
                proj_angular_input(T.P, T.dir) ? "angular in" : "linear in");
        fprintf(T.fout, "%s\n",
                proj_angular_output(T.P, T.dir) ? "angular out" : "linear out");
        fprintf(T.fout, "left: %d   right:  %d\n", T.P->left, T.P->right);
    }

    tests++;
    T.e = parse_coord(args);
    if (HUGE_VAL == T.e.v[0])
        return expect_message_cannot_parse(args);

    /* expected angular values, probably in degrees */
    ce = proj_angular_output(T.P, T.dir) ? torad_coord(T.P, T.dir, T.e) : T.e;
    if (T.verbosity > 3)
        fprintf(T.fout, "EXPECTS  %.12f  %.12f  %.12f  %.12f\n", ce.v[0],
                ce.v[1], ce.v[2], ce.v[3]);

    /* input ("accepted") values, also probably in degrees */
    ci = proj_angular_input(T.P, T.dir) ? torad_coord(T.P, T.dir, T.a) : T.a;
    if (T.verbosity > 3)
        fprintf(T.fout, "ACCEPTS  %.12f  %.12f  %.12f  %.12f\n", ci.v[0],
                ci.v[1], ci.v[2], ci.v[3]);

    /* do the transformation, but mask off dimensions not given in expect-ation
     */
    co = expect_trans_n_dim(ci);
    if (T.dimensions_given < 4)
        co.v[3] = 0;
    if (T.dimensions_given < 3)
        co.v[2] = 0;

    /* angular output from proj_trans comes in radians */
    T.b = proj_angular_output(T.P, T.dir) ? todeg_coord(T.P, T.dir, co) : co;
    if (T.verbosity > 3)
        fprintf(T.fout, "GOT      %.12f  %.12f  %.12f  %.12f\n", co.v[0],
                co.v[1], co.v[2], co.v[3]);

#if 0
    /* We need to handle unusual axis orders - that'll be an item for version 5.1 */
    if (T.P->axisswap) {
        ce = proj_trans (T.P->axisswap, T.dir, ce);
        co = proj_trans (T.P->axisswap, T.dir, co);
    }
#endif
    if (std::isnan(co.v[0]) && std::isnan(ce.v[0])) {
        d = 0.0;
    } else if (proj_angular_output(T.P, T.dir)) {
        d = proj_lpz_dist(T.P, ce, co);
    } else {
        d = proj_xyz_dist(co, ce);
    }

    // Test written like that to handle NaN
    if (!(d <= T.tolerance))
        return expect_message(d, args);
    succs++;

    another_success();
    return 0;
}

/*****************************************************************************/
static int verbose(const char *args) {
    /*****************************************************************************
    Tell the system how noisy it should be
    ******************************************************************************/
    int i = (int)proj_atof(args);

    /* if -q/--quiet flag has been given, we do nothing */
    if (T.verbosity < 0)
        return 0;

    if (strlen(args))
        T.verbosity = i;
    else
        T.verbosity++;
    return 0;
}

/*****************************************************************************/
static int echo(const char *args) {
    /*****************************************************************************
    Add user defined noise to the output stream
    ******************************************************************************/
    fprintf(T.fout, "%s\n", args);
    return 0;
}

/*****************************************************************************/
static int skip(const char *args) {
    /*****************************************************************************
    Indicate that the remaining material should be skipped. Mostly for
    debugging.
    ******************************************************************************/
    T.skip = 1;
    (void)args;
    F->level = 2; /* Silence complaints about missing </gie> element */
    return 0;
}

static int dispatch(const char *cmnd, const char *args) {
    if (T.skip)
        return SKIP;
    if (0 == strcmp(cmnd, "operation"))
        return operation(args);
    if (0 == strcmp(cmnd, "crs_src"))
        return crs_src(args);
    if (0 == strcmp(cmnd, "crs_dst"))
        return crs_dst(args);
    if (T.skip_test) {
        if (0 == strcmp(cmnd, "expect"))
            return another_skip();
        return 0;
    }
    if (0 == strcmp(cmnd, "accept"))
        return accept(args);
    if (0 == strcmp(cmnd, "expect"))
        return expect(args);
    if (0 == strcmp(cmnd, "roundtrip"))
        return roundtrip(args);
    if (0 == strcmp(cmnd, "banner"))
        return banner(args);
    if (0 == strcmp(cmnd, "verbose"))
        return verbose(args);
    if (0 == strcmp(cmnd, "direction"))
        return direction(args);
    if (0 == strcmp(cmnd, "tolerance"))
        return tolerance(args);
    if (0 == strcmp(cmnd, "ignore"))
        return ignore(args);
    if (0 == strcmp(cmnd, "require_grid"))
        return require_grid(args);
    if (0 == strcmp(cmnd, "echo"))
        return echo(args);
    if (0 == strcmp(cmnd, "skip"))
        return skip(args);
    if (0 == strcmp(cmnd, "use_proj4_init_rules"))
        return use_proj4_init_rules(args);

    return 0;
}

namespace { // anonymous namespace
static const struct {
    /* cppcheck-suppress unusedStructMember */
    const char *the_err_const;
    /* cppcheck-suppress unusedStructMember */
    int the_errno;
} lookup[] = {

    {"invalid_op", PROJ_ERR_INVALID_OP},
    {"invalid_op_wrong_syntax", PROJ_ERR_INVALID_OP_WRONG_SYNTAX},
    {"invalid_op_missing_arg", PROJ_ERR_INVALID_OP_MISSING_ARG},
    {"invalid_op_illegal_arg_value", PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE},
    {"invalid_op_mutually_exclusive_args",
     PROJ_ERR_INVALID_OP_MUTUALLY_EXCLUSIVE_ARGS},
    {"invalid_op_file_not_found_or_invalid",
     PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID},
    {"coord_transfm", PROJ_ERR_COORD_TRANSFM},
    {"coord_transfm_invalid_coord", PROJ_ERR_COORD_TRANSFM_INVALID_COORD},
    {"coord_transfm_outside_projection_domain",
     PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN},
    {"coord_transfm_no_operation", PROJ_ERR_COORD_TRANSFM_NO_OPERATION},
    {"coord_transfm_outside_grid", PROJ_ERR_COORD_TRANSFM_OUTSIDE_GRID},
    {"coord_transfm_grid_at_nodata", PROJ_ERR_COORD_TRANSFM_GRID_AT_NODATA},
    {"coord_transfm_missing_time", PROJ_ERR_COORD_TRANSFM_MISSING_TIME},
    {"other", PROJ_ERR_OTHER},
    {"api_misuse", PROJ_ERR_OTHER_API_MISUSE},
    {"no_inverse_op", PROJ_ERR_OTHER_NO_INVERSE_OP},
    {"network_error", PROJ_ERR_OTHER_NETWORK_ERROR},
};
} // anonymous namespace

static int list_err_codes(void) {
    int i;
    const int n = sizeof lookup / sizeof lookup[0];

    for (i = 0; i < n; i++) {
        fprintf(T.fout, "%25s  (%2.2d):  %s\n", lookup[i].the_err_const,
                lookup[i].the_errno, proj_errno_string(lookup[i].the_errno));
    }
    return 0;
}

static const char *err_const_from_errno(int err) {
    size_t i;
    const size_t n = sizeof lookup / sizeof lookup[0];

    for (i = 0; i < n; i++) {
        if (err == lookup[i].the_errno)
            return lookup[i].the_err_const;
    }
    return "unknown";
}

static int errno_from_err_const(const char *err_const) {
    const size_t n = sizeof lookup / sizeof lookup[0];
    size_t i, len;
    int ret;
    char tolower_err_const[100] = {};

    /* Make a lower case copy for matching */
    for (i = 0; i < 99; i++) {
        if (0 == err_const[i] || isspace(err_const[i]))
            break;
        tolower_err_const[i] = (char)tolower(err_const[i]);
    }
    tolower_err_const[i] = 0;

    /* If it looks numeric, return that numeric */
    ret = (int)pj_atof(err_const);
    if (0 != ret)
        return ret;

    /* Else try to find a matching identifier */
    len = strlen(tolower_err_const);

    for (i = 0; i < n; i++) {
        if (0 == strncmp(lookup[i].the_err_const, err_const, len))
            return lookup[i].the_errno;
    }

    /* On failure, return something unlikely */
    return 9999;
}

static int errmsg(int errlev, const char *msg, ...) {
    va_list args;
    va_start(args, msg);
    vfprintf(stdout, msg, args);
    va_end(args);
    if (errlev)
        errno = errlev;
    return errlev;
}

/****************************************************************************************

FFIO - Flexible format I/O

FFIO provides functionality for reading proj style instruction strings written
in a less strict format than usual:

*  Whitespace is generally allowed everywhere
*  Comments can be written inline, '#' style
*  ... or as free format blocks

The overall mission of FFIO is to facilitate communications of geodetic
parameters and test material in a format that is highly human readable,
and provides ample room for comment, documentation, and test material.

See the PROJ ".gie" test suites for examples of supported formatting.

****************************************************************************************/

/***************************************************************************************/
static ffio *ffio_create(const char *const *tags, size_t n_tags,
                         size_t max_record_size) {
    /****************************************************************************************
    Constructor for the ffio object.
    ****************************************************************************************/
    ffio *G = static_cast<ffio *>(calloc(1, sizeof(ffio)));
    if (nullptr == G)
        return nullptr;

    if (0 == max_record_size)
        max_record_size = 1000;

    G->args = static_cast<char *>(calloc(1, 5 * max_record_size));
    if (nullptr == G->args) {
        free(G);
        return nullptr;
    }

    G->next_args = static_cast<char *>(calloc(1, max_record_size));
    if (nullptr == G->args) {
        free(G->args);
        free(G);
        return nullptr;
    }

    G->args_size = 5 * max_record_size;
    G->next_args_size = max_record_size;

    G->tags = tags;
    G->n_tags = n_tags;
    return G;
}

/***************************************************************************************/
static ffio *ffio_destroy(ffio *G) {
    /****************************************************************************************
    Free all allocated associated memory, then free G itself. For extra RAII
    compliance, the file object should also be closed if still open, but this
    will require additional control logic, and ffio is a gie tool specific
    package, so we fall back to asserting that fclose has been called prior to
    ffio_destroy.
    ****************************************************************************************/
    free(G->args);
    free(G->next_args);
    free(G);
    return nullptr;
}

/***************************************************************************************/
static int at_decorative_element(ffio *G) {
    /****************************************************************************************
    A decorative element consists of a line of at least 5 consecutive identical
    chars, starting at buffer position 0:
    "-----", "=====", "*****", etc.

    A decorative element serves as a end delimiter for the current element, and
    continues until a gie command verb is found at the start of a line
    ****************************************************************************************/
    int i;
    char *c;
    if (nullptr == G)
        return 0;
    c = G->next_args;
    if (nullptr == c)
        return 0;
    if (0 == c[0])
        return 0;
    for (i = 1; i < 5; i++)
        if (c[i] != c[0])
            return 0;
    return 1;
}

/***************************************************************************************/
static const char *at_tag(ffio *G) {
    /****************************************************************************************
    A start of a new command serves as an end delimiter for the current command
    ****************************************************************************************/
    size_t j;
    for (j = 0; j < G->n_tags; j++)
        if (strncmp(G->next_args, G->tags[j], strlen(G->tags[j])) == 0)
            return G->tags[j];
    return nullptr;
}

/***************************************************************************************/
static int at_end_delimiter(ffio *G) {
    /****************************************************************************************
    An instruction consists of everything from its introductory tag to its end
    delimiter.  An end delimiter can be either the introductory tag of the next
    instruction, or a "decorative element", i.e. one of the "ascii art" style
    block delimiters typically used to mark up block comments in a free format
    file.
    ****************************************************************************************/
    if (G == nullptr)
        return 0;
    if (at_decorative_element(G))
        return 1;
    if (at_tag(G))
        return 1;
    return 0;
}

/***************************************************************************************/
static int nextline(ffio *G) {
    /****************************************************************************************
    Read next line of input file. Returns 1 on success, 0 on failure.
    ****************************************************************************************/
    G->next_args[0] = 0;
    if (T.skip)
        return 0;
    if (nullptr == fgets(G->next_args, (int)G->next_args_size - 1, G->f))
        return 0;
    if (feof(G->f))
        return 0;
    pj_chomp(G->next_args);
    G->next_lineno++;
    return 1;
}

/***************************************************************************************/
static int step_into_gie_block(ffio *G) {
    /****************************************************************************************
    Make sure we're inside a <gie>-block. Return 1 on success, 0 otherwise.
    ****************************************************************************************/
    /* Already inside */
    if (G->level % 2)
        return 1;

    while (strncmp(G->next_args, "<gie>", strlen("<gie>")) != 0 &&
           strncmp(G->next_args, "<gie-strict>", strlen("<gie-strict>")) != 0) {
        if (0 == nextline(G))
            return 0;
    }

    G->level++;

    if (strncmp(G->next_args, "<gie-strict>", strlen("<gie-strict>")) == 0) {
        G->strict_mode = true;
        return 0;
    } else {
        /* We're ready at the start - now step into the block */
        return nextline(G);
    }
}

/***************************************************************************************/
static int skip_to_next_tag(ffio *G) {
    /****************************************************************************************
    Skip forward to the next command tag. Return 1 on success, 0 otherwise.
    ****************************************************************************************/
    const char *c;
    if (0 == step_into_gie_block(G))
        return 0;

    c = at_tag(G);

    /* If not already there - get there */
    while (!c) {
        if (0 == nextline(G))
            return 0;
        c = at_tag(G);
    }

    /* If we reached the end of a <gie> block, locate the next and retry */
    if (0 == strcmp(c, "</gie>")) {
        G->level++;
        if (feof(G->f))
            return 0;
        if (0 == step_into_gie_block(G))
            return 0;
        G->args[0] = 0;
        return skip_to_next_tag(G);
    }
    G->lineno = G->next_lineno;

    return 1;
}

/* Add the most recently read line of input to the block already stored. */
static int append_args(ffio *G) {
    size_t skip_chars = 0;
    size_t next_len = strlen(G->next_args);
    size_t args_len = strlen(G->args);
    const char *tag = at_tag(G);

    if (tag)
        skip_chars = strlen(tag);

    /* +2: 1 for the space separator and 1 for the NUL termination. */
    if (G->args_size < args_len + next_len - skip_chars + 2) {
        char *p = static_cast<char *>(realloc(G->args, 2 * G->args_size));
        if (nullptr == p)
            return 0;
        G->args = p;
        G->args_size = 2 * G->args_size;
    }

    G->args[args_len] = ' ';
    strcpy(G->args + args_len + 1, G->next_args + skip_chars);

    G->next_args[0] = 0;
    return 1;
}

/***************************************************************************************/
static int get_inp(ffio *G) {
    /****************************************************************************************
    The primary command reader for gie. Reads a block of gie input, cleans up
    repeated whitespace etc. The block is stored in G->args. Returns 1 on
    success, 0 otherwise.
    ****************************************************************************************/
    G->args[0] = 0;

    // Special parsing in strict_mode:
    // - All non-comment/decoration lines must start with a valid tag
    // - Commands split on several lines should be terminated with " \"
    if (G->strict_mode) {
        while (nextline(G)) {
            G->lineno = G->next_lineno;
            if (G->next_args[0] == 0 || at_decorative_element(G)) {
                continue;
            }
            G->tag = at_tag(G);
            if (nullptr == G->tag) {
                another_failure();
                fprintf(T.fout, "unsupported command line %d: %s\n",
                        (int)G->lineno, G->next_args);
                return 0;
            }

            append_args(G);
            pj_shrink(G->args);
            while (G->args[0] != '\0' && G->args[strlen(G->args) - 1] == '\\') {
                G->args[strlen(G->args) - 1] = 0;
                if (!nextline(G)) {
                    return 0;
                }
                G->lineno = G->next_lineno;
                append_args(G);
                pj_shrink(G->args);
            }
            if (0 == strcmp(G->tag, "</gie-strict>")) {
                G->level++;
                G->strict_mode = false;
            }
            return 1;
        }
        return 0;
    }

    if (0 == skip_to_next_tag(G)) {
        // If we just entered <gie-strict>, re-enter to read the first command
        if (G->strict_mode)
            return get_inp(G);
        return 0;
    }
    G->tag = at_tag(G);

    if (nullptr == G->tag)
        return 0;

    do {
        append_args(G);
        if (0 == nextline(G))
            return 0;
    } while (!at_end_delimiter(G));

    pj_shrink(G->args);
    return 1;
}
