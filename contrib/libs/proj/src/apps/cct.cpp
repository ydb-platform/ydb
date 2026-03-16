/***********************************************************************

                 The cct 4D Transformation program

************************************************************************

cct is a 4D equivalent to the "proj" projection program.

cct is an acronym meaning "Coordinate Conversion and Transformation".

The acronym refers to definitions given in the OGC 08-015r2/ISO-19111
standard "Geographical Information -- Spatial Referencing by Coordinates",
which defines two different classes of coordinate operations:

*Coordinate Conversions*, which are coordinate operations where input
and output datum are identical (e.g. conversion from geographical to
cartesian coordinates) and

*Coordinate Transformations*, which are coordinate operations where
input and output datums differ (e.g. change of reference frame).

cct, however, also refers to Carl Christian Tscherning (1942--2014),
professor of Geodesy at the University of Copenhagen, mentor and advisor
for a generation of Danish geodesists, colleague and collaborator for
two generations of global geodesists, Secretary General for the
International Association of Geodesy, IAG (1995--2007), fellow of the
American Geophysical Union (1991), recipient of the IAG Levallois Medal
(2007), the European Geosciences Union Vening Meinesz Medal (2008), and
of numerous other honours.

cct, or Christian, as he was known to most of us, was recognized for his
good mood, his sharp wit, his tireless work, and his great commitment to
the development of geodesy - both through his scientific contributions,
comprising more than 250 publications, and by his mentoring and teaching
of the next generations of geodesists.

As Christian was an avid Fortran programmer, and a keen Unix connoisseur,
he would have enjoyed to know that his initials would be used to name a
modest Unix style transformation filter, hinting at the tireless aspect
of his personality, which was certainly one of the reasons he accomplished
so much, and meant so much to so many people.

Hence, in honour of cct (the geodesist) this is cct (the program).

************************************************************************

Thomas Knudsen, thokn@sdfe.dk, 2016-05-25/2017-10-26

************************************************************************

* Copyright (c) 2016, 2017 Thomas Knudsen
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
#include <math.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <algorithm>
#include <cstdint>
#include <fstream> // std::ifstream
#include <iostream>

#include "optargpm.h"
#include "proj.h"
#include "proj_internal.h"
#include "proj_strtod.h"

static void logger(void *data, int level, const char *msg);
static void print(PJ_LOG_LEVEL log_level, const char *fmt, ...);

/* Prototypes from functions in this file */
static const char *column(const char *buf, int n);
static char *column(char *buf, int n);
PJ_COORD parse_input_line(const char *buf, int *columns, double fixed_height,
                          double fixed_time);

static const char usage[] = {
    "--------------------------------------------------------------------------"
    "------\n"
    "Usage: %s [-options]... [+operator_specs]... infile...\n"
    "--------------------------------------------------------------------------"
    "------\n"
    "Options:\n"
    "--------------------------------------------------------------------------"
    "------\n"
    "    -c x,y,z,t        Specify input columns for (up to) 4 input "
    "parameters.\n"
    "                      Defaults to 1,2,3,4\n"
    "    -d n              Specify number of decimals in output.\n"
    "    -I                Do the inverse transformation\n"
    "    -o /path/to/file  Specify output file name\n"
    "    -t value          Provide a fixed t value for all input data (e.g. -t "
    "0)\n"
    "    -z value          Provide a fixed z value for all input data (e.g. -z "
    "0)\n"
    "    -s n              Skip n first lines of a infile\n"
    "    -v                Verbose: Provide non-essential informational "
    "output.\n"
    "                      Repeat -v for more verbosity (e.g. -vv)\n"
    "--------------------------------------------------------------------------"
    "------\n"
    "Long Options:\n"
    "--------------------------------------------------------------------------"
    "------\n"
    "    --output          Alias for -o\n"
    "    --columns         Alias for -c\n"
    "    --decimals        Alias for -d\n"
    "    --height          Alias for -z\n"
    "    --time            Alias for -t\n"
    "    --verbose         Alias for -v\n"
    "    --inverse         Alias for -I\n"
    "    --skip-lines      Alias for -s\n"
    "    --help            Alias for -h\n"
    "    --version         Print version number\n"
    "--------------------------------------------------------------------------"
    "------\n"
    "Operator Specs:\n"
    "--------------------------------------------------------------------------"
    "------\n"
    "The operator specs describe the action to be performed by cct, e.g:\n"
    "\n"
    "        +proj=utm  +ellps=GRS80  +zone=32\n"
    "\n"
    "instructs cct to convert input data to Universal Transverse Mercator, "
    "zone 32\n"
    "coordinates, based on the GRS80 ellipsoid.\n"
    "\n"
    "Hence, the command\n"
    "\n"
    "        echo 12 55 | cct -z0 -t0 +proj=utm +zone=32 +ellps=GRS80\n"
    "\n"
    "Should give results comparable to the classic proj command\n"
    "\n"
    "        echo 12 55 | proj +proj=utm +zone=32 +ellps=GRS80\n"
    "--------------------------------------------------------------------------"
    "------\n"
    "Examples:\n"
    "--------------------------------------------------------------------------"
    "------\n"
    "1. convert geographical input to UTM zone 32 on the GRS80 ellipsoid:\n"
    "    cct +proj=utm +ellps=GRS80 +zone=32\n"
    "2. roundtrip accuracy check for the case above:\n"
    "    cct +proj=pipeline +ellps=GRS80 +zone=32 +step +proj=utm \\\n"
    "        +step +proj=utm +inv\n"
    "3. as (1) but specify input columns for longitude, latitude, height and "
    "time:\n"
    "    cct -c 5,2,1,4  +proj=utm +ellps=GRS80 +zone=32\n"
    "4. as (1) but specify fixed height and time, hence needing only 2 cols in "
    "input:\n"
    "    cct -t 0 -z 0  +proj=utm  +ellps=GRS80  +zone=32\n"
    "--------------------------------------------------------------------------"
    "------\n"};

static void logger(void *data, int level, const char *msg) {
    FILE *stream;
    int log_tell = proj_log_level(PJ_DEFAULT_CTX, PJ_LOG_TELL);

    stream = (FILE *)data;

    /* if we use PJ_LOG_NONE we always want to print stuff to stream */
    if (level == PJ_LOG_NONE) {
        fprintf(stream, "%s\n", msg);
        return;
    }

    /* otherwise only print if log level set by user is high enough or error */
    if (level <= log_tell || level == PJ_LOG_ERROR)
        fprintf(stderr, "%s\n", msg);
}

FILE *fout;

static void print(PJ_LOG_LEVEL log_level, const char *fmt, ...) {

    va_list args;
    char *msg_buf;

    va_start(args, fmt);

    const size_t msg_buf_size = 100000;
    msg_buf = (char *)malloc(msg_buf_size);
    if (msg_buf == nullptr) {
        va_end(args);
        return;
    }

    vsnprintf(msg_buf, msg_buf_size, fmt, args);

    logger((void *)fout, log_level, msg_buf);

    va_end(args);
    free(msg_buf);
}

int main(int argc, char **argv) {
    PJ *P = nullptr;
    PJ_COORD point;
    PJ_PROJ_INFO info;
    OPTARGS *o;
    char blank_comment[] = "";
    char whitespace[] = " ";
    int i, nfields = 4, skip_lines = 0, verbose;
    double fixed_z = HUGE_VAL, fixed_time = HUGE_VAL;
    int decimals_angles = 10;
    int decimals_distances = 4;
    int columns_xyzt[] = {1, 2, 3, 4};
    const char *longflags[] = {"v=verbose", "h=help", "I=inverse", "version",
                               nullptr};
    const char *longkeys[] = {"o=output", "c=columns", "d=decimals",
                              "z=height", "t=time",    "s=skip-lines",
                              nullptr};

    fout = stdout;

    pj_stderr_proj_lib_deprecation_warning();

    /* coverity[tainted_data] */
    o = opt_parse(argc, argv, "hvI", "cdozts", longflags, longkeys);
    if (nullptr == o)
        return 1;

    if (opt_given(o, "h") || argc == 1) {
        printf(usage, o->progname);
        free(o);
        return 0;
    }

    PJ_DIRECTION direction = opt_given(o, "I") ? PJ_INV : PJ_FWD;

    verbose =
        std::min(opt_given(o, "v"), 3); /* log level can't be larger than 3 */
    if (verbose > 0) {
        proj_log_level(PJ_DEFAULT_CTX, static_cast<PJ_LOG_LEVEL>(verbose));
    }
    proj_log_func(PJ_DEFAULT_CTX, (void *)fout, logger);

    if (opt_given(o, "version")) {
        print(PJ_LOG_NONE, "%s: %s", o->progname, pj_get_release());
        free(o);
        return 0;
    }

    if (opt_given(o, "o"))
        fout = fopen(opt_arg(o, "output"), "wt");
    if (nullptr == fout) {
        print(PJ_LOG_ERROR, "%s: Cannot open '%s' for output", o->progname,
              opt_arg(o, "output"));
        free(o);
        return 1;
    }

    print(PJ_LOG_TRACE, "%s: Running in very verbose mode", o->progname);

    if (opt_given(o, "z")) {
        fixed_z = proj_atof(opt_arg(o, "z"));
        nfields--;
    }

    if (opt_given(o, "t")) {
        fixed_time = proj_atof(opt_arg(o, "t"));
        nfields--;
    }

    if (opt_given(o, "d")) {
        int dec = atoi(opt_arg(o, "d"));
        decimals_angles = dec;
        decimals_distances = dec;
    }

    if (opt_given(o, "s")) {
        skip_lines = atoi(opt_arg(o, "s"));
    }

    if (opt_given(o, "c")) {
        int ncols;
        /* reset column numbers to ease comment output later on */
        for (i = 0; i < 4; i++)
            columns_xyzt[i] = 0;

        /* cppcheck-suppress invalidscanf */
        ncols = sscanf(opt_arg(o, "c"), "%d,%d,%d,%d", columns_xyzt,
                       columns_xyzt + 1, columns_xyzt + 2, columns_xyzt + 3);
        if (ncols != nfields) {
            print(PJ_LOG_ERROR, "%s: Too few input columns given: '%s'",
                  o->progname, opt_arg(o, "c"));
            free(o);
            if (stdout != fout)
                fclose(fout);
            return 1;
        }
    }

    /* Setup transformation */
    if (o->pargc == 0 && o->fargc > 0) {
        std::string input(o->fargv[0]);

        if (!input.empty() && input[0] == '@') {
            std::ifstream fs;
            auto filename = input.substr(1);
            fs.open(filename, std::fstream::in | std::fstream::binary);
            if (!fs.is_open()) {
                std::cerr << "cannot open " << filename << std::endl;
                std::exit(1);
            }
            input.clear();
            while (!fs.eof()) {
                char buffer[256];
                fs.read(buffer, sizeof(buffer));
                input.append(buffer, static_cast<size_t>(fs.gcount()));
                if (input.size() > 100 * 1000) {
                    fs.close();
                    std::cerr << "too big file " << filename << std::endl;
                    std::exit(1);
                }
            }
            fs.close();
        }

        /* Assume we got a auth:code combination */
        auto n = input.find(":");
        if (n > 0) {
            std::string auth = input.substr(0, n);
            std::string code = input.substr(n + 1, input.length());
            // Check that the authority matches one of the known ones
            auto authorityList = proj_get_authorities_from_database(nullptr);
            if (authorityList) {
                for (auto iter = authorityList; *iter; iter++) {
                    if (*iter == auth) {
                        P = proj_create_from_database(
                            nullptr, auth.c_str(), code.c_str(),
                            PJ_CATEGORY_COORDINATE_OPERATION, 0, nullptr);
                        break;
                    }
                }
                proj_string_list_destroy(authorityList);
            }
        }
        if (P == nullptr) {
            /* if we didn't get a auth:code combo we try to see if the input
             * matches
             */
            /* anything else */
            P = proj_create(nullptr, input.c_str());
            if (P) {
                const auto type = proj_get_type(P);
                switch (type) {
                case PJ_TYPE_CONVERSION:
                case PJ_TYPE_TRANSFORMATION:
                case PJ_TYPE_CONCATENATED_OPERATION:
                case PJ_TYPE_OTHER_COORDINATE_OPERATION:
                    // ok;
                    break;
                default:
                    print(PJ_LOG_ERROR,
                          "%s: Input object is not a coordinate operation%s.",
                          o->progname, proj_is_crs(P) ? ", but a CRS" : "");
                    free(o);
                    proj_destroy(P);
                    if (stdout != fout)
                        fclose(fout);
                    return 1;
                }
            }
        }

        /* If instantiating operation without +-options optargpm thinks the
         * input is
         */
        /* a file, hence we move all o->fargv entries one place closer to the
         * start
         */
        /* of the array. This effectively overwrites the input and only leaves a
         * list */
        /* of files in o->fargv. */
        o->fargc = o->fargc - 1;
        for (int j = 0; j < o->fargc; j++) {
            o->fargv[j] = o->fargv[j + 1];
        }
    } else {
        P = proj_create_argv(nullptr, o->pargc, o->pargv);
    }

    if (nullptr == P) {
        print(PJ_LOG_ERROR,
              "%s: Bad transformation arguments - (%s)\n    '%s -h' for help",
              o->progname, proj_errno_string(proj_errno(P)), o->progname);
        free(o);
        if (stdout != fout)
            fclose(fout);
        return 1;
    }

    info = proj_pj_info(P);
    print(PJ_LOG_TRACE, "Final: %s argc=%d pargc=%d", info.definition, argc,
          o->pargc);

    if (direction == PJ_INV) {
        /* fail if an inverse operation is not available */
        if (!info.has_inverse) {
            print(PJ_LOG_ERROR, "Inverse operation not available");
            free(o);
            if (stdout != fout)
                fclose(fout);
            return 1;
        }
        /* We have no API call for inverting an operation, so we brute force it.
         */
        P->inverted = !(P->inverted);
    }
    direction = PJ_FWD;

    /* Allocate input buffer */
    constexpr int BUFFER_SIZE = 10000;
    char *buf = static_cast<char *>(calloc(1, BUFFER_SIZE));
    if (nullptr == buf) {
        print(PJ_LOG_ERROR, "%s: Out of memory", o->progname);
        proj_destroy(P);
        free(o);
        if (stdout != fout)
            fclose(fout);
        return 1;
    }

    /* Loop over all records of all input files */
    int previous_index = -1;
    bool gotError = false;
    while (opt_input_loop(o, optargs_file_format_text, &gotError)) {
        int err;
        char *bufptr = fgets(buf, BUFFER_SIZE - 1, o->input);
        if (opt_eof(o)) {
            continue;
        }
        if (nullptr == bufptr) {
            print(PJ_LOG_ERROR, "Read error in record %d",
                  (int)o->record_index);
            continue;
        }

        const bool bFirstLine = o->input_index != previous_index;
        previous_index = o->input_index;
        if (bFirstLine && static_cast<uint8_t>(bufptr[0]) == 0xEF &&
            static_cast<uint8_t>(bufptr[1]) == 0xBB &&
            static_cast<uint8_t>(bufptr[2]) == 0xBF) {
            // Skip UTF-8 Byte Order Marker (BOM)
            bufptr += 3;
        }

        point = parse_input_line(bufptr, columns_xyzt, fixed_z, fixed_time);
        if (skip_lines > 0) {
            skip_lines--;
            continue;
        }

        /* if it's a comment or blank line, we reflect it */
        const char *c = column(bufptr, 1);
        if (c && ((*c == '\0') || (*c == '#'))) {
            fprintf(fout, "%s", bufptr);
            continue;
        }

        if (HUGE_VAL == point.xyzt.x) {
            /* otherwise, it must be a syntax error */
            print(PJ_LOG_NONE, "# Record %d UNREADABLE: %s",
                  (int)o->record_index, bufptr);
            print(PJ_LOG_ERROR, "%s: Could not parse file '%s' line %d",
                  o->progname, opt_filename(o), opt_record(o));
            continue;
        }

        if (proj_angular_input(P, direction)) {
            point.lpzt.lam = proj_torad(point.lpzt.lam);
            point.lpzt.phi = proj_torad(point.lpzt.phi);
        }
        err = proj_errno_reset(P);
        /* coverity[returned_value] */
        point = proj_trans(P, direction, point);

        if (HUGE_VAL == point.xyzt.x) {
            /* transformation error */
            print(PJ_LOG_NONE, "# Record %d TRANSFORMATION ERROR: %s (%s)",
                  (int)o->record_index, bufptr,
                  proj_errno_string(proj_errno(P)));
            proj_errno_restore(P, err);
            continue;
        }
        proj_errno_restore(P, err);

        /* handle comment string */
        char *comment = column(bufptr, nfields + 1);
        if (opt_given(o, "c")) {
            /* what number is the last coordinate column in the input data? */
            int colmax = 0;
            for (i = 0; i < 4; i++)
                colmax = MAX(colmax, columns_xyzt[i]);
            comment = column(bufptr, colmax + 1);
        }
        /* remove the line feed from comment, as logger() above, invoked
           by print() below (output), will add one */
        size_t len = strlen(comment);
        if (len >= 1)
            comment[len - 1] = '\0';
        const char *comment_delimiter = *comment ? whitespace : blank_comment;

        /* Time to print the result */
        /* use same arguments to printf format string for both radians and
           degrees; convert radians to degrees before printing */
        if (proj_angular_output(P, direction) ||
            proj_degree_output(P, direction)) {
            if (proj_angular_output(P, direction)) {
                point.lpzt.lam = proj_todeg(point.lpzt.lam);
                point.lpzt.phi = proj_todeg(point.lpzt.phi);
            }
            print(PJ_LOG_NONE, "%14.*f  %14.*f  %12.*f  %12.4f%s%s",
                  decimals_angles, point.xyzt.x, decimals_angles, point.xyzt.y,
                  decimals_distances, point.xyzt.z, point.xyzt.t,
                  comment_delimiter, comment);
        } else
            print(PJ_LOG_NONE, "%13.*f  %13.*f  %12.*f  %12.4f%s%s",
                  decimals_distances, point.xyzt.x, decimals_distances,
                  point.xyzt.y, decimals_distances, point.xyzt.z, point.xyzt.t,
                  comment_delimiter, comment);
        if (fout == stdout)
            fflush(stdout);
    }

    proj_destroy(P);

    if (stdout != fout)
        fclose(fout);
    free(o);
    free(buf);
    return gotError ? 1 : 0;
}

/* return a pointer to the n'th column of buf */
static const char *column(const char *buf, int n) {
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

static char *column(char *buf, int n) {
    return const_cast<char *>(column(const_cast<const char *>(buf), n));
}

/* column to double */
static double cold(const char *args, int col) {
    char *endp;
    double d;
    const char *target = column(args, col);
    d = proj_strtod(target, &endp);
    if (endp == target)
        return HUGE_VAL;
    return d;
}

PJ_COORD parse_input_line(const char *buf, int *columns, double fixed_height,
                          double fixed_time) {
    PJ_COORD err = proj_coord(HUGE_VAL, HUGE_VAL, HUGE_VAL, HUGE_VAL);
    PJ_COORD result = err;
    int prev_errno = errno;
    errno = 0;

    result.xyzt.z = fixed_height;
    result.xyzt.t = fixed_time;
    result.xyzt.x = cold(buf, columns[0]);
    result.xyzt.y = cold(buf, columns[1]);
    if (result.xyzt.z == HUGE_VAL)
        result.xyzt.z = cold(buf, columns[2]);
    if (result.xyzt.t == HUGE_VAL)
        result.xyzt.t = cold(buf, columns[3]);

    if (0 != errno)
        return err;

    errno = prev_errno;
    return result;
}
