/******************************************************************************
 * Project:  PROJ.4
 * Purpose:  Mainline program sort of like ``proj'' for converting between
 *           two coordinate systems.
 * Author:   Frank Warmerdam, warmerda@home.com
 *
 ******************************************************************************
 * Copyright (c) 2000, Frank Warmerdam
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
 *****************************************************************************/

#define FROM_PROJ_CPP

#include <ctype.h>
#include <locale.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <cassert>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include <proj/io.hpp>
#include <proj/metadata.hpp>
#include <proj/util.hpp>

#include <proj/internal/internal.hpp>

// PROJ include order is sensitive
// clang-format off
#include "proj.h"
#include "proj_experimental.h"
#include "proj_internal.h"
#include "emess.h"
#include "utils.h"
// clang-format on

#define MAX_LINE 1000

static PJ *transformation = nullptr;

static bool srcIsLongLat = false;
static double srcToRadians = 0.0;

static bool destIsLongLat = false;
static double destToRadians = 0.0;
static bool destIsLatLong = false;

static int reversein = 0, /* != 0 reverse input arguments */
    reverseout = 0,       /* != 0 reverse output arguments */
    echoin = 0,           /* echo input data to output line */
    tag = '#';            /* beginning of line tag character */

static const char *oform =
    nullptr;                  /* output format for x-y or decimal degrees */
static char oform_buffer[16]; /* buffer for oform when using -d */
static const char *oterr = "*\t*"; /* output line for unprojectable input */
static const char *usage =
    "%s\nusage: %s [-dDeEfIlrstvwW [args]]\n"
    "              [[--area name_or_code] | [--bbox "
    "west_long,south_lat,east_long,north_lat]]\n"
    "              [--authority {name}] [--3d]\n"
    "              [--accuracy {accuracy}] [--only-best[=yes|=no]] "
    "[--no-ballpark]\n"
    "              [--s_epoch {epoch}] [--t_epoch {epoch}]\n"
    "              [+opt[=arg] ...] [+to +opt[=arg] ...] [file ...]\n";

static double (*informat)(const char *,
                          char **); /* input data deformatter function */

using namespace NS_PROJ::io;
using namespace NS_PROJ::metadata;
using namespace NS_PROJ::util;
using namespace NS_PROJ::internal;

/************************************************************************/
/*                              process()                               */
/*                                                                      */
/*      File processing function.                                       */
/************************************************************************/
static void process(FILE *fid)

{
    char line[MAX_LINE + 3], *s, pline[40];
    PJ_UV data;
    int nLineNumber = 0;

    while (true) {
        double z;
        ++nLineNumber;
        ++emess_dat.File_line;
        if (!(s = fgets(line, MAX_LINE, fid)))
            break;

        if (nLineNumber == 1 && static_cast<uint8_t>(s[0]) == 0xEF &&
            static_cast<uint8_t>(s[1]) == 0xBB &&
            static_cast<uint8_t>(s[2]) == 0xBF) {
            // Skip UTF-8 Byte Order Marker (BOM)
            s += 3;
        }
        const char *pszLineAfterBOM = s;

        if (!strchr(s, '\n')) { /* overlong line */
            int c;
            (void)strcat(s, "\n");
            /* gobble up to newline */
            while ((c = fgetc(fid)) != EOF && c != '\n')
                ;
        }
        if (*s == tag) {
            fputs(line, stdout);
            continue;
        }

        if (reversein) {
            data.v = (*informat)(s, &s);
            data.u = (*informat)(s, &s);
        } else {
            data.u = (*informat)(s, &s);
            data.v = (*informat)(s, &s);
        }

        z = strtod(s, &s);

        /* To avoid breaking existing tests, we read what is a possible t    */
        /* component of the input and rewind the s-pointer so that the final */
        /* output has consistent behavior, with or without t values.        */
        /* This is a bit of a hack, in most cases 4D coordinates will be     */
        /* written to STDOUT (except when using -E) but the output format    */
        /* specified with -f is not respected for the t component, rather it */
        /* is forward verbatim from the input.                               */
        char *before_time = s;
        double t = strtod(s, &s);
        if (s == before_time)
            t = HUGE_VAL;
        s = before_time;

        if (data.v == HUGE_VAL)
            data.u = HUGE_VAL;

        if (!*s && (s > line))
            --s; /* assumed we gobbled \n */

        if (echoin) {
            char temp;
            temp = *s;
            *s = '\0';
            (void)fputs(pszLineAfterBOM, stdout);
            *s = temp;
            putchar('\t');
        }

        if (data.u != HUGE_VAL) {

            if (srcIsLongLat && fabs(srcToRadians - M_PI / 180) < 1e-10) {
                /* dmstor gives values to radians. Convert now to the SRS unit
                 */
                data.u /= srcToRadians;
                data.v /= srcToRadians;
            }

            PJ_COORD coord;
            coord.xyzt.x = data.u;
            coord.xyzt.y = data.v;
            coord.xyzt.z = z;
            coord.xyzt.t = t;
            coord = proj_trans(transformation, PJ_FWD, coord);
            data.u = coord.xyz.x;
            data.v = coord.xyz.y;
            z = coord.xyz.z;
        }

        if (data.u == HUGE_VAL) /* error output */
            fputs(oterr, stdout);

        else if (destIsLongLat && !oform) { /*ascii DMS output */

            // rtodms() expect radians: convert from the output SRS unit
            data.u *= destToRadians;
            data.v *= destToRadians;

            if (destIsLatLong) {
                if (reverseout) {
                    fputs(rtodms(pline, sizeof(pline), data.v, 'E', 'W'),
                          stdout);
                    putchar('\t');
                    fputs(rtodms(pline, sizeof(pline), data.u, 'N', 'S'),
                          stdout);
                } else {
                    fputs(rtodms(pline, sizeof(pline), data.u, 'N', 'S'),
                          stdout);
                    putchar('\t');
                    fputs(rtodms(pline, sizeof(pline), data.v, 'E', 'W'),
                          stdout);
                }
            } else if (reverseout) {
                fputs(rtodms(pline, sizeof(pline), data.v, 'N', 'S'), stdout);
                putchar('\t');
                fputs(rtodms(pline, sizeof(pline), data.u, 'E', 'W'), stdout);
            } else {
                fputs(rtodms(pline, sizeof(pline), data.u, 'E', 'W'), stdout);
                putchar('\t');
                fputs(rtodms(pline, sizeof(pline), data.v, 'N', 'S'), stdout);
            }

        } else { /* x-y or decimal degree ascii output */
            if (destIsLongLat) {
                data.v *= destToRadians * RAD_TO_DEG;
                data.u *= destToRadians * RAD_TO_DEG;
            }
            if (reverseout) {
                limited_fprintf_for_number(stdout, oform, data.v);
                putchar('\t');
                limited_fprintf_for_number(stdout, oform, data.u);
            } else {
                limited_fprintf_for_number(stdout, oform, data.u);
                putchar('\t');
                limited_fprintf_for_number(stdout, oform, data.v);
            }
        }

        putchar(' ');
        if (oform != nullptr)
            limited_fprintf_for_number(stdout, oform, z);
        else
            printf("%.3f", z);
        if (s)
            printf("%s", s);
        else
            printf("\n");
        fflush(stdout);
    }
}

/************************************************************************/
/*                          instantiate_crs()                           */
/************************************************************************/

static PJ *instantiate_crs(const PJ *crs_in, bool &isLongLatCS,
                           double &toRadians, bool &isLatFirst) {

    PJ *crs = nullptr;
    isLongLatCS = false;
    toRadians = 0.0;
    isLatFirst = false;

    auto type = proj_get_type(crs_in);
    if (type == PJ_TYPE_BOUND_CRS) {
        crs = proj_get_source_crs(nullptr, crs_in);
        type = proj_get_type(crs);
    } else {
        crs = proj_clone(nullptr, crs_in);
    }
    if (type == PJ_TYPE_GEOGRAPHIC_2D_CRS ||
        type == PJ_TYPE_GEOGRAPHIC_3D_CRS || type == PJ_TYPE_GEODETIC_CRS) {
        auto cs = proj_crs_get_coordinate_system(nullptr, crs);
        assert(cs);

        const char *axisName = "";
        proj_cs_get_axis_info(nullptr, cs, 0,
                              &axisName, // name,
                              nullptr,   // abbrev
                              nullptr,   // direction
                              &toRadians,
                              nullptr, // unit name
                              nullptr, // unit authority
                              nullptr  // unit code
        );
        isLatFirst =
            NS_PROJ::internal::ci_find(std::string(axisName), "latitude") !=
            std::string::npos;
        isLongLatCS = isLatFirst || NS_PROJ::internal::ci_find(
                                        std::string(axisName), "longitude") !=
                                        std::string::npos;

        proj_destroy(cs);
    }

    return crs;
}

/************************************************************************/
/*               get_geog_crs_proj_string_from_proj_crs()               */
/************************************************************************/

static PJ *get_geog_crs_proj_string_from_proj_crs(const PJ *src,
                                                  double &toRadians,
                                                  bool &isLatFirst) {
    auto srcType = proj_get_type(src);
    if (srcType != PJ_TYPE_PROJECTED_CRS) {
        return nullptr;
    }

    auto base = proj_get_source_crs(nullptr, src);
    assert(base);
    auto baseType = proj_get_type(base);
    if (baseType != PJ_TYPE_GEOGRAPHIC_2D_CRS &&
        baseType != PJ_TYPE_GEOGRAPHIC_3D_CRS) {
        proj_destroy(base);
        return nullptr;
    }

    auto cs = proj_crs_get_coordinate_system(nullptr, base);
    assert(cs);

    const char *axisName = "";
    proj_cs_get_axis_info(nullptr, cs, 0,
                          &axisName, // name,
                          nullptr,   // abbrev
                          nullptr,   // direction
                          &toRadians,
                          nullptr, // unit name
                          nullptr, // unit authority
                          nullptr  // unit code
    );
    isLatFirst = NS_PROJ::internal::ci_find(std::string(axisName),
                                            "latitude") != std::string::npos;

    proj_destroy(cs);

    return base;
}

// ---------------------------------------------------------------------------

static bool is3DCRS(const PJ *crs) {
    auto type = proj_get_type(crs);
    if (type == PJ_TYPE_COMPOUND_CRS)
        return true;
    if (type == PJ_TYPE_GEOGRAPHIC_3D_CRS)
        return true;
    if (type == PJ_TYPE_GEODETIC_CRS || type == PJ_TYPE_PROJECTED_CRS ||
        type == PJ_TYPE_DERIVED_PROJECTED_CRS) {
        auto cs = proj_crs_get_coordinate_system(nullptr, crs);
        assert(cs);
        const bool ret = proj_cs_get_axis_count(nullptr, cs) == 3;
        proj_destroy(cs);
        return ret;
    }
    return false;
}

/************************************************************************/
/*                                main()                                */
/************************************************************************/

int main(int argc, char **argv) {
    char *arg;
    char **eargv = argv;
    std::string fromStr;
    std::string toStr;
    FILE *fid;
    int eargc = 0, mon = 0;
    int have_to_flag = 0, inverse = 0;
    int use_env_locale = 0;

    pj_stderr_proj_lib_deprecation_warning();

    if (argc == 0) {
        exit(1);
    }

    /* This is just to check that pj_init() is locale-safe */
    /* Used by test/cli/test_cs2cs_locale.sh */
    if (getenv("PROJ_USE_ENV_LOCALE") != nullptr)
        use_env_locale = 1;

    /* Enable compatibility mode for init=epsg:XXXX by default */
    if (getenv("PROJ_USE_PROJ4_INIT_RULES") == nullptr) {
        proj_context_use_proj4_init_rules(nullptr, true);
    }

    if ((emess_dat.Prog_name = strrchr(*argv, DIR_CHAR)) != nullptr)
        ++emess_dat.Prog_name;
    else
        emess_dat.Prog_name = *argv;
    inverse = !strncmp(emess_dat.Prog_name, "inv", 3);
    if (argc <= 1) {
        (void)fprintf(stderr, usage, pj_get_release(), emess_dat.Prog_name);
        exit(0);
    }

    // First pass to check if we have "cs2cs [-bla]* <SRC> <DEST> [<filename>]"
    // syntax
    bool isProj4StyleSyntax = false;
    for (int i = 1; i < argc; i++) {
        if (argv[i][0] == '+') {
            isProj4StyleSyntax = true;
            break;
        }
    }

    ExtentPtr bboxFilter;
    std::string area;
    const char *authority = nullptr;
    double accuracy = -1;
    bool allowBallpark = true;
    bool onlyBestSet = false;
    bool errorIfBestTransformationNotAvailable = false;
    bool promoteTo3D = false;
    std::string sourceEpoch;
    std::string targetEpoch;

    /* process run line arguments */
    while (--argc > 0) { /* collect run line arguments */
        ++argv;
        if (strcmp(*argv, "--area") == 0) {
            ++argv;
            --argc;
            if (argc == 0) {
                emess(1, "missing argument for --area");
                std::exit(1);
            }
            area = *argv;
        } else if (strcmp(*argv, "--bbox") == 0) {
            ++argv;
            --argc;
            if (argc == 0) {
                emess(1, "missing argument for --bbox");
                std::exit(1);
            }
            auto bboxStr(*argv);
            auto bbox(split(bboxStr, ','));
            if (bbox.size() != 4) {
                std::cerr << "Incorrect number of values for option --bbox: "
                          << bboxStr << std::endl;
                std::exit(1);
            }
            try {
                std::vector<double> bboxValues = {
                    c_locale_stod(bbox[0]), c_locale_stod(bbox[1]),
                    c_locale_stod(bbox[2]), c_locale_stod(bbox[3])};
                const double west = bboxValues[0];
                const double south = bboxValues[1];
                const double east = bboxValues[2];
                const double north = bboxValues[3];
                constexpr double SOME_MARGIN = 10;
                if (south < -90 - SOME_MARGIN && std::fabs(west) <= 90 &&
                    std::fabs(east) <= 90)
                    std::cerr << "Warning: suspicious south latitude: " << south
                              << std::endl;
                if (north > 90 + SOME_MARGIN && std::fabs(west) <= 90 &&
                    std::fabs(east) <= 90)
                    std::cerr << "Warning: suspicious north latitude: " << north
                              << std::endl;
                bboxFilter = Extent::createFromBBOX(west, south, east, north)
                                 .as_nullable();
            } catch (const std::exception &e) {
                std::cerr << "Invalid value for option --bbox: " << bboxStr
                          << ", " << e.what() << std::endl;
                std::exit(1);
            }
        } else if (strcmp(*argv, "--accuracy") == 0) {
            ++argv;
            --argc;
            if (argc == 0) {
                emess(1, "missing argument for --accuracy");
                std::exit(1);
            }
            try {
                accuracy = c_locale_stod(*argv);
            } catch (const std::exception &e) {
                std::cerr << "Invalid value for option --accuracy: " << e.what()
                          << std::endl;
                std::exit(1);
            }
        } else if (strcmp(*argv, "--authority") == 0) {
            ++argv;
            --argc;
            if (argc == 0) {
                emess(1, "missing argument for --authority");
                std::exit(1);
            }
            authority = *argv;
        } else if (strcmp(*argv, "--no-ballpark") == 0) {
            allowBallpark = false;
        } else if (strcmp(*argv, "--only-best") == 0 ||
                   strcmp(*argv, "--only-best=yes") == 0) {
            onlyBestSet = true;
            errorIfBestTransformationNotAvailable = true;
        } else if (strcmp(*argv, "--only-best=no") == 0) {
            onlyBestSet = true;
            errorIfBestTransformationNotAvailable = false;
        } else if (strcmp(*argv, "--3d") == 0) {
            promoteTo3D = true;
        } else if (strcmp(*argv, "--s_epoch") == 0) {
            ++argv;
            --argc;
            if (argc == 0) {
                emess(1, "missing argument for --s_epoch");
                std::exit(1);
            }
            sourceEpoch = *argv;
        } else if (strcmp(*argv, "--t_epoch") == 0) {
            ++argv;
            --argc;
            if (argc == 0) {
                emess(1, "missing argument for --t_epoch");
                std::exit(1);
            }
            targetEpoch = *argv;
        } else if (**argv == '-') {
            for (arg = *argv;;) {
                switch (*++arg) {
                case '\0': /* position of "stdin" */
                    if (arg[-1] == '-')
                        eargv[eargc++] = const_cast<char *>("-");
                    break;
                case 'v': /* monitor dump of initialization */
                    mon = 1;
                    continue;
                case 'I': /* alt. method to spec inverse */
                    inverse = 1;
                    continue;
                case 'E': /* echo ascii input to ascii output */
                    echoin = 1;
                    continue;
                case 't': /* set col. one char */
                    if (arg[1])
                        tag = *++arg;
                    else
                        emess(1, "missing -t col. 1 tag");
                    continue;
                case 'l': /* list projections, ellipses or units */
                    if (!arg[1] || arg[1] == 'p' || arg[1] == 'P') {
                        /* list projections */
                        const struct PJ_LIST *lp;
                        int do_long = arg[1] == 'P', c;
                        const char *str;

                        for (lp = proj_list_operations(); lp->id; ++lp) {
                            (void)printf("%s : ", lp->id);
                            if (do_long) /* possibly multiline description */
                                (void)puts(*lp->descr);
                            else { /* first line, only */
                                str = *lp->descr;
                                while ((c = *str++) && c != '\n')
                                    putchar(c);
                                putchar('\n');
                            }
                        }
                    } else if (arg[1] == '=') { /* list projection 'descr' */
                        const struct PJ_LIST *lp;

                        arg += 2;
                        for (lp = proj_list_operations(); lp->id; ++lp)
                            if (!strcmp(lp->id, arg)) {
                                (void)printf("%9s : %s\n", lp->id, *lp->descr);
                                break;
                            }
                    } else if (arg[1] == 'e') { /* list ellipses */
                        const struct PJ_ELLPS *le;

                        for (le = proj_list_ellps(); le->id; ++le)
                            (void)printf("%9s %-16s %-16s %s\n", le->id,
                                         le->major, le->ell, le->name);
                    } else if (arg[1] == 'u') { /* list units */
                        auto units = proj_get_units_from_database(
                            nullptr, nullptr, "linear", false, nullptr);
                        for (int i = 0; units && units[i]; i++) {
                            if (units[i]->proj_short_name) {
                                (void)printf("%12s %-20.15g %s\n",
                                             units[i]->proj_short_name,
                                             units[i]->conv_factor,
                                             units[i]->name);
                            }
                        }
                        proj_unit_list_destroy(units);
                    } else if (arg[1] == 'm') { /* list prime meridians */
                        (void)fprintf(stderr, "This list is no longer updated, "
                                              "and some values may "
                                              "conflict with other sources.\n");
                        const struct PJ_PRIME_MERIDIANS *lpm;
                        for (lpm = proj_list_prime_meridians(); lpm->id; ++lpm)
                            (void)printf("%12s %-30s\n", lpm->id, lpm->defn);
                    } else
                        emess(1, "invalid list option: l%c", arg[1]);
                    exit(0);
                    /* cppcheck-suppress duplicateBreak */
                    continue; /* artificial */
                case 'e':     /* error line alternative */
                    if (--argc <= 0)
                    noargument:
                        emess(1, "missing argument for -%c", *arg);
                    oterr = *++argv;
                    continue;
                case 'W': /* specify seconds precision */
                case 'w': /* -W for constant field width */
                {
                    char c = arg[1];
                    // Check that the value is in the [0, 8] range
                    if (c >= '0' && c <= '8' &&
                        ((arg[2] == 0 || !(arg[2] >= '0' && arg[2] <= '9')))) {
                        set_rtodms(c - '0', *arg == 'W');
                        ++arg;
                    } else
                        emess(1, "-W argument missing or not in range [0,8]");
                    continue;
                }
                case 'f': /* alternate output format degrees or xy */
                    if (--argc <= 0)
                        goto noargument;
                    oform = *++argv;
                    continue;
                case 'r': /* reverse input */
                    reversein = 1;
                    continue;
                case 's': /* reverse output */
                    reverseout = 1;
                    continue;
                case 'D': /* set debug level */
                {
                    if (--argc <= 0)
                        goto noargument;
                    int log_level = atoi(*++argv);
                    if (log_level <= 0) {
                        proj_log_level(pj_get_default_ctx(), PJ_LOG_NONE);
                    } else if (log_level == 1) {
                        proj_log_level(pj_get_default_ctx(), PJ_LOG_ERROR);
                    } else if (log_level == 2) {
                        proj_log_level(pj_get_default_ctx(), PJ_LOG_DEBUG);
                    } else if (log_level == 3) {
                        proj_log_level(pj_get_default_ctx(), PJ_LOG_TRACE);
                    } else {
                        proj_log_level(pj_get_default_ctx(), PJ_LOG_TELL);
                    }
                    continue;
                }
                case 'd':
                    if (--argc <= 0)
                        goto noargument;
                    snprintf(oform_buffer, sizeof(oform_buffer), "%%.%df",
                             atoi(*++argv));
                    oform = oform_buffer;
                    break;
                default:
                    emess(1, "invalid option: -%c", *arg);
                    break;
                }
                break;
            }
        } else if (!isProj4StyleSyntax) {
            if (fromStr.empty())
                fromStr = *argv;
            else if (toStr.empty())
                toStr = *argv;
            else {
                /* assumed to be input file name(s) */
                eargv[eargc++] = *argv;
            }
        } else if (strcmp(*argv, "+to") == 0) {
            have_to_flag = 1;

        } else if (**argv == '+') { /* + argument */
            if (have_to_flag) {
                if (!toStr.empty())
                    toStr += ' ';
                toStr += *argv;
            } else {
                if (!fromStr.empty())
                    fromStr += ' ';
                fromStr += *argv;
            }
        } else if (!have_to_flag) {
            fromStr = *argv;
        } else if (toStr.empty()) {
            toStr = *argv;
        } else /* assumed to be input file name(s) */
            eargv[eargc++] = *argv;
    }
    if (eargc == 0) /* if no specific files force sysin */
        eargv[eargc++] = const_cast<char *>("-");

    if (oform) {
        if (!validate_form_string_for_numbers(oform)) {
            emess(3, "invalid format string");
            exit(0);
        }
    }

    if (bboxFilter && !area.empty()) {
        std::cerr << "ERROR: --bbox and --area are exclusive" << std::endl;
        std::exit(1);
    }

    PJ_AREA *pj_area = nullptr;
    if (!area.empty()) {

        DatabaseContextPtr dbContext;
        try {
            dbContext = DatabaseContext::create().as_nullable();
        } catch (const std::exception &e) {
            std::cerr << "ERROR: Cannot create database connection: "
                      << e.what() << std::endl;
            std::exit(1);
        }

        // Process area of use
        try {
            if (area.find(' ') == std::string::npos &&
                area.find(':') != std::string::npos) {
                auto tokens = split(area, ':');
                if (tokens.size() == 2) {
                    const std::string &areaAuth = tokens[0];
                    const std::string &areaCode = tokens[1];
                    bboxFilter = AuthorityFactory::create(
                                     NN_NO_CHECK(dbContext), areaAuth)
                                     ->createExtent(areaCode)
                                     .as_nullable();
                }
            }
            if (!bboxFilter) {
                auto authFactory = AuthorityFactory::create(
                    NN_NO_CHECK(dbContext), std::string());
                auto res = authFactory->listAreaOfUseFromName(area, false);
                if (res.size() == 1) {
                    bboxFilter = AuthorityFactory::create(
                                     NN_NO_CHECK(dbContext), res.front().first)
                                     ->createExtent(res.front().second)
                                     .as_nullable();
                } else {
                    res = authFactory->listAreaOfUseFromName(area, true);
                    if (res.size() == 1) {
                        bboxFilter =
                            AuthorityFactory::create(NN_NO_CHECK(dbContext),
                                                     res.front().first)
                                ->createExtent(res.front().second)
                                .as_nullable();
                    } else if (res.empty()) {
                        std::cerr << "No area of use matching provided name"
                                  << std::endl;
                        std::exit(1);
                    } else {
                        std::cerr << "Several candidates area of use "
                                     "matching provided name :"
                                  << std::endl;
                        for (const auto &candidate : res) {
                            auto obj =
                                AuthorityFactory::create(NN_NO_CHECK(dbContext),
                                                         candidate.first)
                                    ->createExtent(candidate.second);
                            std::cerr << "  " << candidate.first << ":"
                                      << candidate.second << " : "
                                      << *obj->description() << std::endl;
                        }
                        std::exit(1);
                    }
                }
            }
        } catch (const std::exception &e) {
            std::cerr << "Area of use retrieval failed: " << e.what()
                      << std::endl;
            std::exit(1);
        }
    }

    if (bboxFilter) {
        auto geogElts = bboxFilter->geographicElements();
        if (geogElts.size() == 1) {
            auto bbox = std::dynamic_pointer_cast<GeographicBoundingBox>(
                geogElts[0].as_nullable());
            if (bbox) {
                pj_area = proj_area_create();
                proj_area_set_bbox(pj_area, bbox->westBoundLongitude(),
                                   bbox->southBoundLatitude(),
                                   bbox->eastBoundLongitude(),
                                   bbox->northBoundLatitude());
                if (bboxFilter->description().has_value()) {
                    proj_area_set_name(pj_area,
                                       bboxFilter->description()->c_str());
                }
            }
        }
    }

    /*
     * If the user has requested inverse, then just reverse the
     * coordinate systems.
     */
    if (inverse) {
        std::swap(fromStr, toStr);
    }

    if (use_env_locale) {
        /* Set locale from environment */
        setlocale(LC_ALL, "");
    }

    if (fromStr.empty() && toStr.empty()) {
        emess(3, "missing source and target coordinate systems");
    }

    proj_context_use_proj4_init_rules(
        nullptr, proj_context_get_use_proj4_init_rules(nullptr, TRUE));

    PJ *src =
        !fromStr.empty()
            ? proj_create(nullptr, pj_add_type_crs_if_needed(fromStr).c_str())
            : nullptr;
    PJ *dst =
        !toStr.empty()
            ? proj_create(nullptr, pj_add_type_crs_if_needed(toStr).c_str())
            : nullptr;

    PJ *src_unbound = nullptr;
    if (src) {
        bool ignored;
        src_unbound = instantiate_crs(src, srcIsLongLat, srcToRadians, ignored);
        if (!src_unbound) {
            emess(3, "cannot instantiate source coordinate system");
        }
    }

    PJ *dst_unbound = nullptr;
    if (dst) {
        dst_unbound =
            instantiate_crs(dst, destIsLongLat, destToRadians, destIsLatLong);
        if (!dst_unbound) {
            emess(3, "cannot instantiate target coordinate system");
        }
    }

    if (!dst) {
        assert(src_unbound);
        dst = get_geog_crs_proj_string_from_proj_crs(src_unbound, destToRadians,
                                                     destIsLatLong);
        if (!dst) {
            emess(3,
                  "missing target CRS and source CRS is not a projected CRS");
        }
        destIsLongLat = true;
    } else if (!src) {
        assert(dst_unbound);
        bool ignored;
        src = get_geog_crs_proj_string_from_proj_crs(dst_unbound, srcToRadians,
                                                     ignored);
        if (!src) {
            emess(3,
                  "missing source CRS and target CRS is not a projected CRS");
        }
        srcIsLongLat = true;
    }
    proj_destroy(src_unbound);
    proj_destroy(dst_unbound);

    if (promoteTo3D) {
        auto src3D = proj_crs_promote_to_3D(nullptr, nullptr, src);
        if (src3D) {
            proj_destroy(src);
            src = src3D;
        }

        auto dst3D = proj_crs_promote_to_3D(nullptr, nullptr, dst);
        if (dst3D) {
            proj_destroy(dst);
            dst = dst3D;
        }
    } else {
        // Auto-promote source/target CRS if it is specified by its name,
        // if it has a known 3D version of it and that the other CRS is 3D.
        // e.g cs2cs "WGS 84 + EGM96 height" "WGS 84"
        if (is3DCRS(dst) && !is3DCRS(src) &&
            proj_get_id_code(src, 0) != nullptr &&
            Identifier::isEquivalentName(fromStr.c_str(), proj_get_name(src))) {
            auto promoted = proj_crs_promote_to_3D(nullptr, nullptr, src);
            if (promoted) {
                if (proj_get_id_code(promoted, 0) != nullptr) {
                    proj_destroy(src);
                    src = promoted;
                } else {
                    proj_destroy(promoted);
                }
            }
        } else if (is3DCRS(src) && !is3DCRS(dst) &&
                   proj_get_id_code(dst, 0) != nullptr &&
                   Identifier::isEquivalentName(toStr.c_str(),
                                                proj_get_name(dst))) {
            auto promoted = proj_crs_promote_to_3D(nullptr, nullptr, dst);
            if (promoted) {
                if (proj_get_id_code(promoted, 0) != nullptr) {
                    proj_destroy(dst);
                    dst = promoted;
                } else {
                    proj_destroy(promoted);
                }
            }
        }
    }

    if (!sourceEpoch.empty()) {
        PJ *srcMetadata = nullptr;
        double sourceEpochDbl;
        try {
            sourceEpochDbl = c_locale_stod(sourceEpoch);
        } catch (const std::exception &e) {
            sourceEpochDbl = 0;
            emess(3, "%s", e.what());
        }
        srcMetadata =
            proj_coordinate_metadata_create(nullptr, src, sourceEpochDbl);
        if (!srcMetadata) {
            emess(3, "cannot instantiate source coordinate system");
        }
        proj_destroy(src);
        src = srcMetadata;
    }

    if (!targetEpoch.empty()) {
        PJ *dstMetadata = nullptr;
        double targetEpochDbl;
        try {
            targetEpochDbl = c_locale_stod(targetEpoch);
        } catch (const std::exception &e) {
            targetEpochDbl = 0;
            emess(3, "%s", e.what());
        }
        dstMetadata =
            proj_coordinate_metadata_create(nullptr, dst, targetEpochDbl);
        if (!dstMetadata) {
            emess(3, "cannot instantiate target coordinate system");
        }
        proj_destroy(dst);
        dst = dstMetadata;
    }

    std::string authorityOption; /* keep this variable in this outer scope ! */
    std::string accuracyOption;  /* keep this variable in this outer scope ! */
    std::vector<const char *> options;
    if (authority) {
        authorityOption = "AUTHORITY=";
        authorityOption += authority;
        options.push_back(authorityOption.data());
    }
    if (accuracy >= 0) {
        accuracyOption = "ACCURACY=";
        accuracyOption += toString(accuracy);
        options.push_back(accuracyOption.data());
    }
    if (!allowBallpark) {
        options.push_back("ALLOW_BALLPARK=NO");
    }
    if (onlyBestSet) {
        if (errorIfBestTransformationNotAvailable) {
            options.push_back("ONLY_BEST=YES");
        } else {
            options.push_back("ONLY_BEST=NO");
        }
    }
    options.push_back(nullptr);
    transformation = proj_create_crs_to_crs_from_pj(nullptr, src, dst, pj_area,
                                                    options.data());

    proj_destroy(src);
    proj_destroy(dst);
    proj_area_destroy(pj_area);

    if (!transformation) {
        emess(3, "cannot initialize transformation\ncause: %s",
              proj_errno_string(proj_context_errno(nullptr)));
    }

    if (use_env_locale) {
        /* Restore C locale to avoid issues in parsing/outputting numbers*/
        setlocale(LC_ALL, "C");
    }

    if (mon) {
        printf("%c ---- From Coordinate System ----\n", tag);
        printf("%s\n", fromStr.c_str());
        printf("%c ---- To Coordinate System ----\n", tag);
        printf("%s\n", toStr.c_str());
    }

    /* set input formatting control */
    if (srcIsLongLat && fabs(srcToRadians - M_PI / 180) < 1e-10)
        informat = dmstor;
    else {
        informat = strtod;
    }

    if (!destIsLongLat && !oform)
        oform = "%.2f";

    /* process input file list */
    for (; eargc--; ++eargv) {
        if (**eargv == '-') {
            fid = stdin;
            emess_dat.File_name = const_cast<char *>("<stdin>");

        } else {
            if ((fid = fopen(*eargv, "rt")) == nullptr) {
                emess(-2, "input file: %s", *eargv);
                continue;
            }
            emess_dat.File_name = *eargv;
        }
        emess_dat.File_line = 0;
        process(fid);
        fclose(fid);
        emess_dat.File_name = nullptr;
    }

    proj_destroy(transformation);

    proj_cleanup();

    exit(0); /* normal completion */
}
