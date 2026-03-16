/* <<<< Cartographic projection filter program >>>> */
#include "proj.h"
#include "emess.h"
#include "proj_experimental.h"
#include "proj_internal.h"
#include "utils.h"
#include <ctype.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <proj/crs.hpp>

#include <vector>

#if defined(MSDOS) || defined(OS2) || defined(WIN32) || defined(__WIN32__)
#include <fcntl.h>
#include <io.h>
#define SET_BINARY_MODE(file) _setmode(_fileno(file), O_BINARY)
#else
#define SET_BINARY_MODE(file)
#endif

#define MAX_LINE 1000
#define PJ_INVERSE(P) (P->inv ? 1 : 0)

static PJ *Proj = nullptr;
static PJ *ProjForFactors = nullptr;
static bool swapAxisCrs = false;
static union {
    PJ_XY (*fwd)(PJ_LP, PJ *);
    PJ_LP (*inv)(PJ_XY, PJ *);
} proj;

static int reversein = 0, /* != 0 reverse input arguments */
    reverseout = 0,       /* != 0 reverse output arguments */
    bin_in = 0,           /* != 0 then binary input */
    bin_out = 0,          /* != 0 then binary output */
    echoin = 0,           /* echo input data to output line */
    tag = '#',            /* beginning of line tag character */
    inverse = 0,          /* != 0 then inverse projection */
    prescale = 0,         /* != 0 apply cartesian scale factor */
    dofactors = 0,        /* determine scale factors */
    very_verby = 0,       /* very verbose mode */
    postscale = 0;

static const char *oform =
    nullptr;                  /* output format for x-y or decimal degrees */
static char oform_buffer[16]; /* Buffer for oform when using -d */

static const char *oterr = "*\t*", /* output line for unprojectable input */
    *usage = "%s\nusage: %s [-bdeEfiIlmorsStTvVwW [args]] [+opt[=arg] ...] "
             "[file ...]\n";

static PJ_FACTORS facs;

static double (*informat)(const char *,
                          char **), /* input data deformatter function */
    fscale = 0.;                    /* cartesian scale factor */

/* file processing function */
static void process(FILE *fid) {
    char line[MAX_LINE + 3], *s = nullptr, pline[40];
    PJ_COORD data;

    for (;;) {
        int facs_bad = 0;
        ++emess_dat.File_line;

        if (bin_in) { /* binary input */
            if (fread(&data, sizeof(PJ_UV), 1, fid) != 1)
                break;
        } else { /* ascii input */
            if (!(s = fgets(line, MAX_LINE, fid)))
                break;

            if (!strchr(s, '\n')) { /* overlong line */
                int c;
                (void)strcat(s, "\n");
                /* gobble up to newline */
                while ((c = fgetc(fid)) != EOF && c != '\n')
                    ;
            }

            if (*s == tag) {
                if (!bin_out)
                    (void)fputs(line, stdout);
                continue;
            }

            if (reversein) {
                data.uv.v = (*informat)(s, &s);
                data.uv.u = (*informat)(s, &s);
            } else {
                data.uv.u = (*informat)(s, &s);
                data.uv.v = (*informat)(s, &s);
            }

            if (data.uv.v == HUGE_VAL)
                data.uv.u = HUGE_VAL;

            if (!*s && (s > line))
                --s; /* assumed we gobbled \n */
            if (!bin_out && echoin) {
                char t;
                t = *s;
                *s = '\0';
                (void)fputs(line, stdout);
                *s = t;
                putchar('\t');
            }
        }

        if (data.uv.u != HUGE_VAL) {
            PJ_COORD coord;
            coord.lp = data.lp;
            if (prescale) {
                data.uv.u *= fscale;
                data.uv.v *= fscale;
            }
            if (dofactors && !inverse) {
                facs = proj_factors(ProjForFactors, coord);
                facs_bad = proj_errno(ProjForFactors);
            }

            const auto xy = (*proj.fwd)(data.lp, Proj);
            data.xy = xy;

            if (dofactors && inverse) {
                facs = proj_factors(ProjForFactors, coord);
                facs_bad = proj_errno(ProjForFactors);
            }

            if (postscale && data.uv.u != HUGE_VAL) {
                data.uv.u *= fscale;
                data.uv.v *= fscale;
            }
        }

        if (bin_out) { /* binary output */
            (void)fwrite(&data, sizeof(PJ_UV), 1, stdout);
            continue;
        } else if (data.uv.u == HUGE_VAL) /* error output */
            (void)fputs(oterr, stdout);
        else if (inverse && !oform) { /*ascii DMS output */
            if (reverseout) {
                (void)fputs(rtodms(pline, sizeof(pline), data.uv.v, 'N', 'S'),
                            stdout);
                putchar('\t');
                (void)fputs(rtodms(pline, sizeof(pline), data.uv.u, 'E', 'W'),
                            stdout);
            } else {
                (void)fputs(rtodms(pline, sizeof(pline), data.uv.u, 'E', 'W'),
                            stdout);
                putchar('\t');
                (void)fputs(rtodms(pline, sizeof(pline), data.uv.v, 'N', 'S'),
                            stdout);
            }
        } else { /* x-y or decimal degree ascii output, scale if warranted by
                    output units */
            if (inverse) {
                if (proj_angular_input(Proj, PJ_FWD)) {
                    data.uv.v *= RAD_TO_DEG;
                    data.uv.u *= RAD_TO_DEG;
                }
            } else {
                if (proj_angular_output(Proj, PJ_FWD)) {
                    data.uv.v *= RAD_TO_DEG;
                    data.uv.u *= RAD_TO_DEG;
                }
            }

            if (reverseout) {
                limited_fprintf_for_number(stdout, oform, data.uv.v);
                putchar('\t');
                limited_fprintf_for_number(stdout, oform, data.uv.u);
            } else {
                limited_fprintf_for_number(stdout, oform, data.uv.u);
                putchar('\t');
                limited_fprintf_for_number(stdout, oform, data.uv.v);
            }
        }

        /* print scale factor data */
        if (dofactors) {
            if (!facs_bad)
                (void)printf("\t<%g %g %g %g %g %g>", facs.meridional_scale,
                             facs.parallel_scale, facs.areal_scale,
                             facs.angular_distortion * RAD_TO_DEG,
                             facs.tissot_semimajor, facs.tissot_semiminor);
            else
                (void)fputs("\t<* * * * * *>", stdout);
        }
        (void)fputs(bin_in ? "\n" : s, stdout);
        fflush(stdout);
    }
}

/* file processing function --- verbosely */
static void vprocess(FILE *fid) {
    char line[MAX_LINE + 3], *s, pline[40];
    PJ_LP dat_ll;
    PJ_XY dat_xy;
    int linvers;
    PJ_COORD coord;

    if (!oform)
        oform = "%.3f";

    if (bin_in || bin_out)
        emess(1, "binary I/O not available in -V option");

    for (;;) {
        proj_errno_reset(Proj);
        ++emess_dat.File_line;

        if (!(s = fgets(line, MAX_LINE, fid)))
            break;

        if (!strchr(s, '\n')) { /* overlong line */
            int c;
            (void)strcat(s, "\n");
            /* gobble up to newline */
            while ((c = fgetc(fid)) != EOF && c != '\n')
                ;
        }

        if (*s == tag) { /* pass on data */
            (void)fputs(s, stdout);
            continue;
        }

        /* check to override default input mode */
        if (*s == 'I' || *s == 'i') {
            linvers = 1;
            ++s;
        } else
            linvers = inverse;

        if (linvers) {
            if (!PJ_INVERSE(Proj)) {
                emess(-1, "inverse for this projection not avail.\n");
                continue;
            }
            dat_xy.x = strtod(s, &s);
            dat_xy.y = strtod(s, &s);
            if (dat_xy.x == HUGE_VAL || dat_xy.y == HUGE_VAL) {
                emess(-1, "lon-lat input conversion failure\n");
                continue;
            }
            if (prescale) {
                dat_xy.x *= fscale;
                dat_xy.y *= fscale;
            }
            if (reversein) {
                PJ_XY temp = dat_xy;
                dat_xy.x = temp.y;
                dat_xy.y = temp.x;
            }
            dat_ll = pj_inv(dat_xy, Proj);
        } else {
            dat_ll.lam = proj_dmstor(s, &s);
            dat_ll.phi = proj_dmstor(s, &s);
            if (dat_ll.lam == HUGE_VAL || dat_ll.phi == HUGE_VAL) {
                emess(-1, "lon-lat input conversion failure\n");
                continue;
            }
            if (reversein) {
                PJ_LP temp = dat_ll;
                dat_ll.lam = temp.phi;
                dat_ll.phi = temp.lam;
            }
            dat_xy = pj_fwd(dat_ll, Proj);
            if (postscale) {
                dat_xy.x *= fscale;
                dat_xy.y *= fscale;
            }
        }

        if (proj_context_errno(nullptr)) {
            emess(-1, "%s", proj_errno_string(proj_context_errno(nullptr)));
            continue;
        }

        if (!*s && (s > line))
            --s; /* assumed we gobbled \n */
        coord.lp = dat_ll;
        facs = proj_factors(ProjForFactors, coord);
        if (proj_errno(ProjForFactors)) {
            emess(-1, "failed to compute factors\n\n");
            continue;
        }

        if (*s != '\n')
            (void)fputs(s, stdout);

        (void)fputs("Longitude: ", stdout);
        (void)fputs(proj_rtodms2(pline, sizeof(pline), dat_ll.lam, 'E', 'W'),
                    stdout);
        (void)printf(" [ %.11g ]\n", dat_ll.lam * RAD_TO_DEG);
        (void)fputs("Latitude:  ", stdout);
        (void)fputs(proj_rtodms2(pline, sizeof(pline), dat_ll.phi, 'N', 'S'),
                    stdout);
        (void)printf(" [ %.11g ]\n", dat_ll.phi * RAD_TO_DEG);
        (void)fputs(swapAxisCrs ? "Northing (y):  " : "Easting (x):   ",
                    stdout);
        limited_fprintf_for_number(stdout, oform, dat_xy.x);
        putchar('\n');
        (void)fputs(swapAxisCrs ? "Easting (x):   " : "Northing (y):  ",
                    stdout);
        limited_fprintf_for_number(stdout, oform, dat_xy.y);
        putchar('\n');
        (void)printf("Meridian scale (h) : %.8f  ( %.4g %% error )\n",
                     facs.meridional_scale,
                     (facs.meridional_scale - 1.) * 100.);
        (void)printf("Parallel scale (k) : %.8f  ( %.4g %% error )\n",
                     facs.parallel_scale, (facs.parallel_scale - 1.) * 100.);
        (void)printf("Areal scale (s):     %.8f  ( %.4g %% error )\n",
                     facs.areal_scale, (facs.areal_scale - 1.) * 100.);
        (void)printf("Angular distortion (w): %.3f\n",
                     facs.angular_distortion * RAD_TO_DEG);
        (void)printf("Meridian/Parallel angle: %.5f\n",
                     facs.meridian_parallel_angle * RAD_TO_DEG);
        (void)printf("Convergence : ");
        (void)fputs(
            proj_rtodms2(pline, sizeof(pline), facs.meridian_convergence, 0, 0),
            stdout);
        (void)printf(" [ %.8f ]\n", facs.meridian_convergence * RAD_TO_DEG);
        (void)printf("Max-min (Tissot axis a-b) scale error: %.5f %.5f\n\n",
                     facs.tissot_semimajor, facs.tissot_semiminor);

        fflush(stdout);
    }
}

int main(int argc, char **argv) {
    char *arg;
    std::vector<char *> argvVector;
    char **eargv = argv;
    FILE *fid;
    int eargc = 0, mon = 0;

    pj_stderr_proj_lib_deprecation_warning();

    if (argc == 0) {
        exit(1);
    }

    if ((emess_dat.Prog_name = strrchr(*argv, DIR_CHAR)) != nullptr)
        ++emess_dat.Prog_name;
    else
        emess_dat.Prog_name = *argv;
    inverse = strncmp(emess_dat.Prog_name, "inv", 3) == 0 ||
              strncmp(emess_dat.Prog_name, "lt-inv", 6) ==
                  0; // older libtool have a lt- prefix
    if (argc <= 1) {
        (void)fprintf(stderr, usage, pj_get_release(), emess_dat.Prog_name);
        exit(0);
    }

    /* process run line arguments */
    while (--argc > 0) { /* collect run line arguments */
        if (**++argv == '-')
            for (arg = *argv;;) {
                switch (*++arg) {
                case '\0': /* position of "stdin" */
                    if (arg[-1] == '-')
                        eargv[eargc++] = const_cast<char *>("-");
                    break;
                case 'b': /* binary I/O */
                    bin_in = bin_out = 1;
                    continue;
                case 'v': /* monitor dump of initialization */
                    mon = 1;
                    continue;
                case 'i': /* input binary */
                    bin_in = 1;
                    continue;
                case 'o': /* output binary */
                    bin_out = 1;
                    continue;
                case 'I': /* alt. method to spec inverse */
                    inverse = 1;
                    continue;
                case 'E': /* echo ascii input to ascii output */
                    echoin = 1;
                    continue;
                case 'V': /* very verbose processing mode */
                    very_verby = 1;
                    mon = 1;
                    continue;
                case 'S': /* compute scale factors */
                    dofactors = 1;
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
                            if (strcmp(lp->id, "latlong") == 0 ||
                                strcmp(lp->id, "longlat") == 0 ||
                                strcmp(lp->id, "geocent") == 0)
                                continue;

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
                case 'm': /* cartesian multiplier */
                    if (--argc <= 0)
                        goto noargument;
                    postscale = 1;
                    if (!strncmp("1/", *++argv, 2) ||
                        !strncmp("1:", *argv, 2)) {
                        if ((fscale = atof((*argv) + 2)) == 0.)
                            goto badscale;
                        fscale = 1. / fscale;
                    } else if ((fscale = atof(*argv)) == 0.) {
                    badscale:
                        emess(1, "invalid scale argument");
                    }
                    continue;
                case 'W': /* specify seconds precision */
                case 'w': /* -W for constant field width */
                {
                    int c = arg[1];
                    if (c != 0 && isdigit(c)) {
                        set_rtodms(c - '0', *arg == 'W');
                        ++arg;
                    } else
                        emess(1, "-W argument missing or non-digit");
                    continue;
                }
                case 'f': /* alternate output format degrees or xy */
                    if (--argc <= 0)
                        goto noargument;
                    oform = *++argv;
                    continue;
                case 'd':
                    if (--argc <= 0)
                        goto noargument;
                    snprintf(oform_buffer, sizeof(oform_buffer), "%%.%df",
                             atoi(*++argv));
                    oform = oform_buffer;
                    break;
                case 'r': /* reverse input */
                    reversein = 1;
                    continue;
                case 's': /* reverse output */
                    reverseout = 1;
                    continue;
                default:
                    emess(1, "invalid option: -%c", *arg);
                    break;
                }
                break;
            }
        else if (**argv == '+') { /* + argument */
            argvVector.push_back(*argv + 1);
        } else /* assumed to be input file name(s) */
            eargv[eargc++] = *argv;
    }

    if (oform) {
        if (!validate_form_string_for_numbers(oform)) {
            emess(3, "invalid format string");
            exit(0);
        }
    }

    /* done with parameter and control input */
    if (inverse && postscale) {
        prescale = 1;
        postscale = 0;
        fscale = 1. / fscale;
    }
    proj_context_use_proj4_init_rules(nullptr, true);

    if (argvVector.empty() && eargc >= 1) {
        // Consider the next arg as a CRS, not a file.
        std::string ocrs = eargv[0];
        eargv++;
        eargc--;
        // logic copied from proj_factors function
        // coverity[tainted_data]
        if (PJ *P = proj_create(nullptr, ocrs.c_str())) {
            auto type = proj_get_type(P);
            auto ctx = P->ctx;
            if (type == PJ_TYPE_COMPOUND_CRS) {
                auto horiz = proj_crs_get_sub_crs(ctx, P, 0);
                if (horiz) {
                    if (proj_get_type(horiz) == PJ_TYPE_PROJECTED_CRS) {
                        proj_destroy(P);
                        P = horiz;
                        type = proj_get_type(P);
                    } else {
                        proj_destroy(horiz);
                    }
                }
            }
            if (type == PJ_TYPE_PROJECTED_CRS) {
                try {
                    auto crs = dynamic_cast<const NS_PROJ::crs::ProjectedCRS *>(
                        P->iso_obj.get());
                    auto &dir =
                        crs->coordinateSystem()->axisList()[0]->direction();
                    swapAxisCrs = dir == NS_PROJ::cs::AxisDirection::NORTH ||
                                  dir == NS_PROJ::cs::AxisDirection::SOUTH;
                } catch (...) {
                }
                auto geodetic_crs = proj_get_source_crs(ctx, P);
                assert(geodetic_crs);
                auto pm = proj_get_prime_meridian(ctx, geodetic_crs);
                double pm_longitude = 0;
                proj_prime_meridian_get_parameters(ctx, pm, &pm_longitude,
                                                   nullptr, nullptr);
                proj_destroy(pm);
                PJ *geogCRSNormalized;
                auto cs = proj_create_ellipsoidal_2D_cs(
                    ctx, PJ_ELLPS2D_LONGITUDE_LATITUDE, "Radian", 1.0);
                if (pm_longitude != 0) {
                    auto ellipsoid = proj_get_ellipsoid(ctx, geodetic_crs);
                    double semi_major_metre = 0;
                    double inv_flattening = 0;
                    proj_ellipsoid_get_parameters(ctx, ellipsoid,
                                                  &semi_major_metre, nullptr,
                                                  nullptr, &inv_flattening);
                    geogCRSNormalized = proj_create_geographic_crs(
                        ctx, "unname crs", "unnamed datum",
                        proj_get_name(ellipsoid), semi_major_metre,
                        inv_flattening, "reference prime meridian", 0, nullptr,
                        0, cs);
                    proj_destroy(ellipsoid);
                } else {
                    auto datum = proj_crs_get_datum(ctx, geodetic_crs);
                    auto datum_ensemble =
                        proj_crs_get_datum_ensemble(ctx, geodetic_crs);
                    geogCRSNormalized = proj_create_geographic_crs_from_datum(
                        ctx, "unnamed crs", datum ? datum : datum_ensemble, cs);
                    proj_destroy(datum);
                    proj_destroy(datum_ensemble);
                }
                proj_destroy(cs);
                Proj = proj_create_crs_to_crs_from_pj(ctx, geogCRSNormalized, P,
                                                      nullptr, nullptr);

                auto conversion = proj_crs_get_coordoperation(ctx, P);
                auto projCS = proj_create_cartesian_2D_cs(
                    ctx, PJ_CART2D_EASTING_NORTHING, "metre", 1.0);
                auto projCRSNormalized = proj_create_projected_crs(
                    ctx, nullptr, geodetic_crs, conversion, projCS);
                assert(projCRSNormalized);
                proj_destroy(geodetic_crs);
                proj_destroy(conversion);
                proj_destroy(projCS);
                ProjForFactors = proj_create_crs_to_crs_from_pj(
                    ctx, geogCRSNormalized, projCRSNormalized, nullptr,
                    nullptr);

                proj_destroy(geogCRSNormalized);
                proj_destroy(projCRSNormalized);
            } else {
                emess(3, "CRS must be projected");
            }
            proj_destroy(P);
        } else {
            emess(3, "CRS is not parseable");
        }
    }
    if (eargc == 0) /* if no specific files force sysin */
        eargv[eargc++] = const_cast<char *>("-");

    // proj historically ignores any datum shift specifier, like nadgrids,
    // towgs84, etc
    argvVector.push_back(const_cast<char *>("break_cs2cs_recursion"));

    if (!Proj) {
        if (!(Proj =
                  proj_create_argv(nullptr, static_cast<int>(argvVector.size()),
                                   argvVector.data())))
            emess(3, "projection initialization failure\ncause: %s",
                  proj_errno_string(proj_context_errno(nullptr)));

        ProjForFactors = Proj;
    }

    if (!proj_angular_input(Proj, PJ_FWD)) {
        emess(3, "can't initialize operations that take non-angular input "
                 "coordinates. Try cct.");
        exit(0);
    }

    if (proj_angular_output(Proj, PJ_FWD)) {
        emess(3, "can't initialize operations that produce angular output "
                 "coordinates");
        exit(0);
    }

    // Ugly hack. See https://github.com/OSGeo/PROJ/issues/1782
    if (Proj->right == PJ_IO_UNITS_WHATEVER && Proj->descr &&
        strncmp(Proj->descr, "General Oblique Transformation",
                strlen("General Oblique Transformation")) == 0) {
        Proj->right = PJ_IO_UNITS_PROJECTED;
    }

    if (inverse) {
        if (!Proj->inv)
            emess(3, "inverse projection not available");
        proj.inv = pj_inv;
    } else
        proj.fwd = pj_fwd;
    /* set input formatting control */
    if (mon) {
        pj_pr_list(Proj);
        if (very_verby) {
            (void)printf("#Final Earth figure: ");
            if (Proj->es != 0.0) {
                (void)printf("ellipsoid\n#  Major axis (a): ");
                limited_fprintf_for_number(stdout, oform ? oform : "%.3f",
                                           Proj->a);
                (void)printf("\n#  1/flattening: %.6f\n",
                             1. / (1. - sqrt(1. - Proj->es)));
                (void)printf("#  squared eccentricity: %.12f\n", Proj->es);
            } else {
                (void)printf("sphere\n#  Radius: ");
                limited_fprintf_for_number(stdout, oform ? oform : "%.3f",
                                           Proj->a);
                (void)putchar('\n');
            }
        }
    }

    if (inverse)
        informat = strtod;
    else {
        informat = proj_dmstor;
        if (!oform)
            oform = "%.2f";
    }

    if (bin_out) {
        SET_BINARY_MODE(stdout);
    }

    /* process input file list */
    for (; eargc--; ++eargv) {
        if (**eargv == '-') {
            fid = stdin;
            emess_dat.File_name = const_cast<char *>("<stdin>");

            if (bin_in) {
                SET_BINARY_MODE(stdin);
            }

        } else {
            if ((fid = fopen(*eargv, "rb")) == nullptr) {
                emess(-2, "input file: %s", *eargv);
                continue;
            }
            emess_dat.File_name = *eargv;
        }
        emess_dat.File_line = 0;
        if (very_verby)
            vprocess(fid);
        else
            process(fid);
        (void)fclose(fid);
        emess_dat.File_name = nullptr;
    }

    if (ProjForFactors && ProjForFactors != Proj)
        proj_destroy(ProjForFactors);
    if (Proj)
        proj_destroy(Proj);

    exit(0); /* normal completion */
}
