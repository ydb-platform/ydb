#define GEOD_IN_GEOD_SET

#include <math.h>
#include <stdlib.h>
#include <string.h>

#include "emess.h"
#include "geod_interface.h"
#include "proj.h"
#include "proj_internal.h"

void geod_set(int argc, char **argv) {
    paralist *start = nullptr, *curr;
    double es;
    char *name;

    /* put arguments into internal linked list */
    if (argc <= 0)
        emess(1, "no arguments in initialization list");
    start = curr = pj_mkparam(argv[0]);
    if (!curr)
        emess(1, "memory allocation failed");
    for (int i = 1; curr != nullptr && i < argc; ++i) {
        curr->next = pj_mkparam(argv[i]);
        if (!curr->next)
            emess(1, "memory allocation failed");
        curr = curr->next;
    }
    /* set elliptical parameters */
    if (pj_ell_set(pj_get_default_ctx(), start, &geod_a, &es))
        emess(1, "ellipsoid setup failure");
    /* set units */
    if ((name = pj_param(nullptr, start, "sunits").s) != nullptr) {
        bool unit_found = false;
        auto units = proj_get_units_from_database(nullptr, nullptr, "linear",
                                                  false, nullptr);
        for (int i = 0; units && units[i]; i++) {
            if (units[i]->proj_short_name &&
                strcmp(units[i]->proj_short_name, name) == 0) {
                unit_found = true;
                to_meter = units[i]->conv_factor;
                fr_meter = 1 / to_meter;
            }
        }
        proj_unit_list_destroy(units);
        if (!unit_found)
            emess(1, "%s unknown unit conversion id", name);
    } else
        to_meter = fr_meter = 1;
    geod_f = es / (1 + sqrt(1 - es));
    geod_ini();
    /* check if line or arc mode */
    if (pj_param(nullptr, start, "tlat_1").i) {
        double del_S;
        phi1 = pj_param(nullptr, start, "rlat_1").f;
        lam1 = pj_param(nullptr, start, "rlon_1").f;
        if (pj_param(nullptr, start, "tlat_2").i) {
            phi2 = pj_param(nullptr, start, "rlat_2").f;
            lam2 = pj_param(nullptr, start, "rlon_2").f;
            geod_inv();
            geod_pre();
        } else if ((geod_S = pj_param(nullptr, start, "dS").f) != 0.) {
            al12 = pj_param(nullptr, start, "rA").f;
            geod_pre();
            geod_for();
        } else
            emess(1, "incomplete geodesic/arc info");
        if ((n_alpha = pj_param(nullptr, start, "in_A").i) > 0) {
            if ((del_alpha = pj_param(nullptr, start, "rdel_A").f) == 0.0)
                emess(1, "del azimuth == 0");
        } else if ((del_S = fabs(pj_param(nullptr, start, "ddel_S").f)) != 0.) {
            n_S = (int)(geod_S / del_S + .5);
        } else if ((n_S = pj_param(nullptr, start, "in_S").i) <= 0)
            emess(1, "no interval divisor selected");
    }
    /* free up linked list */
    for (; start; start = curr) {
        curr = start->next;
        free(start);
    }
}
