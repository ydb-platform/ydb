/* arc sin, cosine, tan2 and sqrt that will NOT fail */

#include <math.h>

#include "proj.h"
#include "proj_internal.h"

#define ONE_TOL 1.00000000000001
#define ATOL 1e-50

double aasin(PJ_CONTEXT *ctx, double v) {
    double av;

    if ((av = fabs(v)) >= 1.) {
        if (av > ONE_TOL)
            proj_context_errno_set(
                ctx, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return (v < 0. ? -M_HALFPI : M_HALFPI);
    }
    return asin(v);
}

double aacos(PJ_CONTEXT *ctx, double v) {
    double av;

    if ((av = fabs(v)) >= 1.) {
        if (av > ONE_TOL)
            proj_context_errno_set(
                ctx, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return (v < 0. ? M_PI : 0.);
    }
    return acos(v);
}
double asqrt(double v) { return ((v <= 0) ? 0. : sqrt(v)); }
double aatan2(double n, double d) {
    return ((fabs(n) < ATOL && fabs(d) < ATOL) ? 0. : atan2(n, d));
}
