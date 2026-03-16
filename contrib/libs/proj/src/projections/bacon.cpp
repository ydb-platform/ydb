
#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

#define HLFPI2 2.46740110027233965467 /* (pi/2)^2 */
#define EPS 1e-10

namespace { // anonymous namespace
struct pj_bacon {
    int bacn;
    int ortl;
};
} // anonymous namespace

PROJ_HEAD(apian, "Apian Globular I") "\n\tMisc Sph, no inv";
PROJ_HEAD(ortel, "Ortelius Oval") "\n\tMisc Sph, no inv";
PROJ_HEAD(bacon, "Bacon Globular") "\n\tMisc Sph, no inv";

static PJ_XY bacon_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_bacon *Q = static_cast<struct pj_bacon *>(P->opaque);
    double ax, f;

    xy.y = Q->bacn ? M_HALFPI * sin(lp.phi) : lp.phi;
    ax = fabs(lp.lam);
    if (ax >= EPS) {
        if (Q->ortl && ax >= M_HALFPI)
            xy.x = sqrt(HLFPI2 - lp.phi * lp.phi + EPS) + ax - M_HALFPI;
        else {
            f = 0.5 * (HLFPI2 / ax + ax);
            xy.x = ax - f + sqrt(f * f - xy.y * xy.y);
        }
        if (lp.lam < 0.)
            xy.x = -xy.x;
    } else
        xy.x = 0.;
    return (xy);
}

PJ *PJ_PROJECTION(bacon) {
    struct pj_bacon *Q =
        static_cast<struct pj_bacon *>(calloc(1, sizeof(struct pj_bacon)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    Q->bacn = 1;
    Q->ortl = 0;
    P->es = 0.;
    P->fwd = bacon_s_forward;
    return P;
}

PJ *PJ_PROJECTION(apian) {
    struct pj_bacon *Q =
        static_cast<struct pj_bacon *>(calloc(1, sizeof(struct pj_bacon)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    Q->bacn = Q->ortl = 0;
    P->es = 0.;
    P->fwd = bacon_s_forward;
    return P;
}

PJ *PJ_PROJECTION(ortel) {
    struct pj_bacon *Q =
        static_cast<struct pj_bacon *>(calloc(1, sizeof(struct pj_bacon)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    Q->bacn = 0;
    Q->ortl = 1;
    P->es = 0.;
    P->fwd = bacon_s_forward;
    return P;
}

#undef HLFPI2
#undef EPS
