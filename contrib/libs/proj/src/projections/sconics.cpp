
#include "proj.h"
#include "proj_internal.h"
#include <errno.h>
#include <math.h>

namespace pj_sconics_ns {
enum Type {
    EULER = 0,
    MURD1 = 1,
    MURD2 = 2,
    MURD3 = 3,
    PCONIC = 4,
    TISSOT = 5,
    VITK1 = 6
};
}

namespace { // anonymous namespace
struct pj_sconics_data {
    double n;
    double rho_c;
    double rho_0;
    double sig;
    double c1, c2;
    enum pj_sconics_ns::Type type;
};
} // anonymous namespace

#define EPS10 1.e-10
#define EPS 1e-10
#define LINE2 "\n\tConic, Sph\n\tlat_1= and lat_2="

PROJ_HEAD(euler, "Euler") LINE2;
PROJ_HEAD(murd1, "Murdoch I") LINE2;
PROJ_HEAD(murd2, "Murdoch II") LINE2;
PROJ_HEAD(murd3, "Murdoch III") LINE2;
PROJ_HEAD(pconic, "Perspective Conic") LINE2;
PROJ_HEAD(tissot, "Tissot") LINE2;
PROJ_HEAD(vitk1, "Vitkovsky I") LINE2;

/* get common factors for simple conics */
static int phi12(PJ *P, double *del) {
    double p1, p2;
    int err = 0;

    if (!pj_param(P->ctx, P->params, "tlat_1").i) {
        proj_log_error(P, _("Missing parameter: lat_1 should be specified"));
        err = PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE;
    } else if (!pj_param(P->ctx, P->params, "tlat_2").i) {
        proj_log_error(P, _("Missing parameter: lat_2 should be specified"));
        err = PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE;
    } else {
        p1 = pj_param(P->ctx, P->params, "rlat_1").f;
        p2 = pj_param(P->ctx, P->params, "rlat_2").f;
        *del = 0.5 * (p2 - p1);
        const double sig = 0.5 * (p2 + p1);
        static_cast<struct pj_sconics_data *>(P->opaque)->sig = sig;
        err = (fabs(*del) < EPS || fabs(sig) < EPS)
                  ? PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE
                  : 0;
        if (err) {
            proj_log_error(
                P, _("Illegal value for lat_1 and lat_2: |lat_1 - lat_2| "
                     "and |lat_1 + lat_2| should be > 0"));
        }
    }
    return err;
}

static PJ_XY sconics_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_sconics_data *Q =
        static_cast<struct pj_sconics_data *>(P->opaque);
    double rho;

    switch (Q->type) {
    case pj_sconics_ns::MURD2:
        rho = Q->rho_c + tan(Q->sig - lp.phi);
        break;
    case pj_sconics_ns::PCONIC:
        rho = Q->c2 * (Q->c1 - tan(lp.phi - Q->sig));
        break;
    default:
        rho = Q->rho_c - lp.phi;
        break;
    }

    xy.x = rho * sin(lp.lam *= Q->n);
    xy.y = Q->rho_0 - rho * cos(lp.lam);
    return xy;
}

static PJ_LP
sconics_s_inverse(PJ_XY xy,
                  PJ *P) { /* Spheroidal, (and ellipsoidal?) inverse */
    PJ_LP lp = {0.0, 0.0};
    struct pj_sconics_data *Q =
        static_cast<struct pj_sconics_data *>(P->opaque);
    double rho;

    xy.y = Q->rho_0 - xy.y;
    rho = hypot(xy.x, xy.y);
    if (Q->n < 0.) {
        rho = -rho;
        xy.x = -xy.x;
        xy.y = -xy.y;
    }

    lp.lam = atan2(xy.x, xy.y) / Q->n;

    switch (Q->type) {
    case pj_sconics_ns::PCONIC:
        lp.phi = atan(Q->c1 - rho / Q->c2) + Q->sig;
        break;
    case pj_sconics_ns::MURD2:
        lp.phi = Q->sig - atan(rho - Q->rho_c);
        break;
    default:
        lp.phi = Q->rho_c - rho;
    }
    return lp;
}

static PJ *pj_sconics_setup(PJ *P, enum pj_sconics_ns::Type type) {
    double del, cs;
    int err;
    struct pj_sconics_data *Q = static_cast<struct pj_sconics_data *>(
        calloc(1, sizeof(struct pj_sconics_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;
    Q->type = type;

    err = phi12(P, &del);
    if (err)
        return pj_default_destructor(P, err);

    switch (Q->type) {

    case pj_sconics_ns::TISSOT:
        Q->n = sin(Q->sig);
        cs = cos(del);
        Q->rho_c = Q->n / cs + cs / Q->n;
        Q->rho_0 = sqrt((Q->rho_c - 2 * sin(P->phi0)) / Q->n);
        break;

    case pj_sconics_ns::MURD1:
        Q->rho_c = sin(del) / (del * tan(Q->sig)) + Q->sig;
        Q->rho_0 = Q->rho_c - P->phi0;
        Q->n = sin(Q->sig);
        break;

    case pj_sconics_ns::MURD2:
        Q->rho_c = (cs = sqrt(cos(del))) / tan(Q->sig);
        Q->rho_0 = Q->rho_c + tan(Q->sig - P->phi0);
        Q->n = sin(Q->sig) * cs;
        break;

    case pj_sconics_ns::MURD3:
        Q->rho_c = del / (tan(Q->sig) * tan(del)) + Q->sig;
        Q->rho_0 = Q->rho_c - P->phi0;
        Q->n = sin(Q->sig) * sin(del) * tan(del) / (del * del);
        break;

    case pj_sconics_ns::EULER:
        Q->n = sin(Q->sig) * sin(del) / del;
        del *= 0.5;
        Q->rho_c = del / (tan(del) * tan(Q->sig)) + Q->sig;
        Q->rho_0 = Q->rho_c - P->phi0;
        break;

    case pj_sconics_ns::PCONIC:
        Q->n = sin(Q->sig);
        Q->c2 = cos(del);
        Q->c1 = 1. / tan(Q->sig);
        del = P->phi0 - Q->sig;
        if (fabs(del) - EPS10 >= M_HALFPI) {
            proj_log_error(
                P, _("Invalid value for lat_0/lat_1/lat_2: |lat_0 - 0.5 * "
                     "(lat_1 + lat_2)| should be < 90°"));
            return pj_default_destructor(P,
                                         PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }
        Q->rho_0 = Q->c2 * (Q->c1 - tan(del));
        break;

    case pj_sconics_ns::VITK1:
        cs = tan(del);
        Q->n = cs * sin(Q->sig) / del;
        Q->rho_c = del / (cs * tan(Q->sig)) + Q->sig;
        Q->rho_0 = Q->rho_c - P->phi0;
        break;
    }

    P->inv = sconics_s_inverse;
    P->fwd = sconics_s_forward;
    P->es = 0;
    return (P);
}

PJ *PJ_PROJECTION(euler) { return pj_sconics_setup(P, pj_sconics_ns::EULER); }

PJ *PJ_PROJECTION(tissot) { return pj_sconics_setup(P, pj_sconics_ns::TISSOT); }

PJ *PJ_PROJECTION(murd1) { return pj_sconics_setup(P, pj_sconics_ns::MURD1); }

PJ *PJ_PROJECTION(murd2) { return pj_sconics_setup(P, pj_sconics_ns::MURD2); }

PJ *PJ_PROJECTION(murd3) { return pj_sconics_setup(P, pj_sconics_ns::MURD3); }

PJ *PJ_PROJECTION(pconic) { return pj_sconics_setup(P, pj_sconics_ns::PCONIC); }

PJ *PJ_PROJECTION(vitk1) { return pj_sconics_setup(P, pj_sconics_ns::VITK1); }

#undef EPS10
#undef EPS
#undef LINE2
