

#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(hatano, "Hatano Asymmetrical Equal Area") "\n\tPCyl, Sph";

#define NITER 20
#define EPS 1e-7
#define ONETOL 1.000001
#define CN 2.67595
#define CSz 2.43763
#define RCN 0.37369906014686373063
#define RCS 0.41023453108141924738
#define FYCN 1.75859
#define FYCS 1.93052
#define RYCN 0.56863737426006061674
#define RYCS 0.51799515156538134803
#define FXC 0.85
#define RXC 1.17647058823529411764

static PJ_XY hatano_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    int i;
    (void)P;

    const double c = sin(lp.phi) * (lp.phi < 0. ? CSz : CN);
    for (i = NITER; i; --i) {
        const double th1 = (lp.phi + sin(lp.phi) - c) / (1. + cos(lp.phi));
        lp.phi -= th1;
        if (fabs(th1) < EPS)
            break;
    }
    xy.x = FXC * lp.lam * cos(lp.phi *= .5);
    xy.y = sin(lp.phi) * (lp.phi < 0. ? FYCS : FYCN);

    return xy;
}

static PJ_LP hatano_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    double th;

    th = xy.y * (xy.y < 0. ? RYCS : RYCN);
    if (fabs(th) > 1.) {
        if (fabs(th) > ONETOL) {
            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
            return lp;
        } else {
            th = th > 0. ? M_HALFPI : -M_HALFPI;
        }
    } else {
        th = asin(th);
    }

    lp.lam = RXC * xy.x / cos(th);
    th += th;
    lp.phi = (th + sin(th)) * (xy.y < 0. ? RCS : RCN);
    if (fabs(lp.phi) > 1.) {
        if (fabs(lp.phi) > ONETOL) {
            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
            return lp;
        } else {
            lp.phi = lp.phi > 0. ? M_HALFPI : -M_HALFPI;
        }
    } else {
        lp.phi = asin(lp.phi);
    }

    return (lp);
}

PJ *PJ_PROJECTION(hatano) {
    P->es = 0.;
    P->inv = hatano_s_inverse;
    P->fwd = hatano_s_forward;

    return P;
}

#undef NITER
#undef EPS
#undef ONETOL
#undef CN
#undef CSz
#undef RCN
#undef RCS
#undef FYCN
#undef FYCS
#undef RYCN
#undef RYCS
#undef FXC
#undef RXC
