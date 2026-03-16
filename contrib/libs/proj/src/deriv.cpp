/* dervative of (*P->fwd) projection */

#include <math.h>

#include "proj.h"
#include "proj_internal.h"

int pj_deriv(PJ_LP lp, double h, const PJ *P, struct DERIVS *der) {
    PJ_XY t;
    /* get rid of constness until we can do it for real */
    PJ *Q = (PJ *)P;
    if (nullptr == Q->fwd)
        return 1;

    lp.lam += h;
    lp.phi += h;
    if (fabs(lp.phi) > M_HALFPI)
        return 1;

    h += h;
    t = (*Q->fwd)(lp, Q);
    if (t.x == HUGE_VAL)
        return 1;

    der->x_l = t.x;
    der->y_p = t.y;
    der->x_p = t.x;
    der->y_l = t.y;

    lp.phi -= h;
    if (fabs(lp.phi) > M_HALFPI)
        return 1;

    t = (*Q->fwd)(lp, Q);
    if (t.x == HUGE_VAL)
        return 1;

    der->x_l += t.x;
    der->y_p -= t.y;
    der->x_p -= t.x;
    der->y_l += t.y;

    lp.lam -= h;
    t = (*Q->fwd)(lp, Q);
    if (t.x == HUGE_VAL)
        return 1;

    der->x_l -= t.x;
    der->y_p -= t.y;
    der->x_p -= t.x;
    der->y_l -= t.y;

    lp.phi += h;
    t = (*Q->fwd)(lp, Q);
    if (t.x == HUGE_VAL)
        return 1;

    der->x_l -= t.x;
    der->y_p += t.y;
    der->x_p += t.x;
    der->y_l -= t.y;

    h += h;
    der->x_l /= h;
    der->y_p /= h;
    der->x_p /= h;
    der->y_l /= h;

    return 0;
}
