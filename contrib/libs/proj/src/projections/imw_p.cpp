

#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(imw_p, "International Map of the World Polyconic")
"\n\tMod. Polyconic, Ell\n\tlat_1= and lat_2= [lon_1=]";

#define TOL 1e-10
#define EPS 1e-10

namespace { // anonymous namespace
enum Mode {
    NONE_IS_ZERO = 0,  /* phi_1 and phi_2 != 0 */
    PHI_1_IS_ZERO = 1, /* phi_1 = 0 */
    PHI_2_IS_ZERO = -1 /* phi_2 = 0 */
};
} // anonymous namespace

namespace { // anonymous namespace
struct pj_imw_p_data {
    double P, Pp, Q, Qp, R_1, R_2, sphi_1, sphi_2, C2;
    double phi_1, phi_2, lam_1;
    double *en;
    enum Mode mode;
};
} // anonymous namespace

static int phi12(PJ *P, double *del, double *sig) {
    struct pj_imw_p_data *Q = static_cast<struct pj_imw_p_data *>(P->opaque);
    int err = 0;

    if (!pj_param(P->ctx, P->params, "tlat_1").i) {
        proj_log_error(P, _("Missing parameter: lat_1 should be specified"));
        err = PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE;
    } else if (!pj_param(P->ctx, P->params, "tlat_2").i) {
        proj_log_error(P, _("Missing parameter: lat_2 should be specified"));
        err = PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE;
    } else {
        Q->phi_1 = pj_param(P->ctx, P->params, "rlat_1").f;
        Q->phi_2 = pj_param(P->ctx, P->params, "rlat_2").f;
        *del = 0.5 * (Q->phi_2 - Q->phi_1);
        *sig = 0.5 * (Q->phi_2 + Q->phi_1);
        err = (fabs(*del) < EPS || fabs(*sig) < EPS)
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

static PJ_XY loc_for(PJ_LP lp, PJ *P, double *yc) {
    struct pj_imw_p_data *Q = static_cast<struct pj_imw_p_data *>(P->opaque);
    PJ_XY xy;

    if (lp.phi == 0.0) {
        xy.x = lp.lam;
        xy.y = 0.;
    } else {
        double xa, ya, xb, yb, xc, D, B, m, sp, t, R, C;

        sp = sin(lp.phi);
        m = pj_mlfn(lp.phi, sp, cos(lp.phi), Q->en);
        xa = Q->Pp + Q->Qp * m;
        ya = Q->P + Q->Q * m;
        R = 1. / (tan(lp.phi) * sqrt(1. - P->es * sp * sp));
        C = sqrt(R * R - xa * xa);
        if (lp.phi < 0.)
            C = -C;
        C += ya - R;
        if (Q->mode == PHI_2_IS_ZERO) {
            xb = lp.lam;
            yb = Q->C2;
        } else {
            t = lp.lam * Q->sphi_2;
            xb = Q->R_2 * sin(t);
            yb = Q->C2 + Q->R_2 * (1. - cos(t));
        }
        if (Q->mode == PHI_1_IS_ZERO) {
            xc = lp.lam;
            *yc = 0.;
        } else {
            t = lp.lam * Q->sphi_1;
            xc = Q->R_1 * sin(t);
            *yc = Q->R_1 * (1. - cos(t));
        }
        D = (xb - xc) / (yb - *yc);
        B = xc + D * (C + R - *yc);
        xy.x = D * sqrt(R * R * (1 + D * D) - B * B);
        if (lp.phi > 0)
            xy.x = -xy.x;
        xy.x = (B + xy.x) / (1. + D * D);
        xy.y = sqrt(R * R - xy.x * xy.x);
        if (lp.phi > 0)
            xy.y = -xy.y;
        xy.y += C + R;
    }
    return xy;
}

static PJ_XY imw_p_e_forward(PJ_LP lp, PJ *P) { /* Ellipsoidal, forward */
    double yc;
    PJ_XY xy = loc_for(lp, P, &yc);
    return (xy);
}

static PJ_LP imw_p_e_inverse(PJ_XY xy, PJ *P) { /* Ellipsoidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    struct pj_imw_p_data *Q = static_cast<struct pj_imw_p_data *>(P->opaque);
    PJ_XY t;
    double yc = 0.0;
    int i = 0;
    const int N_MAX_ITER = 1000; /* Arbitrarily chosen number... */

    lp.phi = Q->phi_2;
    lp.lam = xy.x / cos(lp.phi);
    do {
        t = loc_for(lp, P, &yc);
        const double denom = t.y - yc;
        if (denom != 0 || fabs(t.y - xy.y) > TOL) {
            if (denom == 0) {
                proj_errno_set(
                    P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return proj_coord_error().lp;
            }
            lp.phi = ((lp.phi - Q->phi_1) * (xy.y - yc) / denom) + Q->phi_1;
        }
        if (t.x != 0 && fabs(t.x - xy.x) > TOL)
            lp.lam = lp.lam * xy.x / t.x;
        i++;
    } while (i < N_MAX_ITER &&
             (fabs(t.x - xy.x) > TOL || fabs(t.y - xy.y) > TOL));

    if (i == N_MAX_ITER) {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return proj_coord_error().lp;
    }

    return lp;
}

static void xy(PJ *P, double phi, double *x, double *y, double *sp, double *R) {
    double F;

    *sp = sin(phi);
    *R = 1. / (tan(phi) * sqrt(1. - P->es * *sp * *sp));
    F = static_cast<struct pj_imw_p_data *>(P->opaque)->lam_1 * *sp;
    *y = *R * (1 - cos(F));
    *x = *R * sin(F);
}

static PJ *pj_imw_p_destructor(PJ *P, int errlev) {
    if (nullptr == P)
        return nullptr;

    if (nullptr == P->opaque)
        return pj_default_destructor(P, errlev);

    if (static_cast<struct pj_imw_p_data *>(P->opaque)->en)
        free(static_cast<struct pj_imw_p_data *>(P->opaque)->en);

    return pj_default_destructor(P, errlev);
}

PJ *PJ_PROJECTION(imw_p) {
    double del, sig, s, t, x1, x2, T2, y1, m1, m2, y2;
    int err;
    struct pj_imw_p_data *Q = static_cast<struct pj_imw_p_data *>(
        calloc(1, sizeof(struct pj_imw_p_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    if (!(Q->en = pj_enfn(P->n)))
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    if ((err = phi12(P, &del, &sig)) != 0) {
        return pj_imw_p_destructor(P, err);
    }
    if (Q->phi_2 < Q->phi_1) { /* make sure P->phi_1 most southerly */
        del = Q->phi_1;
        Q->phi_1 = Q->phi_2;
        Q->phi_2 = del;
    }
    if (pj_param(P->ctx, P->params, "tlon_1").i)
        Q->lam_1 = pj_param(P->ctx, P->params, "rlon_1").f;
    else { /* use predefined based upon latitude */
        sig = fabs(sig * RAD_TO_DEG);
        if (sig <= 60)
            sig = 2.;
        else if (sig <= 76)
            sig = 4.;
        else
            sig = 8.;
        Q->lam_1 = sig * DEG_TO_RAD;
    }
    Q->mode = NONE_IS_ZERO;
    if (Q->phi_1 != 0.0)
        xy(P, Q->phi_1, &x1, &y1, &Q->sphi_1, &Q->R_1);
    else {
        Q->mode = PHI_1_IS_ZERO;
        y1 = 0.;
        x1 = Q->lam_1;
    }
    if (Q->phi_2 != 0.0)
        xy(P, Q->phi_2, &x2, &T2, &Q->sphi_2, &Q->R_2);
    else {
        Q->mode = PHI_2_IS_ZERO;
        T2 = 0.;
        x2 = Q->lam_1;
    }
    m1 = pj_mlfn(Q->phi_1, Q->sphi_1, cos(Q->phi_1), Q->en);
    m2 = pj_mlfn(Q->phi_2, Q->sphi_2, cos(Q->phi_2), Q->en);
    t = m2 - m1;
    s = x2 - x1;
    y2 = sqrt(t * t - s * s) + y1;
    Q->C2 = y2 - T2;
    t = 1. / t;
    Q->P = (m2 * y1 - m1 * y2) * t;
    Q->Q = (y2 - y1) * t;
    Q->Pp = (m2 * x1 - m1 * x2) * t;
    Q->Qp = (x2 - x1) * t;

    P->fwd = imw_p_e_forward;
    P->inv = imw_p_e_inverse;
    P->destructor = pj_imw_p_destructor;

    return P;
}

#undef TOL
#undef EPS
