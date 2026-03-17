

#include "proj_internal.h"
#include <errno.h>

PROJ_HEAD(set, "Set coordinate value");

/* Projection specific elements for the PJ object */
namespace { // anonymous namespace
struct Set {
    bool v1;
    bool v2;
    bool v3;
    bool v4;
    double v1_val;
    double v2_val;
    double v3_val;
    double v4_val;
};
} // anonymous namespace

static void set_fwd_inv(PJ_COORD &point, PJ *P) {

    struct Set *set = static_cast<struct Set *>(P->opaque);

    if (set->v1)
        point.v[0] = set->v1_val;
    if (set->v2)
        point.v[1] = set->v2_val;
    if (set->v3)
        point.v[2] = set->v3_val;
    if (set->v4)
        point.v[3] = set->v4_val;
}

PJ *OPERATION(set, 0) {
    P->inv4d = set_fwd_inv;
    P->fwd4d = set_fwd_inv;

    auto set = static_cast<struct Set *>(calloc(1, sizeof(struct Set)));
    P->opaque = set;
    if (nullptr == P->opaque)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);

    if (pj_param_exists(P->params, "v_1")) {
        set->v1 = true;
        set->v1_val = pj_param(P->ctx, P->params, "dv_1").f;
    }

    if (pj_param_exists(P->params, "v_2")) {
        set->v2 = true;
        set->v2_val = pj_param(P->ctx, P->params, "dv_2").f;
    }

    if (pj_param_exists(P->params, "v_3")) {
        set->v3 = true;
        set->v3_val = pj_param(P->ctx, P->params, "dv_3").f;
    }

    if (pj_param_exists(P->params, "v_4")) {
        set->v4 = true;
        set->v4_val = pj_param(P->ctx, P->params, "dv_4").f;
    }

    P->left = PJ_IO_UNITS_WHATEVER;
    P->right = PJ_IO_UNITS_WHATEVER;

    return P;
}
