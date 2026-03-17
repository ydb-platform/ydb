

#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(goode, "Goode Homolosine") "\n\tPCyl, Sph";

#define Y_COR 0.05280
#define PHI_LIM 0.71093078197902358062

C_NAMESPACE PJ *pj_sinu(PJ *), *pj_moll(PJ *);

namespace { // anonymous namespace
struct pj_goode_data {
    PJ *sinu;
    PJ *moll;
};
} // anonymous namespace

static PJ_XY goode_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy;
    struct pj_goode_data *Q = static_cast<struct pj_goode_data *>(P->opaque);

    if (fabs(lp.phi) <= PHI_LIM)
        xy = Q->sinu->fwd(lp, Q->sinu);
    else {
        xy = Q->moll->fwd(lp, Q->moll);
        xy.y -= lp.phi >= 0.0 ? Y_COR : -Y_COR;
    }
    return xy;
}

static PJ_LP goode_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp;
    struct pj_goode_data *Q = static_cast<struct pj_goode_data *>(P->opaque);

    if (fabs(xy.y) <= PHI_LIM)
        lp = Q->sinu->inv(xy, Q->sinu);
    else {
        xy.y += xy.y >= 0.0 ? Y_COR : -Y_COR;
        lp = Q->moll->inv(xy, Q->moll);
    }
    return lp;
}

static PJ *goode_destructor(PJ *P, int errlev) { /* Destructor */
    if (nullptr == P)
        return nullptr;
    if (nullptr == P->opaque)
        return pj_default_destructor(P, errlev);
    proj_destroy(static_cast<struct pj_goode_data *>(P->opaque)->sinu);
    proj_destroy(static_cast<struct pj_goode_data *>(P->opaque)->moll);
    return pj_default_destructor(P, errlev);
}

PJ *PJ_PROJECTION(goode) {
    struct pj_goode_data *Q = static_cast<struct pj_goode_data *>(
        calloc(1, sizeof(struct pj_goode_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;
    P->destructor = goode_destructor;

    P->es = 0.;
    Q->sinu = pj_sinu(nullptr);
    Q->moll = pj_moll(nullptr);
    if (Q->sinu == nullptr || Q->moll == nullptr)
        return goode_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    Q->sinu->es = 0.;
    Q->sinu->ctx = P->ctx;
    Q->moll->ctx = P->ctx;
    Q->sinu = pj_sinu(Q->sinu);
    Q->moll = pj_moll(Q->moll);
    if (Q->sinu == nullptr || Q->moll == nullptr)
        return goode_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);

    P->fwd = goode_s_forward;
    P->inv = goode_s_inverse;

    return P;
}

#undef Y_COR
#undef PHI_LIM
