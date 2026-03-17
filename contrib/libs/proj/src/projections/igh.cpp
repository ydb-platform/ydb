

#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(igh, "Interrupted Goode Homolosine") "\n\tPCyl, Sph";

/*
This projection is a compilation of 12 separate sub-projections.
Sinusoidal projections are found near the equator and Mollweide
projections are found at higher latitudes. The transition between
the two occurs at 40 degrees latitude and is represented by the
constant `igh_phi_boundary`.

Each sub-projection is assigned an integer label
numbered 1 through 12. Most of this code contains logic to assign
the labels based on latitude (phi) and longitude (lam) regions.

Original Reference:
J. Paul Goode (1925) THE HOMOLOSINE PROJECTION: A NEW DEVICE FOR
    PORTRAYING THE EARTH'S SURFACE ENTIRE, Annals of the Association of
    American Geographers, 15:3, 119-125, DOI: 10.1080/00045602509356949
*/

C_NAMESPACE PJ *pj_sinu(PJ *), *pj_moll(PJ *);

/*
Transition from sinusoidal to Mollweide projection
Latitude (phi): 40deg 44' 11.8"
*/

constexpr double igh_phi_boundary = (40 + 44 / 60. + 11.8 / 3600.) * DEG_TO_RAD;

namespace pj_igh_ns {
struct pj_igh_data {
    struct PJconsts *pj[12];
    double dy0;
};

constexpr double d10 = 10 * DEG_TO_RAD;
constexpr double d20 = 20 * DEG_TO_RAD;
constexpr double d30 = 30 * DEG_TO_RAD;
constexpr double d40 = 40 * DEG_TO_RAD;
constexpr double d50 = 50 * DEG_TO_RAD;
constexpr double d60 = 60 * DEG_TO_RAD;
constexpr double d80 = 80 * DEG_TO_RAD;
constexpr double d90 = 90 * DEG_TO_RAD;
constexpr double d100 = 100 * DEG_TO_RAD;
constexpr double d140 = 140 * DEG_TO_RAD;
constexpr double d160 = 160 * DEG_TO_RAD;
constexpr double d180 = 180 * DEG_TO_RAD;

constexpr double EPSLN =
    1.e-10; /* allow a little 'slack' on zone edge positions */

} // namespace pj_igh_ns

static PJ_XY igh_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    using namespace pj_igh_ns;
    PJ_XY xy;
    struct pj_igh_data *Q = static_cast<struct pj_igh_data *>(P->opaque);
    int z;

    if (lp.phi >= igh_phi_boundary) { /* 1|2 */
        z = (lp.lam <= -d40 ? 1 : 2);
    } else if (lp.phi >= 0) { /* 3|4 */
        z = (lp.lam <= -d40 ? 3 : 4);
    } else if (lp.phi >= -igh_phi_boundary) { /* 5|6|7|8 */
        if (lp.lam <= -d100)
            z = 5; /* 5 */
        else if (lp.lam <= -d20)
            z = 6; /* 6 */
        else if (lp.lam <= d80)
            z = 7; /* 7 */
        else
            z = 8; /* 8 */
    } else {       /* 9|10|11|12 */
        if (lp.lam <= -d100)
            z = 9; /* 9 */
        else if (lp.lam <= -d20)
            z = 10; /* 10 */
        else if (lp.lam <= d80)
            z = 11; /* 11 */
        else
            z = 12; /* 12 */
    }

    lp.lam -= Q->pj[z - 1]->lam0;
    xy = Q->pj[z - 1]->fwd(lp, Q->pj[z - 1]);
    xy.x += Q->pj[z - 1]->x0;
    xy.y += Q->pj[z - 1]->y0;

    return xy;
}

static PJ_LP igh_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    using namespace pj_igh_ns;
    PJ_LP lp = {0.0, 0.0};
    struct pj_igh_data *Q = static_cast<struct pj_igh_data *>(P->opaque);
    const double y90 =
        Q->dy0 + sqrt(2.0); /* lt=90 corresponds to y=y0+sqrt(2) */

    int z = 0;
    if (xy.y > y90 + EPSLN || xy.y < -y90 + EPSLN) /* 0 */
        z = 0;
    else if (xy.y >= igh_phi_boundary) /* 1|2 */
        z = (xy.x <= -d40 ? 1 : 2);
    else if (xy.y >= 0) /* 3|4 */
        z = (xy.x <= -d40 ? 3 : 4);
    else if (xy.y >= -igh_phi_boundary) { /* 5|6|7|8 */
        if (xy.x <= -d100)
            z = 5; /* 5 */
        else if (xy.x <= -d20)
            z = 6; /* 6 */
        else if (xy.x <= d80)
            z = 7; /* 7 */
        else
            z = 8; /* 8 */
    } else {       /* 9|10|11|12 */
        if (xy.x <= -d100)
            z = 9; /* 9 */
        else if (xy.x <= -d20)
            z = 10; /* 10 */
        else if (xy.x <= d80)
            z = 11; /* 11 */
        else
            z = 12; /* 12 */
    }

    if (z) {
        bool ok = false;

        xy.x -= Q->pj[z - 1]->x0;
        xy.y -= Q->pj[z - 1]->y0;
        lp = Q->pj[z - 1]->inv(xy, Q->pj[z - 1]);
        lp.lam += Q->pj[z - 1]->lam0;

        switch (z) {
        case 1:
            ok = (lp.lam >= -d180 - EPSLN && lp.lam <= -d40 + EPSLN) ||
                 ((lp.lam >= -d40 - EPSLN && lp.lam <= -d10 + EPSLN) &&
                  (lp.phi >= d60 - EPSLN && lp.phi <= d90 + EPSLN));
            break;
        case 2:
            ok = (lp.lam >= -d40 - EPSLN && lp.lam <= d180 + EPSLN) ||
                 ((lp.lam >= -d180 - EPSLN && lp.lam <= -d160 + EPSLN) &&
                  (lp.phi >= d50 - EPSLN && lp.phi <= d90 + EPSLN)) ||
                 ((lp.lam >= -d50 - EPSLN && lp.lam <= -d40 + EPSLN) &&
                  (lp.phi >= d60 - EPSLN && lp.phi <= d90 + EPSLN));
            break;
        case 3:
            ok = (lp.lam >= -d180 - EPSLN && lp.lam <= -d40 + EPSLN);
            break;
        case 4:
            ok = (lp.lam >= -d40 - EPSLN && lp.lam <= d180 + EPSLN);
            break;
        case 5:
            ok = (lp.lam >= -d180 - EPSLN && lp.lam <= -d100 + EPSLN);
            break;
        case 6:
            ok = (lp.lam >= -d100 - EPSLN && lp.lam <= -d20 + EPSLN);
            break;
        case 7:
            ok = (lp.lam >= -d20 - EPSLN && lp.lam <= d80 + EPSLN);
            break;
        case 8:
            ok = (lp.lam >= d80 - EPSLN && lp.lam <= d180 + EPSLN);
            break;
        case 9:
            ok = (lp.lam >= -d180 - EPSLN && lp.lam <= -d100 + EPSLN);
            break;
        case 10:
            ok = (lp.lam >= -d100 - EPSLN && lp.lam <= -d20 + EPSLN);
            break;
        case 11:
            ok = (lp.lam >= -d20 - EPSLN && lp.lam <= d80 + EPSLN);
            break;
        case 12:
            ok = (lp.lam >= d80 - EPSLN && lp.lam <= d180 + EPSLN);
            break;
        }
        z = (!ok ? 0 : z); /* projectable? */
    }

    if (!z)
        lp.lam = HUGE_VAL;
    if (!z)
        lp.phi = HUGE_VAL;

    return lp;
}

static PJ *pj_igh_data_destructor(PJ *P, int errlev) {
    using namespace pj_igh_ns;
    int i;
    if (nullptr == P)
        return nullptr;

    if (nullptr == P->opaque)
        return pj_default_destructor(P, errlev);

    struct pj_igh_data *Q = static_cast<struct pj_igh_data *>(P->opaque);

    for (i = 0; i < 12; ++i) {
        if (Q->pj[i])
            Q->pj[i]->destructor(Q->pj[i], errlev);
    }

    return pj_default_destructor(P, errlev);
}

/*
  Zones:

    -180            -40                       180
      +--------------+-------------------------+    Zones 1,2,9,10,11 & 12:
      |1             |2                        |      Mollweide projection
      |              |                         |
      +--------------+-------------------------+    Zones 3,4,5,6,7 & 8:
      |3             |4                        |      Sinusoidal projection
      |              |                         |
    0 +-------+------+-+-----------+-----------+
      |5      |6       |7          |8          |
      |       |        |           |           |
      +-------+--------+-----------+-----------+
      |9      |10      |11         |12         |
      |       |        |           |           |
      +-------+--------+-----------+-----------+
    -180    -100      -20         80          180
*/

static bool pj_igh_setup_zone(PJ *P, struct pj_igh_ns::pj_igh_data *Q, int n,
                              PJ *(*proj_ptr)(PJ *), double x_0, double y_0,
                              double lon_0) {
    if (!(Q->pj[n - 1] = proj_ptr(nullptr)))
        return false;
    if (!(Q->pj[n - 1] = proj_ptr(Q->pj[n - 1])))
        return false;
    Q->pj[n - 1]->ctx = P->ctx;
    Q->pj[n - 1]->x0 = x_0;
    Q->pj[n - 1]->y0 = y_0;
    Q->pj[n - 1]->lam0 = lon_0;
    return true;
}

PJ *PJ_PROJECTION(igh) {
    using namespace pj_igh_ns;

    PJ_XY xy1, xy3;
    PJ_LP lp = {0, igh_phi_boundary};
    struct pj_igh_data *Q = static_cast<struct pj_igh_data *>(
        calloc(1, sizeof(struct pj_igh_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    /* sinusoidal zones */
    if (!pj_igh_setup_zone(P, Q, 3, pj_sinu, -d100, 0, -d100) ||
        !pj_igh_setup_zone(P, Q, 4, pj_sinu, d30, 0, d30) ||
        !pj_igh_setup_zone(P, Q, 5, pj_sinu, -d160, 0, -d160) ||
        !pj_igh_setup_zone(P, Q, 6, pj_sinu, -d60, 0, -d60) ||
        !pj_igh_setup_zone(P, Q, 7, pj_sinu, d20, 0, d20) ||
        !pj_igh_setup_zone(P, Q, 8, pj_sinu, d140, 0, d140)) {
        return pj_igh_data_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    }

    /* mollweide zones */
    if (!pj_igh_setup_zone(P, Q, 1, pj_moll, -d100, 0, -d100)) {
        return pj_igh_data_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    }

    /* y0 ? */
    xy1 = Q->pj[0]->fwd(lp, Q->pj[0]); /* zone 1 */
    xy3 = Q->pj[2]->fwd(lp, Q->pj[2]); /* zone 3 */
    /* y0 + xy1.y = xy3.y for lt = 40d44'11.8" */
    Q->dy0 = xy3.y - xy1.y;

    Q->pj[0]->y0 = Q->dy0;

    /* mollweide zones (cont'd) */
    if (!pj_igh_setup_zone(P, Q, 2, pj_moll, d30, Q->dy0, d30) ||
        !pj_igh_setup_zone(P, Q, 9, pj_moll, -d160, -Q->dy0, -d160) ||
        !pj_igh_setup_zone(P, Q, 10, pj_moll, -d60, -Q->dy0, -d60) ||
        !pj_igh_setup_zone(P, Q, 11, pj_moll, d20, -Q->dy0, d20) ||
        !pj_igh_setup_zone(P, Q, 12, pj_moll, d140, -Q->dy0, d140)) {
        return pj_igh_data_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    }

    P->inv = igh_s_inverse;
    P->fwd = igh_s_forward;
    P->destructor = pj_igh_data_destructor;
    P->es = 0.;

    return P;
}
