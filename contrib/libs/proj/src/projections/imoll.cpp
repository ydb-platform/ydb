

#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(imoll, "Interrupted Mollweide") "\n\tPCyl, Sph";

/*
This projection is a compilation of 6 separate sub-projections.
It is related to the Interrupted Goode Homolosine projection,
but uses Mollweide at all latitudes.

Each sub-projection is assigned an integer label
numbered 1 through 6. Most of this code contains logic to assign
the labels based on latitude (phi) and longitude (lam) regions.
The code is adapted from igh.cpp.

Original Reference:
J. Paul Goode (1919) STUDIES IN PROJECTIONS: ADAPTING THE
    HOMOLOGRAPHIC PROJECTION TO THE PORTRAYAL OF THE EARTH'S
    ENTIRE SURFACE, Bul. Geog. SOC.Phila., Vol. XWIJNO.3.
    July, 1919, pp. 103-113.
*/

C_NAMESPACE PJ *pj_moll(PJ *);

namespace pj_imoll_ns {
struct pj_imoll_data {
    struct PJconsts *pj[6];
    // We need to know the inverse boundary locations of the "seams".
    double boundary12;
    double boundary34;
    double boundary45;
    double boundary56;
};

constexpr double d20 = 20 * DEG_TO_RAD;
constexpr double d30 = 30 * DEG_TO_RAD;
constexpr double d40 = 40 * DEG_TO_RAD;
constexpr double d60 = 60 * DEG_TO_RAD;
constexpr double d80 = 80 * DEG_TO_RAD;
constexpr double d100 = 100 * DEG_TO_RAD;
constexpr double d140 = 140 * DEG_TO_RAD;
constexpr double d160 = 160 * DEG_TO_RAD;
constexpr double d180 = 180 * DEG_TO_RAD;

constexpr double EPSLN =
    1.e-10; /* allow a little 'slack' on zone edge positions */

} // namespace pj_imoll_ns

/*
Assign an integer index representing each of the 6
sub-projection zones based on latitude (phi) and
longitude (lam) ranges.
*/

static PJ_XY imoll_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    using namespace pj_imoll_ns;

    PJ_XY xy;
    struct pj_imoll_data *Q = static_cast<struct pj_imoll_data *>(P->opaque);
    int z;

    if (lp.phi >= 0) { /* 1|2 */
        z = (lp.lam <= -d40 ? 1 : 2);
    } else { /* 3|4|5|6 */
        if (lp.lam <= -d100)
            z = 3; /* 3 */
        else if (lp.lam <= -d20)
            z = 4; /* 4 */
        else if (lp.lam <= d80)
            z = 5; /* 5 */
        else
            z = 6; /* 6 */
    }

    lp.lam -= Q->pj[z - 1]->lam0;
    xy = Q->pj[z - 1]->fwd(lp, Q->pj[z - 1]);
    xy.x += Q->pj[z - 1]->x0;
    xy.y += Q->pj[z - 1]->y0;

    return xy;
}

static PJ_LP imoll_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    using namespace pj_imoll_ns;

    PJ_LP lp = {0.0, 0.0};
    struct pj_imoll_data *Q = static_cast<struct pj_imoll_data *>(P->opaque);
    const double y90 = sqrt(2.0); /* lt=90 corresponds to y=sqrt(2) */

    int z = 0;
    if (xy.y > y90 + EPSLN || xy.y < -y90 + EPSLN) /* 0 */
        z = 0;
    else if (xy.y >= 0)
        z = (xy.x <= (Q->boundary12) ? 1 : 2); /* 1|2 */
    else {
        if (xy.x <= Q->boundary34)
            z = 3; /* 3 */
        else if (xy.x <= Q->boundary45)
            z = 4; /* 4 */
        else if (xy.x <= Q->boundary56)
            z = 5; /* 5 */
        else
            z = 6; /* 6 */
    }

    if (z) {
        bool ok = false;

        xy.x -= Q->pj[z - 1]->x0;
        xy.y -= Q->pj[z - 1]->y0;
        lp = Q->pj[z - 1]->inv(xy, Q->pj[z - 1]);
        lp.lam += Q->pj[z - 1]->lam0;

        switch (z) {
        case 1:
            ok = ((lp.lam >= -d180 - EPSLN && lp.lam <= -d40 + EPSLN) &&
                  (lp.phi >= 0.0 - EPSLN));
            break;
        case 2:
            ok = ((lp.lam >= -d40 - EPSLN && lp.lam <= d180 + EPSLN) &&
                  (lp.phi >= 0.0 - EPSLN));
            break;
        case 3:
            ok = ((lp.lam >= -d180 - EPSLN && lp.lam <= -d100 + EPSLN) &&
                  (lp.phi <= 0.0 + EPSLN));
            break;
        case 4:
            ok = ((lp.lam >= -d100 - EPSLN && lp.lam <= -d20 + EPSLN) &&
                  (lp.phi <= 0.0 + EPSLN));
            break;
        case 5:
            ok = ((lp.lam >= -d20 - EPSLN && lp.lam <= d80 + EPSLN) &&
                  (lp.phi <= 0.0 + EPSLN));
            break;
        case 6:
            ok = ((lp.lam >= d80 - EPSLN && lp.lam <= d180 + EPSLN) &&
                  (lp.phi <= 0.0 + EPSLN));
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

static PJ *pj_imoll_destructor(PJ *P, int errlev) {
    using namespace pj_imoll_ns;

    int i;
    if (nullptr == P)
        return nullptr;

    if (nullptr == P->opaque)
        return pj_default_destructor(P, errlev);

    struct pj_imoll_data *Q = static_cast<struct pj_imoll_data *>(P->opaque);

    for (i = 0; i < 6; ++i) {
        if (Q->pj[i])
            Q->pj[i]->destructor(Q->pj[i], errlev);
    }

    return pj_default_destructor(P, errlev);
}

/*
  Zones:

    -180            -40                       180
      +--------------+-------------------------+
      |1             |2                        |
      |              |                         |
      |              |                         |
      |              |                         |
      |              |                         |
    0 +-------+------+-+-----------+-----------+
      |3      |4       |5          |6          |
      |       |        |           |           |
      |       |        |           |           |
      |       |        |           |           |
      |       |        |           |           |
      +-------+--------+-----------+-----------+
    -180    -100      -20         80          180
*/

static bool setup_zone(PJ *P, struct pj_imoll_ns::pj_imoll_data *Q, int n,
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

static double compute_zone_offset(struct pj_imoll_ns::pj_imoll_data *Q,
                                  int zone1, int zone2, double lam, double phi1,
                                  double phi2) {
    PJ_LP lp1, lp2;
    PJ_XY xy1, xy2;

    lp1.lam = lam - (Q->pj[zone1 - 1]->lam0);
    lp1.phi = phi1;
    lp2.lam = lam - (Q->pj[zone2 - 1]->lam0);
    lp2.phi = phi2;
    xy1 = Q->pj[zone1 - 1]->fwd(lp1, Q->pj[zone1 - 1]);
    xy2 = Q->pj[zone2 - 1]->fwd(lp2, Q->pj[zone2 - 1]);
    return (xy2.x + Q->pj[zone2 - 1]->x0) - (xy1.x + Q->pj[zone1 - 1]->x0);
}

static double compute_zone_x_boundary(PJ *P, double lam, double phi) {
    PJ_LP lp1, lp2;
    PJ_XY xy1, xy2;

    lp1.lam = lam - pj_imoll_ns::EPSLN;
    lp1.phi = phi;
    lp2.lam = lam + pj_imoll_ns::EPSLN;
    lp2.phi = phi;
    xy1 = imoll_s_forward(lp1, P);
    xy2 = imoll_s_forward(lp2, P);
    return (xy1.x + xy2.x) / 2.;
}

PJ *PJ_PROJECTION(imoll) {
    using namespace pj_imoll_ns;

    struct pj_imoll_data *Q = static_cast<struct pj_imoll_data *>(
        calloc(1, sizeof(struct pj_imoll_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    /* Setup zones */
    if (!setup_zone(P, Q, 1, pj_moll, -d100, 0, -d100) ||
        !setup_zone(P, Q, 2, pj_moll, d30, 0, d30) ||
        !setup_zone(P, Q, 3, pj_moll, -d160, 0, -d160) ||
        !setup_zone(P, Q, 4, pj_moll, -d60, 0, -d60) ||
        !setup_zone(P, Q, 5, pj_moll, d20, 0, d20) ||
        !setup_zone(P, Q, 6, pj_moll, d140, 0, d140)) {
        return pj_imoll_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    }

    /* Adjust zones */

    /* Match 3 (south) to 1 (north) */
    Q->pj[2]->x0 +=
        compute_zone_offset(Q, 3, 1, -d160, 0.0 - EPSLN, 0.0 + EPSLN);

    /* Match 2 (north-east) to 1 (north-west) */
    Q->pj[1]->x0 +=
        compute_zone_offset(Q, 2, 1, -d40, 0.0 + EPSLN, 0.0 + EPSLN);

    /* Match 4 (south) to 1 (north) */
    Q->pj[3]->x0 +=
        compute_zone_offset(Q, 4, 1, -d100, 0.0 - EPSLN, 0.0 + EPSLN);

    /* Match 5 (south) to 2 (north) */
    Q->pj[4]->x0 +=
        compute_zone_offset(Q, 5, 2, -d20, 0.0 - EPSLN, 0.0 + EPSLN);

    /* Match 6 (south) to 2 (north) */
    Q->pj[5]->x0 += compute_zone_offset(Q, 6, 2, d80, 0.0 - EPSLN, 0.0 + EPSLN);

    /*
      The most straightforward way of computing the x locations of the "seams"
      in the interrupted projection is to compute the forward transform at the
      seams and record these values.
    */
    Q->boundary12 = compute_zone_x_boundary(P, -d40, 0.0 + EPSLN);
    Q->boundary34 = compute_zone_x_boundary(P, -d100, 0.0 - EPSLN);
    Q->boundary45 = compute_zone_x_boundary(P, -d20, 0.0 - EPSLN);
    Q->boundary56 = compute_zone_x_boundary(P, d80, 0.0 - EPSLN);

    P->inv = imoll_s_inverse;
    P->fwd = imoll_s_forward;
    P->destructor = pj_imoll_destructor;
    P->es = 0.;

    return P;
}
