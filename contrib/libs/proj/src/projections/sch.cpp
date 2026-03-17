/******************************************************************************
 * Project:  SCH Coordinate system
 * Purpose:  Implementation of SCH Coordinate system
 * References :
 *      1. Hensley. Scott. SCH Coordinates and various transformations. June 15,
 *2000.
 *      2. Buckley, Sean Monroe. Radar interferometry measurement of land
 *subsidence. 2000.. PhD Thesis. UT Austin. (Appendix)
 *      3. Hensley, Scott, Elaine Chapin, and T. Michel. "Improved processing of
 *AIRSAR data based on the GeoSAR processor." Airsar earth science and
 *applications workshop, March. 2002.
 *(http://airsar.jpl.nasa.gov/documents/workshop2002/papers/T3.pdf)
 *
 * Author:   Piyush Agram (piyush.agram@jpl.nasa.gov)
 * Copyright (c) 2015 California Institute of Technology.
 * Government sponsorship acknowledged.
 *
 * NOTE:  The SCH coordinate system is a sensor aligned coordinate system
 * developed at JPL for radar mapping missions. Details pertaining to the
 * coordinate system have been release in the public domain (see references
 *above). This code is an independent implementation of the SCH coordinate
 *system that conforms to the PROJ.4 conventions and uses the details presented
 *in these publicly released documents. All credit for the development of the
 *coordinate system and its use should be directed towards the original
 *developers at JPL.
 ******************************************************************************
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 ****************************************************************************/

#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

namespace { // anonymous namespace
struct pj_sch_data {
    double plat; /*Peg Latitude */
    double plon; /*Peg Longitude*/
    double phdg; /*Peg heading  */
    double h0;   /*Average altitude */
    double transMat[9];
    double xyzoff[3];
    double rcurv;
    PJ *cart;
    PJ *cart_sph;
};
} // anonymous namespace

PROJ_HEAD(sch, "Spherical Cross-track Height")
"\n\tMisc\n\tplat_0= plon_0= phdg_0= [h_0=]";

static PJ_LPZ sch_inverse3d(PJ_XYZ xyz, PJ *P) {
    struct pj_sch_data *Q = static_cast<struct pj_sch_data *>(P->opaque);

    PJ_LPZ lpz;
    lpz.lam = xyz.x * (P->a / Q->rcurv);
    lpz.phi = xyz.y * (P->a / Q->rcurv);
    lpz.z = xyz.z;
    xyz = Q->cart_sph->fwd3d(lpz, Q->cart_sph);

    /* Apply rotation */
    xyz = {Q->transMat[0] * xyz.x + Q->transMat[1] * xyz.y +
               Q->transMat[2] * xyz.z,
           Q->transMat[3] * xyz.x + Q->transMat[4] * xyz.y +
               Q->transMat[5] * xyz.z,
           Q->transMat[6] * xyz.x + Q->transMat[7] * xyz.y +
               Q->transMat[8] * xyz.z};

    /* Apply offset */
    xyz.x += Q->xyzoff[0];
    xyz.y += Q->xyzoff[1];
    xyz.z += Q->xyzoff[2];

    /* Convert geocentric coordinates to lat long */
    return Q->cart->inv3d(xyz, Q->cart);
}

static PJ_XYZ sch_forward3d(PJ_LPZ lpz, PJ *P) {
    struct pj_sch_data *Q = static_cast<struct pj_sch_data *>(P->opaque);

    /* Convert lat long to geocentric coordinates */
    PJ_XYZ xyz = Q->cart->fwd3d(lpz, Q->cart);

    /* Adjust for offset */
    xyz.x -= Q->xyzoff[0];
    xyz.y -= Q->xyzoff[1];
    xyz.z -= Q->xyzoff[2];

    /* Apply rotation */
    xyz = {Q->transMat[0] * xyz.x + Q->transMat[3] * xyz.y +
               Q->transMat[6] * xyz.z,
           Q->transMat[1] * xyz.x + Q->transMat[4] * xyz.y +
               Q->transMat[7] * xyz.z,
           Q->transMat[2] * xyz.x + Q->transMat[5] * xyz.y +
               Q->transMat[8] * xyz.z};

    /* Convert to local lat,long */
    lpz = Q->cart_sph->inv3d(xyz, Q->cart_sph);

    /* Scale by radius */
    xyz.x = lpz.lam * (Q->rcurv / P->a);
    xyz.y = lpz.phi * (Q->rcurv / P->a);
    xyz.z = lpz.z;

    return xyz;
}

static PJ *pj_sch_destructor(PJ *P, int errlev) {
    if (nullptr == P)
        return nullptr;

    auto Q = static_cast<struct pj_sch_data *>(P->opaque);
    if (Q) {
        if (Q->cart)
            Q->cart->destructor(Q->cart, errlev);
        if (Q->cart_sph)
            Q->cart_sph->destructor(Q->cart_sph, errlev);
    }

    return pj_default_destructor(P, errlev);
}

static PJ *pj_sch_setup(PJ *P) { /* general initialization */
    struct pj_sch_data *Q = static_cast<struct pj_sch_data *>(P->opaque);

    /* Setup original geocentric system */
    // Pass a dummy ellipsoid definition that will be overridden just afterwards
    Q->cart = proj_create(P->ctx, "+proj=cart +a=1");
    if (Q->cart == nullptr)
        return pj_sch_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);

    /* inherit ellipsoid definition from P to Q->cart */
    pj_inherit_ellipsoid_def(P, Q->cart);

    const double clt = cos(Q->plat);
    const double slt = sin(Q->plat);
    const double clo = cos(Q->plon);
    const double slo = sin(Q->plon);

    /* Estimate the radius of curvature for given peg */
    const double temp = sqrt(1.0 - (P->es) * slt * slt);
    const double reast = (P->a) / temp;
    const double rnorth = (P->a) * (1.0 - (P->es)) / pow(temp, 3);

    const double chdg = cos(Q->phdg);
    const double shdg = sin(Q->phdg);

    Q->rcurv =
        Q->h0 + (reast * rnorth) / (reast * chdg * chdg + rnorth * shdg * shdg);

    /* Set up local sphere at the given peg point */
    Q->cart_sph = proj_create(P->ctx, "+proj=cart +a=1");
    if (Q->cart_sph == nullptr)
        return pj_sch_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    pj_calc_ellipsoid_params(Q->cart_sph, Q->rcurv, 0);

    /* Set up the transformation matrices */
    Q->transMat[0] = clt * clo;
    Q->transMat[1] = -shdg * slo - slt * clo * chdg;
    Q->transMat[2] = slo * chdg - slt * clo * shdg;
    Q->transMat[3] = clt * slo;
    Q->transMat[4] = clo * shdg - slt * slo * chdg;
    Q->transMat[5] = -clo * chdg - slt * slo * shdg;
    Q->transMat[6] = slt;
    Q->transMat[7] = clt * chdg;
    Q->transMat[8] = clt * shdg;

    PJ_LPZ lpz;
    lpz.lam = Q->plon;
    lpz.phi = Q->plat;
    lpz.z = Q->h0;
    PJ_XYZ xyz = Q->cart->fwd3d(lpz, Q->cart);
    Q->xyzoff[0] = xyz.x - (Q->rcurv) * clt * clo;
    Q->xyzoff[1] = xyz.y - (Q->rcurv) * clt * slo;
    Q->xyzoff[2] = xyz.z - (Q->rcurv) * slt;

    P->fwd3d = sch_forward3d;
    P->inv3d = sch_inverse3d;
    return P;
}

PJ *PJ_PROJECTION(sch) {
    struct pj_sch_data *Q = static_cast<struct pj_sch_data *>(
        calloc(1, sizeof(struct pj_sch_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;
    P->destructor = pj_sch_destructor;

    Q->h0 = 0.0;

    /* Check if peg latitude was defined */
    if (pj_param(P->ctx, P->params, "tplat_0").i)
        Q->plat = pj_param(P->ctx, P->params, "rplat_0").f;
    else {
        proj_log_error(P, _("Missing parameter plat_0."));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_MISSING_ARG);
    }

    /* Check if peg longitude was defined */
    if (pj_param(P->ctx, P->params, "tplon_0").i)
        Q->plon = pj_param(P->ctx, P->params, "rplon_0").f;
    else {
        proj_log_error(P, _("Missing parameter plon_0."));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_MISSING_ARG);
    }

    /* Check if peg heading is defined */
    if (pj_param(P->ctx, P->params, "tphdg_0").i)
        Q->phdg = pj_param(P->ctx, P->params, "rphdg_0").f;
    else {
        proj_log_error(P, _("Missing parameter phdg_0."));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_MISSING_ARG);
    }

    /* Check if average height was defined - If so read it in */
    if (pj_param(P->ctx, P->params, "th_0").i)
        Q->h0 = pj_param(P->ctx, P->params, "dh_0").f;

    return pj_sch_setup(P);
}
