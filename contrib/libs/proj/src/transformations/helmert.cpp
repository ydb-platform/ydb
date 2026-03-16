/***********************************************************************

             3-, 4-and 7-parameter shifts, and their 6-, 8-
                and 14-parameter kinematic counterparts.

                    Thomas Knudsen, 2016-05-24

************************************************************************

  Implements 3(6)-, 4(8) and 7(14)-parameter Helmert transformations for
  3D data.

  Also incorporates Molodensky-Badekas variant of 7-parameter Helmert
  transformation, where the rotation is not applied regarding the centre
  of the spheroid, but given a reference point.

  Primarily useful for implementation of datum shifts in transformation
  pipelines.

************************************************************************

Thomas Knudsen, thokn@sdfe.dk, 2016-05-24/06-05
Kristian Evers, kreve@sdfe.dk, 2017-05-01
Even Rouault, even.rouault@spatialys.com
Last update: 2018-10-26

************************************************************************
* Copyright (c) 2016, Thomas Knudsen / SDFE
*
* Permission is hereby granted, free of charge, to any person obtaining a
* copy of this software and associated documentation files (the "Software"),
* to deal in the Software without restriction, including without limitation
* the rights to use, copy, modify, merge, publish, distribute, sublicense,
* and/or sell copies of the Software, and to permit persons to whom the
* Software is furnished to do so, subject to the following conditions:
*
* The above copyright notice and this permission notice shall be included
* in all copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
* OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
* THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
* FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
* DEALINGS IN THE SOFTWARE.
*
***********************************************************************/

#include <errno.h>
#include <math.h>

#include "proj_internal.h"

PROJ_HEAD(helmert, "3(6)-, 4(8)- and 7(14)-parameter Helmert shift");
PROJ_HEAD(molobadekas, "Molodensky-Badekas transformation");

static PJ_XYZ helmert_forward_3d(PJ_LPZ lpz, PJ *P);
static PJ_LPZ helmert_reverse_3d(PJ_XYZ xyz, PJ *P);

/***********************************************************************/
namespace { // anonymous namespace
struct pj_opaque_helmert {
    /************************************************************************
        Projection specific elements for the "helmert" PJ object
    ************************************************************************/
    PJ_XYZ xyz;
    PJ_XYZ xyz_0;
    PJ_XYZ dxyz;
    PJ_XYZ refp;
    PJ_OPK opk;
    PJ_OPK opk_0;
    PJ_OPK dopk;
    double scale;
    double scale_0;
    double dscale;
    double theta;
    double theta_0;
    double dtheta;
    double R[3][3];
    double t_epoch, t_obs;
    int no_rotation, exact, fourparam;
    int is_position_vector; /* 1 = position_vector, 0 = coordinate_frame */
};
} // anonymous namespace

/* Make the maths of the rotation operations somewhat more readable and textbook
 * like */
#define R00 (Q->R[0][0])
#define R01 (Q->R[0][1])
#define R02 (Q->R[0][2])

#define R10 (Q->R[1][0])
#define R11 (Q->R[1][1])
#define R12 (Q->R[1][2])

#define R20 (Q->R[2][0])
#define R21 (Q->R[2][1])
#define R22 (Q->R[2][2])

/**************************************************************************/
static void update_parameters(PJ *P) {
    /***************************************************************************

        Update transformation parameters.
        ---------------------------------

        The 14-parameter Helmert transformation is at it's core the same as the
        7-parameter transformation, since the transformation parameters are
        projected forward or backwards in time via the rate of changes of the
        parameters. The transformation parameters are calculated for a specific
        epoch before the actual Helmert transformation is carried out.

        The transformation parameters are updated with the following equation
    [0]:

                          .
        P(t) = P(EPOCH) + P * (t - EPOCH)

                                                                  .
        where EPOCH is the epoch indicated in the above table and P is the rate
        of that parameter.

        [0] http://itrf.ign.fr/doc_ITRF/Transfo-ITRF2008_ITRFs.txt

    *******************************************************************************/

    struct pj_opaque_helmert *Q = (struct pj_opaque_helmert *)P->opaque;
    double dt = Q->t_obs - Q->t_epoch;

    Q->xyz.x = Q->xyz_0.x + Q->dxyz.x * dt;
    Q->xyz.y = Q->xyz_0.y + Q->dxyz.y * dt;
    Q->xyz.z = Q->xyz_0.z + Q->dxyz.z * dt;

    Q->opk.o = Q->opk_0.o + Q->dopk.o * dt;
    Q->opk.p = Q->opk_0.p + Q->dopk.p * dt;
    Q->opk.k = Q->opk_0.k + Q->dopk.k * dt;

    Q->scale = Q->scale_0 + Q->dscale * dt;

    Q->theta = Q->theta_0 + Q->dtheta * dt;

    /* debugging output */
    if (proj_log_level(P->ctx, PJ_LOG_TELL) >= PJ_LOG_TRACE) {
        proj_log_trace(P,
                       "Transformation parameters for observation "
                       "t_obs=%g (t_epoch=%g):",
                       Q->t_obs, Q->t_epoch);
        proj_log_trace(P, "x: %g", Q->xyz.x);
        proj_log_trace(P, "y: %g", Q->xyz.y);
        proj_log_trace(P, "z: %g", Q->xyz.z);
        proj_log_trace(P, "s: %g", Q->scale * 1e-6);
        proj_log_trace(P, "rx: %g", Q->opk.o);
        proj_log_trace(P, "ry: %g", Q->opk.p);
        proj_log_trace(P, "rz: %g", Q->opk.k);
        proj_log_trace(P, "theta: %g", Q->theta);
    }
}

/**************************************************************************/
static void build_rot_matrix(PJ *P) {
    /***************************************************************************

        Build rotation matrix.
        ----------------------

        Here we rename rotation indices from omega, phi, kappa (opk), to
        fi (i.e. phi), theta, psi (ftp), in order to reduce the mental agility
        needed to implement the expression for the rotation matrix derived over
        at https://en.wikipedia.org/wiki/Rotation_formalisms_in_three_dimensions
        The relevant section is Euler angles ( z-’-x" intrinsic) -> Rotation
    matrix

        By default small angle approximations are used:
        The matrix elements are approximated by expanding the trigonometric
        functions to linear order (i.e. cos(x) = 1, sin(x) = x), and discarding
        products of second order.

        This was a useful hack when calculating by hand was the only option,
        but in general, today, should be avoided because:

        1. It does not save much computation time, as the rotation matrix
           is built only once and probably used many times (except when
           transforming spatio-temporal coordinates).

        2. The error induced may be too large for ultra high accuracy
           applications: the Earth is huge and the linear error is
           approximately the angular error multiplied by the Earth radius.

        However, in many cases the approximation is necessary, since it has
        been used historically: Rotation angles from older published datum
        shifts may actually be a least squares fit to the linearized rotation
        approximation, hence not being strictly valid for deriving the exact
        rotation matrix. In fact, most publicly available transformation
        parameters are based on the approximate Helmert transform, which is why
        we use that as the default setting, even though it is more correct to
        use the exact form of the equations.

        So in order to fit historically derived coordinates, the access to
        the approximate rotation matrix is necessary - at least in principle.

        Also, when using any published datum transformation information, one
        should always check which convention (exact or approximate rotation
        matrix) is expected, and whether the induced error for selecting
        the opposite convention is acceptable (which it often is).


        Sign conventions
        ----------------

        Take care: Two different sign conventions exist for the rotation terms.

        Conceptually they relate to whether we rotate the coordinate system
        or the "position vector" (the vector going from the coordinate system
        origin to the point being transformed, i.e. the point coordinates
        interpreted as vector coordinates).

        Switching between the "position vector" and "coordinate system"
        conventions is simply a matter of switching the sign of the rotation
        angles, which algebraically also translates into a transposition of
        the rotation matrix.

        Hence, as geodetic constants should preferably be referred to exactly
        as published, the "convention" option provides the ability to switch
        between the conventions.

    ***************************************************************************/
    struct pj_opaque_helmert *Q = (struct pj_opaque_helmert *)P->opaque;

    double f, t, p;    /* phi/fi , theta, psi  */
    double cf, ct, cp; /* cos (fi, theta, psi) */
    double sf, st, sp; /* sin (fi, theta, psi) */

    /* rename   (omega, phi, kappa)   to   (fi, theta, psi)   */
    f = Q->opk.o;
    t = Q->opk.p;
    p = Q->opk.k;

    /* Those equations are given assuming coordinate frame convention. */
    /* For the position vector convention, we transpose the matrix just after.
     */
    if (Q->exact) {
        cf = cos(f);
        sf = sin(f);
        ct = cos(t);
        st = sin(t);
        cp = cos(p);
        sp = sin(p);

        R00 = ct * cp;
        R01 = cf * sp + sf * st * cp;
        R02 = sf * sp - cf * st * cp;

        R10 = -ct * sp;
        R11 = cf * cp - sf * st * sp;
        R12 = sf * cp + cf * st * sp;

        R20 = st;
        R21 = -sf * ct;
        R22 = cf * ct;
    } else {
        R00 = 1;
        R01 = p;
        R02 = -t;

        R10 = -p;
        R11 = 1;
        R12 = f;

        R20 = t;
        R21 = -f;
        R22 = 1;
    }

    /*
        For comparison: Description from Engsager/Poder implementation
        in set_dtm_1.c (trlib)

        DATUM SHIFT:
        TO = scale * ROTZ * ROTY * ROTX * FROM + TRANSLA

             ( cz sz 0)         (cy 0 -sy)         (1   0  0)
        ROTZ=(-sz cz 0),   ROTY=(0  1   0),   ROTX=(0  cx sx)
             (  0  0 1)         (sy 0  cy)         (0 -sx cx)

        trp->r11  =  cos_ry*cos_rz;
        trp->r12  =  cos_rx*sin_rz + sin_rx*sin_ry*cos_rz;
        trp->r13  =  sin_rx*sin_rz - cos_rx*sin_ry*cos_rz;

        trp->r21  = -cos_ry*sin_rz;
        trp->r22  =  cos_rx*cos_rz - sin_rx*sin_ry*sin_rz;
        trp->r23  =  sin_rx*cos_rz + cos_rx*sin_ry*sin_rz;

        trp->r31  =  sin_ry;
        trp->r32  = -sin_rx*cos_ry;
        trp->r33  =  cos_rx*cos_ry;

        trp->scale = 1.0 + scale;
    */

    if (Q->is_position_vector) {
        double r;
        r = R01;
        R01 = R10;
        R10 = r;
        r = R02;
        R02 = R20;
        R20 = r;
        r = R12;
        R12 = R21;
        R21 = r;
    }

    /* some debugging output */
    if (proj_log_level(P->ctx, PJ_LOG_TELL) >= PJ_LOG_TRACE) {
        proj_log_trace(P, "Rotation Matrix:");
        proj_log_trace(P, "  | % 6.6g  % 6.6g  % 6.6g |", R00, R01, R02);
        proj_log_trace(P, "  | % 6.6g  % 6.6g  % 6.6g |", R10, R11, R12);
        proj_log_trace(P, "  | % 6.6g  % 6.6g  % 6.6g |", R20, R21, R22);
    }
}

/***********************************************************************/
static PJ_XY helmert_forward(PJ_LP lp, PJ *P) {
    /***********************************************************************/
    struct pj_opaque_helmert *Q = (struct pj_opaque_helmert *)P->opaque;
    PJ_COORD point = {{0, 0, 0, 0}};
    double x, y, cr, sr;
    point.lp = lp;

    cr = cos(Q->theta) * Q->scale;
    sr = sin(Q->theta) * Q->scale;
    x = point.xy.x;
    y = point.xy.y;

    point.xy.x = cr * x + sr * y + Q->xyz_0.x;
    point.xy.y = -sr * x + cr * y + Q->xyz_0.y;

    return point.xy;
}

/***********************************************************************/
static PJ_LP helmert_reverse(PJ_XY xy, PJ *P) {
    /***********************************************************************/
    struct pj_opaque_helmert *Q = (struct pj_opaque_helmert *)P->opaque;
    PJ_COORD point = {{0, 0, 0, 0}};
    double x, y, sr, cr;
    point.xy = xy;

    cr = cos(Q->theta) / Q->scale;
    sr = sin(Q->theta) / Q->scale;
    x = point.xy.x - Q->xyz_0.x;
    y = point.xy.y - Q->xyz_0.y;

    point.xy.x = x * cr - y * sr;
    point.xy.y = x * sr + y * cr;

    return point.lp;
}

/***********************************************************************/
static PJ_XYZ helmert_forward_3d(PJ_LPZ lpz, PJ *P) {
    /***********************************************************************/
    struct pj_opaque_helmert *Q = (struct pj_opaque_helmert *)P->opaque;
    PJ_COORD point = {{0, 0, 0, 0}};
    double X, Y, Z, scale;

    point.lpz = lpz;

    if (Q->fourparam) {
        const auto xy = helmert_forward(point.lp, P);
        point.xy = xy;
        return point.xyz;
    }

    if (Q->no_rotation && Q->scale == 0) {
        point.xyz.x = lpz.lam + Q->xyz.x;
        point.xyz.y = lpz.phi + Q->xyz.y;
        point.xyz.z = lpz.z + Q->xyz.z;
        return point.xyz;
    }

    scale = 1 + Q->scale * 1e-6;

    X = lpz.lam - Q->refp.x;
    Y = lpz.phi - Q->refp.y;
    Z = lpz.z - Q->refp.z;

    point.xyz.x = scale * (R00 * X + R01 * Y + R02 * Z);
    point.xyz.y = scale * (R10 * X + R11 * Y + R12 * Z);
    point.xyz.z = scale * (R20 * X + R21 * Y + R22 * Z);

    point.xyz.x += Q->xyz.x; /* for Molodensky-Badekas, Q->xyz already
                                incorporates the Q->refp offset */
    point.xyz.y += Q->xyz.y;
    point.xyz.z += Q->xyz.z;

    return point.xyz;
}

/***********************************************************************/
static PJ_LPZ helmert_reverse_3d(PJ_XYZ xyz, PJ *P) {
    /***********************************************************************/
    struct pj_opaque_helmert *Q = (struct pj_opaque_helmert *)P->opaque;
    PJ_COORD point = {{0, 0, 0, 0}};
    double X, Y, Z, scale;

    point.xyz = xyz;

    if (Q->fourparam) {
        const auto lp = helmert_reverse(point.xy, P);
        point.lp = lp;
        return point.lpz;
    }

    if (Q->no_rotation && Q->scale == 0) {
        point.xyz.x = xyz.x - Q->xyz.x;
        point.xyz.y = xyz.y - Q->xyz.y;
        point.xyz.z = xyz.z - Q->xyz.z;
        return point.lpz;
    }

    scale = 1 + Q->scale * 1e-6;

    /* Unscale and deoffset */
    X = (xyz.x - Q->xyz.x) / scale;
    Y = (xyz.y - Q->xyz.y) / scale;
    Z = (xyz.z - Q->xyz.z) / scale;

    /* Inverse rotation through transpose multiplication */
    point.xyz.x = (R00 * X + R10 * Y + R20 * Z) + Q->refp.x;
    point.xyz.y = (R01 * X + R11 * Y + R21 * Z) + Q->refp.y;
    point.xyz.z = (R02 * X + R12 * Y + R22 * Z) + Q->refp.z;

    return point.lpz;
}

static void helmert_forward_4d(PJ_COORD &point, PJ *P) {
    struct pj_opaque_helmert *Q = (struct pj_opaque_helmert *)P->opaque;

    /* We only need to rebuild the rotation matrix if the
     * observation time is different from the last call */
    double t_obs = (point.xyzt.t == HUGE_VAL) ? Q->t_epoch : point.xyzt.t;
    if (t_obs != Q->t_obs) {
        Q->t_obs = t_obs;
        update_parameters(P);
        build_rot_matrix(P);
    }

    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
    const auto xyz = helmert_forward_3d(point.lpz, P);
    point.xyz = xyz;
}

static void helmert_reverse_4d(PJ_COORD &point, PJ *P) {
    struct pj_opaque_helmert *Q = (struct pj_opaque_helmert *)P->opaque;

    /* We only need to rebuild the rotation matrix if the
     * observation time is different from the last call */
    double t_obs = (point.xyzt.t == HUGE_VAL) ? Q->t_epoch : point.xyzt.t;
    if (t_obs != Q->t_obs) {
        Q->t_obs = t_obs;
        update_parameters(P);
        build_rot_matrix(P);
    }

    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
    const auto lpz = helmert_reverse_3d(point.xyz, P);
    point.lpz = lpz;
}

/* Arcsecond to radians */
#define ARCSEC_TO_RAD (DEG_TO_RAD / 3600.0)

static PJ *init_helmert_six_parameters(PJ *P) {
    struct pj_opaque_helmert *Q = static_cast<struct pj_opaque_helmert *>(
        calloc(1, sizeof(struct pj_opaque_helmert)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = (void *)Q;

    /* In most cases, we work on 3D cartesian coordinates */
    P->left = PJ_IO_UNITS_CARTESIAN;
    P->right = PJ_IO_UNITS_CARTESIAN;

    /* Translations */
    if (pj_param(P->ctx, P->params, "tx").i)
        Q->xyz_0.x = pj_param(P->ctx, P->params, "dx").f;

    if (pj_param(P->ctx, P->params, "ty").i)
        Q->xyz_0.y = pj_param(P->ctx, P->params, "dy").f;

    if (pj_param(P->ctx, P->params, "tz").i)
        Q->xyz_0.z = pj_param(P->ctx, P->params, "dz").f;

    /* Rotations */
    if (pj_param(P->ctx, P->params, "trx").i)
        Q->opk_0.o = pj_param(P->ctx, P->params, "drx").f * ARCSEC_TO_RAD;

    if (pj_param(P->ctx, P->params, "try").i)
        Q->opk_0.p = pj_param(P->ctx, P->params, "dry").f * ARCSEC_TO_RAD;

    if (pj_param(P->ctx, P->params, "trz").i)
        Q->opk_0.k = pj_param(P->ctx, P->params, "drz").f * ARCSEC_TO_RAD;

    /* Use small angle approximations? */
    if (pj_param(P->ctx, P->params, "bexact").i)
        Q->exact = 1;

    return P;
}

static PJ *read_convention(PJ *P) {

    struct pj_opaque_helmert *Q = (struct pj_opaque_helmert *)P->opaque;

    /* In case there are rotational terms, we require an explicit convention
     * to be provided. */
    if (!Q->no_rotation) {
        const char *convention = pj_param(P->ctx, P->params, "sconvention").s;
        if (!convention) {
            proj_log_error(P, _("helmert: missing 'convention' argument"));
            return pj_default_destructor(P, PROJ_ERR_INVALID_OP_MISSING_ARG);
        }
        if (strcmp(convention, "position_vector") == 0) {
            Q->is_position_vector = 1;
        } else if (strcmp(convention, "coordinate_frame") == 0) {
            Q->is_position_vector = 0;
        } else {
            proj_log_error(
                P, _("helmert: invalid value for 'convention' argument"));
            return pj_default_destructor(P,
                                         PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }

        /* historically towgs84 in PROJ has always been using position_vector
         * convention. Accepting coordinate_frame would be confusing. */
        if (pj_param_exists(P->params, "towgs84")) {
            if (!Q->is_position_vector) {
                proj_log_error(P, _("helmert: towgs84 should only be used with "
                                    "convention=position_vector"));
                return pj_default_destructor(
                    P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
            }
        }
    }

    return P;
}

/***********************************************************************/
PJ *PJ_TRANSFORMATION(helmert, 0) {
    /***********************************************************************/

    struct pj_opaque_helmert *Q;

    if (!init_helmert_six_parameters(P)) {
        return nullptr;
    }

    /* In the 2D case, the coordinates are projected */
    if (pj_param_exists(P->params, "theta")) {
        P->left = PJ_IO_UNITS_PROJECTED;
        P->right = PJ_IO_UNITS_PROJECTED;
        P->fwd = helmert_forward;
        P->inv = helmert_reverse;
    }

    P->fwd4d = helmert_forward_4d;
    P->inv4d = helmert_reverse_4d;
    P->fwd3d = helmert_forward_3d;
    P->inv3d = helmert_reverse_3d;

    Q = (struct pj_opaque_helmert *)P->opaque;

    /* Detect obsolete transpose flag and error out if found */
    if (pj_param(P->ctx, P->params, "ttranspose").i) {
        proj_log_error(P, _("helmert: 'transpose' argument is no longer valid. "
                            "Use convention=position_vector/coordinate_frame"));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }

    /* Support the classic PROJ towgs84 parameter, but allow later overrides.*/
    /* Note that if towgs84 is specified, the datum_params array is set up   */
    /* for us automagically by the pj_datum_set call in pj_init_ctx */
    if (pj_param_exists(P->params, "towgs84")) {
        Q->xyz_0.x = P->datum_params[0];
        Q->xyz_0.y = P->datum_params[1];
        Q->xyz_0.z = P->datum_params[2];

        Q->opk_0.o = P->datum_params[3];
        Q->opk_0.p = P->datum_params[4];
        Q->opk_0.k = P->datum_params[5];

        /* We must undo conversion to absolute scale from pj_datum_set */
        if (0 == P->datum_params[6])
            Q->scale_0 = 0;
        else
            Q->scale_0 = (P->datum_params[6] - 1) * 1e6;
    }

    if (pj_param(P->ctx, P->params, "ttheta").i) {
        Q->theta_0 = pj_param(P->ctx, P->params, "dtheta").f * ARCSEC_TO_RAD;
        Q->fourparam = 1;
        Q->scale_0 = 1.0; /* default scale for the 4-param shift */
    }

    /* Scale */
    if (pj_param(P->ctx, P->params, "ts").i) {
        Q->scale_0 = pj_param(P->ctx, P->params, "ds").f;
        if (Q->scale_0 <= -1.0e6) {
            proj_log_error(P, _("helmert: invalid value for s."));
            return pj_default_destructor(P,
                                         PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }
        if (pj_param(P->ctx, P->params, "ttheta").i && Q->scale_0 == 0.0) {
            proj_log_error(P, _("helmert: invalid value for s."));
            return pj_default_destructor(P,
                                         PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }
    }

    /* Translation rates */
    if (pj_param(P->ctx, P->params, "tdx").i)
        Q->dxyz.x = pj_param(P->ctx, P->params, "ddx").f;

    if (pj_param(P->ctx, P->params, "tdy").i)
        Q->dxyz.y = pj_param(P->ctx, P->params, "ddy").f;

    if (pj_param(P->ctx, P->params, "tdz").i)
        Q->dxyz.z = pj_param(P->ctx, P->params, "ddz").f;

    /* Rotations rates */
    if (pj_param(P->ctx, P->params, "tdrx").i)
        Q->dopk.o = pj_param(P->ctx, P->params, "ddrx").f * ARCSEC_TO_RAD;

    if (pj_param(P->ctx, P->params, "tdry").i)
        Q->dopk.p = pj_param(P->ctx, P->params, "ddry").f * ARCSEC_TO_RAD;

    if (pj_param(P->ctx, P->params, "tdrz").i)
        Q->dopk.k = pj_param(P->ctx, P->params, "ddrz").f * ARCSEC_TO_RAD;

    if (pj_param(P->ctx, P->params, "tdtheta").i)
        Q->dtheta = pj_param(P->ctx, P->params, "ddtheta").f * ARCSEC_TO_RAD;

    /* Scale rate */
    if (pj_param(P->ctx, P->params, "tds").i)
        Q->dscale = pj_param(P->ctx, P->params, "dds").f;

    /* Epoch */
    if (pj_param(P->ctx, P->params, "tt_epoch").i)
        Q->t_epoch = pj_param(P->ctx, P->params, "dt_epoch").f;

    Q->xyz = Q->xyz_0;
    Q->opk = Q->opk_0;
    Q->scale = Q->scale_0;
    Q->theta = Q->theta_0;

    if ((Q->opk.o == 0) && (Q->opk.p == 0) && (Q->opk.k == 0) &&
        (Q->dopk.o == 0) && (Q->dopk.p == 0) && (Q->dopk.k == 0)) {
        Q->no_rotation = 1;
    }

    if (!read_convention(P)) {
        return nullptr;
    }

    /* Let's help with debugging */
    if (proj_log_level(P->ctx, PJ_LOG_TELL) >= PJ_LOG_TRACE) {
        proj_log_trace(P, "Helmert parameters:");
        proj_log_trace(P, "x=  %8.5f  y=  %8.5f  z=  %8.5f", Q->xyz.x, Q->xyz.y,
                       Q->xyz.z);
        proj_log_trace(P, "rx= %8.5f  ry= %8.5f  rz= %8.5f",
                       Q->opk.o / ARCSEC_TO_RAD, Q->opk.p / ARCSEC_TO_RAD,
                       Q->opk.k / ARCSEC_TO_RAD);
        proj_log_trace(P, "s=  %8.5f  exact=%d%s", Q->scale, Q->exact,
                       Q->no_rotation ? ""
                       : Q->is_position_vector
                           ? "  convention=position_vector"
                           : "  convention=coordinate_frame");
        proj_log_trace(P, "dx= %8.5f  dy= %8.5f  dz= %8.5f", Q->dxyz.x,
                       Q->dxyz.y, Q->dxyz.z);
        proj_log_trace(P, "drx=%8.5f  dry=%8.5f  drz=%8.5f", Q->dopk.o,
                       Q->dopk.p, Q->dopk.k);
        proj_log_trace(P, "ds= %8.5f  t_epoch=%8.5f", Q->dscale, Q->t_epoch);
    }

    update_parameters(P);
    build_rot_matrix(P);

    return P;
}

/***********************************************************************/
PJ *PJ_TRANSFORMATION(molobadekas, 0) {
    /***********************************************************************/

    struct pj_opaque_helmert *Q;

    if (!init_helmert_six_parameters(P)) {
        return nullptr;
    }

    P->fwd3d = helmert_forward_3d;
    P->inv3d = helmert_reverse_3d;

    Q = (struct pj_opaque_helmert *)P->opaque;

    /* Scale */
    if (pj_param(P->ctx, P->params, "ts").i) {
        Q->scale_0 = pj_param(P->ctx, P->params, "ds").f;
    }

    Q->opk = Q->opk_0;
    Q->scale = Q->scale_0;

    if (!read_convention(P)) {
        return nullptr;
    }

    /* Reference point */
    if (pj_param(P->ctx, P->params, "tpx").i)
        Q->refp.x = pj_param(P->ctx, P->params, "dpx").f;

    if (pj_param(P->ctx, P->params, "tpy").i)
        Q->refp.y = pj_param(P->ctx, P->params, "dpy").f;

    if (pj_param(P->ctx, P->params, "tpz").i)
        Q->refp.z = pj_param(P->ctx, P->params, "dpz").f;

    /* Let's help with debugging */
    if (proj_log_level(P->ctx, PJ_LOG_TELL) >= PJ_LOG_TRACE) {
        proj_log_trace(P, "Molodensky-Badekas parameters:");
        proj_log_trace(P, "x=  %8.5f  y=  %8.5f  z=  %8.5f", Q->xyz_0.x,
                       Q->xyz_0.y, Q->xyz_0.z);
        proj_log_trace(P, "rx= %8.5f  ry= %8.5f  rz= %8.5f",
                       Q->opk.o / ARCSEC_TO_RAD, Q->opk.p / ARCSEC_TO_RAD,
                       Q->opk.k / ARCSEC_TO_RAD);
        proj_log_trace(P, "s=  %8.5f  exact=%d%s", Q->scale, Q->exact,
                       Q->is_position_vector ? "  convention=position_vector"
                                             : "  convention=coordinate_frame");
        proj_log_trace(P, "px= %8.5f  py= %8.5f  pz= %8.5f", Q->refp.x,
                       Q->refp.y, Q->refp.z);
    }

    /* as an optimization, we incorporate the refp in the translation terms */
    Q->xyz_0.x += Q->refp.x;
    Q->xyz_0.y += Q->refp.y;
    Q->xyz_0.z += Q->refp.z;

    Q->xyz = Q->xyz_0;

    build_rot_matrix(P);

    return P;
}
