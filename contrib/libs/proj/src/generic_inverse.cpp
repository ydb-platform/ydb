/******************************************************************************
 *
 * Project:  PROJ
 * Purpose:  Generic method to compute inverse projection from forward method
 * Author:   Even Rouault <even dot rouault at spatialys dot com>
 *
 ******************************************************************************
 * Copyright (c) 2018, Even Rouault <even dot rouault at spatialys dot com>
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
 ****************************************************************************/

#include "proj_internal.h"

#include <algorithm>
#include <cmath>

/** Compute (lam, phi) corresponding to input (xy.x, xy.y) for projection P.
 *
 * Uses Newton-Raphson method, extended to 2D variables, that is using
 * inversion of the Jacobian 2D matrix of partial derivatives. The derivatives
 * are estimated numerically from the P->fwd method evaluated at close points.
 *
 * Note: thresholds used have been verified to work with adams_ws2 and wink2
 *
 * Starts with initial guess provided by user in lpInitial
 */
PJ_LP pj_generic_inverse_2d(PJ_XY xy, PJ *P, PJ_LP lpInitial,
                            double deltaXYTolerance) {
    PJ_LP lp = lpInitial;
    double deriv_lam_X = 0;
    double deriv_lam_Y = 0;
    double deriv_phi_X = 0;
    double deriv_phi_Y = 0;
    for (int i = 0; i < 15; i++) {
        PJ_XY xyApprox = P->fwd(lp, P);
        const double deltaX = xyApprox.x - xy.x;
        const double deltaY = xyApprox.y - xy.y;
        if (fabs(deltaX) < deltaXYTolerance &&
            fabs(deltaY) < deltaXYTolerance) {
            return lp;
        }

        if (i == 0 || fabs(deltaX) > 1e-6 || fabs(deltaY) > 1e-6) {
            // Compute Jacobian matrix (only if we aren't close to the final
            // result to speed things a bit)
            PJ_LP lp2;
            PJ_XY xy2;
            const double dLam = lp.lam > 0 ? -1e-6 : 1e-6;
            lp2.lam = lp.lam + dLam;
            lp2.phi = lp.phi;
            xy2 = P->fwd(lp2, P);
            const double deriv_X_lam = (xy2.x - xyApprox.x) / dLam;
            const double deriv_Y_lam = (xy2.y - xyApprox.y) / dLam;

            const double dPhi = lp.phi > 0 ? -1e-6 : 1e-6;
            lp2.lam = lp.lam;
            lp2.phi = lp.phi + dPhi;
            xy2 = P->fwd(lp2, P);
            const double deriv_X_phi = (xy2.x - xyApprox.x) / dPhi;
            const double deriv_Y_phi = (xy2.y - xyApprox.y) / dPhi;

            // Inverse of Jacobian matrix
            const double det =
                deriv_X_lam * deriv_Y_phi - deriv_X_phi * deriv_Y_lam;
            if (det != 0) {
                deriv_lam_X = deriv_Y_phi / det;
                deriv_lam_Y = -deriv_X_phi / det;
                deriv_phi_X = -deriv_Y_lam / det;
                deriv_phi_Y = deriv_X_lam / det;
            }
        }

        // Limit the amplitude of correction to avoid overshoots due to
        // bad initial guess
        const double delta_lam = std::max(
            std::min(deltaX * deriv_lam_X + deltaY * deriv_lam_Y, 0.3), -0.3);
        lp.lam -= delta_lam;
        if (lp.lam < -M_PI)
            lp.lam = -M_PI;
        else if (lp.lam > M_PI)
            lp.lam = M_PI;

        const double delta_phi = std::max(
            std::min(deltaX * deriv_phi_X + deltaY * deriv_phi_Y, 0.3), -0.3);
        lp.phi -= delta_phi;
        if (lp.phi < -M_HALFPI)
            lp.phi = -M_HALFPI;
        else if (lp.phi > M_HALFPI)
            lp.phi = M_HALFPI;
    }
    proj_context_errno_set(P->ctx,
                           PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
    return lp;
}
