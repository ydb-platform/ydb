/* Determine latitude angle phi-2. */

#include <algorithm>
#include <limits>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

double pj_sinhpsi2tanphi(PJ_CONTEXT *ctx, const double taup, const double e) {
    /***************************************************************************
     * Convert tau' = sinh(psi) = tan(chi) to tau = tan(phi).  The code is taken
     * from GeographicLib::Math::tauf(taup, e).
     *
     * Here
     *   phi = geographic latitude (radians)
     * psi is the isometric latitude
     *   psi = asinh(tan(phi)) - e * atanh(e * sin(phi))
     *       = asinh(tan(chi))
     * chi is the conformal latitude
     *
     * The representation of latitudes via their tangents, tan(phi) and
     * tan(chi), maintains full *relative* accuracy close to latitude = 0 and
     * +/- pi/2. This is sometimes important, e.g., to compute the scale of the
     * transverse Mercator projection which involves cos(phi)/cos(chi) tan(phi)
     *
     * From Karney (2011), Eq. 7,
     *
     *   tau' = sinh(psi) = sinh(asinh(tan(phi)) - e * atanh(e * sin(phi)))
     *        = tan(phi) * cosh(e * atanh(e * sin(phi))) -
     *          sec(phi) * sinh(e * atanh(e * sin(phi)))
     *        = tau * sqrt(1 + sigma^2) - sqrt(1 + tau^2) * sigma
     * where
     *   sigma = sinh(e * atanh( e * tau / sqrt(1 + tau^2) ))
     *
     * For e small,
     *
     *    tau' = (1 - e^2) * tau
     *
     * The relation tau'(tau) can therefore by reliably inverted by Newton's
     * method with
     *
     *    tau = tau' / (1 - e^2)
     *
     * as an initial guess.  Newton's method requires dtau'/dtau.  Noting that
     *
     *   dsigma/dtau = e^2 * sqrt(1 + sigma^2) /
     *                 (sqrt(1 + tau^2) * (1 + (1 - e^2) * tau^2))
     *   d(sqrt(1 + tau^2))/dtau = tau / sqrt(1 + tau^2)
     *
     * we have
     *
     *   dtau'/dtau = (1 - e^2) * sqrt(1 + tau'^2) * sqrt(1 + tau^2) /
     *                (1 + (1 - e^2) * tau^2)
     *
     * This works fine unless tau^2 and tau'^2 overflows.  This may be partially
     * cured by writing, e.g., sqrt(1 + tau^2) as hypot(1, tau).  However, nan
     * will still be generated with tau' = inf, since (inf - inf) will appear in
     * the Newton iteration.
     *
     * If we note that for sufficiently large |tau|, i.e., |tau| >= 2/sqrt(eps),
     * sqrt(1 + tau^2) = |tau| and
     *
     *   tau' = exp(- e * atanh(e)) * tau
     *
     * So
     *
     *   tau = exp(e * atanh(e)) * tau'
     *
     * can be returned unless |tau| >= 2/sqrt(eps); this then avoids overflow
     * problems for large tau' and returns the correct result for tau' = +/-inf
     * and nan.
     *
     * Newton's method usually take 2 iterations to converge to double precision
     * accuracy (for WGS84 flattening).  However only 1 iteration is needed for
     * |chi| < 3.35 deg.  In addition, only 1 iteration is needed for |chi| >
     * 89.18 deg (tau' > 70), if tau = exp(e * atanh(e)) * tau' is used as the
     * starting guess.
     *
     * For small flattening, |f| <= 1/50, the series expansion in n can be
     * used:
     *
     * Assuming n = e^2 / (1 + sqrt(1 - e^2))^2 is passed as an argument
     *
     *   double F[int(AuxLat::AUXORDER)];
     *   pj_auxlat_coeffs(n, AuxLat::CONFORMAL, AuxLat::GEOGRAPHIC, F);
     *   double sphi, cphi;
     *   //                schi                   cchi
     *   pj_auxlat_convert(taup/hypot(1.0, taup), 1/hypot(1.0, taup),
     *                     sphi, cphi, F);
     *   return sphi/cphi;
     **************************************************************************/
    constexpr int numit = 5;
    // min iterations = 1, max iterations = 2; mean = 1.954
    static const double rooteps = sqrt(std::numeric_limits<double>::epsilon());
    static const double tol = rooteps / 10; // the criterion for Newton's method
    static const double tmax =
        2 / rooteps; // threshold for large arg limit exact
    const double e2m = 1 - e * e;
    const double stol = tol * std::max(1.0, fabs(taup));
    // The initial guess.  70 corresponds to chi = 89.18 deg (see above)
    double tau = fabs(taup) > 70 ? taup * exp(e * atanh(e)) : taup / e2m;
    if (!(fabs(tau) < tmax)) // handles +/-inf and nan and e = 1
        return tau;
    // If we need to deal with e > 1, then we could include:
    // if (e2m < 0) return std::numeric_limits<double>::quiet_NaN();
    int i = numit;
    for (; i; --i) {
        double tau1 = sqrt(1 + tau * tau);
        double sig = sinh(e * atanh(e * tau / tau1));
        double taupa = sqrt(1 + sig * sig) * tau - sig * tau1;
        double dtau = ((taup - taupa) * (1 + e2m * (tau * tau)) /
                       (e2m * tau1 * sqrt(1 + taupa * taupa)));
        tau += dtau;
        if (!(fabs(dtau) >= stol)) // backwards test to allow nans to succeed.
            break;
    }
    if (i == 0)
        proj_context_errno_set(ctx, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    return tau;
}

/*****************************************************************************/
double pj_phi2(PJ_CONTEXT *ctx, const double ts0, const double e) {
    /****************************************************************************
     * Determine latitude angle phi-2.
     * Inputs:
     *   ts = exp(-psi) where psi is the isometric latitude (dimensionless)
     *        this variable is defined in Snyder (1987), Eq. (7-10)
     *   e = eccentricity of the ellipsoid (dimensionless)
     * Output:
     *   phi = geographic latitude (radians)
     * Here isometric latitude is defined by
     *   psi = log( tan(pi/4 + phi/2) *
     *              ( (1 - e*sin(phi)) / (1 + e*sin(phi)) )^(e/2) )
     *       = asinh(tan(phi)) - e * atanh(e * sin(phi))
     *       = asinh(tan(chi))
     *   chi = conformal latitude
     *
     * This routine converts t = exp(-psi) to
     *
     *   tau' = tan(chi) = sinh(psi) = (1/t - t)/2
     *
     * returns atan(sinpsi2tanphi(tau'))
     ***************************************************************************/
    return atan(pj_sinhpsi2tanphi(ctx, (1 / ts0 - ts0) / 2, e));
}
