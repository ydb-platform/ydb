/* determine small t */
#include "proj.h"
#include "proj_internal.h"
#include <math.h>

double pj_tsfn(double phi, double sinphi, double e) {
    /****************************************************************************
     * Determine function ts(phi) defined in Snyder (1987), Eq. (7-10)
     * Inputs:
     *   phi = geographic latitude (radians)
     *   e = eccentricity of the ellipsoid (dimensionless)
     * Output:
     *   ts = exp(-psi) where psi is the isometric latitude (dimensionless)
     *      = 1 / (tan(chi) + sec(chi))
     * Here isometric latitude is defined by
     *   psi = log( tan(pi/4 + phi/2) *
     *              ( (1 - e*sin(phi)) / (1 + e*sin(phi)) )^(e/2) )
     *       = asinh(tan(phi)) - e * atanh(e * sin(phi))
     *       = asinh(tan(chi))
     *   chi = conformal latitude
     ***************************************************************************/

    double cosphi = cos(phi);
    // exp(-asinh(tan(phi))) = 1 / (tan(phi) + sec(phi))
    //                       = cos(phi) / (1 + sin(phi)) good for phi > 0
    //                       = (1 - sin(phi)) / cos(phi) good for phi < 0
    return exp(e * atanh(e * sinphi)) *
           (sinphi > 0 ? cosphi / (1 + sinphi) : (1 - sinphi) / cosphi);
}
