#include "proj_internal.h"
#include <math.h>

/* meridional distance for ellipsoid and inverse using 6th-order expansion in
** the third flattening n.  This gives full double precision accuracy for |f|
** <= 1/150.
*/

double *pj_enfn(double n) {

    int Lmax = int(AuxLat::ORDER);
    // 2*Lmax for the Fourier coeffs for each direction of conversion + 1 for
    // overall multiplier.
    double *en;
    en = (double *)malloc((2 * Lmax + 1) * sizeof(double));
    if (nullptr == en)
        return nullptr;
    en[0] = pj_rectifying_radius(n);
    pj_auxlat_coeffs(n, AuxLat::GEOGRAPHIC, AuxLat::RECTIFYING, en + 1);
    pj_auxlat_coeffs(n, AuxLat::RECTIFYING, AuxLat::GEOGRAPHIC, en + 1 + Lmax);
    return en;
}

double pj_mlfn(double phi, double sphi, double cphi, const double *en) {
    return en[0] * pj_auxlat_convert(phi, sphi, cphi, en + 1);
}

double pj_inv_mlfn(double mu, const double *en) {
    int Lmax = int(AuxLat::ORDER);
    return pj_auxlat_convert(mu / en[0], en + 1 + Lmax);
}
