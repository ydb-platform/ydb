/* determine constant small m */
#include "proj.h"
#include "proj_internal.h"
#include <math.h>
double pj_msfn(double sinphi, double cosphi, double es) {
    return (cosphi / sqrt(1. - es * sinphi * sinphi));
}
