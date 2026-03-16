/* reduce argument to range +/- PI */
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

double adjlon(double longitude) {
    /* Let longitude slightly overshoot, to avoid spurious sign switching at the
     * date line */
    if (fabs(longitude) < M_PI + 1e-12)
        return longitude;

    /* adjust to 0..2pi range */
    longitude += M_PI;

    /* remove integral # of 'revolutions'*/
    longitude -= M_TWOPI * floor(longitude / M_TWOPI);

    /* adjust back to -pi..pi range */
    longitude -= M_PI;

    return longitude;
}
