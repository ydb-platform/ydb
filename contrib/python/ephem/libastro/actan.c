#include <math.h>

/* @(#) $Id: actan.c,v 1.3 2001/01/10 16:32:21 ecdowney Exp $ */

/* commonly in math.h, but not in strict ANSI C */
#ifndef M_PI
#define M_PI            3.14159265358979323846
#define M_PI_2          1.57079632679489661923
#endif

double
actan(double sinx, double cosx)
{
    double ret;

    ret = 0.0;
    if(cosx < 0.0) {
	ret = M_PI;
    } else if(cosx == 0.0) {
	if(sinx < 0.0) {
	    return 3.0 * M_PI_2;
	} else if(sinx == 0.0) {
	    return ret;
	} else /* sinx > 0.0 */ {
	    return M_PI_2;
	}
    } else /* cosx > 0.0 */ {
	if(sinx < 0.0) {
	    ret = 2.0 * M_PI;
	} else if(sinx == 0.0) {
	    return ret;
	}
    }

    return ret + atan(sinx / cosx);
}


#if 0

#define D(X) (180.0 * (X) / M_PI)

void main() {
    double a, b;

    a =  0.0; b =  2.0; printf("actan(%f, %f) = %f\n", a, b, D(actan(a, b)));
    a =  1.0; b =  2.0; printf("actan(%f, %f) = %f\n", a, b, D(actan(a, b)));
    a =  2.0; b =  2.0; printf("actan(%f, %f) = %f\n", a, b, D(actan(a, b)));
    a =  2.0; b =  1.0; printf("actan(%f, %f) = %f\n", a, b, D(actan(a, b)));
    a =  2.0; b =  0.0; printf("actan(%f, %f) = %f\n", a, b, D(actan(a, b)));
    a =  2.0; b = -1.0; printf("actan(%f, %f) = %f\n", a, b, D(actan(a, b)));
    a =  2.0; b = -2.0; printf("actan(%f, %f) = %f\n", a, b, D(actan(a, b)));
    a =  1.0; b = -2.0; printf("actan(%f, %f) = %f\n", a, b, D(actan(a, b)));
    a =  0.0; b = -2.0; printf("actan(%f, %f) = %f\n", a, b, D(actan(a, b)));
    a = -1.0; b = -2.0; printf("actan(%f, %f) = %f\n", a, b, D(actan(a, b)));
    a = -2.0; b = -2.0; printf("actan(%f, %f) = %f\n", a, b, D(actan(a, b)));
    a = -2.0; b = -1.0; printf("actan(%f, %f) = %f\n", a, b, D(actan(a, b)));
    a = -2.0; b =  0.0; printf("actan(%f, %f) = %f\n", a, b, D(actan(a, b)));
    a = -2.0; b =  1.0; printf("actan(%f, %f) = %f\n", a, b, D(actan(a, b)));
    a = -2.0; b =  2.0; printf("actan(%f, %f) = %f\n", a, b, D(actan(a, b)));
    a = -1.0; b =  2.0; printf("actan(%f, %f) = %f\n", a, b, D(actan(a, b)));
}

#endif /* 0 */

