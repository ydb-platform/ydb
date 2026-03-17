#include <math.h>

#include "deepconst.h"

/* @(#) $Id: thetag.c,v 1.3 2000/10/07 05:12:17 ecdowney Exp $ */


/*
 *      FUNCTION THETAG(EP)
 *      COMMON /E1/XMO,XNODEO,OMEGAO,EO,XINCL,XNO,XNDT2O,XNDD6O,BSTAR,
 *     1 X,Y,Z,XDOT,YDOT,ZDOT,EPOCH,DS50
 *      DOUBLE PRECISION EPOCH,D,THETA,TWOPI,YR,TEMP,EP,DS50
 *      TWOPI=6.28318530717959D0
 *      YR=(EP+2.D-7)*1.D-3
 *      JY=YR
 *      YR=JY
 *      D=EP-YR*1.D3
 *      IF(JY.LT.10) JY=JY+80
 *      N=(JY-69)/4
 *      IF(JY.LT.70) N=(JY-72)/4
 *      DS50=7305.D0 + 365.D0*(JY-70) +N + D
 *      THETA=1.72944494D0 + 6.3003880987D0*DS50
 *      TEMP=THETA/TWOPI
 *      I=TEMP
 *      TEMP=I
 *      THETAG=THETA-TEMP*TWOPI
 *      IF(THETAG.LT.0.D0) THETAG=THETAG+TWOPI
 *      RETURN
 *      END
 */

/*       FUNCTION THETAG(EP) */
double
thetag(double EP, double *DS50)
{
    int JY, N, I;
    double YR, D, THETA, TEMP, THETAG;

    YR = (EP + 2.0E-7) * 1.0E-3;

    JY = (int) YR;

    YR = JY;

    D = EP - YR * 1.0E3;

    if(JY < 10)
	JY += 80;

    N = (JY - 69) / 4;

    if(JY < 70)
	N = (JY - 72) / 4;

/*    printf("N = %d\n", N); */

    *DS50 = 7305.0 + 365.0 * (JY-70) + N + D;

/*    printf("DS50 = %f\n", *DS50); */

    THETA = 1.72944494 + 6.3003880987 * *DS50;

/*    printf("THETA = %f\n", THETA); */

    TEMP = THETA / TWOPI;

    I = (int)TEMP;
    TEMP = I;

    THETAG = THETA - TEMP * TWOPI;

    if(THETAG < 0.0)
	THETAG += TWOPI;

    return THETAG;
}

#if 0
void main(int argc, char **argv) {
    double ds50, gwa;

    if(argc >= 2) {
	gwa = thetag(atof(argv[1]), &ds50);
	printf("%f, %f\n", gwa, ds50);
    }
}
#endif

