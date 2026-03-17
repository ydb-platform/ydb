#include <stdlib.h>
#include <math.h>

#include "deepconst.h"
#include "satspec.h"

/* *      DEEP SPACE                                         31 OCT 80 */
/*       SUBROUTINE DEEP */
/*       COMMON/E1/XMO,XNODEO,OMEGAO,EO,XINCL,XNO,XNDT2O, */
/*      1           XNDD6O,BSTAR,X,Y,Z,XDOT,YDOT,ZDOT,EPOCH,DS50 */
/*       COMMON/C1/CK2,CK4,E6A,QOMS2T,S,TOTHRD, */
/*      1           XJ3,XKE,XKMPER,XMNPDA,AE */
/*       COMMON/C2/DE2RA,PI,PIO2,TWOPI,X3PIO2 */
/*       DOUBLE PRECISION EPOCH, DS50 */
/*       DOUBLE PRECISION */
/*      *     DAY,PREEP,XNODCE,ATIME,DELT,SAVTSN,STEP2,STEPN,STEPP */
/*       DATA              ZNS,           C1SS,          ZES/ */
/*      A                  1.19459E-5,    2.9864797E-6, .01675/ */
/*       DATA              ZNL,           C1L,           ZEL/ */
/*      A                  1.5835218E-4,  4.7968065E-7,  .05490/ */
/*       DATA              ZCOSIS,        ZSINIS,        ZSINGS/ */
/*      A                  .91744867,     .39785416,     -.98088458/ */
/*       DATA              ZCOSGS,        ZCOSHS,        ZSINHS/ */
/*      A                  .1945905,      1.0,           0.0/ */
/*       DATA Q22,Q31,Q33/1.7891679E-6,2.1460748E-6,2.2123015E-7/ */
/*       DATA G22,G32/5.7686396,0.95240898/ */
/*       DATA G44,G52/1.8014998,1.0508330/ */
/*       DATA G54/4.4108898/ */
/*       DATA ROOT22,ROOT32/1.7891679E-6,3.7393792E-7/ */
/*       DATA ROOT44,ROOT52/7.3636953E-9,1.1428639E-7/ */
/*       DATA ROOT54/2.1765803E-9/ */
/*       DATA THDT/4.3752691E-3/ */

#define XMO	(sat->elem->se_XMO)
#define XNODEO	(sat->elem->se_XNODEO)
#define OMEGAO	(sat->elem->se_OMEGAO)
#define EO	(sat->elem->se_EO)
#define XINCL	(sat->elem->se_XINCL)
#define XNO	(sat->elem->se_XNO)
#define XNDT20	(sat->elem->se_XNDT20)
#define XNDD60	(sat->elem->se_XNDD60)
#define BSTAR	(sat->elem->se_BSTAR)
#define EPOCH	(sat->elem->se_EPOCH)

#define ZNS	(1.19459E-5)
#define C1SS	(2.9864797E-6)
#define ZES	(.01675)
#define ZNL	(1.5835218E-4)
#define C1L	(4.7968065E-7)
#define ZEL	(.05490)
#define ZCOSIS	(.91744867)
#define ZSINIS	(.39785416)
#define ZSINGS	(-.98088458)
#define ZCOSGS	(.1945905)
#define ZCOSHS	(1.0)
#define ZSINHS	(0.0)

#define Q22	(1.7891679E-6)
#define Q31	(2.1460748E-6)
#define Q33	(2.2123015E-7)
#define G22	(5.7686396)
#define G32	(0.95240898)
#define G44	(1.8014998)
#define G52	(1.0508330)
#define G54	(4.4108898)
#define ROOT22	(1.7891679E-6)
#define ROOT32	(3.7393792E-7)
#define ROOT44	(7.3636953E-9)
#define ROOT52	(1.1428639E-7)
#define ROOT54	(2.1765803E-9)
#define THDT	(4.3752691E-3)

#define IRESFL	(sat->deep->deep_flags.IRESFL)
#define ISYNFL	(sat->deep->deep_flags.ISYNFL)

#define s_SINIQ	(sat->deep->deep_s_SINIQ)
#define s_COSIQ	(sat->deep->deep_s_COSIQ)
#define s_OMGDT	(sat->deep->deep_s_OMGDT)
#define ATIME	(sat->deep->deep_ATIME)
#define D2201	(sat->deep->deep_D2201)
#define D2211	(sat->deep->deep_D2211)
#define D3210	(sat->deep->deep_D3210)
#define D3222	(sat->deep->deep_D3222)
#define D4410	(sat->deep->deep_D4410)
#define D4422	(sat->deep->deep_D4422)
#define D5220	(sat->deep->deep_D5220)
#define D5232	(sat->deep->deep_D5232)
#define D5421	(sat->deep->deep_D5421)
#define D5433	(sat->deep->deep_D5433)
#define DEL1	(sat->deep->deep_DEL1)
#define DEL2	(sat->deep->deep_DEL2)
#define DEL3	(sat->deep->deep_DEL3)
#define E3	(sat->deep->deep_E3)
#define EE2	(sat->deep->deep_EE2)
#define FASX2	(sat->deep->deep_FASX2)
#define FASX4	(sat->deep->deep_FASX4)
#define FASX6	(sat->deep->deep_FASX6)
#define OMEGAQ	(sat->deep->deep_OMEGAQ)
#define PE	(sat->deep->deep_PE)
#define PINC	(sat->deep->deep_PINC)
#define PL	(sat->deep->deep_PL)
#define SAVTSN	(sat->deep->deep_SAVTSN)
#define SE2	(sat->deep->deep_SE2)
#define SE3	(sat->deep->deep_SE3)
#define SGH2	(sat->deep->deep_SGH2)
#define SGH3	(sat->deep->deep_SGH3)
#define SGH4	(sat->deep->deep_SGH4)
#define SGHL	(sat->deep->deep_SGHL)
#define SGHS	(sat->deep->deep_SGHS)
#define SH2	(sat->deep->deep_SH2)
#define SH3	(sat->deep->deep_SH3)
#define SHS	(sat->deep->deep_SHS)
#define SHL	(sat->deep->deep_SHL)
#define SI2	(sat->deep->deep_SI2)
#define SI3	(sat->deep->deep_SI3)
#define SL2	(sat->deep->deep_SL2)
#define SL3	(sat->deep->deep_SL3)
#define SL4	(sat->deep->deep_SL4)
#define SSE	(sat->deep->deep_SSE)
#define SSG	(sat->deep->deep_SSG)
#define SSH	(sat->deep->deep_SSH)
#define SSI	(sat->deep->deep_SSI)
#define SSL	(sat->deep->deep_SSL)
#define STEP2	(sat->deep->deep_STEP2)
#define STEPN	(sat->deep->deep_STEPN)
#define STEPP	(sat->deep->deep_STEPP)
#define THGR	(sat->deep->deep_THGR)
#define XFACT	(sat->deep->deep_XFACT)
#define XGH2	(sat->deep->deep_XGH2)
#define XGH3	(sat->deep->deep_XGH3)
#define XGH4	(sat->deep->deep_XGH4)
#define XH2	(sat->deep->deep_XH2)
#define XH3	(sat->deep->deep_XH3)
#define XI2	(sat->deep->deep_XI2)
#define XI3	(sat->deep->deep_XI3)
#define XL2	(sat->deep->deep_XL2)
#define XL3	(sat->deep->deep_XL3)
#define XL4	(sat->deep->deep_XL4)
#define XLAMO	(sat->deep->deep_XLAMO)
#define XLI	(sat->deep->deep_XLI)
#define XNI	(sat->deep->deep_XNI)
#define XNQ	(sat->deep->deep_XNQ)
#define XQNCL	(sat->deep->deep_XQNCL)
#define ZMOL	(sat->deep->deep_ZMOL)
#define ZMOS	(sat->deep->deep_ZMOS)

/* *     ENTRANCE FOR DEEP SPACE INITIALIZATION */

/*       ENTRY DPINIT(EQSQ,SINIQ,COSIQ,RTEQSQ,AO,COSQ2,SINOMO,COSOMO, */
/*      1         BSQ,XLLDOT,OMGDT,XNODOT,XNODP) */

void
dpinit(SatData *sat, double EQSQ, double SINIQ, double COSIQ,
	    double RTEQSQ, double AO, double COSQ2, double SINOMO,
	    double COSOMO, double BSQ, double XLLDOT, double OMGDT,
	    double XNODOT, double XNODP)
{
    double A1, A10, A2, A3, A4, A5, A6, A7, A8, A9, AINV2, AQNV, BFACT,
	C, CC, COSQ, CTEM, DAY, DS50, EOC, EQ, F220, F221, F311, F321, F322,
	F330, F441, F442, F522, F523, F542, F543, G200, G201, G211, G300,
	G310, G322, G410, G422, G520, G521, G532, G533, GAM, PREEP, S1, S2,
	S3, S4, S5, S6, S7, SE, SGH, SH, SI, SINI2, SINQ, SL, STEM, TEMP,
	TEMP1, X1, X2, X3, X4, X5, X6, X7, X8, XMAO, XNO2, XNODCE, XNOI,
	XPIDOT, Z1, Z11, Z12, Z13, Z2, Z21, Z22, Z23, Z3, Z31, Z32, Z33,
	ZCOSG, ZCOSGL, ZCOSH, ZCOSHL, ZCOSI, ZCOSIL, ZE, ZN, ZSING,
	ZSINGL, ZSINH, ZSINHL, ZSINI, ZSINIL, ZX, ZY;

    int c;
#if 0
    A1=A10=A2=A3=A4=A5=A6=A7=A8=A9=AINV2=AQNV=BFACT = signaling_nan();
    C=CC=COSQ=CTEM=DAY=DS50=EOC=EQ=F220=F221=F311=F321=F322 = signaling_nan();
    F330=F441=F442=F522=F523=F542=F543=G200=G201=G211=G300 = signaling_nan();
    G310=G322=G410=G422=G520=G521=G532=G533=GAM=PREEP=S1=S2 = signaling_nan();
    S3=S4=S5=S6=S7=SE=SGH=SH=SI=SINI2=SINQ=SL=STEM=TEMP = signaling_nan();
    TEMP1=X1=X2=X3=X4=X5=X6=X7=X8=XMAO=XNO2=XNODCE=XNOI = signaling_nan();
    XPIDOT=Z1=Z11=Z12=Z13=Z2=Z21=Z22=Z23=Z3=Z31=Z32=Z33 = signaling_nan();
    ZCOSG=ZCOSGL=ZCOSH=ZCOSHL=ZCOSI=ZCOSIL=ZE=ZMO=ZN=ZSING = signaling_nan();
    ZSINGL=ZSINH=ZSINHL=ZSINI=ZSINIL=ZX=ZY = signaling_nan();
#endif
    if(!sat->deep)
	sat->deep = (struct deep_data *) malloc(sizeof(struct deep_data));
    else
	return;

    /* init_deep(sat->deep); */
    PREEP = 0.0;

    ZCOSGL = ZCOSHL = ZCOSIL = ZSINGL = ZSINHL = ZSINIL = 0.0;

    /* Save some of the arguments, for use by dpsec() and dpper() */
    s_SINIQ = SINIQ;
    s_COSIQ = COSIQ;
    s_OMGDT = OMGDT;

    THGR = thetag(EPOCH, &DS50);

    EQ = EO;
    XNQ = XNODP;
    AQNV = 1.0/AO;
    XQNCL = XINCL;
    XMAO = XMO;
    XPIDOT = OMGDT + XNODOT;
    SINQ = sin(XNODEO);
    COSQ = cos(XNODEO);
    OMEGAQ = OMEGAO;

    /* INITIALIZE LUNAR SOLAR TERMS */

    DAY = DS50 + 18261.5;

    if(DAY != PREEP) {
	PREEP = DAY;
	XNODCE = 4.5236020 - 9.2422029E-4 * DAY;
	STEM = sin(XNODCE);
	CTEM = cos(XNODCE);
	ZCOSIL = .91375164 - .03568096 * CTEM;
	ZSINIL = sqrt(1.0 - ZCOSIL * ZCOSIL);
	ZSINHL = .089683511 * STEM / ZSINIL;
	ZCOSHL = sqrt(1.0 - ZSINHL * ZSINHL);
	C = 4.7199672 + .22997150 * DAY;
	GAM = 5.8351514 + .0019443680 * DAY;
	ZMOL = fmod(C-GAM, TWOPI);
	ZX = .39785416 * STEM / ZSINIL;
	ZY = ZCOSHL * CTEM + 0.91744867 * ZSINHL * STEM;
	ZX = actan(ZX, ZY);
	ZX = GAM + ZX - XNODCE;
	ZCOSGL = cos(ZX);
	ZSINGL = sin(ZX);
	ZMOS = 6.2565837 + .017201977 * DAY;
	ZMOS = fmod(ZMOS, TWOPI);
    }

    /* DO SOLAR TERMS */

    SAVTSN = 1.0E20;
    ZCOSG = ZCOSGS;
    ZSING = ZSINGS;
    ZCOSI = ZCOSIS;
    ZSINI = ZSINIS;
    ZCOSH = COSQ;
    ZSINH = SINQ;
    CC = C1SS;
    ZN = ZNS;
    ZE = ZES;
    XNOI = 1.0 / XNQ;

    for(c = 0; c < 2; c++) {
	A1 = ZCOSG * ZCOSH + ZSING * ZCOSI * ZSINH;
	A3 = -ZSING * ZCOSH + ZCOSG * ZCOSI * ZSINH;
	A7 = -ZCOSG * ZSINH + ZSING * ZCOSI * ZCOSH;
	A8 = ZSING * ZSINI;
	A9 = ZSING * ZSINH + ZCOSG * ZCOSI * ZCOSH;
	A10 = ZCOSG * ZSINI;
	A2 = COSIQ * A7 + SINIQ * A8;
	A4 = COSIQ * A9 + SINIQ * A10;
	A5 = - SINIQ * A7 + COSIQ * A8;
	A6 = - SINIQ * A9 + COSIQ * A10;

	X1 = A1 * COSOMO + A2 * SINOMO;
	X2 = A3 * COSOMO + A4 * SINOMO;
	X3 = - A1 * SINOMO + A2 * COSOMO;
	X4 = - A3 * SINOMO + A4 * COSOMO;
	X5 = A5 * SINOMO;
	X6 = A6 * SINOMO;
	X7 = A5 * COSOMO;
	X8 = A6 * COSOMO;

	Z31 = 12.0 * X1 * X1 -3.0 * X3 * X3;
	Z32 = 24.0 * X1 * X2 -6.0 * X3 * X4;
	Z33 = 12.0 * X2 * X2 -3.0 * X4 * X4;
	Z1 = 3.0 * (A1 * A1 + A2 * A2) + Z31 * EQSQ;
	Z2 = 6.0 * (A1 * A3 + A2 * A4) + Z32 * EQSQ;
	Z3 = 3.0 * (A3 * A3 + A4 * A4) + Z33 * EQSQ;
	Z11 = -6.0 * A1 * A5 + EQSQ * (-24.0 * X1 * X7 - 6.0 * X3 * X5);

	Z12 = -6.0 * (A1 * A6 + A3 * A5) +
	    EQSQ * (-24.0 * (X2 * X7 + X1 * X8) - 6.0 * (X3 * X6 + X4 * X5));

	Z13 = -6.0 * A3 * A6 + EQSQ * (-24.0 * X2 * X8 - 6.0 * X4 * X6);
	Z21 = 6.0 * A2 * A5 + EQSQ * (24.0 * X1 * X5 - 6.0 * X3 * X7);

	Z22 = 6.0 * (A4 * A5 + A2 * A6) +
	    EQSQ * (24.0 * (X2 * X5 + X1 * X6) - 6.0 * (X4 * X7 + X3 * X8));

	Z23 = 6.0 * A4 * A6 + EQSQ * (24.0 * X2 * X6 - 6.0 * X4 * X8);
	Z1 = Z1 + Z1 + BSQ * Z31;
	Z2 = Z2 + Z2 + BSQ * Z32;
	Z3 = Z3 + Z3 + BSQ * Z33;
	S3 = CC * XNOI;
	S2 = -.5 * S3 / RTEQSQ;
	S4 = S3 * RTEQSQ;
	S1 = -15.0 * EQ * S4;
	S5 = X1 * X3 + X2 * X4;
	S6 = X2 * X3 + X1 * X4;
	S7 = X2 * X4 - X1 * X3;
	SE = S1 * ZN * S5;
	SI = S2 * ZN * (Z11 + Z13);
	SL = -ZN * S3 * (Z1 + Z3 - 14.0 - 6.0 * EQSQ);
	SGH = S4 * ZN * (Z31 + Z33 - 6.0);
	SH = -ZN * S2 * (Z21 + Z23);

	if(XQNCL < 5.2359877E-2)
	    SH = 0.0;

	EE2 = 2.0 * S1 * S6;
	E3 = 2.0 * S1 * S7;
	XI2 = 2.0 * S2 * Z12;
	XI3 = 2.0 * S2 * (Z13 - Z11);
	XL2 = -2.0 * S3 * Z2;
	XL3 = -2.0 * S3 * (Z3 - Z1);
	XL4 = -2.0 * S3 * (-21.0 - 9.0 * EQSQ) * ZE;
	XGH2 = 2.0 * S4 * Z32;
	XGH3 = 2.0 * S4 * (Z33 - Z31);
	XGH4 = -18.0 * S4 * ZE;
	XH2 = -2.0 * S2 * Z22;
	XH3 = -2.0 * S2 * (Z23 - Z21);

	if(c == 0) {
	    /* DO LUNAR TERMS */
	    SSE = SE;
	    SSI = SI;
	    SSL = SL;
	    SSH = SH / SINIQ;
	    SSG = SGH - COSIQ * SSH;
	    SE2 = EE2;
	    SI2 = XI2;
	    SL2 = XL2;
	    SGH2 = XGH2;
	    SH2 = XH2;
	    SE3 = E3;
	    SI3 = XI3;
	    SL3 = XL3;
	    SGH3 = XGH3;
	    SH3 = XH3;
	    SL4 = XL4;
	    SGH4 = XGH4;

	    ZCOSG = ZCOSGL;
	    ZSING = ZSINGL;
	    ZCOSI = ZCOSIL;
	    ZSINI = ZSINIL;
	    ZCOSH = ZCOSHL * COSQ + ZSINHL * SINQ;
	    ZSINH = SINQ * ZCOSHL - COSQ * ZSINHL;
	    ZN = ZNL;
	    CC = C1L;
	    ZE = ZEL;
	}
    }

    SSE = SSE + SE;
    SSI = SSI + SI;
    SSL = SSL + SL;
    SSG = SSG + SGH - COSIQ / SINIQ * SH;
    SSH = SSH + SH / SINIQ;

    /* GEOPOTENTIAL RESONANCE INITIALIZATION FOR 12 HOUR ORBITS */

    IRESFL = 0;
    ISYNFL = 0;

    if(XNQ <= .0034906585 || XNQ >= .0052359877) {

	if(XNQ < (8.26E-3) || XNQ > (9.24E-3))
	    return;

	if(EQ < 0.5)
	    return;

	IRESFL = 1;
	EOC = EQ * EQSQ;
	G201 = -.306 - (EQ - .64) * .440;

	if(EQ <= (.65)) {
	    G211 = 3.616 - 13.247 * EQ + 16.290 * EQSQ;
	    G310 = -19.302 + 117.390 * EQ - 228.419 * EQSQ + 156.591 * EOC;
	    G322 = -18.9068 + 109.7927 * EQ - 214.6334 * EQSQ + 146.5816 * EOC;
	    G410 = -41.122 + 242.694 * EQ - 471.094 * EQSQ + 313.953 * EOC;
	    G422 = -146.407 + 841.880 * EQ - 1629.014 * EQSQ + 1083.435 * EOC;
	    G520 = -532.114 + 3017.977 * EQ - 5740 * EQSQ + 3708.276 * EOC;
	} else {
	    G211 = -72.099 + 331.819 * EQ - 508.738 * EQSQ + 266.724 * EOC;
	    G310 = -346.844 + 1582.851 * EQ - 2415.925 * EQSQ + 1246.113 * EOC;
	    G322 = -342.585 + 1554.908 * EQ - 2366.899 * EQSQ + 1215.972 * EOC;
	    G410 = -1052.797 + 4758.686 * EQ - 7193.992 * EQSQ +
		3651.957 * EOC;
	    G422 = -3581.69 + 16178.11 * EQ - 24462.77 * EQSQ + 12422.52 * EOC;

	    if(EQ > (.715))
		G520 = -5149.66 + 29936.92 * EQ - 54087.36 * EQSQ +
		    31324.56 * EOC;

	    G520 = 1464.74 - 4664.75 * EQ + 3763.64 * EQSQ;
	}

	if(EQ < (.7)) {
	    G533 = -919.2277 + 4988.61 * EQ - 9064.77 * EQSQ + 5542.21 * EOC;

	    G521 = -822.71072 + 4568.6173 * EQ - 8491.4146 * EQSQ +
		5337.524 * EOC;

	    G532 = -853.666 + 4690.25 * EQ - 8624.77 * EQSQ + 5341.4 * EOC;
	} else {
	    G533 = -37995.78 + 161616.52 * EQ - 229838.2 * EQSQ +
		109377.94 * EOC;

	    G521 = -51752.104 + 218913.95 * EQ - 309468.16 * EQSQ +
		146349.42 * EOC;

	    G532 = -40023.88 + 170470.89 * EQ - 242699.48 * EQSQ +
		115605.82 * EOC;
	}

	SINI2 = SINIQ * SINIQ;
	F220 = .75 * (1.0 + 2.0 * COSIQ + COSQ2);
	F221 = 1.5 * SINI2;
	F321 = 1.875 * SINIQ * (1.0 - 2.0 * COSIQ - 3.0 * COSQ2);
	F322 = -1.875 * SINIQ * (1.0 + 2.0 * COSIQ - 3.0 * COSQ2);
	F441 = 35.0 * SINI2 * F220;
	F442 = 39.3750 * SINI2 * SINI2;

	F522 = 9.84375 * SINIQ * (SINI2 * (1.0 - 2.0 * COSIQ - 5.0 * COSQ2) +
				  .33333333 * (-2.0 + 4.0 * COSIQ +
					       6.0 * COSQ2));

	F523 = SINIQ * (4.92187512 * SINI2 * (-2.0 - 4.0 * COSIQ +
					      10.0 * COSQ2) +
					      6.56250012 * (1.0 +
							    2.0 * COSIQ -
							    3.0 * COSQ2));

	F542 = 29.53125 * SINIQ * (2.0 - 8.0 * COSIQ +
				   COSQ2 * (-12.0 + 8.0 * COSIQ +
					    10.0 * COSQ2));

	F543 = 29.53125 * SINIQ * (-2.0 - 8.0 * COSIQ +
				   COSQ2 * (12.0 + 8.0 * COSIQ -
					    10.0 * COSQ2));

	XNO2 = XNQ * XNQ;
	AINV2 = AQNV * AQNV;
	TEMP1 = 3.0 * XNO2 * AINV2;
	TEMP = TEMP1 * ROOT22;
	D2201 = TEMP * F220 * G201;
	D2211 = TEMP * F221 * G211;
	TEMP1 = TEMP1 * AQNV;
	TEMP = TEMP1 * ROOT32;
	D3210 = TEMP * F321 * G310;
	D3222 = TEMP * F322 * G322;
	TEMP1 = TEMP1 * AQNV;
	TEMP = 2.0 * TEMP1 * ROOT44;
	D4410 = TEMP * F441 * G410;
	D4422 = TEMP * F442 * G422;
	TEMP1 = TEMP1 * AQNV;
	TEMP = TEMP1 * ROOT52;
	D5220 = TEMP * F522 * G520;
	D5232 = TEMP * F523 * G532;
	TEMP = 2.0 * TEMP1 * ROOT54;
	D5421 = TEMP * F542* G521;
	D5433 = TEMP * F543* G533;
	XLAMO = XMAO + XNODEO + XNODEO - THGR - THGR;
	BFACT = XLLDOT + XNODOT + XNODOT - THDT - THDT;
	BFACT = BFACT + SSL + SSH + SSH;
    } else {
	/* SYNCHRONOUS RESONANCE TERMS INITIALIZATION */

	IRESFL = 1;
	ISYNFL = 1;
	G200 = 1.0 + EQSQ * (-2.5 + .8125 * EQSQ);
	G310 = 1.0 + 2.0 * EQSQ;
	G300 = 1.0 + EQSQ * (-6.0 + 6.60937 * EQSQ);
	F220 = .75 * (1.0 + COSIQ) * (1.0 + COSIQ);
	F311 = .9375 * SINIQ * SINIQ * (1.0 + 3.0 * COSIQ) -
	    .75 * (1.0 + COSIQ);
	F330 = 1.0 + COSIQ;
	F330 = 1.875 * F330 * F330 * F330;
	DEL1 = 3.0 * XNQ * XNQ * AQNV * AQNV;
	DEL2 = 2.0 * DEL1 * F220 * G200 * Q22;
	DEL3 = 3.0 * DEL1 * F330 * G300 * Q33 * AQNV;
	DEL1 = DEL1 * F311 * G310 * Q31 * AQNV;
	FASX2 = .13130908;
	FASX4 = 2.8843198;
	FASX6 = .37448087;
	XLAMO = XMAO + XNODEO + OMEGAO - THGR;
	BFACT = XLLDOT + XPIDOT - THDT;
	BFACT = BFACT + SSL + SSG + SSH;

    }

    XFACT = BFACT - XNQ;

    XLI = XLAMO;
    XNI = XNQ;
    ATIME = 0.0;
    STEPP = 720.0;
    STEPN = -720.0;
    STEP2 = 259200.0;
}

/* ENTRANCE FOR DEEP SPACE SECULAR EFFECTS */

void
dpsec(SatData *sat, double *XLL, double *OMGASM, double *XNODES,
	   double *EM, double *XINC, double *XN, double T)
{
    double DELT, XL, TEMP, XOMI, X2OMI, X2LI, XLDOT;
    double XNDOT, XNDDT, FT;
    int state, iret, iretn, done;

    DELT = XLDOT = XNDOT = XNDDT = FT = 0.0;
    iret = iretn = 0;

#if 0
    DELT = XL = TEMP = XOMI = X2OMI = X2LI = XLDOT = signaling_nan();
    XNDOT = XNDDT = FT = signaling_nan();
#endif

    *XLL = *XLL + SSL * T;
    *OMGASM = *OMGASM + SSG * T;
    *XNODES = *XNODES + SSH * T;
    *EM = EO + SSE * T;
    *XINC = XINCL + SSI * T;

    if(*XINC < 0.0) {
	*XINC = -*XINC;
	*XNODES = *XNODES + PI;
	*OMGASM = *OMGASM - PI;
    }

    if(IRESFL == 0)
	return;

    state = 1;
    done = 0;
    while(!done) {
	/* printf("state = %d\n", state); */
	switch(state) {
	case 1:
	    /*
	     * Chunk #1
	     */
	    if(ATIME == 0.0 || (T >= 0.0 && ATIME < 0.0) ||
	       (T < 0.0 && ATIME >= 0.0)) {
		/*
		 * Chunk #10
		 */
		if(T >= 0.0)
		    DELT = STEPP;
		else
		    DELT = STEPN;

		ATIME = 0.0;
		XNI = XNQ;
		XLI = XLAMO;
		state = 4;
		break;
	    }

	    /* Fall through */
	case 2:
	    /*
	     * Chunk #2
	     */
	    if(fabs(T) < fabs(ATIME)) {
		/*
		 * Chunk #2
		 */
		if(T >= 0.0)
		    DELT = STEPN;
		else
		    DELT = STEPP;

		iret = 1;
		state = 8;
		break;
	    }

	    /*
	     * Chunk #3
	     */
	    if(T > 0.0)
		DELT = STEPP;
	    else
		DELT = STEPN;

	    /* fall through */
	case 4:
	    /*
	     * Chunk #4
	     */
	    if(fabs(T - ATIME) >= STEPP) {
		iret = 4;
		state = 8;
	    } else {
		/*
		 * Chunk #5
		 */
		FT = T - ATIME;
		iretn = 6;
		state = 7;
	    }

	    break;

	case 6:
	    /*
	     * Chunk #6
	     */
	    *XN = XNI + XNDOT * FT + XNDDT * FT * FT * 0.5;
	    XL = XLI + XLDOT * FT + XNDOT * FT * FT * 0.5;
	    TEMP = -*XNODES + THGR + T * THDT;

	    if(ISYNFL == 0)
		*XLL = XL + 2.0 * TEMP;
	    else
		*XLL = XL - *OMGASM + TEMP;

	    done = 1;
	    break;

	case 7:
	    /* DOT TERMS CALCULATED */

	    /*
	     * Chunk #7
	     */
	    if(ISYNFL != 0) {
		XNDOT =
		    DEL1 * sin(XLI - FASX2) +
		    DEL2 * sin(2.0 * (XLI - FASX4)) +
		    DEL3 * sin(3.0 * (XLI - FASX6));

		XNDDT =
		    DEL1 * cos(XLI - FASX2) +
		    2.0 * DEL2 * cos(2.0 * (XLI - FASX4)) +
		    3.0 * DEL3 * cos(3.0 * (XLI - FASX6));
	    } else {
		XOMI = OMEGAQ + s_OMGDT * ATIME;
		X2OMI = XOMI + XOMI;
		X2LI = XLI + XLI;
		XNDOT = D2201 * sin(X2OMI + XLI - G22) +
		    D2211 * sin(XLI - G22) +
		    D3210 * sin(XOMI + XLI - G32) +
		    D3222 * sin(- XOMI + XLI - G32) +
		    D4410 * sin(X2OMI + X2LI - G44) +
		    D4422 * sin(X2LI - G44) +
		    D5220 * sin(XOMI + XLI - G52) +
		    D5232 * sin(- XOMI + XLI - G52) +
		    D5421 * sin(XOMI + X2LI - G54) +
		    D5433 * sin(- XOMI + X2LI - G54);

		XNDDT = D2201 * cos(X2OMI + XLI - G22) +
		    D2211 * cos(XLI - G22) +
		    D3210 * cos(XOMI + XLI - G32) +
		    D3222 * cos(- XOMI + XLI - G32) +
		    D5220 * cos(XOMI + XLI - G52) +
		    D5232 * cos(- XOMI + XLI - G52) +
		    2.*(D4410 * cos(X2OMI + X2LI - G44) +
			D4422 * cos(X2LI - G44) +
			D5421 * cos(XOMI + X2LI - G54) +
			D5433 * cos(- XOMI + X2LI - G54));
	    }

	    XLDOT = XNI + XFACT;
	    XNDDT = XNDDT * XLDOT;

	    state = iretn;
	    break;

	case 8:
	    /*
	     * Chunk #8
	     */

	    /* INTEGRATOR */
	    iretn = 9;
	    state = 7;
	    break;

	case 9:
	    XLI = XLI + XLDOT * DELT + XNDOT * STEP2;
	    XNI = XNI + XNDOT * DELT + XNDDT * STEP2;
	    ATIME = ATIME + DELT;

	    state = iret;
	    break;
	}
    }
}

/* local */

/* C */
/* C     ENTRANCES FOR LUNAR-SOLAR PERIODICS */
/* C */
/* C */
/*       ENTRY DPPER(EM,XINC,OMGASM,XNODES,XLL) */
void
dpper(SatData *sat, double *EM, double *XINC, double *OMGASM,
	   double *XNODES, double *XLL, double T)
{
    double SINIS, COSIS, ZM, ZF, SINZF, F2, F3, SES, SIS, SLS, SEL, SIL, SLL, PGH, PH, SINOK, COSOK, ALFDP, BETDP, DALF, DBET, XLS, DLS;

#if 0
    SINIS = COSIS = ZM = ZF = SINZF = F2 = F3 = SES = SIS = signaling_nan();
    SLS = SEL = SIL = SLL = PGH = signaling_nan();
    PH = SINOK = COSOK = ALFDP = BETDP = DALF = DBET = XLS = signaling_nan();
    DLS = signaling_nan();;
#endif
    SINIS = sin(*XINC);
    COSIS = cos(*XINC);


/*       IF (DABS(SAVTSN-T).LT.(30.D0))    GO TO 210 */
    if(fabs(SAVTSN - T) >= (30.0)) {
	SAVTSN = T;
	ZM = ZMOS + ZNS * T;
/*   205 ZF = ZM + 2.0 * ZES * sin(ZM) */
	ZF = ZM + 2.0 * ZES * sin(ZM);
	SINZF = sin(ZF);
	F2 = .5 * SINZF * SINZF - .25;
	F3 = -.5 * SINZF * cos(ZF);
	SES = SE2 * F2 + SE3 * F3;
	SIS = SI2 * F2 + SI3 * F3;
	SLS = SL2 * F2 + SL3 * F3 + SL4 * SINZF;
	SGHS = SGH2 * F2 + SGH3 * F3 + SGH4 * SINZF;
	SHS = SH2 * F2 + SH3 * F3;
	ZM = ZMOL + ZNL * T;
	ZF = ZM + 2.0 * ZEL * sin(ZM);
	SINZF = sin(ZF);
	F2 = .5 * SINZF * SINZF -.25;
	F3 = -.5 * SINZF * cos(ZF);
	SEL = EE2 * F2 + E3 * F3;
	SIL = XI2 * F2 + XI3 * F3;
	SLL = XL2 * F2 + XL3 * F3 + XL4 * SINZF;
	SGHL = XGH2 * F2 + XGH3 * F3 + XGH4 * SINZF;
	SHL = XH2 * F2 + XH3 * F3;
	PE = SES + SEL;
	PINC = SIS + SIL;
	PL = SLS + SLL;
    }

/*   210 PGH=SGHS+SGHL */
    PGH = SGHS + SGHL;
    PH = SHS + SHL;
    *XINC = *XINC + PINC;
    *EM = *EM + PE;

/*       IF(XQNCL.LT.(.2)) GO TO 220 */
    if(XQNCL >= (.2)) {
/*       GO TO 218 */
/* C */
/* C     APPLY PERIODICS DIRECTLY */
/* C */
/*   218 PH=PH/SINIQ */
	PH = PH / s_SINIQ;
	PGH = PGH - s_COSIQ * PH;
	*OMGASM = *OMGASM + PGH;
	*XNODES = *XNODES + PH;
	*XLL = *XLL + PL;
/*       GO TO 230 */
    } else {
/* C */
/* C     APPLY PERIODICS WITH LYDDANE MODIFICATION */
/* C */
/*   220 SINOK=sin(XNODES) */
	SINOK = sin(*XNODES);
	COSOK = cos(*XNODES);
	ALFDP = SINIS * SINOK;
	BETDP = SINIS * COSOK;
	DALF = PH * COSOK + PINC * COSIS * SINOK;
	DBET = -PH * SINOK + PINC * COSIS * COSOK;
	ALFDP = ALFDP + DALF;
	BETDP = BETDP + DBET;
	XLS = *XLL + *OMGASM + COSIS * *XNODES;
	DLS = PL + PGH - PINC * *XNODES * SINIS;
	XLS = XLS + DLS;
	*XNODES = actan(ALFDP, BETDP);
	*XLL = *XLL + PL;
	*OMGASM = XLS - *XLL - cos(*XINC) * *XNODES;
    }
/*   230 CONTINUE */
/*       RETURN */

}
/*       END */

