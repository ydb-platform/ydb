#include <stdlib.h>
#include <math.h>
#undef SING

#include "sattypes.h"
#include "vector.h"
#include "satspec.h"

/*      SDP4                                               3 NOV 80 */
/*      SUBROUTINE SDP4(IFLAG,TSINCE)
      COMMON/E1/XMO,XNODEO,OMEGAO,EO,XINCL,XNO,XNDT2O,
     1           XNDD6O,BSTAR,X,Y,Z,XDOT,YDOT,ZDOT,EPOCH,DS50
      COMMON/C1/CK2,CK4,E6A,QOMS2T,S,TOTHRD,
     1           XJ3,XKE,XKMPER,XMNPDA,AE
      DOUBLE PRECISION EPOCH, DS50
      */

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

#define CK2	(5.413080e-04)
#define CK4	(6.209887e-07)
#define QOMS2T	(1.880279e-09)
#define S	(1.012229e+00)

#define AE (1.0)
#define DE2RA (.174532925E-1)
#define E6A (1.E-6)
#define PI (3.14159265)
#define PIO2 (1.57079633)
#define QO (120.0)
#define SO (78.0)
#define TOTHRD (.66666667)
#define TWOPI (6.2831853)
#define X3PIO2 (4.71238898)
#define XJ2 (1.082616E-3)
#define XJ3 (-.253881E-5)
#define XJ4 (-1.65597E-6)
#define XKE (.743669161E-1)
#define XKMPER (6378.135)
#define XMNPDA (1440.)

/* int IFLAG; */

#define X	(pos->sl_X)
#define XDOT	(pos->sl_XDOT)
#define Y	(pos->sl_Y)
#define YDOT	(pos->sl_YDOT)
#define Z	(pos->sl_Z)
#define ZDOT	(pos->sl_ZDOT)

/* sat->prop.sdp4-> */
#define AODP	(sat->prop.sdp4->sdp4_AODP)
#define AYCOF	(sat->prop.sdp4->sdp4_AYCOF)
#define BETAO	(sat->prop.sdp4->sdp4_BETAO)
#define BETAO2	(sat->prop.sdp4->sdp4_BETAO2)
#define C1	(sat->prop.sdp4->sdp4_C1)
#define C4	(sat->prop.sdp4->sdp4_C4)
#define COSG	(sat->prop.sdp4->sdp4_COSG)
#define COSIO	(sat->prop.sdp4->sdp4_COSIO)
#define EOSQ	(sat->prop.sdp4->sdp4_EOSQ)
#define OMGDOT	(sat->prop.sdp4->sdp4_OMGDOT)
#define SING	(sat->prop.sdp4->sdp4_SING)
#define SINIO	(sat->prop.sdp4->sdp4_SINIO)
#define T2COF	(sat->prop.sdp4->sdp4_T2COF)
#define THETA2	(sat->prop.sdp4->sdp4_THETA2)
#define X1MTH2	(sat->prop.sdp4->sdp4_X1MTH2)
#define X3THM1	(sat->prop.sdp4->sdp4_X3THM1)
#define X7THM1	(sat->prop.sdp4->sdp4_X7THM1)
#define XLCOF	(sat->prop.sdp4->sdp4_XLCOF)
#define XMDOT	(sat->prop.sdp4->sdp4_XMDOT)
#define XNODCF	(sat->prop.sdp4->sdp4_XNODCF)
#define XNODOT	(sat->prop.sdp4->sdp4_XNODOT)
#define XNODP	(sat->prop.sdp4->sdp4_XNODP)

#define XMDF_seco   (sat->prop.sdp4->sdp4_XMDF_seco)
#define OMGADF_seco (sat->prop.sdp4->sdp4_OMGADF_seco)
#define XNODE_seco  (sat->prop.sdp4->sdp4_XNODE_seco)
#define EM_seco     (sat->prop.sdp4->sdp4_EM_seco)
#define XINC_seco   (sat->prop.sdp4->sdp4_XINC_seco)
#define XN_seco     (sat->prop.sdp4->sdp4_XN_seco)

#define E_pero      (sat->prop.sdp4->sdp4_E_pero)
#define XINC_pero   (sat->prop.sdp4->sdp4_XINC_pero)
#define OMGADF_pero (sat->prop.sdp4->sdp4_OMGADF_pero)
#define XNODE_pero  (sat->prop.sdp4->sdp4_XNODE_pero)
#define XMAM_pero   (sat->prop.sdp4->sdp4_XMAM_pero)

void
sdp4 (SatData *sat, Vec3 *pos, Vec3 *dpos, double TSINCE)
{
    int i;

    /* private temporary variables used only in init section */
    double A1,A3OVK2,AO,C2,COEF,COEF1,DEL1,DELO,EETA,ETA,
	ETASQ,PERIGE,PINVSQ,PSISQ,QOMS24,S4,THETA4,TSI,X1M5TH,XHDOT1;

    /* private temporary variables */
    double A,AXN,AYN,AYNL,BETA,BETAL,CAPU,COS2U,COSEPW=0,
	COSIK,COSNOK,COSU,COSUK,E,ECOSE,ELSQ,EM=0,EPW,ESINE,OMGADF,PL,
	R,RDOT,RDOTK,RFDOT,RFDOTK,RK,SIN2U,SINEPW=0,SINIK,SINNOK,
	SINU,SINUK,TEMP,TEMP1,TEMP2,TEMP3=0,TEMP4=0,TEMP5=0,TEMP6=0,TEMPA,
	TEMPE,TEMPL,TSQ,U,UK,UX,UY,UZ,VX,VY,VZ,XINC=0,XINCK,XL,XLL,XLT,
	XMAM,XMDF,XMX,XMY,XN,XNODDF,XNODE,XNODEK;

#if 0
    A1=A3OVK2=AO=C2=COEF=COEF1=DEL1=DELO=EETA=ETA = signaling_nan();
    ETASQ=PERIGE=PINVSQ=PSISQ=QOMS24=S4=THETA4=TSI=X1M5TH=XHDOT1 = signaling_nan();

     A=AXN=AYN=AYNL=BETA=BETAL=CAPU=COS2U=COSEPW = signaling_nan();
     COSIK=COSNOK=COSU=COSUK=E=ECOSE=ELSQ=EM=EPW=ESINE=OMGADF=PL = signaling_nan();
     R=RDOT=RDOTK=RFDOT=RFDOTK=RK=SIN2U=SINEPW=SINIK=SINNOK = signaling_nan();
     SINU=SINUK=TEMP=TEMP1=TEMP2=TEMP3=TEMP4=TEMP5=TEMP6=TEMPA = signaling_nan();
     TEMPE=TEMPL=TSQ=U=UK=UX=UY=UZ=VX=VY=VZ=XINC=XINCK=XL=XLL=XLT = signaling_nan();
     XMAM=XMDF=XMX=XMY=XN=XNODDF=XNODE=XNODEK = signaling_nan();
#endif

    if(TSINCE != 0.0 && !sat->prop.sdp4) {
	/*
	 * Yes, this is a recursive call.
	 */
	sdp4(sat, pos, dpos, 0.0);
    }

/*      IF  (IFLAG .EQ. 0) GO TO 100 */
/*    if(!IFLAG) */
    if(!sat->prop.sdp4) {
	sat->prop.sdp4 = (struct sdp4_data *) malloc(sizeof(struct sdp4_data));

/*	init_sdp4(sat->prop.sdp4); */

/*      RECOVER ORIGINAL MEAN MOTION (XNODP) AND SEMIMAJOR AXIS (AODP) */
/*      FROM INPUT ELEMENTS */

	A1=pow((XKE/XNO), TOTHRD);
	COSIO=cos(XINCL);
	THETA2=COSIO*COSIO;
	X3THM1=3.0 * THETA2 - 1.0;
	EOSQ = EO * EO;
	BETAO2 = 1.0 - EOSQ;
	BETAO = sqrt(BETAO2);
	DEL1 = 1.5 * CK2 * X3THM1 / (A1 * A1 * BETAO * BETAO2);
	AO = A1 * (1.0 - DEL1 * (0.5 * TOTHRD +
				DEL1 * (1.0 + 134.0 / 81.0 * DEL1)));
	DELO = 1.5 * CK2 * X3THM1 / (AO * AO * BETAO * BETAO2);
	XNODP = XNO / (1.0 + DELO);
	AODP = AO / (1.0 - DELO);

/*      INITIALIZATION */

/*      FOR PERIGEE BELOW 156 KM, THE VALUES OF
*      S AND QOMS2T ARE ALTERED */

	S4 = S;
	QOMS24 = QOMS2T;
	PERIGE = (AODP * (1.0 - EO) - AE) * XKMPER;

/*      IF(PERIGE .GE. 156.) GO TO 10 */

	if(PERIGE < 156.0) {
	    S4 = PERIGE - 78.0;

	    if(PERIGE <= 98.0)  { /* GO TO 9 */
		S4 = 20.0;
	    }

	    QOMS24 = pow((120.0 - S4) * AE / XKMPER, 4.0); /* 9 */
	    S4 = S4 / XKMPER + AE;
	}
	PINVSQ = 1.0 / (AODP * AODP * BETAO2 * BETAO2); /* 10 */
	SING = sin(OMEGAO);
	COSG = cos(OMEGAO);
	TSI = 1.0 / (AODP - S4);
	ETA = AODP * EO * TSI;
	ETASQ = ETA * ETA;
	EETA = EO * ETA;
	PSISQ = fabs(1.0 - ETASQ);
	COEF = QOMS24 * pow(TSI, 4.0);
	COEF1 = COEF / pow(PSISQ, 3.5);
	C2 = COEF1 * XNODP * (AODP * (1.0 + 1.5 * ETASQ +
				      EETA * (4.0 + ETASQ)) +
			      .75 * CK2 * TSI / PSISQ * X3THM1 *
			      (8.0 + 3.0 * ETASQ * (8.0 + ETASQ)));
	C1 = BSTAR * C2;
	SINIO = sin(XINCL);
	A3OVK2 = -XJ3 / CK2 * AE * AE * AE; /* A3OVK2=-XJ3/CK2*AE**3; */
	X1MTH2 = 1.0 - THETA2;
	C4 = 2.0 * XNODP * COEF1 * AODP * BETAO2 *
	    (ETA * (2.0 + .5 * ETASQ) + EO * (.5 + 2.0 * ETASQ) -
	     2.0 * CK2 * TSI / (AODP * PSISQ) *
	     (-3.0 * X3THM1 * (1.0 - 2.0 * EETA + ETASQ *
			       (1.5 - .5 * EETA)) +
	      .75 * X1MTH2 * (2.0 * ETASQ - EETA *
			      (1.0 + ETASQ)) * cos(2.0 * OMEGAO)));
	THETA4 = THETA2 * THETA2;
	TEMP1 = 3.0 * CK2 * PINVSQ * XNODP;
	TEMP2 = TEMP1 * CK2 * PINVSQ;
	TEMP3 = 1.25 * CK4 * PINVSQ * PINVSQ * XNODP;
	XMDOT = XNODP + 0.5 * TEMP1 * BETAO * X3THM1 + .0625 * TEMP2 * BETAO *
	    (13.0 - 78.0 * THETA2 + 137.0 * THETA4);
	X1M5TH=1.0 - 5.0 * THETA2;
	OMGDOT = -.5 * TEMP1 * X1M5TH + .0625 * TEMP2 * 
	    (7.0 - 114.0 * THETA2 + 395.0 * THETA4) +
	    TEMP3 * (3.0 - 36.0 * THETA2 + 49.0 * THETA4);
	XHDOT1 = -TEMP1 * COSIO;
	XNODOT = XHDOT1 + (.5 * TEMP2 * (4.0 - 19.0 * THETA2) +
			   2.0 * TEMP3 * (3.0 - 7.0 * THETA2)) * COSIO;
	XNODCF = 3.5 * BETAO2 * XHDOT1 * C1;
	T2COF = 1.5 * C1;
	XLCOF = .125 * A3OVK2 * SINIO * (3.0 + 5.0 * COSIO) / (1.0 + COSIO);
	AYCOF = .25 * A3OVK2 * SINIO;
	X7THM1 = 7.0 * THETA2 - 1.0;
/*   90 IFLAG=0 */

#ifdef SDP_DEEP_DEBUG
	printf("calling dpinit\n");
	printf("%f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f\n",
	       EOSQ,SINIO,COSIO,BETAO,AODP,THETA2,
	       SING,COSG,BETAO2,XMDOT,OMGDOT,XNODOT,XNODP);
#endif
	dpinit(sat, EOSQ, SINIO, COSIO, BETAO, AODP, THETA2,
	       SING, COSG, BETAO2, XMDOT, OMGDOT, XNODOT, XNODP);

/*      CALL DPINIT(EOSQ,SINIO,COSIO,BETAO,AODP,THETA2,
	1         SING,COSG,BETAO2,XMDOT,OMGDOT,XNODOT,XNODP) */

/*      UPDATE FOR SECULAR GRAVITY AND ATMOSPHERIC DRAG */
    }

    XMDF = XMO + XMDOT * TSINCE; /* 100 */
    OMGADF = OMEGAO + OMGDOT * TSINCE;
    XNODDF = XNODEO + XNODOT * TSINCE;
    TSQ = TSINCE * TSINCE;
    XNODE = XNODDF + XNODCF * TSQ;
    TEMPA = 1.0 - C1 * TSINCE;
    TEMPE = BSTAR * C4 * TSINCE;
    TEMPL = T2COF * TSQ;
    XN = XNODP;

    if(TSINCE == 0.0) {
	XMDF_seco   = XMDF;
	OMGADF_seco = OMGADF;
	XNODE_seco  = XNODE;
	EM_seco     = EM;
	XINC_seco   = XINC;
	XN_seco     = XN;
    }

    dpsec(sat, &XMDF, &OMGADF, &XNODE, &EM, &XINC, &XN, TSINCE);

    if(TSINCE == 0.0) {
	XMDF_seco   = XMDF - XMDF_seco;
	OMGADF_seco = OMGADF - OMGADF_seco;
	XNODE_seco  = XNODE - XNODE_seco;
	EM_seco     = EM - EM_seco;
	XINC_seco   = XINC - XINC_seco;
	XN_seco     = XN - XN_seco;

#if 0
	printf("XMDF_seco   = %e\n", XMDF_seco);
	printf("OMGADF_seco = %e\n", OMGADF_seco);
	printf("XNODE_seco  = %e\n", XNODE_seco);
	printf("EM_seco     = %e\n", EM_seco);
	printf("XINC_seco   = %e\n", XINC_seco);
	printf("XN_seco     = %e\n", XN_seco);
#endif
    }

    /*
    XMDF   -= XMDF_seco;
    OMGADF -= OMGADF_seco;
    XNODE  -= XNODE_seco;
    EM     -= EM_seco;
    XINC   -= XINC_seco;
    XN     -= XN_seco;
    */

    A = pow(XKE/XN, TOTHRD) * TEMPA * TEMPA;
    E = EM - TEMPE;
#ifdef SDP_DEEP_DEBUG
    printf("*** E = %f\n", E);
#endif
    XMAM = XMDF + XNODP * TEMPL;
/*      CALL DPPER(E,XINC,OMGADF,XNODE,XMAM) */

#ifdef SDP_DEEP_DEBUG
    printf("%12s %12s %12s %12s %12s\n",
	   "E", "XINC", "OMGADF", "XNODE", "XMAM");
    printf("%12f %12f %12f %12f %12f\n",
	   E, XINC, OMGADF, XNODE, XMAM);
#endif

    if(TSINCE == 0.0) {
	E_pero      = E;
	XINC_pero   = XINC;
	OMGADF_pero = OMGADF;
	XNODE_pero  = XNODE;
	XMAM_pero   = XMAM;
    }

    dpper(sat, &E, &XINC, &OMGADF, &XNODE, &XMAM, TSINCE);

    if(TSINCE == 0.0) {
	E_pero      = E - E_pero;
	XINC_pero   = XINC - XINC_pero;
	OMGADF_pero = OMGADF - OMGADF_pero;
	XNODE_pero  = XNODE - XNODE_pero;
	XMAM_pero   = XMAM - XMAM_pero;

#if 0
	printf("E_pero      = %e\n", E_pero);
	printf("XINC_pero   = %e\n", XINC_pero);
	printf("OMGADF_pero = %e\n", OMGADF_pero);
	printf("XNODE_pero  = %e\n", XNODE_pero);
	printf("XMAM_pero   = %e\n\n", XMAM_pero);
#endif
    }

    /*
    E      -= E_pero;
    XINC   -= XINC_pero;
    OMGADF -= OMGADF_pero;
    XNODE  -= XNODE_pero;
    XMAM   -= XMAM_pero;
    */
    XL = XMAM + OMGADF + XNODE;
    BETA = sqrt(1.0 - E * E);
    XN = XKE / pow(A, 1.5);

/*      LONG PERIOD PERIODICS */

    AXN = E * cos(OMGADF);
    TEMP=1./(A*BETA*BETA);
    XLL=TEMP*XLCOF*AXN;
    AYNL=TEMP*AYCOF;
    XLT=XL+XLL;
    AYN=E*sin(OMGADF)+AYNL;

/*      SOLVE KEPLERS EQUATION */

    CAPU=fmod(XLT-XNODE, TWOPI);
    TEMP2=CAPU;
/*      DO 130 I=1,10*/
    for(i = 1; i < 10; i++) {
	SINEPW=sin(TEMP2);
      COSEPW=cos(TEMP2);
      TEMP3=AXN*SINEPW;
      TEMP4=AYN*COSEPW;
      TEMP5=AXN*COSEPW;
      TEMP6=AYN*SINEPW;
      EPW=(CAPU-TEMP4+TEMP3-TEMP2)/(1.-TEMP5-TEMP6)+TEMP2;
/*      IF(ABS(EPW-TEMP2) .LE. E6A) GO TO 140 */
      if(fabs(EPW-TEMP2) <= E6A)
	  break;
      TEMP2=EPW; /* 130 */
    }

/*      SHORT PERIOD PRELIMINARY QUANTITIES */

    ECOSE=TEMP5+TEMP6; /* 140  */
    ESINE=TEMP3-TEMP4;
    ELSQ=AXN*AXN+AYN*AYN;
    TEMP=1.-ELSQ;
    PL=A*TEMP;
    R=A*(1.-ECOSE);
    TEMP1=1./R;
    RDOT=XKE*sqrt(A)*ESINE*TEMP1;
    RFDOT=XKE*sqrt(PL)*TEMP1;
    TEMP2=A*TEMP1;
    BETAL=sqrt(TEMP);
    TEMP3=1./(1.+BETAL);
    COSU=TEMP2*(COSEPW-AXN+AYN*ESINE*TEMP3);
    SINU=TEMP2*(SINEPW-AYN-AXN*ESINE*TEMP3);
    U=actan(SINU,COSU);
    SIN2U=2.*SINU*COSU;
    COS2U=2.*COSU*COSU-1.0;
    TEMP=1./PL;
    TEMP1=CK2*TEMP;
    TEMP2=TEMP1*TEMP;

/*      UPDATE FOR SHORT PERIODICS */

    RK=R*(1.-1.5*TEMP2*BETAL*X3THM1)+.5*TEMP1*X1MTH2*COS2U;
    UK=U - .25 * TEMP2 * X7THM1 * SIN2U;
    XNODEK=XNODE+1.5*TEMP2*COSIO*SIN2U;
    XINCK=XINC+1.5*TEMP2*COSIO*SINIO*COS2U;
    RDOTK=RDOT-XN*TEMP1*X1MTH2*SIN2U;
    RFDOTK=RFDOT+XN*TEMP1*(X1MTH2*COS2U+1.5*X3THM1);

/*      ORIENTATION VECTORS */
    SINUK=sin(UK);
    COSUK=cos(UK);
    SINIK=sin(XINCK);
    COSIK=cos(XINCK);
    SINNOK=sin(XNODEK);
    COSNOK=cos(XNODEK);
    XMX=-SINNOK*COSIK;
    XMY=COSNOK*COSIK;
    UX=XMX*SINUK+COSNOK*COSUK;
    UY=XMY*SINUK+SINNOK*COSUK;
    UZ=SINIK*SINUK;
    VX=XMX*COSUK-COSNOK*SINUK;
    VY=XMY*COSUK-SINNOK*SINUK;
    VZ=SINIK*COSUK;
#if 0
    printf("UX = %f VX = %f RK = %f RDOTK = %f RFDOTK = %f\n",
	   UX, VX, RK, RDOTK, RFDOTK);
#endif
/*      POSITION AND VELOCITY */

    pos->x = RK*UX;
    pos->y = RK*UY;
    pos->z = RK*UZ;
    dpos->x = RDOTK*UX+RFDOTK*VX;
    dpos->y = RDOTK*UY+RFDOTK*VY;
    dpos->z = RDOTK*UZ+RFDOTK*VZ;
/*      RETURN
      END */
}

