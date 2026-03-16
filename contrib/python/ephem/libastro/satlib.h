#ifndef __SATLIB_H
#define __SATLIB_H

/* $Id: satlib.h,v 1.1 2000/09/25 17:21:25 ecdowney Exp $ */

typedef struct _SatElem {
    float  se_XMO;
    float  se_XNODEO;
    float  se_OMEGAO;
    float  se_EO;
    float  se_XINCL;
    float  se_XNDD60;
    float  se_BSTAR;
    float  pad1;
    double se_XNO;
    double se_XNDT20;
    double se_EPOCH;
    struct {
	unsigned int catno	: 21;
	unsigned int classif	: 5;
	unsigned int elnum	: 10;
	unsigned int year	: 14;
	unsigned int launch	: 10;
	unsigned int piece	: 15;
	unsigned int ephtype	: 4;
	unsigned int orbit	: 17;
    } se_id;
} SatElem;

#if 0
struct sat_loc {
    double sl_X;
    double sl_XDOT;
    double sl_Y;
    double sl_YDOT;
    double sl_Z;
    double sl_ZDOT;
};
#endif

struct sgp4_data {
    unsigned int sgp4_flags;
    unsigned int pad;
    double sgp4_AODP;
    double sgp4_AYCOF;
    double sgp4_C1;
    double sgp4_C4;
    double sgp4_C5;
    double sgp4_COSIO;
    double sgp4_D2;
    double sgp4_D3;
    double sgp4_D4;
    double sgp4_DELMO;
    double sgp4_ETA;
    double sgp4_OMGCOF;
    double sgp4_OMGDOT;
    double sgp4_SINIO;
    double sgp4_SINMO;
    double sgp4_T2COF;
    double sgp4_T3COF;
    double sgp4_T4COF;
    double sgp4_T5COF;
    double sgp4_X1MTH2;
    double sgp4_X3THM1;
    double sgp4_X7THM1;
    double sgp4_XLCOF;
    double sgp4_XMCOF;
    double sgp4_XMDOT;
    double sgp4_XNODCF;
    double sgp4_XNODOT;
    double sgp4_XNODP;
};

struct deep_data {
    struct {
	unsigned int IRESFL : 1;
	unsigned int ISYNFL : 1;
    } deep_flags;
    double deep_s_SINIQ;
    double deep_s_COSIQ;
    double deep_s_OMGDT;
    double deep_ATIME;
    double deep_D2201;
    double deep_D2211;
    double deep_D3210;
    double deep_D3222;
    double deep_D4410;
    double deep_D4422;
    double deep_D5220;
    double deep_D5232;
    double deep_D5421;
    double deep_D5433;
    double deep_DEL1;
    double deep_DEL2;
    double deep_DEL3;
    double deep_E3;
    double deep_EE2;
    double deep_FASX2;
    double deep_FASX4;
    double deep_FASX6;
    double deep_OMEGAQ;
    double deep_PE;
    double deep_PINC;
    double deep_PL;
    double deep_SAVTSN;
    double deep_SE2;
    double deep_SE3;
    double deep_SGH2;
    double deep_SGH3;
    double deep_SGH4;
    double deep_SGHL;
    double deep_SGHS;
    double deep_SH2;
    double deep_SH3;
    double deep_SHS;
    double deep_SHL;
    double deep_SI2;
    double deep_SI3;
    double deep_SL2;
    double deep_SL3;
    double deep_SL4;
    double deep_SSE;
    double deep_SSG;
    double deep_SSH;
    double deep_SSI;
    double deep_SSL;
    double deep_STEP2;
    double deep_STEPN;
    double deep_STEPP;
    double deep_THGR;
    double deep_XFACT;
    double deep_XGH2;
    double deep_XGH3;
    double deep_XGH4;
    double deep_XH2;
    double deep_XH3;
    double deep_XI2;
    double deep_XI3;
    double deep_XL2;
    double deep_XL3;
    double deep_XL4;
    double deep_XLAMO;
    double deep_XLI;
    double deep_XNI;
    double deep_XNQ;
    double deep_XQNCL;
    double deep_ZMOL;
    double deep_ZMOS;
};

struct sdp4_data {
    double sdp4_AODP;	/* dpa */
    double sdp4_AYCOF;
    double sdp4_BETAO;	/* dpa */
    double sdp4_BETAO2;	/* dpa */
    double sdp4_C1;
    double sdp4_C4;
    double sdp4_COSG;	/* dpa */
    double sdp4_COSIO;	/* dpa */
    double sdp4_EOSQ;	/* dpa */
    double sdp4_OMGDOT;	/* dpa */
    double sdp4_SING;	/* dpa */
    double sdp4_SINIO;	/* dpa */
    double sdp4_T2COF;
    double sdp4_THETA2;	/* dpa */
    double sdp4_X1MTH2;
    double sdp4_X3THM1;
    double sdp4_X7THM1;
    double sdp4_XLCOF;
    double sdp4_XMDOT;	/* dpa */
    double sdp4_XNODCF;
    double sdp4_XNODOT;	/* dpa */
    double sdp4_XNODP;	/* dpa */

    double sdp4_XMDF_seco;
    double sdp4_OMGADF_seco;
    double sdp4_XNODE_seco;
    double sdp4_EM_seco;
    double sdp4_XINC_seco;
    double sdp4_XN_seco;

    double sdp4_E_pero;
    double sdp4_XINC_pero;
    double sdp4_OMGADF_pero;
    double sdp4_XNODE_pero;
    double sdp4_XMAM_pero;
};

typedef struct _SatData {
    struct _SatElem *elem;
    union {
	struct sgp4_data *sgp4;
	struct sdp4_data *sdp4;
    } prop;
    struct deep_data *deep;
} SatData;

void sgp4(SatData *sat, Vec3 *pos, Vec3 *dpos, double t);

void sdp4(SatData *sat, Vec3 *pos, Vec3 *dpos, double TSINCE);

#endif /* __SATLIB_H */

