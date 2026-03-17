#ifndef __SATSPEC_H
#define __SATSPEC_H

/* $Id: satspec.h,v 1.1 2000/09/25 17:21:25 ecdowney Exp $ */

#include "sattypes.h"
#include "satlib.h"

#define SGP4_SIMPLE	0x00000001

extern void init_deep(struct deep_data *deep);
void init_sdp4(struct sdp4_data *sdp);
char *tleerr(int);
int readtle(char *, char *, SatElem *);

double current_jd();

double ut1_to_gha(double);

void smallsleep(double t);

double epoch_jd(double);

double actan(double sinx, double cosx);

double thetag(double EP, double *DS50);

void dpinit(SatData *sat, double EQSQ, double SINIQ, double COSIQ,
	    double RTEQSQ, double AO, double COSQ2, double SINOMO,
	    double COSOMO, double BSQ, double XLLDOT, double OMGDT,
	    double XNODOT, double XNODP);

void dpsec(SatData *sat, double *XLL, double *OMGASM, double *XNODES,
	   double *EM, double *XINC, double *XN, double T);

void dpper(SatData *sat, double *EM, double *XINC, double *OMGASM,
	   double *XNODES, double *XLL, double T);

#endif /* __SATSPEC_H */

