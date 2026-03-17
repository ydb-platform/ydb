/* crack natural satellite files from BDL */

#include <math.h>

#include "astro.h"
#include "bdl.h"

int read_bdl (FILE *fp, double jd, double *xp, double *yp, double *zp,
              char ynot[]) { return 0;}

/* using a BDL planetary moon dataset defined in a struct in RAM and a
 * JD, find the x/y/z positions of each satellite. store in the given arrays,
 * assumed to have one entry per moon. values are planetocentric, +x east, +y
 * north, +z away from earth, all in au. corrected for light time.
 * files obtained from ftp://ftp.bdl.fr/pub/misc/satxyz.
 */
void
do_bdl (BDL_Dataset *dataset, double jd, double *xp, double *yp, double *zp)
{
        int nsat = dataset->nsat;
        double djj = dataset->djj;
        unsigned *idn = dataset->idn;
        double *freq = dataset->freq;
        double *delt = dataset->delt;
	double t0;
	int i;

	/* compute location of each satellite */
	for (i = 0; i < nsat; i++) {
	    int id = (int)floor((jd-djj)/delt[i]) + idn[i] - 2;
	    double t1, anu, tau, tau2, at;
	    double tbx, tby, tbz;
            double *cmx, *cfx, *cmy, *cfy, *cmz, *cfz;
            BDL_Record *r = & dataset->moonrecords[id];

            t0 = r->t0;
	    t1 = floor(t0) + 0.5;
	    anu = freq[i];
	    tau = jd - t1;
	    tau2 = tau * tau;
	    at = tau*anu;

            cmx = & (r->cmx[0]); /* point at data in appropriate record */
            cfx = & (r->cfx[0]);
            cmy = & (r->cmy[0]);
            cfy = & (r->cfy[0]);
            cmz = & (r->cmz[0]);
            cfz = & (r->cfz[0]);

	    tbx = cmx[0]+cmx[1]*tau+cmx[2]*sin(at+cfx[0])
			    +cmx[3]*tau*sin(at+cfx[1])
			    +cmx[4]*tau2*sin(at+cfx[2])
			    +cmx[5]*sin(2*at+cfx[3]);
	    tby = cmy[0]+cmy[1]*tau+cmy[2]*sin(at+cfy[0])
			    +cmy[3]*tau*sin(at+cfy[1])
			    +cmy[4]*tau2*sin(at+cfy[2])
			    +cmy[5]*sin(2*at+cfy[3]);
	    tbz = cmz[0]+cmz[1]*tau+cmz[2]*sin(at+cfz[0])
			    +cmz[3]*tau*sin(at+cfz[1])
			    +cmz[4]*tau2*sin(at+cfz[2])
			    +cmz[5]*sin(2*at+cfz[3]);

	    xp[i] = tbx*1000./149597870.;
	    yp[i] = tby*1000./149597870.;
	    zp[i] = tbz*1000./149597870.;
	}
}

