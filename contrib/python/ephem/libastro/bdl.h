#include <stdio.h>

extern int read_bdl (FILE *fp, double jd, double *xp, double *yp, double *zp,
    char ynot[]);

typedef struct {
     double t0; /* start time of this set of coefficients */
     double cmx[6], cfx[4], cmy[6], cfy[4], cmz[6], cfz[4]; /* coefficients */
} BDL_Record;

typedef struct {
     unsigned nsat; /* number of satellites described in file */
     double djj; /* beginning Julian date of dataset*/
     unsigned *idn; /* moonrecord index at which this moon's data starts */
     double *freq; /* frequency of moon records? */
     double *delt; /* time delta between successive moon records */
     BDL_Record *moonrecords;
} BDL_Dataset;

extern void do_bdl (BDL_Dataset *dataset, double jd,
                    double *xp, double *yp, double *zp);

/* Data sets */

extern BDL_Dataset mars_9910, mars_1020, mars_2040;
extern BDL_Dataset jupiter_9910, jupiter_1020, jupiter_2040;
extern BDL_Dataset saturne_9910, saturne_1020, saturne_2040;
extern BDL_Dataset uranus_9910, uranus_1020, uranus_2040;
