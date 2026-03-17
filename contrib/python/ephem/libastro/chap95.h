/* Position of outer planets; straightforward from:
ftp://adc.gsfc.nasa.gov/pub/adc/archives/journal_tables/A+AS/109/181:

J/A+AS/109/181                            Planetary ephemerides (Chapront, 1995)
===============================================================================
Representation of planetary ephemerides by frequency analysis. Application to
the five outer planets.
       CHAPRONT J.
      <Astron. Astrophys. Suppl. Ser. 109, 181 (1995)>
      =1995A&AS..109..181C      (SIMBAD/NED Reference)
===============================================================================

Keywords: ephemerides - planets and satellites: general - methods: numerical

Contents:
  Heliocentric equatorial rectangular coordinates of the five outer planets
  (X, Y and Z). The source is based on DE200 (tables 4 to 7) or a reconstruction
  of DE200 by numerical integration (tables 9 to 13). The reference frame is
  the mean equator and equinox J2000 of DE200.

  The general formulation of the series X is:
  X = SUM[i=1,Records] T**n_i*(CX_i*cos(Nu_k*t)+SX_i*sin(Nu_k*t))
  The formulation is identical for Y and Z.
  T is the time (TDB) in Julian centuries from J2000:
        T = (JulianDate - 2451545.0)/36525
  t is the time (TDB) in Julian years from J2000:
        t = (JulianDate - 2451545.0)/365.25
  Nu is the frequency. Frequencies are identical for all terms of rank k:
                Nu_k = Nu_i when n_i = 0
                For purely secular terms k = 0 and Nu_0 = 0

===============================================================================
(End)                                          Patricia Bauer [CDS] 03-Oct-1994
*/

#define CHAP_SCALE	1e10

/* JDs of validity period */
#define CHAP_BEGIN	(2338032.5 - MJD0)	/* 1689/3/19 */
#define CHAP_END	(2542032.5 - MJD0)	/* 2247/10/1 */

/* coding flags */
/* calculating rates increases time by about 10%
 *
 * On an HP715/75, for pluto the times per step are 0.00049 s and 0.00057 s
 * This method is quite fast.
 */
#define CHAP_GETRATE    1

typedef struct {
	short n;	/* order of time; "-1" marks end of list */
	double amp[6];	/* amplitudes of cosine and sine terms for x,y,z */
			/* in original order [CX,SX,CY,SY,CZ,SZ] */
	double Nu;	/* Frequency Nu_k; given only at n=0 */
} chap95_rec;

extern chap95_rec chap95_jupiter[];
extern chap95_rec chap95_saturn[];
extern chap95_rec chap95_uranus[];
extern chap95_rec chap95_neptune[];
extern chap95_rec chap95_pluto[];

extern int chap95 (double m, int obj, double prec, double *ret);


