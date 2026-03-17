/* Position of planets mercury to neptune; from:
ftp://ftp.bdl.fr/pub/ephem/planets/vsop87/
from README:

==========================                         ===========================
                              BUREAU DES LONGITUDES
                            PLANETARY SOLUTION VSOP87
                                  1996, January
==========================                         ===========================

These files and programs are associated to :

Planetary Theories in rectangular and spherical variables: VSOP87 solution.
    Bretagnon P., Francou G.
    Astron. Astrophys. 202, 309 (1988).

Theorie du mouvement de l'ensemble des planetes (VSOP82).
    Bretagnon P.
    Astron. Astrophys. 114, 278 (1982).

==============================================================================

Description:
    The Planetary solutions VSOP87 (Variations Seculaires des Orbites
    Planetaires) are analytical solutions of the motion of the planets in
    different versions. The main version VSOP87 consists of the series in
    elliptic elements as in the case of VSOP82 solution and the other
    versions VSOP87 (A-B-C-D-E) are built in rectangular and spherical
    variables.

Authors' Address:
    P. Bretagnon, G. Francou
    Bureau des Longitudes, CNRS URA 707
    77, Avenue Denfert-Rochereau
    75014, Paris, France
    Tel    : (33) 1 40 51 22 69  (33) 1 40 51 22 60
    Fax    : (33) 1 46 33 28 34
    E-mail : pierre@bdl.fr  francou@bdl.fr

Contents:
    The main version of VSOP87 is similar to the previous theory VSOP82.
    In the both cases the constants of integration have been determined by
    fitting to the numerical integration DE200 of the Jet Propulsion
    Laboratory. The various versions of VSOP87 are different from one to
    another in the type of coordinates and the reference frame.
    VSOP87  : heliocentric elliptic    variables; equinox and ecliptic J2000.
    VSOP87A : heliocentric rectangular variables; equinox and ecliptic J2000.
    VSOP87B : heliocentric spherical   variables; equinox and ecliptic J2000.
    VSOP87C : heliocentric rectangular variables; equinox and ecliptic of date.
    VSOP87D : heliocentric spherical   variables; equinox and ecliptic of date.
    VSOP87E : barycentric  rectangular variables; equinox and ecliptic J2000.
...
==============================================================================
User feed-back is encouraged. Unless otherwise specified, send comments and bug
reports to:                    E-mail     : comments@bdl.fr
                               Fax        : (33) 1 46 33 28 34
                               Postal mail: Bureau des longitudes
                                            77 avenue Denfert Rochereau
                                            F-75014 PARIS
==============================================================================
  implemented for C: stern
*/

#define VSOP_ASCALE	1e8	/* amplitude factor as stored */

/* coding flags */
#define VSOP_SPHERICAL	1	/* version in data.c uses spherical coords */
#define VSOP_GETRATE	0	/* calculate time derivatives of coordinates */

/* data tables */
extern double vx_mercury[][3];
extern int vn_mercury[][3];
extern double vx_venus[][3];
extern int vn_venus[][3];
extern double vx_earth[][3];
extern int vn_earth[][3];
extern double vx_mars[][3];
extern int vn_mars[][3];
extern double vx_jupiter[][3];
extern int vn_jupiter[][3];
extern double vx_saturn[][3];
extern int vn_saturn[][3];
extern double vx_uranus[][3];
extern int vn_uranus[][3];
extern double vx_neptune[][3];
extern int vn_neptune[][3];

extern int vsop87 (double mj, int obj, double prec, double *ret);


