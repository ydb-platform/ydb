#ifndef _ASTRO_H
#define _ASTRO_H

/* for PyEphem: silence Windows complaints about sprintf() */
#ifdef _MSC_VER
#define _CRT_SECURE_NO_WARNINGS
#endif

#include "astro_export.h"
#include <stdio.h>

#ifndef PI
#define	PI		3.141592653589793
#endif

/* conversions among hours (of ra), degrees and radians. */
#define	degrad(x)	((x)*PI/180.)
#define	raddeg(x)	((x)*180./PI)
#define	hrdeg(x)	((x)*15.)
#define	deghr(x)	((x)/15.)
#define	hrrad(x)	degrad(hrdeg(x))
#define	radhr(x)	deghr(raddeg(x))

/* ratio of from synodic (solar) to sidereal (stellar) rate */
#define	SIDRATE		.9972695677

/* manifest names for planets.
 * N.B. must coincide with usage in pelement.c and plans.c.
 * N.B. only the first 8 are valid for use with plans().
 */
typedef enum {
    MERCURY,
    VENUS,
    MARS,
    JUPITER,
    SATURN,
    URANUS,
    NEPTUNE,
    PLUTO,
    SUN,
    MOON,
    NOBJ	/* total number of basic objects */
} PLCode;

/* moon constants for pl_moon */
typedef enum {
    X_PLANET = 0,			/* use to mean planet itself */
    PHOBOS = NOBJ, DEIMOS,
    IO, EUROPA, GANYMEDE, CALLISTO,
    MIMAS, ENCELADUS, TETHYS, DIONE, RHEA, TITAN, HYPERION, IAPETUS,
    ARIEL, UMBRIEL, TITANIA, OBERON, MIRANDA,
    NBUILTIN
} MCode;

/* starting point for MJD calculations
 */
#define MJD0  2415020.0
#define J2000 (2451545.0 - MJD0)      /* yes, 2000 January 1 at 12h */

/* the Now and Obj typedefs.
 * also, a few miscellaneous constants and declarations.
 */

#define	SPD	(24.0*3600.0)	/* seconds per day */
#define	MAU	(1.4959787e11)	/* m / au */
#define	LTAU	499.005		/* seconds light takes to travel 1 AU */
#define	ERAD	(6.37816e6)	/* earth equitorial radius, m */
#define	MRAD	(1.740e6)	/* moon equitorial radius, m */
#define	SRAD	(6.95e8)	/* sun equitorial radius, m */
#define	FTPM	3.28084		/* ft per m */
#define	ESAT_MAG	2	/* default satellite magnitude */
#define	FAST_SAT_RPD	0.25	/* max earth sat rev/day considered "fast" */

#define	EOD	(-9786)		/* special epoch flag: use epoch of date */

/* info about the local observing circumstances and misc preferences */
typedef struct {
	double n_mjd;	/* modified Julian date, ie, days since
			 * Jan 0.5 1900 (== 12 noon, Dec 30, 1899), utc.
			 * enough precision to get well better than 1 second.
			 * N.B. if not first member, must move NOMJD inits.
			 */
	double n_lat;	/* geographic (surface-normal) lt, >0 north, rads */
	double n_lng;	/* longitude, >0 east, rads */
	double n_tz;	/* time zone, hrs behind UTC */
	double n_temp;	/* atmospheric temp, degrees C */
	double n_pressure; /* atmospheric pressure, mBar */
	double n_elev;	/* elevation above sea level, earth radii */
	double n_dip;	/* dip of sun below hzn at twilight, >0 below, rads */
	double n_epoch;	/* desired precession display ep as an mjd, or EOD */
	char n_tznm[8];	/* time zone name; 7 chars or less, always 0 at end */
} Now;

/* handy shorthands for fields in a Now pointer, np */
#define mjd	np->n_mjd
#define lat	np->n_lat
#define lng	np->n_lng
#define tz	np->n_tz
#define temp	np->n_temp
#define pressure np->n_pressure
#define elev	np->n_elev
#define	dip	np->n_dip
#define epoch	np->n_epoch
#define tznm	np->n_tznm
#define mjed	mm_mjed(np)

/* structures to describe objects of various types.
 */

/* magnitude values in two different systems */
typedef struct {
    float m1, m2;	/* either g/k or H/G, depending on... */
    int whichm;		/* one of MAG_gk or MAG_HG */
} Mag;

/* whichm */
#define MAG_HG          0       /* using 0 makes HG the initial default */
#define MAG_gk          1

/* we actually store magnitudes times this scale factor in a short int */
#define	MAGSCALE	100.0
#define	set_smag(op,m)	((op)->s_mag = (short)floor((m)*MAGSCALE + 0.5))
#define	set_fmag(op,m)	((op)->f_mag = (short)floor((m)*MAGSCALE + 0.5))
#define	get_mag(op)	((op)->s_mag / MAGSCALE)
#define	get_fmag(op)	((op)->f_mag / MAGSCALE)

/* longest object name, including trailing '\0' */
#define	MAXNM	21

typedef unsigned char ObjType_t;
typedef unsigned char ObjAge_t;
typedef unsigned char byte;

/* Obj is a massive union.
 * many fields are in common so we use macros to make things a little easier.
 */

/* fields common to *all* structs in the Obj union */
#define	OBJ_COMMON_FLDS							\
    ObjType_t co_type;	/* current object type; see flags, below */	\
    byte co_flags;	/* FUSER*... used by others */			\
    ObjAge_t co_age;	/* update aging code; see db.c */		\
    char co_name[MAXNM];/* name, including \0 */			\
    double co_ha;	/* geo/topo app/mean ha, rads */		\
    double co_ra;	/* geo/topo app/mean ra, rads */		\
    double co_dec;	/* geo/topo app/mean dec, rads */		\
    double co_gaera;	/* geo apparent ra, rads */			\
    double co_gaedec;	/* geo apparent dec, rads */			\
    double co_astrora;	/* geo astrometric ra, rads */			\
    double co_astrodec;	/* geo astrometric dec, rads */			\
    float co_az;	/* azimuth, >0 e of n, rads */			\
    float co_alt;	/* altitude above topocentric horizon, rads */	\
    float co_elong;	/* angular sep btwen obj and sun, >0 E, degs */	\
    float co_size;	/* angular size, arc secs */			\
    short co_mag	/* visual magnitude * MAGSCALE */

/* fields common to all solar system objects in the Obj union */
#define	OBJ_SOLSYS_FLDS							\
    OBJ_COMMON_FLDS;	/* all the fixed ones plus ... */		\
    float so_sdist;	/* dist from object to sun, au */		\
    float so_edist;	/* dist from object to earth, au */		\
    float so_hlong;	/* heliocentric longitude, rads */		\
    float so_hlat;	/* heliocentric latitude, rads */		\
    float so_phase	/* phase, % */

/* fields common to all fixed objects in the Obj union */
#define	OBJ_FIXED_FLDS 							\
    char  fo_spect[2];	/* spectral codes, if appropriate */		\
    double fo_epoch;	/* eq of ra/dec and time when pm=0; mjd */ 	\
    double fo_ra;	/* ra, rads, in epoch frame */ 			\
    double fo_dec;	/* dec, rads, in epoch frame */ 		\
    float fo_pmra;	/* ra proper motion, rads/day/cos(dec) */ 	\
    float fo_pmdec;	/* dec proper motion, rads/day */ 		\
    char  fo_class	/* object class */

/* a generic object */
typedef struct {
    OBJ_COMMON_FLDS;
} ObjAny;

/* a generic sol system object */
typedef struct {
    OBJ_SOLSYS_FLDS;
} ObjSS;

/* basic Fixed object info.
 */
typedef struct {
    OBJ_COMMON_FLDS;
    OBJ_FIXED_FLDS;

    /* following are for galaxies */
    byte  fo_ratio;	/* minor/major diameter ratio. use s/get_ratio() */
    byte  fo_pa;	/* position angle, E of N, rads. use s/get_pa() */
} ObjF;

/* true-orbit parameters of binary-star object type */
typedef struct {
    float bo_T;		/* epoch of periastron, years */
    float bo_e;		/* eccentricity */
    float bo_o;		/* argument of periastron, degress */
    float bo_O;		/* longitude of node, degrees */
    float bo_i;		/* inclination to plane of sky, degrees */
    float bo_a;		/* semi major axis, arc secs */
    float bo_P;		/* period, years */

    /* companion position, computed by obj_cir() iff b_2compute */
    float bo_pa;	/* position angle @ ep, rads E of N */
    float bo_sep;	/* separation @ ep, arc secs */
    float bo_ra;	/* geo/topo app/mean ra, rads */
    float bo_dec;	/* geo/topo app/mean dec, rads */
} BinOrbit;
typedef struct {
    float bp_ep;	/* epoch of pa/sep, year */
    float bp_pa;	/* position angle @ ep, rads E of N */
    float bp_sep;	/* separation @ ep, arc secs */

    /* companion position, computed by obj_cir() iff b_2compute */
    float bp_ra;	/* geo/topo app/mean ra, rads */
    float bp_dec;	/* geo/topo app/mean dec, rads */
} BinPos;
#define MAXBINPOS 2	/* max discrete epochs to store when no elements */
typedef struct {
    OBJ_COMMON_FLDS;
    OBJ_FIXED_FLDS;

    byte b_2compute;	/* whether to compute secondary positions */
    byte b_nbp;		/* number of b_bp[] or 0 to use b_bo */
    short b_2mag;	/* secondary's magnitude * MAGSCALE */
    char b_2spect[2];	/* secondary's spectrum */

    /* either a real orbit or a set of discrete pa/sep */
    union {
	BinOrbit b_bo;			/* orbital elements */
	BinPos b_bp[MAXBINPOS];		/* table of discrete positions */
    } u;
} ObjB;

#define	fo_mag	co_mag	/* pseudonym for so_mag since it is not computed */
#define	fo_size	co_size	/* pseudonym for so_size since it is not computed */

/* macros to pack/unpack some fields */
#define	SRSCALE		255.0		/* galaxy size ratio scale */
#define	PASCALE		(255.0/(2*PI))	/* pos angle scale factor */
#define	get_ratio(op)	(((int)(op)->f_ratio)/SRSCALE)
#define	set_ratio(op,maj,min) ((op)->f_ratio = (byte)(((maj) > 0)	    \
					? ((min)*SRSCALE/(double)(maj)+0.5) \
					: 0))
#define	get_pa(op)	((double)(op)->f_pa/PASCALE)
#define	set_pa(op,s)	((op)->f_pa = (byte)((s)*PASCALE + 0.5))

#define	NCLASSES	128 /* n potential fo_classes -- allow for all ASCII */

/* basic planet object info */
typedef struct {
    OBJ_SOLSYS_FLDS;
    PLCode plo_code;		/* which planet */
    MCode plo_moon;		/* which moon, or X_PLANET if planet */
    char plo_evis, plo_svis;	/* if moon: whether visible from earth, sun */
    double plo_x, plo_y, plo_z;	/* if moon: eq dist from center, planet radii */
    double plo_aux1, plo_aux2;	/* various values, depending on type */
} ObjPl;

/* basic info about an object in elliptical heliocentric orbit */
typedef struct {
    OBJ_SOLSYS_FLDS;
    float  eo_inc;	/* inclination, degrees */
    float  eo_Om;	/* longitude of ascending node, degrees */
    float  eo_om;	/* argument of perihelion, degress */
    float  eo_a;	/* mean distance, aka,semi-maj axis,AU */
    float  eo_M;	/* mean anomaly, ie, degrees from perihelion at cepoch*/
    float  eo_size;	/* angular size, in arc seconds at 1 AU */
    float  eo_startok;	/* nominal first mjd this set is ok, else 0 */
    float  eo_endok;	/* nominal last mjd this set is ok, else 0 */
    double eo_e;	/* eccentricity (double for when near 1 computing q) */
    double eo_cepoch;	/* epoch date (M reference), as an mjd */
    double eo_epoch;	/* equinox year (inc/Om/om reference), as an mjd. */
    Mag    eo_mag;	/* magnitude */
} ObjE;

/* basic info about an object in hyperbolic heliocentric orbit */
typedef struct {
    OBJ_SOLSYS_FLDS;
    double ho_epoch;	/* equinox year (inc/Om/om reference), as an mjd */
    double ho_ep;	/* epoch of perihelion, as an mjd */
    float  ho_startok;	/* nominal first mjd this set is ok, else 0 */
    float  ho_endok;	/* nominal last mjd this set is ok, else 0 */
    float  ho_inc;	/* inclination, degs */
    float  ho_Om;	/* longitude of ascending node, degs */
    float  ho_om;	/* argument of perihelion, degs. */
    float  ho_e;	/* eccentricity */
    float  ho_qp;	/* perihelion distance, AU */
    float  ho_g, ho_k;	/* magnitude model coefficients */
    float  ho_size;	/* angular size, in arc seconds at 1 AU */
} ObjH;

/* basic info about an object in parabolic heliocentric orbit */
typedef struct {
    OBJ_SOLSYS_FLDS;
    double po_epoch;	/* reference epoch, as an mjd */
    double po_ep;	/* epoch of perihelion, as an mjd */
    float  po_startok;	/* nominal first mjd this set is ok, else 0 */
    float  po_endok;	/* nominal last mjd this set is ok, else 0 */
    float  po_inc;	/* inclination, degs */
    float  po_qp;	/* perihelion distance, AU */
    float  po_om;	/* argument of perihelion, degs. */
    float  po_Om;	/* longitude of ascending node, degs */
    float  po_g, po_k;	/* magnitude model coefficients */
    float  po_size;	/* angular size, in arc seconds at 1 AU */
} ObjP;

/* basic earth satellite object info */
typedef struct {
    OBJ_COMMON_FLDS;
    double eso_epoch;	/* reference epoch, as an mjd */
    double eso_n;	/* mean motion, rev/day
			 * N.B. we need double due to a sensitive differencing
			 * operation used to compute MeanAnomaly in
			 * esat_main()/satellite.c.
			 */
    float  eso_startok;	/* nominal first mjd this set is ok, else 0 */
    float  eso_endok;	/* nominal last mjd this set is ok, else 0 */
    float  eso_inc;	/* inclination, degs */
    float  eso_raan;	/* RA of ascending node, degs */
    float  eso_e;	/* eccentricity */
    float  eso_ap;	/* argument of perigee at epoch, degs */
    float  eso_M;	/* mean anomaly, ie, degrees from perigee at epoch */
    float  eso_decay;	/* orbit decay rate, rev/day^2 */
    float  eso_drag;	/* object drag coefficient, (earth radii)^-1 */
    int    eso_orbit;	/* integer orbit number of epoch */

    /* computed "sky" results unique to earth satellites */
    float  ess_elev;	/* height of satellite above sea level, m */
    float  ess_range;	/* line-of-site distance from observer to satellite, m*/
    float  ess_rangev;	/* rate-of-change of range, m/s */
    float  ess_sublat;	/* latitude below satellite, >0 north, rads */
    float  ess_sublng;	/* longitude below satellite, >0 east, rads */
    int    ess_eclipsed;/* 1 if satellite is in earth's shadow, else 0 */
} ObjES;

typedef union {
    ObjAny  any;	/* these fields valid for all types */
    ObjSS   anyss;	/* these fields valid for all solar system types */
    ObjPl   pl;		/* planet */
    ObjF    f;		/* fixed object, plus proper motion */
    ObjB    b;		/* bona fide binary stars (doubles are stored in f) */
    ObjE    e;		/* object in heliocentric elliptical orbit */
    ObjH    h;		/* object in heliocentric hyperbolic trajectory */
    ObjP    p;		/* object in heliocentric parabolic trajectory */
    ObjES   es;		/* earth satellite */
} Obj;


/* for o_flags -- everybody must agree */
#define	FUSER0		0x01
#define	FUSER1		0x02
#define	FUSER2		0x04
#define	FUSER3		0x08
#define	FUSER4		0x10
#define	FUSER5		0x20
#define	FUSER6		0x40
#define	FUSER7		0x80

/* mark an object as being a "field star" */
#define	FLDSTAR		FUSER3
/* mark an object as circum calculation failed */
#define NOCIRCUM	FUSER7

/* Obj shorthands: */
#define	o_type		any.co_type
#define	o_name		any.co_name
#define	o_flags		any.co_flags
#define	o_age		any.co_age
#define	s_ha		any.co_ha
#define	s_ra		any.co_ra
#define	s_dec		any.co_dec
#define	s_gaera		any.co_gaera
#define	s_gaedec 	any.co_gaedec
#define	s_astrora	any.co_astrora
#define	s_astrodec	any.co_astrodec
#define	s_az		any.co_az
#define	s_alt		any.co_alt
#define	s_elong		any.co_elong
#define	s_size		any.co_size
#define	s_mag		any.co_mag

#define	s_sdist		anyss.so_sdist
#define	s_edist		anyss.so_edist
#define	s_hlong		anyss.so_hlong
#define	s_hlat		anyss.so_hlat
#define	s_phase 	anyss.so_phase

#define	s_elev		es.ess_elev
#define	s_range		es.ess_range
#define	s_rangev	es.ess_rangev
#define	s_sublat	es.ess_sublat
#define	s_sublng	es.ess_sublng
#define	s_eclipsed	es.ess_eclipsed

#define	f_class		f.fo_class
#define	f_spect		f.fo_spect
#define	f_ratio		f.fo_ratio
#define	f_pa		f.fo_pa
#define	f_epoch		f.fo_epoch
#define	f_RA		f.fo_ra
#define	f_pmRA		f.fo_pmra
#define	f_dec		f.fo_dec
#define	f_pmdec		f.fo_pmdec
#define	f_mag		f.fo_mag
#define	f_size		f.fo_size

#define	e_cepoch 	e.eo_cepoch
#define	e_epoch		e.eo_epoch
#define	e_startok	e.eo_startok
#define	e_endok		e.eo_endok
#define	e_inc		e.eo_inc
#define	e_Om		e.eo_Om
#define	e_om		e.eo_om
#define	e_a		e.eo_a
#define	e_e		e.eo_e
#define	e_M		e.eo_M
#define	e_size		e.eo_size
#define	e_mag		e.eo_mag

#define	h_epoch		h.ho_epoch
#define	h_startok	h.ho_startok
#define	h_endok		h.ho_endok
#define	h_ep		h.ho_ep
#define	h_inc		h.ho_inc
#define	h_Om		h.ho_Om
#define	h_om		h.ho_om
#define	h_e		h.ho_e
#define	h_qp		h.ho_qp
#define	h_g		h.ho_g
#define	h_k		h.ho_k
#define	h_size		h.ho_size

#define	p_epoch		p.po_epoch
#define	p_startok	p.po_startok
#define	p_endok		p.po_endok
#define	p_ep		p.po_ep
#define	p_inc		p.po_inc
#define	p_qp		p.po_qp
#define	p_om		p.po_om
#define	p_Om		p.po_Om
#define	p_g		p.po_g
#define	p_k		p.po_k
#define	p_size		p.po_size

#define	es_epoch	es.eso_epoch
#define	es_startok	es.eso_startok
#define	es_endok	es.eso_endok
#define	es_inc		es.eso_inc
#define	es_raan		es.eso_raan
#define	es_e		es.eso_e
#define	es_ap		es.eso_ap
#define	es_M		es.eso_M
#define	es_n		es.eso_n
#define	es_decay	es.eso_decay
#define	es_drag		es.eso_drag
#define	es_orbit	es.eso_orbit

#define	pl_code		pl.plo_code
#define	pl_moon		pl.plo_moon
#define	pl_evis		pl.plo_evis
#define	pl_svis		pl.plo_svis
#define	pl_x		pl.plo_x
#define	pl_y		pl.plo_y
#define	pl_z		pl.plo_z
#define	pl_aux1		pl.plo_aux1
#define	pl_aux2		pl.plo_aux2

#define b_2compute	b.b_2compute
#define b_2spect	b.b_2spect
#define b_2mag		b.b_2mag
#define b_bo		b.u.b_bo
#define b_bp		b.u.b_bp
#define b_nbp		b.b_nbp

/* insure we always refer to the fields and no monkey business */
#undef OBJ_COMMON_FLDS
#undef OBJ_SOLSYS_FLDS

/* o_type code.
 * N.B. names are assigned in order in objmenu.c
 * N.B. if add one add switch in obj_cir().
 * N.B. UNDEFOBJ must be zero so new objects are undefinied by being zeroed.
 * N.B. maintain the bitmasks too.
 */
enum ObjType {
    UNDEFOBJ=0,
    FIXED, BINARYSTAR, ELLIPTICAL, HYPERBOLIC, PARABOLIC, EARTHSAT, PLANET,
    NOBJTYPES
};

/* types as handy bitmasks too */
#define	OBJTYPE2MASK(t)	(1<<(t))
#define	FIXEDM		OBJTYPE2MASK(FIXED)
#define	BINARYSTARM	OBJTYPE2MASK(BINARYSTAR)
#define	ELLIPTICALM	OBJTYPE2MASK(ELLIPTICAL)
#define	HYPERBOLICM	OBJTYPE2MASK(HYPERBOLIC)
#define	PARABOLICM	OBJTYPE2MASK(PARABOLIC)
#define	EARTHSATM	OBJTYPE2MASK(EARTHSAT)
#define	PLANETM		OBJTYPE2MASK(PLANET)
#define	ALLM		(~0)

/* rise, set and transit information.
 */
typedef struct {
    int rs_flags;	/* info about what has been computed and any
			 * special conditions; see flags, below.
			 */
    double rs_risetm;	/* mjd time of rise today */
    double rs_riseaz;	/* azimuth of rise, rads E of N */
    double rs_trantm;	/* mjd time of transit today */
    double rs_tranalt;	/* altitude of transit, rads up from horizon */
    double rs_tranaz;	/* azimuth of transit, rads E of N */
    double rs_settm;	/* mjd time of set today */
    double rs_setaz;	/* azimuth of set, rads E of N */
} RiseSet;

/* RiseSet flags */
#define	RS_NORISE	0x0001	/* object does not rise as such today */
#define	RS_NOSET	0x0002	/* object does not set as such today */
#define	RS_NOTRANS	0x0004	/* object does not transit as such today */
#define	RS_CIRCUMPOLAR	0x0010	/* object stays up all day today */
#define	RS_NEVERUP	0x0020	/* object never up at all today */
#define	RS_ERROR	0x1000	/* can't figure out anything! */
#define	RS_RISERR	(0x0100|RS_ERROR) /* error computing rise */
#define	RS_SETERR	(0x0200|RS_ERROR) /* error computing set */
#define	RS_TRANSERR	(0x0400|RS_ERROR) /* error computing transit */

#define	is_type(op,m)	(OBJTYPE2MASK((op)->o_type) & (m))

/* any planet or its moons */
#define	is_planet(op,p)	(is_type(op,PLANETM) && op->pl_code == (p))

/* any solar system object */
#define	is_ssobj(op)	is_type(op,PLANETM|HYPERBOLICM|PARABOLICM|ELLIPTICALM)


/* natural satellite support */

typedef struct {
    char *full;		/* full name */
    char *tag;		/* Roman numeral tag */
    float x, y, z;	/* sky loc in planet radii: +x:east +y:south +z:front */
    float ra, dec;	/* sky location in ra/dec */
    float mag;		/* magnitude */
    int evis;		/* whether geometrically visible from earth */
    int svis;		/* whether in sun light */
    int pshad;		/* whether moon is casting shadow on planet */
    int trans;		/* whether moon is transiting */
    float sx, sy;	/* shadow sky loc in planet radii: +x:east +y:south */
} MoonData;

/* separate set for each planet -- use in pl_moon */


enum _marsmoons {
    M_MARS = 0,					/* == X_PLANET */
    M_PHOBOS, M_DEIMOS,
    M_NMOONS					/* including planet at 0 */
};

enum _jupmoons {
    J_JUPITER = 0,				/* == X_PLANET */
    J_IO, J_EUROPA, J_GANYMEDE, J_CALLISTO,
    J_NMOONS					/* including planet */
};

enum _satmoons {
    S_SATURN = 0,				/* == X_PLANET */
    S_MIMAS, S_ENCELADUS, S_TETHYS, S_DIONE,
    S_RHEA, S_TITAN, S_HYPERION, S_IAPETUS,
    S_NMOONS					/* including planet */
};

enum _uramoons {
    U_URANUS = 0,				/* == X_PLANET */
    U_ARIEL, U_UMBRIEL, U_TITANIA, U_OBERON, U_MIRANDA,
    U_NMOONS					/* including planet */
};

#define	X_MAXNMOONS	S_NMOONS		/* N.B. chosen by hand */


/* global function declarations */


/* aa_hadec.c */
ASTRO_EXPORT void aa_hadec (double lt, double alt, double az, double *ha,
    double *dec);
ASTRO_EXPORT  void hadec_aa (double lt, double ha, double dec, double *alt,
    double *az); 

/* aberration.c */
ASTRO_EXPORT  void ab_ecl (double m, double lsn, double *lam, double *bet);
ASTRO_EXPORT  void ab_eq (double m, double lsn, double *ra, double *dec);

/* airmass.c */
ASTRO_EXPORT  void airmass (double aa, double *Xp);

/* anomaly.c */
ASTRO_EXPORT  void anomaly (double ma, double s, double *nu, double *ea);

/* ap_as.c */
ASTRO_EXPORT  void ap_as ( Now *np, double Mjd, double *rap, double *decp);
ASTRO_EXPORT  void as_ap ( Now *np, double Mjd, double *rap, double *decp);

/* atlas.c */
ASTRO_EXPORT  char *um_atlas (double ra, double dec);
ASTRO_EXPORT  char *u2k_atlas (double ra, double dec);
ASTRO_EXPORT  char *msa_atlas (double ra, double dec);

/* aux.c */
ASTRO_EXPORT  double mm_mjed (Now *np);

/* chap95.c */
ASTRO_EXPORT  int chap95 (double m, int obj, double prec, double *ret);

/* chap95_data.c */

/* circum.c */
ASTRO_EXPORT  int obj_cir (Now *np, Obj *op);

/* comet.c */
ASTRO_EXPORT  void comet (double m, double ep, double inc, double ap, double qp,
    double om, double *lpd, double *psi, double *rp, double *rho, double *lam,
    double *bet);

/* constel.c */
#define	NCNS	89
ASTRO_EXPORT  int cns_pick (double r, double d, double e);
ASTRO_EXPORT  int cns_id (char *abbrev);
ASTRO_EXPORT  char *cns_name (int id);
ASTRO_EXPORT  int cns_edges (double e, double **ra0p, double **dec0p, double **ra1p,
    double **dec1p);
ASTRO_EXPORT  int cns_list (double ra, double dec, double e, double rad, int ids[]);
ASTRO_EXPORT  int cns_figure (int id, double e, double ra[],double dec[],int dcodes[]);
ASTRO_EXPORT  int cns_loadfigs (FILE *fp, char msg[]);

/* dbfmt.c */
ASTRO_EXPORT  int db_crack_line (char s[], Obj *op, char nm[][MAXNM], int nnm,
    char whynot[]);
ASTRO_EXPORT  void db_write_line (Obj *op, char *lp);
ASTRO_EXPORT  int dbline_candidate (char line[]);
ASTRO_EXPORT  int get_fields (char *s, int delim, char *fields[]);
ASTRO_EXPORT  int db_tle (char *name, char *l1, char *l2, Obj *op);
ASTRO_EXPORT  int dateRangeOK (Now *np, Obj *op);

/* deltat.c */
ASTRO_EXPORT  double deltat (double m);

/* earthsat.c */
ASTRO_EXPORT  int obj_earthsat (Now *np, Obj *op);

/* eq_ecl.c */
ASTRO_EXPORT  void eq_ecl (double m, double ra, double dec, double *lt,double *lg);
ASTRO_EXPORT  void ecl_eq (double m, double lt, double lg, double *ra,double *dec);

/* eq_gal.c */
ASTRO_EXPORT  void eq_gal (double m, double ra, double dec, double *lt,double *lg);
ASTRO_EXPORT  void gal_eq (double m, double lt, double lg, double *ra,double *dec);

/* formats.c */
ASTRO_EXPORT  int fs_sexa (char *out, double a, int w, int fracbase);
ASTRO_EXPORT  int fs_date (char out[], int format, double jd);
ASTRO_EXPORT  int f_scansexa (const char *str, double *dp);
ASTRO_EXPORT  void f_sscandate (char *bp, int pref, int *m, double *d, int *y);

/* helio.c */
ASTRO_EXPORT  void heliocorr (double jd, double ra, double dec, double *hcp);

/* jupmoon.c */
ASTRO_EXPORT  void jupiter_data (double Mjd, char dir[], Obj *sop, Obj *jop,
    double *jupsize, double *cmlI, double *cmlII, double *polera,
    double *poledec, MoonData md[J_NMOONS]); 
ASTRO_EXPORT  void meeus_jupiter (double d, double *cmlI, double *cmlII,
    MoonData md[J_NMOONS]);

/* libration.c */
ASTRO_EXPORT  void llibration (double JD, double *llatp, double *llonp);

/* magdecl.c */
ASTRO_EXPORT  int magdecl (double l, double L, double e, double y, char *dir,
    double *dp, char *err);

/* marsmoon.c */
ASTRO_EXPORT  void marsm_data (double Mjd, char dir[], Obj *sop, Obj *mop,
    double *marssize, double *polera, double *poledec, MoonData md[M_NMOONS]); 

/* misc.c */
ASTRO_EXPORT  void zero_mem (void *loc, unsigned len);
ASTRO_EXPORT  int tickmarks (double min, double max, int numdiv, double ticks[]);
ASTRO_EXPORT  int lc (int cx, int cy, int cw, int x1, int y1, int x2, int y2,
    int *sx1, int *sy1, int *sx2, int *sy2);
ASTRO_EXPORT  void hg_mag (double h, double g, double rp, double rho, double rsn,
    double *mp);
ASTRO_EXPORT  int magdiam (int fmag, int magstp, double scale, double mag,
    double size);
ASTRO_EXPORT  void gk_mag (double g, double k, double rp, double rho, double *mp);
ASTRO_EXPORT  double atod (char *buf);
ASTRO_EXPORT  void solve_sphere (double A, double b, double cc, double sc,
    double *cap, double *Bp);
ASTRO_EXPORT  double delra (double dra);
ASTRO_EXPORT  void now_lst (Now *np, double *lstp);
ASTRO_EXPORT  void radec2ha (Now *np, double ra, double dec, double *hap);
ASTRO_EXPORT  void gha (Now *np, Obj *op, double *ghap);
ASTRO_EXPORT  char *obj_description (Obj *op);
ASTRO_EXPORT  int is_deepsky (Obj *op);

/* mjd.c */
ASTRO_EXPORT  void cal_mjd (int mn, double dy, int yr, double *m);
ASTRO_EXPORT  void mjd_cal (double m, int *mn, double *dy, int *yr);
ASTRO_EXPORT  int mjd_dow (double m, int *dow);
ASTRO_EXPORT  int isleapyear (int year);
ASTRO_EXPORT  void mjd_dpm (double m, int *ndays);
ASTRO_EXPORT  void mjd_year (double m, double *yr);
ASTRO_EXPORT  void year_mjd (double y, double *m);
ASTRO_EXPORT  void rnd_second (double *t);
ASTRO_EXPORT  void mjd_dayno (double jd, int *yr, double *dy);
ASTRO_EXPORT  double mjd_day (double jd);
ASTRO_EXPORT  double mjd_hr (double jd);
ASTRO_EXPORT  void range (double *v, double r);
ASTRO_EXPORT  void radecrange (double *ra, double *dec);

/* moon.c */
ASTRO_EXPORT  void moon (double m, double *lam, double *bet, double *rho,
    double *msp, double *mdp);

/* mooncolong.c */
ASTRO_EXPORT  void moon_colong (double jd, double lt, double lg, double *cp,
    double *kp, double *ap, double *sp);

/* moonnf.c */
ASTRO_EXPORT  void moonnf (double mj, double *mjn, double *mjf);

/* nutation.c */
ASTRO_EXPORT  void nutation (double m, double *deps, double *dpsi);
ASTRO_EXPORT  void nut_eq (double m, double *ra, double *dec);

/* obliq.c */
ASTRO_EXPORT  void obliquity (double m, double *eps);

/* parallax.c */
ASTRO_EXPORT  void ta_par (double tha, double tdec, double phi, double ht,
    double *rho, double *aha, double *adec);

/* parallactic.c */
ASTRO_EXPORT  double parallacticLDA (double lt, double dec, double alt);
ASTRO_EXPORT  double parallacticLHD (double lt, double ha, double dec);

/* plans.c */
ASTRO_EXPORT  void plans (double m, PLCode p, double *lpd0, double *psi0,
    double *rp0, double *rho0, double *lam, double *bet, double *dia,
    double *mag);

/* plshadow.c */
ASTRO_EXPORT  int plshadow (Obj *op, Obj *sop, double polera,
    double poledec, double x, double y, double z, float *sxp, float *syp);

/* plmoon_cir.c */
ASTRO_EXPORT  int plmoon_cir (Now *np, Obj *moonop);
ASTRO_EXPORT  int getBuiltInObjs (Obj **opp);
ASTRO_EXPORT  void setMoonDir (char *dir);

/* precess.c */
ASTRO_EXPORT  void precess (double mjd1, double mjd2, double *ra, double *dec);

/* reduce.c */
ASTRO_EXPORT  void reduce_elements (double mjd0, double m, double inc0,
    double ap0, double om0, double *inc, double *ap, double *om);

/* refract.c */
ASTRO_EXPORT  void unrefract (double pr, double tr, double aa, double *ta);
ASTRO_EXPORT  void refract (double pr, double tr, double ta, double *aa);

/* rings.c */
ASTRO_EXPORT  void satrings (double sb, double sl, double sr, double el, double er,
    double JD, double *etiltp, double *stiltp);

/* riset.c */
ASTRO_EXPORT  void riset (double ra, double dec, double lt, double dis,
    double *lstr, double *lsts, double *azr, double *azs, int *status);

/* riset_cir.c */
ASTRO_EXPORT  void riset_cir (Now *np, Obj *op, double dis, RiseSet *rp);
ASTRO_EXPORT  void twilight_cir (Now *np, double dis, double *dawn, double *dusk,
    int *status);

/* satmoon.c */
ASTRO_EXPORT  void saturn_data (double Mjd, char dir[], Obj *eop, Obj *sop,
    double *satsize, double *etilt, double *stlit, double *polera, 
    double *poledec, MoonData md[S_NMOONS]); 

/* sphcart.c */
ASTRO_EXPORT  void sphcart (double l, double b, double r, double *x, double *y,
    double *z);
ASTRO_EXPORT  void cartsph (double x, double y, double z, double *l, double *b,
    double *r);

/* sun.c */
ASTRO_EXPORT  void sunpos (double m, double *lsn, double *rsn, double *bsn);

/* twobody.c */
ASTRO_EXPORT  int vrc (double *v, double *r, double tp, double e, double q);

/* umoon.c */
ASTRO_EXPORT  void uranus_data (double Mjd, char dir[], Obj *sop, Obj *uop,
    double *usize, double *polera, double *poledec, MoonData md[U_NMOONS]); 

/* utc_gst.c */
ASTRO_EXPORT  void utc_gst (double m, double utc, double *gst);
ASTRO_EXPORT  void gst_utc (double m, double gst, double *utc);

/* vsop87.c */
ASTRO_EXPORT  int vsop87 (double m, int obj, double prec, double *ret);

#endif /* _ASTRO_H */

