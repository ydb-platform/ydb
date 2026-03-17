/* find rise and set circumstances, ie, riset_cir() and related functions. */

#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>

#include "astro.h"


static void e_riset_cir (Now *np, Obj *op, double dis, RiseSet *rp);
static int find_0alt (double dt, double fstep, double dis, Now *np, Obj *op);
static int find_transit (double dt, Now *np, Obj *op);
static int find_maxalt (Now *np, Obj *op, double tr, double ts, double *tp,
    double *alp, double *azp);

/* find where and when an object, op, will rise and set and
 *   it's transit circumstances. all times are utc mjd, angles rads e of n.
 * dis is the angle down from an ideal horizon, in rads (see riset()).
 * N.B. dis should NOT include refraction, we do that here.
 */
void
riset_cir (Now *np, Obj *op, double dis, RiseSet *rp)
{
	double mjdn;	/* mjd of local noon */
	double lstn;	/* lst at local noon */
	double lr, ls;	/* lst rise/set times */
	double ar, as;	/* az of rise/set */
	double ran;	/* RA at noon */
	Now n;		/* copy to move time around */
	Obj o;		/* copy to get circumstances at n */
	int rss;	/* temp status */

	/* work with local copies so we can move the time around */
	(void) memcpy ((void *)&n, (void *)np, sizeof(n));
	(void) memcpy ((void *)&o, (void *)op, sizeof(o));

	/* fast Earth satellites need a different approach.
	 * "fast" here is pretty arbitrary -- just too fast to work with the
	 * iterative approach based on refining the times for a "fixed" object.
	 */
	if (op->o_type == EARTHSAT && op->es_n > FAST_SAT_RPD) {
	    e_riset_cir (&n, &o, dis, rp);
	    return;
	}

	/* assume no problems initially */
	rp->rs_flags = 0;

	/* start the iteration at local noon */
	mjdn = mjd_day(mjd - tz/24.0) + tz/24.0 + 0.5;
	n.n_mjd = mjdn;
	now_lst (&n, &lstn);

	/* first approximation is to find rise/set times of a fixed object
	 * at the current epoch in its position at local noon.
	 * N.B. add typical refraction if dis is above horizon for initial
	 *   go/no-go test. if it passes, real code does refraction rigorously.
	 */
	n.n_mjd = mjdn;
	if (obj_cir (&n, &o) < 0) {
	    rp->rs_flags = RS_ERROR;
	    return;
	}
	ran = o.s_gaera;
	riset (o.s_gaera, o.s_gaedec, lat, dis+(dis>.01 ? 0 : .01), &lr, &ls,
								&ar, &as, &rss);
	switch (rss) {
	case  0:  break;
	case  1: rp->rs_flags = RS_NEVERUP; return;
	case -1: rp->rs_flags = RS_CIRCUMPOLAR; goto dotransit;
	default: rp->rs_flags = RS_ERROR; return;
	}

	/* iterate to find better rise time */
	n.n_mjd = mjdn;
	switch (find_0alt ((lr - lstn)/SIDRATE, 60/SPD, dis, &n, &o)) {
	case 0: /* ok */
	    rp->rs_risetm = n.n_mjd;
	    rp->rs_riseaz = o.s_az;
	    break;
	case -1: /* obj_cir error */
	    rp->rs_flags |= RS_RISERR;
	    break;
	case -2: /* converged but not today, err but give times anyway */
	    rp->rs_risetm = n.n_mjd;
	    rp->rs_riseaz = o.s_az;
	    rp->rs_flags |= RS_NORISE;
	    break;
	case -3: /* probably never up */
	    rp->rs_flags |= RS_NEVERUP;
	    break;
	}

	/* iterate to find better set time */
	n.n_mjd = mjdn;
	switch (find_0alt ((ls - lstn)/SIDRATE, 60/SPD, dis, &n, &o)) {
	case 0: /* ok */
	    rp->rs_settm = n.n_mjd;
	    rp->rs_setaz = o.s_az;
	    break;
	case -1: /* obj_cir error */
	    rp->rs_flags |= RS_SETERR;
	    break;
	case -2: /* converged but not today, err but give times anyway */
	    rp->rs_settm = n.n_mjd;
	    rp->rs_setaz = o.s_az;
	    rp->rs_flags |= RS_NOSET;
	    break;
	case -3: /* probably circumpolar */
	    rp->rs_flags |= RS_CIRCUMPOLAR;
	    break;
	}

	/* can try transit even if rise or set failed */
    dotransit:
	n.n_mjd = mjdn;
	switch (find_transit ((radhr(ran) - lstn)/SIDRATE, &n, &o)) {
	case 0: /* ok */
	    rp->rs_trantm = n.n_mjd;
	    rp->rs_tranalt = o.s_alt;
	    rp->rs_tranaz = o.s_az;
	    break;
	case -1: /* did not converge */
	    rp->rs_flags |= RS_TRANSERR;
	    break;
	case -2: /* converged but not today */
	    rp->rs_flags |= RS_NOTRANS;
	    break;
	}
}

/* find local times when sun is dis rads below horizon.
 */
void
twilight_cir (Now *np, double dis, double *dawn, double *dusk, int *status)
{
	RiseSet rs;
	Obj o;

	memset (&o, 0, sizeof(o));
	o.o_type = PLANET;
	o.pl_code = SUN;
	(void) strcpy (o.o_name, "Sun");
	riset_cir (np, &o, dis, &rs);
	*dawn = rs.rs_risetm;
	*dusk = rs.rs_settm;
	*status = rs.rs_flags;
}

/* find where and when a fast-moving Earth satellite, op, will rise and set and
 *   it's transit circumstances. all times are mjd, angles rads e of n.
 * dis is the angle down from the local topo horizon, in rads (see riset()).
 * idea is to walk forward in time looking for alt+dis==0 crossings.
 * initial time step is a few degrees (based on average daily motion).
 * we stop as soon as we see both a rise and set.
 * N.B. we assume *np and *op are working copies we can mess up.
 */
static void
e_riset_cir (Now *np, Obj *op, double dis, RiseSet *rp)
{
#define	DEGSTEP	2		/* time step is about this many degrees */
	int steps;		/* max number of time steps */
	double dt;		/* time change per step, days */
	double t0, t1;		/* current and next mjd values */
	double a0, a1;		/* altitude at t0 and t1 */
	int rise, set;		/* flags to check when we find these events */
	int i;

	dt = DEGSTEP * (1.0/360.0/op->es_n);
	steps = (int)(1.0/dt);
	rise = set = 0;
	rp->rs_flags = 0;

	if (obj_cir (np, op) < 0) {
	    rp->rs_flags |= RS_ERROR;
	    return;
	}

	t0 = mjd;
	a0 = op->s_alt + dis;

	for (i = 0; i < steps && (!rise || !set); i++) {
	    mjd = t1 = t0 + dt;
	    if (obj_cir (np, op) < 0) {
		rp->rs_flags |= RS_ERROR;
		return;
	    }
	    a1 = op->s_alt + dis;

	    if (a0 < 0 && a1 > 0 && !rise) {
		/* found a rise event -- interate to refine */
		switch (find_0alt (10./3600., 5./SPD, dis, np, op)) {
		case 0: /* ok */
		    rp->rs_risetm = np->n_mjd;
		    rp->rs_riseaz = op->s_az;
		    rise = 1;
		    break;
		case -1: /* obj_cir error */
		    rp->rs_flags |= RS_RISERR;
		    return;
		case -2: /* converged but not today */ /* FALLTHRU */
		case -3: /* probably never up */
		    rp->rs_flags |= RS_NORISE;
		    return;
		}
	    } else if (a0 > 0 && a1 < 0 && !set) {
		/* found a setting event -- interate to refine */
		switch (find_0alt (10./3600., 5./SPD, dis, np, op)) {
		case 0: /* ok */
		    rp->rs_settm = np->n_mjd;
		    rp->rs_setaz = op->s_az;
		    set = 1;
		    break;
		case -1: /* obj_cir error */
		    rp->rs_flags |= RS_SETERR;
		    return;
		case -2: /* converged but not today */ /* FALLTHRU */
		case -3: /* probably circumpolar */
		    rp->rs_flags |= RS_NOSET;
		    return;
		}
	    }

	    t0 = t1;
	    a0 = a1;
	}

	/* instead of transit, for satellites we find time of maximum
	 * altitude, if we know both the rise and set times.
	 */
	if (rise && set) {
	    double tt, al, az;
	    if (find_maxalt (np, op, rp->rs_risetm, rp->rs_settm, &tt, &al, &az) < 0) {
		rp->rs_flags |= RS_TRANSERR;
		return;
	    }
	    rp->rs_trantm = tt;
	    rp->rs_tranalt = al;
	    rp->rs_tranaz = az;
	} else
	    rp->rs_flags |= RS_NOTRANS;

	/* check for some bad conditions */
	if (!rise) {
	    if (a0 > 0)
		rp->rs_flags |= RS_CIRCUMPOLAR;
	    else
		rp->rs_flags |= RS_NORISE;
	}
	if (!set) {
	    if (a0 < 0)
		rp->rs_flags |= RS_NEVERUP;
	    else
		rp->rs_flags |= RS_NOSET;
	}
}

/* given a Now at noon and a dt from np, in hours, for a first approximation
 * to a rise or set event, refine the event by searching for when alt+dis = 0.
 * return 0: if find one within 12 hours of noon with np and op set to the
 *    better time and circumstances;
 * return -1: if error from obj_cir;
 * return -2: if converges but not today;
 * return -3: if does not converge at all (probably circumpolar or never up);
 */
static int
find_0alt (
double dt,	/* hours from initial np to first guess at event */
double fstep,	/* first step size, days */
double dis,	/* horizon displacement, rads */
Now *np,	/* working Now -- starts with mjd is noon, returns as answer */
Obj *op)	/* working object -- returns as answer */
{
#define	TMACC		(0.01/SPD)	/* convergence accuracy, days; tight for stable az */
#define	MAXPASSES	20		/* max iterations to try */
#define	MAXSTEP		(12.0/24.0)	/* max time step,days (to detect flat)*/

	double a0 = 0;
	double mjdn = mjd;
	int npasses;

	/* insure initial guess is today -- if not, move by 24 hours */
	if (dt < -12.0 && !find_0alt (dt+24, fstep, dis, np, op))
	    return (0);
	mjd = mjdn;
	if (dt > 12.0 && !find_0alt (dt-24, fstep, dis, np, op))
	    return (0);
	mjd = mjdn;
	
	/* convert dt to days for remainder of algorithm */
	dt /= 24.0;

	/* use secant method to look for s_alt + dis == 0 */
	npasses = 0;
	do {
	    double a1;

	    mjd += dt;
	    if (obj_cir (np, op) < 0)
		return (-1);
	    a1 = op->s_alt;

	    dt = (npasses == 0) ? fstep : (dis+a1)*dt/(a0-a1);
	    a0 = a1;

	    if (++npasses > MAXPASSES || fabs(dt) >= MAXSTEP)
		return (-3);

	} while (fabs(dt)>TMACC);
	// fprintf (stderr, "%s 0alt npasses = %d\n", op->o_name, npasses);

	/* return codes */
	return (fabs(mjdn-mjd) < .5 ? 0 : -2);

#undef	MAXPASSES
#undef	MAXSTEP
#undef	TMACC
}

/* find when the given object transits. start the search when LST matches the
 *   object's RA at noon.
 * if ok, return 0 with np and op set to the transit conditions; if can't
 *   converge return -1; if converges ok but not today return -2.
 * N.B. we assume np is passed set to local noon.
 */
static int
find_transit (double dt, Now *np, Obj *op)
{
#define	MAXLOOPS	10
#define	MAXERR		(1./3600.)		/* hours */
	double mjdn = mjd;
	double lst;
	int i;

	/* insure initial guess is today -- if not, move by 24 hours */
	if (dt < -12.0)
	    dt += 24.0;
	if (dt > 12.0)
	    dt -= 24.0;

	i = 0;
	do {
	    mjd += dt/24.0;
	    if (obj_cir (np, op) < 0)
		return (-1);
	    now_lst (np, &lst);
	    dt = (radhr(op->s_gaera) - lst);
	    if (dt < -12.0)
		dt += 24.0;
	    if (dt > 12.0)
		dt -= 24.0;
	} while (++i < MAXLOOPS && fabs(dt) > MAXERR);

	/* fprintf (stderr, "%s find_transit loops = %d, dt = %g seconds\n", op->o_name, i, dt*3600); */

	/* return codes */
	if (i == MAXLOOPS)
	    return (-1);
	return (fabs(mjd - mjdn) < 0.5 ? 0 : -2);

#undef	MAXLOOPS
#undef	MAXERR
}

/* find the mjd time of max altitude between the given rise and set times.
 * N.B. we assume *np and *op are working copies we can modify.
 * return 0 if ok, else -1.
 */
static int
find_maxalt (
Now *np,
Obj *op,
double tr, double ts,		/* mjd of rise and set */
double *tp, 			/* time of max altitude */
double *alp, double *azp)	/* max altitude and transit az at said time */
{
#define	MAXLOOPS	100	/* max loops */
#define	MAXERR	(1.0/SPD)	/* days */

	double l, r;		/* times known to bracket max alt */
	double m1, m2;		/* intermediate range points inside l and r */
	double a1, a2;		/* alt at m1 and m2 */
	int nloops;		/* max loop check */

	/* want rise before set */
	while (ts < tr)
	    tr -= 1.0/op->es_n;

	/* init time bracket */
	l = tr;
	r = ts;

	/* ternary search for max */
	for (nloops = 0; r - l > MAXERR && nloops < MAXLOOPS; nloops++) {

	    mjd = m1 = (2*l + r)/3;
	    obj_cir (np, op);
	    a1 = op->s_alt;

	    mjd = m2 = (l + 2*r)/3;
	    obj_cir (np, op);
	    a2 = op->s_alt;

	    if (a1 < a2)
		l = m1;
	    else
	        r = m2;
	}
	// fprintf (stderr, "tern nloops = %d\n", nloops);
	if (nloops >= MAXLOOPS)
	    return (-1);

	/* best is between l and r */
	mjd = *tp = (l+r)/2;
	obj_cir (np, op);
	*alp = op->s_alt;
	*azp = op->s_az;

	return (0);
#undef	MAXERR
#undef	MAXLOOPS
}

