#include <stdio.h>
#include <math.h>

#include "astro.h"

static void m (double t, double k, double *mj);

#define	unw(w,z)	((w)-floor((w)/(z))*(z))

/* given a modified Julian date, mj, return the mjd of the new
 * and full moons about then, mjn and mjf.
 * TODO: exactly which ones does it find? eg:
 *   5/28/1988 yields 5/15 and 5/31
 *   5/29             6/14     6/29
 */
void
moonnf (double mj, double *mjn, double *mjf)
{
	int mo, yr;
	double dy;
	double mj0;
	double k, tn, tf, t;

	mjd_cal (mj, &mo, &dy, &yr);
	cal_mjd (1, 0., yr, &mj0);
	k = (yr-1900+((mj-mj0)/365))*12.3685;
	k = floor(k+0.5);
	tn = k/1236.85;
	tf = (k+0.5)/1236.85;
	t = tn;
	m (t, k, mjn);
	t = tf;
	k += 0.5;
	m (t, k, mjf);
}

static void
m (double t, double k, double *mj)
{
	double t2, a, a1, b, b1, c, ms, mm, f, ddjd;

	t2 = t*t;
	a = 29.53*k;
	c = degrad(166.56+(132.87-9.173e-3*t)*t);
	b = 5.8868e-4*k+(1.178e-4-1.55e-7*t)*t2+3.3e-4*sin(c)+7.5933E-1;
	ms = 359.2242+360*unw(k/1.236886e1,1)-(3.33e-5+3.47e-6*t)*t2;
	mm = 306.0253+360*unw(k/9.330851e-1,1)+(1.07306e-2+1.236e-5*t)*t2;
	f = 21.2964+360*unw(k/9.214926e-1,1)-(1.6528e-3+2.39e-6*t)*t2;
	ms = unw(ms,360);
	mm = unw(mm,360);
	f = unw(f,360);
	ms = degrad(ms);
	mm = degrad(mm);
	f = degrad(f);
	ddjd = (1.734e-1-3.93e-4*t)*sin(ms)+2.1e-3*sin(2*ms)
		-4.068e-1*sin(mm)+1.61e-2*sin(2*mm)-4e-4*sin(3*mm)
		+1.04e-2*sin(2*f)-5.1e-3*sin(ms+mm)-7.4e-3*sin(ms-mm)
		+4e-4*sin(2*f+ms)-4e-4*sin(2*f-ms)-6e-4*sin(2*f+mm)
		+1e-3*sin(2*f-mm)+5e-4*sin(ms+2*mm);
	a1 = (long)a;
	b = b+ddjd+(a-a1);
	b1 = (long)b;
	a = a1+b1;
	b = b-b1;
	*mj = a + b;
}

