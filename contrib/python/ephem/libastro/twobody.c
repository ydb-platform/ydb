/*
 *
 * TWOBODY.C
 *
 *  Computation of planetary position, two-body computation
 *
 *  Paul Schlyter, 1987-06-15
 *
 *  Decreased EPSILON from 2E-4 to 3E-8,  1988-12-05
 *
 *  1990-01-01:  Bug fix in almost parabolic orbits: now the routine
 *       doesn't bomb there (an if block was too large)
 *
 *  2000-12-06:  Donated to Elwood Downey if he wants to use it in XEphem
 */
 
 
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
 
 
/* Constants used when solving Kepler's equation */
#undef  EPSILON
#define EPSILON   3E-8
#undef  INFINITY
#define INFINITY  1E+10
 
/* Math constants */
#undef  PI
#define PI      3.14159265358979323846
#define RADEG   ( 180.0 / PI )
#define DEGRAD  ( PI / 180.0 )
 
/* Trig functions in degrees */
#define sind(x)      sin(x*DEGRAD)
#define cosd(x)      cos(x*DEGRAD)
#define atand(x)     (RADEG*atan(x))
#define atan2d(y,x)  (RADEG*atan2(y,x))
 
/* Gauss' grav.-konstant */
#define K    1.720209895E-2
#define KD   ( K * 180.0 / PI )
#define K2   ( K / 2.0 )
 
 
 
 
static double cubroot( double x )
/* Cubic root */
{
    double a,b;
 
    if ( x == 0.0 )
        return  0.0;
    else
    {
        a = fabs(x);
        b = exp( log(a) / 3.0 );
        return  x > 0.0 ? b : -b;
    }
}  /* cubroot */
 
 
static double rev180( double ang )
/* Normalize angle to between +180 and -180 degrees */
{
    return  ang  -  360.0 * floor(ang*(1.0/360.0) + 0.5);
}  /* rev180 */
 
 
 
static double kepler( double m, double ex )
/*
 * Solves Kepler's equation
 *  m      = mean anomaly
 *  ex     = eccentricity
 *  kepler = eccentric anomaly
 */
{
    double m1, sinm, cosm, exd, exan, dexan, lim1, adko, adk, denom;
    int converged;
 
    m1 = rev180(m);
    sinm = sind(m1);
    cosm = cosd(m1);
    /* 1st approximation: */
    exan = atan2d(sinm,cosm-ex);
    if ( ex > 0.008 )
    { /* Iteration formula: */
        exd = ex * RADEG;
        lim1 = 1E-3 / ex;
        adko = INFINITY;
        denom = 1.0 - ex * cosd(exan);
        do
        {
            dexan = (m1 + exd * sind(exan) - exan) / denom;
            exan = exan + dexan;
            adk = fabs(dexan);
            converged = adk < EPSILON  ||  adk >= adko ;
            adko = adk;
            if ( !converged  &&  adk > lim1 )
                denom = 1.0 - ex * cosd(exan);
        } while ( !converged );
    }
    return  exan;
}  /* kepler */
 
 
static void vr( double *v, double *r, double m, double e, double a )
/*
 * Elliptic orbits only:
 * computes: v = true anomaly   (degrees)
 *           r = radius vector  (a.u.)
 *   from:   m = mean anomaly   (degrees)
 *           e = eccentricity
 *           a = semimajor axis (a.u.)
 */
{
    double ean, x, y;
 
    ean = kepler(m,e);
    x = a*(cosd(ean)-e);
    y = a*sqrt(1.-e*e)*sind(ean);
    *r = sqrt(x*x+y*y);
    *v = atan2d(y,x);
}  /* vr */
 
 
/* return 0 if ok, else -1 */
int vrc( double *v, double *r, double tp, double e, double q )
/*
 * Elliptic, hyperbolic and near-parabolic orbits:
 * computes: v  = true anomaly  (degrees)
 *           r  = radius vector (a.u.)
 *   from:   tp = time from perihelion (days)
 *           e  = eccentricity
 *           q  = perihelion distance (a.u.)
 */
{
 
    double lambda;
 
    double a, b, w, w2, w4, c, c1, c2, c3, c5, a0, a1, a2,
           a3, m, n, g, adgg, adgg2, gs, dg;
 
    if ( tp == 0.0 )  /* In perihelion */
    {
        *v = 0.0;
        *r = q;
        return 0;
    }
 
 
    lambda = (1.0-e) / (1.0+e);
 
    if ( fabs(lambda) < 0.01 )
    {  /* Near-parabolic orbits */
        a = K2 * sqrt((1.0+e)/(q*q*q)) * tp;
        b = sqrt( 1.0 + 2.25*a*a );
        w = cubroot( b + 1.5*a ) - cubroot( b - 1.5*a );
 
        /* Test if it's accuate enough to compute this as a near-parabolic orbit */
        if ( fabs(w*w*lambda) > 0.2 )
        {
            if ( fabs(lambda) < 0.0002 )
            {
                /* Sorry, but we cannot compute this at all -- we must give up!
                 *
                 * This happens very rarely, in orbits having an eccentricity
                 * some 2% away from 1.0 AND if the body is very very far from
                 * perihelion.  E.g. a Kreutz sun-grazing comet having
                 * eccentricity near 0.98 or 1.02, and being outside
                 * the orbit of Pluto.  For any reasonable orbit this will
                 * never happen in practice.
                 *
                 */
		return -1;
            }
            else
            {
                /* We cannot compute this as a near-parabolic orbit, so let's
                   compute it as an elliptic or hyperbolic orbit instead. */
                goto ellipse_hyperbola;
            }
        }
 
        /* Go ahead computing the near-parabolic case */
        c = 1.0 + 1.0 / (w*w);
        c1 = 1.0 / c;
        c2 = c1*c1;
        c3 = c1*c2;
        c5 = c3*c2;
        w2 = w*w;
        w4 = w2*w2;
        a0 = w;
        a1 = 2.0 * w * (0.33333333 + 0.2*w2) * c1;
        a2 = 0.2 * w * (7.0 + 0.14285714 * (33.0*w2+7.4*w4)) * c3;
        a3 = 0.022857143 * (108.0 + 37.177777*w2 + 5.1111111*w4) * c5;
        w = (( lambda * a3 + a2 ) * lambda + a1 ) * lambda + a0;
        w2 = w*w;
        *v = 2.0 * atand(w);
        *r = q * (1+w2) / ( 1.0 + w2*lambda );
        return 0;  /* Near-parabolic orbit */
    }
 
 
ellipse_hyperbola:
 
    if ( lambda > 0.0 )
    {   /* Elliptic orbit: */
        a = q / (1.0-e);            /* Semi-major axis */
        m = KD * tp / sqrt(a*a*a);  /* Mean Anomaly */
        vr( v, r, m, e, a );        /* Solve Kepler's equation, etc */
    }
    else
    {   /* Hyperbolic orbit: */
        a = q / (e-1.0);            /* Semi-major axis */
        n = K * tp / sqrt(a*a*a);   /* "Daily motion" */
        g = n/e;
        adgg = INFINITY;
        do  
        {
            adgg2 = adgg;
            gs = sqrt(g*g+1.0);
            dg = -( e*g - log(g+gs) - n ) / ( e - 1.0/gs );
            g = g + dg;
            adgg = fabs(dg/g);
        } while ( adgg < adgg2  &&  adgg > 1E-5 );
        gs = sqrt(g*g+1.0);
        *v = 2.0 * atand( sqrt( (e+1.0)/(e-1.0) ) * g / (gs+1.0) );
        *r = q * (1.0+e) / ( 1.0 + e*cosd(*v) );
    }
    return 0;
 
} /* vrc */

