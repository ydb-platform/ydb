/* aux functions so programs besides XEphem can use this library.
 */

#include <stdio.h>
#include <stdlib.h>

#include "astro.h"
#include "preferences.h"

/* default preferences */
static int prefs[NPREFS] = {
    PREF_TOPO, PREF_METRIC, PREF_MDY, PREF_UTCTZ, PREF_HIPREC, PREF_NOMSGBELL,
    PREF_PREFILL, PREF_TIPSON, PREF_CONFIRMON, PREF_SUN
};

/* called anytime we want to know a preference.
 */
int
pref_get(Preferences pref)
{
	return (prefs[pref]);
}

/* call to force a certain preference, return the old setting.
 */
int
pref_set (Preferences pref, int newp)
{
	int prior = pref_get(pref);
	prefs[pref] = newp;
	return (prior);
}

/* given an mjd, return it modified for terrestial dynamical time */
double
mm_mjed (Now *np)
{
	return (mjd + deltat(mjd)/86400.0);
}

