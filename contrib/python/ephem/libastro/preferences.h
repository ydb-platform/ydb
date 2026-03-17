/* global info for the preferences facility.
 * N.B. many of these enums are used as indexes -- don't change without
 *   checking where they are used!
 */
#ifndef _PREFERENCES_H
#define _PREFERENCES_H

typedef enum {
    PREF_EQUATORIAL, PREF_UNITS, PREF_DATE_FORMAT, PREF_ZONE, PREF_DPYPREC,
    PREF_MSG_BELL, PREF_PRE_FILL, PREF_TIPS, PREF_CONFIRM, PREF_WEEKSTART,
    NPREFS
} Preferences;

typedef enum {PREF_GEO, PREF_TOPO} PrefEquatorial;
typedef enum {PREF_ENGLISH, PREF_METRIC} PrefUnits;
typedef enum {PREF_MDY, PREF_YMD, PREF_DMY} PrefDateFormat;
typedef enum {PREF_LOCALTZ, PREF_UTCTZ} PrefStampZone;
typedef enum {PREF_LOPREC, PREF_HIPREC} PrefDpyPrec;
typedef enum {PREF_NOMSGBELL, PREF_MSGBELL} PrefMsgBell;
typedef enum {PREF_PREFILL, PREF_NOPREFILL} PrefPreFill;
typedef enum {PREF_TIPSON, PREF_NOTIPS} PrefTips;
typedef enum {PREF_CONFIRMON, PREF_NOCONFIRM} PrefConfirm;
typedef enum {PREF_SAT, PREF_SUN, PREF_MON} PrefWeekStart;

extern int pref_get (Preferences p);
extern int pref_set (Preferences p, int newp);

#endif /* _PREFERENCES_H */

