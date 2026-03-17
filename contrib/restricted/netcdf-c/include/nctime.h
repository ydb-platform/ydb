/*********************************************************************
 *   Copyright 2018, University Corporation for Atmospheric Research
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *   $Id: nctime.h,v 1.6 2010/03/18 19:24:26 russ Exp $
 *********************************************************************/

#ifndef _NCTIME_H
#define _NCTIME_H

#define CU_FATAL 1   /* Exit immediately on fatal error */
#define CU_VERBOSE 2 /* Report errors */

struct bounds_node{
    int ncid;	  /* group (or file) in which variable with associated
		   * bounds variable resides */
    int varid; /* has "bounds" attribute naming its bounds variable */
    char *bounds_name; /* the named variable, which stores bounds for varid */
    struct bounds_node *next; /* next node on list or NULL ifn last list node */
};

typedef struct bounds_node bounds_node_t;

/*
 * This code was extracted with permission from the CDMS time
 * conversion and arithmetic routines developed by Bob Drach, Lawrence
 * Livermore National Laboratory as part of the cdtime library.
 * Changes and additions were made to support the "-t" option of the
 * netCDF ncdump utility.
 *
 * For the complete time conversion and climate calendar facilities of
 * the CDMS library, get the original sources from LLNL.
 */

#define CD_MAX_RELUNITS 64	/* Max characters in relative units */
#define CD_MAX_CHARTIME 48	/* Max characters in character time */
#define CD_NULL_DAY 1		/* Null day value */
#define CD_NULL_HOUR 0.0	/* Null hour value */
#define CD_NULL_ID 0		/* Reserved ID */
#define CD_NULL_MONTH 1		/* Null month value */
#define CD_NULL_YEAR 0		/* Null year value, component time */

/* Why do we have same enum defined twice? */

typedef enum CdTimeUnit {
        CdBadTimeUnit = 0,
	CdMinute = 1,
	CdHour = 2,
	CdDay = 3,
	CdWeek = 4,		/* Always = 7 days */
	CdMonth = 5,
	CdSeason = 6,		/* Always = 3 months */
	CdYear = 7,
	CdSecond = 8
} CdTimeUnit;

typedef enum cdUnitTime {
        cdBadUnit = CdBadTimeUnit,
	cdMinute = CdMinute,
	cdHour = CdHour,
	cdDay = CdDay,
	cdWeek = CdWeek,	/* Always = 7 days */
	cdMonth = CdMonth,
	cdSeason = CdSeason,	/* Always = 3 months */
	cdYear = CdYear,
	cdSecond = CdSecond,
	cdFraction		/* Fractional part of absolute time */
} cdUnitTime;

#define CdChronCal    0x1
#define CdClimCal     0x0
#define CdBaseRel    0x00
#define CdBase1970   0x10
#define CdHasLeap   0x100
#define CdNoLeap    0x000
#define Cd366      0x2000
#define Cd365      0x1000
#define Cd360      0x0000
#define CdJulianType 0x10000

typedef enum CdTimeType {
	CdChron       = ( CdChronCal | CdBase1970 | CdHasLeap | Cd365),	/* 4369 */
	CdJulianCal   = ( CdChronCal | CdBase1970 | CdHasLeap | Cd365 | CdJulianType),
	CdChronNoLeap = ( CdChronCal | CdBase1970 | CdNoLeap  | Cd365),	/* 4113 */
	CdChron360    = ( CdChronCal | CdBase1970 | CdNoLeap  | Cd360),	/*   17 */
	CdRel         = ( CdChronCal | CdBaseRel  | CdHasLeap | Cd365),	/* 4353 */
	CdRelNoLeap   = ( CdChronCal | CdBaseRel  | CdNoLeap  | Cd365),	/* 4097 */
	CdClim        = ( CdClimCal  | CdBaseRel  | CdNoLeap  | Cd365), /* 4096 */
	CdClimLeap    = ( CdClimCal  | CdBaseRel  | CdHasLeap | Cd365),
	CdClim360     = ( CdClimCal  | CdBaseRel  | CdNoLeap  | Cd365),
	CdChron366    = ( CdChronCal | CdBase1970 | CdNoLeap  | Cd366)
}  CdTimeType;

typedef struct {
	long    		year;	     /* e.g., 1979 */
	short			month;	     /* e.g., CdDec */
	short			day;	     /* e.g., 30 */
	double			hour;	     /* hour and fractional hour */
	long			baseYear;    /* base year for relative, 1970 for CdChron */
	CdTimeType		timeType;    /* e.g., CdChron */
} CdTime;

#define cdStandardCal   0x11
#define cdClimCal        0x0
#define cdHasLeap      0x100
#define cdHasNoLeap    0x000
#define cd366Days      0x2000
#define cd365Days     0x1000
#define cd360Days     0x0000
#define cdJulianCal  0x10000
#define cdMixedCal   0x20000

typedef enum cdCalenType {
	cdStandard    = ( cdStandardCal | cdHasLeap   | cd365Days),
	cdJulian      = ( cdStandardCal | cdHasLeap   | cd365Days | cdJulianCal),
	cdNoLeap      = ( cdStandardCal | cdHasNoLeap | cd365Days),
	cd360         = ( cdStandardCal | cdHasNoLeap | cd360Days),
	cd366         = ( cdStandardCal | cdHasNoLeap | cd366Days),
	cdClim        = ( cdClimCal     | cdHasNoLeap | cd365Days),
	cdClimLeap    = ( cdClimCal     | cdHasLeap   | cd365Days),
	cdClim360     = ( cdClimCal     | cdHasNoLeap | cd360Days),
	cdMixed       = ( cdStandardCal | cdHasLeap   | cd365Days | cdMixedCal)
}  cdCalenType;

/* Component time */
typedef struct {
	long 		year;		     /* Year */
	short 		month;		     /* Numerical month (1..12) */
	short 		day;		     /* Day of month (1..31) */
	double 		hour;		     /* Hour and fractional hours */
} cdCompTime;

typedef struct {
	long   			count;	     /* units count  */
	CdTimeUnit		units;	     /* time interval units */
} CdDeltaTime;

typedef struct timeinfo_t {
    cdCalenType calendar;
    cdUnitTime unit;
    char *units;
    cdCompTime origin;
} timeinfo_t;



#if defined(DLL_NETCDF) /* Defined when library is a DLL */
# if defined(DLL_EXPORT) /* define when building the library. */
#   define MSC_NCTIME_EXTRA __declspec(dllexport)
# else
#   define MSC_NCTIME_EXTRA __declspec(dllimport)
# endif

MSC_NCTIME_EXTRA extern void cdRel2Iso(cdCalenType timetype, char* relunits, int separator, double reltime, char* chartime, size_t chartime_size);
MSC_NCTIME_EXTRA extern void cdChar2Comp(cdCalenType timetype, char* chartime, cdCompTime* comptime);
MSC_NCTIME_EXTRA extern void Cdh2e(CdTime *htime, double *etime);
MSC_NCTIME_EXTRA extern void Cde2h(double etime, CdTimeType timeType, long baseYear, CdTime *htime);
MSC_NCTIME_EXTRA extern int cdParseRelunits(cdCalenType timetype, char* relunits, cdUnitTime* unit, cdCompTime* base_comptime);
MSC_NCTIME_EXTRA extern int cdSetErrOpts(int opts);
#else
extern void cdRel2Iso(cdCalenType timetype, char* relunits, int separator, double reltime, char* chartime, size_t chartime_size);
extern void cdChar2Comp(cdCalenType timetype, char* chartime, cdCompTime* comptime);
extern void Cdh2e(CdTime *htime, double *etime);
extern void Cde2h(double etime, CdTimeType timeType, long baseYear, CdTime *htime);
extern int cdParseRelunits(cdCalenType timetype, char* relunits, cdUnitTime* unit, cdCompTime* base_comptime);
extern int cdSetErrOpts(int opts);
#endif /* DLL Considerations. */


#endif /* ifdef */
