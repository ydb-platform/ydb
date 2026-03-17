/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://www.hdfgroup.org/licenses.               *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*-------------------------------------------------------------------------
 * Created:		H5timer.c
 *
 * Purpose:             Internal, platform-independent 'timer' support routines.
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/
#include "H5module.h" /* This source code file is part of the H5 module */

/***********/
/* Headers */
/***********/
#include "H5private.h" /* Generic Functions			*/

/****************/
/* Local Macros */
/****************/

/* Size of a generated time string.
 * Most time strings should be < 20 or so characters (max!) so this should be a
 * safe size.  Dynamically allocating the correct size would be painful.
 */
#define H5TIMER_TIME_STRING_LEN 1536

/* Conversion factors */
#define H5_SEC_PER_DAY  (24.0 * 60.0 * 60.0)
#define H5_SEC_PER_HOUR (60.0 * 60.0)
#define H5_SEC_PER_MIN  (60.0)

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:	H5_bandwidth
 *
 * Purpose:	Prints the bandwidth (bytes per second) in a field 10
 *		characters wide widh four digits of precision like this:
 *
 * 			       NaN	If <=0 seconds
 *			1234. TB/s
 * 			123.4 TB/s
 *			12.34 GB/s
 *			1.234 MB/s
 *			4.000 kB/s
 *			1.000  B/s
 *			0.000  B/s	If NBYTES==0
 *			1.2345e-10	For bandwidth less than 1
 *			6.7893e+94	For exceptionally large values
 *			6.678e+106	For really big values
 *
 * Return:	void
 *
 *-------------------------------------------------------------------------
 */
void
H5_bandwidth(char *buf /*out*/, size_t bufsize, double nbytes, double nseconds)
{
    double bw;

    if (nseconds <= 0.0)
        strcpy(buf, "       NaN");
    else {
        bw = nbytes / nseconds;
        if (H5_DBL_ABS_EQUAL(bw, 0.0))
            strcpy(buf, "0.000  B/s");
        else if (bw < 1.0)
            snprintf(buf, bufsize, "%10.4e", bw);
        else if (bw < (double)H5_KB) {
            snprintf(buf, bufsize, "%05.4f", bw);
            strcpy(buf + 5, "  B/s");
        }
        else if (bw < (double)H5_MB) {
            snprintf(buf, bufsize, "%05.4f", bw / (double)H5_KB);
            strcpy(buf + 5, " kB/s");
        }
        else if (bw < (double)H5_GB) {
            snprintf(buf, bufsize, "%05.4f", bw / (double)H5_MB);
            strcpy(buf + 5, " MB/s");
        }
        else if (bw < (double)H5_TB) {
            snprintf(buf, bufsize, "%05.4f", bw / (double)H5_GB);
            strcpy(buf + 5, " GB/s");
        }
        else if (bw < (double)H5_PB) {
            snprintf(buf, bufsize, "%05.4f", bw / (double)H5_TB);
            strcpy(buf + 5, " TB/s");
        }
        else if (bw < (double)H5_EB) {
            snprintf(buf, bufsize, "%05.4f", bw / (double)H5_PB);
            strcpy(buf + 5, " PB/s");
        }
        else {
            snprintf(buf, bufsize, "%10.4e", bw);
            if (strlen(buf) > 10)
                snprintf(buf, bufsize, "%10.3e", bw);
        } /* end else-if */
    }     /* end else */
} /* end H5_bandwidth() */

/*-------------------------------------------------------------------------
 * Function:	H5_now
 *
 * Purpose:	Retrieves the current time, as seconds after the UNIX epoch.
 *
 * Return:	# of seconds from the epoch (can't fail)
 *
 *-------------------------------------------------------------------------
 */
time_t
H5_now(void)
{
    time_t now; /* Current time */

#ifdef H5_HAVE_GETTIMEOFDAY
    {
        struct timeval now_tv;

        HDgettimeofday(&now_tv, NULL);
        now = now_tv.tv_sec;
    }
#else  /* H5_HAVE_GETTIMEOFDAY */
    now = HDtime(NULL);
#endif /* H5_HAVE_GETTIMEOFDAY */

    return (now);
} /* end H5_now() */

/*-------------------------------------------------------------------------
 * Function:	H5_now_usec
 *
 * Purpose:	Retrieves the current time, as microseconds after the UNIX epoch.
 *
 * Return:	# of microseconds from the epoch (can't fail)
 *
 *-------------------------------------------------------------------------
 */
uint64_t
H5_now_usec(void)
{
    uint64_t now; /* Current time, in microseconds */

#if defined(H5_HAVE_CLOCK_GETTIME)
    {
        struct timespec ts;

        clock_gettime(CLOCK_MONOTONIC, &ts);

        /* Cast all values in this expression to uint64_t to ensure that all intermediate
         * calculations are done in 64 bit, to prevent overflow */
        now = ((uint64_t)ts.tv_sec * ((uint64_t)1000 * (uint64_t)1000)) +
              ((uint64_t)ts.tv_nsec / (uint64_t)1000);
    }
#elif defined(H5_HAVE_GETTIMEOFDAY)
    {
        struct timeval now_tv;

        HDgettimeofday(&now_tv, NULL);

        /* Cast all values in this expression to uint64_t to ensure that all intermediate
         * calculations are done in 64 bit, to prevent overflow */
        now = ((uint64_t)now_tv.tv_sec * ((uint64_t)1000 * (uint64_t)1000)) + (uint64_t)now_tv.tv_usec;
    }
#else  /* H5_HAVE_GETTIMEOFDAY */
    /* Cast all values in this expression to uint64_t to ensure that all intermediate calculations
     * are done in 64 bit, to prevent overflow */
    now       = ((uint64_t)HDtime(NULL) * ((uint64_t)1000 * (uint64_t)1000));
#endif /* H5_HAVE_GETTIMEOFDAY */

    return (now);
} /* end H5_now_usec() */

/*--------------------------------------------------------------------------
 * Function:    H5_get_time
 *
 * Purpose:     Get the current time, as the time of seconds after the UNIX epoch
 *
 * Return:      Success:    A non-negative time value
 *              Failure:    -1.0 (in theory, can't currently fail)
 *
 *--------------------------------------------------------------------------
 */
double
H5_get_time(void)
{
    double ret_value = 0.0;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

#if defined(H5_HAVE_CLOCK_GETTIME)
    {
        struct timespec ts;

        clock_gettime(CLOCK_MONOTONIC, &ts);
        ret_value = (double)ts.tv_sec + ((double)ts.tv_nsec / 1000000000.0);
    }
#elif defined(H5_HAVE_GETTIMEOFDAY)
    {
        struct timeval now_tv;

        HDgettimeofday(&now_tv, NULL);
        ret_value = (double)now_tv.tv_sec + ((double)now_tv.tv_usec / 1000000.0);
    }
#else
    ret_value = (double)HDtime(NULL);
#endif

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5_get_time() */

/*-------------------------------------------------------------------------
 * Function:    H5__timer_get_timevals
 *
 * Purpose:     Internal platform-specific function to get time system,
 *              user and elapsed time values.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5__timer_get_timevals(H5_timevals_t *times /*in,out*/)
{
    /* Sanity check */
    assert(times);

    /* Windows call handles both system/user and elapsed times */
#ifdef H5_HAVE_WIN32_API
    if (H5_get_win32_times(times) < 0) {
        times->elapsed = -1.0;
        times->system  = -1.0;
        times->user    = -1.0;

        return -1;
    } /* end if */
#else /* H5_HAVE_WIN32_API */

    /*************************
     * System and user times *
     *************************/
#if defined(H5_HAVE_GETRUSAGE)
    {
        struct rusage res;

        if (getrusage(RUSAGE_SELF, &res) < 0)
            return -1;
        times->system = (double)res.ru_stime.tv_sec + ((double)res.ru_stime.tv_usec / 1.0E6);
        times->user   = (double)res.ru_utime.tv_sec + ((double)res.ru_utime.tv_usec / 1.0E6);
    }
#else
    /* No suitable way to get system/user times */
    /* This is not an error condition, they just won't be available */
    times->system = -1.0;
    times->user   = -1.0;
#endif

    /****************
     * Elapsed time *
     ****************/

    times->elapsed = H5_get_time();

#endif /* H5_HAVE_WIN32_API */

    return 0;
} /* end H5__timer_get_timevals() */

/*-------------------------------------------------------------------------
 * Function:    H5_timer_init
 *
 * Purpose:     Initialize a platform-independent timer.
 *
 *              Timer usage is as follows:
 *
 *              1) Call H5_timer_init(), passing in a timer struct, to set
 *                 up the timer.
 *
 *              2) Wrap any code you'd like to time with calls to
 *                 H5_timer_start/stop().  For accurate timing, place these
 *                 calls as close to the code of interest as possible.  You
 *                 can call start/stop multiple times on the same timer -
 *                 when you do this, H5_timer_get_times() will return time
 *                 values for the current/last session and
 *                 H5_timer_get_total_times() will return the summed times
 *                 of all sessions (see #3 and #4, below).
 *
 *              3) Use H5_timer_get_times() to get the current system, user
 *                 and elapsed times from a running timer.  If called on a
 *                 stopped timer, this will return the time recorded at the
 *                 stop point.
 *
 *              4) Call H5_timer_get_total_times() to get the total system,
 *                 user and elapsed times recorded across multiple start/stop
 *                 sessions.  If called on a running timer, it will return the
 *                 time recorded up to that point.  On a stopped timer, it
 *                 will return the time recorded at the stop point.
 *
 *              NOTE: Obtaining a time point is not free!  Keep in mind that
 *                    the time functions make system calls and can have
 *                    non-trivial overhead.  If you call one of the get_time
 *                    functions on a running timer, that overhead will be
 *                    added to the reported times.
 *
 *              5) All times recorded will be in seconds.  These can be
 *                 converted into human-readable strings with the
 *                 H5_timer_get_time_string() function.
 *
 *              6) A timer can be reset using by calling H5_timer_init() on
 *                 it.  This will set its state to 'stopped' and reset all
 *                 accumulated times to zero.
 *
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_timer_init(H5_timer_t *timer /*in,out*/)
{
    /* Sanity check */
    assert(timer);

    /* Initialize everything */
    memset(timer, 0, sizeof(H5_timer_t));

    return 0;
} /* end H5_timer_init() */

/*-------------------------------------------------------------------------
 * Function:    H5_timer_start
 *
 * Purpose:     Start tracking time in a platform-independent timer.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_timer_start(H5_timer_t *timer /*in,out*/)
{
    /* Sanity check */
    assert(timer);

    /* Start the timer
     * This sets the "initial" times to the system-defined start times.
     */
    if (H5__timer_get_timevals(&(timer->initial)) < 0)
        return -1;

    timer->is_running = true;

    return 0;
} /* end H5_timer_start() */

/*-------------------------------------------------------------------------
 * Function:    H5_timer_stop
 *
 * Purpose:     Stop tracking time in a platform-independent timer.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_timer_stop(H5_timer_t *timer /*in,out*/)
{
    /* Sanity check */
    assert(timer);

    /* Stop the timer */
    if (H5__timer_get_timevals(&(timer->final_interval)) < 0)
        return -1;

    /* The "final" times are stored as intervals (final - initial)
     * for more useful reporting to the user.
     */
    timer->final_interval.elapsed = timer->final_interval.elapsed - timer->initial.elapsed;
    timer->final_interval.system  = timer->final_interval.system - timer->initial.system;
    timer->final_interval.user    = timer->final_interval.user - timer->initial.user;

    /* Add the intervals to the elapsed time */
    timer->total.elapsed += timer->final_interval.elapsed;
    timer->total.system += timer->final_interval.system;
    timer->total.user += timer->final_interval.user;

    timer->is_running = false;

    return 0;
} /* end H5_timer_stop() */

/*-------------------------------------------------------------------------
 * Function:    H5_timer_get_times
 *
 * Purpose:     Get the system, user and elapsed times from a timer.  These
 *              are the times since the timer was last started and will be
 *              0.0 in a timer that has not been started since it was
 *              initialized.
 *
 *              This function can be called either before or after
 *              H5_timer_stop() has been called.  If it is called before the
 *              stop function, the timer will continue to run.
 *
 *              The system and user times will be -1.0 if those times cannot
 *              be computed on a particular platform.  The elapsed time will
 *              always be present.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_timer_get_times(H5_timer_t timer, H5_timevals_t *times /*in,out*/)
{
    /* Sanity check */
    assert(times);

    if (timer.is_running) {
        H5_timevals_t now;

        /* Get the current times and report the current intervals without
         * stopping the timer.
         */
        if (H5__timer_get_timevals(&now) < 0)
            return -1;

        times->elapsed = now.elapsed - timer.initial.elapsed;
        times->system  = now.system - timer.initial.system;
        times->user    = now.user - timer.initial.user;
    } /* end if */
    else {
        times->elapsed = timer.final_interval.elapsed;
        times->system  = timer.final_interval.system;
        times->user    = timer.final_interval.user;
    } /* end else */

    return 0;
} /* end H5_timer_get_times() */

/*-------------------------------------------------------------------------
 * Function:    H5_timer_get_total_times
 *
 * Purpose:     Get the TOTAL system, user and elapsed times recorded by
 *              the timer since its initialization.  This is the sum of all
 *              times recorded while the timer was running.
 *
 *              These will be 0.0 in a timer that has not been started
 *              since it was initialized.  Calling H5_timer_init() on a
 *              timer will reset these values to 0.0.
 *
 *              This function can be called either before or after
 *              H5_timer_stop() has been called.  If it is called before the
 *              stop function, the timer will continue to run.
 *
 *              The system and user times will be -1.0 if those times cannot
 *              be computed on a particular platform.  The elapsed time will
 *              always be present.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_timer_get_total_times(H5_timer_t timer, H5_timevals_t *times /*in,out*/)
{
    /* Sanity check */
    assert(times);

    if (timer.is_running) {
        H5_timevals_t now;

        /* Get the current times and report the current totals without
         * stopping the timer.
         */
        if (H5__timer_get_timevals(&now) < 0)
            return -1;

        times->elapsed = timer.total.elapsed + (now.elapsed - timer.initial.elapsed);
        times->system  = timer.total.system + (now.system - timer.initial.system);
        times->user    = timer.total.user + (now.user - timer.initial.user);
    } /* end if */
    else {
        times->elapsed = timer.total.elapsed;
        times->system  = timer.total.system;
        times->user    = timer.total.user;
    } /* end else */

    return 0;
} /* end H5_timer_get_total_times() */

/*-------------------------------------------------------------------------
 * Function:    H5_timer_get_time_string
 *
 * Purpose:     Converts a time (in seconds) into a human-readable string
 *              suitable for log messages.
 *
 * Return:      Success:  The time string.
 *
 *                        The general format of the time string is:
 *
 *                        "N/A"                 time < 0 (invalid time)
 *                        "%.f ns"              time < 1 microsecond
 *                        "%.1f us"             time < 1 millisecond
 *                        "%.1f ms"             time < 1 second
 *                        "%.2f s"              time < 1 minute
 *                        "%.f m %.f s"         time < 1 hour
 *                        "%.f h %.f m %.f s"   longer times
 *
 *              Failure:  NULL
 *
 *-------------------------------------------------------------------------
 */
char *
H5_timer_get_time_string(double seconds)
{
    char *s; /* output string */

    /* Used when the time is greater than 59 seconds */
    double days          = 0.0;
    double hours         = 0.0;
    double minutes       = 0.0;
    double remainder_sec = 0.0;

    /* Extract larger time units from count of seconds */
    if (seconds > 60.0) {
        /* Set initial # of seconds */
        remainder_sec = seconds;

        /* Extract days */
        days = floor(remainder_sec / H5_SEC_PER_DAY);
        remainder_sec -= (days * H5_SEC_PER_DAY);

        /* Extract hours */
        hours = floor(remainder_sec / H5_SEC_PER_HOUR);
        remainder_sec -= (hours * H5_SEC_PER_HOUR);

        /* Extract minutes */
        minutes = floor(remainder_sec / H5_SEC_PER_MIN);
        remainder_sec -= (minutes * H5_SEC_PER_MIN);

        /* The # of seconds left is in remainder_sec */
    } /* end if */

    /* Allocate */
    if (NULL == (s = (char *)calloc(H5TIMER_TIME_STRING_LEN, sizeof(char))))
        return NULL;

    /* Do we need a format string? Some people might like a certain
     * number of milliseconds or s before spilling to the next highest
     * time unit.  Perhaps this could be passed as an integer.
     * (name? round_up_size? ?)
     */
    if (seconds < 0.0)
        snprintf(s, H5TIMER_TIME_STRING_LEN, "N/A");
    else if (H5_DBL_ABS_EQUAL(0.0, seconds))
        snprintf(s, H5TIMER_TIME_STRING_LEN, "0.0 s");
    else if (seconds < 1.0E-6)
        /* t < 1 us, Print time in ns */
        snprintf(s, H5TIMER_TIME_STRING_LEN, "%.f ns", seconds * 1.0E9);
    else if (seconds < 1.0E-3)
        /* t < 1 ms, Print time in us */
        snprintf(s, H5TIMER_TIME_STRING_LEN, "%.1f us", seconds * 1.0E6);
    else if (seconds < 1.0)
        /* t < 1 s, Print time in ms */
        snprintf(s, H5TIMER_TIME_STRING_LEN, "%.1f ms", seconds * 1.0E3);
    else if (seconds < H5_SEC_PER_MIN)
        /* t < 1 m, Print time in s */
        snprintf(s, H5TIMER_TIME_STRING_LEN, "%.2f s", seconds);
    else if (seconds < H5_SEC_PER_HOUR)
        /* t < 1 h, Print time in m and s */
        snprintf(s, H5TIMER_TIME_STRING_LEN, "%.f m %.f s", minutes, remainder_sec);
    else if (seconds < H5_SEC_PER_DAY)
        /* t < 1 d, Print time in h, m and s */
        snprintf(s, H5TIMER_TIME_STRING_LEN, "%.f h %.f m %.f s", hours, minutes, remainder_sec);
    else
        /* Print time in d, h, m and s */
        snprintf(s, H5TIMER_TIME_STRING_LEN, "%.f d %.f h %.f m %.f s", days, hours, minutes, remainder_sec);

    return s;
} /* end H5_timer_get_time_string() */
