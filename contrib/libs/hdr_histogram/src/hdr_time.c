/**
* hdr_time.h
* Written by Michael Barker and Philip Orwig and released to the public domain,
* as explained at http://creativecommons.org/publicdomain/zero/1.0/
*/

#include "hdr_time.h"

#if defined(_WIN32) || defined(_WIN64)

#if !defined(WIN32_LEAN_AND_MEAN)
#define WIN32_LEAN_AND_MEAN
#endif

#include <windows.h>

static int s_clockPeriodSet = 0;
static double s_clockPeriod = 1.0;

void hdr_gettime(hdr_timespec* t)
{
    LARGE_INTEGER num;
    /* if this is distasteful, we can add in an hdr_time_init() */
    if (!s_clockPeriodSet)
    {
        QueryPerformanceFrequency(&num);
        s_clockPeriod = 1.0 / (double) num.QuadPart;
        s_clockPeriodSet = 1;
    }

    QueryPerformanceCounter(&num);
    double seconds = num.QuadPart * s_clockPeriod;
    double integral;
    double remainder = modf(seconds, &integral);

    t->tv_sec  = (long) integral;
    t->tv_nsec = (long) (remainder * 1000000000);
}

#elif defined(__APPLE__)

#include <mach/clock.h>
#include <mach/mach.h>


void hdr_gettime(hdr_timespec* ts)
{
    clock_serv_t cclock;
    mach_timespec_t mts;
    host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);
    clock_get_time(cclock, &mts);
    mach_port_deallocate(mach_task_self(), cclock);
    ts->tv_sec = mts.tv_sec;
    ts->tv_nsec = mts.tv_nsec;
}

#elif defined(__linux__) || defined(__CYGWIN__)


void hdr_gettime(hdr_timespec* t)
{
    clock_gettime(CLOCK_MONOTONIC, (struct timespec*)t);
}

#else

#warning "Platform not supported\n"

#endif

