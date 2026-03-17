#ifndef YTIMING_H
#define YTIMING_H

#include "config.h"

#if defined(_WINDOWS)
#include <windows.h>

#define USE_CLOCK_TYPE_GETTHREADTIMES

#elif defined(_MACH)

#include <mach/mach.h>
#include <mach/thread_info.h>

#define USE_CLOCK_TYPE_THREADINFO

#elif defined(_UNIX)

#include <time.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/resource.h>

/*
    Policy of clock usage on *nix systems is as follows:

    1)  If clock_gettime() is possible, then use it, it has nanosecond
        resolution. It is available in >Linux 2.6.0.
    2)  If get_rusage() is possible use that. >Linux 2.6.26 and Solaris have that.
*/
#if (defined(_POSIX_THREAD_CPUTIME) && defined(LIB_RT_AVAILABLE))
#define USE_CLOCK_TYPE_CLOCKGETTIME
#elif (defined(RUSAGE_THREAD) || defined(RUSAGE_LWP))
#define USE_CLOCK_TYPE_RUSAGE
    #if defined(RUSAGE_LWP)
        #define RUSAGE_WHO RUSAGE_LWP
    #elif defined(RUSAGE_THREAD)
        #define RUSAGE_WHO RUSAGE_THREAD
    #else
        #error "No OS API found for getting cpu time per thread."
    #endif
#endif
#endif

typedef enum
{
    WALL_CLOCK = 0x00,
    CPU_CLOCK = 0x01,
}clock_type_t;

long long tickcount(void);
double tickfactor(void);
int set_timing_clock_type(clock_type_t type);
clock_type_t get_timing_clock_type(void);

#endif
