#include "timing.h"

static clock_type_t g_clock_type = CPU_CLOCK;

int set_timing_clock_type(clock_type_t type)
{
    // validate
    if ((type != WALL_CLOCK) && (type != CPU_CLOCK))
    {
        return 0;
    }

    g_clock_type = type;

    return 1;
}

clock_type_t get_timing_clock_type(void)
{
    return g_clock_type;
}

#if !defined(_WINDOWS)
static long long gettimeofday_usec(void)
{
    struct timeval tv;
    long long rc;
#ifdef GETTIMEOFDAY_NO_TZ
    gettimeofday(&tv);
#else
    gettimeofday(&tv, (struct timezone *)NULL);
#endif
    rc = tv.tv_sec;
    rc = rc * 1000000 + tv.tv_usec;
    return rc;
}
#endif

#if defined(_WINDOWS)

long long
tickcount(void)
{
    LARGE_INTEGER li;
    FILETIME ftCreate, ftExit, ftKernel, ftUser;

    if (g_clock_type == CPU_CLOCK) {
        GetThreadTimes(GetCurrentThread(), &ftCreate, &ftExit, &ftKernel, &ftUser);
        li.LowPart = ftKernel.dwLowDateTime+ftUser.dwLowDateTime;
        li.HighPart = ftKernel.dwHighDateTime+ftUser.dwHighDateTime;
    } else if (g_clock_type == WALL_CLOCK) {
        QueryPerformanceCounter(&li);
    }
    return li.QuadPart;
}

double
tickfactor(void)
{
    LARGE_INTEGER li;

    if (g_clock_type == WALL_CLOCK) {
        if (QueryPerformanceFrequency(&li)) {
            return 1.0 / li.QuadPart;
        } else {
            return 0.000001;
        }
    } else if (g_clock_type == CPU_CLOCK) {
        return 0.0000001;
    }

    return 0.0000001; // for suppressing warning: all paths must return a value
}

#elif defined(_MACH)

long long
tickcount(void)
{
    long long rc;

    rc = 0;
    if (g_clock_type == CPU_CLOCK) {
        thread_basic_info_t tinfo_b;
        thread_info_data_t tinfo_d;
        mach_msg_type_number_t tinfo_cnt;

        tinfo_cnt = THREAD_INFO_MAX;
        thread_info(mach_thread_self(), THREAD_BASIC_INFO, (thread_info_t)tinfo_d, &tinfo_cnt);
        tinfo_b = (thread_basic_info_t)tinfo_d;

        if (!(tinfo_b->flags & TH_FLAGS_IDLE))
        {
            rc = (tinfo_b->user_time.seconds + tinfo_b->system_time.seconds);
            rc = (rc * 1000000) + (tinfo_b->user_time.microseconds + tinfo_b->system_time.microseconds);
        }
    } else if (g_clock_type == WALL_CLOCK) {
        rc = gettimeofday_usec();
    }
    return rc;
}

double
tickfactor(void)
{
     return 0.000001;
}

#elif defined(_UNIX)

long long
tickcount(void)
{
    long long rc;

    rc = 0; // suppress "may be uninitialized" warning
    if (g_clock_type == CPU_CLOCK) {
#if defined(USE_CLOCK_TYPE_CLOCKGETTIME)
        struct timespec tp;

        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &tp);
        rc = tp.tv_sec;
        rc = rc * 1000000000 + (tp.tv_nsec);
#elif (defined(USE_CLOCK_TYPE_RUSAGE) && defined(RUSAGE_WHO))
        struct timeval tv;
        struct rusage usage;

        getrusage(RUSAGE_WHO, &usage);
        rc = (usage.ru_utime.tv_sec + usage.ru_stime.tv_sec);
        rc = (rc * 1000000) + (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec);
#endif
    } else if (g_clock_type == WALL_CLOCK) {
        rc = gettimeofday_usec();
    }
    return rc;
}

double
tickfactor(void)
{
    if (g_clock_type == CPU_CLOCK) {
#if defined(USE_CLOCK_TYPE_CLOCKGETTIME)
            return 0.000000001;
#elif defined(USE_CLOCK_TYPE_RUSAGE)
            return 0.000001;
#endif
    } else if (g_clock_type == WALL_CLOCK) {
        return 0.000001;
    }

    return 1.0; // suppress "reached end of non-void function" warning
}

#endif /* *nix */
