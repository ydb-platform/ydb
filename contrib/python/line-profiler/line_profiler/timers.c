#include "Python.h"

/* The following timer code comes from Python 2.5.2's _lsprof.c */

#if !defined(HAVE_LONG_LONG)
#error "This module requires long longs!"
#endif

/*** Selection of a high-precision timer ***/

#ifdef MS_WINDOWS

#include <windows.h>

PY_LONG_LONG
hpTimer(void)
{
        LARGE_INTEGER li;
        QueryPerformanceCounter(&li);
        return li.QuadPart;
}

double
hpTimerUnit(void)
{
        LARGE_INTEGER li;
        if (QueryPerformanceFrequency(&li))
                return 1.0 / li.QuadPart;
        else
                return 0.000001;  /* unlikely */
}

#elif (defined(PYOS_OS2) && defined(PYCC_GCC))

#include <sys/time.h>

PY_LONG_LONG
hpTimer(void)
{
        struct timeval tv;
        PY_LONG_LONG ret;
        gettimeofday(&tv, (struct timezone *)NULL);
        ret = tv.tv_sec;
        ret = ret * 1000000 + tv.tv_usec;
        return ret;
}

double
hpTimerUnit(void)
{
        return 0.000001;
}

#else

#include <sys/resource.h>
#include <sys/times.h>

PY_LONG_LONG
hpTimer(void)
{
        struct timespec ts;
        PY_LONG_LONG ret;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        ret = (PY_LONG_LONG)ts.tv_sec * 1000000000 + (PY_LONG_LONG)ts.tv_nsec;
        return ret;
}

double
hpTimerUnit(void)
{
        return 0.000000001;
}

#endif
