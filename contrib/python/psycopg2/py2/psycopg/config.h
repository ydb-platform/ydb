/* config.h - general config and Dprintf macro
 *
 * Copyright (C) 2003-2019 Federico Di Gregorio <fog@debian.org>
 * Copyright (C) 2020 The Psycopg Team
 *
 * This file is part of psycopg.
 *
 * psycopg2 is free software: you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * In addition, as a special exception, the copyright holders give
 * permission to link this program with the OpenSSL library (or with
 * modified versions of OpenSSL that use the same license as OpenSSL),
 * and distribute linked combinations including the two.
 *
 * You must obey the GNU Lesser General Public License in all respects for
 * all of the code used other than OpenSSL.
 *
 * psycopg2 is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 */

#ifndef PSYCOPG_CONFIG_H
#define PSYCOPG_CONFIG_H 1

/* GCC 4.0 and later have support for specifying symbol visibility */
#if __GNUC__ >= 4 && !defined(__MINGW32__)
#  define HIDDEN __attribute__((visibility("hidden")))
#else
#  define HIDDEN
#endif

/* support for getpid() */
#if defined( __GNUC__)
#define CONN_CHECK_PID
#include <sys/types.h>
#include <unistd.h>
#endif
#ifdef _WIN32
/* Windows doesn't seem affected by bug #829: just make it compile. */
#define pid_t int
#endif


/* debug printf-like function */
#ifdef PSYCOPG_DEBUG
extern HIDDEN int psycopg_debug_enabled;
#endif

#if defined( __GNUC__) && !defined(__APPLE__)
#ifdef PSYCOPG_DEBUG
#define Dprintf(fmt, args...) \
    if (!psycopg_debug_enabled) ; else \
        fprintf(stderr, "[%d] " fmt "\n", (int) getpid() , ## args)
#else
#define Dprintf(fmt, args...)
#endif
#else /* !__GNUC__ or __APPLE__ */
#ifdef PSYCOPG_DEBUG
#include <stdarg.h>
#ifdef _WIN32
#include <process.h>
#define getpid _getpid
#endif
static void Dprintf(const char *fmt, ...)
{
    va_list ap;

    if (!psycopg_debug_enabled)
      return;
    printf("[%d] ", (int) getpid());
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    printf("\n");
}
#else
static void Dprintf(const char *fmt, ...) {}
#endif
#endif

/* pthreads work-arounds for mutilated operating systems */
#if defined(_WIN32) || defined(__BEOS__)

#ifdef _WIN32

/* A Python extension should be linked to only one C runtime:  the same one as
 * the Python interpreter itself.  Straightforwardly using the strdup function
 * causes MinGW to implicitly link to the msvcrt.dll, which is not appropriate
 * for any Python version later than 2.3.
 * Microsoft C runtimes for Windows 98 and later make a _strdup function
 * available, which replaces the "normal" strdup.  If we replace all psycopg
 * calls to strdup with calls to _strdup, MinGW no longer implicitly links to
 * the obsolete C runtime. */
#define strdup _strdup

#include <winsock2.h>
#define pthread_mutex_t HANDLE
#define pthread_condvar_t HANDLE
#define pthread_mutex_lock(object) WaitForSingleObject(*(object), INFINITE)
#define pthread_mutex_unlock(object) ReleaseMutex(*(object))
#define pthread_mutex_destroy(ref) (CloseHandle(*(ref)))
/* convert pthread mutex to native mutex */
static int pthread_mutex_init(pthread_mutex_t *mutex, void* fake)
{
  *mutex = CreateMutex(NULL, FALSE, NULL);
  return 0;
}
#endif /* _WIN32 */

#ifdef __BEOS__
#error #include <OS.h>
#define pthread_mutex_t sem_id
#define pthread_mutex_lock(object) acquire_sem(object)
#define pthread_mutex_unlock(object) release_sem(object)
#define pthread_mutex_destroy(ref) delete_sem(*ref)
static int pthread_mutex_init(pthread_mutex_t *mutex, void* fake)
{
        *mutex = create_sem(1, "psycopg_mutex");
        if (*mutex < B_OK)
                return *mutex;
        return 0;
}
#endif /* __BEOS__ */

#else /* pthread is available */
#include <pthread.h>
#endif

/* to work around the fact that Windows does not have a gmtime_r function, or
   a proper gmtime function */
#ifdef _WIN32
#define gmtime_r(t, tm) (gmtime(t)?memcpy((tm), gmtime(t), sizeof(*(tm))):NULL)
#define localtime_r(t, tm) (localtime(t)?memcpy((tm), localtime(t), sizeof(*(tm))):NULL)

/* remove the inline keyword, since it doesn't work unless C++ file */
#define inline

/* Hmmm, MSVC <2015 doesn't have a isnan/isinf function, but has _isnan function */
#if defined (_MSC_VER)
#if !defined(isnan)
#define isnan(x) (_isnan(x))
/* The following line was hacked together from simliar code by Bjorn Reese
 * in libxml2 code */
#define isinf(x) ((_fpclass(x) == _FPCLASS_PINF) ? 1 \
	: ((_fpclass(x) == _FPCLASS_NINF) ? -1 : 0))
#endif
#define strcasecmp(x, y) lstrcmpi(x, y)

#if !defined(__clang__)
typedef __int8              int8_t;
typedef __int16             int16_t;
typedef __int32             int32_t;
typedef __int64             int64_t;
typedef unsigned __int8     uint8_t;
typedef unsigned __int16    uint16_t;
typedef unsigned __int32    uint32_t;
typedef unsigned __int64    uint64_t;
#endif
#endif

#include "win32_support.h"
#endif

/* what's this, we have no round function either? */
#if (defined(_WIN32) && !defined(__GNUC__)) \
    || (defined(sun) || defined(__sun__)) \
        && (defined(__SunOS_5_8) || defined(__SunOS_5_9))

/* round has been added in the standard library with MSVC 2015 */
#if _MSC_VER < 1900
static double round(double num)
{
  return (num >= 0) ? floor(num + 0.5) : ceil(num - 0.5);
}
#endif
#endif

/* resolve missing isinf() function for Solaris */
#if defined (__SVR4) && defined (__sun)
#include <ieeefp.h>
#define isinf(x) (!finite((x)) && (x)==(x))
#endif

/* decorators for the gcc cpychecker plugin */
#if defined(WITH_CPYCHECKER_RETURNS_BORROWED_REF_ATTRIBUTE)
#define BORROWED \
    __attribute__((cpychecker_returns_borrowed_ref))
#else
#define BORROWED
#endif

#if defined(WITH_CPYCHECKER_STEALS_REFERENCE_TO_ARG_ATTRIBUTE)
#define STEALS(n) \
    __attribute__((cpychecker_steals_reference_to_arg(n)))
#else
#define STEALS(n)
#endif

#if defined(WITH_CPYCHECKER_NEGATIVE_RESULT_SETS_EXCEPTION_ATTRIBUTE)
#define RAISES_NEG \
    __attribute__((cpychecker_negative_result_sets_exception))
#else
#define RAISES_NEG
#endif

#if defined(WITH_CPYCHECKER_SETS_EXCEPTION_ATTRIBUTE)
#define RAISES \
    __attribute__((cpychecker_sets_exception))
#else
#define RAISES
#endif

#endif /* !defined(PSYCOPG_CONFIG_H) */
