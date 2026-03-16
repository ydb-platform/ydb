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
 *
 * Created:     H5TSprivate.h
 *
 * Purpose:     Thread-safety abstractions used by the library
 *
 *-------------------------------------------------------------------------
 */
#ifndef H5TSprivate_H_
#define H5TSprivate_H_

#ifdef H5_HAVE_THREADSAFE
/* Include package's public headers */
#include "H5TSdevelop.h"

#ifdef H5_HAVE_WIN_THREADS

/* Library level data structures */

/* Mutexes, Threads, and Attributes */
typedef struct H5TS_mutex_struct {
    CRITICAL_SECTION CriticalSection;
} H5TS_mutex_t;

/* Portability wrappers around Windows Threads types */
typedef CRITICAL_SECTION H5TS_mutex_simple_t;
typedef HANDLE           H5TS_thread_t;
typedef HANDLE           H5TS_attr_t;
typedef DWORD            H5TS_key_t;
typedef INIT_ONCE        H5TS_once_t;

/* Defines */
/* not used on windows side, but need to be defined to something */
#define H5TS_SCOPE_SYSTEM  0
#define H5TS_SCOPE_PROCESS 0
#define H5TS_CALL_CONV     WINAPI

/* Portability function aliases */
#define H5TS_get_thread_local_value(key)        TlsGetValue(key)
#define H5TS_set_thread_local_value(key, value) TlsSetValue(key, value)
#define H5TS_attr_init(attr_ptr)                0
#define H5TS_attr_setscope(attr_ptr, scope)     0
#define H5TS_attr_destroy(attr_ptr)             0
#define H5TS_wait_for_thread(thread)            WaitForSingleObject(thread, INFINITE)
#define H5TS_mutex_init(mutex)                  InitializeCriticalSection(mutex)
#define H5TS_mutex_lock_simple(mutex)           EnterCriticalSection(mutex)
#define H5TS_mutex_unlock_simple(mutex)         LeaveCriticalSection(mutex)

/* Functions called from DllMain */
H5_DLL BOOL CALLBACK H5TS_win32_process_enter(PINIT_ONCE InitOnce, PVOID Parameter, PVOID *lpContex);
H5_DLL void          H5TS_win32_process_exit(void);
H5_DLL herr_t        H5TS_win32_thread_enter(void);
H5_DLL herr_t        H5TS_win32_thread_exit(void);

#define H5TS_thread_id() ((uint64_t)GetCurrentThreadId())

#else /* H5_HAVE_WIN_THREADS */

/* Library level data structures */

/* Mutexes, Threads, and Attributes */
typedef struct H5TS_mutex_struct {
    pthread_t       owner_thread; /* current lock owner */
    pthread_mutex_t atomic_lock;  /* lock for atomicity of new mechanism */
    pthread_cond_t  cond_var;     /* condition variable */
    unsigned int    lock_count;

    pthread_mutex_t atomic_lock2; /* lock for attempt_lock_count */
    unsigned int    attempt_lock_count;
} H5TS_mutex_t;

/* Portability wrappers around pthread types */
typedef pthread_t       H5TS_thread_t;
typedef pthread_attr_t  H5TS_attr_t;
typedef pthread_mutex_t H5TS_mutex_simple_t;
typedef pthread_key_t   H5TS_key_t;
typedef pthread_once_t  H5TS_once_t;

/* Scope Definitions */
#define H5TS_SCOPE_SYSTEM                       PTHREAD_SCOPE_SYSTEM
#define H5TS_SCOPE_PROCESS                      PTHREAD_SCOPE_PROCESS
#define H5TS_CALL_CONV                          /* unused - Windows only */

/* Portability function aliases */
#define H5TS_get_thread_local_value(key)        pthread_getspecific(key)
#define H5TS_set_thread_local_value(key, value) pthread_setspecific(key, value)
#define H5TS_attr_init(attr_ptr)                pthread_attr_init((attr_ptr))
#define H5TS_attr_setscope(attr_ptr, scope)     pthread_attr_setscope(attr_ptr, scope)
#define H5TS_attr_destroy(attr_ptr)             pthread_attr_destroy(attr_ptr)
#define H5TS_wait_for_thread(thread)            pthread_join(thread, NULL)
#define H5TS_mutex_init(mutex)                  pthread_mutex_init(mutex, NULL)
#define H5TS_mutex_lock_simple(mutex)           pthread_mutex_lock(mutex)
#define H5TS_mutex_unlock_simple(mutex)         pthread_mutex_unlock(mutex)

/* Pthread-only routines */
H5_DLL uint64_t H5TS_thread_id(void);
H5_DLL void     H5TS_pthread_first_thread_init(void);

#endif /* H5_HAVE_WIN_THREADS */

/* Library-scope global variables */
extern H5TS_once_t H5TS_first_init_g; /* Library initialization */
extern H5TS_key_t  H5TS_errstk_key_g; /* Error stacks */
#ifdef H5_HAVE_CODESTACK
extern H5TS_key_t H5TS_funcstk_key_g; /* Function stacks */
#endif                                /* H5_HAVE_CODESTACK */
extern H5TS_key_t H5TS_apictx_key_g;  /* API contexts */

/* Library-scope routines */
/* (Only used within H5private.h macros) */
H5_DLL herr_t H5TS_mutex_lock(H5TS_mutex_t *mutex);
H5_DLL herr_t H5TS_mutex_unlock(H5TS_mutex_t *mutex);
H5_DLL herr_t H5TS_cancel_count_inc(void);
H5_DLL herr_t H5TS_cancel_count_dec(void);

/* Testing routines */
H5_DLL H5TS_thread_t H5TS_create_thread(void *(*func)(void *), H5TS_attr_t *attr, void *udata);

#else /* H5_HAVE_THREADSAFE */

#define H5TS_thread_id() ((uint64_t)0)

#endif /* H5_HAVE_THREADSAFE */

#endif /* H5TSprivate_H_ */
