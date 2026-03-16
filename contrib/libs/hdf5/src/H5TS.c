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

/*
 * Purpose: This file contains the framework for ensuring that the global
 *          library lock is held when an API routine is called.  This
 *          framework works in concert with the FUNC_ENTER_API / FUNC_LEAVE_API
 *          macros defined in H5private.h.
 *
 * Note:    Because this threadsafety framework operates outside the library,
 *          it does not use the error stack and only uses the "namecheck only"
 *          FUNC_ENTER_* / FUNC_LEAVE_* macros.
 */

/****************/
/* Module Setup */
/****************/

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5MMprivate.h" /* Memory management                        */

#ifdef H5_HAVE_THREADSAFE

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/* Cancellability structure */
typedef struct H5TS_cancel_struct {
    int          previous_state;
    unsigned int cancel_count;
} H5TS_cancel_t;

/* Function pointer typedef for thread callback function */
typedef void *(*H5TS_thread_cb_t)(void *);

/********************/
/* Local Prototypes */
/********************/
static void   H5TS__key_destructor(void *key_val);
static herr_t H5TS__mutex_acquire(H5TS_mutex_t *mutex, unsigned int lock_count, bool *acquired);
static herr_t H5TS__mutex_unlock(H5TS_mutex_t *mutex, unsigned int *lock_count);

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/* Global variable definitions */
#ifdef H5_HAVE_WIN_THREADS
H5TS_once_t H5TS_first_init_g;
#else
H5TS_once_t       H5TS_first_init_g = PTHREAD_ONCE_INIT;
#endif

/* Thread-local keys, used by other interfaces */
/* Error stack */
#ifdef H5_HAVE_WIN_THREADS
H5TS_key_t H5TS_errstk_key_g = TLS_OUT_OF_INDEXES;
#else
H5TS_key_t        H5TS_errstk_key_g;
#endif

#ifdef H5_HAVE_CODESTACK
/* Function stack */
#ifdef H5_HAVE_WIN_THREADS
H5TS_key_t H5TS_funcstk_key_g = TLS_OUT_OF_INDEXES;
#else
H5TS_key_t H5TS_funcstk_key_g;
#endif
#endif /* H5_HAVE_CODESTACK */

/* API context */
#ifdef H5_HAVE_WIN_THREADS
H5TS_key_t H5TS_apictx_key_g = TLS_OUT_OF_INDEXES;
#else
H5TS_key_t        H5TS_apictx_key_g;
#endif

/*******************/
/* Local Variables */
/*******************/

/* Thread-local keys, used in this module */
/* Thread cancellation state */
#ifdef H5_HAVE_WIN_THREADS
static H5TS_key_t H5TS_cancel_key_s = TLS_OUT_OF_INDEXES;
#else
static H5TS_key_t H5TS_cancel_key_s;
#endif

#ifndef H5_HAVE_WIN_THREADS

/* An H5TS_tid_t is a record of a thread identifier that is
 * available for reuse.
 */
struct _tid;
typedef struct _tid H5TS_tid_t;

struct _tid {
    H5TS_tid_t *next;
    uint64_t    id;
};

/* Pointer to first free thread ID record or NULL. */
static H5TS_tid_t *H5TS_tid_next_free = NULL;
static uint64_t    H5TS_tid_next_id   = 0;

/* Mutual exclusion for access to H5TS_tid_next_free and H5TS_tid_next_id. */
static pthread_mutex_t H5TS_tid_mtx;

/* Key for thread-local storage of the thread ID. */
static H5TS_key_t H5TS_tid_key;

#endif /* H5_HAVE_WIN_THREADS */

/*--------------------------------------------------------------------------
 * NAME
 *    H5TS__key_destructor
 *
 * USAGE
 *    H5TS__key_destructor()
 *
 * RETURNS
 *   None
 *
 * DESCRIPTION
 *   Frees the memory for a key.  Called by each thread as it exits.
 *   Currently all the thread-specific information for all keys are simple
 *   structures allocated with malloc, so we can free them all uniformly.
 *--------------------------------------------------------------------------
 */
static void
H5TS__key_destructor(void *key_val)
{
    FUNC_ENTER_PACKAGE_NAMECHECK_ONLY

    /* Use free() here instead of H5MM_xfree(), to avoid calling the H5CS routines */
    if (key_val != NULL)
        free(key_val);

    FUNC_LEAVE_NOAPI_VOID_NAMECHECK_ONLY
} /* end H5TS__key_destructor() */

#ifndef H5_HAVE_WIN_THREADS

/*--------------------------------------------------------------------------
 * NAME
 *    H5TS_tid_destructor
 *
 * USAGE
 *    H5TS_tid_destructor()
 *
 * RETURNS
 *
 * DESCRIPTION
 *   When a thread shuts down, put its ID record on the free list.
 *
 *--------------------------------------------------------------------------
 */
static void
H5TS_tid_destructor(void *_v)
{
    H5TS_tid_t *tid = _v;

    if (tid == NULL)
        return;

    /* TBD use an atomic CAS */
    pthread_mutex_lock(&H5TS_tid_mtx);
    tid->next          = H5TS_tid_next_free;
    H5TS_tid_next_free = tid;
    pthread_mutex_unlock(&H5TS_tid_mtx);
}

/*--------------------------------------------------------------------------
 * NAME
 *    H5TS_tid_init
 *
 * USAGE
 *    H5TS_tid_init()
 *
 * RETURNS
 *
 * DESCRIPTION
 *   Initialize for integer thread identifiers.
 *
 *--------------------------------------------------------------------------
 */
static void
H5TS_tid_init(void)
{
    pthread_mutex_init(&H5TS_tid_mtx, NULL);
    pthread_key_create(&H5TS_tid_key, H5TS_tid_destructor);
}

/*--------------------------------------------------------------------------
 * NAME
 *    H5TS_thread_id
 *
 * USAGE
 *    uint64_t id = H5TS_thread_id()
 *
 * RETURNS
 *   Return an integer identifier, ID, for the current thread.
 *
 * DESCRIPTION
 *   The ID satisfies the following properties:
 *
 *   1 1 <= ID <= UINT64_MAX
 *   2 ID is constant over the thread's lifetime.
 *   3 No two threads share an ID during their lifetimes.
 *   4 A thread's ID is available for reuse as soon as it is joined.
 *
 *   ID 0 is reserved.  H5TS_thread_id() returns 0 if the library was not
 *   built with thread safety or if an error prevents it from assigning an
 *   ID.
 *
 *--------------------------------------------------------------------------
 */
uint64_t
H5TS_thread_id(void)
{
    H5TS_tid_t *tid = pthread_getspecific(H5TS_tid_key);
    H5TS_tid_t  proto_tid;

    /* An ID is already assigned. */
    if (tid != NULL)
        return tid->id;

    /* An ID is *not* already assigned: reuse an ID that's on the
     * free list, or else generate a new ID.
     *
     * Allocating memory while holding a mutex is bad form, so
     * point `tid` at `proto_tid` if we need to allocate some
     * memory.
     */
    pthread_mutex_lock(&H5TS_tid_mtx);
    if ((tid = H5TS_tid_next_free) != NULL)
        H5TS_tid_next_free = tid->next;
    else if (H5TS_tid_next_id != UINT64_MAX) {
        tid     = &proto_tid;
        tid->id = ++H5TS_tid_next_id;
    }
    pthread_mutex_unlock(&H5TS_tid_mtx);

    /* If a prototype ID record was established, copy it to the heap. */
    if (tid == &proto_tid)
        if ((tid = malloc(sizeof(*tid))) != NULL)
            *tid = proto_tid;

    if (tid == NULL)
        return 0;

    /* Finish initializing the ID record and set a thread-local pointer
     * to it.
     */
    tid->next = NULL;
    if (pthread_setspecific(H5TS_tid_key, tid) != 0) {
        H5TS_tid_destructor(tid);
        return 0;
    }

    return tid->id;
}

/*--------------------------------------------------------------------------
 * NAME
 *    H5TS_pthread_first_thread_init
 *
 * USAGE
 *    H5TS_pthread_first_thread_init()
 *
 * RETURNS
 *
 * DESCRIPTION
 *   Initialization of global API lock, keys for per-thread error stacks and
 *   cancallability information. Called by the first thread that enters the
 *   library.
 *
 * PROGRAMMER: Chee Wai LEE
 *             May 2, 2000
 *
 *--------------------------------------------------------------------------
 */
void
H5TS_pthread_first_thread_init(void)
{
    H5_g.H5_libinit_g = false; /* Library hasn't been initialized */
    H5_g.H5_libterm_g = false; /* Library isn't being shutdown */

    FUNC_ENTER_NOAPI_NAMECHECK_ONLY

#ifdef H5_HAVE_WIN32_API
#ifdef PTW32_STATIC_LIB
    pthread_win32_process_attach_np();
#endif
#endif

    /* initialize global API mutex lock */
    pthread_mutex_init(&H5_g.init_lock.atomic_lock, NULL);
    pthread_cond_init(&H5_g.init_lock.cond_var, NULL);
    H5_g.init_lock.lock_count = 0;

    pthread_mutex_init(&H5_g.init_lock.atomic_lock2, NULL);
    H5_g.init_lock.attempt_lock_count = 0;

    /* Initialize integer thread identifiers. */
    H5TS_tid_init();

    /* initialize key for thread-specific error stacks */
    pthread_key_create(&H5TS_errstk_key_g, H5TS__key_destructor);

#ifdef H5_HAVE_CODESTACK
    /* initialize key for thread-specific function stacks */
    pthread_key_create(&H5TS_funcstk_key_g, H5TS__key_destructor);
#endif /* H5_HAVE_CODESTACK */

    /* initialize key for thread-specific API contexts */
    pthread_key_create(&H5TS_apictx_key_g, H5TS__key_destructor);

    /* initialize key for thread cancellability mechanism */
    pthread_key_create(&H5TS_cancel_key_s, H5TS__key_destructor);

    FUNC_LEAVE_NOAPI_VOID_NAMECHECK_ONLY
} /* end H5TS_pthread_first_thread_init() */
#endif /* H5_HAVE_WIN_THREADS */

/*--------------------------------------------------------------------------
 * Function:    H5TS__mutex_acquire
 *
 * Purpose:     Attempts to acquire a mutex lock, without blocking
 *
 * Note:        On success, the 'acquired' flag indicates if the HDF5 library
 *              global lock was acquired.
 *
 * Note:        The Windows threads code is very likely bogus.
 *
 * Return:      Non-negative on success / Negative on failure
 *--------------------------------------------------------------------------
 */
static herr_t
H5TS__mutex_acquire(H5TS_mutex_t *mutex, unsigned int lock_count, bool *acquired)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE_NAMECHECK_ONLY

#ifdef H5_HAVE_WIN_THREADS
    EnterCriticalSection(&mutex->CriticalSection);
    *acquired = true;
#else  /* H5_HAVE_WIN_THREADS */
    /* Attempt to acquire the mutex lock */
    if (0 == pthread_mutex_lock(&mutex->atomic_lock)) {
        pthread_t my_thread_id = pthread_self();

        /* Check if locked already */
        if (mutex->lock_count) {
            /* Check for this thread already owning the lock */
            if (pthread_equal(my_thread_id, mutex->owner_thread)) {
                /* Already owned by self - increment count */
                mutex->lock_count += lock_count;
                *acquired = true;
            }
            else
                *acquired = false;
        }
        else {
            /* Take ownership of the mutex */
            mutex->owner_thread = my_thread_id;
            mutex->lock_count   = lock_count;
            *acquired           = true;
        }

        if (0 != pthread_mutex_unlock(&mutex->atomic_lock))
            ret_value = -1;
    }
    else
        ret_value = -1;
#endif /* H5_HAVE_WIN_THREADS */

    FUNC_LEAVE_NOAPI_NAMECHECK_ONLY(ret_value)
} /* end H5TS__mutex_acquire() */

/*--------------------------------------------------------------------------
 * Function:    H5TSmutex_acquire
 *
 * Purpose:     Attempts to acquire the HDF5 library global lock
 *
 * Note:        On success, the 'acquired' flag indicates if the HDF5 library
 *              global lock was acquired.
 *
 * Return:      Non-negative on success / Negative on failure
 *--------------------------------------------------------------------------
 */
herr_t
H5TSmutex_acquire(unsigned int lock_count, bool *acquired){
    FUNC_ENTER_API_NAMECHECK_ONLY

        FUNC_LEAVE_API_NAMECHECK_ONLY(H5TS__mutex_acquire(&H5_g.init_lock, lock_count, acquired))}
/* end H5TSmutex_acquire() */

/*--------------------------------------------------------------------------
 * NAME
 *    H5TS_mutex_lock
 *
 * USAGE
 *    H5TS_mutex_lock(&mutex_var)
 *
 * RETURNS
 *    Non-negative on success / Negative on failure
 *
 * DESCRIPTION
 *    Recursive lock semantics for HDF5 (locking) -
 *    Multiple acquisition of a lock by a thread is permitted with a
 *    corresponding unlock operation required.
 *
 * PROGRAMMER: Chee Wai LEE
 *             May 2, 2000
 *
 *--------------------------------------------------------------------------
 */
herr_t H5TS_mutex_lock(H5TS_mutex_t *mutex)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NAMECHECK_ONLY

#ifdef H5_HAVE_WIN_THREADS
    EnterCriticalSection(&mutex->CriticalSection);
#else  /* H5_HAVE_WIN_THREADS */
    /* Acquire the "attempt" lock, increment the attempt lock count, release the lock */
    ret_value = pthread_mutex_lock(&mutex->atomic_lock2);
    if (ret_value)
        HGOTO_DONE(ret_value);
    mutex->attempt_lock_count++;
    ret_value = pthread_mutex_unlock(&mutex->atomic_lock2);
    if (ret_value)
        HGOTO_DONE(ret_value);

    /* Acquire the library lock */
    ret_value = pthread_mutex_lock(&mutex->atomic_lock);
    if (ret_value)
        HGOTO_DONE(ret_value);

    /* Check if this thread already owns the lock */
    if (mutex->lock_count && pthread_equal(pthread_self(), mutex->owner_thread))
        /* already owned by self - increment count */
        mutex->lock_count++;
    else {
        /* Wait until the lock is released by current owner thread */
        while (mutex->lock_count)
            pthread_cond_wait(&mutex->cond_var, &mutex->atomic_lock);

        /* After we've received the signal, take ownership of the mutex */
        mutex->owner_thread = pthread_self();
        mutex->lock_count   = 1;
    }

    /* Release the library lock */
    ret_value = pthread_mutex_unlock(&mutex->atomic_lock);

done:
#endif /* H5_HAVE_WIN_THREADS */
    FUNC_LEAVE_NOAPI_NAMECHECK_ONLY(ret_value)
} /* end H5TS_mutex_lock() */

/*--------------------------------------------------------------------------
 * NAME
 *    H5TS__mutex_unlock
 *
 * USAGE
 *    H5TS__mutex_unlock(&mutex_var, &lock_count)
 *
 * RETURNS
 *    Non-negative on success / Negative on failure
 *
 * DESCRIPTION
 *    Recursive lock semantics for HDF5 (unlocking) -
 *    Reset the lock and return the current lock count
 *
 * PROGRAMMER: Houjun Tang
 *             Nov 3, 2020
 *
 *--------------------------------------------------------------------------
 */
static herr_t
H5TS__mutex_unlock(H5TS_mutex_t *mutex, unsigned int *lock_count)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NAMECHECK_ONLY

#ifdef H5_HAVE_WIN_THREADS
    /* Releases ownership of the specified critical section object. */
    LeaveCriticalSection(&mutex->CriticalSection);
#else  /* H5_HAVE_WIN_THREADS */

    /* Reset the lock count for this thread */
    ret_value = pthread_mutex_lock(&mutex->atomic_lock);
    if (ret_value)
        HGOTO_DONE(ret_value);
    *lock_count       = mutex->lock_count;
    mutex->lock_count = 0;
    ret_value         = pthread_mutex_unlock(&mutex->atomic_lock);

    /* If the lock count drops to zero, signal the condition variable, to
     * wake another thread.
     */
    if (mutex->lock_count == 0) {
        int err;

        err = pthread_cond_signal(&mutex->cond_var);
        if (err != 0)
            ret_value = err;
    }

done:
#endif /* H5_HAVE_WIN_THREADS */
    FUNC_LEAVE_NOAPI_NAMECHECK_ONLY(ret_value)
} /* H5TS__mutex_unlock */

/*--------------------------------------------------------------------------
 * NAME
 *    H5TS_mutex_unlock
 *
 * USAGE
 *    H5TS_mutex_unlock(&mutex_var)
 *
 * RETURNS
 *    Non-negative on success / Negative on failure
 *
 * DESCRIPTION
 *    Recursive lock semantics for HDF5 (unlocking) -
 *    Multiple acquisition of a lock by a thread is permitted with a
 *    corresponding unlock operation required.
 *
 * PROGRAMMER: Chee Wai LEE
 *             May 2, 2000
 *
 *--------------------------------------------------------------------------
 */
herr_t
H5TS_mutex_unlock(H5TS_mutex_t *mutex)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NAMECHECK_ONLY

#ifdef H5_HAVE_WIN_THREADS
    /* Releases ownership of the specified critical section object. */
    LeaveCriticalSection(&mutex->CriticalSection);
#else  /* H5_HAVE_WIN_THREADS */

    /* Decrement the lock count for this thread */
    ret_value = pthread_mutex_lock(&mutex->atomic_lock);
    if (ret_value)
        HGOTO_DONE(ret_value);
    mutex->lock_count--;
    ret_value = pthread_mutex_unlock(&mutex->atomic_lock);

    /* If the lock count drops to zero, signal the condition variable, to
     * wake another thread.
     */
    if (mutex->lock_count == 0) {
        int err;

        err = pthread_cond_signal(&mutex->cond_var);
        if (err != 0)
            ret_value = err;
    }

done:
#endif /* H5_HAVE_WIN_THREADS */
    FUNC_LEAVE_NOAPI_NAMECHECK_ONLY(ret_value)
} /* H5TS_mutex_unlock */

/*--------------------------------------------------------------------------
 * Function:    H5TSmutex_get_attempt_count
 *
 * Purpose:     Get the current count of the global lock attempt
 *
 * Return:      Non-negative on success / Negative on failure
 *--------------------------------------------------------------------------
 */
herr_t
H5TSmutex_get_attempt_count(unsigned int *count)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_API_NAMECHECK_ONLY

#ifdef H5_HAVE_WIN_THREADS
    /* Add Win32 equivalent here when async is supported */
#else  /* H5_HAVE_WIN_THREADS */
    ret_value = pthread_mutex_lock(&H5_g.init_lock.atomic_lock2);
    if (ret_value)
        HGOTO_DONE(ret_value);

    *count = H5_g.init_lock.attempt_lock_count;

    ret_value = pthread_mutex_unlock(&H5_g.init_lock.atomic_lock2);
    if (ret_value)
        HGOTO_DONE(ret_value);

done:
#endif /* H5_HAVE_WIN_THREADS */
    FUNC_LEAVE_API_NAMECHECK_ONLY(ret_value)
} /* end H5TSmutex_get_attempt_count() */

/*--------------------------------------------------------------------------
 * Function:    H5TSmutex_release
 *
 * Purpose:     Releases the HDF5 library global lock
 *
 * Return:      Non-negative on success / Negative on failure
 *--------------------------------------------------------------------------
 */
herr_t
H5TSmutex_release(unsigned int *lock_count)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_API_NAMECHECK_ONLY

    *lock_count = 0;
    if (0 != H5TS__mutex_unlock(&H5_g.init_lock, lock_count))
        ret_value = -1;

    FUNC_LEAVE_API_NAMECHECK_ONLY(ret_value)
} /* end H5TSmutex_release() */

/*--------------------------------------------------------------------------
 * NAME
 *    H5TS_cancel_count_inc
 *
 * USAGE
 *    H5TS_cancel_count_inc()
 *
 * RETURNS
 *    Non-negative on success / Negative on failure
 *
 * DESCRIPTION
 *    Creates a cancellation counter for a thread if it is the first time
 *    the thread is entering the library.
 *
 *    if counter value is zero, then set cancellability type of the thread
 *    to PTHREAD_CANCEL_DISABLE as thread is entering the library and store
 *    the previous cancellability type into cancellation counter.
 *    Increase the counter value by 1.
 *
 * PROGRAMMER: Chee Wai LEE
 *            May 2, 2000
 *
 *--------------------------------------------------------------------------
 */
herr_t
H5TS_cancel_count_inc(void)
{
#ifndef H5_HAVE_WIN_THREADS
    H5TS_cancel_t *cancel_counter;
#endif /* H5_HAVE_WIN_THREADS */
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NAMECHECK_ONLY

#ifdef H5_HAVE_WIN_THREADS
    /* unsupported */
#else  /* H5_HAVE_WIN_THREADS */
    /* Acquire the thread's cancellation counter */
    cancel_counter = (H5TS_cancel_t *)H5TS_get_thread_local_value(H5TS_cancel_key_s);

    /* Check if it's created yet */
    if (!cancel_counter) {
        /*
         * First time thread calls library - create new counter and associate
         * with key.
         *
         * Don't use H5MM calls here since the destructor has to use free in
         * order to avoid codestack calls.
         */
        cancel_counter = (H5TS_cancel_t *)calloc(1, sizeof(H5TS_cancel_t));
        if (NULL == cancel_counter)
            HGOTO_DONE(FAIL);

        /* Set the thread's cancellation counter with the new object */
        ret_value = pthread_setspecific(H5TS_cancel_key_s, (void *)cancel_counter);
        if (ret_value) {
            free(cancel_counter);
            HGOTO_DONE(FAIL);
        }
    }

    /* Check if thread entering library */
    if (cancel_counter->cancel_count == 0)
        /* Set cancellation state to 'disable', and remember previous state */
        ret_value = pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &cancel_counter->previous_state);

    /* Increment # of times the library API was re-entered, to avoid resetting
     * previous cancellation state until the final API routine is returning.
     */
    ++cancel_counter->cancel_count;

done:
#endif /* H5_HAVE_WIN_THREADS */
    FUNC_LEAVE_NOAPI_NAMECHECK_ONLY(ret_value)
} /* end H5TS_cancel_count_inc() */

/*--------------------------------------------------------------------------
 * NAME
 *    H5TS_cancel_count_dec
 *
 * USAGE
 *    H5TS_cancel_count_dec()
 *
 * RETURNS
 *    0 on success and a non-zero error code on error.
 *
 * DESCRIPTION
 *    If counter value is one, then set cancellability type of the thread
 *    to the previous cancellability type stored in the cancellation counter.
 *    (the thread is leaving the library).
 *
 *    Decrement the counter value by 1.
 *
 * PROGRAMMER: Chee Wai LEE
 *             May 2, 2000
 *
 *--------------------------------------------------------------------------
 */
herr_t
H5TS_cancel_count_dec(void)
{
#ifndef H5_HAVE_WIN_THREADS
    H5TS_cancel_t *cancel_counter;
#endif /* H5_HAVE_WIN_THREADS */
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NAMECHECK_ONLY

#ifdef H5_HAVE_WIN_THREADS
    /* unsupported */
#else  /* H5_HAVE_WIN_THREADS */
    /* Acquire the thread's cancellation counter */
    cancel_counter = (H5TS_cancel_t *)H5TS_get_thread_local_value(H5TS_cancel_key_s);

    /* Check for leaving last API routine */
    if (cancel_counter->cancel_count == 1)
        /* Reset to previous thread cancellation state, if last API */
        ret_value = pthread_setcancelstate(cancel_counter->previous_state, NULL);

    /* Decrement cancellation counter */
    --cancel_counter->cancel_count;
#endif /* H5_HAVE_WIN_THREADS */

    FUNC_LEAVE_NOAPI_NAMECHECK_ONLY(ret_value)
} /* end H5TS_cancel_count_dec() */

#ifdef H5_HAVE_WIN_THREADS
/*--------------------------------------------------------------------------
 * NAME
 *    H5TS_win32_process_enter
 *
 * RETURNS
 *    SUCCEED/FAIL
 *
 * DESCRIPTION
 *    Per-process setup on Windows when using Win32 threads.
 *
 *--------------------------------------------------------------------------
 */
H5_DLL BOOL CALLBACK
H5TS_win32_process_enter(PINIT_ONCE InitOnce, PVOID Parameter, PVOID *lpContex)
{
    BOOL ret_value = true;

    FUNC_ENTER_NOAPI_NAMECHECK_ONLY

    /* Initialize the critical section (can't fail) */
    InitializeCriticalSection(&H5_g.init_lock.CriticalSection);

    /* Set up thread local storage */
    if (TLS_OUT_OF_INDEXES == (H5TS_errstk_key_g = TlsAlloc()))
        ret_value = false;

#ifdef H5_HAVE_CODESTACK
    if (TLS_OUT_OF_INDEXES == (H5TS_funcstk_key_g = TlsAlloc()))
        ret_value = false;
#endif /* H5_HAVE_CODESTACK */

    if (TLS_OUT_OF_INDEXES == (H5TS_apictx_key_g = TlsAlloc()))
        ret_value = false;

    FUNC_LEAVE_NOAPI_NAMECHECK_ONLY(ret_value)
} /* H5TS_win32_process_enter() */

/*--------------------------------------------------------------------------
 * NAME
 *    H5TS_win32_thread_enter
 *
 * RETURNS
 *    SUCCEED/FAIL
 *
 * DESCRIPTION
 *    Per-thread setup on Windows when using Win32 threads.
 *
 *--------------------------------------------------------------------------
 */
herr_t
H5TS_win32_thread_enter(void)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NAMECHECK_ONLY

    /* Currently a placeholder function.  TLS setup is performed
     * elsewhere in the library.
     *
     * WARNING: Do NOT use C standard library functions here.
     * CRT functions are not allowed in DllMain, which is where this code
     * is used.
     */

    FUNC_LEAVE_NOAPI_NAMECHECK_ONLY(ret_value)
} /* H5TS_win32_thread_enter() */

/*--------------------------------------------------------------------------
 * NAME
 *    H5TS_win32_process_exit
 *
 * RETURNS
 *    SUCCEED/FAIL
 *
 * DESCRIPTION
 *    Per-process cleanup on Windows when using Win32 threads.
 *
 *--------------------------------------------------------------------------
 */
void
H5TS_win32_process_exit(void)
{
    FUNC_ENTER_NOAPI_NAMECHECK_ONLY

    /* Windows uses a different thread local storage mechanism which does
     * not support auto-freeing like pthreads' keys.
     *
     * This function is currently registered via atexit() and is called
     * AFTER H5_term_library().
     */

    /* Clean up critical section resources (can't fail) */
    DeleteCriticalSection(&H5_g.init_lock.CriticalSection);

    /* Clean up per-process thread local storage */
    if (H5TS_errstk_key_g != TLS_OUT_OF_INDEXES)
        TlsFree(H5TS_errstk_key_g);
#ifdef H5_HAVE_CODESTACK
    if (H5TS_funcstk_key_g != TLS_OUT_OF_INDEXES)
        TlsFree(H5TS_funcstk_key_g);
#endif /* H5_HAVE_CODESTACK */
    if (H5TS_apictx_key_g != TLS_OUT_OF_INDEXES)
        TlsFree(H5TS_apictx_key_g);

    FUNC_LEAVE_NOAPI_VOID_NAMECHECK_ONLY
} /* H5TS_win32_process_exit() */

/*--------------------------------------------------------------------------
 * NAME
 *    H5TS_win32_thread_exit
 *
 * RETURNS
 *    SUCCEED/FAIL
 *
 * DESCRIPTION
 *    Per-thread cleanup on Windows when using Win32 threads.
 *
 *--------------------------------------------------------------------------
 */
herr_t
H5TS_win32_thread_exit(void)
{
    LPVOID lpvData;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NAMECHECK_ONLY

    /* Windows uses a different thread local storage mechanism which does
     * not support auto-freeing like pthreads' keys.
     *
     * WARNING: Do NOT use C standard library functions here.
     * CRT functions are not allowed in DllMain, which is where this code
     * is used.
     */

    /* Clean up per-thread thread local storage */
    if (H5TS_errstk_key_g != TLS_OUT_OF_INDEXES) {
        lpvData = TlsGetValue(H5TS_errstk_key_g);
        if (lpvData)
            LocalFree((HLOCAL)lpvData);
    }

#ifdef H5_HAVE_CODESTACK
    if (H5TS_funcstk_key_g != TLS_OUT_OF_INDEXES) {
        lpvData = TlsGetValue(H5TS_funcstk_key_g);
        if (lpvData)
            LocalFree((HLOCAL)lpvData);
    }
#endif /* H5_HAVE_CODESTACK */

    if (H5TS_apictx_key_g != TLS_OUT_OF_INDEXES) {
        lpvData = TlsGetValue(H5TS_apictx_key_g);
        if (lpvData)
            LocalFree((HLOCAL)lpvData);
    }

    FUNC_LEAVE_NOAPI_NAMECHECK_ONLY(ret_value)
} /* H5TS_win32_thread_exit() */
#endif /* H5_HAVE_WIN_THREADS */

/*--------------------------------------------------------------------------
 * NAME
 *    H5TS_create_thread
 *
 * RETURNS
 *    Thread identifier.
 *
 * DESCRIPTION
 *    Spawn off a new thread calling function 'func' with input 'udata'.
 *
 * PROGRAMMER: Mike McGreevy
 *             August 31, 2010
 *
 *--------------------------------------------------------------------------
 */
H5TS_thread_t
H5TS_create_thread(H5TS_thread_cb_t func, H5TS_attr_t *attr, void *udata)
{
    H5TS_thread_t ret_value;

    FUNC_ENTER_NOAPI_NAMECHECK_ONLY

#ifdef H5_HAVE_WIN_THREADS
    /* When calling C runtime functions, you should use _beginthread or
     * _beginthreadex instead of CreateThread.  Threads created with
     * CreateThread risk being killed in low-memory situations. Since we
     * only create threads in our test code, this is unlikely to be an issue
     * and we'll use the easier-to-deal-with CreateThread for now.
     *
     * NOTE: _beginthread() auto-recycles its handle when execution completes
     *       so you can't wait on it, making it unsuitable for the existing
     *       test code.
     */
    ret_value = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)func, udata, 0, NULL);

#else /* H5_HAVE_WIN_THREADS */

    pthread_create(&ret_value, attr, (void *(*)(void *))func, udata);

#endif /* H5_HAVE_WIN_THREADS */

    FUNC_LEAVE_NOAPI_NAMECHECK_ONLY(ret_value)
} /* H5TS_create_thread */

#endif /* H5_HAVE_THREADSAFE */
