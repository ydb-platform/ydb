#include "pycurl.h"

#ifdef WITH_THREAD

PYCURL_INTERNAL PyThreadState *
pycurl_get_thread_state(const CurlObject *self)
{
    /* Get the thread state for callbacks to run in.
     * This is either `self->state' when running inside perform() or
     * `self->multi_stack->state' when running inside multi_perform().
     * When the result is != NULL we also implicitly assert
     * a valid `self->handle'.
     */
    if (self == NULL)
        return NULL;
    assert(Py_TYPE(self) == p_Curl_Type);
    if (self->state != NULL)
    {
        /* inside perform() */
        assert(self->handle != NULL);
        if (self->multi_stack != NULL) {
            assert(self->multi_stack->state == NULL);
        }
        return self->state;
    }
    if (self->multi_stack != NULL && self->multi_stack->state != NULL)
    {
        /* inside multi_perform() */
        assert(self->handle != NULL);
        assert(self->multi_stack->multi_handle != NULL);
        assert(self->state == NULL);
        return self->multi_stack->state;
    }
    return NULL;
}


PYCURL_INTERNAL PyThreadState *
pycurl_get_thread_state_multi(const CurlMultiObject *self)
{
    /* Get the thread state for callbacks to run in when given
     * multi handles instead of regular handles
     */
    if (self == NULL)
        return NULL;
    assert(Py_TYPE(self) == p_CurlMulti_Type);
    if (self->state != NULL)
    {
        /* inside multi_perform() */
        assert(self->multi_handle != NULL);
        return self->state;
    }
    return NULL;
}


PYCURL_INTERNAL int
pycurl_acquire_thread(const CurlObject *self, PyThreadState **state)
{
    *state = pycurl_get_thread_state(self);
    if (*state == NULL)
        return 0;
    PyEval_AcquireThread(*state);
    return 1;
}


PYCURL_INTERNAL int
pycurl_acquire_thread_multi(const CurlMultiObject *self, PyThreadState **state)
{
    *state = pycurl_get_thread_state_multi(self);
    if (*state == NULL)
        return 0;
    PyEval_AcquireThread(*state);
    return 1;
}


PYCURL_INTERNAL void
pycurl_release_thread(PyThreadState *state)
{
    PyEval_ReleaseThread(state);
}

/*************************************************************************
// SSL TSL
**************************************************************************/

#ifdef PYCURL_NEED_OPENSSL_TSL

#if OPENSSL_VERSION_NUMBER < 0x10100000
static PyThread_type_lock *pycurl_openssl_tsl = NULL;

static void
pycurl_ssl_lock(int mode, int n, const char * file, int line)
{
    if (mode & CRYPTO_LOCK) {
        PyThread_acquire_lock(pycurl_openssl_tsl[n], 1);
    } else {
        PyThread_release_lock(pycurl_openssl_tsl[n]);
    }
}

#if OPENSSL_VERSION_NUMBER >= 0x10000000
/* use new CRYPTO_THREADID API. */
static void
pycurl_ssl_threadid_callback(CRYPTO_THREADID *id)
{
    CRYPTO_THREADID_set_numeric(id, (unsigned long)PyThread_get_thread_ident());
}
#else
/* deprecated CRYPTO_set_id_callback() API. */
static unsigned long
pycurl_ssl_id(void)
{
    return (unsigned long) PyThread_get_thread_ident();
}
#endif
#endif

PYCURL_INTERNAL int
pycurl_ssl_init(void)
{
#if OPENSSL_VERSION_NUMBER < 0x10100000
    int i, c = CRYPTO_num_locks();

    pycurl_openssl_tsl = PyMem_New(PyThread_type_lock, c);
    if (pycurl_openssl_tsl == NULL) {
        PyErr_NoMemory();
        return -1;
    }
    memset(pycurl_openssl_tsl, 0, sizeof(PyThread_type_lock) * c);

    for (i = 0; i < c; ++i) {
        pycurl_openssl_tsl[i] = PyThread_allocate_lock();
        if (pycurl_openssl_tsl[i] == NULL) {
            for (--i; i >= 0; --i) {
                PyThread_free_lock(pycurl_openssl_tsl[i]);
            }
            PyMem_Free(pycurl_openssl_tsl);
            PyErr_NoMemory();
            return -1;
        }
    }

#if OPENSSL_VERSION_NUMBER >= 0x10000000
    CRYPTO_THREADID_set_callback(pycurl_ssl_threadid_callback);
#else
    CRYPTO_set_id_callback(pycurl_ssl_id);
#endif
    CRYPTO_set_locking_callback(pycurl_ssl_lock);
#endif
    return 0;
}

PYCURL_INTERNAL void
pycurl_ssl_cleanup(void)
{
#if OPENSSL_VERSION_NUMBER < 0x10100000
    if (pycurl_openssl_tsl) {
        int i, c = CRYPTO_num_locks();

#if OPENSSL_VERSION_NUMBER >= 0x10000000
        CRYPTO_THREADID_set_callback(NULL);
#else
        CRYPTO_set_id_callback(NULL);
#endif
        CRYPTO_set_locking_callback(NULL);

        for (i = 0; i < c; ++i) {
            PyThread_free_lock(pycurl_openssl_tsl[i]);
        }

        PyMem_Free(pycurl_openssl_tsl);
        pycurl_openssl_tsl = NULL;
    }
#endif
}
#endif

#ifdef PYCURL_NEED_GNUTLS_TSL
static int
pycurl_ssl_mutex_create(void **m)
{
    if ((*((PyThread_type_lock *) m) = PyThread_allocate_lock()) == NULL) {
        return -1;
    } else {
        return 0;
    }
}

static int
pycurl_ssl_mutex_destroy(void **m)
{
    PyThread_free_lock(*((PyThread_type_lock *) m));
    return 0;
}

static int
pycurl_ssl_mutex_lock(void **m)
{
    return !PyThread_acquire_lock(*((PyThread_type_lock *) m), 1);
}

static int
pycurl_ssl_mutex_unlock(void **m)
{
    PyThread_release_lock(*((PyThread_type_lock *) m));
    return 0;
}

static struct gcry_thread_cbs pycurl_gnutls_tsl = {
    GCRY_THREAD_OPTION_USER,
    NULL,
    pycurl_ssl_mutex_create,
    pycurl_ssl_mutex_destroy,
    pycurl_ssl_mutex_lock,
    pycurl_ssl_mutex_unlock
};

PYCURL_INTERNAL int
pycurl_ssl_init(void)
{
    gcry_control(GCRYCTL_SET_THREAD_CBS, &pycurl_gnutls_tsl);
    return 0;
}

PYCURL_INTERNAL void
pycurl_ssl_cleanup(void)
{
    return;
}
#endif

/* mbedTLS */

#ifdef PYCURL_NEED_MBEDTLS_TSL
static int
pycurl_ssl_mutex_create(void **m)
{
    if ((*((PyThread_type_lock *) m) = PyThread_allocate_lock()) == NULL) {
        return -1;
    } else {
        return 0;
    }
}

static int
pycurl_ssl_mutex_destroy(void **m)
{
    PyThread_free_lock(*((PyThread_type_lock *) m));
    return 0;
}

static int
pycurl_ssl_mutex_lock(void **m)
{
    return !PyThread_acquire_lock(*((PyThread_type_lock *) m), 1);
}

PYCURL_INTERNAL int
pycurl_ssl_init(void)
{
    return 0;
}

PYCURL_INTERNAL void
pycurl_ssl_cleanup(void)
{
    return;
}
#endif

/*************************************************************************
// CurlShareObject
**************************************************************************/

PYCURL_INTERNAL void
share_lock_lock(ShareLock *lock, curl_lock_data data)
{
    PyThread_acquire_lock(lock->locks[data], 1);
}

PYCURL_INTERNAL void
share_lock_unlock(ShareLock *lock, curl_lock_data data)
{
    PyThread_release_lock(lock->locks[data]);
}

PYCURL_INTERNAL ShareLock *
share_lock_new(void)
{
    int i;
    ShareLock *lock = PyMem_New(ShareLock, 1);
    if (lock == NULL) {
        PyErr_NoMemory();
        return NULL;
    }

    for (i = 0; i < CURL_LOCK_DATA_LAST; ++i) {
        lock->locks[i] = PyThread_allocate_lock();
        if (lock->locks[i] == NULL) {
            PyErr_NoMemory();
            goto error;
        }
    }
    return lock;

error:
    for (--i; i >= 0; --i) {
        PyThread_free_lock(lock->locks[i]);
        lock->locks[i] = NULL;
    }
    PyMem_Free(lock);
    return NULL;
}

PYCURL_INTERNAL void
share_lock_destroy(ShareLock *lock)
{
    int i;

    assert(lock);
    for (i = 0; i < CURL_LOCK_DATA_LAST; ++i){
        assert(lock->locks[i] != NULL);
        PyThread_free_lock(lock->locks[i]);
    }
    PyMem_Free(lock);
    lock = NULL;
}

PYCURL_INTERNAL void
share_lock_callback(CURL *handle, curl_lock_data data, curl_lock_access locktype, void *userptr)
{
    CurlShareObject *share = (CurlShareObject*)userptr;
    share_lock_lock(share->lock, data);
}

PYCURL_INTERNAL void
share_unlock_callback(CURL *handle, curl_lock_data data, void *userptr)
{
    CurlShareObject *share = (CurlShareObject*)userptr;
    share_lock_unlock(share->lock, data);
}

#else /* WITH_THREAD */

#if defined(PYCURL_NEED_SSL_TSL)
PYCURL_INTERNAL void
pycurl_ssl_init(void)
{
    return 0;
}

PYCURL_INTERNAL void
pycurl_ssl_cleanup(void)
{
    return;
}
#endif

#endif /* WITH_THREAD */

/* vi:ts=4:et:nowrap
 */
