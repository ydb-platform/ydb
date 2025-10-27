/*
  Logic for a better replacement of PyGILState_Ensure().

  This version is ready to handle the case of a non-Python-started
  thread in which we do a large number of calls to CFFI callbacks.  If
  we were to rely on PyGILState_Ensure() for that, we would constantly
  be creating and destroying PyThreadStates---it is slow, and
  PyThreadState_Delete() will actually walk the list of all thread
  states, making it O(n). :-(

  This version only creates one PyThreadState object the first time we
  see a given thread, and keep it alive until the thread is really
  shut down, using a destructor on the tls key.
*/
#ifndef CFFI_MISC_THREAD_POSIX_H
#define CFFI_MISC_THREAD_POSIX_H

#include <pthread.h>
#include "misc_thread_common.h"


static pthread_key_t cffi_tls_key;

static void init_cffi_tls(void)
{
    if (pthread_key_create(&cffi_tls_key, &cffi_thread_shutdown) != 0)
        PyErr_SetString(PyExc_OSError, "pthread_key_create() failed");
}

static struct cffi_tls_s *_make_cffi_tls(void)
{
    void *p = calloc(1, sizeof(struct cffi_tls_s));
    if (p == NULL)
        return NULL;
    if (pthread_setspecific(cffi_tls_key, p) != 0) {
        free(p);
        return NULL;
    }
    return p;
}

static struct cffi_tls_s *get_cffi_tls(void)
{
    void *p = pthread_getspecific(cffi_tls_key);
    if (p == NULL)
        p = _make_cffi_tls();
    return (struct cffi_tls_s *)p;
}

#define save_errno      save_errno_only
#define restore_errno   restore_errno_only

#ifdef Py_GIL_DISABLED
# ifndef __ATOMIC_SEQ_CST
#  error "The free threading build needs atomic support"
# endif

/* Minimal atomic support */
static void *cffi_atomic_load(void **ptr)
{
    return __atomic_load_n(ptr, __ATOMIC_SEQ_CST);
}

static void cffi_atomic_store(void **ptr, void *value)
{
    __atomic_store_n(ptr, value, __ATOMIC_SEQ_CST);
}

static uint8_t cffi_atomic_load_uint8(uint8_t *ptr)
{
    return __atomic_load_n(ptr, __ATOMIC_SEQ_CST);
}

static void cffi_atomic_store_uint8(uint8_t *ptr, uint8_t value)
{
    __atomic_store_n(ptr, value, __ATOMIC_SEQ_CST);
}

static Py_ssize_t cffi_atomic_load_ssize(Py_ssize_t *ptr)
{
    return __atomic_load_n(ptr, __ATOMIC_SEQ_CST);
}

static void cffi_atomic_store_ssize(Py_ssize_t *ptr, Py_ssize_t value)
{
    __atomic_store_n(ptr, value, __ATOMIC_SEQ_CST);
}

#endif

#endif /* CFFI_MISC_THREAD_POSIX_H */
