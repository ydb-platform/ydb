/* Copyright (c) 1999 Ng Pheng Siong. All rights reserved. */
/* $Id$ */

%{
#include <pythread.h>
#include <openssl/crypto.h>

#if defined(THREADING) && OPENSSL_VERSION_NUMBER < 0x10100000L
#define CRYPTO_num_locks()      (CRYPTO_NUM_LOCKS)
static PyThread_type_lock lock_cs[CRYPTO_num_locks()];
static long lock_count[CRYPTO_num_locks()];
static int thread_mode = 0;
#endif

void threading_locking_callback(int mode, int type, const char *file, int line) {
#if defined(THREADING) && OPENSSL_VERSION_NUMBER < 0x10100000L
        if (mode & CRYPTO_LOCK) {
                PyThread_acquire_lock(lock_cs[type], WAIT_LOCK);
                lock_count[type]++;
        } else {
                PyThread_release_lock(lock_cs[type]);
                lock_count[type]--;
        }
#endif
}

unsigned long threading_id_callback(void) {
#if defined(THREADING) && OPENSSL_VERSION_NUMBER < 0x10100000L
    return (unsigned long)PyThread_get_thread_ident();
#else
    return (unsigned long)0;
#endif
}
%}

%inline %{
void threading_init(void) {
#if defined(THREADING) && OPENSSL_VERSION_NUMBER < 0x10100000L
    int i;
    if (!thread_mode) {
        for (i=0; i<CRYPTO_num_locks(); i++) {
            lock_count[i]=0;
            lock_cs[i]=PyThread_allocate_lock();
        }
        CRYPTO_set_id_callback(threading_id_callback);
        CRYPTO_set_locking_callback(threading_locking_callback);
    }
    thread_mode = 1;
#endif
}

void threading_cleanup(void) {
#if defined(THREADING) && OPENSSL_VERSION_NUMBER < 0x10100000L
    int i;
    if (thread_mode) {
        CRYPTO_set_locking_callback(NULL);
        for (i=0; i<CRYPTO_num_locks(); i++) {
            lock_count[i]=0;
            PyThread_release_lock(lock_cs[i]);
            PyThread_free_lock(lock_cs[i]);
        }
    }
    thread_mode = 0;
#endif
}
%}

