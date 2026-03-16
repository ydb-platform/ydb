#ifndef WIN32COND_H
#define WIN32COND_H

/**
 * pthread_cond API for Win32
 * 
 * ACE(TM), TAO(TM), CIAO(TM), DAnCE>(TM), and CoSMIC(TM) (henceforth
 * referred to as "DOC software") are copyrighted by Douglas C. Schmidt
 * and his research group at Washington University, University of California,
 * Irvine, and Vanderbilt University, Copyright (c) 1993-2009, all rights
 * reserved.
 *
 * Since DOC software is open-source, freely available software, you are free
 * to use, modify, copy, and distribute--perpetually and irrevocably--the DOC
 * software source code and object code produced from the source, as well as
 * copy and distribute modified versions of this software. You must, however,
 * include this copyright statement along with any code built using DOC
 * software that you release.
 * 
 * No copyright statement needs to be provided if you just ship binary
 * executables of your software products.
 *
 * See "Strategies for Implementing POSIX Condition Variables on Win32" at
 * http://www.cs.wustl.edu/~schmidt/win32-cv-1.html
 */

#include <windows.h>

typedef struct
{
  long waiters_count_;
  // Number of waiting threads.

  CRITICAL_SECTION waiters_count_lock_;
  // Serialize access to <waiters_count_>.

  HANDLE sema_;
  // Semaphore used to queue up threads waiting for the condition to
  // become signaled. 

  HANDLE waiters_done_;
  // An auto-reset event used by the broadcast/signal thread to wait
  // for all the waiting thread(s) to wake up and be released from the
  // semaphore. 

  size_t was_broadcast_;
  // Keeps track of whether we were broadcasting or signaling.  This
  // allows us to optimize the code if we're just signaling.
} win32_cond_t;

#ifdef __cplusplus
extern "C" {
#endif

int win32_cond_init(win32_cond_t *cv);
int win32_cond_destroy(win32_cond_t *cv);
int win32_cond_wait(win32_cond_t *cv, HANDLE *external_mutex);
int win32_cond_signal(win32_cond_t *cv);
int win32_cond_broadcast(win32_cond_t *cv);

#ifdef __cplusplus
}
#endif

#endif
