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

#include "win32cond.h"

int win32_cond_init(win32_cond_t *cv)
{
  cv->waiters_count_ = 0;
  cv->was_broadcast_ = 0;
  cv->sema_ = CreateSemaphore (NULL,       // no security
                                0,          // initially 0
                                0x7fffffff, // max count
                                NULL);      // unnamed 
  InitializeCriticalSection (&cv->waiters_count_lock_);
  cv->waiters_done_ = CreateEvent (NULL,  // no security
                                   FALSE, // auto-reset
                                   FALSE, // non-signaled initially
                                   NULL); // unnamed
  return 0;
}

int win32_cond_destroy(win32_cond_t *cv)
{
  CloseHandle(cv->waiters_done_);
  DeleteCriticalSection(&cv->waiters_count_lock_);
  CloseHandle(cv->sema_);
  return 0;
}

int win32_cond_wait(win32_cond_t *cv, HANDLE *external_mutex)
{
  int last_waiter;

  // Avoid race conditions.
  EnterCriticalSection (&cv->waiters_count_lock_);
  cv->waiters_count_++;
  LeaveCriticalSection (&cv->waiters_count_lock_);

  // This call atomically releases the mutex and waits on the
  // semaphore until <pthread_cond_signal> or <pthread_cond_broadcast>
  // are called by another thread.
  SignalObjectAndWait (*external_mutex, cv->sema_, INFINITE, FALSE);

  // Reacquire lock to avoid race conditions.
  EnterCriticalSection (&cv->waiters_count_lock_);

  // We're no longer waiting...
  cv->waiters_count_--;

  // Check to see if we're the last waiter after <pthread_cond_broadcast>.
  last_waiter = cv->was_broadcast_ && cv->waiters_count_ == 0;

  LeaveCriticalSection (&cv->waiters_count_lock_);

  // If we're the last waiter thread during this particular broadcast
  // then let all the other threads proceed.
  if (last_waiter)
    // This call atomically signals the <waiters_done_> event and waits until
    // it can acquire the <external_mutex>.  This is required to ensure fairness. 
    SignalObjectAndWait (cv->waiters_done_, *external_mutex, INFINITE, FALSE);
  else
    // Always regain the external mutex since that's the guarantee we
    // give to our callers. 
    WaitForSingleObject (*external_mutex, INFINITE);
  return 0;
}

int win32_cond_signal(win32_cond_t *cv)
{
  int have_waiters;

  EnterCriticalSection (&cv->waiters_count_lock_);
  have_waiters = cv->waiters_count_ > 0;
  LeaveCriticalSection (&cv->waiters_count_lock_);

  // If there aren't any waiters, then this is a no-op.  
  if (have_waiters)
    ReleaseSemaphore (cv->sema_, 1, 0);
  return 0;
}

int win32_cond_broadcast(win32_cond_t *cv)
{
  int have_waiters = 0;

  // This is needed to ensure that <waiters_count_> and <was_broadcast_> are
  // consistent relative to each other.
  EnterCriticalSection (&cv->waiters_count_lock_);

  if (cv->waiters_count_ > 0) {
    // We are broadcasting, even if there is just one waiter...
    // Record that we are broadcasting, which helps optimize
    // <pthread_cond_wait> for the non-broadcast case.
    cv->was_broadcast_ = 1;
    have_waiters = 1;
  }

  if (have_waiters) {
    // Wake up all the waiters atomically.
    ReleaseSemaphore (cv->sema_, cv->waiters_count_, 0);

    LeaveCriticalSection (&cv->waiters_count_lock_);

    // Wait for all the awakened threads to acquire the counting
    // semaphore. 
    WaitForSingleObject (cv->waiters_done_, INFINITE);
    // This assignment is okay, even without the <waiters_count_lock_> held 
    // because no other waiter threads can wake up to access it.
    cv->was_broadcast_ = 0;
  }
  else
    LeaveCriticalSection (&cv->waiters_count_lock_);
  return 0;
}
