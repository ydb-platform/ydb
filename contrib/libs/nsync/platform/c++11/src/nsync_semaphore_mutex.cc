/* Copyright 2017 Google Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. */

#include "headers.h"

/* This module implements a binary semaphore using C++11 constructs.

   The interface of std::condition_variable requires the use of
   std::unique_lock, but the justification of correctness here is more subtle
   than I'd like.  Presumably, the idea behind std::unique_lock was to provide
   exception safety:  it guarantees to release the lock no matter how a block
   is exited.  However, if the lock had a non-trivial monitor invariant,
   std::unique_lock wouldn't restore it, so the programmer would be forced to
   maintain/restore the monitor invariant at every point in the region where an
   exception could be thown.  This is why other languages with locks and
   exceptions provide try-finally, even if they also provide scoped locked
   regions.

   In this code we can get away with std::unique_lock only because the
   monitor invariant happens to be trivial, and holds at all procedure call
   sites within the region.  The lock and unlock calls are explicit so readers
   can see the region bounaries.  */

NSYNC_CPP_START_

#define ASSERT(x) do { if (!(x)) { *(volatile int *)0 = 0; } } while (0)

struct mutex_cond {
	std::mutex mu;
	std::condition_variable cv;
	uint32_t i;
};

static_assert (sizeof (struct mutex_cond) <= sizeof (nsync_semaphore),
               "mutex_cond must fit in an nsync_semaphore");

/* Initialize *s; the initial value is 0. */
void nsync_mu_semaphore_init (nsync_semaphore *s) {
	struct mutex_cond *mc = (struct mutex_cond *) s;
	new (&mc->mu) std::mutex ();
	new (&mc->cv) std::condition_variable ();
	mc->i = 0;
}

/* Wait until the count of *s exceeds 0, and decrement it. */
void nsync_mu_semaphore_p (nsync_semaphore *s) {
	struct mutex_cond *mc = (struct mutex_cond *) s;
        /* std::unique_lock works here because the monitor invariant holds
           throughout the region.  */
	std::unique_lock<std::mutex> mc_mu(mc->mu, std::defer_lock);

	mc_mu.lock ();
	while (mc->i == 0) {
		mc->cv.wait (mc_mu);
	}
	mc->i = 0;
	mc_mu.unlock ();
}

/* Wait until one of:
   the count of *s is non-zero, in which case decrement *s and return 0;
   or abs_deadline expires, in which case return ETIMEDOUT. */
int nsync_mu_semaphore_p_with_deadline (nsync_semaphore *s,
					nsync_time abs_deadline) {
	struct mutex_cond *mc = (struct mutex_cond *)s;
	std::cv_status res = std::cv_status::no_timeout;
        /* std::unique_lock works here because the monitor invariant holds
           throughout the regions. */
	std::unique_lock<std::mutex> mc_mu(mc->mu, std::defer_lock);

	if (nsync_time_cmp (abs_deadline, nsync_time_no_deadline) == 0) {
		mc_mu.lock ();
		while (mc->i == 0) {
			mc->cv.wait (mc_mu);
		}
		mc->i = 0;
		mc_mu.unlock ();
	} else {
		nsync_cpp_time_point_ tp_deadline = nsync_to_time_point_ (abs_deadline);
		mc_mu.lock ();
		while (mc->i == 0 &&
		       ((res = mc->cv.wait_until (mc_mu, tp_deadline)) == std::cv_status::no_timeout ||
			(res == std::cv_status::timeout && /* Various systems wake up too early. */
			 nsync_time_cmp (abs_deadline, nsync_time_now ()) > 0))) {
		}
		ASSERT (res == std::cv_status::no_timeout ||
			res == std::cv_status::timeout);
		if (mc->i != 0) {
			res = std::cv_status::no_timeout;
			mc->i = 0;
		}
		mc_mu.unlock ();
	}
	return (res == std::cv_status::timeout? ETIMEDOUT : 0);
}

/* Ensure that the count of *s is at least 1. */
void nsync_mu_semaphore_v (nsync_semaphore *s) {
	struct mutex_cond *mc = (struct mutex_cond *) s;
        /* std::unique_lock works here because the monitor invariant holds
           throughout the region. */
	std::unique_lock<std::mutex> mc_mu(mc->mu, std::defer_lock);
	mc_mu.lock ();
	mc->i = 1;
	mc->cv.notify_all ();
	mc_mu.unlock ();
}

NSYNC_CPP_END_
