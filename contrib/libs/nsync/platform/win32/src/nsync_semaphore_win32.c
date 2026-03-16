/* Copyright 2016 Google Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. */

#include <Windows.h>
#include <stdlib.h>
#include "nsync_cpp.h"
#include "nsync_time.h"
#include "sem.h"

NSYNC_CPP_START_

/* Initialize *s; the initial value is 0. */
void nsync_mu_semaphore_init (nsync_semaphore *s) {
	HANDLE *h = (HANDLE *) s;
	*h = CreateSemaphore(NULL, 0, 1, NULL);
	if (*h == NULL) {
		abort ();
	}
}

/* Wait until the count of *s exceeds 0, and decrement it. */
void nsync_mu_semaphore_p (nsync_semaphore *s) {
	HANDLE *h = (HANDLE *) s;
	WaitForSingleObject(*h, INFINITE);
}

/* Wait until one of:
   the count of *s is non-zero, in which case decrement *s and return 0;
   or abs_deadline expires, in which case return ETIMEDOUT. */
int nsync_mu_semaphore_p_with_deadline (nsync_semaphore *s, nsync_time abs_deadline) {
	HANDLE *h = (HANDLE *) s;
	int result;

	if (nsync_time_cmp (abs_deadline, nsync_time_no_deadline) == 0) {
		result = WaitForSingleObject(*h, INFINITE);
	} else {
		nsync_time now;
		now = nsync_time_now ();
		do {
			if (nsync_time_cmp (abs_deadline, now) <= 0) {
				result = WaitForSingleObject (*h, 0);
			} else {
				nsync_time delay;
				delay = nsync_time_sub (abs_deadline, now);
				if (NSYNC_TIME_SEC (delay) > 1000*1000) {
					result = WaitForSingleObject (*h, 1000*1000);
				} else {
					result = WaitForSingleObject (*h,
						(unsigned) (NSYNC_TIME_SEC (delay) * 1000 +
							(NSYNC_TIME_NSEC (delay) + 999999) / (1000 * 1000)));
				}
			}
			if (result == WAIT_TIMEOUT) {
				now = nsync_time_now ();
			}
		} while (result == WAIT_TIMEOUT && /* Windows generates early wakeups. */
			 nsync_time_cmp (abs_deadline, now) > 0);
	}
	return (result == WAIT_TIMEOUT? ETIMEDOUT : 0);
}

/* Ensure that the count of *s is at least 1. */
void nsync_mu_semaphore_v (nsync_semaphore *s) {
	HANDLE *h = (HANDLE *) s;
	ReleaseSemaphore(*h, 1, NULL);
}

NSYNC_CPP_END_
