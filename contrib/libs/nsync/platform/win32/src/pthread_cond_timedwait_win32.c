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

#include "nsync_cpp.h"
#include "platform.h"

NSYNC_CPP_START_

int nsync_pthread_cond_timedwait (pthread_cond_t *cv, pthread_mutex_t *mu,
			    const struct timespec *abs_deadline) {
	int result;
        if (abs_deadline == NULL) {
                result = SleepConditionVariableCS (cv, mu, INFINITE);
	} else {
		int again;
		do {
			struct timespec now;
			again = 0;
			clock_gettime (CLOCK_REALTIME, &now);
			if (abs_deadline->tv_sec < now.tv_sec ||
                            (abs_deadline->tv_sec == now.tv_sec && abs_deadline->tv_nsec <= now.tv_nsec)) {
				result = SleepConditionVariableCS (cv, mu, 0);
			} else {
				struct timespec delay;
				delay = *abs_deadline;
				delay.tv_sec -= now.tv_sec;
				if (delay.tv_nsec < now.tv_nsec) {
					delay.tv_nsec += 1000 * 1000 * 1000;
					delay.tv_sec--;
				}
				delay.tv_nsec -= now.tv_nsec;
				if (delay.tv_sec > 1000*1000) {
					result = SleepConditionVariableCS (cv, mu, 1000*1000);
					again = (result == ERROR_TIMEOUT);
				} else {
					result = SleepConditionVariableCS (cv, mu,
						(unsigned) (delay.tv_sec * 1000 +
						(delay.tv_nsec + 999999) / (1000 * 1000)));
					if (result == ERROR_TIMEOUT) {
						/* Windows often generates early wakeups. */
						clock_gettime (CLOCK_REALTIME, &now);
						again = (abs_deadline->tv_sec > now.tv_sec ||
						         (abs_deadline->tv_sec == now.tv_sec &&
							  abs_deadline->tv_nsec > now.tv_nsec));
					}
				}
			}
		} while (again);
	}
	return (result == ERROR_TIMEOUT? ETIMEDOUT : 0);
}

NSYNC_CPP_END_
