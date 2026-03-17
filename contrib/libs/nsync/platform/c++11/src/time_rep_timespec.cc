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

#include "nsync_cpp.h"
#include "platform.h"
#include "compiler.h"
#include "cputype.h"
#include "nsync_time_init.h"
#include "nsync_time.h"
#include <chrono>
#include <thread>

NSYNC_CPP_START_

#define NSYNC_NS_IN_S_ (1000 * 1000 * 1000)

/* Return the maximum t, assuming it's an integral
   type, and the representation is not too strange.  */
#define MAX_INT_TYPE(t) (((t)~(t)0) > 1?   /*is t unsigned?*/ \
                (t)~(t)0 :  /*unsigned*/ \
                (t) ((((uintmax_t)1) << (sizeof (t) * CHAR_BIT - 1)) - 1)) /*signed*/

const nsync_time nsync_time_no_deadline =
        NSYNC_TIME_STATIC_INIT (MAX_INT_TYPE (time_t), NSYNC_NS_IN_S_ - 1);

const nsync_time nsync_time_zero = NSYNC_TIME_STATIC_INIT (0, 0);

nsync_time nsync_time_s_ns (time_t s, unsigned ns) {
	nsync_time t;
	memset (&t, 0, sizeof (t));
	t.tv_sec = s;
	t.tv_nsec = ns;
	return (t);
}

/* Return the nsync_time corresponding to tp. */
nsync_time nsync_from_time_point_ (nsync_cpp_time_point_ tp) {
        /* std::chrono::system_clock::to_time_t() says that "[i]t is implementation
           defined whether values are rounded or truncated to the required
           precision", which is a little tricky to use.
           Assume instead that the epoch for std::chrono::system_clock::time_point is
           the same as that for a struct timespec.  This seems to be true on at
           least Linux, NetBSD, MacOS, and Win32; and with 64 bits gives
	   us an implementation that will work for over 200 years.   By then,
	   we'll be able to call C++17's timespec_get.  */
        int64_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                tp.time_since_epoch ()).count();
	struct timespec ts;
	memset (&ts, 0, sizeof (ts));
	ts.tv_sec = ns / NSYNC_NS_IN_S_;
	ts.tv_nsec = (long) (ns - ts.tv_sec * NSYNC_NS_IN_S_);
	return (ts);
}

/* Return the nsync_cpp_time_point_ corresponding to absolute time t. */
nsync_cpp_time_point_ nsync_to_time_point_ (nsync_time t) {
	nsync_cpp_time_point_ tp;
	std::chrono::nanoseconds t_ns(NSYNC_TIME_NSEC (t) +
                                      NSYNC_NS_IN_S_ * (int64_t) NSYNC_TIME_SEC (t));
	nsync_cpp_time_point_::duration tp_dur =
		std::chrono::duration_cast<nsync_cpp_time_point_::duration>(t_ns);
	return (tp + tp_dur);
}

nsync_time nsync_time_now (void) {
        return (nsync_from_time_point_ (std::chrono::system_clock::now ()));
}

nsync_time nsync_time_sleep (nsync_time delay) {
	nsync_time start = nsync_time_now ();
	nsync_time expected_end = nsync_time_add (start, delay);
	nsync_time remain;
	std::chrono::nanoseconds delay_ns(NSYNC_TIME_NSEC (delay) +
					  NSYNC_NS_IN_S_ * (int64_t) NSYNC_TIME_SEC (delay));
	std::this_thread::sleep_for (delay_ns);
	nsync_time actual_end = nsync_time_now ();
	if (nsync_time_cmp (actual_end, expected_end) < 0) {
		remain = nsync_time_sub (expected_end, actual_end);
	} else {
		remain = nsync_time_zero;
	}
	return (remain);
}

nsync_time nsync_time_add (nsync_time a, nsync_time b) {
	a.tv_sec += b.tv_sec;
	a.tv_nsec += b.tv_nsec;
	if (a.tv_nsec >= NSYNC_NS_IN_S_) {
		a.tv_nsec -= NSYNC_NS_IN_S_;
		a.tv_sec++;
	}
	return (a);
}

nsync_time nsync_time_sub (nsync_time a, nsync_time b) {
	a.tv_sec -= b.tv_sec;
	if (a.tv_nsec < b.tv_nsec) {
		a.tv_nsec += NSYNC_NS_IN_S_;
		a.tv_sec--;
	}
	a.tv_nsec -= b.tv_nsec;
	return (a);
}

int nsync_time_cmp (nsync_time a, nsync_time b) {
	int cmp = (NSYNC_TIME_SEC (a) > NSYNC_TIME_SEC (b)) -
		  (NSYNC_TIME_SEC (a) < NSYNC_TIME_SEC (b));
	if (cmp == 0) {
		cmp = (NSYNC_TIME_NSEC (a) > NSYNC_TIME_NSEC (b)) -
		      (NSYNC_TIME_NSEC (a) < NSYNC_TIME_NSEC (b));
	}
	return (cmp);
}

NSYNC_CPP_END_
