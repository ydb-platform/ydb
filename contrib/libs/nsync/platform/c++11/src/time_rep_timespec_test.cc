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

#include "platform.h"
#include "compiler.h"
#include "nsync.h"
#include "time_extra.h"
#include "smprintf.h"
/* #include "closure.h" */
#include "testing.h"

/* Test that the functions that accept and return C++11 time_point values work
   as advertised.  */

NSYNC_CPP_USING_

/* Test the form of nsync_counter_wait() that takes a time_point.  */
static void test_nsync_counter_wait_cpp (testing t) {
	nsync_time start;
	nsync_time waited;
	nsync_counter c = nsync_counter_new (1);
	start = nsync_time_now ();
	if (nsync_counter_wait (c, std::chrono::system_clock::now () +
				   std::chrono::milliseconds (1000)) != 1) {
		TEST_ERROR (t, ("counter is not 1 (1s wait)"));
	}
	waited = nsync_time_sub (nsync_time_now (), start);
        if (nsync_time_cmp (waited, nsync_time_ms (900)) < 0) {
                TEST_ERROR (t, ("timed wait on non-zero counter returned too quickly (1s wait took %s)",
                            nsync_time_str (waited, 2)));
        }
        if (nsync_time_cmp (waited, nsync_time_ms (2000)) > 0) {
                TEST_ERROR (t, ("timed wait on non-zero counter returned too slowly (1s wait took %s)",
                           nsync_time_str (waited, 2)));
        }
	if (nsync_counter_add (c, -1) != 0) {
		TEST_ERROR (t, ("zero counter is not 0 (add)"));
        }
	if (nsync_counter_wait (c, std::chrono::system_clock::now () +
				   std::chrono::milliseconds (1000)) != 0) {
		TEST_ERROR (t, ("counter is not 0 (1s wait)"));
	}
	nsync_counter_free (c);
}

/* Call nsync_mu_lock() on v. */
static void mu_lock (void *v) {
	nsync_mu_lock ((nsync_mu *) v);
}

/* Call nsync_mu_unlock() on v. */
static void mu_unlock (void *v) {
	nsync_mu_unlock ((nsync_mu *) v);
}

/* Test the forms of nsync_cv_wait_with_deadline() and
   nsync_cv_wait_with_deadline_generic() that take a time_point.  */
static void test_nsync_cv_wait_cpp (testing t UNUSED) {
	nsync_time start;
	nsync_time waited;
	nsync_mu mu;
	nsync_cv cv;
	nsync_mu_init (&mu);
	nsync_cv_init (&cv);
	nsync_mu_lock (&mu);
	start = nsync_time_now ();

	nsync_cv_wait_with_deadline (&cv, &mu,
				     std::chrono::system_clock::now () +
				     std::chrono::milliseconds (1000), nullptr);
	waited = nsync_time_sub (nsync_time_now (), start);
        if (nsync_time_cmp (waited, nsync_time_ms (900)) < 0) {
                TEST_ERROR (t, ("timed wait on condition variable returned too quickly (1s wait took %s)",
                            nsync_time_str (waited, 2)));
        }
        if (nsync_time_cmp (waited, nsync_time_ms (2000)) > 0) {
                TEST_ERROR (t, ("timed wait on condition variable returned too slowly (1s wait took %s)",
                           nsync_time_str (waited, 2)));
        }

	start = nsync_time_now ();
	nsync_cv_wait_with_deadline_generic (&cv, &mu, &mu_lock, &mu_unlock,
				     std::chrono::system_clock::now () +
				     std::chrono::milliseconds (1000), nullptr);
	waited = nsync_time_sub (nsync_time_now (), start);
        if (nsync_time_cmp (waited, nsync_time_ms (900)) < 0) {
                TEST_ERROR (t, ("timed wait generic on condition variable returned too quickly (1s wait took %s)",
                            nsync_time_str (waited, 2)));
        }
        if (nsync_time_cmp (waited, nsync_time_ms (2000)) > 0) {
                TEST_ERROR (t, ("timed wait generic on condition variable returned too slowly (1s wait took %s)",
                           nsync_time_str (waited, 2)));
        }

	nsync_mu_unlock (&mu);
}

/* A false condition. */
static int false_cond (const void *v UNUSED) {
	return (0);
}

/* Test the form of nsync_mu_wait_with_deadline() that takes a time_point.  */
static void test_nsync_mu_wait_cpp (testing t UNUSED) {
	nsync_time start;
	nsync_time waited;
	nsync_mu mu;
	nsync_mu_init (&mu);
	nsync_mu_lock (&mu);
	start = nsync_time_now ();
        nsync_mu_wait_with_deadline (&mu, &false_cond, nullptr, nullptr,
				     std::chrono::system_clock::now () +
				     std::chrono::milliseconds (1000), nullptr);
	waited = nsync_time_sub (nsync_time_now (), start);
        if (nsync_time_cmp (waited, nsync_time_ms (900)) < 0) {
                TEST_ERROR (t, ("timed wait on mutex returned too quickly (1s wait took %s)",
                            nsync_time_str (waited, 2)));
        }
        if (nsync_time_cmp (waited, nsync_time_ms (2000)) > 0) {
                TEST_ERROR (t, ("timed wait on mutex returned too slowly (1s wait took %s)",
                           nsync_time_str (waited, 2)));
        }
	nsync_mu_unlock (&mu);
}

/* Test the forms of nsync_note_new() and nsync_note_wait() that take a
   time_point, plus nsync_note_expiry_timepoint().  */
static void test_nsync_note_wait_cpp (testing t) {
	nsync_time start;
	nsync_time waited;
	std::chrono::system_clock::time_point now = std::chrono::system_clock::now ();
	std::chrono::system_clock::time_point expected_expiry =
		now + std::chrono::milliseconds (2000);
	nsync_note n = nsync_note_new (nullptr, expected_expiry);
	std::chrono::system_clock::time_point expiry = nsync_note_expiry_timepoint (n);
	/* Assume that "system_clock" is no coarser than 100ms */
	if (expiry < expected_expiry - std::chrono::milliseconds(101)) {
                TEST_ERROR (t, ("expiry earlier than expected"));
        }
	if (expected_expiry + std::chrono::milliseconds(101) < expiry) {
                TEST_ERROR (t, ("expiry later than expected"));
        }
	start = nsync_time_now ();
	if (nsync_note_wait (n, std::chrono::system_clock::now () +
				   std::chrono::milliseconds (1000))) {
		TEST_ERROR (t, ("note is ready (1s wait)"));
	}
	waited = nsync_time_sub (nsync_time_now (), start);
        if (nsync_time_cmp (waited, nsync_time_ms (900)) < 0) {
                TEST_ERROR (t, ("timed wait on non-ready note returned too quickly (1s wait took %s)",
                            nsync_time_str (waited, 2)));
        }
        if (nsync_time_cmp (waited, nsync_time_ms (1900)) > 0) {
                TEST_ERROR (t, ("timed wait on non-ready note returned too slowly (1s wait took %s)",
                           nsync_time_str (waited, 2)));
        }
	start = nsync_time_now ();
	if (!nsync_note_wait (n, std::chrono::system_clock::now () +
				 std::chrono::milliseconds (2000))) {
		TEST_ERROR (t, ("note did not expire (2s wait)"));
	}
	waited = nsync_time_sub (nsync_time_now (), start);
        if (nsync_time_cmp (waited, nsync_time_ms (900)) < 0) {
                TEST_ERROR (t, ("timed wait on expired note returned too quickly (1s wait took %s)",
                            nsync_time_str (waited, 2)));
        }
        if (nsync_time_cmp (waited, nsync_time_ms (1900)) > 0) {
                TEST_ERROR (t, ("timed wait on expired note returned too slowly (1s wait took %s)",
                           nsync_time_str (waited, 2)));
        }
	nsync_note_free (n);
}

/* Test the form of nsync_wait_n() that takes a time_point.  */
static void test_nsync_wait_n_cpp (testing t UNUSED) {
	nsync_time start;
	nsync_time waited;
	struct nsync_waitable_s w[1];
	struct nsync_waitable_s *wp[1];
	nsync_note n = nsync_note_new (nullptr, std::chrono::system_clock::now () +
						std::chrono::milliseconds (2000));
	w[0].v = n;
	w[0].funcs = &nsync_note_waitable_funcs;
	wp[0] = &w[0];
	start = nsync_time_now ();
	nsync_wait_n (nullptr, nullptr, nullptr,
		      std::chrono::system_clock::now () +
		      std::chrono::milliseconds (1000),
		      1, wp);
	waited = nsync_time_sub (nsync_time_now (), start);
        if (nsync_time_cmp (waited, nsync_time_ms (900)) < 0) {
                TEST_ERROR (t, ("timed wait_n on non-ready note returned too quickly (1s wait took %s)",
                            nsync_time_str (waited, 2)));
        }
        if (nsync_time_cmp (waited, nsync_time_ms (2000)) > 0) {
                TEST_ERROR (t, ("timed wait_n on non-ready note returned too slowly (1s wait took %s)",
                           nsync_time_str (waited, 2)));
        }
	nsync_note_free (n);
}

int main (int argc, char *argv[]) {
	testing_base tb = testing_new (argc, argv, 0);
        TEST_RUN (tb, test_nsync_counter_wait_cpp);
        TEST_RUN (tb, test_nsync_cv_wait_cpp);
        TEST_RUN (tb, test_nsync_mu_wait_cpp);
        TEST_RUN (tb, test_nsync_note_wait_cpp);
        TEST_RUN (tb, test_nsync_wait_n_cpp);
        return (testing_base_exit (tb));
}

#if 0
#define NSYNC_WAITER_CPP_OVERLOAD_ \
        static inline int nsync_wait_n (void *mu, void (*lock) (void *), \
                                        void (*unlock) (void *), \
                                        nsync_cpp_time_point_ abs_deadline, \
                                        int count, struct nsync_waitable_s *waitable[]) { \
                return (nsync_wait_n (mu, lock, unlock, \
                                      nsync_from_time_point_ (abs_deadline), count, waitable)); \
        }
#endif
