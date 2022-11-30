#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright 2020 Mellanox Technologies, Ltd
 */

#include <time.h>

#include <rte_cycles.h>

void
rte_delay_us_sleep(unsigned int us)
{
	struct timespec wait[2];
	int ind = 0;

	wait[0].tv_sec = 0;
	if (us >= US_PER_S) {
		wait[0].tv_sec = us / US_PER_S;
		us -= wait[0].tv_sec * US_PER_S;
	}
	wait[0].tv_nsec = 1000 * us;

	while (nanosleep(&wait[ind], &wait[1 - ind]) && errno == EINTR) {
		/*
		 * Sleep was interrupted. Flip the index, so the 'remainder'
		 * will become the 'request' for a next call.
		 */
		ind = 1 - ind;
	}
}
