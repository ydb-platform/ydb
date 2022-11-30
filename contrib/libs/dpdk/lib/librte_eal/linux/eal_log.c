#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <sys/types.h>
#include <syslog.h>
#include <sys/queue.h>

#include <rte_memory.h>
#include <rte_eal.h>
#include <rte_launch.h>
#include <rte_per_lcore.h>
#include <rte_lcore.h>
#include <rte_spinlock.h>
#include <rte_log.h>

#include "eal_private.h"

/*
 * default log function
 */
static ssize_t
console_log_write(__rte_unused void *c, const char *buf, size_t size)
{
	ssize_t ret;

	/* write on stdout */
	ret = fwrite(buf, 1, size, stdout);
	fflush(stdout);

	/* Syslog error levels are from 0 to 7, so subtract 1 to convert */
	syslog(rte_log_cur_msg_loglevel() - 1, "%.*s", (int)size, buf);

	return ret;
}

static cookie_io_functions_t console_log_func = {
	.write = console_log_write,
};

/*
 * set the log to default function, called during eal init process,
 * once memzones are available.
 */
int
rte_eal_log_init(const char *id, int facility)
{
	FILE *log_stream;

	log_stream = fopencookie(NULL, "w+", console_log_func);
	if (log_stream == NULL)
		return -1;

	openlog(id, LOG_NDELAY | LOG_PID, facility);

	eal_log_set_default(log_stream);

	return 0;
}
