#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#ifdef RTE_BACKTRACE
#include <execinfo.h>
#endif
#include <stdarg.h>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>

#include <rte_log.h>
#include <rte_debug.h>
#include <rte_common.h>
#include <rte_eal.h>

#define BACKTRACE_SIZE 256

/* dump the stack of the calling core */
void rte_dump_stack(void)
{
#ifdef RTE_BACKTRACE
	void *func[BACKTRACE_SIZE];
	char **symb = NULL;
	int size;

	size = backtrace(func, BACKTRACE_SIZE);
	symb = backtrace_symbols(func, size);

	if (symb == NULL)
		return;

	while (size > 0) {
		rte_log(RTE_LOG_ERR, RTE_LOGTYPE_EAL,
			"%d: [%s]\n", size, symb[size - 1]);
		size --;
	}

	free(symb);
#endif /* RTE_BACKTRACE */
}
