#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright 2018 Red Hat, Inc.
 */

#include <elf.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#if defined(__GLIBC__) && defined(__GLIBC_PREREQ)
#if __GLIBC_PREREQ(2, 16)
#include <sys/auxv.h>
#define HAS_AUXV 1
#endif
#endif

#include <rte_cpuflags.h>

#ifndef HAS_AUXV
static unsigned long
getauxval(unsigned long type __rte_unused)
{
	errno = ENOTSUP;
	return 0;
}
#endif

#ifdef RTE_ARCH_64
typedef Elf64_auxv_t Internal_Elfx_auxv_t;
#else
typedef Elf32_auxv_t Internal_Elfx_auxv_t;
#endif

/**
 * Provides a method for retrieving values from the auxiliary vector and
 * possibly running a string comparison.
 *
 * @return Always returns a result.  When the result is 0, check errno
 * to see if an error occurred during processing.
 */
static unsigned long
_rte_cpu_getauxval(unsigned long type, const char *str)
{
	unsigned long val;

	errno = 0;
	val = getauxval(type);

	if (!val && (errno == ENOTSUP || errno == ENOENT)) {
		int auxv_fd = open("/proc/self/auxv", O_RDONLY);
		Internal_Elfx_auxv_t auxv;

		if (auxv_fd == -1)
			return 0;

		errno = ENOENT;
		while (read(auxv_fd, &auxv, sizeof(auxv)) == sizeof(auxv)) {
			if (auxv.a_type == type) {
				errno = 0;
				val = auxv.a_un.a_val;
				if (str)
					val = strcmp((const char *)val, str);
				break;
			}
		}
		close(auxv_fd);
	}

	return val;
}

unsigned long
rte_cpu_getauxval(unsigned long type)
{
	return _rte_cpu_getauxval(type, NULL);
}

int
rte_cpu_strcmp_auxval(unsigned long type, const char *str)
{
	return _rte_cpu_getauxval(type, str);
}
