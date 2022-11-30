/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2018 Intel Corporation
 */

/**
 * @file
 * Stores functions and path defines for files and directories
 * on the filesystem for Linux, that are used by the Linux EAL.
 */

#ifndef EAL_FILESYSTEM_H
#define EAL_FILESYSTEM_H

/** Path of rte config file. */

#include <stdint.h>
#include <limits.h>
#include <unistd.h>
#include <stdlib.h>

#include <rte_string_fns.h>
#include "eal_internal_cfg.h"

/* sets up platform-specific runtime data dir */
int
eal_create_runtime_dir(void);

int
eal_clean_runtime_dir(void);

/** Function to return hugefile prefix that's currently set up */
const char *
eal_get_hugefile_prefix(void);

#define RUNTIME_CONFIG_FNAME "config"
static inline const char *
eal_runtime_config_path(void)
{
	static char buffer[PATH_MAX]; /* static so auto-zeroed */

	snprintf(buffer, sizeof(buffer), "%s/%s", rte_eal_get_runtime_dir(),
			RUNTIME_CONFIG_FNAME);
	return buffer;
}

/** Path of primary/secondary communication unix socket file. */
#define MP_SOCKET_FNAME "mp_socket"
static inline const char *
eal_mp_socket_path(void)
{
	static char buffer[PATH_MAX]; /* static so auto-zeroed */

	snprintf(buffer, sizeof(buffer), "%s/%s", rte_eal_get_runtime_dir(),
			MP_SOCKET_FNAME);
	return buffer;
}

#define FBARRAY_NAME_FMT "%s/fbarray_%s"
static inline const char *
eal_get_fbarray_path(char *buffer, size_t buflen, const char *name) {
	snprintf(buffer, buflen, FBARRAY_NAME_FMT, rte_eal_get_runtime_dir(),
			name);
	return buffer;
}

/** Path of hugepage info file. */
#define HUGEPAGE_INFO_FNAME "hugepage_info"
static inline const char *
eal_hugepage_info_path(void)
{
	static char buffer[PATH_MAX]; /* static so auto-zeroed */

	snprintf(buffer, sizeof(buffer), "%s/%s", rte_eal_get_runtime_dir(),
			HUGEPAGE_INFO_FNAME);
	return buffer;
}

/** Path of hugepage data file. */
#define HUGEPAGE_DATA_FNAME "hugepage_data"
static inline const char *
eal_hugepage_data_path(void)
{
	static char buffer[PATH_MAX]; /* static so auto-zeroed */

	snprintf(buffer, sizeof(buffer), "%s/%s", rte_eal_get_runtime_dir(),
			HUGEPAGE_DATA_FNAME);
	return buffer;
}

/** String format for hugepage map files. */
#define HUGEFILE_FMT "%s/%smap_%d"
static inline const char *
eal_get_hugefile_path(char *buffer, size_t buflen, const char *hugedir, int f_id)
{
	snprintf(buffer, buflen, HUGEFILE_FMT, hugedir,
			eal_get_hugefile_prefix(), f_id);
	return buffer;
}

/** define the default filename prefix for the %s values above */
#define HUGEFILE_PREFIX_DEFAULT "rte"

/** Function to read a single numeric value from a file on the filesystem.
 * Used to read information from files on /sys */
int eal_parse_sysfs_value(const char *filename, unsigned long *val);

#endif /* EAL_FILESYSTEM_H */
