#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2020 Mellanox Technologies, Ltd
 */
#include <string.h>

#include <rte_os.h>
#include <rte_string_fns.h>

#include "eal_private.h"
#include "eal_memcfg.h"

/* early configuration structure, when memory config is not mmapped */
static struct rte_mem_config early_mem_config;

/* Address of global and public configuration */
static struct rte_config rte_config = {
	.mem_config = &early_mem_config,
};

/* platform-specific runtime dir */
static char runtime_dir[PATH_MAX];

/* internal configuration */
static struct internal_config internal_config;

const char *
rte_eal_get_runtime_dir(void)
{
	return runtime_dir;
}

int
eal_set_runtime_dir(char *run_dir, size_t size)
{
	size_t str_size;

	str_size = strlcpy(runtime_dir, run_dir, size);
	if (str_size >= size) {
		RTE_LOG(ERR, EAL, "Runtime directory string too long\n");
		return -1;
	}

	return 0;
}

/* Return a pointer to the configuration structure */
struct rte_config *
rte_eal_get_configuration(void)
{
	return &rte_config;
}

/* Return a pointer to the internal configuration structure */
struct internal_config *
eal_get_internal_configuration(void)
{
	return &internal_config;
}

enum rte_iova_mode
rte_eal_iova_mode(void)
{
	return rte_eal_get_configuration()->iova_mode;
}

enum rte_proc_type_t
rte_eal_process_type(void)
{
	return rte_config.process_type;
}

/* Return user provided mbuf pool ops name */
const char *
rte_eal_mbuf_user_pool_ops(void)
{
	return internal_config.user_mbuf_pool_ops_name;
}

/* return non-zero if hugepages are enabled. */
int
rte_eal_has_hugepages(void)
{
	return !internal_config.no_hugetlbfs;
}

int
rte_eal_has_pci(void)
{
	return !internal_config.no_pci;
}
