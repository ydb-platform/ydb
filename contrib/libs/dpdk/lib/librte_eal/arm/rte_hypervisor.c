/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright 2017 Mellanox Technologies, Ltd
 */

#include "rte_hypervisor.h"

enum rte_hypervisor
rte_hypervisor_get(void)
{
	return RTE_HYPERVISOR_UNKNOWN;
}
