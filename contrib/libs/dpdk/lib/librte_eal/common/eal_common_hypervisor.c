#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright 2017 Mellanox Technologies, Ltd
 */

#include "rte_hypervisor.h"

const char *
rte_hypervisor_get_name(enum rte_hypervisor id)
{
	switch (id) {
	case RTE_HYPERVISOR_NONE:
		return "none";
	case RTE_HYPERVISOR_KVM:
		return "KVM";
	case RTE_HYPERVISOR_HYPERV:
		return "Hyper-V";
	case RTE_HYPERVISOR_VMWARE:
		return "VMware";
	default:
		return "unknown";
	}
}
