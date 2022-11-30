/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright 2017 Mellanox Technologies, Ltd
 */

#ifndef RTE_HYPERVISOR_H
#define RTE_HYPERVISOR_H

/**
 * @file
 * Hypervisor awareness.
 */

enum rte_hypervisor {
	RTE_HYPERVISOR_NONE,
	RTE_HYPERVISOR_KVM,
	RTE_HYPERVISOR_HYPERV,
	RTE_HYPERVISOR_VMWARE,
	RTE_HYPERVISOR_UNKNOWN
};

/**
 * Get the id of hypervisor it is running on.
 */
enum rte_hypervisor
rte_hypervisor_get(void);

/**
 * Get the name of a given hypervisor id.
 */
const char *
rte_hypervisor_get_name(enum rte_hypervisor id);

#endif /* RTE_HYPERVISOR_H */
