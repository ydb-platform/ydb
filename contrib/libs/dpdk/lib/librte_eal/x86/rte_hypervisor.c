#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright 2017 Mellanox Technologies, Ltd
 */

#include "rte_hypervisor.h"

#include <stdint.h>
#include <string.h>

#include "rte_cpuflags.h"
#include "rte_cpuid.h"

/* See http://lwn.net/Articles/301888/ */
#define HYPERVISOR_INFO_LEAF 0x40000000

enum rte_hypervisor
rte_hypervisor_get(void)
{
	cpuid_registers_t regs;
	int reg;
	char name[13];

	if (!rte_cpu_get_flag_enabled(RTE_CPUFLAG_HYPERVISOR))
		return RTE_HYPERVISOR_NONE;

	__cpuid(HYPERVISOR_INFO_LEAF,
			regs[RTE_REG_EAX], regs[RTE_REG_EBX],
			regs[RTE_REG_ECX], regs[RTE_REG_EDX]);
	for (reg = 1; reg < 4; reg++)
		memcpy(name + (reg - 1) * 4, &regs[reg], 4);
	name[12] = '\0';

	if (strcmp("KVMKVMKVM", name) == 0)
		return RTE_HYPERVISOR_KVM;
	if (strcmp("Microsoft Hv", name) == 0)
		return RTE_HYPERVISOR_HYPERV;
	if (strcmp("VMwareVMware", name) == 0)
		return RTE_HYPERVISOR_VMWARE;
	return RTE_HYPERVISOR_UNKNOWN;
}
