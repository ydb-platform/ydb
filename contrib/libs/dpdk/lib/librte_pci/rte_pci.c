#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation.
 * Copyright 2013-2014 6WIND S.A.
 */

#include <string.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/queue.h>

#include <rte_errno.h>
#include <rte_interrupts.h>
#include <rte_log.h>
#include <rte_bus.h>
#include <rte_eal_paging.h>
#include <rte_per_lcore.h>
#include <rte_memory.h>
#include <rte_eal.h>
#include <rte_string_fns.h>
#include <rte_common.h>
#include <rte_debug.h>

#include "rte_pci.h"

static inline const char *
get_u8_pciaddr_field(const char *in, void *_u8, char dlm)
{
	unsigned long val;
	uint8_t *u8 = _u8;
	char *end;

	/* empty string is an error though strtoul() returns 0 */
	if (*in == '\0')
		return NULL;

	/* PCI field starting with spaces is forbidden.
	 * Negative wrap-around is not reported as an error by strtoul.
	 */
	if (*in == ' ' || *in == '-')
		return NULL;

	errno = 0;
	val = strtoul(in, &end, 16);
	if (errno != 0 || end[0] != dlm || val > UINT8_MAX) {
		errno = errno ? errno : EINVAL;
		return NULL;
	}
	*u8 = (uint8_t)val;
	return end + 1;
}

static int
pci_bdf_parse(const char *input, struct rte_pci_addr *dev_addr)
{
	const char *in = input;

	dev_addr->domain = 0;
	in = get_u8_pciaddr_field(in, &dev_addr->bus, ':');
	if (in == NULL)
		return -EINVAL;
	in = get_u8_pciaddr_field(in, &dev_addr->devid, '.');
	if (in == NULL)
		return -EINVAL;
	in = get_u8_pciaddr_field(in, &dev_addr->function, '\0');
	if (in == NULL)
		return -EINVAL;
	return 0;
}

static int
pci_dbdf_parse(const char *input, struct rte_pci_addr *dev_addr)
{
	const char *in = input;
	unsigned long val;
	char *end;

	/* PCI id starting with spaces is forbidden.
	 * Negative wrap-around is not reported as an error by strtoul.
	 */
	if (*in == ' ' || *in == '-')
		return -EINVAL;

	errno = 0;
	val = strtoul(in, &end, 16);
	/* Empty string is not an error for strtoul, but the check
	 *   end[0] != ':'
	 * will detect the issue.
	 */
	if (errno != 0 || end[0] != ':' || val > UINT32_MAX)
		return -EINVAL;
	dev_addr->domain = (uint32_t)val;
	in = end + 1;
	in = get_u8_pciaddr_field(in, &dev_addr->bus, ':');
	if (in == NULL)
		return -EINVAL;
	in = get_u8_pciaddr_field(in, &dev_addr->devid, '.');
	if (in == NULL)
		return -EINVAL;
	in = get_u8_pciaddr_field(in, &dev_addr->function, '\0');
	if (in == NULL)
		return -EINVAL;
	return 0;
}

void
rte_pci_device_name(const struct rte_pci_addr *addr,
		char *output, size_t size)
{
	RTE_VERIFY(size >= PCI_PRI_STR_SIZE);
	RTE_VERIFY(snprintf(output, size, PCI_PRI_FMT,
			    addr->domain, addr->bus,
			    addr->devid, addr->function) >= 0);
}

int
rte_pci_addr_cmp(const struct rte_pci_addr *addr,
	     const struct rte_pci_addr *addr2)
{
	uint64_t dev_addr, dev_addr2;

	if ((addr == NULL) || (addr2 == NULL))
		return -1;

	dev_addr = ((uint64_t)addr->domain << 24) |
		(addr->bus << 16) | (addr->devid << 8) | addr->function;
	dev_addr2 = ((uint64_t)addr2->domain << 24) |
		(addr2->bus << 16) | (addr2->devid << 8) | addr2->function;

	if (dev_addr > dev_addr2)
		return 1;
	else if (dev_addr < dev_addr2)
		return -1;
	else
		return 0;
}

int
rte_pci_addr_parse(const char *str, struct rte_pci_addr *addr)
{
	if (pci_bdf_parse(str, addr) == 0 ||
	    pci_dbdf_parse(str, addr) == 0)
		return 0;
	return -1;
}
