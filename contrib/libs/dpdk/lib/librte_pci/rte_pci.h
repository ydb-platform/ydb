/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2015 Intel Corporation.
 * Copyright 2013-2014 6WIND S.A.
 */

#ifndef _RTE_PCI_H_
#define _RTE_PCI_H_

/**
 * @file
 *
 * RTE PCI Library
 */

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <limits.h>
#include <sys/queue.h>
#include <inttypes.h>
#include <sys/types.h>

/*
 * Conventional PCI and PCI-X Mode 1 devices have 256 bytes of
 * configuration space.  PCI-X Mode 2 and PCIe devices have 4096 bytes of
 * configuration space.
 */
#define RTE_PCI_CFG_SPACE_SIZE		256
#define RTE_PCI_CFG_SPACE_EXP_SIZE	4096

#define RTE_PCI_VENDOR_ID	0x00	/* 16 bits */
#define RTE_PCI_DEVICE_ID	0x02	/* 16 bits */

/* PCI Express capability registers */
#define RTE_PCI_EXP_DEVCTL	8	/* Device Control */

/* Extended Capabilities (PCI-X 2.0 and Express) */
#define RTE_PCI_EXT_CAP_ID(header)	(header & 0x0000ffff)
#define RTE_PCI_EXT_CAP_NEXT(header)	((header >> 20) & 0xffc)

#define RTE_PCI_EXT_CAP_ID_ERR		0x01	/* Advanced Error Reporting */
#define RTE_PCI_EXT_CAP_ID_DSN		0x03	/* Device Serial Number */
#define RTE_PCI_EXT_CAP_ID_SRIOV	0x10	/* SR-IOV*/

/* Single Root I/O Virtualization */
#define RTE_PCI_SRIOV_CAP		0x04	/* SR-IOV Capabilities */
#define RTE_PCI_SRIOV_CTRL		0x08	/* SR-IOV Control */
#define RTE_PCI_SRIOV_INITIAL_VF	0x0c	/* Initial VFs */
#define RTE_PCI_SRIOV_TOTAL_VF		0x0e	/* Total VFs */
#define RTE_PCI_SRIOV_NUM_VF		0x10	/* Number of VFs */
#define RTE_PCI_SRIOV_FUNC_LINK		0x12	/* Function Dependency Link */
#define RTE_PCI_SRIOV_VF_OFFSET		0x14	/* First VF Offset */
#define RTE_PCI_SRIOV_VF_STRIDE		0x16	/* Following VF Stride */
#define RTE_PCI_SRIOV_VF_DID		0x1a	/* VF Device ID */
#define RTE_PCI_SRIOV_SUP_PGSIZE	0x1c	/* Supported Page Sizes */

/** Formatting string for PCI device identifier: Ex: 0000:00:01.0 */
#define PCI_PRI_FMT "%.4" PRIx32 ":%.2" PRIx8 ":%.2" PRIx8 ".%" PRIx8
#define PCI_PRI_STR_SIZE sizeof("XXXXXXXX:XX:XX.X")

/** Short formatting string, without domain, for PCI device: Ex: 00:01.0 */
#define PCI_SHORT_PRI_FMT "%.2" PRIx8 ":%.2" PRIx8 ".%" PRIx8

/** Nb. of values in PCI device identifier format string. */
#define PCI_FMT_NVAL 4

/** Nb. of values in PCI resource format. */
#define PCI_RESOURCE_FMT_NVAL 3

/** Maximum number of PCI resources. */
#define PCI_MAX_RESOURCE 6

/**
 * A structure describing an ID for a PCI driver. Each driver provides a
 * table of these IDs for each device that it supports.
 */
struct rte_pci_id {
	uint32_t class_id;            /**< Class ID or RTE_CLASS_ANY_ID. */
	uint16_t vendor_id;           /**< Vendor ID or PCI_ANY_ID. */
	uint16_t device_id;           /**< Device ID or PCI_ANY_ID. */
	uint16_t subsystem_vendor_id; /**< Subsystem vendor ID or PCI_ANY_ID. */
	uint16_t subsystem_device_id; /**< Subsystem device ID or PCI_ANY_ID. */
};

/**
 * A structure describing the location of a PCI device.
 */
struct rte_pci_addr {
	uint32_t domain;                /**< Device domain */
	uint8_t bus;                    /**< Device bus */
	uint8_t devid;                  /**< Device ID */
	uint8_t function;               /**< Device function. */
};

/** Any PCI device identifier (vendor, device, ...) */
#define PCI_ANY_ID (0xffff)
#define RTE_CLASS_ANY_ID (0xffffff)

/**
 * Utility function to write a pci device name, this device name can later be
 * used to retrieve the corresponding rte_pci_addr using eal_parse_pci_*
 * BDF helpers.
 *
 * @param addr
 *	The PCI Bus-Device-Function address
 * @param output
 *	The output buffer string
 * @param size
 *	The output buffer size
 */
void rte_pci_device_name(const struct rte_pci_addr *addr,
		     char *output, size_t size);

/**
 * Utility function to compare two PCI device addresses.
 *
 * @param addr
 *	The PCI Bus-Device-Function address to compare
 * @param addr2
 *	The PCI Bus-Device-Function address to compare
 * @return
 *	0 on equal PCI address.
 *	Positive on addr is greater than addr2.
 *	Negative on addr is less than addr2, or error.
 */
int rte_pci_addr_cmp(const struct rte_pci_addr *addr,
		     const struct rte_pci_addr *addr2);


/**
 * Utility function to parse a string into a PCI location.
 *
 * @param str
 *	The string to parse
 * @param addr
 *	The reference to the structure where the location
 *	is stored.
 * @return
 *	0 on success
 *	<0 otherwise
 */
int rte_pci_addr_parse(const char *str, struct rte_pci_addr *addr);

#ifdef __cplusplus
}
#endif

#endif /* _RTE_PCI_H_ */
