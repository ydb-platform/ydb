/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#ifndef EAL_PCI_INIT_H_
#define EAL_PCI_INIT_H_

#include <rte_vfio.h>

#include "private.h"

/** IO resource type: */
#define IORESOURCE_IO         0x00000100
#define IORESOURCE_MEM        0x00000200

/*
 * Helper function to map PCI resources right after hugepages in virtual memory
 */
extern void *pci_map_addr;
void *pci_find_max_end_va(void);

/* parse one line of the "resource" sysfs file (note that the 'line'
 * string is modified)
 */
int pci_parse_one_sysfs_resource(char *line, size_t len, uint64_t *phys_addr,
	uint64_t *end_addr, uint64_t *flags);

int pci_uio_alloc_resource(struct rte_pci_device *dev,
		struct mapped_pci_resource **uio_res);
void pci_uio_free_resource(struct rte_pci_device *dev,
		struct mapped_pci_resource *uio_res);
int pci_uio_map_resource_by_index(struct rte_pci_device *dev, int res_idx,
		struct mapped_pci_resource *uio_res, int map_idx);

int pci_uio_read_config(const struct rte_intr_handle *intr_handle,
			void *buf, size_t len, off_t offs);
int pci_uio_write_config(const struct rte_intr_handle *intr_handle,
			 const void *buf, size_t len, off_t offs);

int pci_uio_ioport_map(struct rte_pci_device *dev, int bar,
		       struct rte_pci_ioport *p);
void pci_uio_ioport_read(struct rte_pci_ioport *p,
			 void *data, size_t len, off_t offset);
void pci_uio_ioport_write(struct rte_pci_ioport *p,
			  const void *data, size_t len, off_t offset);
int pci_uio_ioport_unmap(struct rte_pci_ioport *p);

#ifdef VFIO_PRESENT

#ifdef PCI_MSIX_TABLE_BIR
#define RTE_PCI_MSIX_TABLE_BIR    PCI_MSIX_TABLE_BIR
#else
#define RTE_PCI_MSIX_TABLE_BIR    0x7
#endif

#ifdef PCI_MSIX_TABLE_OFFSET
#define RTE_PCI_MSIX_TABLE_OFFSET PCI_MSIX_TABLE_OFFSET
#else
#define RTE_PCI_MSIX_TABLE_OFFSET 0xfffffff8
#endif

#ifdef PCI_MSIX_FLAGS_QSIZE
#define RTE_PCI_MSIX_FLAGS_QSIZE  PCI_MSIX_FLAGS_QSIZE
#else
#define RTE_PCI_MSIX_FLAGS_QSIZE  0x07ff
#endif

/* access config space */
int pci_vfio_read_config(const struct rte_intr_handle *intr_handle,
			 void *buf, size_t len, off_t offs);
int pci_vfio_write_config(const struct rte_intr_handle *intr_handle,
			  const void *buf, size_t len, off_t offs);

int pci_vfio_ioport_map(struct rte_pci_device *dev, int bar,
		        struct rte_pci_ioport *p);
void pci_vfio_ioport_read(struct rte_pci_ioport *p,
			  void *data, size_t len, off_t offset);
void pci_vfio_ioport_write(struct rte_pci_ioport *p,
			   const void *data, size_t len, off_t offset);
int pci_vfio_ioport_unmap(struct rte_pci_ioport *p);

/* map/unmap VFIO resource prototype */
int pci_vfio_map_resource(struct rte_pci_device *dev);
int pci_vfio_unmap_resource(struct rte_pci_device *dev);

int pci_vfio_is_enabled(void);

#endif

#endif /* EAL_PCI_INIT_H_ */
