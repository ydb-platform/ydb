/* SPDX-License-Identifier: (BSD-3-Clause OR GPL-2.0)
 * Copyright(c) 2010-2014 Intel Corporation
 */

#ifndef _RTE_PCI_DEV_DEFS_H_
#define _RTE_PCI_DEV_DEFS_H_

/* interrupt mode */
enum rte_intr_mode {
	RTE_INTR_MODE_NONE = 0,
	RTE_INTR_MODE_LEGACY,
	RTE_INTR_MODE_MSI,
	RTE_INTR_MODE_MSIX
};

#endif /* _RTE_PCI_DEV_DEFS_H_ */
