/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2017 Intel Corporation
 */

#ifndef _RTE_CONFIG_H_
#error "This file should only be included via rte_config.h"
#endif

/*
 * NOTE: these defines are for compatibility only and will be removed in a
 * future DPDK release.
 */

#ifdef RTE_LIBRTE_BITRATESTATS
#define RTE_LIBRTE_BITRATE
#endif

#ifdef RTE_LIBRTE_LATENCYSTATS
#define RTE_LIBRTE_LATENCY_STATS
#endif

#ifdef RTE_LIBRTE_DPAAX_COMMON
#define RTE_LIBRTE_COMMON_DPAAX
#endif

#ifdef RTE_LIBRTE_VMBUS_BUS
#define RTE_LIBRTE_VMBUS
#endif

#ifdef RTE_LIBRTE_BUCKET_MEMPOOL
#define RTE_DRIVER_MEMPOOL_BUCKET
#endif

#ifdef RTE_LIBRTE_RING_MEMPOOL
#define RTE_DRIVER_MEMPOOL_RING
#endif

#ifdef RTE_LIBRTE_STACK_MEMPOOL
#define RTE_DRIVER_MEMPOOL_STACK
#endif

#ifdef RTE_LIBRTE_AF_PACKET_PMD
#define RTE_LIBRTE_PMD_AF_PACKET
#endif

#ifdef RTE_LIBRTE_AF_XDP_PMD
#define RTE_LIBRTE_PMD_AF_XDP
#endif

#ifdef RTE_LIBRTE_BOND_PMD
#define RTE_LIBRTE_PMD_BOND
#endif

#ifdef RTE_LIBRTE_E1000_PMD
#define RTE_LIBRTE_EM_PMD
#endif

#ifdef RTE_LIBRTE_E1000_PMD
#define RTE_LIBRTE_IGB_PMD
#endif

#ifdef RTE_LIBRTE_FAILSAFE_PMD
#define RTE_LIBRTE_PMD_FAILSAFE
#endif

#ifdef RTE_LIBRTE_KNI_PMD
#define RTE_LIBRTE_PMD_KNI
#endif

#ifdef RTE_LIBRTE_LIQUIDIO_PMD
#define RTE_LIBRTE_LIO_PMD
#endif

#ifdef RTE_LIBRTE_MEMIF_PMD
#define RTE_LIBRTE_PMD_MEMIF
#endif

#ifdef RTE_LIBRTE_NULL_PMD
#define RTE_LIBRTE_PMD_NULL
#endif

#ifdef RTE_LIBRTE_PCAP_PMD
#define RTE_LIBRTE_PMD_PCAP
#endif

#ifdef RTE_LIBRTE_RING_PMD
#define RTE_LIBRTE_PMD_RING
#endif

#ifdef RTE_LIBRTE_SFC_PMD
#define RTE_LIBRTE_SFC_EFX_PMD
#endif

#ifdef RTE_LIBRTE_SOFTNIC_PMD
#define RTE_LIBRTE_PMD_SOFTNIC
#endif

#ifdef RTE_LIBRTE_SZEDATA2_PMD
#define RTE_LIBRTE_PMD_SZEDATA2
#endif

#ifdef RTE_LIBRTE_TAP_PMD
#define RTE_LIBRTE_PMD_TAP
#endif

#ifdef RTE_LIBRTE_THUNDERX_PMD
#define RTE_LIBRTE_THUNDERX_NICVF_PMD
#endif

#ifdef RTE_LIBRTE_VHOST_PMD
#define RTE_LIBRTE_PMD_VHOST
#endif

#ifdef RTE_LIBRTE_PMD_ARMV8
#define RTE_LIBRTE_PMD_ARMV8_CRYPTO
#endif

#ifdef RTE_LIBRTE_PMD_MVSAM
#define RTE_LIBRTE_PMD_MVSAM_CRYPTO
#endif

#ifdef RTE_LIBRTE_PMD_OCTEONTX_COMPRESS
#define RTE_LIBRTE_PMD_OCTEONTX_ZIPVF
#endif

#ifdef RTE_LIBRTE_PMD_OCTEONTX_EVENTDEV
#define RTE_LIBRTE_PMD_OCTEONTX_SSOVF
#endif

