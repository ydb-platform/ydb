/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#ifndef EAL_VFIO_H_
#define EAL_VFIO_H_

#include <rte_common.h>

/*
 * determine if VFIO is present on the system
 */
#if !defined(VFIO_PRESENT) && defined(RTE_EAL_VFIO)
#include <linux/version.h>
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3, 6, 0)
#define VFIO_PRESENT
#else
#pragma message("VFIO configured but not supported by this kernel, disabling.")
#endif /* kernel version >= 3.6.0 */
#endif /* RTE_EAL_VFIO */

#ifdef VFIO_PRESENT

#include <stdint.h>
#include <linux/vfio.h>

#define RTE_VFIO_TYPE1 VFIO_TYPE1_IOMMU

#ifndef VFIO_SPAPR_TCE_v2_IOMMU
#define RTE_VFIO_SPAPR 7
#define VFIO_IOMMU_SPAPR_REGISTER_MEMORY _IO(VFIO_TYPE, VFIO_BASE + 17)
#define VFIO_IOMMU_SPAPR_UNREGISTER_MEMORY _IO(VFIO_TYPE, VFIO_BASE + 18)
#define VFIO_IOMMU_SPAPR_TCE_CREATE _IO(VFIO_TYPE, VFIO_BASE + 19)
#define VFIO_IOMMU_SPAPR_TCE_REMOVE _IO(VFIO_TYPE, VFIO_BASE + 20)

struct vfio_iommu_spapr_register_memory {
	uint32_t argsz;
	uint32_t flags;
	uint64_t vaddr;
	uint64_t size;
};

struct vfio_iommu_spapr_tce_create {
	uint32_t argsz;
	uint32_t flags;
	/* in */
	uint32_t page_shift;
	uint32_t __resv1;
	uint64_t window_size;
	uint32_t levels;
	uint32_t __resv2;
	/* out */
	uint64_t start_addr;
};

struct vfio_iommu_spapr_tce_remove {
	uint32_t argsz;
	uint32_t flags;
	/* in */
	uint64_t start_addr;
};

struct vfio_iommu_spapr_tce_ddw_info {
	uint64_t pgsizes;
	uint32_t max_dynamic_windows_supported;
	uint32_t levels;
};

/* SPAPR_v2 is not present, but SPAPR might be */
#ifndef VFIO_SPAPR_TCE_IOMMU
#define VFIO_IOMMU_SPAPR_TCE_GET_INFO _IO(VFIO_TYPE, VFIO_BASE + 12)

struct vfio_iommu_spapr_tce_info {
	uint32_t argsz;
	uint32_t flags;
	uint32_t dma32_window_start;
	uint32_t dma32_window_size;
	struct vfio_iommu_spapr_tce_ddw_info ddw;
};
#endif /* VFIO_SPAPR_TCE_IOMMU */

#else /* VFIO_SPAPR_TCE_v2_IOMMU */
#define RTE_VFIO_SPAPR VFIO_SPAPR_TCE_v2_IOMMU
#endif

#define VFIO_MAX_GROUPS RTE_MAX_VFIO_GROUPS
#define VFIO_MAX_CONTAINERS RTE_MAX_VFIO_CONTAINERS

/*
 * we don't need to store device fd's anywhere since they can be obtained from
 * the group fd via an ioctl() call.
 */
struct vfio_group {
	int group_num;
	int fd;
	int devices;
};

/* DMA mapping function prototype.
 * Takes VFIO container fd as a parameter.
 * Returns 0 on success, -1 on error.
 * */
typedef int (*vfio_dma_func_t)(int);

/* Custom memory region DMA mapping function prototype.
 * Takes VFIO container fd, virtual address, phisical address, length and
 * operation type (0 to unmap 1 for map) as a parameters.
 * Returns 0 on success, -1 on error.
 **/
typedef int (*vfio_dma_user_func_t)(int fd, uint64_t vaddr, uint64_t iova,
		uint64_t len, int do_map);

struct vfio_iommu_type {
	int type_id;
	const char *name;
	vfio_dma_user_func_t dma_user_map_func;
	vfio_dma_func_t dma_map_func;
};

/* get the vfio container that devices are bound to by default */
int vfio_get_default_container_fd(void);

/* pick IOMMU type. returns a pointer to vfio_iommu_type or NULL for error */
const struct vfio_iommu_type *
vfio_set_iommu_type(int vfio_container_fd);

int
vfio_get_iommu_type(void);

/* check if we have any supported extensions */
int
vfio_has_supported_extensions(int vfio_container_fd);

int vfio_mp_sync_setup(void);

#define EAL_VFIO_MP "eal_vfio_mp_sync"

#define SOCKET_REQ_CONTAINER 0x100
#define SOCKET_REQ_GROUP 0x200
#define SOCKET_REQ_DEFAULT_CONTAINER 0x400
#define SOCKET_REQ_IOMMU_TYPE 0x800
#define SOCKET_OK 0x0
#define SOCKET_NO_FD 0x1
#define SOCKET_ERR 0xFF

struct vfio_mp_param {
	int req;
	int result;
	RTE_STD_C11
	union {
		int group_num;
		int iommu_type_id;
	};
};

#endif /* VFIO_PRESENT */

#endif /* EAL_VFIO_H_ */
