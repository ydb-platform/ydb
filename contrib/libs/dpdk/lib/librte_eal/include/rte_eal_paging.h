/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2020 Dmitry Kozlyuk
 */

#include <stdint.h>

#include <rte_compat.h>

/**
 * @file
 * @internal
 *
 * Wrappers for OS facilities related to memory paging, used across DPDK.
 */

/** Memory protection flags. */
enum rte_mem_prot {
	RTE_PROT_READ = 1 << 0,   /**< Read access. */
	RTE_PROT_WRITE = 1 << 1,  /**< Write access. */
	RTE_PROT_EXECUTE = 1 << 2 /**< Code execution. */
};

/** Additional flags for memory mapping. */
enum rte_map_flags {
	/** Changes to the mapped memory are visible to other processes. */
	RTE_MAP_SHARED = 1 << 0,
	/** Mapping is not backed by a regular file. */
	RTE_MAP_ANONYMOUS = 1 << 1,
	/** Copy-on-write mapping, changes are invisible to other processes. */
	RTE_MAP_PRIVATE = 1 << 2,
	/**
	 * Force mapping to the requested address. This flag should be used
	 * with caution, because to fulfill the request implementation
	 * may remove all other mappings in the requested region. However,
	 * it is not required to do so, thus mapping with this flag may fail.
	 */
	RTE_MAP_FORCE_ADDRESS = 1 << 3
};

/**
 * Map a portion of an opened file or the page file into memory.
 *
 * This function is similar to POSIX mmap(3) with common MAP_ANONYMOUS
 * extension, except for the return value.
 *
 * @param requested_addr
 *  Desired virtual address for mapping. Can be NULL to let OS choose.
 * @param size
 *  Size of the mapping in bytes.
 * @param prot
 *  Protection flags, a combination of rte_mem_prot values.
 * @param flags
 *  Additional mapping flags, a combination of rte_map_flags.
 * @param fd
 *  Mapped file descriptor. Can be negative for anonymous mapping.
 * @param offset
 *  Offset of the mapped region in fd. Must be 0 for anonymous mappings.
 * @return
 *  Mapped address or NULL on failure and rte_errno is set to OS error.
 */
__rte_internal
void *
rte_mem_map(void *requested_addr, size_t size, int prot, int flags,
	int fd, size_t offset);

/**
 * OS-independent implementation of POSIX munmap(3).
 */
__rte_internal
int
rte_mem_unmap(void *virt, size_t size);

/**
 * Get system page size. This function never fails.
 *
 * @return
 *   Page size in bytes.
 */
__rte_internal
size_t
rte_mem_page_size(void);

/**
 * Lock in physical memory all pages crossed by the address region.
 *
 * @param virt
 *   Base virtual address of the region.
 * @param size
 *   Size of the region.
 * @return
 *   0 on success, negative on error.
 *
 * @see rte_mem_page_size() to retrieve the page size.
 * @see rte_mem_lock_page() to lock an entire single page.
 */
__rte_internal
int
rte_mem_lock(const void *virt, size_t size);
