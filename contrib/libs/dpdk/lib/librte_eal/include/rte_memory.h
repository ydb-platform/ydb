/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#ifndef _RTE_MEMORY_H_
#define _RTE_MEMORY_H_

/**
 * @file
 *
 * Memory-related RTE API.
 */

#include <stdint.h>
#include <stddef.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

#include <rte_common.h>
#include <rte_compat.h>
#include <rte_config.h>
#include <rte_fbarray.h>

#define RTE_PGSIZE_4K   (1ULL << 12)
#define RTE_PGSIZE_64K  (1ULL << 16)
#define RTE_PGSIZE_256K (1ULL << 18)
#define RTE_PGSIZE_2M   (1ULL << 21)
#define RTE_PGSIZE_16M  (1ULL << 24)
#define RTE_PGSIZE_256M (1ULL << 28)
#define RTE_PGSIZE_512M (1ULL << 29)
#define RTE_PGSIZE_1G   (1ULL << 30)
#define RTE_PGSIZE_4G   (1ULL << 32)
#define RTE_PGSIZE_16G  (1ULL << 34)

#define SOCKET_ID_ANY -1                    /**< Any NUMA socket. */

/**
 * Physical memory segment descriptor.
 */
#define RTE_MEMSEG_FLAG_DO_NOT_FREE (1 << 0)
/**< Prevent this segment from being freed back to the OS. */
struct rte_memseg {
	rte_iova_t iova;            /**< Start IO address. */
	RTE_STD_C11
	union {
		void *addr;         /**< Start virtual address. */
		uint64_t addr_64;   /**< Makes sure addr is always 64 bits */
	};
	size_t len;               /**< Length of the segment. */
	uint64_t hugepage_sz;       /**< The pagesize of underlying memory */
	int32_t socket_id;          /**< NUMA socket ID. */
	uint32_t nchannel;          /**< Number of channels. */
	uint32_t nrank;             /**< Number of ranks. */
	uint32_t flags;             /**< Memseg-specific flags */
} __rte_packed;

/**
 * memseg list is a special case as we need to store a bunch of other data
 * together with the array itself.
 */
struct rte_memseg_list {
	RTE_STD_C11
	union {
		void *base_va;
		/**< Base virtual address for this memseg list. */
		uint64_t addr_64;
		/**< Makes sure addr is always 64-bits */
	};
	uint64_t page_sz; /**< Page size for all memsegs in this list. */
	int socket_id; /**< Socket ID for all memsegs in this list. */
	volatile uint32_t version; /**< version number for multiprocess sync. */
	size_t len; /**< Length of memory area covered by this memseg list. */
	unsigned int external; /**< 1 if this list points to external memory */
	unsigned int heap; /**< 1 if this list points to a heap */
	struct rte_fbarray memseg_arr;
};

/**
 * Lock page in physical memory and prevent from swapping.
 *
 * @param virt
 *   The virtual address.
 * @return
 *   0 on success, negative on error.
 */
int rte_mem_lock_page(const void *virt);

/**
 * Get physical address of any mapped virtual address in the current process.
 * It is found by browsing the /proc/self/pagemap special file.
 * The page must be locked.
 *
 * @param virt
 *   The virtual address.
 * @return
 *   The physical address or RTE_BAD_IOVA on error.
 */
phys_addr_t rte_mem_virt2phy(const void *virt);

/**
 * Get IO virtual address of any mapped virtual address in the current process.
 *
 * @note This function will not check internal page table. Instead, in IOVA as
 *       PA mode, it will fall back to getting real physical address (which may
 *       not match the expected IOVA, such as what was specified for external
 *       memory).
 *
 * @param virt
 *   The virtual address.
 * @return
 *   The IO address or RTE_BAD_IOVA on error.
 */
rte_iova_t rte_mem_virt2iova(const void *virt);

/**
 * Get virtual memory address corresponding to iova address.
 *
 * @note This function read-locks the memory hotplug subsystem, and thus cannot
 *       be used within memory-related callback functions.
 *
 * @param iova
 *   The iova address.
 * @return
 *   Virtual address corresponding to iova address (or NULL if address does not
 *   exist within DPDK memory map).
 */
__rte_experimental
void *
rte_mem_iova2virt(rte_iova_t iova);

/**
 * Get memseg to which a particular virtual address belongs.
 *
 * @param virt
 *   The virtual address.
 * @param msl
 *   The memseg list in which to look up based on ``virt`` address
 *   (can be NULL).
 * @return
 *   Memseg pointer on success, or NULL on error.
 */
__rte_experimental
struct rte_memseg *
rte_mem_virt2memseg(const void *virt, const struct rte_memseg_list *msl);

/**
 * Get memseg list corresponding to virtual memory address.
 *
 * @param virt
 *   The virtual address.
 * @return
 *   Memseg list to which this virtual address belongs to.
 */
__rte_experimental
struct rte_memseg_list *
rte_mem_virt2memseg_list(const void *virt);

/**
 * Memseg walk function prototype.
 *
 * Returning 0 will continue walk
 * Returning 1 will stop the walk
 * Returning -1 will stop the walk and report error
 */
typedef int (*rte_memseg_walk_t)(const struct rte_memseg_list *msl,
		const struct rte_memseg *ms, void *arg);

/**
 * Memseg contig walk function prototype. This will trigger a callback on every
 * VA-contiguous area starting at memseg ``ms``, so total valid VA space at each
 * callback call will be [``ms->addr``, ``ms->addr + len``).
 *
 * Returning 0 will continue walk
 * Returning 1 will stop the walk
 * Returning -1 will stop the walk and report error
 */
typedef int (*rte_memseg_contig_walk_t)(const struct rte_memseg_list *msl,
		const struct rte_memseg *ms, size_t len, void *arg);

/**
 * Memseg list walk function prototype. This will trigger a callback on every
 * allocated memseg list.
 *
 * Returning 0 will continue walk
 * Returning 1 will stop the walk
 * Returning -1 will stop the walk and report error
 */
typedef int (*rte_memseg_list_walk_t)(const struct rte_memseg_list *msl,
		void *arg);

/**
 * Walk list of all memsegs.
 *
 * @note This function read-locks the memory hotplug subsystem, and thus cannot
 *       be used within memory-related callback functions.
 *
 * @note This function will also walk through externally allocated segments. It
 *       is up to the user to decide whether to skip through these segments.
 *
 * @param func
 *   Iterator function
 * @param arg
 *   Argument passed to iterator
 * @return
 *   0 if walked over the entire list
 *   1 if stopped by the user
 *   -1 if user function reported error
 */
__rte_experimental
int
rte_memseg_walk(rte_memseg_walk_t func, void *arg);

/**
 * Walk each VA-contiguous area.
 *
 * @note This function read-locks the memory hotplug subsystem, and thus cannot
 *       be used within memory-related callback functions.
 *
 * @note This function will also walk through externally allocated segments. It
 *       is up to the user to decide whether to skip through these segments.
 *
 * @param func
 *   Iterator function
 * @param arg
 *   Argument passed to iterator
 * @return
 *   0 if walked over the entire list
 *   1 if stopped by the user
 *   -1 if user function reported error
 */
__rte_experimental
int
rte_memseg_contig_walk(rte_memseg_contig_walk_t func, void *arg);

/**
 * Walk each allocated memseg list.
 *
 * @note This function read-locks the memory hotplug subsystem, and thus cannot
 *       be used within memory-related callback functions.
 *
 * @note This function will also walk through externally allocated segments. It
 *       is up to the user to decide whether to skip through these segments.
 *
 * @param func
 *   Iterator function
 * @param arg
 *   Argument passed to iterator
 * @return
 *   0 if walked over the entire list
 *   1 if stopped by the user
 *   -1 if user function reported error
 */
__rte_experimental
int
rte_memseg_list_walk(rte_memseg_list_walk_t func, void *arg);

/**
 * Walk list of all memsegs without performing any locking.
 *
 * @note This function does not perform any locking, and is only safe to call
 *       from within memory-related callback functions.
 *
 * @param func
 *   Iterator function
 * @param arg
 *   Argument passed to iterator
 * @return
 *   0 if walked over the entire list
 *   1 if stopped by the user
 *   -1 if user function reported error
 */
__rte_experimental
int
rte_memseg_walk_thread_unsafe(rte_memseg_walk_t func, void *arg);

/**
 * Walk each VA-contiguous area without performing any locking.
 *
 * @note This function does not perform any locking, and is only safe to call
 *       from within memory-related callback functions.
 *
 * @param func
 *   Iterator function
 * @param arg
 *   Argument passed to iterator
 * @return
 *   0 if walked over the entire list
 *   1 if stopped by the user
 *   -1 if user function reported error
 */
__rte_experimental
int
rte_memseg_contig_walk_thread_unsafe(rte_memseg_contig_walk_t func, void *arg);

/**
 * Walk each allocated memseg list without performing any locking.
 *
 * @note This function does not perform any locking, and is only safe to call
 *       from within memory-related callback functions.
 *
 * @param func
 *   Iterator function
 * @param arg
 *   Argument passed to iterator
 * @return
 *   0 if walked over the entire list
 *   1 if stopped by the user
 *   -1 if user function reported error
 */
__rte_experimental
int
rte_memseg_list_walk_thread_unsafe(rte_memseg_list_walk_t func, void *arg);

/**
 * Return file descriptor associated with a particular memseg (if available).
 *
 * @note This function read-locks the memory hotplug subsystem, and thus cannot
 *       be used within memory-related callback functions.
 *
 * @note This returns an internal file descriptor. Performing any operations on
 *       this file descriptor is inherently dangerous, so it should be treated
 *       as read-only for all intents and purposes.
 *
 * @param ms
 *   A pointer to memseg for which to get file descriptor.
 *
 * @return
 *   Valid file descriptor in case of success.
 *   -1 in case of error, with ``rte_errno`` set to the following values:
 *     - EINVAL  - ``ms`` pointer was NULL or did not point to a valid memseg
 *     - ENODEV  - ``ms`` fd is not available
 *     - ENOENT  - ``ms`` is an unused segment
 *     - ENOTSUP - segment fd's are not supported
 */
__rte_experimental
int
rte_memseg_get_fd(const struct rte_memseg *ms);

/**
 * Return file descriptor associated with a particular memseg (if available).
 *
 * @note This function does not perform any locking, and is only safe to call
 *       from within memory-related callback functions.
 *
 * @note This returns an internal file descriptor. Performing any operations on
 *       this file descriptor is inherently dangerous, so it should be treated
 *       as read-only for all intents and purposes.
 *
 * @param ms
 *   A pointer to memseg for which to get file descriptor.
 *
 * @return
 *   Valid file descriptor in case of success.
 *   -1 in case of error, with ``rte_errno`` set to the following values:
 *     - EINVAL  - ``ms`` pointer was NULL or did not point to a valid memseg
 *     - ENODEV  - ``ms`` fd is not available
 *     - ENOENT  - ``ms`` is an unused segment
 *     - ENOTSUP - segment fd's are not supported
 */
__rte_experimental
int
rte_memseg_get_fd_thread_unsafe(const struct rte_memseg *ms);

/**
 * Get offset into segment file descriptor associated with a particular memseg
 * (if available).
 *
 * @note This function read-locks the memory hotplug subsystem, and thus cannot
 *       be used within memory-related callback functions.
 *
 * @param ms
 *   A pointer to memseg for which to get file descriptor.
 * @param offset
 *   A pointer to offset value where the result will be stored.
 *
 * @return
 *   Valid file descriptor in case of success.
 *   -1 in case of error, with ``rte_errno`` set to the following values:
 *     - EINVAL  - ``ms`` pointer was NULL or did not point to a valid memseg
 *     - EINVAL  - ``offset`` pointer was NULL
 *     - ENODEV  - ``ms`` fd is not available
 *     - ENOENT  - ``ms`` is an unused segment
 *     - ENOTSUP - segment fd's are not supported
 */
__rte_experimental
int
rte_memseg_get_fd_offset(const struct rte_memseg *ms, size_t *offset);

/**
 * Get offset into segment file descriptor associated with a particular memseg
 * (if available).
 *
 * @note This function does not perform any locking, and is only safe to call
 *       from within memory-related callback functions.
 *
 * @param ms
 *   A pointer to memseg for which to get file descriptor.
 * @param offset
 *   A pointer to offset value where the result will be stored.
 *
 * @return
 *   Valid file descriptor in case of success.
 *   -1 in case of error, with ``rte_errno`` set to the following values:
 *     - EINVAL  - ``ms`` pointer was NULL or did not point to a valid memseg
 *     - EINVAL  - ``offset`` pointer was NULL
 *     - ENODEV  - ``ms`` fd is not available
 *     - ENOENT  - ``ms`` is an unused segment
 *     - ENOTSUP - segment fd's are not supported
 */
__rte_experimental
int
rte_memseg_get_fd_offset_thread_unsafe(const struct rte_memseg *ms,
		size_t *offset);

/**
 * @warning
 * @b EXPERIMENTAL: this API may change without prior notice
 *
 * Register external memory chunk with DPDK.
 *
 * @note Using this API is mutually exclusive with ``rte_malloc`` family of
 *   API's.
 *
 * @note This API will not perform any DMA mapping. It is expected that user
 *   will do that themselves.
 *
 * @note Before accessing this memory in other processes, it needs to be
 *   attached in each of those processes by calling ``rte_extmem_attach`` in
 *   each other process.
 *
 * @param va_addr
 *   Start of virtual area to register. Must be aligned by ``page_sz``.
 * @param len
 *   Length of virtual area to register. Must be aligned by ``page_sz``.
 * @param iova_addrs
 *   Array of page IOVA addresses corresponding to each page in this memory
 *   area. Can be NULL, in which case page IOVA addresses will be set to
 *   RTE_BAD_IOVA.
 * @param n_pages
 *   Number of elements in the iova_addrs array. Ignored if  ``iova_addrs``
 *   is NULL.
 * @param page_sz
 *   Page size of the underlying memory
 *
 * @return
 *   - 0 on success
 *   - -1 in case of error, with rte_errno set to one of the following:
 *     EINVAL - one of the parameters was invalid
 *     EEXIST - memory chunk is already registered
 *     ENOSPC - no more space in internal config to store a new memory chunk
 */
__rte_experimental
int
rte_extmem_register(void *va_addr, size_t len, rte_iova_t iova_addrs[],
		unsigned int n_pages, size_t page_sz);

/**
 * @warning
 * @b EXPERIMENTAL: this API may change without prior notice
 *
 * Unregister external memory chunk with DPDK.
 *
 * @note Using this API is mutually exclusive with ``rte_malloc`` family of
 *   API's.
 *
 * @note This API will not perform any DMA unmapping. It is expected that user
 *   will do that themselves.
 *
 * @note Before calling this function, all other processes must call
 *   ``rte_extmem_detach`` to detach from the memory area.
 *
 * @param va_addr
 *   Start of virtual area to unregister
 * @param len
 *   Length of virtual area to unregister
 *
 * @return
 *   - 0 on success
 *   - -1 in case of error, with rte_errno set to one of the following:
 *     EINVAL - one of the parameters was invalid
 *     ENOENT - memory chunk was not found
 */
__rte_experimental
int
rte_extmem_unregister(void *va_addr, size_t len);

/**
 * @warning
 * @b EXPERIMENTAL: this API may change without prior notice
 *
 * Attach to external memory chunk registered in another process.
 *
 * @note Using this API is mutually exclusive with ``rte_malloc`` family of
 *   API's.
 *
 * @note This API will not perform any DMA mapping. It is expected that user
 *   will do that themselves.
 *
 * @param va_addr
 *   Start of virtual area to register
 * @param len
 *   Length of virtual area to register
 *
 * @return
 *   - 0 on success
 *   - -1 in case of error, with rte_errno set to one of the following:
 *     EINVAL - one of the parameters was invalid
 *     ENOENT - memory chunk was not found
 */
__rte_experimental
int
rte_extmem_attach(void *va_addr, size_t len);

/**
 * @warning
 * @b EXPERIMENTAL: this API may change without prior notice
 *
 * Detach from external memory chunk registered in another process.
 *
 * @note Using this API is mutually exclusive with ``rte_malloc`` family of
 *   API's.
 *
 * @note This API will not perform any DMA unmapping. It is expected that user
 *   will do that themselves.
 *
 * @param va_addr
 *   Start of virtual area to unregister
 * @param len
 *   Length of virtual area to unregister
 *
 * @return
 *   - 0 on success
 *   - -1 in case of error, with rte_errno set to one of the following:
 *     EINVAL - one of the parameters was invalid
 *     ENOENT - memory chunk was not found
 */
__rte_experimental
int
rte_extmem_detach(void *va_addr, size_t len);

/**
 * Dump the physical memory layout to a file.
 *
 * @note This function read-locks the memory hotplug subsystem, and thus cannot
 *       be used within memory-related callback functions.
 *
 * @param f
 *   A pointer to a file for output
 */
void rte_dump_physmem_layout(FILE *f);

/**
 * Get the total amount of available physical memory.
 *
 * @note This function read-locks the memory hotplug subsystem, and thus cannot
 *       be used within memory-related callback functions.
 *
 * @return
 *    The total amount of available physical memory in bytes.
 */
uint64_t rte_eal_get_physmem_size(void);

/**
 * Get the number of memory channels.
 *
 * @return
 *   The number of memory channels on the system. The value is 0 if unknown
 *   or not the same on all devices.
 */
unsigned rte_memory_get_nchannel(void);

/**
 * Get the number of memory ranks.
 *
 * @return
 *   The number of memory ranks on the system. The value is 0 if unknown or
 *   not the same on all devices.
 */
unsigned rte_memory_get_nrank(void);

/**
 * @warning
 * @b EXPERIMENTAL: this API may change without prior notice
 *
 * Check if all currently allocated memory segments are compliant with
 * supplied DMA address width.
 *
 *  @param maskbits
 *    Address width to check against.
 */
__rte_experimental
int rte_mem_check_dma_mask(uint8_t maskbits);

/**
 * @warning
 * @b EXPERIMENTAL: this API may change without prior notice
 *
 * Check if all currently allocated memory segments are compliant with
 * supplied DMA address width. This function will use
 * rte_memseg_walk_thread_unsafe instead of rte_memseg_walk implying
 * memory_hotplug_lock will not be acquired avoiding deadlock during
 * memory initialization.
 *
 * This function is just for EAL core memory internal use. Drivers should
 * use the previous rte_mem_check_dma_mask.
 *
 *  @param maskbits
 *    Address width to check against.
 */
__rte_experimental
int rte_mem_check_dma_mask_thread_unsafe(uint8_t maskbits);

/**
 * @warning
 * @b EXPERIMENTAL: this API may change without prior notice
 *
 *  Set dma mask to use once memory initialization is done. Previous functions
 *  rte_mem_check_dma_mask and rte_mem_check_dma_mask_thread_unsafe can not be
 *  used safely until memory has been initialized.
 */
__rte_experimental
void rte_mem_set_dma_mask(uint8_t maskbits);

/**
 * Drivers based on uio will not load unless physical
 * addresses are obtainable. It is only possible to get
 * physical addresses when running as a privileged user.
 *
 * @return
 *   1 if the system is able to obtain physical addresses.
 *   0 if using DMA addresses through an IOMMU.
 */
int rte_eal_using_phys_addrs(void);


/**
 * Enum indicating which kind of memory event has happened. Used by callbacks to
 * distinguish between memory allocations and deallocations.
 */
enum rte_mem_event {
	RTE_MEM_EVENT_ALLOC = 0, /**< Allocation event. */
	RTE_MEM_EVENT_FREE,      /**< Deallocation event. */
};
#define RTE_MEM_EVENT_CALLBACK_NAME_LEN 64
/**< maximum length of callback name */

/**
 * Function typedef used to register callbacks for memory events.
 */
typedef void (*rte_mem_event_callback_t)(enum rte_mem_event event_type,
		const void *addr, size_t len, void *arg);

/**
 * Function used to register callbacks for memory events.
 *
 * @note callbacks will happen while memory hotplug subsystem is write-locked,
 *       therefore some functions (e.g. `rte_memseg_walk()`) will cause a
 *       deadlock when called from within such callbacks.
 *
 * @note mem event callbacks not being supported is an expected error condition,
 *       so user code needs to handle this situation. In these cases, return
 *       value will be -1, and rte_errno will be set to ENOTSUP.
 *
 * @param name
 *   Name associated with specified callback to be added to the list.
 *
 * @param clb
 *   Callback function pointer.
 *
 * @param arg
 *   Argument to pass to the callback.
 *
 * @return
 *   0 on successful callback register
 *   -1 on unsuccessful callback register, with rte_errno value indicating
 *   reason for failure.
 */
__rte_experimental
int
rte_mem_event_callback_register(const char *name, rte_mem_event_callback_t clb,
		void *arg);

/**
 * Function used to unregister callbacks for memory events.
 *
 * @param name
 *   Name associated with specified callback to be removed from the list.
 *
 * @param arg
 *   Argument to look for among callbacks with specified callback name.
 *
 * @return
 *   0 on successful callback unregister
 *   -1 on unsuccessful callback unregister, with rte_errno value indicating
 *   reason for failure.
 */
__rte_experimental
int
rte_mem_event_callback_unregister(const char *name, void *arg);


#define RTE_MEM_ALLOC_VALIDATOR_NAME_LEN 64
/**< maximum length of alloc validator name */
/**
 * Function typedef used to register memory allocation validation callbacks.
 *
 * Returning 0 will allow allocation attempt to continue. Returning -1 will
 * prevent allocation from succeeding.
 */
typedef int (*rte_mem_alloc_validator_t)(int socket_id,
		size_t cur_limit, size_t new_len);

/**
 * @brief Register validator callback for memory allocations.
 *
 * Callbacks registered by this function will be called right before memory
 * allocator is about to trigger allocation of more pages from the system if
 * said allocation will bring total memory usage above specified limit on
 * specified socket. User will be able to cancel pending allocation if callback
 * returns -1.
 *
 * @note callbacks will happen while memory hotplug subsystem is write-locked,
 *       therefore some functions (e.g. `rte_memseg_walk()`) will cause a
 *       deadlock when called from within such callbacks.
 *
 * @note validator callbacks not being supported is an expected error condition,
 *       so user code needs to handle this situation. In these cases, return
 *       value will be -1, and rte_errno will be set to ENOTSUP.
 *
 * @param name
 *   Name associated with specified callback to be added to the list.
 *
 * @param clb
 *   Callback function pointer.
 *
 * @param socket_id
 *   Socket ID on which to watch for allocations.
 *
 * @param limit
 *   Limit above which to trigger callbacks.
 *
 * @return
 *   0 on successful callback register
 *   -1 on unsuccessful callback register, with rte_errno value indicating
 *   reason for failure.
 */
__rte_experimental
int
rte_mem_alloc_validator_register(const char *name,
		rte_mem_alloc_validator_t clb, int socket_id, size_t limit);

/**
 * @brief Unregister validator callback for memory allocations.
 *
 * @param name
 *   Name associated with specified callback to be removed from the list.
 *
 * @param socket_id
 *   Socket ID on which to watch for allocations.
 *
 * @return
 *   0 on successful callback unregister
 *   -1 on unsuccessful callback unregister, with rte_errno value indicating
 *   reason for failure.
 */
__rte_experimental
int
rte_mem_alloc_validator_unregister(const char *name, int socket_id);

#ifdef __cplusplus
}
#endif

#endif /* _RTE_MEMORY_H_ */
