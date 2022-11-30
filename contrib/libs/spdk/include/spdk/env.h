/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   Copyright (c) NetApp, Inc.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/** \file
 * Encapsulated third-party dependencies
 */

#ifndef SPDK_ENV_H
#define SPDK_ENV_H

#include "spdk/stdinc.h"
#include "spdk/queue.h"
#include "spdk/pci_ids.h"

#ifdef __cplusplus
extern "C" {
#endif

#define SPDK_ENV_SOCKET_ID_ANY	(-1)
#define SPDK_ENV_LCORE_ID_ANY	(UINT32_MAX)

/**
 * Memory is dma-safe.
 */
#define SPDK_MALLOC_DMA    0x01

/**
 * Memory is sharable across process boundaries.
 */
#define SPDK_MALLOC_SHARE  0x02

#define SPDK_MAX_MEMZONE_NAME_LEN 32
#define SPDK_MAX_MEMPOOL_NAME_LEN 29

/**
 * Memzone flags
 */
#define SPDK_MEMZONE_NO_IOVA_CONTIG 0x00100000 /**< no iova contiguity */

/**
 * \brief Environment initialization options
 */
struct spdk_env_opts {
	const char		*name;
	const char		*core_mask;
	int			shm_id;
	int			mem_channel;
	int			main_core;
	int			mem_size;
	bool			no_pci;
	bool			hugepage_single_segments;
	bool			unlink_hugepage;
	size_t			num_pci_addr;
	const char		*hugedir;
	struct spdk_pci_addr	*pci_blocked;
	struct spdk_pci_addr	*pci_allowed;
	const char		*iova_mode;
	uint64_t		base_virtaddr;

	/** Opaque context for use of the env implementation. */
	void			*env_context;
};

/**
 * Allocate dma/sharable memory based on a given dma_flg. It is a memory buffer
 * with the given size, alignment and socket id.
 *
 * \param size Size in bytes.
 * \param align If non-zero, the allocated buffer is aligned to a multiple of
 * align. In this case, it must be a power of two. The returned buffer is always
 * aligned to at least cache line size.
 * \param phys_addr **Deprecated**. Please use spdk_vtophys() for retrieving physical
 * addresses. A pointer to the variable to hold the physical address of
 * the allocated buffer is passed. If NULL, the physical address is not returned.
 * \param socket_id Socket ID to allocate memory on, or SPDK_ENV_SOCKET_ID_ANY
 * for any socket.
 * \param flags Combination of SPDK_MALLOC flags (\ref SPDK_MALLOC_DMA, \ref SPDK_MALLOC_SHARE).
 * At least one flag must be specified.
 *
 * \return a pointer to the allocated memory buffer.
 */
void *spdk_malloc(size_t size, size_t align, uint64_t *phys_addr, int socket_id, uint32_t flags);

/**
 * Allocate dma/sharable memory based on a given dma_flg. It is a memory buffer
 * with the given size, alignment and socket id. Also, the buffer will be zeroed.
 *
 * \param size Size in bytes.
 * \param align If non-zero, the allocated buffer is aligned to a multiple of
 * align. In this case, it must be a power of two. The returned buffer is always
 * aligned to at least cache line size.
 * \param phys_addr **Deprecated**. Please use spdk_vtophys() for retrieving physical
 * addresses. A pointer to the variable to hold the physical address of
 * the allocated buffer is passed. If NULL, the physical address is not returned.
 * \param socket_id Socket ID to allocate memory on, or SPDK_ENV_SOCKET_ID_ANY
 * for any socket.
 * \param flags Combination of SPDK_MALLOC flags (\ref SPDK_MALLOC_DMA, \ref SPDK_MALLOC_SHARE).
 *
 * \return a pointer to the allocated memory buffer.
 */
void *spdk_zmalloc(size_t size, size_t align, uint64_t *phys_addr, int socket_id, uint32_t flags);

/**
 * Resize a dma/sharable memory buffer with the given new size and alignment.
 * Existing contents are preserved.
 *
 * \param buf Buffer to resize.
 * \param size Size in bytes.
 * \param align If non-zero, the allocated buffer is aligned to a multiple of
 * align. In this case, it must be a power of two. The returned buffer is always
 * aligned to at least cache line size.
 *
 * \return a pointer to the resized memory buffer.
 */
void *spdk_realloc(void *buf, size_t size, size_t align);

/**
 * Free buffer memory that was previously allocated with spdk_malloc() or spdk_zmalloc().
 *
 * \param buf Buffer to free.
 */
void spdk_free(void *buf);

/**
 * Initialize the default value of opts.
 *
 * \param opts Data structure where SPDK will initialize the default options.
 */
void spdk_env_opts_init(struct spdk_env_opts *opts);

/**
 * Initialize or reinitialize the environment library.
 * For initialization, this must be called prior to using any other functions
 * in this library. For reinitialization, the parameter `opts` must be set to
 * NULL and this must be called after the environment library was finished by
 * spdk_env_fini() within the same process.
 *
 * \param opts Environment initialization options.
 * \return 0 on success, or negative errno on failure.
 */
int spdk_env_init(const struct spdk_env_opts *opts);

/**
 * Release any resources of the environment library that were allocated with
 * spdk_env_init(). After this call, no SPDK env function calls may be made.
 * It is expected that common usage of this function is to call it just before
 * terminating the process or before reinitializing the environment library
 * within the same process.
 */
void spdk_env_fini(void);

/**
 * Allocate a pinned memory buffer with the given size and alignment.
 *
 * \param size Size in bytes.
 * \param align If non-zero, the allocated buffer is aligned to a multiple of
 * align. In this case, it must be a power of two. The returned buffer is always
 * aligned to at least cache line size.
 * \param phys_addr A pointer to the variable to hold the physical address of
 * the allocated buffer is passed. If NULL, the physical address is not returned.
 *
 * \return a pointer to the allocated memory buffer.
 */
void *spdk_dma_malloc(size_t size, size_t align, uint64_t *phys_addr);

/**
 * Allocate a pinned, memory buffer with the given size, alignment and socket id.
 *
 * \param size Size in bytes.
 * \param align If non-zero, the allocated buffer is aligned to a multiple of
 * align. In this case, it must be a power of two. The returned buffer is always
 * aligned to at least cache line size.
 * \param phys_addr A pointer to the variable to hold the physical address of
 * the allocated buffer is passed. If NULL, the physical address is not returned.
 * \param socket_id Socket ID to allocate memory on, or SPDK_ENV_SOCKET_ID_ANY
 * for any socket.
 *
 * \return a pointer to the allocated memory buffer.
 */
void *spdk_dma_malloc_socket(size_t size, size_t align, uint64_t *phys_addr, int socket_id);

/**
 * Allocate a pinned memory buffer with the given size and alignment. The buffer
 * will be zeroed.
 *
 * \param size Size in bytes.
 * \param align If non-zero, the allocated buffer is aligned to a multiple of
 * align. In this case, it must be a power of two. The returned buffer is always
 * aligned to at least cache line size.
 * \param phys_addr A pointer to the variable to hold the physical address of
 * the allocated buffer is passed. If NULL, the physical address is not returned.
 *
 * \return a pointer to the allocated memory buffer.
 */
void *spdk_dma_zmalloc(size_t size, size_t align, uint64_t *phys_addr);

/**
 * Allocate a pinned memory buffer with the given size, alignment and socket id.
 * The buffer will be zeroed.
 *
 * \param size Size in bytes.
 * \param align If non-zero, the allocated buffer is aligned to a multiple of
 * align. In this case, it must be a power of two. The returned buffer is always
 * aligned to at least cache line size.
 * \param phys_addr A pointer to the variable to hold the physical address of
 * the allocated buffer is passed. If NULL, the physical address is not returned.
 * \param socket_id Socket ID to allocate memory on, or SPDK_ENV_SOCKET_ID_ANY
 * for any socket.
 *
 * \return a pointer to the allocated memory buffer.
 */
void *spdk_dma_zmalloc_socket(size_t size, size_t align, uint64_t *phys_addr, int socket_id);

/**
 * Resize the allocated and pinned memory buffer with the given new size and
 * alignment. Existing contents are preserved.
 *
 * \param buf Buffer to resize.
 * \param size Size in bytes.
 * \param align If non-zero, the allocated buffer is aligned to a multiple of
 * align. In this case, it must be a power of two. The returned buffer is always
 * aligned to at least cache line size.
 * \param phys_addr A pointer to the variable to hold the physical address of
 * the allocated buffer is passed. If NULL, the physical address is not returned.
 *
 * \return a pointer to the resized memory buffer.
 */
void *spdk_dma_realloc(void *buf, size_t size, size_t align, uint64_t *phys_addr);

/**
 * Free a memory buffer previously allocated, for example from spdk_dma_zmalloc().
 * This call is never made from the performance path.
 *
 * \param buf Buffer to free.
 */
void spdk_dma_free(void *buf);

/**
 * Reserve a named, process shared memory zone with the given size, socket_id
 * and flags. Unless `SPDK_MEMZONE_NO_IOVA_CONTIG` flag is provided, the returned
 * memory will be IOVA contiguous.
 *
 * \param name Name to set for this memory zone.
 * \param len Length in bytes.
 * \param socket_id Socket ID to allocate memory on, or SPDK_ENV_SOCKET_ID_ANY
 * for any socket.
 * \param flags Flags to set for this memory zone.
 *
 * \return a pointer to the allocated memory address on success, or NULL on failure.
 */
void *spdk_memzone_reserve(const char *name, size_t len, int socket_id, unsigned flags);

/**
 * Reserve a named, process shared memory zone with the given size, socket_id,
 * flags and alignment. Unless `SPDK_MEMZONE_NO_IOVA_CONTIG` flag is provided,
 * the returned memory will be IOVA contiguous.
 *
 * \param name Name to set for this memory zone.
 * \param len Length in bytes.
 * \param socket_id Socket ID to allocate memory on, or SPDK_ENV_SOCKET_ID_ANY
 * for any socket.
 * \param flags Flags to set for this memory zone.
 * \param align Alignment for resulting memzone. Must be a power of 2.
 *
 * \return a pointer to the allocated memory address on success, or NULL on failure.
 */
void *spdk_memzone_reserve_aligned(const char *name, size_t len, int socket_id,
				   unsigned flags, unsigned align);

/**
 * Lookup the memory zone identified by the given name.
 *
 * \param name Name of the memory zone.
 *
 * \return a pointer to the reserved memory address on success, or NULL on failure.
 */
void *spdk_memzone_lookup(const char *name);

/**
 * Free the memory zone identified by the given name.
 *
 * \return 0 on success, -1 on failure.
 */
int spdk_memzone_free(const char *name);

/**
 * Dump debug information about all memzones.
 *
 * \param f File to write debug information to.
 */
void spdk_memzone_dump(FILE *f);

struct spdk_mempool;

#define SPDK_MEMPOOL_DEFAULT_CACHE_SIZE	SIZE_MAX

/**
 * Create a thread-safe memory pool.
 *
 * \param name Name for the memory pool.
 * \param count Count of elements.
 * \param ele_size Element size in bytes.
 * \param cache_size How many elements may be cached in per-core caches. Use
 * SPDK_MEMPOOL_DEFAULT_CACHE_SIZE for a reasonable default, or 0 for no per-core cache.
 * \param socket_id Socket ID to allocate memory on, or SPDK_ENV_SOCKET_ID_ANY
 * for any socket.
 *
 * \return a pointer to the created memory pool.
 */
struct spdk_mempool *spdk_mempool_create(const char *name, size_t count,
		size_t ele_size, size_t cache_size, int socket_id);

/**
 * An object callback function for memory pool.
 *
 * Used by spdk_mempool_create_ctor().
 */
typedef void (spdk_mempool_obj_cb_t)(struct spdk_mempool *mp,
				     void *opaque, void *obj, unsigned obj_idx);

/**
 * Create a thread-safe memory pool with user provided initialization function
 * and argument.
 *
 * \param name Name for the memory pool.
 * \param count Count of elements.
 * \param ele_size Element size in bytes.
 * \param cache_size How many elements may be cached in per-core caches. Use
 * SPDK_MEMPOOL_DEFAULT_CACHE_SIZE for a reasonable default, or 0 for no per-core cache.
 * \param socket_id Socket ID to allocate memory on, or SPDK_ENV_SOCKET_ID_ANY
 * for any socket.
 * \param obj_init User provided object calllback initialization function.
 * \param obj_init_arg User provided callback initialization function argument.
 *
 * \return a pointer to the created memory pool.
 */
struct spdk_mempool *spdk_mempool_create_ctor(const char *name, size_t count,
		size_t ele_size, size_t cache_size, int socket_id,
		spdk_mempool_obj_cb_t *obj_init, void *obj_init_arg);

/**
 * Get the name of a memory pool.
 *
 * \param mp Memory pool to query.
 *
 * \return the name of the memory pool.
 */
char *spdk_mempool_get_name(struct spdk_mempool *mp);

/**
 * Free a memory pool.
 */
void spdk_mempool_free(struct spdk_mempool *mp);

/**
 * Get an element from a memory pool. If no elements remain, return NULL.
 *
 * \param mp Memory pool to query.
 *
 * \return a pointer to the element.
 */
void *spdk_mempool_get(struct spdk_mempool *mp);

/**
 * Get multiple elements from a memory pool.
 *
 * \param mp Memory pool to get multiple elements from.
 * \param ele_arr Array of the elements to fill.
 * \param count Count of elements to get.
 *
 * \return 0 on success, negative errno on failure.
 */
int spdk_mempool_get_bulk(struct spdk_mempool *mp, void **ele_arr, size_t count);

/**
 * Put an element back into the memory pool.
 *
 * \param mp Memory pool to put element back into.
 * \param ele Element to put.
 */
void spdk_mempool_put(struct spdk_mempool *mp, void *ele);

/**
 * Put multiple elements back into the memory pool.
 *
 * \param mp Memory pool to put multiple elements back into.
 * \param ele_arr Array of the elements to put.
 * \param count Count of elements to put.
 */
void spdk_mempool_put_bulk(struct spdk_mempool *mp, void **ele_arr, size_t count);

/**
 * Get the number of entries in the memory pool.
 *
 * \param pool Memory pool to query.
 *
 * \return the number of entries in the memory pool.
 */
size_t spdk_mempool_count(const struct spdk_mempool *pool);

/**
 * Iterate through all elements of the pool and call a function on each one.
 *
 * \param mp Memory pool to iterate on.
 * \param obj_cb Function to call on each element.
 * \param obj_cb_arg Opaque pointer passed to the callback function.
 *
 * \return Number of elements iterated.
 */
uint32_t spdk_mempool_obj_iter(struct spdk_mempool *mp, spdk_mempool_obj_cb_t obj_cb,
			       void *obj_cb_arg);

/**
 * Lookup the memory pool identified by the given name.
 *
 * \param name Name of the memory pool.
 *
 * \return a pointer to the memory pool on success, or NULL on failure.
 */
struct spdk_mempool *spdk_mempool_lookup(const char *name);

/**
 * Get the number of dedicated CPU cores utilized by this env abstraction.
 *
 * \return the number of dedicated CPU cores.
 */
uint32_t spdk_env_get_core_count(void);

/**
 * Get the CPU core index of the current thread.
 *
 * This will only function when called from threads set up by
 * this environment abstraction. For any other threads \c SPDK_ENV_LCORE_ID_ANY
 * will be returned.
 *
 * \return the CPU core index of the current thread.
 */
uint32_t spdk_env_get_current_core(void);

/**
 * Get the index of the first dedicated CPU core for this application.
 *
 * \return the index of the first dedicated CPU core.
 */
uint32_t spdk_env_get_first_core(void);

/**
 * Get the index of the last dedicated CPU core for this application.
 *
 * \return the index of the last dedicated CPU core.
 */
uint32_t spdk_env_get_last_core(void);

/**
 * Get the index of the next dedicated CPU core for this application.
 *
 * If there is no next core, return UINT32_MAX.
 *
 * \param prev_core Index of previous core.
 *
 * \return the index of the next dedicated CPU core.
 */
uint32_t spdk_env_get_next_core(uint32_t prev_core);

#define SPDK_ENV_FOREACH_CORE(i)		\
	for (i = spdk_env_get_first_core();	\
	     i < UINT32_MAX;			\
	     i = spdk_env_get_next_core(i))

/**
 * Get the socket ID for the given core.
 *
 * \param core CPU core to query.
 *
 * \return the socket ID for the given core.
 */
uint32_t spdk_env_get_socket_id(uint32_t core);

typedef int (*thread_start_fn)(void *);

/**
 * Launch a thread pinned to the given core. Only a single pinned thread may be
 * launched per core. Subsequent attempts to launch pinned threads on that core
 * will fail.
 *
 * \param core The core to pin the thread to.
 * \param fn Entry point on the new thread.
 * \param arg Argument apssed to thread_start_fn
 *
 * \return 0 on success, negative errno on failure.
 */
int spdk_env_thread_launch_pinned(uint32_t core, thread_start_fn fn, void *arg);

/**
 * Wait for all threads to exit before returning.
 */
void spdk_env_thread_wait_all(void);

/**
 * Check whether the calling process is primary process.
 *
 * \return true if the calling process is primary process, or false otherwise.
 */
bool spdk_process_is_primary(void);

/**
 * Get a monotonic timestamp counter.
 *
 * \return the monotonic timestamp counter.
 */
uint64_t spdk_get_ticks(void);

/**
 * Get the tick rate of spdk_get_ticks() per second.
 *
 * \return the tick rate of spdk_get_ticks() per second.
 */
uint64_t spdk_get_ticks_hz(void);

/**
 * Delay the given number of microseconds.
 *
 * \param us Number of microseconds.
 */
void spdk_delay_us(unsigned int us);

/**
 * Pause CPU execution for a short while
 */
void spdk_pause(void);

struct spdk_ring;

enum spdk_ring_type {
	SPDK_RING_TYPE_SP_SC,		/* Single-producer, single-consumer */
	SPDK_RING_TYPE_MP_SC,		/* Multi-producer, single-consumer */
	SPDK_RING_TYPE_MP_MC,		/* Multi-producer, multi-consumer */
};

/**
 * Create a ring.
 *
 * \param type Type for the ring. (SPDK_RING_TYPE_SP_SC or SPDK_RING_TYPE_MP_SC).
 * \param count Size of the ring in elements.
 * \param socket_id Socket ID to allocate memory on, or SPDK_ENV_SOCKET_ID_ANY
 * for any socket.
 *
 * \return a pointer to the created ring.
 */
struct spdk_ring *spdk_ring_create(enum spdk_ring_type type, size_t count, int socket_id);

/**
 * Free the ring.
 *
 * \param ring Ring to free.
 */
void spdk_ring_free(struct spdk_ring *ring);

/**
 * Get the number of objects in the ring.
 *
 * \param ring the ring.
 *
 * \return the number of objects in the ring.
 */
size_t spdk_ring_count(struct spdk_ring *ring);

/**
 * Queue the array of objects (with length count) on the ring.
 *
 * \param ring A pointer to the ring.
 * \param objs A pointer to the array to be queued.
 * \param count Length count of the array of objects.
 * \param free_space If non-NULL, amount of free space after the enqueue has finished.
 *
 * \return the number of objects enqueued.
 */
size_t spdk_ring_enqueue(struct spdk_ring *ring, void **objs, size_t count,
			 size_t *free_space);

/**
 * Dequeue count objects from the ring into the array objs.
 *
 * \param ring A pointer to the ring.
 * \param objs A pointer to the array to be dequeued.
 * \param count Maximum number of elements to be dequeued.
 *
 * \return the number of objects dequeued which is less than 'count'.
 */
size_t spdk_ring_dequeue(struct spdk_ring *ring, void **objs, size_t count);

/**
 * Reports whether the SPDK application is using the IOMMU for DMA
 *
 * \return True if we are using the IOMMU, false otherwise.
 */
bool spdk_iommu_is_enabled(void);

#define SPDK_VTOPHYS_ERROR	(0xFFFFFFFFFFFFFFFFULL)

/**
 * Get the physical address of a buffer.
 *
 * \param buf A pointer to a buffer.
 * \param size Contains the size of the memory region pointed to by vaddr.
 * If vaddr is successfully translated, then this is updated with the size of
 * the memory region for which the translation is valid.
 *
 * \return the physical address of this buffer on success, or SPDK_VTOPHYS_ERROR
 * on failure.
 */
uint64_t spdk_vtophys(const void *buf, uint64_t *size);

struct spdk_pci_addr {
	uint32_t			domain;
	uint8_t				bus;
	uint8_t				dev;
	uint8_t				func;
};

struct spdk_pci_id {
	uint32_t	class_id;	/**< Class ID or SPDK_PCI_CLASS_ANY_ID. */
	uint16_t	vendor_id;	/**< Vendor ID or SPDK_PCI_ANY_ID. */
	uint16_t	device_id;	/**< Device ID or SPDK_PCI_ANY_ID. */
	uint16_t	subvendor_id;	/**< Subsystem vendor ID or SPDK_PCI_ANY_ID. */
	uint16_t	subdevice_id;	/**< Subsystem device ID or SPDK_PCI_ANY_ID. */
};

/** Device needs PCI BAR mapping (done with either IGB_UIO or VFIO) */
#define SPDK_PCI_DRIVER_NEED_MAPPING 0x0001
/** Device needs PCI BAR mapping with enabled write combining (wc) */
#define SPDK_PCI_DRIVER_WC_ACTIVATE 0x0002

void spdk_pci_driver_register(const char *name, struct spdk_pci_id *id_table, uint32_t flags);

struct spdk_pci_device {
	struct spdk_pci_device		*parent;
	void				*dev_handle;
	struct spdk_pci_addr		addr;
	struct spdk_pci_id		id;
	int				socket_id;
	const char			*type;

	int (*map_bar)(struct spdk_pci_device *dev, uint32_t bar,
		       void **mapped_addr, uint64_t *phys_addr, uint64_t *size);
	int (*unmap_bar)(struct spdk_pci_device *dev, uint32_t bar,
			 void *addr);
	int (*cfg_read)(struct spdk_pci_device *dev, void *value,
			uint32_t len, uint32_t offset);
	int (*cfg_write)(struct spdk_pci_device *dev, void *value,
			 uint32_t len, uint32_t offset);

	struct _spdk_pci_device_internal {
		struct spdk_pci_driver		*driver;
		bool				attached;
		/* optional fd for exclusive access to this device on this process */
		int				claim_fd;
		bool				pending_removal;
		/* The device was successfully removed on a DPDK interrupt thread,
		 * but to prevent data races we couldn't remove it from the global
		 * device list right away. It'll be removed as soon as possible
		 * on a regular thread when any public pci function is called.
		 */
		bool				removed;
		TAILQ_ENTRY(spdk_pci_device)	tailq;
	} internal;
};

/**
 * Callback for device attach handling.
 *
 * \param enum_ctx Opaque value.
 * \param dev PCI device.
 *
 * \return -1 if an error occurred,
 *          0 if device attached successfully,
 *          1 if device not attached.
 */
typedef int (*spdk_pci_enum_cb)(void *enum_ctx, struct spdk_pci_device *dev);

#define SPDK_PCI_DEVICE(vend, dev)          \
	.class_id = SPDK_PCI_CLASS_ANY_ID,      \
	.vendor_id = (vend),                    \
	.device_id = (dev),                     \
	.subvendor_id = SPDK_PCI_ANY_ID,        \
	.subdevice_id = SPDK_PCI_ANY_ID

#define SPDK_PCI_DRIVER_REGISTER(name, id_table, flags) \
__attribute__((constructor)) static void _spdk_pci_driver_register_##name(void) \
{ \
	spdk_pci_driver_register(#name, id_table, flags); \
}

/**
 * Get the VMD PCI driver object.
 *
 * \return PCI driver.
 */
struct spdk_pci_driver *spdk_pci_vmd_get_driver(void);

/**
 * Get the I/OAT PCI driver object.
 *
 * \return PCI driver.
 */
struct spdk_pci_driver *spdk_pci_ioat_get_driver(void);

/**
 * Get the IDXD PCI driver object.
 *
 * \return PCI driver.
 */
struct spdk_pci_driver *spdk_pci_idxd_get_driver(void);

/**
 * Get the Virtio PCI driver object.
 *
 * \return PCI driver.
 */
struct spdk_pci_driver *spdk_pci_virtio_get_driver(void);

/**
 * Get PCI driver by name (e.g. "nvme", "vmd", "ioat").
 */
struct spdk_pci_driver *spdk_pci_get_driver(const char *name);

/**
 * Get the NVMe PCI driver object.
 *
 * \return PCI driver.
 */
struct spdk_pci_driver *spdk_pci_nvme_get_driver(void);

/**
 * Enumerate all PCI devices supported by the provided driver and try to
 * attach those that weren't attached yet. The provided callback will be
 * called for each such device and its return code will decide whether that
 * device is attached or not. Attached devices have to be manually detached
 * with spdk_pci_device_detach() to be attach-able again.
 *
 * During enumeration all registered pci devices with exposed access to
 * userspace are getting probed internally unless not explicitly specified
 * on denylist. Because of that it becomes not possible to either use such
 * devices with another application or unbind the driver (e.g. vfio).
 *
 * 2s asynchronous delay is introduced to avoid race conditions between
 * user space software initialization and in-kernel device handling for
 * newly inserted devices. Subsequent enumerate call after the delay
 * shall allow for a successful device attachment.
 *
 * \param driver Driver for a specific device type.
 * \param enum_cb Callback to be called for each non-attached PCI device.
 * \param enum_ctx Additional context passed to the callback function.
 *
 * \return -1 if an internal error occurred or the provided callback returned -1,
 *         0 otherwise
 */
int spdk_pci_enumerate(struct spdk_pci_driver *driver, spdk_pci_enum_cb enum_cb, void *enum_ctx);

/**
 * Begin iterating over enumerated PCI device by calling this function to get
 * the first PCI device. If there no PCI devices enumerated, return NULL
 *
 * \return a pointer to a PCI device on success, NULL otherwise.
 */
struct spdk_pci_device *spdk_pci_get_first_device(void);

/**
 * Continue iterating over enumerated PCI devices.
 * If no additional PCI devices, return NULL
 *
 * \param prev Previous PCI device returned from \ref spdk_pci_get_first_device
 * or \ref spdk_pci_get_next_device
 *
 * \return a pointer to the next PCI device on success, NULL otherwise.
 */
struct spdk_pci_device *spdk_pci_get_next_device(struct spdk_pci_device *prev);

/**
 * Map a PCI BAR in the current process.
 *
 * \param dev PCI device.
 * \param bar BAR number.
 * \param mapped_addr A variable to store the virtual address of the mapping.
 * \param phys_addr A variable to store the physical address of the mapping.
 * \param size A variable to store the size of the bar (in bytes).
 *
 * \return 0 on success.
 */
int spdk_pci_device_map_bar(struct spdk_pci_device *dev, uint32_t bar,
			    void **mapped_addr, uint64_t *phys_addr, uint64_t *size);

/**
 * Unmap a PCI BAR from the current process. This happens automatically when
 * the PCI device is detached.
 *
 * \param dev PCI device.
 * \param bar BAR number.
 * \param mapped_addr Virtual address of the bar.
 *
 * \return 0 on success.
 */
int spdk_pci_device_unmap_bar(struct spdk_pci_device *dev, uint32_t bar,
			      void *mapped_addr);

/**
 * Get the domain of a PCI device.
 *
 * \param dev PCI device.
 *
 * \return PCI device domain.
 */
uint32_t spdk_pci_device_get_domain(struct spdk_pci_device *dev);

/**
 * Get the bus number of a PCI device.
 *
 * \param dev PCI device.
 *
 * \return PCI bus number.
 */
uint8_t spdk_pci_device_get_bus(struct spdk_pci_device *dev);

/**
 * Get the device number within the PCI bus the device is on.
 *
 * \param dev PCI device.
 *
 * \return PCI device number.
 */
uint8_t spdk_pci_device_get_dev(struct spdk_pci_device *dev);

/**
 * Get the particular function number represented by struct spdk_pci_device.
 *
 * \param dev PCI device.
 *
 * \return PCI function number.
 */
uint8_t spdk_pci_device_get_func(struct spdk_pci_device *dev);

/**
 * Get the full DomainBDF address of a PCI device.
 *
 * \param dev PCI device.
 *
 * \return PCI address.
 */
struct spdk_pci_addr spdk_pci_device_get_addr(struct spdk_pci_device *dev);

/**
 * Get the vendor ID of a PCI device.
 *
 * \param dev PCI device.
 *
 * \return vendor ID.
 */
uint16_t spdk_pci_device_get_vendor_id(struct spdk_pci_device *dev);

/**
 * Get the device ID of a PCI device.
 *
 * \param dev PCI device.
 *
 * \return device ID.
 */
uint16_t spdk_pci_device_get_device_id(struct spdk_pci_device *dev);

/**
 * Get the subvendor ID of a PCI device.
 *
 * \param dev PCI device.
 *
 * \return subvendor ID.
 */
uint16_t spdk_pci_device_get_subvendor_id(struct spdk_pci_device *dev);

/**
 * Get the subdevice ID of a PCI device.
 *
 * \param dev PCI device.
 *
 * \return subdevice ID.
 */
uint16_t spdk_pci_device_get_subdevice_id(struct spdk_pci_device *dev);

/**
 * Get the PCI ID of a PCI device.
 *
 * \param dev PCI device.
 *
 * \return PCI ID.
 */
struct spdk_pci_id spdk_pci_device_get_id(struct spdk_pci_device *dev);

/**
 * Get the NUMA node the PCI device is on.
 *
 * \param dev PCI device.
 *
 * \return NUMA node index (>= 0).
 */
int spdk_pci_device_get_socket_id(struct spdk_pci_device *dev);

/**
 * Serialize the PCIe Device Serial Number into the provided buffer.
 * The buffer will contain a 16-character-long serial number followed by
 * a NULL terminator.
 *
 * \param dev PCI device.
 * \param sn Buffer to store the serial number in.
 * \param len Length of buffer. Must be at least 17.
 *
 * \return 0 on success, -1 on failure.
 */
int spdk_pci_device_get_serial_number(struct spdk_pci_device *dev, char *sn, size_t len);

/**
 * Claim a PCI device for exclusive SPDK userspace access.
 *
 * Uses F_SETLK on a shared memory file with the PCI address embedded in its name.
 * As long as this file remains open with the lock acquired, other processes will
 * not be able to successfully call this function on the same PCI device.
 *
 * The device can be un-claimed by the owning process with spdk_pci_device_unclaim().
 * It will be also unclaimed automatically when detached.
 *
 * \param dev PCI device to claim.
 *
 * \return -EACCES if the device has already been claimed,
 *	   negative errno on unexpected errors,
 *	   0 on success.
 */
int spdk_pci_device_claim(struct spdk_pci_device *dev);

/**
 * Undo spdk_pci_device_claim().
 *
 * \param dev PCI device to unclaim.
 */
void spdk_pci_device_unclaim(struct spdk_pci_device *dev);

/**
 * Release all resources associated with the given device and detach it. As long
 * as the PCI device is physically available, it will attachable again.
 *
 * \param device PCI device.
 */
void spdk_pci_device_detach(struct spdk_pci_device *device);

/**
 * Attach a PCI device. This will bypass all blocked list rules and explicitly
 * attach a device at the provided address. The return code of the provided
 * callback will decide whether that device is attached or not. Attached
 * devices have to be manually detached with spdk_pci_device_detach() to be
 * attach-able again.
 *
 * \param driver Driver for a specific device type. The device will only be
 * attached if it's supported by this driver.
 * \param enum_cb Callback to be called for the PCI device once it's found.
 * \param enum_ctx Additional context passed to the callback function.
 * \param pci_address Address of the device to attach.
 *
 * \return -1 if a device at the provided PCI address couldn't be found,
 *         -1 if an internal error happened or the provided callback returned non-zero,
 *         0 otherwise
 */
int spdk_pci_device_attach(struct spdk_pci_driver *driver, spdk_pci_enum_cb enum_cb,
			   void *enum_ctx, struct spdk_pci_addr *pci_address);

/**
 * Allow the specified PCI device to be probed by the calling process.
 *
 * When using spdk_pci_enumerate(), only devices with allowed PCI addresses will
 * be probed.  By default, this is all PCI addresses, but the pci_allowed
 * and pci_blocked environment options can override this behavior.
 * This API enables the caller to allow a new PCI address that may have previously
 * been blocked.
 *
 * \param pci_addr PCI address to allow
 * \return 0 if successful
 * \return -ENOMEM if environment-specific data structures cannot be allocated
 * \return -EINVAL if specified PCI address is not valid
 */
int spdk_pci_device_allow(struct spdk_pci_addr *pci_addr);

/**
 * Read \c len bytes from the PCI configuration space.
 *
 * \param dev PCI device.
 * \param buf A buffer to copy the data into.
 * \param len Number of bytes to read.
 * \param offset Offset (in bytes) in the PCI config space to start reading from.
 *
 * \return 0 on success, -1 on failure.
 */
int spdk_pci_device_cfg_read(struct spdk_pci_device *dev, void *buf, uint32_t len,
			     uint32_t offset);

/**
 * Write \c len bytes into the PCI configuration space.
 *
 * \param dev PCI device.
 * \param buf A buffer to copy the data from.
 * \param len Number of bytes to write.
 * \param offset Offset (in bytes) in the PCI config space to start writing to.
 *
 * \return 0 on success, -1 on failure.
 */
int spdk_pci_device_cfg_write(struct spdk_pci_device *dev, void *buf, uint32_t len,
			      uint32_t offset);

/**
 * Read 1 byte from the PCI configuration space.
 *
 * \param dev PCI device.
 * \param value A buffer to copy the data into.
 * \param offset Offset (in bytes) in the PCI config space to start reading from.
 *
 * \return 0 on success, -1 on failure.
 */
int spdk_pci_device_cfg_read8(struct spdk_pci_device *dev, uint8_t *value, uint32_t offset);

/**
 * Write 1 byte into the PCI configuration space.
 *
 * \param dev PCI device.
 * \param value A value to write.
 * \param offset Offset (in bytes) in the PCI config space to start writing to.
 *
 * \return 0 on success, -1 on failure.
 */
int spdk_pci_device_cfg_write8(struct spdk_pci_device *dev, uint8_t value, uint32_t offset);

/**
 * Read 2 bytes from the PCI configuration space.
 *
 * \param dev PCI device.
 * \param value A buffer to copy the data into.
 * \param offset Offset (in bytes) in the PCI config space to start reading from.
 *
 * \return 0 on success, -1 on failure.
 */
int spdk_pci_device_cfg_read16(struct spdk_pci_device *dev, uint16_t *value, uint32_t offset);

/**
 * Write 2 bytes into the PCI configuration space.
 *
 * \param dev PCI device.
 * \param value A value to write.
 * \param offset Offset (in bytes) in the PCI config space to start writing to.
 *
 * \return 0 on success, -1 on failure.
 */
int spdk_pci_device_cfg_write16(struct spdk_pci_device *dev, uint16_t value, uint32_t offset);

/**
 * Read 4 bytes from the PCI configuration space.
 *
 * \param dev PCI device.
 * \param value A buffer to copy the data into.
 * \param offset Offset (in bytes) in the PCI config space to start reading from.
 *
 * \return 0 on success, -1 on failure.
 */
int spdk_pci_device_cfg_read32(struct spdk_pci_device *dev, uint32_t *value, uint32_t offset);

/**
 * Write 4 bytes into the PCI configuration space.
 *
 * \param dev PCI device.
 * \param value A value to write.
 * \param offset Offset (in bytes) in the PCI config space to start writing to.
 *
 * \return 0 on success, -1 on failure.
 */
int spdk_pci_device_cfg_write32(struct spdk_pci_device *dev, uint32_t value, uint32_t offset);

/**
 * Check if device was requested to be removed from the process. This can be
 * caused either by physical device hotremoval or OS-triggered removal. In the
 * latter case, the device may continue to function properly even if this
 * function returns \c true . The upper-layer driver may check this function
 * periodically and eventually detach the device.
 *
 * \param dev PCI device.
 *
 * \return if device was requested to be removed
 */
bool spdk_pci_device_is_removed(struct spdk_pci_device *dev);

/**
 * Compare two PCI addresses.
 *
 * \param a1 PCI address 1.
 * \param a2 PCI address 2.
 *
 * \return 0 if a1 == a2, less than 0 if a1 < a2, greater than 0 if a1 > a2
 */
int spdk_pci_addr_compare(const struct spdk_pci_addr *a1, const struct spdk_pci_addr *a2);

/**
 * Convert a string representation of a PCI address into a struct spdk_pci_addr.
 *
 * \param addr PCI adddress output on success.
 * \param bdf PCI address in domain:bus:device.function format or
 *	domain.bus.device.function format.
 *
 * \return 0 on success, negative errno on failure.
 */
int spdk_pci_addr_parse(struct spdk_pci_addr *addr, const char *bdf);

/**
 * Convert a struct spdk_pci_addr to a string.
 *
 * \param bdf String into which a string will be output in the format
 *  domain:bus:device.function. The string must be at least 14 characters in size.
 * \param sz Size of bdf in bytes. Must be at least 14.
 * \param addr PCI address.
 *
 * \return 0 on success, or a negated errno on failure.
 */
int spdk_pci_addr_fmt(char *bdf, size_t sz, const struct spdk_pci_addr *addr);

/**
 * Hook a custom PCI device into the PCI layer. The device will be attachable,
 * enumerable, and will call provided callbacks on each PCI resource access
 * request.
 *
 * \param drv driver that will be able to attach the device
 * \param dev fully initialized PCI device struct
 */
void spdk_pci_hook_device(struct spdk_pci_driver *drv, struct spdk_pci_device *dev);

/**
 * Un-hook a custom PCI device from the PCI layer. The device must not be attached.
 *
 * \param dev fully initialized PCI device struct
 */
void spdk_pci_unhook_device(struct spdk_pci_device *dev);

/**
 * Return the type of the PCI device.
 *
 * \param dev PCI device
 *
 * \return string representing the type of the device
 */
const char *spdk_pci_device_get_type(const struct spdk_pci_device *dev);

/**
 * Remove any CPU affinity from the current thread.
 */
void spdk_unaffinitize_thread(void);

/**
 * Call a function with CPU affinity unset.
 *
 * This can be used to run a function that creates other threads without inheriting the calling
 * thread's CPU affinity.
 *
 * \param cb Function to call
 * \param arg Parameter to the function cb().
 *
 * \return the return value of cb().
 */
void *spdk_call_unaffinitized(void *cb(void *arg), void *arg);

/**
 * Page-granularity memory address translation table.
 */
struct spdk_mem_map;

enum spdk_mem_map_notify_action {
	SPDK_MEM_MAP_NOTIFY_REGISTER,
	SPDK_MEM_MAP_NOTIFY_UNREGISTER,
};

typedef int (*spdk_mem_map_notify_cb)(void *cb_ctx, struct spdk_mem_map *map,
				      enum spdk_mem_map_notify_action action,
				      void *vaddr, size_t size);

typedef int (*spdk_mem_map_contiguous_translations)(uint64_t addr_1, uint64_t addr_2);

/**
 * A function table to be implemented by each memory map.
 */
struct spdk_mem_map_ops {
	spdk_mem_map_notify_cb notify_cb;
	spdk_mem_map_contiguous_translations are_contiguous;
};

/**
 * Allocate a virtual memory address translation map.
 *
 * \param default_translation Default translation for the map.
 * \param ops Table of callback functions for map operations.
 * \param cb_ctx Argument passed to the callback function.
 *
 * \return a pointer to the allocated virtual memory address translation map.
 */
struct spdk_mem_map *spdk_mem_map_alloc(uint64_t default_translation,
					const struct spdk_mem_map_ops *ops, void *cb_ctx);

/**
 * Free a memory map previously allocated by spdk_mem_map_alloc().
 *
 * \param pmap Memory map to free.
 */
void spdk_mem_map_free(struct spdk_mem_map **pmap);

/**
 * Register an address translation for a range of virtual memory.
 *
 * \param map Memory map.
 * \param vaddr Virtual address of the region to register - must be 2 MB aligned.
 * \param size Size of the region in bytes - must be multiple of 2 MB in the
 *  current implementation.
 * \param translation Translation to store in the map for this address range.
 *
 * \sa spdk_mem_map_clear_translation().
 *
 * \return 0 on success, negative errno on failure.
 */
int spdk_mem_map_set_translation(struct spdk_mem_map *map, uint64_t vaddr, uint64_t size,
				 uint64_t translation);

/**
 * Unregister an address translation.
 *
 * \param map Memory map.
 * \param vaddr Virtual address of the region to unregister - must be 2 MB aligned.
 * \param size Size of the region in bytes - must be multiple of 2 MB in the
 *  current implementation.
 *
 * \sa spdk_mem_map_set_translation().
 *
 * \return 0 on success, negative errno on failure.
 */
int spdk_mem_map_clear_translation(struct spdk_mem_map *map, uint64_t vaddr, uint64_t size);

/**
 * Look up the translation of a virtual address in a memory map.
 *
 * \param map Memory map.
 * \param vaddr Virtual address.
 * \param size Contains the size of the memory region pointed to by vaddr.
 * If vaddr is successfully translated, then this is updated with the size of
 * the memory region for which the translation is valid.
 *
 * \return the translation of vaddr stored in the map, or default_translation
 * as specified in spdk_mem_map_alloc() if vaddr is not present in the map.
 */
uint64_t spdk_mem_map_translate(const struct spdk_mem_map *map, uint64_t vaddr, uint64_t *size);

/**
 * Register the specified memory region for address translation.
 *
 * The memory region must map to pinned huge pages (2MB or greater).
 *
 * \param vaddr Virtual address to register.
 * \param len Length in bytes of the vaddr.
 *
 * \return 0 on success, negative errno on failure.
 */
int spdk_mem_register(void *vaddr, size_t len);

/**
 * Unregister the specified memory region from vtophys address translation.
 *
 * The caller must ensure all in-flight DMA operations to this memory region
 * are completed or cancelled before calling this function.
 *
 * \param vaddr Virtual address to unregister.
 * \param len Length in bytes of the vaddr.
 *
 * \return 0 on success, negative errno on failure.
 */
int spdk_mem_unregister(void *vaddr, size_t len);

/**
 * Reserve the address space specified in all memory maps.
 *
 * This pre-allocates the necessary space in the memory maps such that
 * future calls to spdk_mem_register() on that region require no
 * internal memory allocations.
 *
 * \param vaddr Virtual address to reserve
 * \param len Length in bytes of vaddr
 *
 * \return 0 on success, negated errno on failure.
 */
int spdk_mem_reserve(void *vaddr, size_t len);

/**
 * Get the address's file descriptor and offset, it works with spdk memory allocation APIs
 *
 * \param vaddr Virtual address to get
 * \param offset Virtual address's map offset to the file descriptor
 *
 * \return negative errno on failure, otherwise return the file descriptor
 */
int spdk_mem_get_fd_and_offset(void *vaddr, uint64_t *offset);

enum spdk_pci_event_type {
	SPDK_UEVENT_ADD = 0,
	SPDK_UEVENT_REMOVE = 1,
};

struct spdk_pci_event {
	enum spdk_pci_event_type action;
	struct spdk_pci_addr traddr;
};

typedef void (*spdk_pci_error_handler)(siginfo_t *info, void *ctx);

/**
 * Begin listening for PCI bus events. This is used to detect hot-insert and
 * hot-remove events. Once the system is listening, events may be retrieved
 * by calling spdk_pci_get_event() periodically.
 *
 * \return negative errno on failure, otherwise,  return a file descriptor
 * that may be later passed to spdk_pci_get_event().
 */
int spdk_pci_event_listen(void);

/**
 * Get the next PCI bus event.
 *
 * \param fd A file descriptor returned by spdk_pci_event_listen()
 * \param event An event on the PCI bus
 *
 * \return Negative errno on failure. 0 for no event. A positive number
 * when an event has been returned
 */
int spdk_pci_get_event(int fd, struct spdk_pci_event *event);

/**
 * Register a signal handler to handle bus errors on the PCI bus
 *
 * \param sighandler Signal bus handler of the PCI bus
 * \param ctx The arg pass to the registered signal bus handler.
 *
 * \return negative errno on failure, otherwise it means successful
 */
int spdk_pci_register_error_handler(spdk_pci_error_handler sighandler, void *ctx);

/**
 * Register a signal handler to handle bus errors on the PCI bus
 *
 * \param sighandler Signal bus handler of the PCI bus
 */
void spdk_pci_unregister_error_handler(spdk_pci_error_handler sighandler);

#ifdef __cplusplus
}
#endif

#endif
