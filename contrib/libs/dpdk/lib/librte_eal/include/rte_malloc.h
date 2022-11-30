/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2019 Intel Corporation
 */

#ifndef _RTE_MALLOC_H_
#define _RTE_MALLOC_H_

/**
 * @file
 * RTE Malloc. This library provides methods for dynamically allocating memory
 * from hugepages.
 */

#include <stdio.h>
#include <stddef.h>
#include <rte_compat.h>
#include <rte_memory.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 *  Structure to hold heap statistics obtained from rte_malloc_get_socket_stats function.
 */
struct rte_malloc_socket_stats {
	size_t heap_totalsz_bytes; /**< Total bytes on heap */
	size_t heap_freesz_bytes;  /**< Total free bytes on heap */
	size_t greatest_free_size; /**< Size in bytes of largest free block */
	unsigned free_count;       /**< Number of free elements on heap */
	unsigned alloc_count;      /**< Number of allocated elements on heap */
	size_t heap_allocsz_bytes; /**< Total allocated bytes on heap */
};

/**
 * This function allocates memory from the huge-page area of memory. The memory
 * is not cleared. In NUMA systems, the memory allocated resides on the same
 * NUMA socket as the core that calls this function.
 *
 * @param type
 *   A string identifying the type of allocated objects (useful for debug
 *   purposes, such as identifying the cause of a memory leak). Can be NULL.
 * @param size
 *   Size (in bytes) to be allocated.
 * @param align
 *   If 0, the return is a pointer that is suitably aligned for any kind of
 *   variable (in the same manner as malloc()).
 *   Otherwise, the return is a pointer that is a multiple of *align*. In
 *   this case, it must be a power of two. (Minimum alignment is the
 *   cacheline size, i.e. 64-bytes)
 * @return
 *   - NULL on error. Not enough memory, or invalid arguments (size is 0,
 *     align is not a power of two).
 *   - Otherwise, the pointer to the allocated object.
 */
void *
rte_malloc(const char *type, size_t size, unsigned align)
	__rte_alloc_size(2);

/**
 * Allocate zero'ed memory from the heap.
 *
 * Equivalent to rte_malloc() except that the memory zone is
 * initialised with zeros. In NUMA systems, the memory allocated resides on the
 * same NUMA socket as the core that calls this function.
 *
 * @param type
 *   A string identifying the type of allocated objects (useful for debug
 *   purposes, such as identifying the cause of a memory leak). Can be NULL.
 * @param size
 *   Size (in bytes) to be allocated.
 * @param align
 *   If 0, the return is a pointer that is suitably aligned for any kind of
 *   variable (in the same manner as malloc()).
 *   Otherwise, the return is a pointer that is a multiple of *align*. In
 *   this case, it must obviously be a power of two. (Minimum alignment is the
 *   cacheline size, i.e. 64-bytes)
 * @return
 *   - NULL on error. Not enough memory, or invalid arguments (size is 0,
 *     align is not a power of two).
 *   - Otherwise, the pointer to the allocated object.
 */
void *
rte_zmalloc(const char *type, size_t size, unsigned align)
	__rte_alloc_size(2);

/**
 * Replacement function for calloc(), using huge-page memory. Memory area is
 * initialised with zeros. In NUMA systems, the memory allocated resides on the
 * same NUMA socket as the core that calls this function.
 *
 * @param type
 *   A string identifying the type of allocated objects (useful for debug
 *   purposes, such as identifying the cause of a memory leak). Can be NULL.
 * @param num
 *   Number of elements to be allocated.
 * @param size
 *   Size (in bytes) of a single element.
 * @param align
 *   If 0, the return is a pointer that is suitably aligned for any kind of
 *   variable (in the same manner as malloc()).
 *   Otherwise, the return is a pointer that is a multiple of *align*. In
 *   this case, it must obviously be a power of two. (Minimum alignment is the
 *   cacheline size, i.e. 64-bytes)
 * @return
 *   - NULL on error. Not enough memory, or invalid arguments (size is 0,
 *     align is not a power of two).
 *   - Otherwise, the pointer to the allocated object.
 */
void *
rte_calloc(const char *type, size_t num, size_t size, unsigned align)
	__rte_alloc_size(2, 3);

/**
 * Replacement function for realloc(), using huge-page memory. Reserved area
 * memory is resized, preserving contents. In NUMA systems, the new area
 * may not reside on the same NUMA node as the old one.
 *
 * @param ptr
 *   Pointer to already allocated memory
 * @param size
 *   Size (in bytes) of new area. If this is 0, memory is freed.
 * @param align
 *   If 0, the return is a pointer that is suitably aligned for any kind of
 *   variable (in the same manner as malloc()).
 *   Otherwise, the return is a pointer that is a multiple of *align*. In
 *   this case, it must obviously be a power of two. (Minimum alignment is the
 *   cacheline size, i.e. 64-bytes)
 * @return
 *   - NULL on error. Not enough memory, or invalid arguments (size is 0,
 *     align is not a power of two).
 *   - Otherwise, the pointer to the reallocated memory.
 */
void *
rte_realloc(void *ptr, size_t size, unsigned int align)
	__rte_alloc_size(2);

/**
 * Replacement function for realloc(), using huge-page memory. Reserved area
 * memory is resized, preserving contents. In NUMA systems, the new area
 * resides on requested NUMA socket.
 *
 * @param ptr
 *   Pointer to already allocated memory
 * @param size
 *   Size (in bytes) of new area. If this is 0, memory is freed.
 * @param align
 *   If 0, the return is a pointer that is suitably aligned for any kind of
 *   variable (in the same manner as malloc()).
 *   Otherwise, the return is a pointer that is a multiple of *align*. In
 *   this case, it must obviously be a power of two. (Minimum alignment is the
 *   cacheline size, i.e. 64-bytes)
 * @param socket
 *   NUMA socket to allocate memory on.
 * @return
 *   - NULL on error. Not enough memory, or invalid arguments (size is 0,
 *     align is not a power of two).
 *   - Otherwise, the pointer to the reallocated memory.
 */
__rte_experimental
void *
rte_realloc_socket(void *ptr, size_t size, unsigned int align, int socket)
	__rte_alloc_size(2, 3);

/**
 * This function allocates memory from the huge-page area of memory. The memory
 * is not cleared.
 *
 * @param type
 *   A string identifying the type of allocated objects (useful for debug
 *   purposes, such as identifying the cause of a memory leak). Can be NULL.
 * @param size
 *   Size (in bytes) to be allocated.
 * @param align
 *   If 0, the return is a pointer that is suitably aligned for any kind of
 *   variable (in the same manner as malloc()).
 *   Otherwise, the return is a pointer that is a multiple of *align*. In
 *   this case, it must be a power of two. (Minimum alignment is the
 *   cacheline size, i.e. 64-bytes)
 * @param socket
 *   NUMA socket to allocate memory on. If SOCKET_ID_ANY is used, this function
 *   will behave the same as rte_malloc().
 * @return
 *   - NULL on error. Not enough memory, or invalid arguments (size is 0,
 *     align is not a power of two).
 *   - Otherwise, the pointer to the allocated object.
 */
void *
rte_malloc_socket(const char *type, size_t size, unsigned align, int socket)
	__rte_alloc_size(2);

/**
 * Allocate zero'ed memory from the heap.
 *
 * Equivalent to rte_malloc() except that the memory zone is
 * initialised with zeros.
 *
 * @param type
 *   A string identifying the type of allocated objects (useful for debug
 *   purposes, such as identifying the cause of a memory leak). Can be NULL.
 * @param size
 *   Size (in bytes) to be allocated.
 * @param align
 *   If 0, the return is a pointer that is suitably aligned for any kind of
 *   variable (in the same manner as malloc()).
 *   Otherwise, the return is a pointer that is a multiple of *align*. In
 *   this case, it must obviously be a power of two. (Minimum alignment is the
 *   cacheline size, i.e. 64-bytes)
 * @param socket
 *   NUMA socket to allocate memory on. If SOCKET_ID_ANY is used, this function
 *   will behave the same as rte_zmalloc().
 * @return
 *   - NULL on error. Not enough memory, or invalid arguments (size is 0,
 *     align is not a power of two).
 *   - Otherwise, the pointer to the allocated object.
 */
void *
rte_zmalloc_socket(const char *type, size_t size, unsigned align, int socket)
	__rte_alloc_size(2);

/**
 * Replacement function for calloc(), using huge-page memory. Memory area is
 * initialised with zeros.
 *
 * @param type
 *   A string identifying the type of allocated objects (useful for debug
 *   purposes, such as identifying the cause of a memory leak). Can be NULL.
 * @param num
 *   Number of elements to be allocated.
 * @param size
 *   Size (in bytes) of a single element.
 * @param align
 *   If 0, the return is a pointer that is suitably aligned for any kind of
 *   variable (in the same manner as malloc()).
 *   Otherwise, the return is a pointer that is a multiple of *align*. In
 *   this case, it must obviously be a power of two. (Minimum alignment is the
 *   cacheline size, i.e. 64-bytes)
 * @param socket
 *   NUMA socket to allocate memory on. If SOCKET_ID_ANY is used, this function
 *   will behave the same as rte_calloc().
 * @return
 *   - NULL on error. Not enough memory, or invalid arguments (size is 0,
 *     align is not a power of two).
 *   - Otherwise, the pointer to the allocated object.
 */
void *
rte_calloc_socket(const char *type, size_t num, size_t size, unsigned align, int socket)
	__rte_alloc_size(2, 3);

/**
 * Frees the memory space pointed to by the provided pointer.
 *
 * This pointer must have been returned by a previous call to
 * rte_malloc(), rte_zmalloc(), rte_calloc() or rte_realloc(). The behaviour of
 * rte_free() is undefined if the pointer does not match this requirement.
 *
 * If the pointer is NULL, the function does nothing.
 *
 * @param ptr
 *   The pointer to memory to be freed.
 */
void
rte_free(void *ptr);

/**
 * If malloc debug is enabled, check a memory block for header
 * and trailer markers to indicate that all is well with the block.
 * If size is non-null, also return the size of the block.
 *
 * @param ptr
 *   pointer to the start of a data block, must have been returned
 *   by a previous call to rte_malloc(), rte_zmalloc(), rte_calloc()
 *   or rte_realloc()
 * @param size
 *   if non-null, and memory block pointer is valid, returns the size
 *   of the memory block
 * @return
 *   -1 on error, invalid pointer passed or header and trailer markers
 *   are missing or corrupted
 *   0 on success
 */
int
rte_malloc_validate(const void *ptr, size_t *size);

/**
 * Get heap statistics for the specified heap.
 *
 * @note This function is not thread-safe with respect to
 *    ``rte_malloc_heap_create()``/``rte_malloc_heap_destroy()`` functions.
 *
 * @param socket
 *   An unsigned integer specifying the socket to get heap statistics for
 * @param socket_stats
 *   A structure which provides memory to store statistics
 * @return
 *   Null on error
 *   Pointer to structure storing statistics on success
 */
int
rte_malloc_get_socket_stats(int socket,
		struct rte_malloc_socket_stats *socket_stats);

/**
 * Add memory chunk to a heap with specified name.
 *
 * @note Multiple memory chunks can be added to the same heap
 *
 * @note Before accessing this memory in other processes, it needs to be
 *   attached in each of those processes by calling
 *   ``rte_malloc_heap_memory_attach`` in each other process.
 *
 * @note Memory must be previously allocated for DPDK to be able to use it as a
 *   malloc heap. Failing to do so will result in undefined behavior, up to and
 *   including segmentation faults.
 *
 * @note Calling this function will erase any contents already present at the
 *   supplied memory address.
 *
 * @param heap_name
 *   Name of the heap to add memory chunk to
 * @param va_addr
 *   Start of virtual area to add to the heap. Must be aligned by ``page_sz``.
 * @param len
 *   Length of virtual area to add to the heap. Must be aligned by ``page_sz``.
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
 *     EPERM  - attempted to add memory to a reserved heap
 *     ENOSPC - no more space in internal config to store a new memory chunk
 */
__rte_experimental
int
rte_malloc_heap_memory_add(const char *heap_name, void *va_addr, size_t len,
		rte_iova_t iova_addrs[], unsigned int n_pages, size_t page_sz);

/**
 * Remove memory chunk from heap with specified name.
 *
 * @note Memory chunk being removed must be the same as one that was added;
 *   partially removing memory chunks is not supported
 *
 * @note Memory area must not contain any allocated elements to allow its
 *   removal from the heap
 *
 * @note All other processes must detach from the memory chunk prior to it being
 *   removed from the heap.
 *
 * @param heap_name
 *   Name of the heap to remove memory from
 * @param va_addr
 *   Virtual address to remove from the heap
 * @param len
 *   Length of virtual area to remove from the heap
 *
 * @return
 *   - 0 on success
 *   - -1 in case of error, with rte_errno set to one of the following:
 *     EINVAL - one of the parameters was invalid
 *     EPERM  - attempted to remove memory from a reserved heap
 *     ENOENT - heap or memory chunk was not found
 *     EBUSY  - memory chunk still contains data
 */
__rte_experimental
int
rte_malloc_heap_memory_remove(const char *heap_name, void *va_addr, size_t len);

/**
 * Attach to an already existing chunk of external memory in another process.
 *
 * @note This function must be called before any attempt is made to use an
 *   already existing external memory chunk. This function does *not* need to
 *   be called if a call to ``rte_malloc_heap_memory_add`` was made in the
 *   current process.
 *
 * @param heap_name
 *   Heap name to which this chunk of memory belongs
 * @param va_addr
 *   Start address of memory chunk to attach to
 * @param len
 *   Length of memory chunk to attach to
 * @return
 *   0 on successful attach
 *   -1 on unsuccessful attach, with rte_errno set to indicate cause for error:
 *     EINVAL - one of the parameters was invalid
 *     EPERM  - attempted to attach memory to a reserved heap
 *     ENOENT - heap or memory chunk was not found
 */
__rte_experimental
int
rte_malloc_heap_memory_attach(const char *heap_name, void *va_addr, size_t len);

/**
 * Detach from a chunk of external memory in secondary process.
 *
 * @note This function must be called in before any attempt is made to remove
 *   external memory from the heap in another process. This function does *not*
 *   need to be called if a call to ``rte_malloc_heap_memory_remove`` will be
 *   called in current process.
 *
 * @param heap_name
 *   Heap name to which this chunk of memory belongs
 * @param va_addr
 *   Start address of memory chunk to attach to
 * @param len
 *   Length of memory chunk to attach to
 * @return
 *   0 on successful detach
 *   -1 on unsuccessful detach, with rte_errno set to indicate cause for error:
 *     EINVAL - one of the parameters was invalid
 *     EPERM  - attempted to detach memory from a reserved heap
 *     ENOENT - heap or memory chunk was not found
 */
__rte_experimental
int
rte_malloc_heap_memory_detach(const char *heap_name, void *va_addr, size_t len);

/**
 * Creates a new empty malloc heap with a specified name.
 *
 * @note Heaps created via this call will automatically get assigned a unique
 *   socket ID, which can be found using ``rte_malloc_heap_get_socket()``
 *
 * @param heap_name
 *   Name of the heap to create.
 *
 * @return
 *   - 0 on successful creation
 *   - -1 in case of error, with rte_errno set to one of the following:
 *     EINVAL - ``heap_name`` was NULL, empty or too long
 *     EEXIST - heap by name of ``heap_name`` already exists
 *     ENOSPC - no more space in internal config to store a new heap
 */
__rte_experimental
int
rte_malloc_heap_create(const char *heap_name);

/**
 * Destroys a previously created malloc heap with specified name.
 *
 * @note This function will return a failure result if not all memory allocated
 *   from the heap has been freed back to the heap
 *
 * @note This function will return a failure result if not all memory segments
 *   were removed from the heap prior to its destruction
 *
 * @param heap_name
 *   Name of the heap to create.
 *
 * @return
 *   - 0 on success
 *   - -1 in case of error, with rte_errno set to one of the following:
 *     EINVAL - ``heap_name`` was NULL, empty or too long
 *     ENOENT - heap by the name of ``heap_name`` was not found
 *     EPERM  - attempting to destroy reserved heap
 *     EBUSY  - heap still contains data
 */
__rte_experimental
int
rte_malloc_heap_destroy(const char *heap_name);

/**
 * Find socket ID corresponding to a named heap.
 *
 * @param name
 *   Heap name to find socket ID for
 * @return
 *   Socket ID in case of success (a non-negative number)
 *   -1 in case of error, with rte_errno set to one of the following:
 *     EINVAL - ``name`` was NULL
 *     ENOENT - heap identified by the name ``name`` was not found
 */
__rte_experimental
int
rte_malloc_heap_get_socket(const char *name);

/**
 * Check if a given socket ID refers to externally allocated memory.
 *
 * @note Passing SOCKET_ID_ANY will return 0.
 *
 * @param socket_id
 *   Socket ID to check
 * @return
 *   1 if socket ID refers to externally allocated memory
 *   0 if socket ID refers to internal DPDK memory
 *   -1 if socket ID is invalid
 */
__rte_experimental
int
rte_malloc_heap_socket_is_external(int socket_id);

/**
 * Dump statistics.
 *
 * Dump for the specified type to a file. If the type argument is
 * NULL, all memory types will be dumped.
 *
 * @note This function is not thread-safe with respect to
 *    ``rte_malloc_heap_create()``/``rte_malloc_heap_destroy()`` functions.
 *
 * @param f
 *   A pointer to a file for output
 * @param type
 *   A string identifying the type of objects to dump, or NULL
 *   to dump all objects.
 */
void
rte_malloc_dump_stats(FILE *f, const char *type);

/**
 * Dump contents of all malloc heaps to a file.
 *
 * @note This function is not thread-safe with respect to
 *    ``rte_malloc_heap_create()``/``rte_malloc_heap_destroy()`` functions.
 *
 * @param f
 *   A pointer to a file for output
 */
__rte_experimental
void
rte_malloc_dump_heaps(FILE *f);

/**
 * Set the maximum amount of allocated memory for this type.
 *
 * This is not yet implemented
 *
 * @param type
 *   A string identifying the type of allocated objects.
 * @param max
 *   The maximum amount of allocated bytes for this type.
 * @return
 *   - 0: Success.
 *   - (-1): Error.
 */
__rte_deprecated
int
rte_malloc_set_limit(const char *type, size_t max);

/**
 * Return the IO address of a virtual address obtained through
 * rte_malloc
 *
 * @param addr
 *   Address obtained from a previous rte_malloc call
 * @return
 *   RTE_BAD_IOVA on error
 *   otherwise return an address suitable for IO
 */
rte_iova_t
rte_malloc_virt2iova(const void *addr);

#ifdef __cplusplus
}
#endif

#endif /* _RTE_MALLOC_H_ */
