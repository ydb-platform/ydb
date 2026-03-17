/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file@
 *
 * Hooks for receiving callbacks when memory is allocated or deallocated
 *
 * Hooks for receiving callbacks when memory is allocated or
 * deallocated from the current process.  Intended to be used with
 * RDMA communication devices that require "pinning" of virtual
 * memory.  The hooks allow for a "lazy unpinning" approach, which
 * provides better latency when application buffer reuse is high.
 * Most operating systems do not respond well to memory being freed
 * from a process while still pinned, so some type of callback to
 * unpin is necessary before the memory is returned to the OS.
 *
 * \note For linking reasons, this is not a component framework (some of
 * these require tight coupling into libopal and the wrapper compilers
 * and that entire stack).
 */

#ifndef OPAL_MEMORY_MEMORY_H
#define OPAL_MEMORY_MEMORY_H

#include "opal_config.h"
#include "memory_internal.h"

BEGIN_C_DECLS


/**
 * Initialize the memory hooks subsystem
 *
 * Initialize the memory hooks subsystem.  This is generally called
 * during opal_init() and should be called before any other function
 * in the interface is called.
 *
 * \note Note that some back-end functionality is activated pre-main,
 * so not calling this function does not prevent the memory hooks from
 * becoming active.
 *
 * @retval OPAL_SUCCESS Initialization completed successfully
 */
OPAL_DECLSPEC int opal_mem_hooks_init(void);


/**
 * Finalize the memory hooks subsystem
 *
 * Finalize the memory hooks subsystem.  This is generally called
 * during opal_finalize() and no other memory hooks functions should
 * be called after this function is called.  opal_mem_hooks_finalize()
 * will automatically deregister any callbacks that have not already
 * been deregistered.  In a multi-threaded application, it is possible
 * that one thread will have a memory hook callback while the other
 * thread is in opal_mem_hooks_finalize(), however, no threads will
 * receive a callback once the calling thread has exited
 * opal_mem_hooks_finalize().
 *
 * @retval OPAL_SUCCESS Shutdown completed successfully
 */
OPAL_DECLSPEC int opal_mem_hooks_finalize(void);


/**
 * Query level of support provided by memory hooks
 *
 * Query memory hooks subsystem about the level of support provided by
 * the current system configuration.  The return value is \c 0 if no
 * support is provided or a bit-wise OR of the available return values
 * if support is provided.
 *
 * @retval OPAL_MEMORY_FREE_SUPPORT   Memory hooks subsytem can trigger
 *                                    callback events when memory is going
 *                                    to be released by the process, either
 *                                    by the user calling an allocator
 *                                    function or munmap.  Implies
 *                                    OPAL_MEMORY_MUNMAP_SUPPORT.
 * @retval OPAL_MEMORY_MUNMAP_SUPPORT Subsystem can trigger callback events
 *                                    by the user calling munmap directly.
 * @retval OPAL_MEMORY_CHUNK_SUPPORT  Memory hooks subsystem will only
 *                                    trigger callback events when the
 *                                    process is giving memory back to the
 *                                    operating system, not at ever call
 *                                    to realloc/free/etc.
 *
 * \note This function must be called after opal_mem_hooks_init().
 */
OPAL_DECLSPEC int opal_mem_hooks_support_level(void);


/**
 * Memory status change callback
 *
 * Typedef for callbacks triggered when memory has been allocated or
 * is about to be freed.  The callback will be triggered according to
 * the note in opal_mem_hooks_register_alloc() or
 * opal_mem_hooks_register_release().
 *
 * @param buf     Pointer to the start of the allocation
 * @param lentgh  Length of the allocation
 * @param cbdata  Data passed to memory hooks when callback
 *                was registered
 * @param from_alloc True if the callback is caused by a call to the
 *                general allocation routines (malloc, calloc, free,
 *                etc.) or directly from the user (mmap, munmap, etc.)
 */
typedef void (opal_mem_hooks_callback_fn_t)(void *buf, size_t length,
                                            void *cbdata, bool from_alloc);


/**
 * Register callback for when memory is to be released
 *
 * Register a \c opal_mem_hooks_callback_fn_t function pointer to be called
 * whenever the current process is about to release memory.
 *
 * @param func    Function pointer to call when memory is to be released
 * @param cbdata  A pointer-length field to be passed to func when it is
 *                invoked.
 *
 * @retval OPAL_SUCCESS The registration completed successfully.
 * @retval OPAL_EXISTS  The function is already registered and will not
 *                      be registered again.
 * @retval OPAL_ERR_NOT_SUPPORTED There are no hooks available for
 *                      receiving callbacks when memory is to be released
 */
OPAL_DECLSPEC int opal_mem_hooks_register_release(opal_mem_hooks_callback_fn_t *func,
                                                  void *cbdata);

/**
 * Unregister previously registered release callback
 *
 * Unregister previously registered release callback.
 *
 * @param func   Function pointer to registered callback to remove
 *
 * @retval OPAL_SUCCESS The function was successfully deregistered
 * @retval OPAL_ERR_NOT_FOUND The function was not previously registered
 */
OPAL_DECLSPEC int opal_mem_hooks_unregister_release(opal_mem_hooks_callback_fn_t *func);


END_C_DECLS

#endif /* OPAL_MEMORY_MEMORY_H */
