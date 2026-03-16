/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2009      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2011      NVIDIA Corporation.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
  * @file
  * Description of the Memory Pool framework
  */
#ifndef MCA_MPOOL_H
#define MCA_MPOOL_H
#include "opal_config.h"
#include "opal/mca/mca.h"
#include "opal/class/opal_free_list.h"
#include "opal/mca/rcache/base/rcache_base_vma.h"

#include "opal/mca/crs/crs.h"
#include "opal/mca/crs/base/base.h"

#define MCA_MPOOL_ALLOC_FLAG_DEFAULT      0x00
#define MCA_MPOOL_ALLOC_FLAG_USER         0x01

#define MCA_MPOOL_FLAGS_MPI_ALLOC_MEM     0x80

struct opal_info_t;
struct mca_mpool_base_module_t;
typedef struct mca_mpool_base_module_t mca_mpool_base_module_t;

/**
 * component query function
 *
 * @param[in]  hints      memory pool hints in order of priority. this should
 *                        be replaced by opal_info_t when the work to move
 *                        info down to opal is complete.
 * @param[out] priority   relative priority of this memory pool component
 * @param[out] module     best match module
 *
 * This function should parse the provided hints and return a relative priority
 * of the component based on the number of hints matched. For example, if the
 * hints are "page_size=2M,high-bandwidth" and a pool matches the page_size but
 * not the high-bandwidth hint then the component should return a lower priority
 * than if both matched but a higher priority than if a pool matches only the
 * high-bandwidth hint.
 *
 * Memory pools should try to support at a minimum name=value but can define
 * any additional keys.
 */
typedef int (*mca_mpool_base_component_query_fn_t) (const char *hints, int *priority,
                                                    mca_mpool_base_module_t **module);

/**
  * allocate function typedef
  */
typedef void *(*mca_mpool_base_module_alloc_fn_t) (mca_mpool_base_module_t *mpool,
                                                   size_t size, size_t align,
                                                   uint32_t flags);

/**
  * allocate function typedef
  */
typedef void *(*mca_mpool_base_module_realloc_fn_t) (mca_mpool_base_module_t *mpool,
                                                     void *addr, size_t size);

/**
  * free function typedef
  */
typedef void (*mca_mpool_base_module_free_fn_t) (mca_mpool_base_module_t *mpool,
                                                 void *addr);

/**
  * if appropriate - returns base address of memory pool
  */
typedef void* (*mca_mpool_base_module_address_fn_t) (mca_mpool_base_module_t *mpool);

/**
  * finalize
  */
typedef void (*mca_mpool_base_module_finalize_fn_t)(mca_mpool_base_module_t *mpool);


/**
 * Fault Tolerance Event Notification Function
 * @param state Checkpoint Stae
 * @return OPAL_SUCCESS or failure status
 */
typedef int (*mca_mpool_base_module_ft_event_fn_t)(int state);


/**
 * mpool component descriptor. Contains component version information
 * and open/close/init functions.
 */
struct mca_mpool_base_component_2_0_0_t {
    mca_base_component_t mpool_version;        /**< version */
    mca_base_component_data_t mpool_data;/**< metadata */

    mca_mpool_base_component_query_fn_t mpool_query;  /**< query for matching pools */
};
/**
 * Convenience typedef.
 */
typedef struct mca_mpool_base_component_2_0_0_t mca_mpool_base_component_2_0_0_t;
/**
  * Convenience typedef
  */
typedef struct mca_mpool_base_component_2_0_0_t mca_mpool_base_component_t;

/**
 *  mpool module descriptor. Contains the interface functions exported
 *  by the component.  This does not expose memory management
 *  details.
 */
struct mca_mpool_base_module_t {
    mca_mpool_base_component_t *mpool_component;         /**< component stuct */
    mca_mpool_base_module_address_fn_t mpool_base;       /**< returns the base address */
    mca_mpool_base_module_alloc_fn_t mpool_alloc;        /**< allocate function */
    mca_mpool_base_module_realloc_fn_t mpool_realloc;    /**< reallocate function */
    mca_mpool_base_module_free_fn_t mpool_free;          /**< free function */

    mca_mpool_base_module_finalize_fn_t mpool_finalize;  /**< finalize */
    mca_mpool_base_module_ft_event_fn_t mpool_ft_event;  /**< ft_event */
    uint32_t flags; /**< mpool flags */

    size_t mpool_allocation_unit;                        /**< allocation unit used by this mpool */
    char *mpool_name; /**< name of this pool module */
};


/**
 * Function to allocate special memory according to what the user requests in
 * the info object.
 *
 * If the user passes in a valid info structure then the function will
 * try to allocate the memory and register it with every mpool that there is a
 * key for it in the info struct. If it fails at registering the memory with
 * one of the requested mpools, an error will be returned. Also, if there is a
 * key in info that does not match any mpool, an error will be returned.
 *
 * If the info parameter is MPI_INFO_NULL, then this function will try to allocate
 * the memory and register it wih as many mpools as possible. However,
 * if any of the registratons fail the mpool will simply be ignored.
 *
 * @param size the size of the memory area to allocate
 * @param info an info object which tells us what kind of memory to allocate
 *
 * @retval pointer to the allocated memory
 * @retval NULL on failure
 */
OPAL_DECLSPEC void * mca_mpool_base_alloc(size_t size, struct opal_info_t * info, const char *hints);

/**
 * Function to free memory previously allocated by mca_mpool_base_alloc
 *
 * @param base pointer to the memory to free
 *
 * @retval OPAL_SUCCESS
 * @retval OPAL_ERR_BAD_PARAM if the passed base pointer was invalid
 */
OPAL_DECLSPEC int mca_mpool_base_free(void * base);

/**
 * Function for the red black tree to compare 2 keys
 *
 * @param key1 a pointer to the 1st key
 * @param key2 a pointer to the second key
 *
 * @retval -1 if key1 is below key2
 * @retval 1 if key 1 is above key2
 * @retval 0 if the keys are the same
 */
OPAL_DECLSPEC int mca_mpool_base_tree_node_compare(void * key1, void * key2);

/**
 * Macro for use in components that are of type mpool
 */
#define MCA_MPOOL_BASE_VERSION_3_0_0 \
    OPAL_MCA_BASE_VERSION_2_1_0("mpool", 3, 0, 0)

#endif /* MCA_MPOOL_H */

