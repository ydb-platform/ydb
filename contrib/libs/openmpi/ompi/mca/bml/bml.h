/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2009 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012-2015 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 *
 * BML Management Layer (BML)
 *
 */

#ifndef MCA_BML_H
#define MCA_BML_H

#include "ompi_config.h"
#include "ompi/mca/mca.h"
#include "opal/datatype/opal_convertor.h"
#include "opal/mca/crs/crs.h"
#include "opal/mca/crs/base/base.h"
#include "opal/mca/btl/btl.h"

#include "ompi/mca/bml/base/bml_base_btl.h"
#include "ompi/types.h"

#include "ompi/constants.h"

#define OPAL_ENABLE_DEBUG_RELIABILITY 0

/*
 * BML types
 */

struct ompi_proc_t;
struct mca_bml_base_module_t;
struct mca_bml_base_endpoint_t;
struct mca_mpool_base_resources_t;

/*
 * Cached set of information for each btl
 */

struct mca_bml_base_btl_t {
    uint32_t  btl_flags;                             /**< support for put/get? */
    float     btl_weight;                            /**< BTL weight for scheduling */
    struct    mca_btl_base_module_t *btl;            /**< BTL module */
    struct    mca_btl_base_endpoint_t* btl_endpoint; /**< BTL addressing info */
};
typedef struct mca_bml_base_btl_t mca_bml_base_btl_t;



/**
 * A dynamically growable array of mca_bml_base_btl_t instances.
 * Maintains an index into the array that is used for round-robin
 * scheduling across contents.
 */
struct mca_bml_base_btl_array_t {
    opal_object_t super;
    size_t arr_size;                     /**< number available */
    size_t arr_reserve;                  /**< size of allocated btl_proc array */
    size_t arr_index;                    /**< last used index*/
    mca_bml_base_btl_t* bml_btls;   /**< array of  bml btl's */
};
typedef struct mca_bml_base_btl_array_t mca_bml_base_btl_array_t;

OMPI_DECLSPEC OBJ_CLASS_DECLARATION(mca_bml_base_btl_array_t);


/**
 * If required, reallocate (grow) the array to the indicate size.
 *
 * @param array (IN)
 * @param size (IN)
 */
static inline size_t mca_bml_base_btl_array_get_size(mca_bml_base_btl_array_t* array)
{
    return array->arr_size;
}

/**
 * Grow the array if required, and set the size.
 *
 * @param array (IN)
 * @param size (IN)
 */
static inline void mca_bml_base_btl_array_set_size(mca_bml_base_btl_array_t* array, size_t size)
{
    if(array->arr_size > array->arr_reserve)
        mca_bml_base_btl_array_reserve(array, size);
    array->arr_size = size;
}

/**
 * Grow the array size by one and return the item at that index.
 *
 * @param array (IN)
 */
static inline mca_bml_base_btl_t* mca_bml_base_btl_array_insert(mca_bml_base_btl_array_t* array)
{
#if OPAL_ENABLE_DEBUG
    if(array->arr_size >= array->arr_reserve) {
        opal_output(0, "mca_bml_base_btl_array_insert: invalid array index %lu >= %lu",
                    (unsigned long)array->arr_size, (unsigned long)array->arr_reserve);
        return 0;
    }
#endif
    return &array->bml_btls[array->arr_size++];
}

/**
 * Remove a btl from a bml_btl
 *
 * @param array (IN)
 * @param btl (IN)
 */
static inline bool mca_bml_base_btl_array_remove( mca_bml_base_btl_array_t* array,
                                                  struct mca_btl_base_module_t* btl )
{
    size_t i = 0;
    /* find the btl */
    for( i = 0; i < array->arr_size; i++ ) {
        if( array->bml_btls[i].btl == btl ) {
            /* make sure not to go out of bounds */
            for( ; i < array->arr_size-1; i++ ) {
                /* move all btl's back by 1, so the found
                   btl is "removed" */
                array->bml_btls[i] = array->bml_btls[(i+1)];
            }
            array->arr_size--;
            array->arr_index = 0;
            return true;
        }
    }
    return false;
}


/**
 * Return an array item at the specified index.
 *
 * @param array (IN)
 * @param item_index (IN)
 */
static inline mca_bml_base_btl_t* mca_bml_base_btl_array_get_index(mca_bml_base_btl_array_t* array, size_t item_index)
{
    if (item_index < array->arr_size) {
        return &array->bml_btls[item_index];
    }

    return NULL;
}

/**
 * Return the next LRU index in the array.
 *
 * @param array (IN)
 *
 * @param index (OUT)
 */
static inline mca_bml_base_btl_t* mca_bml_base_btl_array_get_next(mca_bml_base_btl_array_t* array)
{
#if OPAL_ENABLE_DEBUG
    if(array->arr_size == 0) {
        opal_output(0, "mca_bml_base_btl_array_get_next: invalid array size");
        return 0;
    }
#endif
    if( 1 == array->arr_size ) {
        return &array->bml_btls[0];  /* force the return to avoid a jump */
    } else {
        size_t current_position = array->arr_index;  /* force to always start from zero */
        if( (current_position + 1) == array->arr_size ) {
            array->arr_index = 0;  /* next time serve from the beginning */
        } else {
            array->arr_index = current_position + 1;  /* continue */
        }
        return &array->bml_btls[current_position];
    }
}

/**
 * Locate an element in the array
 *
 * @param array (IN)
 * @param index (IN)
 */
static inline mca_bml_base_btl_t* mca_bml_base_btl_array_find(
    mca_bml_base_btl_array_t* array, struct mca_btl_base_module_t* btl)
{
    size_t i=0;
    for(i=0; i<array->arr_size; i++) {
        if(array->bml_btls[i].btl == btl) {
            return &array->bml_btls[i];
        }
    }
    return NULL;
}

/**
 *  Structure associated w/ ompi_proc_t that contains the set
 *  of BTLs used to reach a destination
 */
struct mca_bml_base_endpoint_t {
    opal_list_item_t         super;             /**< base_endpoint is a list item */
    struct ompi_proc_t*      btl_proc;          /**< backpointer to target ompi_proc_t */
    size_t                   btl_pipeline_send_length;   /**< max of pipeline send_length of available BTLs */
    size_t                   btl_send_limit;    /**< max of min rdma pipeline for available rmda btls */
    size_t                   btl_max_send_size; /**< min of max send size for available send btls */
    mca_bml_base_btl_array_t btl_eager;         /**< array of btls to use for first fragments */
    mca_bml_base_btl_array_t btl_send;          /**< array of btls to use for remaining fragments */
    mca_bml_base_btl_array_t btl_rdma;          /**< array of btls that support (prefer) rdma */
    size_t                   btl_rdma_index;    /**< index of last used BTL for RDMA */
    uint32_t                 btl_flags_or;      /**< the bitwise OR of the btl flags */
};
typedef struct mca_bml_base_endpoint_t mca_bml_base_endpoint_t;


OMPI_DECLSPEC OBJ_CLASS_DECLARATION(mca_bml_base_endpoint_t);

static inline void mca_bml_base_alloc( mca_bml_base_btl_t* bml_btl,
                                       mca_btl_base_descriptor_t** des,
                                       uint8_t order, size_t size, uint32_t flags )
{
    mca_btl_base_module_t* btl = bml_btl->btl;
    *des = btl->btl_alloc(btl, bml_btl->btl_endpoint, order, size, flags);
}

static inline void mca_bml_base_free( mca_bml_base_btl_t* bml_btl,
                                      mca_btl_base_descriptor_t* des )
{
    mca_btl_base_module_t* btl = bml_btl->btl;

    btl->btl_free( btl, des );
    /* The previous function is supposed to release the des object
     * so we should not touch it anymore.
     */
}

#if OPAL_ENABLE_DEBUG_RELIABILITY

int mca_bml_base_send( mca_bml_base_btl_t* bml_btl,
                       mca_btl_base_descriptor_t* des,
                       mca_btl_base_tag_t tag );


#else

static inline int mca_bml_base_send( mca_bml_base_btl_t* bml_btl,
                                     mca_btl_base_descriptor_t* des,
                                     mca_btl_base_tag_t tag )
{
    int rc;
    mca_btl_base_module_t* btl = bml_btl->btl;

    des->des_context = (void*) bml_btl;
    rc = btl->btl_send(btl, bml_btl->btl_endpoint, des, tag);
    if (rc == OMPI_ERR_RESOURCE_BUSY)
        rc = OMPI_SUCCESS;

    return rc;
}

#endif

static inline int mca_bml_base_send_status( mca_bml_base_btl_t* bml_btl,
                                            mca_btl_base_descriptor_t* des,
                                            mca_btl_base_tag_t tag )
{
    mca_btl_base_module_t* btl = bml_btl->btl;

    des->des_context = (void*) bml_btl;
    return btl->btl_send(btl, bml_btl->btl_endpoint, des, tag);
}

static inline int  mca_bml_base_sendi( mca_bml_base_btl_t* bml_btl,
                                       struct opal_convertor_t* convertor,
                                       void* header,
                                       size_t header_size,
                                       size_t payload_size,
                                       uint8_t order,
                                       uint32_t flags,
                                       mca_btl_base_tag_t tag,
                                       mca_btl_base_descriptor_t** descriptor )
{
    mca_btl_base_module_t* btl = bml_btl->btl;
    return btl->btl_sendi(btl, bml_btl->btl_endpoint,
                          convertor, header, header_size,
                          payload_size, order, flags, tag, descriptor);
}

static inline int mca_bml_base_put( mca_bml_base_btl_t* bml_btl, void *local_address, uint64_t remote_address,
                                    struct mca_btl_base_registration_handle_t *local_handle,
                                    struct mca_btl_base_registration_handle_t *remote_handle, size_t size,
                                    int flags, int order, mca_btl_base_rdma_completion_fn_t cbfunc, void *cbdata)
{
    mca_btl_base_module_t* btl = bml_btl->btl;

    return btl->btl_put( btl, bml_btl->btl_endpoint, local_address, remote_address, local_handle,
                         remote_handle, size, flags, order, cbfunc, (void *) bml_btl, cbdata);
}

static inline int mca_bml_base_get( mca_bml_base_btl_t* bml_btl, void *local_address, uint64_t remote_address,
                                    struct mca_btl_base_registration_handle_t *local_handle,
                                    struct mca_btl_base_registration_handle_t *remote_handle, size_t size,
                                    int flags, int order, mca_btl_base_rdma_completion_fn_t cbfunc, void *cbdata)
{
    mca_btl_base_module_t* btl = bml_btl->btl;

    return btl->btl_get( btl, bml_btl->btl_endpoint, local_address, remote_address, local_handle,
                         remote_handle, size, flags, order, cbfunc, (void *) bml_btl, cbdata);
}


static inline void mca_bml_base_prepare_src(mca_bml_base_btl_t* bml_btl,
                                            struct opal_convertor_t* conv,
                                            uint8_t order,
                                            size_t reserve,
                                            size_t *size,
                                            uint32_t flags,
                                            mca_btl_base_descriptor_t** des)
{
    mca_btl_base_module_t* btl = bml_btl->btl;

    *des = btl->btl_prepare_src( btl, bml_btl->btl_endpoint, conv,
                                 order, reserve, size, flags );
    if( OPAL_LIKELY((*des) != NULL) ) {
        (*des)->des_context = (void*) bml_btl;
    }
}

static inline void mca_bml_base_register_mem (mca_bml_base_btl_t* bml_btl, void *base,
                                              size_t size, uint32_t flags,
                                              mca_btl_base_registration_handle_t **handle)
{
    mca_btl_base_module_t* btl = bml_btl->btl;

    *handle =  btl->btl_register_mem (btl, bml_btl->btl_endpoint, base, size, flags);
}

static inline void mca_bml_base_deregister_mem (mca_bml_base_btl_t* bml_btl, mca_btl_base_registration_handle_t *handle)
{
    mca_btl_base_module_t* btl = bml_btl->btl;

    btl->btl_deregister_mem (btl, handle);
}

/*
 *  BML component interface functions and datatype.
 */

/**
 * MCA->BML Initializes the BML component and creates specific BML
 * module(s).
 *
 * @param num_bmls (OUT) Returns the number of bml modules created, or 0
 *                       if the transport is not available.
 *
 * @param enable_progress_threads (IN) Whether this component is
 * allowed to run a hidden/progress thread or not.
 *
 * @param enable_mpi_threads (IN) Whether support for multiple MPI
 * threads is enabled or not (i.e., MPI_THREAD_MULTIPLE), which
 * indicates whether multiple threads may invoke this component
 * simultaneously or not.
 *
 * @return Array of pointers to BML modules, or NULL if the transport
 *         is not available.
 *
 * During component initialization, the BML component should discover
 * the physical devices that are available for the given transport,
 * and create a BML module to represent each device. Any addressing
 * information required by peers to reach the device should be published
 * during this function via the mca_base_modex_send() interface.
 *
 */

typedef struct mca_bml_base_module_t* (*mca_bml_base_component_init_fn_t)(
                                                                          int* priority,
                                                                          bool enable_progress_threads,
                                                                          bool enable_mpi_threads
                                                                          );

/**
 *  BML component descriptor. Contains component version information
 *  and component open/close/init functions.
 */

struct mca_bml_base_component_2_0_0_t {
  mca_base_component_t bml_version;
  mca_base_component_data_t bml_data;
  mca_bml_base_component_init_fn_t bml_init;
};
typedef struct mca_bml_base_component_2_0_0_t mca_bml_base_component_2_0_0_t;
typedef struct mca_bml_base_component_2_0_0_t mca_bml_base_component_t;


/*
 * BML module interface functions and datatype.
 */

/**
 * MCA->BML Clean up any resources held by BML module
 * before the module is unloaded.
 *
 * @param bml (IN)   BML module.
 *
 * Prior to unloading a BML module, the MCA framework will call
 * the BML finalize method of the module. Any resources held by
 * the BML should be released and if required the memory corresponding
 * to the BML module freed.
 *
 */
typedef int (*mca_bml_base_module_finalize_fn_t)( void );

/**
 * PML->BML notification of change in the process list.
 *
 * @param nprocs (IN)         Number of processes
 * @param procs (IN)          Set of processes
 * @param reachable (OUT)     Bitmask indicating set of peer processes that are reachable by this BML.
 * @return                    OMPI_SUCCESS or error status on failure.
 *
 * The mca_bml_base_module_add_procs_fn_t() is called by the PML to
 * determine the set of BTLs that should be used to reach each process.
 * Any addressing information exported by the peer via the mca_base_modex_send()
 * function should be available during this call via the corresponding
 * mca_base_modex_recv() function. The BML may utilize this information to
 * determine reachability of each peer process.
 *
 * For each process that is reachable by the BML, the bit corresponding to the index
 * into the proc array (nprocs) should be set in the reachable bitmask. The PML
 * provides the BML the option to return a pointer to a data structure defined
 * by the BML that is returned to the BML on subsequent calls to the BML data
 * transfer functions (e.g bml_send). This may be used by the BML to cache any addressing
 * or connection information (e.g. TCP socket, IP queue pair).
 *
 * \note This function will return OMPI_ERR_UNREACH if one or more
 * processes can not be reached by the currently active BTLs.  This is
 * not a fatal error, and the calling layer is free to continue using
 * the BML interface.
 */
typedef int (*mca_bml_base_module_add_procs_fn_t)(
                                                  size_t nprocs,
                                                  struct ompi_proc_t** procs,
                                                  struct opal_bitmap_t* reachable
                                                  );

/**
 * PML->BML notification of change in the process list.
 *
 * @param proc (IN)           Process
 * @return                    OMPI_SUCCESS or error status on failure.
 *
 * The mca_bml_base_module_add_proc_fn_t() is called by the PML to
 * determine the set of BTLs that should be used to reach each process.
 * Any addressing information exported by the peer via the mca_base_modex_send()
 * function should be available during this call via the corresponding
 * mca_base_modex_recv() function. The BML may utilize this information to
 * determine reachability of each peer process.
 *
 * \note This function will return OMPI_ERR_UNREACH if the process can not
 * be reached by a currently active BTL. This is not a fatal error, and the
 * calling layer is free to continue using the BML interface.
 */
typedef int (*mca_bml_base_module_add_proc_fn_t) (struct ompi_proc_t *proc);

/**
 * Notification of change to the process list.
 *
 * @param nprocs (IN)  Number of processes
 * @param proc (IN)    Set of processes
 * @return             Status indicating if cleanup was successful
 *
 * When the process list changes, the PML notifies the BML of the
 * change, to provide the opportunity to cleanup or release any
 * resources associated with the peer.
 */
typedef int (*mca_bml_base_module_del_procs_fn_t)(
                                                  size_t nprocs,
                                                  struct ompi_proc_t** procs
                                                  );

/**
 * Notification of change to the btl list.
 *
 * @param bml (IN)     BTL module
 * @return             Status indicating if cleanup was successful
 *
 * On recovery of a btl, add it to the set of forwarding
 * entries used by the BML.
 */
typedef int (*mca_bml_base_module_add_btl_fn_t)( struct mca_btl_base_module_t* );

/**
 * Notification of change to the btl list.
 *
 * @param bml (IN)     BTL module
 * @return             Status indicating if cleanup was successful
 *
 * On failure of a btl, remove it from the set of forwarding
 * entries used by the BML.
 */
typedef int (*mca_bml_base_module_del_btl_fn_t)( struct mca_btl_base_module_t* );

/**
 * Notification of change to the btl list.
 *
 * @param bml (IN)     BTL module
 * @return             Status indicating if cleanup was successful
 *
 * On failure of a btl, remove it from the set of forwarding
 * entries used by the BML.
 */
typedef int (*mca_bml_base_module_del_proc_btl_fn_t)(
    struct ompi_proc_t*,
    struct mca_btl_base_module_t* );

/**
 * Register a callback function that is called on receipt
 * of a fragment.
 *
 * @param bml (IN)     BML module
 * @return             Status indicating if cleanup was successful
 *
 * When the process list changes, the PML notifies the BML of the
 * change, to provide the opportunity to cleanup or release any
 * resources associated with the peer.
 */
typedef int (*mca_bml_base_module_register_fn_t)(
                                                 mca_btl_base_tag_t tag,
                                                 mca_btl_base_module_recv_cb_fn_t cbfunc,
                                                 void* cbdata
                                                 );
/**
 * Register a callback function that is called of error.
 *
 * @param bml (IN)     BML module
 * @return             Status indicating if cleanup was successful
 *
 */
typedef int (*mca_bml_base_module_register_error_cb_fn_t)(
        mca_btl_base_module_error_cb_fn_t cbfunc
);

/**
 * Fault Tolerance Event Notification Function
 * @param status Checkpoint Status
 * @return OMPI_SUCCESS or failure status
 */
typedef int (*mca_bml_base_module_ft_event_fn_t)(int status);


/**
 * BML module interface functions and attributes.
 */
struct mca_bml_base_module_t {
    /* BML common attributes */
    mca_bml_base_component_t* bml_component; /**< pointer back to the BML component structure */

    /* BML function table */
    mca_bml_base_module_add_proc_fn_t      bml_add_proc;
    mca_bml_base_module_add_procs_fn_t     bml_add_procs;
    mca_bml_base_module_del_procs_fn_t     bml_del_procs;
    mca_bml_base_module_add_btl_fn_t       bml_add_btl;
    mca_bml_base_module_del_btl_fn_t       bml_del_btl;
    mca_bml_base_module_del_proc_btl_fn_t  bml_del_proc_btl;
    mca_bml_base_module_register_fn_t      bml_register;
    mca_bml_base_module_register_error_cb_fn_t bml_register_error;

    mca_bml_base_module_finalize_fn_t      bml_finalize;

    mca_bml_base_module_ft_event_fn_t      bml_ft_event;
};
typedef struct mca_bml_base_module_t mca_bml_base_module_t;

/*
 * Macro for use in modules that are of type bml
 */
#define MCA_BML_BASE_VERSION_2_0_0 \
    OMPI_MCA_BASE_VERSION_2_1_0("bml", 2, 0, 0)

#endif /* OMPI_MCA_BML_H */
