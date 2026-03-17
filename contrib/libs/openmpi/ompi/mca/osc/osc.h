/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2004-2011 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2015 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016-2017 IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 *
 * One-sided Communication interface
 *
 * Interface for implementing the one-sided communication chapter of
 * the MPI-2 standard.  Similar in scope to the PML for point-to-point
 * communication from MPI-1.
 */

#ifndef OMPI_MCA_OSC_OSC_H
#define OMPI_MCA_OSC_OSC_H

#include "opal_config.h"

#include <stddef.h>

#include "ompi/mca/mca.h"

BEGIN_C_DECLS


/* ******************************************************************** */


struct ompi_win_t;
struct opal_info_t;
struct ompi_communicator_t;
struct ompi_group_t;
struct ompi_datatype_t;
struct ompi_op_t;
struct ompi_request_t;

/* ******************************************************************** */


/**
 * OSC component initialization
 *
 * Initialize the given one-sided component.  This function should
 * initialize any component-level data.

 * @note The component framework is not lazily opened, so attempts
 * should be made to minimize the amount of memory allocated during
 * this function.
 *
 * @param[in] enable_progress_threads True if the component needs to
 *                                support progress threads
 * @param[in] enable_mpi_threads  True if the component needs to
 *                                support MPI_THREAD_MULTIPLE
 *
 * @retval OMPI_SUCCESS Component successfully initialized
 * @retval OMPI_ERROR   An unspecified error occurred
 */
typedef int (*ompi_osc_base_component_init_fn_t)(bool enable_progress_threads,
                                                 bool enable_mpi_threads);


/**
 * OSC component finalization
 *
 * Finalize the given one-sided component.  This function should clean
 * up any component-level data allocated during component_init().  It
 * should also clean up any data created during the lifetime of the
 * component, including any modules that are outstanding.
 *
 * @retval OMPI_SUCCESS Component successfully finalized
 * @retval OMPI_ERROR   An unspecified error occurred
 */
typedef int (*ompi_osc_base_component_finalize_fn_t)(void);


/**
 * OSC component query
 *
 * Query whether, given the info and comm, the component can be used
 * for one-sided support.  The ability to use the component for the
 * window does not mean that the component will be selected.  The win
 * argument should not be modified during this call and no memory
 * should be allocated that is associated with this window.
 *
 * @return The selection priority of the component
 *
 * @param[in]  win  The window handle, already filled in by MPI_WIN_CREATE()
 * @param[in]  info An info structure with hints from the user
 *                  regarding the usage of the component
 * @param[in]  comm The communicator specified by the user for the
 *                  basis of the group membership for the Window.
 *
 * @retval -1          The component can not be used for this window
 * @retval >= 0        The priority of the component for this window
 */
typedef int (*ompi_osc_base_component_query_fn_t)(struct ompi_win_t *win,
                                                  void **base,
                                                  size_t size,
                                                  int disp_unit,
                                                  struct ompi_communicator_t *comm,
                                                  struct opal_info_t *info,
                                                  int flavor);

/**
 * OSC component select
 *
 * This component has been selected to provide one-sided services for
 * the given window.  The win->w_osc_module field can be updated and
 * memory can be associated with this window.  The module should be
 * ready for use immediately upon return of this function, and the
 * module is responsible for providing any required collective
 * synchronization before the end of the call.
 *
 * @note The comm is the communicator specified from the user, so
 * normal internal usage rules apply.  In other words, if you need
 * communication for the life of the window, you should call
 * comm_dup() during this function.
 *
 * @param[in/out]  win  The window handle, already filled in by MPI_WIN_CREATE()
 * @param[in]      info An info structure with hints from the user
 *                      regarding the usage of the component
 * @param[in]      comm The communicator specified by the user for the
 *                      basis of the group membership for the Window.
 *
 * @retval OMPI_SUCCESS Component successfully selected
 * @retval OMPI_ERROR   An unspecified error occurred
 */
typedef int (*ompi_osc_base_component_select_fn_t)(struct ompi_win_t *win,
                                                   void **base,
                                                   size_t size,
                                                   int disp_unit,
                                                   struct ompi_communicator_t *comm,
                                                   struct opal_info_t *info,
                                                   int flavor,
                                                   int *model);

/**
 * OSC component interface
 *
 * Component interface for the OSC framework.  A public instance of
 * this structure, called mca_osc_[component_name]_component, must
 * exist in any OSC component.
 */
struct ompi_osc_base_component_2_0_0_t {
    /** Base component description */
    mca_base_component_t osc_version;
    /** Base component data block */
    mca_base_component_data_t osc_data;
    /** Component initialization function */
    ompi_osc_base_component_init_fn_t osc_init;
    /** Query whether component is useable for give comm/info */
    ompi_osc_base_component_query_fn_t osc_query;
    /** Create module for the given window */
    ompi_osc_base_component_select_fn_t osc_select;
    /* Finalize the component infrastructure */
    ompi_osc_base_component_finalize_fn_t osc_finalize;
};
typedef struct ompi_osc_base_component_2_0_0_t ompi_osc_base_component_2_0_0_t;
typedef ompi_osc_base_component_2_0_0_t ompi_osc_base_component_t;


/* ******************************************************************** */

typedef int (*ompi_osc_base_module_win_shared_query_fn_t)(struct ompi_win_t *win, int rank,
                                                          size_t *size, int *disp_unit, void *baseptr);

typedef int (*ompi_osc_base_module_win_attach_fn_t)(struct ompi_win_t *win, void *base, size_t size);
typedef int (*ompi_osc_base_module_win_detach_fn_t)(struct ompi_win_t *win, const void *base);

/**
 * Free resources associated with win
 *
 * Free all resources associated with \c win.  The component must
 * provide the barrier semantics required by MPI-2 6.2.1.  The caller
 * will guarantee that no new calls into the module are made after the
 * start of this call.  It is possible that the window is locked by
 * remote processes.  win->w_flags will have OMPI_WIN_FREED set before
 * this function is called.
 *
 * @param[in]  win  Window to free
 *
 * @retval OMPI_SUCCESS Component successfully selected
 * @retval OMPI_ERROR   An unspecified error occurred
 */
typedef int (*ompi_osc_base_module_free_fn_t)(struct ompi_win_t *win);


typedef int (*ompi_osc_base_module_put_fn_t)(const void *origin_addr,
                                            int origin_count,
                                            struct ompi_datatype_t *origin_dt,
                                            int target,
                                            ptrdiff_t target_disp,
                                            int target_count,
                                            struct ompi_datatype_t *target_dt,
                                            struct ompi_win_t *win);


typedef int (*ompi_osc_base_module_get_fn_t)(void *origin_addr,
                                            int origin_count,
                                            struct ompi_datatype_t *origin_dt,
                                            int target,
                                            ptrdiff_t target_disp,
                                            int target_count,
                                            struct ompi_datatype_t *target_dt,
                                            struct ompi_win_t *win);


typedef int (*ompi_osc_base_module_accumulate_fn_t)(const void *origin_addr,
                                                   int origin_count,
                                                   struct ompi_datatype_t *origin_dt,
                                                   int target,
                                                   ptrdiff_t target_disp,
                                                   int target_count,
                                                   struct ompi_datatype_t *target_dt,
                                                   struct ompi_op_t *op,
                                                   struct ompi_win_t *win);

typedef int (*ompi_osc_base_module_compare_and_swap_fn_t)(const void *origin_addr,
                                                          const void *compare_addr,
                                                          void *result_addr,
                                                          struct ompi_datatype_t *dt,
                                                          int target,
                                                          ptrdiff_t target_disp,
                                                          struct ompi_win_t *win);

typedef int (*ompi_osc_base_module_fetch_and_op_fn_t)(const void *origin_addr,
                                                      void *result_addr,
                                                      struct ompi_datatype_t *dt,
                                                      int target,
                                                      ptrdiff_t target_disp,
                                                      struct ompi_op_t *op,
                                                      struct ompi_win_t *win);

typedef int (*ompi_osc_base_module_get_accumulate_fn_t)(const void *origin_addr,
                                                        int origin_count,
                                                        struct ompi_datatype_t *origin_datatype,
                                                        void *result_addr,
                                                        int result_count,
                                                        struct ompi_datatype_t *result_datatype,
                                                        int target_rank,
                                                        ptrdiff_t target_disp,
                                                        int target_count,
                                                        struct ompi_datatype_t *target_datatype,
                                                        struct ompi_op_t *op,
                                                        struct ompi_win_t *win);

typedef int (*ompi_osc_base_module_rput_fn_t)(const void *origin_addr,
                                              int origin_count,
                                              struct ompi_datatype_t *origin_dt,
                                              int target,
                                              ptrdiff_t target_disp,
                                              int target_count,
                                              struct ompi_datatype_t *target_dt,
                                              struct ompi_win_t *win,
                                              struct ompi_request_t **request);

typedef int (*ompi_osc_base_module_rget_fn_t)(void *origin_addr,
                                              int origin_count,
                                              struct ompi_datatype_t *origin_dt,
                                              int target,
                                              ptrdiff_t target_disp,
                                              int target_count,
                                              struct ompi_datatype_t *target_dt,
                                              struct ompi_win_t *win,
                                              struct ompi_request_t **request);


typedef int (*ompi_osc_base_module_raccumulate_fn_t)(const void *origin_addr,
                                                     int origin_count,
                                                     struct ompi_datatype_t *origin_dt,
                                                     int target,
                                                     ptrdiff_t target_disp,
                                                     int target_count,
                                                     struct ompi_datatype_t *target_dt,
                                                     struct ompi_op_t *op,
                                                     struct ompi_win_t *win,
                                                     struct ompi_request_t **request);

typedef int (*ompi_osc_base_module_rget_accumulate_fn_t)(const void *origin_addr,
                                                         int origin_count,
                                                         struct ompi_datatype_t *origin_datatype,
                                                         void *result_addr,
                                                         int result_count,
                                                         struct ompi_datatype_t *result_datatype,
                                                         int target_rank,
                                                         ptrdiff_t target_disp,
                                                         int target_count,
                                                         struct ompi_datatype_t *target_datatype,
                                                         struct ompi_op_t *op,
                                                         struct ompi_win_t *win,
                                                         struct ompi_request_t **request);

typedef int (*ompi_osc_base_module_fence_fn_t)(int assert, struct ompi_win_t *win);


typedef int (*ompi_osc_base_module_start_fn_t)(struct ompi_group_t *group,
                                              int assert,
                                              struct ompi_win_t *win);


typedef int (*ompi_osc_base_module_complete_fn_t)(struct ompi_win_t *win);


typedef int (*ompi_osc_base_module_post_fn_t)(struct ompi_group_t *group,
                                             int assert,
                                             struct ompi_win_t *win);


typedef int (*ompi_osc_base_module_wait_fn_t)(struct ompi_win_t *win);


typedef int (*ompi_osc_base_module_test_fn_t)(struct ompi_win_t *win,
                                             int *flag);


typedef int (*ompi_osc_base_module_lock_fn_t)(int lock_type,
                                             int target,
                                             int assert,
                                             struct ompi_win_t *win);

typedef int (*ompi_osc_base_module_unlock_fn_t)(int target,
                                             struct ompi_win_t *win);

typedef int (*ompi_osc_base_module_lock_all_fn_t)(int assert,
                                                  struct ompi_win_t *win);

typedef int (*ompi_osc_base_module_unlock_all_fn_t)(struct ompi_win_t *win);

typedef int (*ompi_osc_base_module_sync_fn_t)(struct ompi_win_t *win);
typedef int (*ompi_osc_base_module_flush_fn_t)(int target,
                                               struct ompi_win_t *win);
typedef int (*ompi_osc_base_module_flush_all_fn_t)(struct ompi_win_t *win);
typedef int (*ompi_osc_base_module_flush_local_fn_t)(int target,
                                               struct ompi_win_t *win);
typedef int (*ompi_osc_base_module_flush_local_all_fn_t)(struct ompi_win_t *win);



/* ******************************************************************** */


/**
 * OSC module instance
 *
 * Module interface to the OSC system.  An instance of this module is
 * attached to each window.  The window contains a pointer to the base
 * module instead of a base module itself so that the component is
 * free to create a structure that inherits this one for use as the
 * module structure.
 */
struct ompi_osc_base_module_3_0_0_t {
    ompi_osc_base_module_win_shared_query_fn_t osc_win_shared_query;

    ompi_osc_base_module_win_attach_fn_t osc_win_attach;
    ompi_osc_base_module_win_detach_fn_t osc_win_detach;
    ompi_osc_base_module_free_fn_t osc_free;

    ompi_osc_base_module_put_fn_t osc_put;
    ompi_osc_base_module_get_fn_t osc_get;
    ompi_osc_base_module_accumulate_fn_t osc_accumulate;
    ompi_osc_base_module_compare_and_swap_fn_t osc_compare_and_swap;
    ompi_osc_base_module_fetch_and_op_fn_t osc_fetch_and_op;
    ompi_osc_base_module_get_accumulate_fn_t osc_get_accumulate;

    ompi_osc_base_module_rput_fn_t osc_rput;
    ompi_osc_base_module_rget_fn_t osc_rget;
    ompi_osc_base_module_raccumulate_fn_t osc_raccumulate;
    ompi_osc_base_module_rget_accumulate_fn_t osc_rget_accumulate;

    ompi_osc_base_module_fence_fn_t osc_fence;

    ompi_osc_base_module_start_fn_t osc_start;
    ompi_osc_base_module_complete_fn_t osc_complete;
    ompi_osc_base_module_post_fn_t osc_post;
    ompi_osc_base_module_wait_fn_t osc_wait;
    ompi_osc_base_module_test_fn_t osc_test;

    ompi_osc_base_module_lock_fn_t osc_lock;
    ompi_osc_base_module_unlock_fn_t osc_unlock;
    ompi_osc_base_module_lock_all_fn_t osc_lock_all;
    ompi_osc_base_module_unlock_all_fn_t osc_unlock_all;

    ompi_osc_base_module_sync_fn_t osc_sync;
    ompi_osc_base_module_flush_fn_t osc_flush;
    ompi_osc_base_module_flush_all_fn_t osc_flush_all;
    ompi_osc_base_module_flush_local_fn_t osc_flush_local;
    ompi_osc_base_module_flush_local_all_fn_t osc_flush_local_all;
};
typedef struct ompi_osc_base_module_3_0_0_t ompi_osc_base_module_3_0_0_t;
typedef ompi_osc_base_module_3_0_0_t ompi_osc_base_module_t;


/* ******************************************************************** */


/** Macro for use in components that are of type osc */
#define OMPI_OSC_BASE_VERSION_3_0_0 \
    OMPI_MCA_BASE_VERSION_2_1_0("osc", 3, 0, 0)


/* ******************************************************************** */


END_C_DECLS


#endif /* OMPI_OSC_H */
