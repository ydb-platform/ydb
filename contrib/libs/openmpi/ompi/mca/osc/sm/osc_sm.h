/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2012      Sandia National Laboratories.  All rights reserved.
 * Copyright (c) 2014-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016-2017 IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OSC_SM_SM_H
#define OSC_SM_SM_H

#include "opal/mca/shmem/base/base.h"

#if OPAL_HAVE_ATOMIC_MATH_64

typedef uint64_t osc_sm_post_type_t;
#define OSC_SM_POST_BITS 6
#define OSC_SM_POST_MASK 0x3f

#else

typedef uint32_t osc_sm_post_type_t;
#define OSC_SM_POST_BITS 5
#define OSC_SM_POST_MASK 0x1f

#endif

/* data shared across all peers */
struct ompi_osc_sm_global_state_t {
    int use_barrier_for_fence;

    pthread_mutex_t mtx;
    pthread_cond_t cond;

    int sense;
    int32_t count;
};
typedef struct ompi_osc_sm_global_state_t ompi_osc_sm_global_state_t;

/* this is data exposed to remote nodes */
struct ompi_osc_sm_lock_t {
    uint32_t counter;
    uint32_t write;
    uint32_t read;
};
typedef struct ompi_osc_sm_lock_t ompi_osc_sm_lock_t;

struct ompi_osc_sm_node_state_t {
    int32_t complete_count;
    ompi_osc_sm_lock_t lock;
    opal_atomic_lock_t accumulate_lock;
};
typedef struct ompi_osc_sm_node_state_t ompi_osc_sm_node_state_t;

struct ompi_osc_sm_component_t {
    ompi_osc_base_component_t super;

    char *backing_directory;
};
typedef struct ompi_osc_sm_component_t ompi_osc_sm_component_t;
OMPI_DECLSPEC extern ompi_osc_sm_component_t mca_osc_sm_component;

enum ompi_osc_sm_locktype_t {
    lock_none = 0,
    lock_nocheck,
    lock_exclusive,
    lock_shared
};

struct ompi_osc_sm_module_t {
    ompi_osc_base_module_t super;
    struct ompi_communicator_t *comm;
    int flavor;
    opal_shmem_ds_t seg_ds;
    void *segment_base;
    bool noncontig;

    size_t *sizes;
    void **bases;
    int *disp_units;

    ompi_group_t *start_group;
    ompi_group_t *post_group;

    int my_sense;

    enum ompi_osc_sm_locktype_t *outstanding_locks;

    /* exposed data */
    ompi_osc_sm_global_state_t *global_state;
    ompi_osc_sm_node_state_t *my_node_state;
    ompi_osc_sm_node_state_t *node_states;

    osc_sm_post_type_t ** volatile posts;

    opal_mutex_t lock;
};
typedef struct ompi_osc_sm_module_t ompi_osc_sm_module_t;

int ompi_osc_sm_shared_query(struct ompi_win_t *win, int rank, size_t *size, int *disp_unit, void *baseptr);

int ompi_osc_sm_attach(struct ompi_win_t *win, void *base, size_t len);
int ompi_osc_sm_detach(struct ompi_win_t *win, const void *base);

int ompi_osc_sm_free(struct ompi_win_t *win);

int ompi_osc_sm_put(const void *origin_addr,
                          int origin_count,
                          struct ompi_datatype_t *origin_dt,
                          int target,
                          ptrdiff_t target_disp,
                          int target_count,
                          struct ompi_datatype_t *target_dt,
                          struct ompi_win_t *win);

int ompi_osc_sm_get(void *origin_addr,
                          int origin_count,
                          struct ompi_datatype_t *origin_dt,
                          int target,
                          ptrdiff_t target_disp,
                          int target_count,
                          struct ompi_datatype_t *target_dt,
                          struct ompi_win_t *win);

int ompi_osc_sm_accumulate(const void *origin_addr,
                                 int origin_count,
                                 struct ompi_datatype_t *origin_dt,
                                 int target,
                                 ptrdiff_t target_disp,
                                 int target_count,
                                 struct ompi_datatype_t *target_dt,
                                 struct ompi_op_t *op,
                                 struct ompi_win_t *win);

int ompi_osc_sm_compare_and_swap(const void *origin_addr,
                                       const void *compare_addr,
                                       void *result_addr,
                                       struct ompi_datatype_t *dt,
                                       int target,
                                       ptrdiff_t target_disp,
                                       struct ompi_win_t *win);

int ompi_osc_sm_fetch_and_op(const void *origin_addr,
                                   void *result_addr,
                                   struct ompi_datatype_t *dt,
                                   int target,
                                   ptrdiff_t target_disp,
                                   struct ompi_op_t *op,
                                   struct ompi_win_t *win);

int ompi_osc_sm_get_accumulate(const void *origin_addr,
                                     int origin_count,
                                     struct ompi_datatype_t *origin_datatype,
                                     void *result_addr,
                                     int result_count,
                                     struct ompi_datatype_t *result_datatype,
                                     int target_rank,
                                     MPI_Aint target_disp,
                                     int target_count,
                                     struct ompi_datatype_t *target_datatype,
                                     struct ompi_op_t *op,
                                     struct ompi_win_t *win);

int ompi_osc_sm_rput(const void *origin_addr,
                           int origin_count,
                           struct ompi_datatype_t *origin_dt,
                           int target,
                           ptrdiff_t target_disp,
                           int target_count,
                           struct ompi_datatype_t *target_dt,
                           struct ompi_win_t *win,
                           struct ompi_request_t **request);

int ompi_osc_sm_rget(void *origin_addr,
                           int origin_count,
                           struct ompi_datatype_t *origin_dt,
                           int target,
                           ptrdiff_t target_disp,
                           int target_count,
                           struct ompi_datatype_t *target_dt,
                           struct ompi_win_t *win,
                           struct ompi_request_t **request);

int ompi_osc_sm_raccumulate(const void *origin_addr,
                                  int origin_count,
                                  struct ompi_datatype_t *origin_dt,
                                  int target,
                                  ptrdiff_t target_disp,
                                  int target_count,
                                  struct ompi_datatype_t *target_dt,
                                  struct ompi_op_t *op,
                                  struct ompi_win_t *win,
                                  struct ompi_request_t **request);

int ompi_osc_sm_rget_accumulate(const void *origin_addr,
                                      int origin_count,
                                      struct ompi_datatype_t *origin_datatype,
                                      void *result_addr,
                                      int result_count,
                                      struct ompi_datatype_t *result_datatype,
                                      int target_rank,
                                      MPI_Aint target_disp,
                                      int target_count,
                                      struct ompi_datatype_t *target_datatype,
                                      struct ompi_op_t *op,
                                      struct ompi_win_t *win,
                                      struct ompi_request_t **request);

int ompi_osc_sm_fence(int assert, struct ompi_win_t *win);

int ompi_osc_sm_start(struct ompi_group_t *group,
                            int assert,
                            struct ompi_win_t *win);

int ompi_osc_sm_complete(struct ompi_win_t *win);

int ompi_osc_sm_post(struct ompi_group_t *group,
                           int assert,
                           struct ompi_win_t *win);

int ompi_osc_sm_wait(struct ompi_win_t *win);

int ompi_osc_sm_test(struct ompi_win_t *win,
                           int *flag);

int ompi_osc_sm_lock(int lock_type,
                           int target,
                           int assert,
                           struct ompi_win_t *win);

int ompi_osc_sm_unlock(int target,
                             struct ompi_win_t *win);


int ompi_osc_sm_lock_all(int assert,
                               struct ompi_win_t *win);

int ompi_osc_sm_unlock_all(struct ompi_win_t *win);

int ompi_osc_sm_sync(struct ompi_win_t *win);

int ompi_osc_sm_flush(int target,
                            struct ompi_win_t *win);
int ompi_osc_sm_flush_all(struct ompi_win_t *win);
int ompi_osc_sm_flush_local(int target,
                                  struct ompi_win_t *win);
int ompi_osc_sm_flush_local_all(struct ompi_win_t *win);

int ompi_osc_sm_set_info(struct ompi_win_t *win, struct opal_info_t *info);
int ompi_osc_sm_get_info(struct ompi_win_t *win, struct opal_info_t **info_used);

#endif
