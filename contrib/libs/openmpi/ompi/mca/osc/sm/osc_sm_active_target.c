/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2012      Sandia National Laboratories.  All rights reserved.
 * Copyright (c) 2014-2017 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2014-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "opal/sys/atomic.h"
#include "ompi/mca/osc/osc.h"
#include "ompi/mca/osc/base/base.h"
#include "ompi/mca/osc/base/osc_base_obj_convert.h"

#include "osc_sm.h"

/**
 * compare_ranks:
 *
 * @param[in] ptra    Pointer to integer item
 * @param[in] ptrb    Pointer to integer item
 *
 * @returns 0 if *ptra == *ptrb
 * @returns -1 if *ptra < *ptrb
 * @returns 1 otherwise
 *
 * This function is used to sort the rank list. It can be removed if
 * groups are always in order.
 */
static int compare_ranks (const void *ptra, const void *ptrb)
{
    int a = *((int *) ptra);
    int b = *((int *) ptrb);

    if (a < b) {
        return -1;
    } else if (a > b) {
        return 1;
    }

    return 0;
}

/**
 * ompi_osc_pt2pt_get_comm_ranks:
 *
 * @param[in] module    - OSC PT2PT module
 * @param[in] sub_group - Group with ranks to translate
 *
 * @returns an array of translated ranks on success or NULL on failure
 *
 * Translate the ranks given in {sub_group} into ranks in the
 * communicator used to create {module}.
 */
static int *ompi_osc_sm_group_ranks (ompi_group_t *group, ompi_group_t *sub_group)
{
    int size = ompi_group_size(sub_group);
    int *ranks1, *ranks2;
    int ret;

    ranks1 = calloc (size, sizeof(int));
    ranks2 = calloc (size, sizeof(int));
    if (NULL == ranks1 || NULL == ranks2) {
        free (ranks1);
        free (ranks2);
        return NULL;
    }

    for (int i = 0 ; i < size ; ++i) {
        ranks1[i] = i;
    }

    ret = ompi_group_translate_ranks (sub_group, size, ranks1, group, ranks2);
    free (ranks1);
    if (OMPI_SUCCESS != ret) {
        free (ranks2);
        return NULL;
    }

    qsort (ranks2, size, sizeof (int), compare_ranks);

    return ranks2;
}


int
ompi_osc_sm_fence(int assert, struct ompi_win_t *win)
{
    ompi_osc_sm_module_t *module =
        (ompi_osc_sm_module_t*) win->w_osc_module;

    /* ensure all memory operations have completed */
    opal_atomic_mb();

    if (module->global_state->use_barrier_for_fence) {
        return module->comm->c_coll->coll_barrier(module->comm,
                                                 module->comm->c_coll->coll_barrier_module);
    } else {
        module->my_sense = !module->my_sense;
        pthread_mutex_lock(&module->global_state->mtx);
        module->global_state->count--;
        if (module->global_state->count == 0) {
            module->global_state->count = ompi_comm_size(module->comm);
            module->global_state->sense = module->my_sense;
            pthread_cond_broadcast(&module->global_state->cond);
        } else {
            while (module->global_state->sense != module->my_sense) {
                pthread_cond_wait(&module->global_state->cond, &module->global_state->mtx);
            }
        }
        pthread_mutex_unlock(&module->global_state->mtx);

        return OMPI_SUCCESS;
    }
}

int
ompi_osc_sm_start(struct ompi_group_t *group,
                  int assert,
                  struct ompi_win_t *win)
{
    ompi_osc_sm_module_t *module =
        (ompi_osc_sm_module_t*) win->w_osc_module;
    int my_rank = ompi_comm_rank (module->comm);
    void *_tmp_ptr = NULL;

    OBJ_RETAIN(group);

    if (!OPAL_ATOMIC_COMPARE_EXCHANGE_STRONG_PTR(&module->start_group, (void *) &_tmp_ptr, group)) {
        OBJ_RELEASE(group);
        return OMPI_ERR_RMA_SYNC;
    }

    if (0 == (assert & MPI_MODE_NOCHECK)) {
        int size;

        int *ranks = ompi_osc_sm_group_ranks (module->comm->c_local_group, group);
        if (NULL == ranks) {
            return OMPI_ERR_OUT_OF_RESOURCE;
        }

        size = ompi_group_size(module->start_group);

        for (int i = 0 ; i < size ; ++i) {
            int rank_byte = ranks[i] >> OSC_SM_POST_BITS;
            osc_sm_post_type_t rank_bit = ((osc_sm_post_type_t) 1) << (ranks[i] & 0x3f);

            /* wait for rank to post */
            while (!(module->posts[my_rank][rank_byte] & rank_bit)) {
                opal_progress();
                opal_atomic_mb();
            }

            opal_atomic_rmb ();

#if OPAL_HAVE_ATOMIC_MATH_64
            (void) opal_atomic_fetch_xor_64 ((volatile int64_t *) module->posts[my_rank] + rank_byte, rank_bit);
#else
            (void) opal_atomic_fetch_xor_32 ((volatile int32_t *) module->posts[my_rank] + rank_byte, rank_bit);
#endif
       }

        free (ranks);
    }

    opal_atomic_mb();
    return OMPI_SUCCESS;
}


int
ompi_osc_sm_complete(struct ompi_win_t *win)
{
    ompi_osc_sm_module_t *module =
        (ompi_osc_sm_module_t*) win->w_osc_module;
    ompi_group_t *group;
    int gsize;

    /* ensure all memory operations have completed */
    opal_atomic_mb();

    group = module->start_group;
    if (NULL == group || !OPAL_ATOMIC_COMPARE_EXCHANGE_STRONG_PTR(&module->start_group, &group, NULL)) {
        return OMPI_ERR_RMA_SYNC;
    }

    opal_atomic_mb();

    int *ranks = ompi_osc_sm_group_ranks (module->comm->c_local_group, group);
    if (NULL == ranks) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    gsize = ompi_group_size(group);
    for (int i = 0 ; i < gsize ; ++i) {
        (void) opal_atomic_add_fetch_32(&module->node_states[ranks[i]].complete_count, 1);
    }

    free (ranks);

    OBJ_RELEASE(group);

    opal_atomic_mb();
    return OMPI_SUCCESS;
}


int
ompi_osc_sm_post(struct ompi_group_t *group,
                       int assert,
                       struct ompi_win_t *win)
{
    ompi_osc_sm_module_t *module =
        (ompi_osc_sm_module_t*) win->w_osc_module;
    int my_rank = ompi_comm_rank (module->comm);
    int my_byte = my_rank >> 6;
    uint64_t my_bit = ((uint64_t) 1) << (my_rank & 0x3f);
    int gsize;

    OPAL_THREAD_LOCK(&module->lock);

    if (NULL != module->post_group) {
        OPAL_THREAD_UNLOCK(&module->lock);
        return OMPI_ERR_RMA_SYNC;
    }

    module->post_group = group;

    OBJ_RETAIN(group);

    if (0 == (assert & MPI_MODE_NOCHECK)) {
        int *ranks = ompi_osc_sm_group_ranks (module->comm->c_local_group, group);
        if (NULL == ranks) {
            return OMPI_ERR_OUT_OF_RESOURCE;
        }

        module->my_node_state->complete_count = 0;
        opal_atomic_mb();

        gsize = ompi_group_size(module->post_group);
        for (int i = 0 ; i < gsize ; ++i) {
            opal_atomic_add ((volatile osc_sm_post_type_t *) module->posts[ranks[i]] + my_byte, my_bit);
        }

        opal_atomic_wmb ();

        free (ranks);

        opal_progress ();
    }

    OPAL_THREAD_UNLOCK(&module->lock);

    return OMPI_SUCCESS;
}


int
ompi_osc_sm_wait(struct ompi_win_t *win)
{
    ompi_osc_sm_module_t *module =
        (ompi_osc_sm_module_t*) win->w_osc_module;
    ompi_group_t *group;

    OPAL_THREAD_LOCK(&module->lock);

    if (NULL == module->post_group) {
        OPAL_THREAD_UNLOCK(&module->lock);
        return OMPI_ERR_RMA_SYNC;
    }

    group = module->post_group;

    int size = ompi_group_size (group);

    while (module->my_node_state->complete_count != size) {
        opal_progress();
        opal_atomic_mb();
    }

    OBJ_RELEASE(group);
    module->post_group = NULL;

    OPAL_THREAD_UNLOCK(&module->lock);

    /* ensure all memory operations have completed */
    opal_atomic_mb();

    return OMPI_SUCCESS;
}


int
ompi_osc_sm_test(struct ompi_win_t *win,
                       int *flag)
{
    ompi_osc_sm_module_t *module =
        (ompi_osc_sm_module_t*) win->w_osc_module;

    OPAL_THREAD_LOCK(&module->lock);

    if (NULL == module->post_group) {
        OPAL_THREAD_UNLOCK(&module->lock);
        return OMPI_ERR_RMA_SYNC;
    }

    int size = ompi_group_size(module->post_group);

    if (module->my_node_state->complete_count == size) {
        OBJ_RELEASE(module->post_group);
        module->post_group = NULL;
        *flag = 1;
    } else {
        *flag = 0;
    }

    OPAL_THREAD_UNLOCK(&module->lock);

    /* ensure all memory operations have completed */
    opal_atomic_mb();

    return OMPI_SUCCESS;
}
