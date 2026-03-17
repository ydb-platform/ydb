/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2011      Sandia National Laboratories.  All rights reserved.
 * Copyright (c) 2014      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "ompi/mca/osc/osc.h"
#include "ompi/mca/osc/base/base.h"
#include "ompi/mca/osc/base/osc_base_obj_convert.h"

#include "osc_sm.h"

int
ompi_osc_sm_rput(const void *origin_addr,
                 int origin_count,
                 struct ompi_datatype_t *origin_dt,
                 int target,
                 ptrdiff_t target_disp,
                 int target_count,
                 struct ompi_datatype_t *target_dt,
                 struct ompi_win_t *win,
                 struct ompi_request_t **ompi_req)
{
    int ret;
    ompi_osc_sm_module_t *module =
        (ompi_osc_sm_module_t*) win->w_osc_module;
    void *remote_address;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "rput: 0x%lx, %d, %s, %d, %d, %d, %s, 0x%lx",
                         (unsigned long) origin_addr, origin_count,
                         origin_dt->name, target, (int) target_disp,
                         target_count, target_dt->name,
                         (unsigned long) win));

    remote_address = ((char*) (module->bases[target])) + module->disp_units[target] * target_disp;

    ret = ompi_datatype_sndrcv((void *)origin_addr, origin_count, origin_dt,
                               remote_address, target_count, target_dt);
    if (OMPI_SUCCESS != ret) {
        return ret;
    }

    /* the only valid field of RMA request status is the MPI_ERROR field.
     * ompi_request_empty has status MPI_SUCCESS and indicates the request is
     * complete. */
    *ompi_req = &ompi_request_empty;

    return OMPI_SUCCESS;
}


int
ompi_osc_sm_rget(void *origin_addr,
                 int origin_count,
                 struct ompi_datatype_t *origin_dt,
                 int target,
                 ptrdiff_t target_disp,
                 int target_count,
                 struct ompi_datatype_t *target_dt,
                 struct ompi_win_t *win,
                 struct ompi_request_t **ompi_req)
{
    int ret;
    ompi_osc_sm_module_t *module =
        (ompi_osc_sm_module_t*) win->w_osc_module;
    void *remote_address;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "rget: 0x%lx, %d, %s, %d, %d, %d, %s, 0x%lx",
                         (unsigned long) origin_addr, origin_count,
                         origin_dt->name, target, (int) target_disp,
                         target_count, target_dt->name,
                         (unsigned long) win));

    remote_address = ((char*) (module->bases[target])) + module->disp_units[target] * target_disp;

    ret = ompi_datatype_sndrcv(remote_address, target_count, target_dt,
                               origin_addr, origin_count, origin_dt);
    if (OMPI_SUCCESS != ret) {
        return ret;
    }

    /* the only valid field of RMA request status is the MPI_ERROR field.
     * ompi_request_empty has status MPI_SUCCESS and indicates the request is
     * complete. */
    *ompi_req = &ompi_request_empty;

    return OMPI_SUCCESS;
}


int
ompi_osc_sm_raccumulate(const void *origin_addr,
                        int origin_count,
                        struct ompi_datatype_t *origin_dt,
                        int target,
                        ptrdiff_t target_disp,
                        int target_count,
                        struct ompi_datatype_t *target_dt,
                        struct ompi_op_t *op,
                        struct ompi_win_t *win,
                        struct ompi_request_t **ompi_req)
{
    int ret;
    ompi_osc_sm_module_t *module =
        (ompi_osc_sm_module_t*) win->w_osc_module;
    void *remote_address;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "raccumulate: 0x%lx, %d, %s, %d, %d, %d, %s, %s, 0x%lx",
                         (unsigned long) origin_addr, origin_count,
                         origin_dt->name, target, (int) target_disp,
                         target_count, target_dt->name,
                         op->o_name,
                         (unsigned long) win));

    remote_address = ((char*) (module->bases[target])) + module->disp_units[target] * target_disp;

    opal_atomic_lock(&module->node_states[target].accumulate_lock);
    if (op == &ompi_mpi_op_replace.op) {
        ret = ompi_datatype_sndrcv((void *)origin_addr, origin_count, origin_dt,
                                    remote_address, target_count, target_dt);
    } else {
        ret = ompi_osc_base_sndrcv_op(origin_addr, origin_count, origin_dt,
                                      remote_address, target_count, target_dt,
                                      op);
    }
    opal_atomic_unlock(&module->node_states[target].accumulate_lock);

    /* the only valid field of RMA request status is the MPI_ERROR field.
     * ompi_request_empty has status MPI_SUCCESS and indicates the request is
     * complete. */
    *ompi_req = &ompi_request_empty;

    return ret;
}



int
ompi_osc_sm_rget_accumulate(const void *origin_addr,
                                  int origin_count,
                                  struct ompi_datatype_t *origin_dt,
                                  void *result_addr,
                                  int result_count,
                                  struct ompi_datatype_t *result_dt,
                                  int target,
                                  MPI_Aint target_disp,
                                  int target_count,
                                  struct ompi_datatype_t *target_dt,
                                  struct ompi_op_t *op,
                                  struct ompi_win_t *win,
                                  struct ompi_request_t **ompi_req)
{
    int ret;
    ompi_osc_sm_module_t *module =
        (ompi_osc_sm_module_t*) win->w_osc_module;
    void *remote_address;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "rget_accumulate: 0x%lx, %d, %s, %d, %d, %d, %s, %s, 0x%lx",
                         (unsigned long) origin_addr, origin_count,
                         origin_dt->name, target, (int) target_disp,
                         target_count, target_dt->name,
                         op->o_name,
                         (unsigned long) win));

    remote_address = ((char*) (module->bases[target])) + module->disp_units[target] * target_disp;

    opal_atomic_lock(&module->node_states[target].accumulate_lock);

    ret = ompi_datatype_sndrcv(remote_address, target_count, target_dt,
                               result_addr, result_count, result_dt);
    if (OMPI_SUCCESS != ret || op == &ompi_mpi_op_no_op.op) goto done;

    if (op == &ompi_mpi_op_replace.op) {
        ret = ompi_datatype_sndrcv((void *)origin_addr, origin_count, origin_dt,
                                   remote_address, target_count, target_dt);
    } else {
        ret = ompi_osc_base_sndrcv_op(origin_addr, origin_count, origin_dt,
                                      remote_address, target_count, target_dt,
                                      op);
    }

 done:
    opal_atomic_unlock(&module->node_states[target].accumulate_lock);

    /* the only valid field of RMA request status is the MPI_ERROR field.
     * ompi_request_empty has status MPI_SUCCESS and indicates the request is
     * complete. */
    *ompi_req = &ompi_request_empty;

    return ret;
}


int
ompi_osc_sm_put(const void *origin_addr,
                      int origin_count,
                      struct ompi_datatype_t *origin_dt,
                      int target,
                      ptrdiff_t target_disp,
                      int target_count,
                      struct ompi_datatype_t *target_dt,
                      struct ompi_win_t *win)
{
    int ret;
    ompi_osc_sm_module_t *module =
        (ompi_osc_sm_module_t*) win->w_osc_module;
    void *remote_address;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "put: 0x%lx, %d, %s, %d, %d, %d, %s, 0x%lx",
                         (unsigned long) origin_addr, origin_count,
                         origin_dt->name, target, (int) target_disp,
                         target_count, target_dt->name,
                         (unsigned long) win));

    remote_address = ((char*) (module->bases[target])) + module->disp_units[target] * target_disp;

    ret = ompi_datatype_sndrcv((void *)origin_addr, origin_count, origin_dt,
                               remote_address, target_count, target_dt);

    return ret;
}


int
ompi_osc_sm_get(void *origin_addr,
                      int origin_count,
                      struct ompi_datatype_t *origin_dt,
                      int target,
                      ptrdiff_t target_disp,
                      int target_count,
                      struct ompi_datatype_t *target_dt,
                      struct ompi_win_t *win)
{
    int ret;
    ompi_osc_sm_module_t *module =
        (ompi_osc_sm_module_t*) win->w_osc_module;
    void *remote_address;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "get: 0x%lx, %d, %s, %d, %d, %d, %s, 0x%lx",
                         (unsigned long) origin_addr, origin_count,
                         origin_dt->name, target, (int) target_disp,
                         target_count, target_dt->name,
                         (unsigned long) win));

    remote_address = ((char*) (module->bases[target])) + module->disp_units[target] * target_disp;

    ret = ompi_datatype_sndrcv(remote_address, target_count, target_dt,
                               origin_addr, origin_count, origin_dt);

    return ret;
}


int
ompi_osc_sm_accumulate(const void *origin_addr,
                       int origin_count,
                       struct ompi_datatype_t *origin_dt,
                       int target,
                       ptrdiff_t target_disp,
                       int target_count,
                       struct ompi_datatype_t *target_dt,
                       struct ompi_op_t *op,
                       struct ompi_win_t *win)
{
    int ret;
    ompi_osc_sm_module_t *module =
        (ompi_osc_sm_module_t*) win->w_osc_module;
    void *remote_address;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "accumulate: 0x%lx, %d, %s, %d, %d, %d, %s, %s, 0x%lx",
                         (unsigned long) origin_addr, origin_count,
                         origin_dt->name, target, (int) target_disp,
                         target_count, target_dt->name,
                         op->o_name,
                         (unsigned long) win));

    remote_address = ((char*) (module->bases[target])) + module->disp_units[target] * target_disp;

    opal_atomic_lock(&module->node_states[target].accumulate_lock);
    if (op == &ompi_mpi_op_replace.op) {
        ret = ompi_datatype_sndrcv((void *)origin_addr, origin_count, origin_dt,
                                    remote_address, target_count, target_dt);
    } else {
        ret = ompi_osc_base_sndrcv_op(origin_addr, origin_count, origin_dt,
                                      remote_address, target_count, target_dt,
                                      op);
    }
    opal_atomic_unlock(&module->node_states[target].accumulate_lock);

    return ret;
}


int
ompi_osc_sm_get_accumulate(const void *origin_addr,
                           int origin_count,
                           struct ompi_datatype_t *origin_dt,
                           void *result_addr,
                           int result_count,
                           struct ompi_datatype_t *result_dt,
                           int target,
                           MPI_Aint target_disp,
                           int target_count,
                           struct ompi_datatype_t *target_dt,
                           struct ompi_op_t *op,
                           struct ompi_win_t *win)
{
    int ret;
    ompi_osc_sm_module_t *module =
        (ompi_osc_sm_module_t*) win->w_osc_module;
    void *remote_address;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "get_accumulate: 0x%lx, %d, %s, %d, %d, %d, %s, %s, 0x%lx",
                         (unsigned long) origin_addr, origin_count,
                         origin_dt->name, target, (int) target_disp,
                         target_count, target_dt->name,
                         op->o_name,
                         (unsigned long) win));

    remote_address = ((char*) (module->bases[target])) + module->disp_units[target] * target_disp;

    opal_atomic_lock(&module->node_states[target].accumulate_lock);

    ret = ompi_datatype_sndrcv(remote_address, target_count, target_dt,
                               result_addr, result_count, result_dt);
    if (OMPI_SUCCESS != ret || op == &ompi_mpi_op_no_op.op) goto done;

    if (op == &ompi_mpi_op_replace.op) {
        ret = ompi_datatype_sndrcv((void *)origin_addr, origin_count, origin_dt,
                                   remote_address, target_count, target_dt);
    } else {
        ret = ompi_osc_base_sndrcv_op(origin_addr, origin_count, origin_dt,
                                      remote_address, target_count, target_dt,
                                      op);
    }

 done:
    opal_atomic_unlock(&module->node_states[target].accumulate_lock);

    return ret;
}


int
ompi_osc_sm_compare_and_swap(const void *origin_addr,
                             const void *compare_addr,
                             void *result_addr,
                             struct ompi_datatype_t *dt,
                             int target,
                             ptrdiff_t target_disp,
                             struct ompi_win_t *win)
{
    ompi_osc_sm_module_t *module =
        (ompi_osc_sm_module_t*) win->w_osc_module;
    void *remote_address;
    size_t size;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "compare_and_swap: 0x%lx, %s, %d, %d, 0x%lx",
                         (unsigned long) origin_addr,
                         dt->name, target, (int) target_disp,
                         (unsigned long) win));

    remote_address = ((char*) (module->bases[target])) + module->disp_units[target] * target_disp;

    ompi_datatype_type_size(dt, &size);

    opal_atomic_lock(&module->node_states[target].accumulate_lock);

    /* fetch */
    ompi_datatype_copy_content_same_ddt(dt, 1, (char*) result_addr, (char*) remote_address);
    /* compare */
    if (0 == memcmp(result_addr, compare_addr, size)) {
        /* set */
        ompi_datatype_copy_content_same_ddt(dt, 1, (char*) remote_address, (char*) origin_addr);
    }

    opal_atomic_unlock(&module->node_states[target].accumulate_lock);

    return OMPI_SUCCESS;
}


int
ompi_osc_sm_fetch_and_op(const void *origin_addr,
                         void *result_addr,
                         struct ompi_datatype_t *dt,
                         int target,
                         ptrdiff_t target_disp,
                         struct ompi_op_t *op,
                         struct ompi_win_t *win)
{
    ompi_osc_sm_module_t *module =
        (ompi_osc_sm_module_t*) win->w_osc_module;
    void *remote_address;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "fetch_and_op: 0x%lx, %s, %d, %d, %s, 0x%lx",
                         (unsigned long) origin_addr,
                         dt->name, target, (int) target_disp,
                         op->o_name,
                         (unsigned long) win));

    remote_address = ((char*) (module->bases[target])) + module->disp_units[target] * target_disp;

    opal_atomic_lock(&module->node_states[target].accumulate_lock);

    /* fetch */
    ompi_datatype_copy_content_same_ddt(dt, 1, (char*) result_addr, (char*) remote_address);
    if (op == &ompi_mpi_op_no_op.op) goto done;

    /* op */
    if (op == &ompi_mpi_op_replace.op) {
        ompi_datatype_copy_content_same_ddt(dt, 1, (char*) remote_address, (char*) origin_addr);
    } else {
        ompi_op_reduce(op, (void *)origin_addr, remote_address, 1, dt);
    }

 done:
    opal_atomic_unlock(&module->node_states[target].accumulate_lock);

    return OMPI_SUCCESS;;
}
