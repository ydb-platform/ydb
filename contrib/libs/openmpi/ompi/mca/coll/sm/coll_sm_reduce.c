/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2015 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2009-2013 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include <string.h>

#include "opal/datatype/opal_convertor.h"
#include "opal/sys/atomic.h"
#include "ompi/constants.h"
#include "ompi/communicator/communicator.h"
#include "ompi/mca/coll/coll.h"
#include "ompi/op/op.h"
#include "coll_sm.h"


/*
 * Local functions
 */
static int reduce_inorder(const void *sbuf, void* rbuf, int count,
                          struct ompi_datatype_t *dtype,
                          struct ompi_op_t *op,
                          int root, struct ompi_communicator_t *comm,
                          mca_coll_base_module_t *module);
#define WANT_REDUCE_NO_ORDER 0
#if WANT_REDUCE_NO_ORDER
static int reduce_no_order(const void *sbuf, void* rbuf, int count,
                           struct ompi_datatype_t *dtype,
                           struct ompi_op_t *op,
                           int root, struct ompi_communicator_t *comm,
                           mca_coll_base_module_t *module);
#endif

/*
 * Useful utility routine
 */
#if !defined(min)
static inline int min(int a, int b)
{
    return (a < b) ? a : b;
}
#endif

/**
 * Shared memory reduction.
 *
 * Simply farms out to the associative or non-associative functions.
 */
int mca_coll_sm_reduce_intra(const void *sbuf, void* rbuf, int count,
                             struct ompi_datatype_t *dtype,
                             struct ompi_op_t *op,
                             int root, struct ompi_communicator_t *comm,
                             mca_coll_base_module_t *module)
{
    size_t size;
    mca_coll_sm_module_t *sm_module = (mca_coll_sm_module_t*) module;

    /* There are several possibilities:
     *
     * 0. If the datatype is larger than a segment, fall back to
     *    underlying module
     * 1. If the op is user-defined, use the strict order
     * 2. If the op is intrinsic:
     *    a. If the op is float-associative, use the unordered
     *    b. If the op is not float-associative:
     *       i. if the data is floating point, use the strict order
     *       ii. if the data is not floating point, use the unordered
     */

    ompi_datatype_type_size(dtype, &size);
    if ((int)size > mca_coll_sm_component.sm_control_size) {
        return sm_module->previous_reduce(sbuf, rbuf, count,
                                          dtype, op, root, comm,
                                          sm_module->previous_reduce_module);
    }
#if WANT_REDUCE_NO_ORDER
    else {
        /* Lazily enable the module the first time we invoke a
           collective on it */
        if (!sm_module->enabled) {
            if (OMPI_SUCCESS !=
                (ret = ompi_coll_sm_lazy_enable(module, comm))) {
                return ret;
            }
        }

        if (!ompi_op_is_intrinsic(op) ||
            (ompi_op_is_intrinsic(op) && !ompi_op_is_float_assoc(op) &&
             0 != (dtype->flags & OMPI_DATATYPE_FLAG_DATA_FLOAT))) {
            return reduce_inorder(sbuf, rbuf, count, dtype, op,
                                  root, comm, module);
        } else {
            return reduce_no_order(sbuf, rbuf, count, dtype, op,
                                   root, comm, module);
        }
    }
#else
    else {
        /* Lazily enable the module the first time we invoke a
           collective on it */
        if (!sm_module->enabled) {
            int ret;

            if (OMPI_SUCCESS !=
                (ret = ompi_coll_sm_lazy_enable(module, comm))) {
                return ret;
            }
        }

        return reduce_inorder(sbuf, rbuf, count, dtype, op, root, comm, module);
    }
#endif
}


/**
 * In-order shared memory reduction.
 *
 * This function performs the reduction in order -- combining elements
 * starting with (0 operation 1), then (result operation 2), then
 * (result operation 3), etc.
 *
 * Root's algorithm:
 *
 * If our datatype is "friendly" (i.e., the representation of the
 * buffer is the same packed as it is unpacked), then the root doesn't
 * need a temporary buffer -- we can combine the operands directly
 * from the shared memory segments to the root's rbuf.  Otherwise, we
 * need a receive convertor and receive each fragment into a temporary
 * buffer where we can combine that operan with the root's rbuf.
 *
 * In general, there are two loops:
 *
 * 1. loop over all fragments (which must be done in units of an
 * integer number of datatypes -- remember that if this function is
 * called, we know that the datattype is smaller than the max size of
 * a fragment, so this is definitely possible)
 *
 * 2. loop over all the processes -- 0 to (comm_size-1).
 * For process 0:
 * - if the root==0, copy the *entire* buffer (i.e., don't copy
 *   fragment by fragment -- might as well copy the entire thing) the
 *   first time through the algorithm, and no-op every other time
 * - else, copy from the shmem fragment to the out buffer
 * For all other proceses:
 * - if root==i, combine the relevant fragment from the sbuf to the
 *   relevant fragment on the rbuf
 * - else, if the datatype is friendly, combine relevant fragment from
 *   the shmem segment to the relevant fragment in the rbuf.  Otherwise,
 *   use the convertor to copy the fragment out of shmem into a temp
 *   buffer and do the combination from there to the rbuf.
 *
 * If we don't have a friendly datatype, then free the temporary
 * buffer at the end.
 */


static int reduce_inorder(const void *sbuf, void* rbuf, int count,
                          struct ompi_datatype_t *dtype,
                          struct ompi_op_t *op,
                          int root, struct ompi_communicator_t *comm,
                          mca_coll_base_module_t *module)
{
    struct iovec iov;
    mca_coll_sm_module_t *sm_module = (mca_coll_sm_module_t*) module;
    mca_coll_sm_comm_t *data = sm_module->sm_comm_data;
    int ret, rank, size;
    int flag_num, segment_num, max_segment_num;
    size_t total_size, max_data, bytes;
    mca_coll_sm_in_use_flag_t *flag;
    mca_coll_sm_data_index_t *index;
    size_t ddt_size, segsize;
    size_t segment_ddt_count, segment_ddt_bytes, zero = 0;
    ptrdiff_t extent, gap;

    /* Setup some identities */

    rank = ompi_comm_rank(comm);
    size = ompi_comm_size(comm);

    /* Figure out how much we should have the convertor copy.  We need
       to have it be in units of a datatype -- i.e., we only want to
       copy a whole datatype worth of data or none at all (we've
       already guaranteed above that the datatype is not larger than a
       segment, so we'll at least get 1). */

    /* ddt_size is the packed size (e.g., MPI_SHORT_INT is 6) */
    ompi_datatype_type_size(dtype, &ddt_size);
    /* extent is from lb to ub (e.g., MPI_SHORT_INT is 8) */
    ompi_datatype_type_extent(dtype, &extent);
    segment_ddt_count = mca_coll_sm_component.sm_fragment_size / ddt_size;
    iov.iov_len = segment_ddt_bytes = segment_ddt_count * ddt_size;
    total_size = ddt_size * count;

    bytes = 0;

    /* Only have one top-level decision as to whether I'm the root or
       not.  Do this at the slight expense of repeating a little logic
       -- but it's better than a conditional branch in every loop
       iteration. */

    /*********************************************************************
     * Root
     *********************************************************************/

    if (root == rank) {
        opal_convertor_t rtb_convertor, rbuf_convertor;
        char *reduce_temp_buffer, *free_buffer, *reduce_target;
        char *inplace_temp;
        int peer;
        size_t count_left = (size_t)count;
        int frag_num = 0;
        bool first_operation = true;

        /* If the datatype is the same packed as it is unpacked, we
           can save a memory copy and just do the reduction operation
           directly from the shared memory segment.  However, if the
           representation is not the same, then we need to get a
           receive convertor and a temporary buffer to receive
           into. */

        if (ompi_datatype_is_contiguous_memory_layout(dtype, count)) {
            reduce_temp_buffer = free_buffer = NULL;
        } else {
            /* When we have a non-contiguous datatype, we need one or
             * two convertors:
             *
             * rtb_convertor: unpacking from the shmem to the
             * reduce_temp_buffer (where we can then apply the
             * reduction).
             *
             * rbuf_convertor: unpacking from the shmem directly to the
             * rbuf (no need to go to the reduce_temp_buffer first and
             * then apply the reduction -- just copy straight to the
             * target buffer).
             */
            OBJ_CONSTRUCT(&rtb_convertor, opal_convertor_t);
            OBJ_CONSTRUCT(&rbuf_convertor, opal_convertor_t);

            /* See lengthy comment in coll basic reduce about
               explanation for how to malloc the extra buffer.  Note
               that we do not need a buffer big enough to hold "count"
               instances of the datatype (i.e., big enough to hold the
               entire user buffer) -- we only need to be able to hold
               "segment_ddt_count" instances (i.e., the number of
               instances that can be held in a single fragment) */

            segsize = opal_datatype_span(&dtype->super, segment_ddt_count, &gap);

            free_buffer = (char*)malloc(segsize);
            if (NULL == free_buffer) {
                return OMPI_ERR_OUT_OF_RESOURCE;
            }
            reduce_temp_buffer = free_buffer - gap;

            /* Trickery here: we use a potentially smaller count than
               the user count -- use the largest count that is <=
               user's count that will fit within a single segment. */

            if (OMPI_SUCCESS !=
                (ret = opal_convertor_copy_and_prepare_for_recv(
                                       ompi_mpi_local_convertor,
                                       &(dtype->super),
                                       segment_ddt_count,
                                       reduce_temp_buffer,
                                       0,
                                       &rtb_convertor))) {
                free(free_buffer);
                return ret;
            }

            /* See if we need the rbuf_convertor */
            if (size - 1 != rank) {
                if (OMPI_SUCCESS !=
                    (ret = opal_convertor_copy_and_prepare_for_recv(
                                       ompi_mpi_local_convertor,
                                       &(dtype->super),
                                       count,
                                       rbuf,
                                       0,
                                       &rbuf_convertor))) {
                    free(free_buffer);
                    return ret;
                }
            }
        }

        /* If we're a) doing MPI_IN_PLACE (which means we're the root
           -- wouldn't have gotten down here with MPI_IN_PLACE if we
           weren't the root), and b) we're not rank (size-1), then we
           need to copy the rbuf into a temporary buffer and use that
           as the sbuf */

        if (MPI_IN_PLACE == sbuf && (size - 1) != rank) {
            segsize = opal_datatype_span(&dtype->super, count, &gap);
            inplace_temp = (char*)malloc(segsize);
            if (NULL == inplace_temp) {
                if (NULL != free_buffer) {
                    free(free_buffer);
                }
                return OMPI_ERR_OUT_OF_RESOURCE;
            }
            sbuf = inplace_temp - gap;
            ompi_datatype_copy_content_same_ddt(dtype, count, (char *)sbuf, (char *)rbuf);
        } else {
            inplace_temp = NULL;
        }

        /* Main loop over receiving / reducing fragments */

        do {
            flag_num = (data->mcb_operation_count %
                        mca_coll_sm_component.sm_comm_num_in_use_flags);
            FLAG_SETUP(flag_num, flag, data);
            FLAG_WAIT_FOR_IDLE(flag, reduce_root_flag_label);
            FLAG_RETAIN(flag, size, data->mcb_operation_count);
            ++data->mcb_operation_count;

            /* Loop over all the segments in this set */

            segment_num =
                flag_num * mca_coll_sm_component.sm_segs_per_inuse_flag;
            max_segment_num =
                (flag_num + 1) * mca_coll_sm_component.sm_segs_per_inuse_flag;
            reduce_target = (((char*) rbuf) + (frag_num * extent * segment_ddt_count));
            do {

                /* Note that all the other coll modules reduce from
                   process (size-1) to 0, so that's the order we'll do
                   it here. */
                /* Process (size-1) is the root (special case) */
                if (size - 1 == rank) {
                    /* If we're the root *and* the first process to be
                       combined *and* this is the first segment in the
                       entire algorithm, then just copy the whole sbuf
                       to rbuf.  That way, we never need to copy from
                       my sbuf again (i.e., do the copy all at once
                       since all the data is local, and then don't
                       worry about it for the rest of the
                       algorithm) */
                    if (first_operation) {
                        first_operation = false;
                        if (MPI_IN_PLACE != sbuf) {
                            ompi_datatype_copy_content_same_ddt(dtype, count,
                                               reduce_target, (char*)sbuf);
                        }
                    }
                }

                /* Process (size-1) is not the root */
                else {
                    /* Wait for the data to be copied into shmem, just
                       like any other non-root process */
                    index = &(data->mcb_data_index[segment_num]);
                    PARENT_WAIT_FOR_NOTIFY_SPECIFIC(size - 1, rank, index, max_data, reduce_root_parent_label1);

                    /* If the datatype is contiguous, just copy it
                       straight to the reduce_target */
                    if (NULL == free_buffer) {
                        memcpy(reduce_target, ((char*)index->mcbmi_data) +
                               (size - 1) * mca_coll_sm_component.sm_fragment_size, max_data);
                    }
                    /* If the datatype is noncontiguous, use the
                       rbuf_convertor to unpack it straight to the
                       rbuf */
                    else {
                        max_data = segment_ddt_bytes;
                        COPY_FRAGMENT_OUT(rbuf_convertor, size - 1, index,
                                          iov, max_data);
                    }
                }

                /* Loop over all the remaining processes, receiving
                   and reducing them in order */

                for (peer = size - 2; peer >= 0; --peer) {

                    /* Handle the case where the source is this
                       process (which, by definition, excludes the
                       sbuf_copied_to_rbuf case because that can
                       *only* happen when root==0).  In this case, we
                       don't need to wait for the peer (i.e., me) to
                       copy into shmem -- just reduce directly from my
                       sbuf. */
                    if (rank == peer) {
                        ompi_op_reduce(op,
                                       ((char *) sbuf) +
                                       frag_num * extent * segment_ddt_count,
                                       reduce_target,
                                       min(count_left, segment_ddt_count),
                                       dtype);
                    }

                    /* Now handle the case where the source is not
                       this process.  Wait for the process to copy to
                       the segment into shmem. */
                    else {
                        index = &(data->mcb_data_index[segment_num]);
                        PARENT_WAIT_FOR_NOTIFY_SPECIFIC(peer, rank,
                                                        index, max_data, reduce_root_parent_label2);

                        /* If we don't need an extra buffer, then do the
                           reduction operation on the fragment straight
                           from the shmem. */

                        if (NULL == free_buffer) {
                            ompi_op_reduce(op,
                                           (index->mcbmi_data +
                                            (peer * mca_coll_sm_component.sm_fragment_size)),
                                           reduce_target,
                                           min(count_left, segment_ddt_count),
                                           dtype);
                        }

                        /* Otherwise, unpack the fragment to the temporary
                           buffer and then do the reduction from there */

                        else {
                            /* Unpack the fragment into my temporary
                               buffer */
                            max_data = segment_ddt_bytes;
                            COPY_FRAGMENT_OUT(rtb_convertor, peer, index,
                                              iov, max_data);
                            opal_convertor_set_position(&rtb_convertor, &zero);

                            /* Do the reduction on this fragment */
                            ompi_op_reduce(op, reduce_temp_buffer,
                                           reduce_target,
                                           min(count_left, segment_ddt_count),
                                           dtype);
                        }
                    } /* whether this process was me or not */
                } /* loop over all proceses */

                /* We've iterated through all the processes -- now we
                   move on to the next segment */

                count_left -= segment_ddt_count;
                bytes += segment_ddt_bytes;
                ++segment_num;
                ++frag_num;
                reduce_target += extent * segment_ddt_count;
            } while (bytes < total_size && segment_num < max_segment_num);

            /* Root is now done with this set of segments */
            FLAG_RELEASE(flag);
        } while (bytes < total_size);

        /* Kill the convertor, if we had one */

        if (NULL != free_buffer) {
            OBJ_DESTRUCT(&rtb_convertor);
            OBJ_DESTRUCT(&rbuf_convertor);
            free(free_buffer);
        }
        if (NULL != inplace_temp) {
            free(inplace_temp);
        }
    }

    /*********************************************************************
     * Non-root
     *********************************************************************/

    else {
        /* Here we get a convertor for the full count that the user
           provided (as opposed to the convertor that the root got) */

        opal_convertor_t sbuf_convertor;
        OBJ_CONSTRUCT(&sbuf_convertor, opal_convertor_t);
        if (OMPI_SUCCESS !=
            (ret =
             opal_convertor_copy_and_prepare_for_send(ompi_mpi_local_convertor,
                                                      &(dtype->super),
                                                      count,
                                                      sbuf,
                                                      0,
                                                      &sbuf_convertor))) {
            return ret;
        }

        /* Loop over sending fragments to the root */

        do {
            flag_num = (data->mcb_operation_count %
                        mca_coll_sm_component.sm_comm_num_in_use_flags);

            /* Wait for the root to mark this set of segments as
               ours */
            FLAG_SETUP(flag_num, flag, data);
            FLAG_WAIT_FOR_OP(flag, data->mcb_operation_count, reduce_nonroot_flag_label);
            ++data->mcb_operation_count;

            /* Loop over all the segments in this set */

            segment_num =
                flag_num * mca_coll_sm_component.sm_segs_per_inuse_flag;
            max_segment_num =
                (flag_num + 1) * mca_coll_sm_component.sm_segs_per_inuse_flag;
            do {
                index = &(data->mcb_data_index[segment_num]);

                /* Copy from the user's buffer to my shared mem
                   segment */
                max_data = segment_ddt_bytes;
                COPY_FRAGMENT_IN(sbuf_convertor, index, rank, iov, max_data);
                bytes += max_data;

                /* Wait for the write to absolutely complete */
                opal_atomic_wmb();

                /* Tell my parent (always the reduction root -- we're
                   ignoring the mcb_tree parent/child relationships
                   here) that this fragment is ready */
                CHILD_NOTIFY_PARENT(rank, root, index, max_data);

                ++segment_num;
            } while (bytes < total_size && segment_num < max_segment_num);

            /* We're finished with this set of segments */
            FLAG_RELEASE(flag);
        } while (bytes < total_size);

        /* Kill the convertor */

        OBJ_DESTRUCT(&sbuf_convertor);
    }

    /* All done */

    return OMPI_SUCCESS;
}


#if WANT_REDUCE_NO_ORDER
/**
 * Unordered shared memory reduction.
 *
 * This function performs the reduction in whatever order the operands
 * arrive.
 */
static int reduce_no_order(const void *sbuf, void* rbuf, int count,
                           struct ompi_datatype_t *dtype,
                           struct ompi_op_t *op,
                           int root, struct ompi_communicator_t *comm,
                           mca_coll_base_module_t *module)
{
    return OMPI_ERR_NOT_IMPLEMENTED;
}
#endif
