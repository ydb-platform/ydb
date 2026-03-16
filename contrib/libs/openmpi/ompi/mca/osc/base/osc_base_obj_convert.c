/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2004-2006 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * Copyright (c) 2004-2008 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2015 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2009      Sun Microsystems, Inc. All rights reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2015      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Intel, Inc. All rights reserved.
 * Copyright (c) 2017      IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/*
 * utility functions for dealing with remote datatype and op structures
 */

#include "ompi_config.h"

#include "opal/datatype/opal_convertor.h"
#include "opal/datatype/opal_convertor_internal.h"
#include "opal/datatype/opal_datatype_prototypes.h"
#include "opal/util/show_help.h"

#include "ompi/op/op.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/datatype/ompi_datatype_internal.h"

#include "osc_base_obj_convert.h"
#include "ompi/memchecker.h"

#define OMPI_OSC_BASE_DECODE_MAX 32

int
ompi_osc_base_get_primitive_type_info(ompi_datatype_t *datatype,
                                      ompi_datatype_t **prim_datatype,
                                      uint32_t *prim_count)
{
    ompi_datatype_t *primitive_datatype = NULL;
    size_t datatype_size, primitive_size, primitive_count;

    primitive_datatype = ompi_datatype_get_single_predefined_type_from_args(datatype);
    if( NULL == primitive_datatype ) {
        *prim_count = 0;
        return OMPI_SUCCESS;
    }
    ompi_datatype_type_size( datatype, &datatype_size );
    ompi_datatype_type_size( primitive_datatype, &primitive_size );
    primitive_count = datatype_size / primitive_size;
#if OPAL_ENABLE_DEBUG
    assert( 0 == (datatype_size % primitive_size) );
#endif  /* OPAL_ENABLE_DEBUG */

    /* We now have the count as a size_t, convert it to an uint32_t */
    *prim_datatype = primitive_datatype;
    *prim_count = (uint32_t)primitive_count;

    return OMPI_SUCCESS;
}

int ompi_osc_base_process_op (void *outbuf, void *inbuf, size_t inbuflen,
                              struct ompi_datatype_t *datatype, int count,
                              ompi_op_t *op)
{
    if (op == &ompi_mpi_op_replace.op) {
        return OMPI_ERR_NOT_SUPPORTED;
    }

    /* TODO: Remove the following check when ompi adds support */
    if(MPI_MINLOC == op || MPI_MAXLOC == op) {
        if(MPI_SHORT_INT == datatype ||
           MPI_DOUBLE_INT == datatype ||
           MPI_LONG_INT == datatype ||
           MPI_LONG_DOUBLE_INT == datatype) {
           ompi_communicator_t *comm = &ompi_mpi_comm_world.comm;
           opal_output(0, "Error: %s datatype is currently "
                       "unsupported for MPI_MINLOC/MPI_MAXLOC "
                       "operation\n", datatype->name);
           opal_show_help("help-mpi-api.txt", "mpi-abort", true,
                          comm->c_my_rank,
                          ('\0' != comm->c_name[0]) ? comm->c_name : "<Unknown>",
                          -1);

           ompi_mpi_abort(comm, -1);
        }
    }

    if (ompi_datatype_is_predefined(datatype)) {
        ompi_op_reduce(op, inbuf, outbuf, count, datatype);
    } else {
        opal_convertor_t convertor;
        struct ompi_datatype_t *primitive_datatype = NULL;
        struct iovec iov[OMPI_OSC_BASE_DECODE_MAX];
        uint32_t iov_count;
        size_t size, primitive_size;
        ptrdiff_t lb, extent;
        bool done;

        primitive_datatype = ompi_datatype_get_single_predefined_type_from_args(datatype);
        ompi_datatype_type_size (primitive_datatype, &primitive_size);

        if (ompi_datatype_is_contiguous_memory_layout (datatype, count) &&
            1 == datatype->super.desc.used) {
            /* NTH: the datatype is made up of a contiguous block of the primitive
             * datatype. fast path. do not set up a convertor to deal with the
             * datatype. */
            (void)ompi_datatype_type_size(datatype, &size);
            count *= (size / primitive_size);
            assert( 0 == (size % primitive_size) );

            /* in case it is possible for the datatype to have a non-zero lb in this case.
             * remove me if this is not possible */
            ompi_datatype_get_extent (datatype, &lb, &extent);
            outbuf = (void *)((uintptr_t) outbuf + lb);

            ompi_op_reduce(op, inbuf, outbuf, count, primitive_datatype);
            return OMPI_SUCCESS;
        }

        /* create convertor */
        OBJ_CONSTRUCT(&convertor, opal_convertor_t);
        opal_convertor_copy_and_prepare_for_recv(ompi_mpi_local_convertor, &datatype->super,
                                                 count, outbuf, 0, &convertor);

        do {
            iov_count = OMPI_OSC_BASE_DECODE_MAX;
            done = opal_convertor_raw (&convertor, iov, &iov_count, &size);

            for (uint32_t i = 0 ; i < iov_count ; ++i) {
                int primitive_count = iov[i].iov_len / primitive_size;
                ompi_op_reduce (op, inbuf, iov[i].iov_base, primitive_count, primitive_datatype);
                inbuf = (void *)((intptr_t) inbuf + iov[i].iov_len);
            }
        } while (!done);

        MEMCHECKER(
            memchecker_convertor_call(&opal_memchecker_base_mem_noaccess,
                                      &convertor);
        );

        opal_convertor_cleanup (&convertor);
        OBJ_DESTRUCT(&convertor);
    }

    return OMPI_SUCCESS;
}

int ompi_osc_base_sndrcv_op (const void *origin, int32_t origin_count,
                             struct ompi_datatype_t *origin_dt,
                             void *target, int32_t target_count,
                             struct ompi_datatype_t *target_dt,
                             ompi_op_t *op)
{
    ompi_datatype_t *origin_primitive, *target_primitive;
    opal_convertor_t origin_convertor, target_convertor;
    struct iovec origin_iovec[OMPI_OSC_BASE_DECODE_MAX];
    struct iovec target_iovec[OMPI_OSC_BASE_DECODE_MAX];
    uint32_t origin_iov_count, target_iov_count;
    uint32_t origin_iov_index, target_iov_index;
    size_t origin_size, target_size, primitive_size;
    int primitive_count;
    size_t acc_len;
    bool done;

    if (ompi_datatype_is_predefined(origin_dt) && origin_dt == target_dt) {
        ompi_op_reduce(op, (void *)origin, target, origin_count, origin_dt);

        return OMPI_SUCCESS;
    }

    origin_primitive = ompi_datatype_get_single_predefined_type_from_args(origin_dt);
    target_primitive = ompi_datatype_get_single_predefined_type_from_args(target_dt);

    /* check that the two primitives are the same */
    if (OPAL_UNLIKELY(origin_primitive != target_primitive)) {
        return OMPI_ERR_RMA_SYNC;
    }

    ompi_datatype_type_size (target_primitive, &primitive_size);

    OBJ_CONSTRUCT(&origin_convertor, opal_convertor_t);
    opal_convertor_copy_and_prepare_for_send (ompi_mpi_local_convertor, &origin_dt->super,
                                              origin_count, origin, 0, &origin_convertor);

    OBJ_CONSTRUCT(&target_convertor, opal_convertor_t);
    opal_convertor_copy_and_prepare_for_recv (ompi_mpi_local_convertor, &target_dt->super,
                                              target_count, target, 0, &target_convertor);

    target_iov_index = 0;
    target_iov_count = 0;

    do {
        /* decode segments of the source data */
        origin_iov_count = OMPI_OSC_BASE_DECODE_MAX;
        origin_iov_index = 0;

        done = opal_convertor_raw (&origin_convertor, origin_iovec, &origin_iov_count, &origin_size);

        /* loop on the target segments until we have exhaused the decoded source data */
        while (origin_iov_index != origin_iov_count) {
            if (target_iov_index == target_iov_count) {
                /* decode segments of the target buffer */
                target_iov_count = OMPI_OSC_BASE_DECODE_MAX;
                target_iov_index = 0;
                (void) opal_convertor_raw (&target_convertor, target_iovec, &target_iov_count, &target_size);
            }

            /* we already checked that the target was large enough. this should be impossible */
            assert (0 != target_iov_count);

            /* determine how much to accumulate */
            if (target_iovec[target_iov_index].iov_len < origin_iovec[origin_iov_index].iov_len) {
                acc_len = target_iovec[target_iov_index].iov_len;
            } else {
                acc_len = origin_iovec[origin_iov_index].iov_len;
            }

            primitive_count = acc_len / primitive_size;

            ompi_op_reduce (op, origin_iovec[origin_iov_index].iov_base, target_iovec[target_iov_index].iov_base,
                            primitive_count, target_primitive);

            /* adjust io vectors */
            target_iovec[target_iov_index].iov_len -= acc_len;
            origin_iovec[origin_iov_index].iov_len -= acc_len;
            target_iovec[target_iov_index].iov_base = (void *)((intptr_t) target_iovec[target_iov_index].iov_base + acc_len);
            origin_iovec[origin_iov_index].iov_base = (void *)((intptr_t) origin_iovec[origin_iov_index].iov_base + acc_len);

            origin_iov_index += 0 == origin_iovec[origin_iov_index].iov_len;
            target_iov_index += 0 == target_iovec[target_iov_index].iov_len;
        }
    } while (!done);

    opal_convertor_cleanup (&origin_convertor);
    OBJ_DESTRUCT(&origin_convertor);

    opal_convertor_cleanup (&target_convertor);
    OBJ_DESTRUCT(&target_convertor);

    return OMPI_SUCCESS;
}
