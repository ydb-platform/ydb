/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2006 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2013 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2006 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2014-2015 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "opal/datatype/opal_datatype.h"
#include "opal/datatype/opal_datatype_internal.h"
#include "opal/datatype/opal_convertor.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/datatype/ompi_datatype_internal.h"
/*
 * opal_datatype_sndrcv
 *
 * Function: - copy MPI message from buffer into another
 *           - send/recv done if cannot optimize
 * Accepts:  - send buffer
 *           - send count
 *           - send datatype
 *           - receive buffer
 *           - receive count
 *           - receive datatype
 *           - tag
 *           - communicator
 * Returns:  - MPI_SUCCESS or error code
 */
int32_t ompi_datatype_sndrcv( const void *sbuf, int32_t scount, const ompi_datatype_t* sdtype,
                              void *rbuf, int32_t rcount, const ompi_datatype_t* rdtype)
{
    opal_convertor_t send_convertor, recv_convertor;
    struct iovec iov;
    int length, completed;
    uint32_t iov_count;
    size_t max_data;

    /* First check if we really have something to do */
    if (0 == rcount || 0 == rdtype->super.size) {
        return ((0 == scount || 0 == sdtype->super.size) ? MPI_SUCCESS : MPI_ERR_TRUNCATE);
    }

    /* If same datatypes used, just copy. */
    if (sdtype == rdtype) {
        int32_t count = ( scount < rcount ? scount : rcount );
        opal_datatype_copy_content_same_ddt(&(rdtype->super), count, (char*)rbuf, (char*)sbuf);
        return ((scount > rcount) ? MPI_ERR_TRUNCATE : MPI_SUCCESS);
    }

    /* If receive packed. */
    if (rdtype->id == OMPI_DATATYPE_MPI_PACKED) {
        OBJ_CONSTRUCT( &send_convertor, opal_convertor_t );
        opal_convertor_copy_and_prepare_for_send( ompi_mpi_local_convertor,
                                                  &(sdtype->super), scount, sbuf, 0,
                                                  &send_convertor );

        iov_count = 1;
        iov.iov_base = (IOVBASE_TYPE*)rbuf;
        iov.iov_len = scount * sdtype->super.size;
        if( (int32_t)iov.iov_len > rcount ) iov.iov_len = rcount;

        opal_convertor_pack( &send_convertor, &iov, &iov_count, &max_data );
        OBJ_DESTRUCT( &send_convertor );
        return ((max_data < (size_t)rcount) ? MPI_ERR_TRUNCATE : MPI_SUCCESS);
    }

    /* If send packed. */
    if (sdtype->id == OMPI_DATATYPE_MPI_PACKED) {
        OBJ_CONSTRUCT( &recv_convertor, opal_convertor_t );
        opal_convertor_copy_and_prepare_for_recv( ompi_mpi_local_convertor,
                                                  &(rdtype->super), rcount, rbuf, 0,
                                                  &recv_convertor );

        iov_count = 1;
        iov.iov_base = (IOVBASE_TYPE*)sbuf;
        iov.iov_len = rcount * rdtype->super.size;
        if( (int32_t)iov.iov_len > scount ) iov.iov_len = scount;

        opal_convertor_unpack( &recv_convertor, &iov, &iov_count, &max_data );
        OBJ_DESTRUCT( &recv_convertor );
        return (((size_t)scount > max_data) ? MPI_ERR_TRUNCATE : MPI_SUCCESS);
    }

    iov.iov_len = length = 64 * 1024;
    iov.iov_base = (IOVBASE_TYPE*)malloc( length * sizeof(char) );

    OBJ_CONSTRUCT( &send_convertor, opal_convertor_t );
    opal_convertor_copy_and_prepare_for_send( ompi_mpi_local_convertor,
                                              &(sdtype->super), scount, sbuf, 0,
                                              &send_convertor );
    OBJ_CONSTRUCT( &recv_convertor, opal_convertor_t );
    opal_convertor_copy_and_prepare_for_recv( ompi_mpi_local_convertor,
                                              &(rdtype->super), rcount, rbuf, 0,
                                              &recv_convertor );

    completed = 0;
    while( !completed ) {
        iov.iov_len = length;
        iov_count = 1;
        max_data = length;
        completed |= opal_convertor_pack( &send_convertor, &iov, &iov_count, &max_data );
        completed |= opal_convertor_unpack( &recv_convertor, &iov, &iov_count, &max_data );
    }
    free( iov.iov_base );
    OBJ_DESTRUCT( &send_convertor );
    OBJ_DESTRUCT( &recv_convertor );

    return ( (scount * sdtype->super.size) <= (rcount * rdtype->super.size) ? MPI_SUCCESS : MPI_ERR_TRUNCATE );
}
