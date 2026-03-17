/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2016 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2008 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2015-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include <stdio.h>

#include "ompi/runtime/params.h"
#include "ompi/communicator/communicator.h"
#include "ompi/datatype/ompi_datatype.h"
#include "opal/datatype/opal_convertor.h"

int ompi_datatype_pack_external(const char datarep[], const void *inbuf, int incount,
                                ompi_datatype_t *datatype, void *outbuf,
                                MPI_Aint outsize, MPI_Aint *position)
{
    int rc = MPI_SUCCESS;
    opal_convertor_t local_convertor;
    struct iovec invec;
    unsigned int iov_count;
    size_t size;

    OBJ_CONSTRUCT(&local_convertor, opal_convertor_t);

    /* The resulting convertor will be set to the position zero. We have to use
     * CONVERTOR_SEND_CONVERSION in order to force the convertor to do anything
     * more than just packing the data.
     */
    opal_convertor_copy_and_prepare_for_send( ompi_mpi_external32_convertor,
                                              &(datatype->super), incount, (void *) inbuf,
                                              CONVERTOR_SEND_CONVERSION,
                                              &local_convertor );

    /* Check for truncation */
    opal_convertor_get_packed_size( &local_convertor, &size );
    if( (*position + size) > (size_t)outsize ) {  /* we can cast as we already checked for < 0 */
        OBJ_DESTRUCT( &local_convertor );
        return MPI_ERR_TRUNCATE;
    }

    /* Prepare the iovec with all informations */
    invec.iov_base = (char*) outbuf + (*position);
    invec.iov_len = size;

    /* Do the actual packing */
    iov_count = 1;
    rc = opal_convertor_pack( &local_convertor, &invec, &iov_count, &size );
    *position += size;
    OBJ_DESTRUCT( &local_convertor );

    /* All done.  Note that the convertor returns 1 upon success, not
       OMPI_SUCCESS. */
    return (rc == 1) ? OMPI_SUCCESS : MPI_ERR_UNKNOWN;
}

int ompi_datatype_unpack_external (const char datarep[], const void *inbuf, MPI_Aint insize,
                                   MPI_Aint *position, void *outbuf, int outcount,
                                   ompi_datatype_t *datatype)
{
    int rc = MPI_SUCCESS;
    opal_convertor_t local_convertor;
    struct iovec outvec;
    unsigned int iov_count;
    size_t size;

    OBJ_CONSTRUCT(&local_convertor, opal_convertor_t);

    /* the resulting convertor will be set to the position ZERO */
    opal_convertor_copy_and_prepare_for_recv( ompi_mpi_external32_convertor,
                                              &(datatype->super), outcount, outbuf,
                                              0,
                                              &local_convertor );

    /* Check for truncation */
    opal_convertor_get_packed_size( &local_convertor, &size );
    if( (*position + size) > (unsigned int)insize ) {
        OBJ_DESTRUCT( &local_convertor );
        return MPI_ERR_TRUNCATE;
    }

    /* Prepare the iovec with all informations */
    outvec.iov_base = (char*) inbuf + (*position);
    outvec.iov_len = size;

    /* Do the actual unpacking */
    iov_count = 1;
    rc = opal_convertor_unpack( &local_convertor, &outvec, &iov_count, &size );
    *position += size;
    OBJ_DESTRUCT( &local_convertor );

    /* All done.  Note that the convertor returns 1 upon success, not
       OMPI_SUCCESS. */
    return (rc == 1) ? OMPI_SUCCESS : MPI_ERR_UNKNOWN;
}

int ompi_datatype_pack_external_size(const char datarep[], int incount,
                                     ompi_datatype_t *datatype, MPI_Aint *size)
{
    opal_convertor_t local_convertor;
    size_t length;

    OBJ_CONSTRUCT(&local_convertor, opal_convertor_t);

    /* the resulting convertor will be set to the position ZERO */
    opal_convertor_copy_and_prepare_for_recv( ompi_mpi_external32_convertor,
                                              &(datatype->super), incount, NULL,
                                              CONVERTOR_SEND_CONVERSION,
                                              &local_convertor );

    opal_convertor_get_unpacked_size( &local_convertor, &length );
    *size = (MPI_Aint)length;
    OBJ_DESTRUCT( &local_convertor );

    return OMPI_SUCCESS;
}
