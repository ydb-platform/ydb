/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2016 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_DATATYPE_PROTOTYPES_H_HAS_BEEN_INCLUDED
#define OPAL_DATATYPE_PROTOTYPES_H_HAS_BEEN_INCLUDED

#include "opal_config.h"


BEGIN_C_DECLS

/*
 * First the public ones
 */

OPAL_DECLSPEC int32_t
opal_pack_general( opal_convertor_t* pConvertor,
                   struct iovec* iov, uint32_t* out_size,
                   size_t* max_data );
OPAL_DECLSPEC int32_t
opal_pack_general_checksum( opal_convertor_t* pConvertor,
                            struct iovec* iov, uint32_t* out_size,
                            size_t* max_data );
OPAL_DECLSPEC int32_t
opal_unpack_general( opal_convertor_t* pConvertor,
                     struct iovec* iov, uint32_t* out_size,
                     size_t* max_data );
OPAL_DECLSPEC int32_t
opal_unpack_general_checksum( opal_convertor_t* pConvertor,
                              struct iovec* iov, uint32_t* out_size,
                              size_t* max_data );

/*
 * Now the internal functions
 */
int32_t
opal_pack_homogeneous_contig( opal_convertor_t* pConv,
                          struct iovec* iov, uint32_t* out_size,
                          size_t* max_data );
int32_t
opal_pack_homogeneous_contig_checksum( opal_convertor_t* pConv,
                                   struct iovec* iov, uint32_t* out_size,
                                   size_t* max_data );
int32_t
opal_pack_homogeneous_contig_with_gaps( opal_convertor_t* pConv,
                                    struct iovec* iov, uint32_t* out_size,
                                    size_t* max_data );
int32_t
opal_pack_homogeneous_contig_with_gaps_checksum( opal_convertor_t* pConv,
                                             struct iovec* iov, uint32_t* out_size,
                                             size_t* max_data );
int32_t
opal_generic_simple_pack( opal_convertor_t* pConvertor,
                          struct iovec* iov, uint32_t* out_size,
                          size_t* max_data );
int32_t
opal_generic_simple_pack_checksum( opal_convertor_t* pConvertor,
                                   struct iovec* iov, uint32_t* out_size,
                                   size_t* max_data );
int32_t
opal_unpack_homogeneous_contig( opal_convertor_t* pConv,
                                struct iovec* iov, uint32_t* out_size,
                                size_t* max_data );
int32_t
opal_unpack_homogeneous_contig_checksum( opal_convertor_t* pConv,
                                         struct iovec* iov, uint32_t* out_size,
                                         size_t* max_data );
int32_t
opal_generic_simple_unpack( opal_convertor_t* pConvertor,
                            struct iovec* iov, uint32_t* out_size,
                            size_t* max_data );
int32_t
opal_generic_simple_unpack_checksum( opal_convertor_t* pConvertor,
                                     struct iovec* iov, uint32_t* out_size,
                                     size_t* max_data );

END_C_DECLS

#endif  /* OPAL_DATATYPE_PROTOTYPES_H_HAS_BEEN_INCLUDED */
