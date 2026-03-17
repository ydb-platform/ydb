/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2014 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2013      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2017      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#ifndef OPAL_CONVERTOR_INTERNAL_HAS_BEEN_INCLUDED
#define OPAL_CONVERTOR_INTERNAL_HAS_BEEN_INCLUDED

#include "opal_config.h"

#include "opal/datatype/opal_convertor.h"

BEGIN_C_DECLS

typedef int32_t (*conversion_fct_t)( opal_convertor_t* pConvertor, uint32_t count,
                                     const void* from, size_t from_len, ptrdiff_t from_extent,
                                     void* to, size_t to_length, ptrdiff_t to_extent,
                                     ptrdiff_t *advance );

typedef struct opal_convertor_master_t {
    struct opal_convertor_master_t* next;
    uint32_t                        remote_arch;
    uint32_t                        flags;
    uint32_t                        hetero_mask;
    const size_t                    remote_sizes[OPAL_DATATYPE_MAX_PREDEFINED];
    conversion_fct_t*               pFunctions;   /**< the convertor functions pointer */
} opal_convertor_master_t;

/*
 * Find or create a new master convertor based on a specific architecture. The master
 * convertor hold all informations related to a defined architecture, such as the sizes
 * of the predefined data-types, the conversion functions, ...
 */
opal_convertor_master_t* opal_convertor_find_or_create_master( uint32_t remote_arch );

/*
 * Destroy all pending master convertors. This function is usually called when we
 * shutdown the data-type engine, once all convertors have been destroyed.
 */
void opal_convertor_destroy_masters( void );


#if OPAL_ENABLE_DEBUG
extern bool opal_pack_debug;
extern bool opal_unpack_debug;
#endif  /* OPAL_ENABLE_DEBUG */

END_C_DECLS

#endif  /* OPAL_CONVERTOR_INTERNAL_HAS_BEEN_INCLUDED */
