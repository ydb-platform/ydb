/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2006 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2006 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2014      NVIDIA Corporation.  All rights reserved.
 * Copyright (c) 2017-2018 Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 * Copyright (c) 2017      Intel, Inc. All rights reserved
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_CONVERTOR_H_HAS_BEEN_INCLUDED
#define OPAL_CONVERTOR_H_HAS_BEEN_INCLUDED

#include "opal_config.h"

#ifdef HAVE_SYS_UIO_H
#include <sys/uio.h>
#endif

#include "opal/constants.h"
#include "opal/datatype/opal_datatype.h"
#include "opal/prefetch.h"

BEGIN_C_DECLS
/*
 * CONVERTOR SECTION
 */
/* keep the last 16 bits free for data flags */
#define CONVERTOR_DATATYPE_MASK    0x0000FFFF
#define CONVERTOR_SEND_CONVERSION  0x00010000
#define CONVERTOR_RECV             0x00020000
#define CONVERTOR_SEND             0x00040000
#define CONVERTOR_HOMOGENEOUS      0x00080000
#define CONVERTOR_NO_OP            0x00100000
#define CONVERTOR_WITH_CHECKSUM    0x00200000
#define CONVERTOR_CUDA             0x00400000
#define CONVERTOR_CUDA_ASYNC       0x00800000
#define CONVERTOR_TYPE_MASK        0x10FF0000
#define CONVERTOR_STATE_START      0x01000000
#define CONVERTOR_STATE_COMPLETE   0x02000000
#define CONVERTOR_STATE_ALLOC      0x04000000
#define CONVERTOR_COMPLETED        0x08000000
#define CONVERTOR_CUDA_UNIFIED     0x10000000
#define CONVERTOR_HAS_REMOTE_SIZE  0x20000000
#define CONVERTOR_SKIP_CUDA_INIT   0x40000000

union dt_elem_desc;
typedef struct opal_convertor_t opal_convertor_t;

typedef int32_t (*convertor_advance_fct_t)( opal_convertor_t* pConvertor,
                                            struct iovec* iov,
                                            uint32_t* out_size,
                                            size_t* max_data );
typedef void*(*memalloc_fct_t)( size_t* pLength, void* userdata );
typedef void*(*memcpy_fct_t)( void* dest, const void* src, size_t n, opal_convertor_t* pConvertor );

/* The master convertor struct (defined in convertor_internal.h) */
struct opal_convertor_master_t;

struct dt_stack_t {
    int32_t           index;    /**< index in the element description */
    int16_t           type;     /**< the type used for the last pack/unpack (original or OPAL_DATATYPE_UINT1) */
    int16_t           padding;
    size_t            count;    /**< number of times we still have to do it */
    ptrdiff_t         disp;     /**< actual displacement depending on the count field */
};
typedef struct dt_stack_t dt_stack_t;

/**
 *
 */
#define DT_STATIC_STACK_SIZE   5                /**< This should be sufficient for most applications */

struct opal_convertor_t {
    opal_object_t                 super;          /**< basic superclass */
    uint32_t                      remoteArch;     /**< the remote architecture */
    uint32_t                      flags;          /**< the properties of this convertor */
    size_t                        local_size;     /**< overall length data on local machine, compared to bConverted */
    size_t                        remote_size;    /**< overall length data on remote machine, compared to bConverted */
    const opal_datatype_t*        pDesc;          /**< the datatype description associated with the convertor */
    const dt_type_desc_t*         use_desc;       /**< the version used by the convertor (normal or optimized) */
    opal_datatype_count_t         count;          /**< the total number of full datatype elements */

    /* --- cacheline boundary (64 bytes - if 64bits arch and !OPAL_ENABLE_DEBUG) --- */
    uint32_t                      stack_size;     /**< size of the allocated stack */
    unsigned char*                pBaseBuf;       /**< initial buffer as supplied by the user */
    dt_stack_t*                   pStack;         /**< the local stack for the actual conversion */
    convertor_advance_fct_t       fAdvance;       /**< pointer to the pack/unpack functions */

    /* --- cacheline boundary (96 bytes - if 64bits arch and !OPAL_ENABLE_DEBUG) --- */
    struct opal_convertor_master_t* master;       /**< the master convertor */

    /* All others fields get modified for every call to pack/unpack functions */
    uint32_t                      stack_pos;      /**< the actual position on the stack */
    size_t                        partial_length; /**< amount of data left over from the last unpack */
    size_t                        bConverted;     /**< # of bytes already converted */

    /* --- cacheline boundary (128 bytes - if 64bits arch and !OPAL_ENABLE_DEBUG) --- */
    uint32_t                      checksum;       /**< checksum computed by pack/unpack operation */
    uint32_t                      csum_ui1;       /**< partial checksum computed by pack/unpack operation */
    size_t                        csum_ui2;       /**< partial checksum computed by pack/unpack operation */

    /* --- fields are no more aligned on cacheline --- */
    dt_stack_t                    static_stack[DT_STATIC_STACK_SIZE];  /**< local stack for small datatypes */

#if OPAL_CUDA_SUPPORT
    memcpy_fct_t                  cbmemcpy;       /**< memcpy or cuMemcpy */
    void *                        stream;         /**< CUstream for async copy */
#endif
};
OPAL_DECLSPEC OBJ_CLASS_DECLARATION( opal_convertor_t );


/*
 *
 */
static inline uint32_t opal_convertor_get_checksum( opal_convertor_t* convertor )
{
    return convertor->checksum;
}


/*
 *
 */
OPAL_DECLSPEC int32_t opal_convertor_pack( opal_convertor_t* pConv, struct iovec* iov,
                                           uint32_t* out_size, size_t* max_data );

/*
 *
 */
OPAL_DECLSPEC int32_t opal_convertor_unpack( opal_convertor_t* pConv, struct iovec* iov,
                                             uint32_t* out_size, size_t* max_data );

/*
 *
 */
OPAL_DECLSPEC opal_convertor_t* opal_convertor_create( int32_t remote_arch, int32_t mode );


/**
 * The cleanup function will put the convertor in exactly the same state as after a call
 * to opal_convertor_construct. Therefore, all PML can call OBJ_DESTRUCT on the request's
 * convertors without having to call OBJ_CONSTRUCT everytime they grab a new one from the
 * cache. The OBJ_CONSTRUCT on the convertor should be called only on the first creation
 * of a request (not when extracted from the cache).
 */
static inline int opal_convertor_cleanup( opal_convertor_t* convertor )
{
    if( OPAL_UNLIKELY(convertor->stack_size > DT_STATIC_STACK_SIZE) ) {
        free( convertor->pStack );
        convertor->pStack     = convertor->static_stack;
        convertor->stack_size = DT_STATIC_STACK_SIZE;
    }
    convertor->pDesc     = NULL;
    convertor->stack_pos = 0;
    convertor->flags     = OPAL_DATATYPE_FLAG_NO_GAPS | CONVERTOR_COMPLETED;

    return OPAL_SUCCESS;
}


/**
 * Return:   0 if no packing is required for sending (the upper layer
 *             can use directly the pointer to the contiguous user
 *             buffer).
 *           1 if data does need to be packed, i.e. heterogeneous peers
 *             (source arch != dest arch) or non contiguous memory
 *             layout.
 */
static inline int32_t opal_convertor_need_buffers( const opal_convertor_t* pConvertor )
{
    if (OPAL_UNLIKELY(0 == (pConvertor->flags & CONVERTOR_HOMOGENEOUS))) return 1;
#if OPAL_CUDA_SUPPORT
    if( pConvertor->flags & (CONVERTOR_CUDA | CONVERTOR_CUDA_UNIFIED)) return 1;
#endif
    if( pConvertor->flags & OPAL_DATATYPE_FLAG_NO_GAPS ) return 0;
    if( (pConvertor->count == 1) && (pConvertor->flags & OPAL_DATATYPE_FLAG_CONTIGUOUS) ) return 0;
    return 1;
}

/**
 * Update the size of the remote datatype representation. The size will
 * depend on the configuration of the master convertor. In homogeneous
 * environments, the local and remote sizes are identical.
 */
size_t
opal_convertor_compute_remote_size( opal_convertor_t* pConv );

/**
 * Return the local size of the convertor (count times the size of the datatype).
 */
static inline void opal_convertor_get_packed_size( const opal_convertor_t* pConv,
                                                   size_t* pSize )
{
    *pSize = pConv->local_size;
}


/**
 * Return the remote size of the convertor (count times the remote size of the
 * datatype). On homogeneous environments the local and remote sizes are
 * identical.
 */
static inline void opal_convertor_get_unpacked_size( const opal_convertor_t* pConv,
                                                     size_t* pSize )
{
    if( pConv->flags & CONVERTOR_HOMOGENEOUS ) {
        *pSize = pConv->local_size;
        return;
    }
    if( 0 == (CONVERTOR_HAS_REMOTE_SIZE & pConv->flags) ) {
        assert(! (pConv->flags & CONVERTOR_SEND));
        opal_convertor_compute_remote_size( (opal_convertor_t*)pConv);
    }
    *pSize = pConv->remote_size;
}

/**
 * Return the current absolute position of the next pack/unpack. This function is
 * mostly useful for contiguous datatypes, when we need to get the pointer to the
 * contiguous piece of memory.
 */
static inline void opal_convertor_get_current_pointer( const opal_convertor_t* pConv,
                                                       void** position )
{
    unsigned char* base = pConv->pBaseBuf + pConv->bConverted + pConv->pDesc->true_lb;
    *position = (void*)base;
}

static inline void opal_convertor_get_offset_pointer( const opal_convertor_t* pConv,
                                                      size_t offset, void** position )
{
    unsigned char* base = pConv->pBaseBuf + offset + pConv->pDesc->true_lb;
    *position = (void*)base;
}


/*
 *
 */
OPAL_DECLSPEC int32_t opal_convertor_prepare_for_send( opal_convertor_t* convertor,
                                                       const struct opal_datatype_t* datatype,
                                                       size_t count,
                                                       const void* pUserBuf);

static inline int32_t opal_convertor_copy_and_prepare_for_send( const opal_convertor_t* pSrcConv,
                                                                const struct opal_datatype_t* datatype,
                                                                size_t count,
                                                                const void* pUserBuf,
                                                                int32_t flags,
                                                                opal_convertor_t* convertor )
{
    convertor->remoteArch = pSrcConv->remoteArch;
    convertor->flags      = pSrcConv->flags | flags;
    convertor->master     = pSrcConv->master;

    return opal_convertor_prepare_for_send( convertor, datatype, count, pUserBuf );
}

/*
 *
 */
OPAL_DECLSPEC int32_t opal_convertor_prepare_for_recv( opal_convertor_t* convertor,
                                                       const struct opal_datatype_t* datatype,
                                                       size_t count,
                                                       const void* pUserBuf );
static inline int32_t opal_convertor_copy_and_prepare_for_recv( const opal_convertor_t* pSrcConv,
                                                                const struct opal_datatype_t* datatype,
                                                                size_t count,
                                                                const void* pUserBuf,
                                                                int32_t flags,
                                                                opal_convertor_t* convertor )
{
    convertor->remoteArch = pSrcConv->remoteArch;
    convertor->flags      = (pSrcConv->flags | flags);
    convertor->master     = pSrcConv->master;

    return opal_convertor_prepare_for_recv( convertor, datatype, count, pUserBuf );
}

/*
 * Give access to the raw memory layout based on the datatype.
 */
OPAL_DECLSPEC int32_t
opal_convertor_raw( opal_convertor_t* convertor,  /* [IN/OUT] */
                    struct iovec* iov,            /* [IN/OUT] */
                    uint32_t* iov_count,          /* [IN/OUT] */
                    size_t* length );             /* [OUT]    */


/*
 * Upper level does not need to call the _nocheck function directly.
 */
OPAL_DECLSPEC int32_t
opal_convertor_set_position_nocheck( opal_convertor_t* convertor,
                                     size_t* position );
static inline int32_t
opal_convertor_set_position( opal_convertor_t* convertor,
                             size_t* position )
{
    /*
     * Do not allow the convertor to go outside the data boundaries. This test include
     * the check for datatype with size zero as well as for convertors with a count of zero.
     */
    if( OPAL_UNLIKELY(convertor->local_size <= *position) ) {
        convertor->flags |= CONVERTOR_COMPLETED;
        convertor->bConverted = convertor->local_size;
        *position = convertor->bConverted;
        return OPAL_SUCCESS;
    }

    /*
     * If the convertor is already at the correct position we are happy.
     */
    if( OPAL_LIKELY((*position) == convertor->bConverted) ) return OPAL_SUCCESS;

    /* Remove the completed flag if it's already set */
    convertor->flags &= ~CONVERTOR_COMPLETED;

    if( !(convertor->flags & CONVERTOR_WITH_CHECKSUM) &&
        (convertor->flags & OPAL_DATATYPE_FLAG_NO_GAPS) &&
        (convertor->flags & (CONVERTOR_SEND | CONVERTOR_HOMOGENEOUS)) ) {
        /* Contiguous and no checkpoint and no homogeneous unpack */
        convertor->bConverted = *position;
        return OPAL_SUCCESS;
    }

    return opal_convertor_set_position_nocheck( convertor, position );
}

/*
 *
 */
static inline int32_t
opal_convertor_personalize( opal_convertor_t* convertor,
                            uint32_t flags,
                            size_t* position )
{
    convertor->flags |= flags;

    if( OPAL_UNLIKELY(NULL == position) )
        return OPAL_SUCCESS;
    return opal_convertor_set_position( convertor, position );
}

/*
 *
 */
OPAL_DECLSPEC int
opal_convertor_clone( const opal_convertor_t* source,
                      opal_convertor_t* destination,
                      int32_t copy_stack );

static inline int
opal_convertor_clone_with_position( const opal_convertor_t* source,
                                    opal_convertor_t* destination,
                                    int32_t copy_stack,
                                    size_t* position )
{
    (void)opal_convertor_clone( source, destination, copy_stack );
    return opal_convertor_set_position( destination, position );
}

/*
 *
 */
OPAL_DECLSPEC void
opal_convertor_dump( opal_convertor_t* convertor );

OPAL_DECLSPEC void
opal_datatype_dump_stack( const dt_stack_t* pStack,
                          int stack_pos,
                          const union dt_elem_desc* pDesc,
                          const char* name );

/*
 *
 */
OPAL_DECLSPEC int
opal_convertor_generic_simple_position( opal_convertor_t* pConvertor,
                                        size_t* position );

END_C_DECLS

#endif  /* OPAL_CONVERTOR_H_HAS_BEEN_INCLUDED */
