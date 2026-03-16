/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2009-2013 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2010-2017 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2013      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2015-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * ompi_datatype_t interface for OMPI internal data type representation
 *
 * ompi_datatype_t is a class which represents contiguous or
 * non-contiguous data together with constituent type-related
 * information.
 */

#ifndef OMPI_DATATYPE_H_HAS_BEEN_INCLUDED
#define OMPI_DATATYPE_H_HAS_BEEN_INCLUDED

#include "ompi_config.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <limits.h>

#include "ompi/constants.h"
#include "opal/datatype/opal_convertor.h"
#include "opal/util/output.h"
#include "mpi.h"

BEGIN_C_DECLS

/* These flags are on top of the flags in opal_datatype.h */
/* Is the datatype predefined as MPI type (not necessarily as OPAL type, e.g. struct/block types) */
#define OMPI_DATATYPE_FLAG_PREDEFINED    0x0200
#define OMPI_DATATYPE_FLAG_ANALYZED      0x0400
#define OMPI_DATATYPE_FLAG_MONOTONIC     0x0800
/* Keep trace of the type of the predefined datatypes */
#define OMPI_DATATYPE_FLAG_DATA_INT      0x1000
#define OMPI_DATATYPE_FLAG_DATA_FLOAT    0x2000
#define OMPI_DATATYPE_FLAG_DATA_COMPLEX  0x3000
#define OMPI_DATATYPE_FLAG_DATA_TYPE     0x3000
/* In which language the datatype is intended for to be used */
#define OMPI_DATATYPE_FLAG_DATA_C        0x4000
#define OMPI_DATATYPE_FLAG_DATA_CPP      0x8000
#define OMPI_DATATYPE_FLAG_DATA_FORTRAN  0xC000
#define OMPI_DATATYPE_FLAG_DATA_LANGUAGE 0xC000

#define OMPI_DATATYPE_MAX_PREDEFINED 47

#if OMPI_DATATYPE_MAX_PREDEFINED > OPAL_DATATYPE_MAX_SUPPORTED
#error Need to increase the number of supported dataypes by OPAL (value OPAL_DATATYPE_MAX_SUPPORTED).
#endif


/* the data description.
 */
struct ompi_datatype_t {
    opal_datatype_t    super;                    /**< Base opal_datatype_t superclass */
    /* --- cacheline 5 boundary (320 bytes) was 32 bytes ago --- */

    int32_t            id;                       /**< OMPI-layers unique id of the type */
    int32_t            d_f_to_c_index;           /**< Fortran index for this datatype */
    struct opal_hash_table_t *d_keyhash;         /**< Attribute fields */

    void*              args;                     /**< Data description for the user */
    void*              packed_description;       /**< Packed description of the datatype */
    uint64_t           pml_data;                 /**< PML-specific information */
    /* --- cacheline 6 boundary (384 bytes) --- */
    char               name[MPI_MAX_OBJECT_NAME];/**< Externally visible name */
    /* --- cacheline 7 boundary (448 bytes) --- */

    /* size: 448, cachelines: 7, members: 7 */
};

typedef struct ompi_datatype_t ompi_datatype_t;

OMPI_DECLSPEC OBJ_CLASS_DECLARATION(ompi_datatype_t);

/**
 * Padded struct to maintain back compatibiltiy.
 * See opal/communicator/communicator.h comments with struct opal_communicator_t
 * for full explanation why we chose the following padding construct for predefines.
 */

/* Using set constant for padding of the DATATYPE handles because the size of
 * base structure is very close to being the same no matter the bitness.
 */
#define PREDEFINED_DATATYPE_PAD 512

struct ompi_predefined_datatype_t {
    struct ompi_datatype_t dt;
    char padding[PREDEFINED_DATATYPE_PAD - sizeof(ompi_datatype_t)];
};

typedef struct ompi_predefined_datatype_t ompi_predefined_datatype_t;

/*
 * The list of predefined datatypes is specified in ompi/include/mpi.h.in
 */

/* Base convertor for all external32 operations */
OMPI_DECLSPEC extern opal_convertor_t* ompi_mpi_external32_convertor;
OMPI_DECLSPEC extern opal_convertor_t* ompi_mpi_local_convertor;
extern struct opal_pointer_array_t ompi_datatype_f_to_c_table;

OMPI_DECLSPEC int32_t ompi_datatype_init( void );
OMPI_DECLSPEC int32_t ompi_datatype_finalize( void );

OMPI_DECLSPEC int32_t ompi_datatype_default_convertors_init( void );
OMPI_DECLSPEC int32_t ompi_datatype_default_convertors_fini( void );

OMPI_DECLSPEC void ompi_datatype_dump (const ompi_datatype_t* pData);
OMPI_DECLSPEC ompi_datatype_t* ompi_datatype_create( int32_t expectedSize );

static inline int32_t
ompi_datatype_is_committed( const ompi_datatype_t* type )
{
    return opal_datatype_is_committed(&type->super);
}

static inline int32_t
ompi_datatype_is_overlapped( const ompi_datatype_t* type )
{
    return opal_datatype_is_overlapped(&type->super);
}

static inline int32_t
ompi_datatype_is_valid( const ompi_datatype_t* type )
{
    return opal_datatype_is_valid(&type->super);
}

static inline int32_t
ompi_datatype_is_predefined( const ompi_datatype_t* type )
{
    return (type->super.flags & OMPI_DATATYPE_FLAG_PREDEFINED);
}

static inline int32_t
ompi_datatype_is_contiguous_memory_layout( const ompi_datatype_t* type, int32_t count )
{
    return opal_datatype_is_contiguous_memory_layout(&type->super, count);
}

static inline int32_t
ompi_datatype_is_monotonic( ompi_datatype_t * type ) {
    if (!(type->super.flags & OMPI_DATATYPE_FLAG_ANALYZED)) {
        if (opal_datatype_is_monotonic(&type->super)) {
            type->super.flags |= OMPI_DATATYPE_FLAG_MONOTONIC;
        }
        type->super.flags |= OMPI_DATATYPE_FLAG_ANALYZED;
    }
    return type->super.flags & OMPI_DATATYPE_FLAG_MONOTONIC;
}

static inline int32_t
ompi_datatype_commit( ompi_datatype_t ** type )
{
    return opal_datatype_commit ( (opal_datatype_t*)*type );
}

OMPI_DECLSPEC int32_t ompi_datatype_destroy( ompi_datatype_t** type);


/*
 * Datatype creation functions
 */
static inline int32_t
ompi_datatype_add( ompi_datatype_t* pdtBase, const ompi_datatype_t* pdtAdd, size_t count,
                   ptrdiff_t disp, ptrdiff_t extent )
{
    return opal_datatype_add( &pdtBase->super, &pdtAdd->super, count, disp, extent );
}

OMPI_DECLSPEC int32_t
ompi_datatype_duplicate( const ompi_datatype_t* oldType, ompi_datatype_t** newType );

OMPI_DECLSPEC int32_t ompi_datatype_create_contiguous( int count, const ompi_datatype_t* oldType, ompi_datatype_t** newType );
OMPI_DECLSPEC int32_t ompi_datatype_create_vector( int count, int bLength, int stride,
                                                   const ompi_datatype_t* oldType, ompi_datatype_t** newType );
OMPI_DECLSPEC int32_t ompi_datatype_create_hvector( int count, int bLength, ptrdiff_t stride,
                                                    const ompi_datatype_t* oldType, ompi_datatype_t** newType );
OMPI_DECLSPEC int32_t ompi_datatype_create_indexed( int count, const int* pBlockLength, const int* pDisp,
                                                    const ompi_datatype_t* oldType, ompi_datatype_t** newType );
OMPI_DECLSPEC int32_t ompi_datatype_create_hindexed( int count, const int* pBlockLength, const ptrdiff_t* pDisp,
                                                     const ompi_datatype_t* oldType, ompi_datatype_t** newType );
OMPI_DECLSPEC int32_t ompi_datatype_create_indexed_block( int count, int bLength, const int* pDisp,
                                                          const ompi_datatype_t* oldType, ompi_datatype_t** newType );
OMPI_DECLSPEC int32_t ompi_datatype_create_hindexed_block( int count, int bLength, const ptrdiff_t* pDisp,
                                                           const ompi_datatype_t* oldType, ompi_datatype_t** newType );
OMPI_DECLSPEC int32_t ompi_datatype_create_struct( int count, const int* pBlockLength, const ptrdiff_t* pDisp,
                                                   ompi_datatype_t* const* pTypes, ompi_datatype_t** newType );
OMPI_DECLSPEC int32_t ompi_datatype_create_darray( int size, int rank, int ndims, int const* gsize_array,
                                                   int const* distrib_array, int const* darg_array,
                                                   int const* psize_array, int order, const ompi_datatype_t* oldtype,
                                                   ompi_datatype_t** newtype);
OMPI_DECLSPEC int32_t ompi_datatype_create_subarray(int ndims, int const* size_array, int const* subsize_array,
                                                    int const* start_array, int order,
                                                    const ompi_datatype_t* oldtype, ompi_datatype_t** newtype);
static inline int32_t
ompi_datatype_create_resized( const ompi_datatype_t* oldType,
                              ptrdiff_t lb,
                              ptrdiff_t extent,
                              ompi_datatype_t** newType )
{
    ompi_datatype_t * type;
    ompi_datatype_duplicate( oldType, &type );
    if ( NULL == type) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }
    opal_datatype_resize ( &type->super, lb, extent );
    *newType = type;
    return OMPI_SUCCESS;
}

static inline int32_t
ompi_datatype_type_lb( const ompi_datatype_t* type, ptrdiff_t* disp )
{
    return opal_datatype_type_lb(&type->super, disp);
}

static inline int32_t
ompi_datatype_type_ub( const ompi_datatype_t* type, ptrdiff_t* disp )
{
    return opal_datatype_type_ub( &type->super, disp);
}

static inline int32_t
ompi_datatype_type_size ( const ompi_datatype_t* type, size_t *size )
{
    return opal_datatype_type_size( &type->super, size);
}

static inline int32_t
ompi_datatype_type_extent( const ompi_datatype_t* type, ptrdiff_t* extent )
{
    return opal_datatype_type_extent( &type->super, extent);
}

static inline int32_t
ompi_datatype_get_extent( const ompi_datatype_t* type, ptrdiff_t* lb, ptrdiff_t* extent)
{
    return opal_datatype_get_extent( &type->super, lb, extent);
}

static inline int32_t
ompi_datatype_get_true_extent( const ompi_datatype_t* type, ptrdiff_t* true_lb, ptrdiff_t* true_extent)
{
    return opal_datatype_get_true_extent( &type->super, true_lb, true_extent);
}

static inline ssize_t
ompi_datatype_get_element_count( const ompi_datatype_t* type, size_t iSize )
{
    return opal_datatype_get_element_count( &type->super, iSize );
}

static inline int32_t
ompi_datatype_set_element_count( const ompi_datatype_t* type, size_t count, size_t* length )
{
    return opal_datatype_set_element_count( &type->super, count, length );
}

static inline int32_t
ompi_datatype_copy_content_same_ddt( const ompi_datatype_t* type, size_t count,
                                     char* pDestBuf, char* pSrcBuf )
{
    int32_t length, rc;
    ptrdiff_t extent;

    ompi_datatype_type_extent( type, &extent );
    while( 0 != count ) {
        length = INT_MAX;
        if( ((size_t)length) > count ) length = (int32_t)count;
        rc = opal_datatype_copy_content_same_ddt( &type->super, length,
                                                  pDestBuf, pSrcBuf );
        if( 0 != rc ) return rc;
        pDestBuf += ((ptrdiff_t)length) * extent;
        pSrcBuf  += ((ptrdiff_t)length) * extent;
        count -= (size_t)length;
    }
    return 0;
}

OMPI_DECLSPEC const ompi_datatype_t* ompi_datatype_match_size( int size, uint16_t datakind, uint16_t datalang );

/*
 *
 */
OMPI_DECLSPEC int32_t ompi_datatype_sndrcv( const void *sbuf, int32_t scount, const ompi_datatype_t* sdtype,
                                            void *rbuf, int32_t rcount, const ompi_datatype_t* rdtype);

/*
 *
 */
OMPI_DECLSPEC int32_t ompi_datatype_get_args( const ompi_datatype_t* pData, int32_t which,
                                              int32_t * ci, int32_t * i,
                                              int32_t * ca, ptrdiff_t* a,
                                              int32_t * cd, ompi_datatype_t** d, int32_t * type);
OMPI_DECLSPEC int32_t ompi_datatype_set_args( ompi_datatype_t* pData,
                                              int32_t ci, const int32_t ** i,
                                              int32_t ca, const ptrdiff_t* a,
                                              int32_t cd, ompi_datatype_t* const * d,int32_t type);
OMPI_DECLSPEC int32_t ompi_datatype_copy_args( const ompi_datatype_t* source_data,
                                               ompi_datatype_t* dest_data );
OMPI_DECLSPEC int32_t ompi_datatype_release_args( ompi_datatype_t* pData );
OMPI_DECLSPEC ompi_datatype_t* ompi_datatype_get_single_predefined_type_from_args( ompi_datatype_t* type );

/**
 * Return the amount of buffer necessary to pack the entire description of
 * the datatype. This value is computed once per datatype, and it is stored
 * in the datatype structure together with the packed description. As a side
 * note special care is taken to align the amount of data on void* for
 * architectures that require it.
 */
OMPI_DECLSPEC size_t ompi_datatype_pack_description_length( ompi_datatype_t* datatype );

/**
 * Return a pointer to the constant packed representation of the datatype.
 * The length can be retrieved with the ompi_datatype_pack_description_length,
 * and it is quarantee this is exactly the amount to be copied and not an
 * upper bound. Additionally, the packed representation is slightly optimized
 * compared with the get_content function, as all combiner_dup have been replaced
 * directly with the target type.
 */
OMPI_DECLSPEC int ompi_datatype_get_pack_description( ompi_datatype_t* datatype,
                                                      const void** packed_buffer );

/**
 * Extract a fully-fledged datatype from the packed representation. This datatype
 * is ready to be used in communications (it is automatically committed). However,
 * this datatype does not have an internal representation, so it might not be
 * repacked. Read the comment for the ompi_datatype_get_pack_description function
 * for extra information.
 */
struct ompi_proc_t;
OMPI_DECLSPEC ompi_datatype_t*
ompi_datatype_create_from_packed_description( void** packed_buffer,
                                              struct ompi_proc_t* remote_processor );

/**
 * Auxiliary function providing a pretty print for the packed data description.
 */
OMPI_DECLSPEC int32_t ompi_datatype_print_args( const ompi_datatype_t* pData );


/**
 * Get the number of basic elements of the datatype in ucount
 *
 * @returns OMPI_SUCCESS if the count is valid
 * @returns OMPI_ERR_VALUE_OUT_OF_BOUNDS if no
 */
OMPI_DECLSPEC int ompi_datatype_get_elements (ompi_datatype_t *datatype, size_t ucount,
                                              size_t *count);

#if OPAL_ENABLE_DEBUG
/*
 * Set a breakpoint to this function in your favorite debugger
 * to make it stop on all pack and unpack errors.
 */
OMPI_DECLSPEC int ompi_datatype_safeguard_pointer_debug_breakpoint( const void* actual_ptr, int length,
                                                                    const void* initial_ptr,
                                                                    const ompi_datatype_t* pData,
                                                                    int count );
#endif  /* OPAL_ENABLE_DEBUG */

OMPI_DECLSPEC int ompi_datatype_pack_external( const char datarep[], const void *inbuf, int incount,
                                               ompi_datatype_t *datatype, void *outbuf,
                                               MPI_Aint outsize, MPI_Aint *position);

OMPI_DECLSPEC int ompi_datatype_unpack_external( const char datarep[], const void *inbuf, MPI_Aint insize,
                                                 MPI_Aint *position, void *outbuf, int outcount,
                                                 ompi_datatype_t *datatype);

OMPI_DECLSPEC int ompi_datatype_pack_external_size( const char datarep[], int incount,
                                                    ompi_datatype_t *datatype, MPI_Aint *size);

#define OMPI_DATATYPE_RETAIN(ddt)                                       \
    {                                                                   \
        if( !ompi_datatype_is_predefined((ddt)) ) {                     \
            OBJ_RETAIN((ddt));                                          \
            OPAL_OUTPUT_VERBOSE((0, 100, "Datatype %p [%s] refcount %d in file %s:%d\n",     \
                                (void*)(ddt), (ddt)->name, (ddt)->super.super.obj_reference_count, \
                                __FILE__, __LINE__));                   \
        }                                                               \
    }

#define OMPI_DATATYPE_RELEASE(ddt)                                      \
    {                                                                   \
        if( !ompi_datatype_is_predefined((ddt)) ) {                     \
            OPAL_OUTPUT_VERBOSE((0, 100, "Datatype %p [%s] refcount %d in file %s:%d\n",     \
                                (void*)(ddt), (ddt)->name, (ddt)->super.super.obj_reference_count, \
                                __func__, __LINE__));                   \
            OBJ_RELEASE((ddt));                                         \
        }                                                               \
    }

END_C_DECLS
#endif  /* OMPI_DATATYPE_H_HAS_BEEN_INCLUDED */
