/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2016 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2006 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2013-2017 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2015-2019 Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include <stddef.h>

#include "opal/align.h"
#include "opal/types.h"
#include "opal/util/arch.h"
#include "opal/datatype/opal_datatype.h"
#include "opal/datatype/opal_datatype_internal.h"
#include "ompi/constants.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/datatype/ompi_datatype_internal.h"
#include "ompi/proc/proc.h"

static inline int
__ompi_datatype_pack_description( ompi_datatype_t* datatype,
                                  void** packed_buffer, int* next_index );
static ompi_datatype_t*
__ompi_datatype_create_from_args( int32_t* i, ptrdiff_t * a,
                                  ompi_datatype_t** d, int32_t type );

typedef struct __dt_args {
    int32_t            ref_count;
    int32_t            create_type;
    size_t             total_pack_size;
    int32_t            ci;
    int32_t            ca;
    int32_t            cd;
    int*               i;
    ptrdiff_t* a;
    ompi_datatype_t**  d;
} ompi_datatype_args_t;

/**
 * Some architectures really don't like having unaligned
 * accesses.  We'll be int aligned, because any sane system will
 * require that.  But we might not be long aligned, and some
 * architectures will complain if a long is accessed on int
 * alignment (but not long alignment).  On those architectures,
 * copy the buffer into an aligned buffer first.
 */
#if OPAL_ALIGN_WORD_SIZE_INTEGERS
#define OMPI_DATATYPE_ALIGN_PTR(PTR, TYPE) \
    (PTR) = OPAL_ALIGN_PTR((PTR), sizeof(ptrdiff_t), TYPE)
#else
#define OMPI_DATATYPE_ALIGN_PTR(PTR, TYPE)
#endif  /* OPAL_ALIGN_WORD_SIZE_INTEGERS */

/**
 * Some architectures require 64 bits pointers (to pointers) to
 * be 64 bits aligned. As in the ompi_datatype_args_t structure we have
 * 2 such array of pointers and one to an array of ints, if we start by
 * setting the 64 bits aligned one we will not have any trouble. Problem
 * originally reported on SPARC 64.
 */
#define ALLOC_ARGS(PDATA, IC, AC, DC)                                   \
    do {                                                                \
        int length = sizeof(ompi_datatype_args_t) + (IC) * sizeof(int) + \
            (AC) * sizeof(ptrdiff_t) + (DC) * sizeof(MPI_Datatype); \
        char* buf = (char*)malloc( length );                            \
        ompi_datatype_args_t* pArgs = (ompi_datatype_args_t*)buf;       \
        pArgs->ci = (IC);                                               \
        pArgs->ca = (AC);                                               \
        pArgs->cd = (DC);                                               \
        buf += sizeof(ompi_datatype_args_t);                            \
        if( pArgs->ca == 0 ) pArgs->a = NULL;                           \
        else {                                                          \
            pArgs->a = (ptrdiff_t*)buf;                         \
            buf += pArgs->ca * sizeof(ptrdiff_t);               \
        }                                                               \
        if( pArgs->cd == 0 ) pArgs->d = NULL;                           \
        else {                                                          \
            pArgs->d = (ompi_datatype_t**)buf;                          \
            buf += pArgs->cd * sizeof(MPI_Datatype);                    \
        }                                                               \
        if( pArgs->ci == 0 ) pArgs->i = NULL;                           \
        else pArgs->i = (int*)buf;                                      \
        pArgs->ref_count = 1;                                           \
        pArgs->total_pack_size = (4 + (IC) + (DC)) * sizeof(int) +      \
            (AC) * sizeof(ptrdiff_t);                                   \
        (PDATA)->args = (void*)pArgs;                                   \
        (PDATA)->packed_description = NULL;                             \
    } while(0)


int32_t ompi_datatype_set_args( ompi_datatype_t* pData,
                                int32_t ci, const int32_t** i,
                                int32_t ca, const ptrdiff_t* a,
                                int32_t cd, ompi_datatype_t* const * d, int32_t type)
{
    int pos;
    ompi_datatype_args_t* pArgs;

    assert( NULL == pData->args );
    ALLOC_ARGS( pData, ci, ca, cd );

    pArgs = (ompi_datatype_args_t*)pData->args;
    pArgs->create_type = type;

    switch(type) {

    case MPI_COMBINER_DUP:
        pArgs->total_pack_size = 0;  /* store no extra data */
        break;

    case MPI_COMBINER_CONTIGUOUS:
        pArgs->i[0] = i[0][0];
        break;

    case MPI_COMBINER_VECTOR:
        pArgs->i[0] = i[0][0];
        pArgs->i[1] = i[1][0];
        pArgs->i[2] = i[2][0];
        break;

    case MPI_COMBINER_HVECTOR_INTEGER:
    case MPI_COMBINER_HVECTOR:
        pArgs->i[0] = i[0][0];
        pArgs->i[1] = i[1][0];
        break;

    case MPI_COMBINER_INDEXED:
        pos = 1;
        pArgs->i[0] = i[0][0];
        memcpy( pArgs->i + pos, i[1], i[0][0] * sizeof(int) );
        pos += i[0][0];
        memcpy( pArgs->i + pos, i[2], i[0][0] * sizeof(int) );
        break;

    case MPI_COMBINER_HINDEXED_INTEGER:
    case MPI_COMBINER_HINDEXED:
        pArgs->i[0] = i[0][0];
        memcpy( pArgs->i + 1, i[1], i[0][0] * sizeof(int) );
        break;

    case MPI_COMBINER_INDEXED_BLOCK:
        pArgs->i[0] = i[0][0];
        pArgs->i[1] = i[1][0];
        memcpy( pArgs->i + 2, i[2], i[0][0] * sizeof(int) );
        break;

    case MPI_COMBINER_STRUCT_INTEGER:
    case MPI_COMBINER_STRUCT:
        pArgs->i[0] = i[0][0];
        memcpy( pArgs->i + 1, i[1], i[0][0] * sizeof(int) );
        break;

    case MPI_COMBINER_SUBARRAY:
        pos = 1;
        pArgs->i[0] = i[0][0];
        memcpy( pArgs->i + pos, i[1], pArgs->i[0] * sizeof(int) );
        pos += pArgs->i[0];
        memcpy( pArgs->i + pos, i[2], pArgs->i[0] * sizeof(int) );
        pos += pArgs->i[0];
        memcpy( pArgs->i + pos, i[3], pArgs->i[0] * sizeof(int) );
        pos += pArgs->i[0];
        pArgs->i[pos] = i[4][0];
        break;

    case MPI_COMBINER_DARRAY:
        pos = 3;
        pArgs->i[0] = i[0][0];
        pArgs->i[1] = i[1][0];
        pArgs->i[2] = i[2][0];

        memcpy( pArgs->i + pos, i[3], i[2][0] * sizeof(int) );
        pos += i[2][0];
        memcpy( pArgs->i + pos, i[4], i[2][0] * sizeof(int) );
        pos += i[2][0];
        memcpy( pArgs->i + pos, i[5], i[2][0] * sizeof(int) );
        pos += i[2][0];
        memcpy( pArgs->i + pos, i[6], i[2][0] * sizeof(int) );
        pos += i[2][0];
        pArgs->i[pos] = i[7][0];
        break;

    case MPI_COMBINER_F90_REAL:
    case MPI_COMBINER_F90_COMPLEX:
        pArgs->i[0] = i[0][0];
        pArgs->i[1] = i[1][0];
        break;

    case MPI_COMBINER_F90_INTEGER:
        pArgs->i[0] = i[0][0];
        break;

    case MPI_COMBINER_RESIZED:
        break;

    case MPI_COMBINER_HINDEXED_BLOCK:
        pArgs->i[0] = i[0][0];
        pArgs->i[1] = i[1][0];
        break;

    default:
        break;
    }

    /* copy the array of MPI_Aint, aka ptrdiff_t */
    if( pArgs->a != NULL )
        memcpy( pArgs->a, a, ca * sizeof(ptrdiff_t) );

    for( pos = 0; pos < cd; pos++ ) {
        pArgs->d[pos] = d[pos];
        if( !(ompi_datatype_is_predefined(d[pos])) ) {
            /* We handle a user defined datatype. We should make sure that the
             * user will not have the oportunity to destroy it before all derived
             * datatypes are destroyed. As we keep pointers to every datatype
             * (for MPI_Type_get_content and MPI_Type_get_envelope) we have to make
             * sure that those datatype will be available if the user ask for them.
             * However, there is no easy way to free them in this case ...
             */
            OBJ_RETAIN( d[pos] );
            pArgs->total_pack_size += ((ompi_datatype_args_t*)d[pos]->args)->total_pack_size;
        } else {
            pArgs->total_pack_size += sizeof(int); /* _NAMED */
        }
        pArgs->total_pack_size += sizeof(int);  /* each data has an ID */
    }

    return OMPI_SUCCESS;
}


int32_t ompi_datatype_print_args( const ompi_datatype_t* pData )
{
    int32_t i;
    ompi_datatype_args_t* pArgs = (ompi_datatype_args_t*)pData->args;

    if( ompi_datatype_is_predefined(pData) ) {
        /* nothing to do for predefined data-types */
        return OMPI_SUCCESS;
    }

    if( pArgs == NULL ) return MPI_ERR_INTERN;

    printf( "type %d count ints %d count disp %d count datatype %d\n",
            pArgs->create_type, pArgs->ci, pArgs->ca, pArgs->cd );
    if( pArgs->i != NULL ) {
        printf( "ints:     " );
        for( i = 0; i < pArgs->ci; i++ ) {
            printf( "%d ", pArgs->i[i] );
        }
        printf( "\n" );
    }
    if( pArgs->a != NULL ) {
        printf( "MPI_Aint: " );
        for( i = 0; i < pArgs->ca; i++ ) {
            printf( "%ld ", (long)pArgs->a[i] );
        }
        printf( "\n" );
    }
    if( pArgs->d != NULL ) {
        int count = 1;
        ompi_datatype_t *temp, *old;

        printf( "types:    " );
        old = pArgs->d[0];
        for( i = 1; i < pArgs->cd; i++ ) {
            temp = pArgs->d[i];
            if( old == temp ) {
                count++;
                continue;
            }
            if( count <= 1 ) {
                if( ompi_datatype_is_predefined(old) )
                    printf( "%s ", old->name );
                else
                    printf( "%p ", (void*)old );
            } else {
                if( ompi_datatype_is_predefined(old) )
                    printf( "(%d * %s) ", count, old->name );
                else
                    printf( "(%d * %p) ", count, (void*)old );
            }
            count = 1;
            old = temp;
        }
        if( count <= 1 ) {
            if( ompi_datatype_is_predefined(old) )
                printf( "%s ", old->name );
            else
                printf( "%p ", (void*)old );
        } else {
            if( ompi_datatype_is_predefined(old) )
                printf( "(%d * %s) ", count, old->name );
            else
                printf( "(%d * %p) ", count, (void*)old );
        }
        printf( "\n" );
    }
    return OMPI_SUCCESS;
}


int32_t ompi_datatype_get_args( const ompi_datatype_t* pData, int32_t which,
                                int32_t* ci, int32_t* i,
                                int32_t* ca, ptrdiff_t* a,
                                int32_t* cd, ompi_datatype_t** d, int32_t* type)
{
    ompi_datatype_args_t* pArgs = (ompi_datatype_args_t*)pData->args;

    if( NULL == pArgs ) {  /* only for predefined datatypes */
        if( ompi_datatype_is_predefined(pData) ) {
            switch(which){
            case 0:
                *ci = 0;
                *ca = 0;
                *cd = 0;
                *type = MPI_COMBINER_NAMED;
                break;
            default:
                return MPI_ERR_INTERN;
            }
            return OMPI_SUCCESS;
        }
        return MPI_ERR_INTERN;
    }

    switch(which){
    case 0:     /* GET THE LENGTHS */
        *ci = pArgs->ci;
        *ca = pArgs->ca;
        *cd = pArgs->cd;
        *type = pArgs->create_type;
        break;
    case 1:     /* GET THE ARGUMENTS */
        if(*ci < pArgs->ci || *ca < pArgs->ca || *cd < pArgs->cd) {
            return MPI_ERR_ARG;
        }
        if( (NULL != i) && (NULL != pArgs->i) ) {
            memcpy( i, pArgs->i, pArgs->ci * sizeof(int) );
        }
        if( (NULL != a) && (NULL != pArgs->a) ) {
            memcpy( a, pArgs->a, pArgs->ca * sizeof(ptrdiff_t) );
        }
        if( (NULL != d) && (NULL != pArgs->d) ) {
            memcpy( d, pArgs->d, pArgs->cd * sizeof(MPI_Datatype) );
        }
        break;
    default:
        return MPI_ERR_INTERN;
    }
    return OMPI_SUCCESS;
}


int32_t ompi_datatype_copy_args( const ompi_datatype_t* source_data,
                                 ompi_datatype_t* dest_data )
{
    ompi_datatype_args_t* pArgs = (ompi_datatype_args_t*)source_data->args;

    /* Increase the reference count of the datatype enveloppe. This
     * prevent us from making extra copies for the enveloppe (which is mostly
     * a read only memory).
     */
    if( NULL != pArgs ) {
        OPAL_THREAD_ADD_FETCH32(&pArgs->ref_count, 1);
        dest_data->args = pArgs;
    }
    return OMPI_SUCCESS;
}


/* In the dt_add function we increase the reference count for all datatypes
 * (except for the predefined ones) that get added to another datatype. This
 * insure that they cannot get released until all the references to them
 * get removed.
 */
int32_t ompi_datatype_release_args( ompi_datatype_t* pData )
{
    int i;
    ompi_datatype_args_t* pArgs = (ompi_datatype_args_t*)pData->args;

    assert( 0 < pArgs->ref_count );
    OPAL_THREAD_ADD_FETCH32(&pArgs->ref_count, -1);
    if( 0 == pArgs->ref_count ) {
        /* There are some duplicated datatypes around that have a pointer to this
         * args. We will release them only when the last datatype will dissapear.
         */
        for( i = 0; i < pArgs->cd; i++ ) {
            if( !(ompi_datatype_is_predefined(pArgs->d[i])) ) {
                OBJ_RELEASE( pArgs->d[i] );
            }
        }
        free( pData->args );
    }
    pData->args = NULL;

    return OMPI_SUCCESS;
}


static inline int __ompi_datatype_pack_description( ompi_datatype_t* datatype,
                                                    void** packed_buffer, int* next_index )
{
    int i, *position = (int*)*packed_buffer;
    ompi_datatype_args_t* args = (ompi_datatype_args_t*)datatype->args;
    char* next_packed = (char*)*packed_buffer;

    if( ompi_datatype_is_predefined(datatype) ) {
        position[0] = MPI_COMBINER_NAMED;
        position[1] = datatype->id;   /* On the OMPI - layer, copy the ompi_datatype.id */
        next_packed += (2 * sizeof(int));
        *packed_buffer = next_packed;
        return OMPI_SUCCESS;
    }
    /* For duplicated datatype we don't have to store all the information */
    if( MPI_COMBINER_DUP == args->create_type ) {
        ompi_datatype_t* temp_data = args->d[0];
        return __ompi_datatype_pack_description(temp_data,
                                                packed_buffer,
                                                next_index );
    }
    position[0] = args->create_type;
    position[1] = args->ci;
    position[2] = args->ca;
    position[3] = args->cd;
    next_packed += (4 * sizeof(int));
    /* Spoiler: We will access the data in this storage structure, and thus we
     * need to align it to the expected boundaries (special thanks to Sparc64).
     * The simplest way is to ensure that prior to each type that must be 64
     * bits aligned, we have a pointer that is 64 bits aligned. That will minimize
     * the memory requirements in all cases where no displacements are stored.
     */
    if( 0 < args->ca ) {
        /* description of the displacements must be 64 bits aligned */
        OMPI_DATATYPE_ALIGN_PTR(next_packed, char*);

        memcpy( next_packed, args->a, sizeof(ptrdiff_t) * args->ca );
        next_packed += sizeof(ptrdiff_t) * args->ca;
    }
    position = (int*)next_packed;
    next_packed += sizeof(int) * args->cd;

    /* copy the aray of counts (32 bits aligned) */
    memcpy( next_packed, args->i, sizeof(int) * args->ci );
    next_packed += args->ci * sizeof(int);

    /* copy the rest of the data */
    for( i = 0; i < args->cd; i++ ) {
        ompi_datatype_t* temp_data = args->d[i];
        if( ompi_datatype_is_predefined(temp_data) ) {
            position[i] = temp_data->id;  /* On the OMPI - layer, copy the ompi_datatype.id */
        } else {
            position[i] = *next_index;
            (*next_index)++;
            __ompi_datatype_pack_description( temp_data,
                                              (void**)&next_packed,
                                              next_index );
        }
    }
    *packed_buffer = next_packed;
    return OMPI_SUCCESS;
}


int ompi_datatype_get_pack_description( ompi_datatype_t* datatype,
                                        const void** packed_buffer )
{
    ompi_datatype_args_t* args = (ompi_datatype_args_t*)datatype->args;
    int next_index = OMPI_DATATYPE_MAX_PREDEFINED;
    void *packed_description = datatype->packed_description;
    void* recursive_buffer;

    if (NULL == packed_description) {
        void *_tmp_ptr = NULL;
        if (opal_atomic_compare_exchange_strong_ptr (&datatype->packed_description, (void *) &_tmp_ptr, (void *) 1)) {
            if( ompi_datatype_is_predefined(datatype) ) {
                packed_description = malloc(2 * sizeof(int));
            } else if( NULL == args ) {
                return OMPI_ERROR;
            } else {
                packed_description = malloc(args->total_pack_size);
            }
            recursive_buffer = packed_description;
            __ompi_datatype_pack_description( datatype, &recursive_buffer, &next_index );

            if (!ompi_datatype_is_predefined(datatype)) {
                /* If the precomputed size is not large enough we're already in troubles, we
                 * have overwritten outside of the allocated buffer. Raise the alarm !
                 * If not reassess the size of the packed buffer necessary for holding the
                 * datatype description.
                 */
                assert(args->total_pack_size >= (uintptr_t)((char*)recursive_buffer - (char *) packed_description));
                args->total_pack_size = (uintptr_t)((char*)recursive_buffer - (char *) packed_description);
            }

            opal_atomic_wmb ();
            datatype->packed_description = packed_description;
        } else {
            /* another thread beat us to it */
            packed_description = datatype->packed_description;
        }
    }

    if ((void *) 1 == packed_description) {
        struct timespec interval = {.tv_sec = 0, .tv_nsec = 1000};

        /* wait until the packed description is updated */
        while ((void *) 1 == datatype->packed_description) {
            nanosleep (&interval, NULL);
        }

        packed_description = datatype->packed_description;
    }

    *packed_buffer = (const void *) packed_description;
    return OMPI_SUCCESS;
}

size_t ompi_datatype_pack_description_length( ompi_datatype_t* datatype )
{
    void *packed_description = datatype->packed_description;

    if( ompi_datatype_is_predefined(datatype) ) {
        return 2 * sizeof(int);
    }
    if( NULL == packed_description || (void *) 1 == packed_description) {
        const void* buf;
        int rc;

        rc = ompi_datatype_get_pack_description(datatype, &buf);
        if( OMPI_SUCCESS != rc ) {
            return 0;
        }
    }
    assert( NULL != (ompi_datatype_args_t*)datatype->args );
    assert( NULL != (ompi_datatype_args_t*)datatype->packed_description );
    return ((ompi_datatype_args_t*)datatype->args)->total_pack_size;
}

static ompi_datatype_t* __ompi_datatype_create_from_packed_description( void** packed_buffer,
                                                                        const struct ompi_proc_t* remote_processor )
{
    int* position;
    ompi_datatype_t* datatype = NULL;
    ompi_datatype_t** array_of_datatype;
    ptrdiff_t* array_of_disp;
    int* array_of_length;
    int number_of_length, number_of_disp, number_of_datatype, data_id;
    int create_type, i;
    char* next_buffer;

#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
    bool need_swap = false;

    if( (remote_processor->super.proc_arch ^ ompi_proc_local()->super.proc_arch) &
        OPAL_ARCH_ISBIGENDIAN ) {
        need_swap = true;
    }
#endif

    next_buffer = (char*)*packed_buffer;
    position = (int*)next_buffer;

    create_type = position[0];
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
    if (need_swap) {
        create_type = opal_swap_bytes4(create_type);
    }
#endif
    if( MPI_COMBINER_NAMED == create_type ) {
        /* there we have a simple predefined datatype */
        data_id = position[1];
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
        if (need_swap) {
            data_id = opal_swap_bytes4(data_id);
        }
#endif
        assert( data_id < OMPI_DATATYPE_MAX_PREDEFINED );
        *packed_buffer = position + 2;
        return (ompi_datatype_t*)ompi_datatype_basicDatatypes[data_id];
    }

    number_of_length   = position[1];
    number_of_disp     = position[2];
    number_of_datatype = position[3];
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
    if (need_swap) {
        number_of_length   = opal_swap_bytes4(number_of_length);
        number_of_disp     = opal_swap_bytes4(number_of_disp);
        number_of_datatype = opal_swap_bytes4(number_of_datatype);
    }
#endif
    array_of_datatype = (ompi_datatype_t**)malloc( sizeof(ompi_datatype_t*) *
                                                   number_of_datatype );
    next_buffer += (4 * sizeof(int));  /* move after the header */

    /* description of the displacements (if ANY !)  should always be aligned
       on MPI_Aint, aka ptrdiff_t */
    if (number_of_disp > 0) {
        OMPI_DATATYPE_ALIGN_PTR(next_buffer, char*);
    }

    array_of_disp   = (ptrdiff_t*)next_buffer;
    next_buffer    += number_of_disp * sizeof(ptrdiff_t);
    /* the other datatypes */
    position        = (int*)next_buffer;
    next_buffer    += number_of_datatype * sizeof(int);
    /* the array of lengths (32 bits aligned) */
    array_of_length = (int*)next_buffer;
    next_buffer    += (number_of_length * sizeof(int));

    for( i = 0; i < number_of_datatype; i++ ) {
        data_id = position[i];
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
        if (need_swap) {
            data_id = opal_swap_bytes4(data_id);
        }
#endif
        if( data_id < OMPI_DATATYPE_MAX_PREDEFINED ) {
            array_of_datatype[i] = (ompi_datatype_t*)ompi_datatype_basicDatatypes[data_id];
            continue;
        }
        array_of_datatype[i] =
            __ompi_datatype_create_from_packed_description( (void**)&next_buffer,
                                                            remote_processor );
        if( NULL == array_of_datatype[i] ) {
            /* don't cleanup more than required. We can now modify these
             * values as we already know we have failed to rebuild the
             * datatype.
             */
            array_of_datatype[i] = (ompi_datatype_t*)ompi_datatype_basicDatatypes[OPAL_DATATYPE_INT1]; /*XXX TODO */
            number_of_datatype = i;
            goto cleanup_and_exit;
        }
    }

#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
    if (need_swap) {
        for (i = 0 ; i < number_of_length ; ++i) {
            array_of_length[i] = opal_swap_bytes4(array_of_length[i]);
        }
        for (i = 0 ; i < number_of_disp ; ++i) {
#if SIZEOF_PTRDIFF_T == 4
            array_of_disp[i] = opal_swap_bytes4(array_of_disp[i]);
#elif SIZEOF_PTRDIFF_T == 8
            array_of_disp[i] = (MPI_Aint)opal_swap_bytes8(array_of_disp[i]);
#else
#error "Unknown size of ptrdiff_t"
#endif
        }
    }
#endif
    datatype = __ompi_datatype_create_from_args( array_of_length, array_of_disp,
                                                 array_of_datatype, create_type );
    *packed_buffer = next_buffer;
 cleanup_and_exit:
    for( i = 0; i < number_of_datatype; i++ ) {
        if( !(ompi_datatype_is_predefined(array_of_datatype[i])) ) {
            OBJ_RELEASE(array_of_datatype[i]);
        }
    }
    free( array_of_datatype );
    return datatype;
}

static ompi_datatype_t* __ompi_datatype_create_from_args( int32_t* i, MPI_Aint* a,
                                                          ompi_datatype_t** d, int32_t type )
{
    ompi_datatype_t* datatype = NULL;

    switch(type){
        /******************************************************************/
    case MPI_COMBINER_DUP:
        /* should we duplicate d[0]? */
        /* ompi_datatype_set_args( datatype, 0, NULL, 0, NULL, 1, d[0], MPI_COMBINER_DUP ); */
        assert(0);  /* shouldn't happen */
        break;
        /******************************************************************/
    case MPI_COMBINER_CONTIGUOUS:
        ompi_datatype_create_contiguous( i[0], d[0], &datatype );
        ompi_datatype_set_args( datatype, 1, (const int **) &i, 0, NULL, 1, d, MPI_COMBINER_CONTIGUOUS );
        break;
        /******************************************************************/
    case MPI_COMBINER_VECTOR:
        ompi_datatype_create_vector( i[0], i[1], i[2], d[0], &datatype );
        {
            const int* a_i[3] = {&i[0], &i[1], &i[2]};
            ompi_datatype_set_args( datatype, 3, a_i, 0, NULL, 1, d, MPI_COMBINER_VECTOR );
        }
        break;
        /******************************************************************/
    case MPI_COMBINER_HVECTOR_INTEGER:
    case MPI_COMBINER_HVECTOR:
        ompi_datatype_create_hvector( i[0], i[1], a[0], d[0], &datatype );
        {
            const int* a_i[2] = {&i[0], &i[1]};
            ompi_datatype_set_args( datatype, 2, a_i, 1, a, 1, d, MPI_COMBINER_HVECTOR );
        }
        break;
        /******************************************************************/
    case MPI_COMBINER_INDEXED:  /* TO CHECK */
        ompi_datatype_create_indexed( i[0], &(i[1]), &(i[1+i[0]]), d[0], &datatype );
        {
            const int* a_i[3] = {&i[0], &i[1], &(i[1+i[0]])};
            ompi_datatype_set_args( datatype, 2 * i[0] + 1, a_i, 0, NULL, 1, d, MPI_COMBINER_INDEXED );
        }
        break;
        /******************************************************************/
    case MPI_COMBINER_HINDEXED_INTEGER:
    case MPI_COMBINER_HINDEXED:
        ompi_datatype_create_hindexed( i[0], &(i[1]), a, d[0], &datatype );
        {
            const int* a_i[2] = {&i[0], &i[1]};
            ompi_datatype_set_args( datatype, i[0] + 1, a_i, i[0], a, 1, d, MPI_COMBINER_HINDEXED );
        }
        break;
        /******************************************************************/
    case MPI_COMBINER_INDEXED_BLOCK:
        ompi_datatype_create_indexed_block( i[0], i[1], &(i[2]), d[0], &datatype );
        {
            const int* a_i[3] = {&i[0], &i[1], &i[2]};
            ompi_datatype_set_args( datatype, i[0] + 2, a_i, 0, NULL, 1, d, MPI_COMBINER_INDEXED_BLOCK );
        }
        break;
        /******************************************************************/
    case MPI_COMBINER_STRUCT_INTEGER:
    case MPI_COMBINER_STRUCT:
        ompi_datatype_create_struct( i[0], &(i[1]), a, d, &datatype );
        {
            const int* a_i[2] = {&i[0], &i[1]};
            ompi_datatype_set_args( datatype, i[0] + 1, a_i, i[0], a, i[0], d, MPI_COMBINER_STRUCT );
        }
        break;
        /******************************************************************/
    case MPI_COMBINER_SUBARRAY:
        ompi_datatype_create_subarray( i[0], &i[1 + 0 * i[0]], &i[1 + 1 * i[0]],
                                       &i[1 + 2 * i[0]], i[1 + 3 * i[0]],
                                       d[0], &datatype );
        {
            const int* a_i[5] = {&i[0], &i[1 + 0 * i[0]], &i[1 + 1 * i[0]], &i[1 + 2 * i[0]], &i[1 + 3 * i[0]]};
            ompi_datatype_set_args( datatype, 3 * i[0] + 2, a_i, 0, NULL, 1, d, MPI_COMBINER_SUBARRAY);
        }
        break;
        /******************************************************************/
    case MPI_COMBINER_DARRAY:
        ompi_datatype_create_darray( i[0] /* size */, i[1] /* rank */, i[2] /* ndims */,
                                     &i[3 + 0 * i[2]], &i[3 + 1 * i[2]],
                                     &i[3 + 2 * i[2]], &i[3 + 3 * i[2]],
                                     i[3 + 4 * i[2]], d[0], &datatype );
        {
            const int* a_i[8] = {&i[0], &i[1], &i[2], &i[3 + 0 * i[2]], &i[3 + 1 * i[2]], &i[3 + 2 * i[2]],
                                 &i[3 + 3 * i[2]], &i[3 + 4 * i[2]]};
            ompi_datatype_set_args( datatype, 4 * i[2] + 4, a_i, 0, NULL, 1, d, MPI_COMBINER_DARRAY);
        }
        break;
        /******************************************************************/
    case MPI_COMBINER_F90_REAL:
    case MPI_COMBINER_F90_COMPLEX:
        /*pArgs->i[0] = i[0][0];
          pArgs->i[1] = i[1][0];
        */
        break;
        /******************************************************************/
    case MPI_COMBINER_F90_INTEGER:
        /*pArgs->i[0] = i[0][0];*/
        break;
        /******************************************************************/
    case MPI_COMBINER_RESIZED:
        ompi_datatype_create_resized(d[0], a[0], a[1], &datatype);
        ompi_datatype_set_args( datatype, 0, NULL, 2, a, 1, d, MPI_COMBINER_RESIZED );
        break;
        /******************************************************************/
    case MPI_COMBINER_HINDEXED_BLOCK:
        ompi_datatype_create_hindexed_block( i[0], i[1], a, d[0], &datatype );
        {
            const int* a_i[2] = {&i[0], &i[1]};
            ompi_datatype_set_args( datatype, 2, a_i, i[0], a, 1, d, MPI_COMBINER_HINDEXED_BLOCK );
        }
        break;
        /******************************************************************/
     default:
        break;
    }

    return datatype;
}

ompi_datatype_t* ompi_datatype_create_from_packed_description( void** packed_buffer,
                                                               struct ompi_proc_t* remote_processor )
{
    ompi_datatype_t* datatype;

    datatype = __ompi_datatype_create_from_packed_description( packed_buffer,
                                                               remote_processor );
    if( NULL == datatype ) {
        return NULL;
    }
    ompi_datatype_commit( &datatype );
    return datatype;
}

/**
 * Parse the datatype description from the args and find if the
 * datatype is created from a single predefined type. If yes,
 * return the type, otherwise return NULL.
 */
ompi_datatype_t* ompi_datatype_get_single_predefined_type_from_args( ompi_datatype_t* type )
{
    ompi_datatype_t *predef = NULL, *current_type, *current_predef;
    ompi_datatype_args_t* args = (ompi_datatype_args_t*)type->args;
    int i;

    if( ompi_datatype_is_predefined(type) )
        return type;

    for( i = 0; i < args->cd; i++ ) {
        current_type = args->d[i];
        if( ompi_datatype_is_predefined(current_type) ) {
            current_predef = current_type;
        } else {
            current_predef = ompi_datatype_get_single_predefined_type_from_args(current_type);
            if( NULL == current_predef ) { /* No single predefined datatype */
                return NULL;
            }
        }
#if OMPI_ENABLE_MPI1_COMPAT
        if (current_predef != MPI_LB && current_predef != MPI_UB) {
#endif
            if( NULL == predef ) {  /* This is the first iteration */
                predef = current_predef;
            } else {
                /**
                 *  What exactly should we consider as identical types?
                 *  If they are the same MPI level type, or if they map
                 *  to the same OPAL datatype? In other words, MPI_FLOAT
                 *  and MPI_REAL4 are they identical?
                 */
                if( predef != current_predef ) {
                    return NULL;
                }
            }
#if OMPI_ENABLE_MPI1_COMPAT
        }
#endif
    }
    return predef;
}
