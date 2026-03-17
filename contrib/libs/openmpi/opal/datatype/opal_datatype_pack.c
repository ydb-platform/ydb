/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2006 The Trustees of Indiana University and Indiana
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
 * Copyright (c) 2013      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2017-2018 Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include <stddef.h>

#include "opal/datatype/opal_convertor_internal.h"
#include "opal/datatype/opal_datatype_internal.h"

#if OPAL_ENABLE_DEBUG
#include "opal/util/output.h"

#define DO_DEBUG(INST)  if( opal_pack_debug ) { INST }
#else
#define DO_DEBUG(INST)
#endif  /* OPAL_ENABLE_DEBUG */

#include "opal/datatype/opal_datatype_checksum.h"
#include "opal/datatype/opal_datatype_pack.h"
#include "opal/datatype/opal_datatype_prototypes.h"

#if defined(CHECKSUM)
#define opal_pack_homogeneous_contig_function           opal_pack_homogeneous_contig_checksum
#define opal_pack_homogeneous_contig_with_gaps_function opal_pack_homogeneous_contig_with_gaps_checksum
#define opal_generic_simple_pack_function               opal_generic_simple_pack_checksum
#define opal_pack_general_function                      opal_pack_general_checksum
#else
#define opal_pack_homogeneous_contig_function           opal_pack_homogeneous_contig
#define opal_pack_homogeneous_contig_with_gaps_function opal_pack_homogeneous_contig_with_gaps
#define opal_generic_simple_pack_function               opal_generic_simple_pack
#define opal_pack_general_function                      opal_pack_general
#endif  /* defined(CHECKSUM) */


#define IOVEC_MEM_LIMIT 8192

/* the contig versions does not use the stack. They can easily retrieve
 * the status with just the informations from pConvertor->bConverted.
 */
int32_t
opal_pack_homogeneous_contig_function( opal_convertor_t* pConv,
                                       struct iovec* iov,
                                       uint32_t* out_size,
                                       size_t* max_data )
{
    dt_stack_t* pStack = pConv->pStack;
    unsigned char *source_base = NULL;
    uint32_t iov_count;
    size_t length = pConv->local_size - pConv->bConverted, initial_amount = pConv->bConverted;
    ptrdiff_t initial_displ = pConv->use_desc->desc[pConv->use_desc->used].end_loop.first_elem_disp;

    source_base = (pConv->pBaseBuf + initial_displ + pStack[0].disp + pStack[1].disp);

    /* There are some optimizations that can be done if the upper level
     * does not provide a buffer.
     */
    for( iov_count = 0; iov_count < (*out_size); iov_count++ ) {
        if( 0 == length ) break;
        if( (size_t)iov[iov_count].iov_len > length )
            iov[iov_count].iov_len = length;
        if( iov[iov_count].iov_base == NULL ) {
            iov[iov_count].iov_base = (IOVBASE_TYPE *) source_base;
            COMPUTE_CSUM( iov[iov_count].iov_base, iov[iov_count].iov_len, pConv );
        } else {
            /* contiguous data just memcpy the smallest data in the user buffer */
            OPAL_DATATYPE_SAFEGUARD_POINTER( source_base, iov[iov_count].iov_len,
                                        pConv->pBaseBuf, pConv->pDesc, pConv->count );
            MEMCPY_CSUM( iov[iov_count].iov_base, source_base, iov[iov_count].iov_len, pConv );
        }
        length -= iov[iov_count].iov_len;
        pConv->bConverted += iov[iov_count].iov_len;
        pStack[0].disp += iov[iov_count].iov_len;
        source_base += iov[iov_count].iov_len;
    }

    /* update the return value */
    *max_data = pConv->bConverted - initial_amount;
    *out_size = iov_count;
    if( pConv->bConverted == pConv->local_size ) {
        pConv->flags |= CONVERTOR_COMPLETED;
        return 1;
    }
    return 0;
}


int32_t
opal_pack_homogeneous_contig_with_gaps_function( opal_convertor_t* pConv,
                                                 struct iovec* iov,
                                                 uint32_t* out_size,
                                                 size_t* max_data )
{
    const opal_datatype_t* pData = pConv->pDesc;
    dt_stack_t* stack = pConv->pStack;
    unsigned char *user_memory, *packed_buffer;
    uint32_t iov_count, index;
    size_t i;
    size_t bConverted, remaining, length, initial_bytes_converted = pConv->bConverted;
    ptrdiff_t extent= pData->ub - pData->lb;
    ptrdiff_t initial_displ = pConv->use_desc->desc[pConv->use_desc->used].end_loop.first_elem_disp;

    assert( (pData->flags & OPAL_DATATYPE_FLAG_CONTIGUOUS) && ((ptrdiff_t)pData->size != extent) );
    DO_DEBUG( opal_output( 0, "pack_homogeneous_contig( pBaseBuf %p, iov_count %d )\n",
                           (void*)pConv->pBaseBuf, *out_size ); );
    if( stack[1].type != opal_datatype_uint1.id ) {
        stack[1].count *= opal_datatype_basicDatatypes[stack[1].type]->size;
        stack[1].type = opal_datatype_uint1.id;
    }

    /* There are some optimizations that can be done if the upper level
     * does not provide a buffer.
     */
    for( iov_count = 0; iov_count < (*out_size); iov_count++ ) {
        /* Limit the amount of packed data to the data left over on this convertor */
        remaining = pConv->local_size - pConv->bConverted;
        if( 0 == remaining ) break;  /* we're done this time */
        if( remaining > iov[iov_count].iov_len )
            remaining = iov[iov_count].iov_len;
        packed_buffer = (unsigned char *)iov[iov_count].iov_base;
        bConverted = remaining; /* how much will get unpacked this time */
        user_memory = pConv->pBaseBuf + initial_displ + stack[0].disp + stack[1].disp;
        i = pConv->count - stack[0].count;  /* how many we already packed */
        assert(i == (pConv->bConverted / pData->size));

        if( packed_buffer == NULL ) {
            /* special case for small data. We avoid allocating memory if we
             * can fill the iovec directly with the address of the remaining
             * data.
             */
            if( stack->count < (size_t)((*out_size) - iov_count) ) {
                stack[1].count = pData->size - (pConv->bConverted % pData->size);
                for( index = iov_count; i < pConv->count; i++, index++ ) {
                    iov[index].iov_base = (IOVBASE_TYPE *) user_memory;
                    iov[index].iov_len = stack[1].count;
                    stack[0].disp += extent;
                    pConv->bConverted += stack[1].count;
                    stack[1].disp  = 0;  /* reset it for the next round */
                    stack[1].count = pData->size;
                    user_memory = pConv->pBaseBuf + initial_displ + stack[0].disp;
                    COMPUTE_CSUM( iov[index].iov_base, iov[index].iov_len, pConv );
                }
                *out_size = iov_count + index;
                *max_data = (pConv->bConverted - initial_bytes_converted);
                pConv->flags |= CONVERTOR_COMPLETED;
                return 1;  /* we're done */
            }
            /* now special case for big contiguous data with gaps around */
            if( pData->size >= IOVEC_MEM_LIMIT ) {
                /* as we dont have to copy any data, we can simply fill the iovecs
                 * with data from the user data description.
                 */
                for( index = iov_count; (i < pConv->count) && (index < (*out_size));
                     i++, index++ ) {
                    if( remaining < pData->size ) {
                        iov[index].iov_base = (IOVBASE_TYPE *) user_memory;
                        iov[index].iov_len = remaining;
                        remaining = 0;
                        COMPUTE_CSUM( iov[index].iov_base, iov[index].iov_len, pConv );
                        break;
                    } else {
                        iov[index].iov_base = (IOVBASE_TYPE *) user_memory;
                        iov[index].iov_len = pData->size;
                        user_memory += extent;
                        COMPUTE_CSUM( iov[index].iov_base, (size_t)iov[index].iov_len, pConv );
                    }
                    remaining -= iov[index].iov_len;
                    pConv->bConverted += iov[index].iov_len;
                }
                *out_size = index;
                *max_data = (pConv->bConverted - initial_bytes_converted);
                if( pConv->bConverted == pConv->local_size ) {
                    pConv->flags |= CONVERTOR_COMPLETED;
                    return 1;
                }
                return 0;
            }
        }

        {
            DO_DEBUG( opal_output( 0, "pack_homogeneous_contig( user_memory %p, packed_buffer %p length %lu\n",
                                   (void*)user_memory, (void*)packed_buffer, (unsigned long)remaining ); );

            length = (0 == pConv->stack_pos ? 0 : stack[1].count);  /* left over from the last pack */
            /* data left from last round and enough space in the buffer */
            if( (0 != length) && (length <= remaining)) {
                /* copy the partial left-over from the previous round */
                OPAL_DATATYPE_SAFEGUARD_POINTER( user_memory, length, pConv->pBaseBuf,
                                                 pData, pConv->count );
                DO_DEBUG( opal_output( 0, "2. pack dest %p src %p length %lu\n",
                                       (void*)user_memory, (void*)packed_buffer, (unsigned long)length ); );
                MEMCPY_CSUM( packed_buffer, user_memory, length, pConv );
                packed_buffer  += length;
                user_memory    += (extent - pData->size + length);
                remaining      -= length;
                stack[1].count -= length;
                if( 0 == stack[1].count) { /* one completed element */
                    stack[0].count--;
                    stack[0].disp += extent;
                    if( 0 != stack[0].count ) {  /* not yet done */
                        stack[1].count = pData->size;
                        stack[1].disp = 0;
                    }
                }
            }
            for( i = 0;  pData->size <= remaining; i++ ) {
                OPAL_DATATYPE_SAFEGUARD_POINTER( user_memory, pData->size, pConv->pBaseBuf,
                                                 pData, pConv->count );
                DO_DEBUG( opal_output( 0, "3. pack dest %p src %p length %lu\n",
                                       (void*)user_memory, (void*)packed_buffer, (unsigned long)pData->size ); );
                MEMCPY_CSUM( packed_buffer, user_memory, pData->size, pConv );
                packed_buffer += pData->size;
                user_memory   += extent;
                remaining   -= pData->size;
            }
            stack[0].count -= i;  /* the filled up and the entire types */
            stack[0].disp  += (i * extent);
            stack[1].disp  += remaining;
            /* Copy the last bits */
            if( 0 != remaining ) {
                OPAL_DATATYPE_SAFEGUARD_POINTER( user_memory, remaining, pConv->pBaseBuf,
                                                 pData, pConv->count );
                DO_DEBUG( opal_output( 0, "4. pack dest %p src %p length %lu\n",
                                       (void*)user_memory, (void*)packed_buffer, (unsigned long)remaining ); );
                MEMCPY_CSUM( packed_buffer, user_memory, remaining, pConv );
                user_memory += remaining;
                stack[1].count -= remaining;
            }
            if( 0 == stack[1].count ) {  /* prepare for the next element */
                stack[1].count = pData->size;
                stack[1].disp  = 0;
            }
        }
        pConv->bConverted += bConverted;
    }
    *out_size = iov_count;
    *max_data = (pConv->bConverted - initial_bytes_converted);
    if( pConv->bConverted == pConv->local_size ) {
        pConv->flags |= CONVERTOR_COMPLETED;
        return 1;
    }
    return 0;
}

/* The pack/unpack functions need a cleanup. I have to create a proper interface to access
 * all basic functionalities, hence using them as basic blocks for all conversion functions.
 *
 * But first let's make some global assumptions:
 * - a datatype (with the flag DT_DATA set) will have the contiguous flags set if and only if
 *   the data is really contiguous (extent equal with size)
 * - for the OPAL_DATATYPE_LOOP type the DT_CONTIGUOUS flag set means that the content of the loop is
 *   contiguous but with a gap in the begining or at the end.
 * - the DT_CONTIGUOUS flag for the type OPAL_DATATYPE_END_LOOP is meaningless.
 */
int32_t
opal_generic_simple_pack_function( opal_convertor_t* pConvertor,
                                   struct iovec* iov, uint32_t* out_size,
                                   size_t* max_data )
{
    dt_stack_t* pStack;       /* pointer to the position on the stack */
    uint32_t pos_desc;        /* actual position in the description of the derived datatype */
    size_t count_desc;        /* the number of items already done in the actual pos_desc */
    size_t total_packed = 0;  /* total amount packed this time */
    dt_elem_desc_t* description;
    dt_elem_desc_t* pElem;
    const opal_datatype_t *pData = pConvertor->pDesc;
    unsigned char *conv_ptr, *iov_ptr;
    size_t iov_len_local;
    uint32_t iov_count;

    DO_DEBUG( opal_output( 0, "opal_convertor_generic_simple_pack( %p:%p, {%p, %lu}, %d )\n",
                           (void*)pConvertor, (void*)pConvertor->pBaseBuf,
                           (void*)iov[0].iov_base, (unsigned long)iov[0].iov_len, *out_size ); );

    description = pConvertor->use_desc->desc;

    /* For the first step we have to add both displacement to the source. After in the
     * main while loop we will set back the conv_ptr to the correct value. This is
     * due to the fact that the convertor can stop in the middle of a data with a count
     */
    pStack = pConvertor->pStack + pConvertor->stack_pos;
    pos_desc   = pStack->index;
    conv_ptr   = pConvertor->pBaseBuf + pStack->disp;
    count_desc = pStack->count;
    pStack--;
    pConvertor->stack_pos--;
    pElem = &(description[pos_desc]);

    DO_DEBUG( opal_output( 0, "pack start pos_desc %d count_desc %" PRIsize_t " disp %ld\n"
                           "stack_pos %d pos_desc %d count_desc %" PRIsize_t " disp %ld\n",
                           pos_desc, count_desc, (long)(conv_ptr - pConvertor->pBaseBuf),
                           pConvertor->stack_pos, pStack->index, pStack->count, pStack->disp ); );

    for( iov_count = 0; iov_count < (*out_size); iov_count++ ) {
        iov_ptr = (unsigned char *) iov[iov_count].iov_base;
        iov_len_local = iov[iov_count].iov_len;
        while( 1 ) {
            while( pElem->elem.common.flags & OPAL_DATATYPE_FLAG_DATA ) {
                /* now here we have a basic datatype */
                PACK_PREDEFINED_DATATYPE( pConvertor, pElem, count_desc,
                                          conv_ptr, iov_ptr, iov_len_local );
                if( 0 == count_desc ) {  /* completed */
                    conv_ptr = pConvertor->pBaseBuf + pStack->disp;
                    pos_desc++;  /* advance to the next data */
                    UPDATE_INTERNAL_COUNTERS( description, pos_desc, pElem, count_desc );
                    continue;
                }
                goto complete_loop;
            }
            if( OPAL_DATATYPE_END_LOOP == pElem->elem.common.type ) { /* end of the current loop */
                DO_DEBUG( opal_output( 0, "pack end_loop count %" PRIsize_t " stack_pos %d"
                                       " pos_desc %d disp %ld space %lu\n",
                                       pStack->count, pConvertor->stack_pos,
                                       pos_desc, pStack->disp, (unsigned long)iov_len_local ); );
                if( --(pStack->count) == 0 ) { /* end of loop */
                    if( 0 == pConvertor->stack_pos ) {
                        /* we're done. Force the exit of the main for loop (around iovec) */
                        *out_size = iov_count;
                        goto complete_loop;
                    }
                    pConvertor->stack_pos--;  /* go one position up on the stack */
                    pStack--;
                    pos_desc++;  /* and move to the next element */
                } else {
                    pos_desc = pStack->index + 1;  /* jump back to the begining of the loop */
                    if( pStack->index == -1 ) {  /* If it's the datatype count loop */
                        pStack->disp += (pData->ub - pData->lb);  /* jump by the datatype extent */
                    } else {
                        assert( OPAL_DATATYPE_LOOP == description[pStack->index].loop.common.type );
                        pStack->disp += description[pStack->index].loop.extent;  /* jump by the loop extent */
                    }
                }
                conv_ptr = pConvertor->pBaseBuf + pStack->disp;
                UPDATE_INTERNAL_COUNTERS( description, pos_desc, pElem, count_desc );
                DO_DEBUG( opal_output( 0, "pack new_loop count %" PRIsize_t " stack_pos %d pos_desc %d count_desc %" PRIsize_t " disp %ld space %lu\n",
                                       pStack->count, pConvertor->stack_pos, pos_desc,
                                       count_desc, pStack->disp, (unsigned long)iov_len_local ); );
            }
            if( OPAL_DATATYPE_LOOP == pElem->elem.common.type ) {
                ptrdiff_t local_disp = (ptrdiff_t)conv_ptr;
                if( pElem->loop.common.flags & OPAL_DATATYPE_FLAG_CONTIGUOUS ) {
                    PACK_CONTIGUOUS_LOOP( pConvertor, pElem, count_desc,
                                          conv_ptr, iov_ptr, iov_len_local );
                    if( 0 == count_desc ) {  /* completed */
                        pos_desc += pElem->loop.items + 1;
                        goto update_loop_description;
                    }
                    /* Save the stack with the correct last_count value. */
                }
                local_disp = (ptrdiff_t)conv_ptr - local_disp;
                PUSH_STACK( pStack, pConvertor->stack_pos, pos_desc, OPAL_DATATYPE_LOOP, count_desc,
                            pStack->disp + local_disp);
                pos_desc++;
            update_loop_description:  /* update the current state */
                conv_ptr = pConvertor->pBaseBuf + pStack->disp;
                UPDATE_INTERNAL_COUNTERS( description, pos_desc, pElem, count_desc );
                DDT_DUMP_STACK( pConvertor->pStack, pConvertor->stack_pos, pElem, "advance loop" );
            }
        }
    complete_loop:
        iov[iov_count].iov_len -= iov_len_local;  /* update the amount of valid data */
        total_packed += iov[iov_count].iov_len;
    }
    *max_data = total_packed;
    pConvertor->bConverted += total_packed;  /* update the already converted bytes */
    *out_size = iov_count;
    if( pConvertor->bConverted == pConvertor->local_size ) {
        pConvertor->flags |= CONVERTOR_COMPLETED;
        return 1;
    }
    /* Save the global position for the next round */
    PUSH_STACK( pStack, pConvertor->stack_pos, pos_desc, pElem->elem.common.type, count_desc,
                conv_ptr - pConvertor->pBaseBuf );
    DO_DEBUG( opal_output( 0, "pack save stack stack_pos %d pos_desc %d count_desc %" PRIsize_t " disp %ld\n",
                           pConvertor->stack_pos, pStack->index, pStack->count, pStack->disp ); );
    return 0;
}

/*
 * Remember that the first item in the stack (ie. position 0) is the number
 * of times the datatype is involved in the operation (ie. the count argument
 * in the MPI_ call).
 */
/* Convert data from multiple input buffers (as received from the network layer)
 * to a contiguous output buffer with a predefined size.
 * return OPAL_SUCCESS if everything went OK and if there is still room before the complete
 *          conversion of the data (need additional call with others input buffers )
 *        1 if everything went fine and the data was completly converted
 *       -1 something wrong occurs.
 */

static inline void
pack_predefined_heterogeneous( opal_convertor_t* CONVERTOR,
                               const dt_elem_desc_t* ELEM,
                               size_t* COUNT,
                               unsigned char** SOURCE,
                               unsigned char** DESTINATION,
                               size_t* SPACE )
{
    const opal_convertor_master_t* master = (CONVERTOR)->master;
    const ddt_elem_desc_t* _elem = &((ELEM)->elem);
    unsigned char* _source = (*SOURCE) + _elem->disp;
    ptrdiff_t advance;
    size_t _count = *(COUNT);
    size_t _r_blength;

    _r_blength = master->remote_sizes[_elem->common.type];
    if( (_count * _r_blength) > *(SPACE) ) {
        _count = (*(SPACE) / _r_blength);
        if( 0 == _count ) return;  /* nothing to do */
    }

    OPAL_DATATYPE_SAFEGUARD_POINTER( _source, (_count * _elem->extent), (CONVERTOR)->pBaseBuf,
                                     (CONVERTOR)->pDesc, (CONVERTOR)->count );
    DO_DEBUG( opal_output( 0, "pack [l %s r %s] memcpy( %p, %p, %lu ) => space %lu\n",
                           ((ptrdiff_t)(opal_datatype_basicDatatypes[_elem->common.type]->size) == _elem->extent) ? "cont" : "----",
                           ((ptrdiff_t)_r_blength == _elem->extent) ? "cont" : "----",
                           (void*)*(DESTINATION), (void*)_source, (unsigned long)_r_blength,
                           (unsigned long)(*(SPACE)) ); );
    master->pFunctions[_elem->common.type]( CONVERTOR, _count,
                                            _source, *SPACE, _elem->extent,
                                            *DESTINATION, *SPACE, _r_blength,
                                            &advance );
    _r_blength     *= _count;  /* update the remote length to encompass all the elements */
    *(SOURCE)      += _count * _elem->extent;
    *(DESTINATION) += _r_blength;
    *(SPACE)       -= _r_blength;
    *(COUNT)       -= _count;
}

int32_t
opal_pack_general_function( opal_convertor_t* pConvertor,
                            struct iovec* iov, uint32_t* out_size,
                            size_t* max_data )
{
    dt_stack_t* pStack;       /* pointer to the position on the stack */
    uint32_t pos_desc;        /* actual position in the description of the derived datatype */
    size_t count_desc;      /* the number of items already done in the actual pos_desc */
    size_t total_packed = 0;  /* total amount packed this time */
    dt_elem_desc_t* description;
    dt_elem_desc_t* pElem;
    const opal_datatype_t *pData = pConvertor->pDesc;
    unsigned char *conv_ptr, *iov_ptr;
    size_t iov_len_local;
    uint32_t iov_count;

    DO_DEBUG( opal_output( 0, "opal_convertor_general_pack( %p:%p, {%p, %lu}, %d )\n",
                           (void*)pConvertor, (void*)pConvertor->pBaseBuf,
                           (void*)iov[0].iov_base, (unsigned long)iov[0].iov_len, *out_size ); );

    description = pConvertor->use_desc->desc;

    /* For the first step we have to add both displacement to the source. After in the
     * main while loop we will set back the conv_ptr to the correct value. This is
     * due to the fact that the convertor can stop in the middle of a data with a count
     */
    pStack = pConvertor->pStack + pConvertor->stack_pos;
    pos_desc   = pStack->index;
    conv_ptr   = pConvertor->pBaseBuf + pStack->disp;
    count_desc = pStack->count;
    pStack--;
    pConvertor->stack_pos--;
    pElem = &(description[pos_desc]);

    DO_DEBUG( opal_output( 0, "pack start pos_desc %d count_desc %" PRIsize_t " disp %ld\n"
                           "stack_pos %d pos_desc %d count_desc %" PRIsize_t " disp %ld\n",
                           pos_desc, count_desc, (long)(conv_ptr - pConvertor->pBaseBuf),
                           pConvertor->stack_pos, pStack->index, pStack->count, pStack->disp ); );

    for( iov_count = 0; iov_count < (*out_size); iov_count++ ) {
        iov_ptr = (unsigned char *) iov[iov_count].iov_base;
        iov_len_local = iov[iov_count].iov_len;
        while( 1 ) {
            while( pElem->elem.common.flags & OPAL_DATATYPE_FLAG_DATA ) {
                /* now here we have a basic datatype */
                DO_DEBUG( opal_output( 0, "pack (%p:%ld, %" PRIsize_t ", %ld) -> (%p, %ld) type %s\n",
                                       (void*)pConvertor->pBaseBuf, conv_ptr + pElem->elem.disp - pConvertor->pBaseBuf,
                                       count_desc, description[pos_desc].elem.extent,
                                       (void*)iov_ptr, iov_len_local,
                                       opal_datatype_basicDatatypes[pElem->elem.common.type]->name ); );

                pack_predefined_heterogeneous( pConvertor, pElem, &count_desc,
                                               &conv_ptr, &iov_ptr, &iov_len_local);
#if 0
                PACK_PREDEFINED_DATATYPE( pConvertor, pElem, count_desc,
                                          conv_ptr, iov_ptr, iov_len_local );
#endif
                if( 0 == count_desc ) {  /* completed */
                    conv_ptr = pConvertor->pBaseBuf + pStack->disp;
                    pos_desc++;  /* advance to the next data */
                    UPDATE_INTERNAL_COUNTERS( description, pos_desc, pElem, count_desc );
                    continue;
                }
                goto complete_loop;
            }
            if( OPAL_DATATYPE_END_LOOP == pElem->elem.common.type ) { /* end of the current loop */
                DO_DEBUG( opal_output( 0, "pack end_loop count %" PRIsize_t " stack_pos %d"
                                       " pos_desc %d disp %ld space %lu\n",
                                       pStack->count, pConvertor->stack_pos,
                                       pos_desc, pStack->disp, (unsigned long)iov_len_local ); );
                if( --(pStack->count) == 0 ) { /* end of loop */
                    if( 0 == pConvertor->stack_pos ) {
                        /* we lie about the size of the next element in order to
                         * make sure we exit the main loop.
                         */
                        *out_size = iov_count;
                        goto complete_loop;  /* completed */
                    }
                    pConvertor->stack_pos--;
                    pStack--;
                    pos_desc++;
                } else {
                    pos_desc = pStack->index + 1;
                    if( pStack->index == -1 ) {
                        pStack->disp += (pData->ub - pData->lb);
                    } else {
                        assert( OPAL_DATATYPE_LOOP == description[pStack->index].loop.common.type );
                        pStack->disp += description[pStack->index].loop.extent;
                    }
                }
                conv_ptr = pConvertor->pBaseBuf + pStack->disp;
                UPDATE_INTERNAL_COUNTERS( description, pos_desc, pElem, count_desc );
                DO_DEBUG( opal_output( 0, "pack new_loop count %" PRIsize_t " stack_pos %d pos_desc %d count_desc %" PRIsize_t " disp %ld space %lu\n",
                                       pStack->count, pConvertor->stack_pos, pos_desc,
                                       count_desc, pStack->disp, (unsigned long)iov_len_local ); );
            }
            if( OPAL_DATATYPE_LOOP == pElem->elem.common.type ) {
                ptrdiff_t local_disp = (ptrdiff_t)conv_ptr;
#if 0
                if( pElem->loop.common.flags & OPAL_DATATYPE_FLAG_CONTIGUOUS ) {
                    PACK_CONTIGUOUS_LOOP( pConvertor, pElem, count_desc,
                                          conv_ptr, iov_ptr, iov_len_local );
                    if( 0 == count_desc ) {  /* completed */
                        pos_desc += pElem->loop.items + 1;
                        goto update_loop_description;
                    }
                    /* Save the stack with the correct last_count value. */
                }
#endif  /* in a heterogeneous environment we can't handle the contiguous loops */
                local_disp = (ptrdiff_t)conv_ptr - local_disp;
                PUSH_STACK( pStack, pConvertor->stack_pos, pos_desc, OPAL_DATATYPE_LOOP, count_desc,
                            pStack->disp + local_disp);
                pos_desc++;
#if 0
            update_loop_description:  /* update the current state */
#endif  /* in a heterogeneous environment we can't handle the contiguous loops */
                conv_ptr = pConvertor->pBaseBuf + pStack->disp;
                UPDATE_INTERNAL_COUNTERS( description, pos_desc, pElem, count_desc );
                DDT_DUMP_STACK( pConvertor->pStack, pConvertor->stack_pos, pElem, "advance loop" );
                continue;
            }
        }
    complete_loop:
        iov[iov_count].iov_len -= iov_len_local;  /* update the amount of valid data */
        total_packed += iov[iov_count].iov_len;
    }
    *max_data = total_packed;
    pConvertor->bConverted += total_packed;  /* update the already converted bytes */
    *out_size = iov_count;
    if( pConvertor->bConverted == pConvertor->local_size ) {
        pConvertor->flags |= CONVERTOR_COMPLETED;
        return 1;
    }
    /* Save the global position for the next round */
    PUSH_STACK( pStack, pConvertor->stack_pos, pos_desc, pElem->elem.common.type, count_desc,
                conv_ptr - pConvertor->pBaseBuf );
    DO_DEBUG( opal_output( 0, "pack save stack stack_pos %d pos_desc %d count_desc %" PRIsize_t" disp %ld\n",
                           pConvertor->stack_pos, pStack->index, pStack->count, pStack->disp ); );
    return 0;
}
