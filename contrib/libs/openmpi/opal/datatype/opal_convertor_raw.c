/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2009 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2013      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2017-2019 Research Organization for Information Science
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
#include "opal_stdint.h"

#if OPAL_ENABLE_DEBUG
#include "opal/util/output.h"

#define DO_DEBUG(INST)  if( opal_pack_debug ) { INST }
#else
#define DO_DEBUG(INST)
#endif /* OPAL_ENABLE_DEBUG */

/**
 * This function always work in local representation. This means no representation
 * conversion (i.e. no heterogeneity) has to be taken into account, and that all
 * length we're working on are local.
 */
int32_t
opal_convertor_raw( opal_convertor_t* pConvertor,
		    struct iovec* iov, uint32_t* iov_count,
		    size_t* length )
{
    const opal_datatype_t *pData = pConvertor->pDesc;
    dt_stack_t* pStack;       /* pointer to the position on the stack */
    uint32_t pos_desc;        /* actual position in the description of the derived datatype */
    size_t count_desc;        /* the number of items already done in the actual pos_desc */
    dt_elem_desc_t* description, *pElem;
    unsigned char *source_base;  /* origin of the data */
    size_t raw_data = 0;      /* sum of raw data lengths in the iov_len fields */
    uint32_t index = 0;       /* the iov index and a simple counter */

    assert( (*iov_count) > 0 );
    if( OPAL_LIKELY(pConvertor->flags & CONVERTOR_COMPLETED) ) {
        iov[0].iov_base = NULL;
        iov[0].iov_len  = 0;
        *iov_count      = 0;
        *length         = iov[0].iov_len;
        return 1;  /* We're still done */
    }
    if( OPAL_LIKELY(pConvertor->flags & CONVERTOR_NO_OP) ) {
        /* The convertor contain minimal informations, we only use the bConverted
         * to manage the conversion. This function work even after the convertor
         * was moved to a specific position.
         */
        opal_convertor_get_current_pointer( pConvertor, (void**)&iov[0].iov_base );
        iov[0].iov_len = pConvertor->local_size - pConvertor->bConverted;
        *length = iov[0].iov_len;
        pConvertor->bConverted = pConvertor->local_size;
        pConvertor->flags |= CONVERTOR_COMPLETED;
        *iov_count = 1;
        return 1;  /* we're done */
    }

    DO_DEBUG( opal_output( 0, "opal_convertor_raw( %p, {%p, %" PRIu32 "}, %"PRIsize_t " )\n", (void*)pConvertor,
                           (void*)iov, *iov_count, *length ); );

    description = pConvertor->use_desc->desc;

    /* For the first step we have to add both displacement to the source. After in the
    * main while loop we will set back the source_base to the correct value. This is
    * due to the fact that the convertor can stop in the middle of a data with a count
    */
    pStack = pConvertor->pStack + pConvertor->stack_pos;
    pos_desc     = pStack->index;
    source_base  = pConvertor->pBaseBuf + pStack->disp;
    count_desc   = pStack->count;
    pStack--;
    pConvertor->stack_pos--;
    pElem = &(description[pos_desc]);
    source_base += pStack->disp;
    DO_DEBUG( opal_output( 0, "raw start pos_desc %d count_desc %" PRIsize_t " disp %ld\n"
                           "stack_pos %d pos_desc %d count_desc %" PRIsize_t " disp %ld\n",
                           pos_desc, count_desc, (long)(source_base - pConvertor->pBaseBuf),
                           pConvertor->stack_pos, pStack->index, pStack->count, (long)pStack->disp ); );
    while( 1 ) {
        while( pElem->elem.common.flags & OPAL_DATATYPE_FLAG_DATA ) {
            size_t blength = opal_datatype_basicDatatypes[pElem->elem.common.type]->size;
            source_base += pElem->elem.disp;
            if( blength == (size_t)pElem->elem.extent ) { /* no resized data */
                if( index < *iov_count ) {
                    blength *= count_desc;
                    /* now here we have a basic datatype */
                    OPAL_DATATYPE_SAFEGUARD_POINTER( source_base, blength, pConvertor->pBaseBuf,
                                                pConvertor->pDesc, pConvertor->count );
                    DO_DEBUG( opal_output( 0, "raw 1. iov[%d] = {base %p, length %" PRIsize_t "}\n",
                                           index, (void*)source_base, (unsigned long)blength ); );
                    iov[index].iov_base = (IOVBASE_TYPE *) source_base;
                    iov[index].iov_len  = blength;
                    source_base += blength;
                    raw_data += blength;
                    index++;
                    count_desc = 0;
                }
            } else {
                for(size_t i = count_desc; (i > 0) && (index < *iov_count); i--, index++ ) {
                    OPAL_DATATYPE_SAFEGUARD_POINTER( source_base, blength, pConvertor->pBaseBuf,
                                                pConvertor->pDesc, pConvertor->count );
                    DO_DEBUG( opal_output( 0, "raw 2. iov[%d] = {base %p, length %" PRIsize_t "}\n",
                                           index, (void*)source_base, (unsigned long)blength ); );
                    iov[index].iov_base = (IOVBASE_TYPE *) source_base;
                    iov[index].iov_len  = blength;
                    source_base += pElem->elem.extent;
                    raw_data += blength;
                    count_desc--;
                }
            }
            source_base -= pElem->elem.disp;
            if( 0 == count_desc ) {  /* completed */
                source_base = pConvertor->pBaseBuf + pStack->disp;
                pos_desc++;  /* advance to the next data */
                UPDATE_INTERNAL_COUNTERS( description, pos_desc, pElem, count_desc );
                continue;
            }
            goto complete_loop;
        }
        if( OPAL_DATATYPE_END_LOOP == pElem->elem.common.type ) { /* end of the current loop */
            DO_DEBUG( opal_output( 0, "raw end_loop count %" PRIsize_t " stack_pos %d"
                                   " pos_desc %d disp %ld space %lu\n",
                                   pStack->count, pConvertor->stack_pos,
                                   pos_desc, (long)pStack->disp, (unsigned long)raw_data ); );
            if( --(pStack->count) == 0 ) { /* end of loop */
                if( pConvertor->stack_pos == 0 ) {
                    /* we lie about the size of the next element in order to
                    * make sure we exit the main loop.
                    */
                    *iov_count = index;
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
            source_base = pConvertor->pBaseBuf + pStack->disp;
            UPDATE_INTERNAL_COUNTERS( description, pos_desc, pElem, count_desc );
            DO_DEBUG( opal_output( 0, "raw new_loop count %" PRIsize_t " stack_pos %d "
                                   "pos_desc %d disp %ld space %lu\n",
                                   pStack->count, pConvertor->stack_pos,
                                   pos_desc, (long)pStack->disp, (unsigned long)raw_data ); );
        }
        if( OPAL_DATATYPE_LOOP == pElem->elem.common.type ) {
            ptrdiff_t local_disp = (ptrdiff_t)source_base;
            ddt_endloop_desc_t* end_loop = (ddt_endloop_desc_t*)(pElem + pElem->loop.items);

            if( pElem->loop.common.flags & OPAL_DATATYPE_FLAG_CONTIGUOUS ) {
                ptrdiff_t offset = end_loop->first_elem_disp;
                source_base += offset;
                for(size_t i = MIN(count_desc, *iov_count - index); i > 0; i--, index++ ) {
                    OPAL_DATATYPE_SAFEGUARD_POINTER( source_base, end_loop->size, pConvertor->pBaseBuf,
                                                pConvertor->pDesc, pConvertor->count );
                    iov[index].iov_base = (IOVBASE_TYPE *) source_base;
                    iov[index].iov_len  = end_loop->size;
                    source_base += pElem->loop.extent;
                    raw_data += end_loop->size;
                    count_desc--;
                    DO_DEBUG( opal_output( 0, "raw contig loop generate iov[%d] = {base %p, length %" PRIsize_t "}"
                                           "space %lu [pos_desc %d]\n",
                                           index, iov[index].iov_base, iov[index].iov_len,
                                           (unsigned long)raw_data, pos_desc ); );
                }
                source_base -= offset;
                if( 0 == count_desc ) {  /* completed */
                    pos_desc += pElem->loop.items + 1;
                    goto update_loop_description;
                }
            }
            if( index == *iov_count ) {  /* all iov have been filled, we need to bail out */
                goto complete_loop;
            }
            local_disp = (ptrdiff_t)source_base - local_disp;
            PUSH_STACK( pStack, pConvertor->stack_pos, pos_desc, OPAL_DATATYPE_LOOP, count_desc,
                        pStack->disp + local_disp);
            pos_desc++;
          update_loop_description:  /* update the current state */
            source_base = pConvertor->pBaseBuf + pStack->disp;
            UPDATE_INTERNAL_COUNTERS( description, pos_desc, pElem, count_desc );
            DDT_DUMP_STACK( pConvertor->pStack, pConvertor->stack_pos, pElem, "advance loop" );
            continue;
        }
    }
complete_loop:
    pConvertor->bConverted += raw_data;  /* update the already converted bytes */
    *length = raw_data;
    *iov_count = index;
    if( pConvertor->bConverted == pConvertor->local_size ) {
        pConvertor->flags |= CONVERTOR_COMPLETED;
        return 1;
    }
    /* I complete an element, next step I should go to the next one */
    PUSH_STACK( pStack, pConvertor->stack_pos, pos_desc, OPAL_DATATYPE_UINT1, count_desc,
                source_base - pStack->disp - pConvertor->pBaseBuf );
    DO_DEBUG( opal_output( 0, "raw save stack stack_pos %d pos_desc %d count_desc %" PRIsize_t " disp %ld\n",
                           pConvertor->stack_pos, pStack->index, pStack->count, (long)pStack->disp ); );
    return 0;
}
