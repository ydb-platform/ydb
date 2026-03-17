/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2006 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2017 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
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
#include <stdlib.h>

#ifdef HAVE_ALLOCA_H
#include <alloca.h>
#endif

#include "opal/datatype/opal_datatype.h"
#include "opal/datatype/opal_convertor.h"
#include "opal/datatype/opal_datatype_internal.h"


extern int opal_convertor_create_stack_with_pos_general( opal_convertor_t* convertor,
                                                         size_t starting_point, const size_t* sizes );

int opal_convertor_create_stack_with_pos_general( opal_convertor_t* pConvertor,
                                                  size_t starting_point, const size_t* sizes )
{
    dt_stack_t* pStack;   /* pointer to the position on the stack */
    int pos_desc;         /* actual position in the description of the derived datatype */
    size_t lastLength = 0;
    const opal_datatype_t* pData = pConvertor->pDesc;
	size_t loop_length, *remoteLength, remote_size;
    size_t resting_place = starting_point;
    dt_elem_desc_t* pElems;
    size_t count;

    assert( 0 != starting_point );
    assert( pConvertor->bConverted != starting_point );
    assert( starting_point <=(pConvertor->count * pData->size) );

    /*opal_output( 0, "Data extent %d size %d count %d total_size %d starting_point %d\n",
                 pData->ub - pData->lb, pData->size, pConvertor->count,
                 pConvertor->local_size, starting_point );*/
    pConvertor->stack_pos = 0;
    pStack = pConvertor->pStack;
    /* Fill the first position on the stack. This one correspond to the
     * last fake OPAL_DATATYPE_END_LOOP that we add to the data representation and
     * allow us to move quickly inside the datatype when we have a count.
     */
    pElems = pConvertor->use_desc->desc;

    if( (pConvertor->flags & CONVERTOR_HOMOGENEOUS) && (pData->flags & OPAL_DATATYPE_FLAG_CONTIGUOUS) ) {
        /* Special case for contiguous datatypes */
        int32_t cnt = (int32_t)(starting_point / pData->size);
        ptrdiff_t extent = pData->ub - pData->lb;

        loop_length = GET_FIRST_NON_LOOP( pElems );
        pStack[0].disp  = pElems[loop_length].elem.disp;
        pStack[0].type  = OPAL_DATATYPE_LOOP;
        pStack[0].count = pConvertor->count - cnt;
        cnt = (int32_t)(starting_point - cnt * pData->size);  /* number of bytes after the loop */
        pStack[1].index    = 0;
        pStack[1].type     = OPAL_DATATYPE_UINT1;
        pStack[1].disp     = pStack[0].disp;
        pStack[1].count    = pData->size - cnt;

        if( (ptrdiff_t)pData->size == extent ) { /* all elements are contiguous */
            pStack[1].disp += starting_point;
        } else {  /* each is contiguous but there are gaps inbetween */
            pStack[1].disp += (pConvertor->count - pStack[0].count) * extent + cnt;
        }

        pConvertor->bConverted = starting_point;
        pConvertor->stack_pos = 1;
        return OPAL_SUCCESS;
    }

    /* remove from the main loop all the complete datatypes */
    assert (! (pConvertor->flags & CONVERTOR_SEND));
    remote_size    = opal_convertor_compute_remote_size( pConvertor );
    count          = starting_point / remote_size;
    resting_place -= (remote_size * count);
    pStack->count  = pConvertor->count - count;
    pStack->index  = -1;

    loop_length = GET_FIRST_NON_LOOP( pElems );
    pStack->disp = count * (pData->ub - pData->lb) + pElems[loop_length].elem.disp;

    pos_desc  = 0;
    remoteLength = (size_t*)alloca( sizeof(size_t) * (pConvertor->pDesc->loops + 1));
    remoteLength[0] = 0;  /* initial value set to ZERO */
    loop_length = 0;

    /* The only way to get out of this loop is when we reach the desired position or
     * when we finish the whole datatype.
     */
    while( pos_desc < (int32_t)pConvertor->use_desc->used ) {
        if( OPAL_DATATYPE_END_LOOP == pElems->elem.common.type ) { /* end of the current loop */
            ddt_endloop_desc_t* end_loop = (ddt_endloop_desc_t*)pElems;
            ptrdiff_t extent;

            if( (loop_length * pStack->count) > resting_place ) {
                /* We will stop somewhere on this loop. To avoid moving inside the loop
                 * multiple times, we can compute the index of the loop where we will
                 * stop. Once this index is computed we can then reparse the loop once
                 * until we find the correct position.
                 */
                int32_t cnt = (int32_t)(resting_place / loop_length);
                if( pStack->index == -1 ) {
                    extent = pData->ub - pData->lb;
                } else {
                    assert( OPAL_DATATYPE_LOOP == (pElems - end_loop->items)->loop.common.type );
                    extent = ((ddt_loop_desc_t*)(pElems - end_loop->items))->extent;
                }
                pStack->count -= (cnt + 1);
                resting_place -= cnt * loop_length;
                pStack->disp += (cnt + 1) * extent;
                /* reset the remoteLength as we act as restarting the last loop */
                pos_desc -= (end_loop->items - 1);  /* go back to the first element in the loop */
                pElems -= (end_loop->items - 1);
                remoteLength[pConvertor->stack_pos] = 0;
                loop_length = 0;
                continue;
            }
            /* Not in this loop. Cleanup the stack and advance to the
             * next data description.
             */
            resting_place -= (loop_length * (pStack->count - 1));  /* update the resting place */
            pStack--;
            pConvertor->stack_pos--;
            pos_desc++;
            pElems++;
            remoteLength[pConvertor->stack_pos] += (loop_length * pStack->count);
            loop_length = remoteLength[pConvertor->stack_pos];
            continue;
        }
        if( OPAL_DATATYPE_LOOP == pElems->elem.common.type ) {
            remoteLength[pConvertor->stack_pos] += loop_length;
            PUSH_STACK( pStack, pConvertor->stack_pos, pos_desc, OPAL_DATATYPE_LOOP,
				        pElems->loop.loops, pStack->disp );
            pos_desc++;
            pElems++;
            remoteLength[pConvertor->stack_pos] = 0;
            loop_length = 0;  /* starting a new loop */
        }
        while( pElems->elem.common.flags & OPAL_DATATYPE_FLAG_DATA ) {
            /* now here we have a basic datatype */
            const opal_datatype_t* basic_type = BASIC_DDT_FROM_ELEM( (*pElems) );
            lastLength = pElems->elem.count * basic_type->size;
            if( resting_place < lastLength ) {
                int32_t cnt = (int32_t)(resting_place / basic_type->size);
                loop_length += (cnt * basic_type->size);
                resting_place -= (cnt * basic_type->size);
                PUSH_STACK( pStack, pConvertor->stack_pos, pos_desc, pElems->elem.common.type,
                            pElems->elem.count - cnt,
                            pElems->elem.disp + cnt * pElems->elem.extent );
                pConvertor->bConverted = starting_point - resting_place;
                DDT_DUMP_STACK( pConvertor->pStack, pConvertor->stack_pos,
                                pConvertor->pDesc->desc.desc, pConvertor->pDesc->name );
                return OPAL_SUCCESS;
            }
            loop_length += lastLength;
            resting_place -= lastLength;
            pos_desc++;  /* advance to the next data */
            pElems++;
        }
    }

    /* Correctly update the bConverted field */
    pConvertor->flags |= CONVERTOR_COMPLETED;
    pConvertor->bConverted = pConvertor->local_size;
    return OPAL_SUCCESS;
}
