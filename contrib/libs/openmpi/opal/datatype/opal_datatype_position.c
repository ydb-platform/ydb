/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2006 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2014 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2006 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2013      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include <stddef.h>
#include <stdlib.h>

#include "opal/datatype/opal_datatype.h"
#include "opal/datatype/opal_convertor.h"
#include "opal/datatype/opal_datatype_internal.h"

#if OPAL_ENABLE_DEBUG
#include "opal/util/output.h"

#define DO_DEBUG(INST)  if( opal_position_debug ) { INST }
#else
#define DO_DEBUG(INST)
#endif  /* OPAL_ENABLE_DEBUG */

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

/**
 * Advance the current position in the convertor based using the
 * current element and a left-over counter. Update the head pointer
 * and the leftover byte space.
 */
static inline void
position_predefined_data( opal_convertor_t* CONVERTOR,
                          dt_elem_desc_t* ELEM,
                          size_t* COUNT,
                          unsigned char** POINTER,
                          size_t* SPACE )
{
    size_t _copy_count = *(COUNT);
    size_t _copy_blength;
    ddt_elem_desc_t* _elem = &((ELEM)->elem);

    _copy_blength =  opal_datatype_basicDatatypes[_elem->common.type]->size;
    if( (_copy_count * _copy_blength) > *(SPACE) ) {
        _copy_count = *(SPACE) / _copy_blength;
        if( 0 == _copy_count ) return;  /* nothing to do */
    }
    _copy_blength *= _copy_count;

    OPAL_DATATYPE_SAFEGUARD_POINTER( *(POINTER) + _elem->disp, _copy_blength, (CONVERTOR)->pBaseBuf,
                                (CONVERTOR)->pDesc, (CONVERTOR)->count );
    *(POINTER) += (_copy_count * _elem->extent);
    *(SPACE)   -= _copy_blength;
    *(COUNT)   -= _copy_count;
}

/**
 * Advance the current position in the convertor based using the
 * current contiguous loop and a left-over counter. Update the head
 * pointer and the leftover byte space.
 */
static inline void
position_contiguous_loop( opal_convertor_t* CONVERTOR,
                          dt_elem_desc_t* ELEM,
                          size_t* COUNT,
                          unsigned char** POINTER,
                          size_t* SPACE )
{
    ddt_loop_desc_t *_loop = (ddt_loop_desc_t*)(ELEM);
    ddt_endloop_desc_t* _end_loop = (ddt_endloop_desc_t*)((ELEM) + (ELEM)->loop.items);
    size_t _copy_loops = *(COUNT);

    if( (_copy_loops * _end_loop->size) > *(SPACE) )
        _copy_loops = *(SPACE) / _end_loop->size;
    OPAL_DATATYPE_SAFEGUARD_POINTER( *(POINTER) + _end_loop->first_elem_disp,
                                (_copy_loops - 1) * _loop->extent + _end_loop->size,
                                (CONVERTOR)->pBaseBuf, (CONVERTOR)->pDesc, (CONVERTOR)->count );
    *(POINTER) += _copy_loops * _loop->extent;
    *(SPACE)   -= _copy_loops * _end_loop->size;
    *(COUNT)   -= _copy_loops;
}

#define POSITION_PREDEFINED_DATATYPE( CONVERTOR, ELEM, COUNT, POSITION, SPACE ) \
    position_predefined_data( (CONVERTOR), (ELEM), &(COUNT), &(POSITION), &(SPACE) )

#define POSITION_CONTIGUOUS_LOOP( CONVERTOR, ELEM, COUNT, POSITION, SPACE ) \
    position_contiguous_loop( (CONVERTOR), (ELEM), &(COUNT), &(POSITION), &(SPACE) )

int opal_convertor_generic_simple_position( opal_convertor_t* pConvertor,
                                            size_t* position )
{
    dt_stack_t* pStack;       /* pointer to the position on the stack */
    uint32_t pos_desc;        /* actual position in the description of the derived datatype */
    size_t count_desc;       /* the number of items already done in the actual pos_desc */
    dt_elem_desc_t* description = pConvertor->use_desc->desc;
    dt_elem_desc_t* pElem;    /* current position */
    unsigned char *base_pointer = pConvertor->pBaseBuf;
    size_t iov_len_local;
    ptrdiff_t extent = pConvertor->pDesc->ub - pConvertor->pDesc->lb;

    DUMP( "opal_convertor_generic_simple_position( %p, &%ld )\n", (void*)pConvertor, (long)*position );
    assert(*position > pConvertor->bConverted);

    /* We dont want to have to parse the datatype multiple times. What we are interested in
     * here is to compute the number of completed datatypes that we can move forward, update
     * the  counters and finally compute the position taking in account only the remaining
     * elements. The only problem is that we have to modify all the elements on the stack.
     */
    iov_len_local = *position - pConvertor->bConverted;
    if( iov_len_local > pConvertor->pDesc->size ) {
        pStack = pConvertor->pStack;  /* we're working with the full stack */
        count_desc = iov_len_local / pConvertor->pDesc->size;
        DO_DEBUG( opal_output( 0, "position before %lu asked %lu data size %lu"
                               " iov_len_local %lu count_desc %" PRIsize_t "\n",
                               (unsigned long)pConvertor->bConverted, (unsigned long)*position, (unsigned long)pConvertor->pDesc->size,
                               (unsigned long)iov_len_local, count_desc ); );
        /* Update all the stack including the last one */
        for( pos_desc = 0; pos_desc <= pConvertor->stack_pos; pos_desc++ )
            pStack[pos_desc].disp += count_desc * extent;
        pConvertor->bConverted += count_desc * pConvertor->pDesc->size;
        iov_len_local = *position - pConvertor->bConverted;
        pStack[0].count -= count_desc;
        DO_DEBUG( opal_output( 0, "after bConverted %lu remaining count %lu iov_len_local %lu\n",
                               (unsigned long)pConvertor->bConverted, (unsigned long)pStack[0].count, (unsigned long)iov_len_local ); );
    }

    pStack = pConvertor->pStack + pConvertor->stack_pos;
    pos_desc      = pStack->index;
    base_pointer += pStack->disp;
    count_desc    = pStack->count;
    pStack--;
    pConvertor->stack_pos--;
    pElem = &(description[pos_desc]);

    DO_DEBUG( opal_output( 0, "position start pos_desc %d count_desc %" PRIsize_t " disp %llx\n"
                           "stack_pos %d pos_desc %d count_desc %" PRIsize_t " disp %llx\n",
                           pos_desc, count_desc, (unsigned long long)(base_pointer - pConvertor->pBaseBuf),
                           pConvertor->stack_pos, pStack->index, pStack->count, (unsigned long long)pStack->disp ); );
    /* Last data has been only partially converted. Compute the relative position */
    if( 0 != pConvertor->partial_length ) {
        size_t element_length = opal_datatype_basicDatatypes[pElem->elem.common.type]->size;
        size_t missing_length = element_length - pConvertor->partial_length;
        if( missing_length >= iov_len_local ) {
            pConvertor->partial_length = (pConvertor->partial_length + iov_len_local) % element_length;
            pConvertor->bConverted    += iov_len_local;
            assert(pConvertor->partial_length < element_length);
            return 0;
        }
        pConvertor->partial_length = (pConvertor->partial_length + missing_length) % element_length;
        assert(pConvertor->partial_length == 0);
        pConvertor->bConverted += missing_length;
        iov_len_local -= missing_length;
        count_desc--;
    }
    while( 1 ) {
        if( OPAL_DATATYPE_END_LOOP == pElem->elem.common.type ) { /* end of the current loop */
            DO_DEBUG( opal_output( 0, "position end_loop count %" PRIsize_t " stack_pos %d pos_desc %d disp %lx space %lu\n",
                                   pStack->count, pConvertor->stack_pos, pos_desc,
                                   pStack->disp, (unsigned long)iov_len_local ); );
            if( --(pStack->count) == 0 ) { /* end of loop */
                if( pConvertor->stack_pos == 0 ) {
                    pConvertor->flags |= CONVERTOR_COMPLETED;
                    pConvertor->partial_length = 0;
                    goto complete_loop;  /* completed */
                }
                pConvertor->stack_pos--;
                pStack--;
                pos_desc++;
            } else {
                if( pStack->index == -1 ) {
                    pStack->disp += extent;
                } else {
                    assert( OPAL_DATATYPE_LOOP == description[pStack->index].loop.common.type );
                    pStack->disp += description[pStack->index].loop.extent;
                }
                pos_desc = pStack->index + 1;
            }
            base_pointer = pConvertor->pBaseBuf + pStack->disp;
            UPDATE_INTERNAL_COUNTERS( description, pos_desc, pElem, count_desc );
            DO_DEBUG( opal_output( 0, "position new_loop count %" PRIsize_t " stack_pos %d pos_desc %d disp %lx space %lu\n",
                                   pStack->count, pConvertor->stack_pos, pos_desc,
                                   pStack->disp, (unsigned long)iov_len_local ); );
        }
        if( OPAL_DATATYPE_LOOP == pElem->elem.common.type ) {
            ptrdiff_t local_disp = (ptrdiff_t)base_pointer;
            if( pElem->loop.common.flags & OPAL_DATATYPE_FLAG_CONTIGUOUS ) {
                POSITION_CONTIGUOUS_LOOP( pConvertor, pElem, count_desc,
                                          base_pointer, iov_len_local );
                if( 0 == count_desc ) {  /* completed */
                    pos_desc += pElem->loop.items + 1;
                    goto update_loop_description;
                }
                /* Save the stack with the correct last_count value. */
            }
            local_disp = (ptrdiff_t)base_pointer - local_disp;
            PUSH_STACK( pStack, pConvertor->stack_pos, pos_desc, OPAL_DATATYPE_LOOP, count_desc,
                        pStack->disp + local_disp );
            pos_desc++;
        update_loop_description:  /* update the current state */
            base_pointer = pConvertor->pBaseBuf + pStack->disp;
            UPDATE_INTERNAL_COUNTERS( description, pos_desc, pElem, count_desc );
            DDT_DUMP_STACK( pConvertor->pStack, pConvertor->stack_pos, pElem, "advance loop" );
            DO_DEBUG( opal_output( 0, "position set loop count %" PRIsize_t " stack_pos %d pos_desc %d disp %lx space %lu\n",
                                   pStack->count, pConvertor->stack_pos, pos_desc,
                                   pStack->disp, (unsigned long)iov_len_local ); );
            continue;
        }
        while( pElem->elem.common.flags & OPAL_DATATYPE_FLAG_DATA ) {
            /* now here we have a basic datatype */
            POSITION_PREDEFINED_DATATYPE( pConvertor, pElem, count_desc,
                                          base_pointer, iov_len_local );
            if( 0 != count_desc ) {  /* completed */
                pConvertor->partial_length = iov_len_local;
                goto complete_loop;
            }
            base_pointer = pConvertor->pBaseBuf + pStack->disp;
            pos_desc++;  /* advance to the next data */
            UPDATE_INTERNAL_COUNTERS( description, pos_desc, pElem, count_desc );
            DO_DEBUG( opal_output( 0, "position set loop count %" PRIsize_t " stack_pos %d pos_desc %d disp %lx space %lu\n",
                                   pStack->count, pConvertor->stack_pos, pos_desc,
                                   pStack->disp, (unsigned long)iov_len_local ); );
        }
    }
 complete_loop:
    pConvertor->bConverted = *position;  /* update the already converted bytes */

    if( !(pConvertor->flags & CONVERTOR_COMPLETED) ) {
        /* I complete an element, next step I should go to the next one */
        PUSH_STACK( pStack, pConvertor->stack_pos, pos_desc, pElem->elem.common.type, count_desc,
                    base_pointer - pConvertor->pBaseBuf );
        DO_DEBUG( opal_output( 0, "position save stack stack_pos %d pos_desc %d count_desc %" PRIsize_t " disp %llx\n",
                               pConvertor->stack_pos, pStack->index, pStack->count, (unsigned long long)pStack->disp ); );
        return 0;
    }
    return 1;
}
