/* -*- Mode: C; c-basic-offset:4 ; -*- */
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
 * Copyright (c) 2009      Sun Microsystems, Inc. All rights reserved.
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2018      Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include <stdlib.h>
#include <stdio.h>

#include "opal/constants.h"
#include "opal/util/output.h"
#include "opal/datatype/opal_datatype.h"
#include "opal/datatype/opal_datatype_internal.h"

/********************************************************
 * Data dumping functions
 ********************************************************/

int opal_datatype_contain_basic_datatypes( const opal_datatype_t* pData, char* ptr, size_t length )
{
    int i;
    int32_t index = 0;
    uint64_t mask = 1;

    if( pData->flags & OPAL_DATATYPE_FLAG_USER_LB ) index += snprintf( ptr, length - index, "lb " );
    if( pData->flags & OPAL_DATATYPE_FLAG_USER_UB ) index += snprintf( ptr + index, length - index, "ub " );
    for( i = 0; i < OPAL_DATATYPE_MAX_PREDEFINED; i++ ) {
        if( pData->bdt_used & mask ) {
            if( NULL == pData->ptypes ) {
                index += snprintf( ptr + index, length - index, "%s:* ", opal_datatype_basicDatatypes[i]->name );
            } else {
                index += snprintf( ptr + index, length - index, "%s:%" PRIsize_t " ", opal_datatype_basicDatatypes[i]->name,
                                   pData->ptypes[i]);
            }
        }
        mask <<= 1;
        if( length <= (size_t)index ) break;
    }
    return index;
}

int opal_datatype_dump_data_flags( unsigned short usflags, char* ptr, size_t length )
{
    int index = 0;
    if( length < 22 ) return 0;
    index = snprintf( ptr, 22, "-----------[---][---]" );  /* set everything to - */
    if( usflags & OPAL_DATATYPE_FLAG_COMMITTED )   ptr[1]  = 'c';
    if( usflags & OPAL_DATATYPE_FLAG_CONTIGUOUS ) ptr[2]  = 'C';
    if( usflags & OPAL_DATATYPE_FLAG_OVERLAP )    ptr[3]  = 'o';
    if( usflags & OPAL_DATATYPE_FLAG_USER_LB )    ptr[4]  = 'l';
    if( usflags & OPAL_DATATYPE_FLAG_USER_UB )    ptr[5]  = 'u';
    if( usflags & OPAL_DATATYPE_FLAG_PREDEFINED ) ptr[6]  = 'P';
    if( !(usflags & OPAL_DATATYPE_FLAG_NO_GAPS) ) ptr[7]  = 'G';
    if( usflags & OPAL_DATATYPE_FLAG_DATA )       ptr[8]  = 'D';
    if( (usflags & OPAL_DATATYPE_FLAG_BASIC) == OPAL_DATATYPE_FLAG_BASIC ) ptr[9]  = 'B';
    /* We know nothing about the upper level language or flags! */
    /* ... */
    return index;
}


int opal_datatype_dump_data_desc( dt_elem_desc_t* pDesc, int nbElems, char* ptr, size_t length )
{
    int i;
    int32_t index = 0;

    for( i = 0; i < nbElems; i++ ) {
        index += opal_datatype_dump_data_flags( pDesc->elem.common.flags, ptr + index, length );
        if( length <= (size_t)index ) break;
        index += snprintf( ptr + index, length - index, "%15s ", opal_datatype_basicDatatypes[pDesc->elem.common.type]->name );
        if( length <= (size_t)index ) break;
        if( OPAL_DATATYPE_LOOP == pDesc->elem.common.type )
            index += snprintf( ptr + index, length - index, "%d times the next %d elements extent %d\n",
                               (int)pDesc->loop.loops, (int)pDesc->loop.items,
                               (int)pDesc->loop.extent );
        else if( OPAL_DATATYPE_END_LOOP == pDesc->elem.common.type )
            index += snprintf( ptr + index, length - index, "prev %d elements first elem displacement %ld size of data %d\n",
                           (int)pDesc->end_loop.items, (long)pDesc->end_loop.first_elem_disp,
                           (int)pDesc->end_loop.size );
        else
            index += snprintf( ptr + index, length - index, "count %" PRIsize_t " disp 0x%lx (%ld) blen %d extent %ld (size %ld)\n",
                               pDesc->elem.count, (long)pDesc->elem.disp, (long)pDesc->elem.disp, (int)pDesc->elem.blocklen,
                               pDesc->elem.extent, (long)(pDesc->elem.count * opal_datatype_basicDatatypes[pDesc->elem.common.type]->size) );
        pDesc++;

        if( length <= (size_t)index ) break;
    }
    return index;
}


void opal_datatype_dump( const opal_datatype_t* pData )
{
    size_t length;
    int index = 0;
    char* buffer;

    length = pData->opt_desc.used + pData->desc.used;
    length = length * 100 + 500;
    buffer = (char*)malloc( length );
    index += snprintf( buffer, length - index, "Datatype %p[%s] size %ld align %d id %d length %d used %d\n"
                                               "true_lb %ld true_ub %ld (true_extent %ld) lb %ld ub %ld (extent %ld)\n"
                                               "nbElems %" PRIsize_t " loops %d flags %X (",
                     (void*)pData, pData->name, (long)pData->size, (int)pData->align, pData->id, (int)pData->desc.length, (int)pData->desc.used,
                     (long)pData->true_lb, (long)pData->true_ub, (long)(pData->true_ub - pData->true_lb),
                     (long)pData->lb, (long)pData->ub, (long)(pData->ub - pData->lb),
                     pData->nbElems, (int)pData->loops, (int)pData->flags );
    /* dump the flags */
    if( pData->flags == OPAL_DATATYPE_FLAG_PREDEFINED )
        index += snprintf( buffer + index, length - index, "predefined " );
    else {
        if( pData->flags & OPAL_DATATYPE_FLAG_COMMITTED ) index += snprintf( buffer + index, length - index, "committed " );
        if( pData->flags & OPAL_DATATYPE_FLAG_CONTIGUOUS) index += snprintf( buffer + index, length - index, "contiguous " );
    }
    index += snprintf( buffer + index, length - index, ")" );
    index += opal_datatype_dump_data_flags( pData->flags, buffer + index, length - index );
    {
        index += snprintf( buffer + index, length - index, "\n   contain " );
        index += opal_datatype_contain_basic_datatypes( pData, buffer + index, length - index );
        index += snprintf( buffer + index, length - index, "\n" );
    }
    if( (pData->opt_desc.desc != pData->desc.desc) && (NULL != pData->opt_desc.desc) ) {
        /* If the data is already committed print everything including the last
         * fake OPAL_DATATYPE_END_LOOP entry.
         */
        index += opal_datatype_dump_data_desc( pData->desc.desc, pData->desc.used + 1, buffer + index, length - index );
        index += snprintf( buffer + index, length - index, "Optimized description \n" );
        index += opal_datatype_dump_data_desc( pData->opt_desc.desc, pData->opt_desc.used + 1, buffer + index, length - index );
    } else {
        index += opal_datatype_dump_data_desc( pData->desc.desc, pData->desc.used, buffer + index, length - index );
        index += snprintf( buffer + index, length - index, "No optimized description\n" );
    }
    buffer[index] = '\0';  /* make sure we end the string with 0 */
    opal_output( 0, "%s\n", buffer );

    free(buffer);
}
