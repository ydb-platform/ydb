/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2009 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2011      NVIDIA Corporation.  All rights reserved.
 * Copyright (c) 2017-2018 Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_DATATYPE_UNPACK_H_HAS_BEEN_INCLUDED
#define OPAL_DATATYPE_UNPACK_H_HAS_BEEN_INCLUDED

#include "opal_config.h"

#if !defined(CHECKSUM) && OPAL_CUDA_SUPPORT
/* Make use of existing macro to do CUDA style memcpy */
#undef MEMCPY_CSUM
#define MEMCPY_CSUM( DST, SRC, BLENGTH, CONVERTOR ) \
    CONVERTOR->cbmemcpy( (DST), (SRC), (BLENGTH), (CONVERTOR) )
#endif

static inline void
unpack_predefined_data( opal_convertor_t* CONVERTOR,  /* the convertor */
                        const dt_elem_desc_t* ELEM,   /* the element description */
                        size_t* COUNT,                /* the number of elements */
                        unsigned char** SOURCE,       /* the source pointer */
                        unsigned char** DESTINATION,  /* the destination pointer */
                        size_t* SPACE )               /* the space in the destination buffer */
{
    size_t _copy_count = *(COUNT);
    size_t _copy_blength;
    const ddt_elem_desc_t* _elem = &((ELEM)->elem);
    unsigned char* _destination = (*DESTINATION) + _elem->disp;

    _copy_blength = opal_datatype_basicDatatypes[_elem->common.type]->size;
    if( (_copy_count * _copy_blength) > *(SPACE) ) {
        _copy_count = (*(SPACE) / _copy_blength);
        if( 0 == _copy_count ) return;  /* nothing to do */
    }

    if( (ptrdiff_t)_copy_blength == _elem->extent ) {
        _copy_blength *= _copy_count;
        /* the extent and the size of the basic datatype are equal */
        OPAL_DATATYPE_SAFEGUARD_POINTER( _destination, _copy_blength, (CONVERTOR)->pBaseBuf,
                                    (CONVERTOR)->pDesc, (CONVERTOR)->count );
        DO_DEBUG( opal_output( 0, "unpack 1. memcpy( %p, %p, %lu ) => space %lu\n",
                               (void*)_destination, (void*)*(SOURCE), (unsigned long)_copy_blength, (unsigned long)(*(SPACE)) ); );
        MEMCPY_CSUM( _destination, *(SOURCE), _copy_blength, (CONVERTOR) );
        *(SOURCE)    += _copy_blength;
        _destination += _copy_blength;
    } else {
        for(size_t _i = 0; _i < _copy_count; _i++ ) {
            OPAL_DATATYPE_SAFEGUARD_POINTER( _destination, _copy_blength, (CONVERTOR)->pBaseBuf,
                                        (CONVERTOR)->pDesc, (CONVERTOR)->count );
            DO_DEBUG( opal_output( 0, "unpack 2. memcpy( %p, %p, %lu ) => space %lu\n",
                                   (void*)_destination, (void*)*(SOURCE), (unsigned long)_copy_blength, (unsigned long)(*(SPACE) - (_i * _copy_blength)) ); );
            MEMCPY_CSUM( _destination, *(SOURCE), _copy_blength, (CONVERTOR) );
            *(SOURCE)    += _copy_blength;
            _destination += _elem->extent;
        }
        _copy_blength *= _copy_count;
    }
    (*DESTINATION)  = _destination - _elem->disp;
    *(SPACE)       -= _copy_blength;
    *(COUNT)       -= _copy_count;
}

static inline void unpack_contiguous_loop( opal_convertor_t* CONVERTOR,
                                           const dt_elem_desc_t* ELEM,
                                           size_t* COUNT,
                                           unsigned char** SOURCE,
                                           unsigned char** DESTINATION,
                                           size_t* SPACE )
{
    const ddt_loop_desc_t *_loop = (ddt_loop_desc_t*)(ELEM);
    const ddt_endloop_desc_t* _end_loop = (ddt_endloop_desc_t*)((ELEM) + _loop->items);
    unsigned char* _destination = (*DESTINATION) + _end_loop->first_elem_disp;
    size_t _copy_loops = *(COUNT);

    if( (_copy_loops * _end_loop->size) > *(SPACE) )
        _copy_loops = (*(SPACE) / _end_loop->size);
    for(size_t _i = 0; _i < _copy_loops; _i++ ) {
        OPAL_DATATYPE_SAFEGUARD_POINTER( _destination, _end_loop->size, (CONVERTOR)->pBaseBuf,
                                    (CONVERTOR)->pDesc, (CONVERTOR)->count );
        DO_DEBUG( opal_output( 0, "unpack 3. memcpy( %p, %p, %lu ) => space %lu\n",
                               (void*)_destination, (void*)*(SOURCE), (unsigned long)_end_loop->size, (unsigned long)(*(SPACE) - _i * _end_loop->size) ); );
        MEMCPY_CSUM( _destination, *(SOURCE), _end_loop->size, (CONVERTOR) );
        *(SOURCE)    += _end_loop->size;
        _destination += _loop->extent;
    }
    *(DESTINATION) = _destination - _end_loop->first_elem_disp;
    *(SPACE)      -= _copy_loops * _end_loop->size;
    *(COUNT)      -= _copy_loops;
}

#define UNPACK_PREDEFINED_DATATYPE( CONVERTOR, ELEM, COUNT, SOURCE, DESTINATION, SPACE ) \
    unpack_predefined_data( (CONVERTOR), (ELEM), &(COUNT), &(SOURCE), &(DESTINATION), &(SPACE) )

#define UNPACK_CONTIGUOUS_LOOP( CONVERTOR, ELEM, COUNT, SOURCE, DESTINATION, SPACE ) \
    unpack_contiguous_loop( (CONVERTOR), (ELEM), &(COUNT), &(SOURCE), &(DESTINATION), &(SPACE) )

#endif  /* OPAL_DATATYPE_UNPACK_H_HAS_BEEN_INCLUDED */
