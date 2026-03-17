/*
 * Copyright (c) 2004-2007 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef __VPROTOCOL_PESSIMIST_SENDERBASED_TYPES_H__
#define __VPROTOCOL_PESSIMIST_SENDERBASED_TYPES_H__

#include "ompi_config.h"
#include "vprotocol_pessimist_event.h"

BEGIN_C_DECLS

/* There is several different ways of packing the data to the sender-based
 * buffer. Just pick one.
 */
#define SB_USE_PACK_METHOD
#undef SB_USE_PROGRESS_METHOD
#undef SB_USE_CONVERTOR_METHOD

typedef struct vprotocol_pessimist_sender_based_t
{
    int sb_pagesize;        /* size of memory pages on this architecture */
#ifdef SB_USE_CONVERTOR_METHOD
    uintptr_t sb_conv_to_pessimist_offset; /* end of request from req_conv */
#endif
    int sb_fd;              /* file descriptor of mapped file */
    off_t sb_offset;        /* offset in mmaped file          */
    uintptr_t sb_addr;      /* base address of mmaped segment */
    size_t sb_length;       /* length of mmaped segment */
    uintptr_t sb_cursor;    /* current pointer to writeable memory */
    size_t sb_available;    /* available space before end of segment */

#ifdef SB_USE_PROGRESS_METHOD
    opal_list_t sb_sendreq; /* requests that needs to be progressed */
#endif
} vprotocol_pessimist_sender_based_t;

typedef struct vprotocol_pessimist_sender_based_header_t
{
    size_t size;
    int dst;
    int tag;
    uint32_t contextid;
    vprotocol_pessimist_clock_t sequence;
} vprotocol_pessimist_sender_based_header_t;

typedef struct vprotocol_pessimist_sender_based_request_t
{
    uintptr_t cursor;
    size_t bytes_progressed;
    convertor_advance_fct_t conv_advance;
    uint32_t conv_flags;
} vprotocol_pessimist_sender_based_request_t;


END_C_DECLS

#endif /* defined(VPROTOCOL_PESSIMIST_SENDERBASED_TYPES_H)*/

