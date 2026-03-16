/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2014      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"
#include "opal/class/opal_fifo.h"

static void opal_fifo_construct (opal_fifo_t *fifo)
{
    OBJ_CONSTRUCT(&fifo->opal_fifo_ghost, opal_list_item_t);

    fifo->opal_fifo_ghost.opal_list_next = &fifo->opal_fifo_ghost;

    /** used to protect against ABA problems when not using a 128-bit compare-and-set */
    fifo->opal_fifo_ghost.item_free = 0;

    fifo->opal_fifo_head.data.counter = 0;
    fifo->opal_fifo_head.data.item = &fifo->opal_fifo_ghost;

    fifo->opal_fifo_tail.data.counter = 0;
    fifo->opal_fifo_tail.data.item = &fifo->opal_fifo_ghost;
}

OBJ_CLASS_INSTANCE(opal_fifo_t, opal_object_t, opal_fifo_construct, NULL);
