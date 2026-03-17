/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2010      Cisco Systems, Inc. All rights reserved.
 * Copyright (c) 2016      Intel, Inc. All rights reserved
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

#include "pmix_common.h"
#include "src/class/pmix_ring_buffer.h"
#include "src/util/output.h"

static void pmix_ring_buffer_construct(pmix_ring_buffer_t *);
static void pmix_ring_buffer_destruct(pmix_ring_buffer_t *);

PMIX_CLASS_INSTANCE(pmix_ring_buffer_t, pmix_object_t,
                    pmix_ring_buffer_construct,
                    pmix_ring_buffer_destruct);

/*
 * pmix_ring_buffer constructor
 */
static void pmix_ring_buffer_construct(pmix_ring_buffer_t *ring)
{
    ring->head = 0;
    ring->tail = -1;
    ring->size = 0;
    ring->addr = NULL;
}

/*
 * pmix_ring_buffer destructor
 */
static void pmix_ring_buffer_destruct(pmix_ring_buffer_t *ring)
{
    if( NULL != ring->addr) {
        free(ring->addr);
        ring->addr = NULL;
    }

    ring->size = 0;
}

/**
 * initialize a ring object
 */
int pmix_ring_buffer_init(pmix_ring_buffer_t* ring, int size)
{
    /* check for errors */
    if (NULL == ring) {
        return PMIX_ERR_BAD_PARAM;
    }

    /* Allocate and set the ring to NULL */
    ring->addr = (char **)calloc(size * sizeof(char*), 1);
    if (NULL == ring->addr) { /* out of memory */
        return PMIX_ERR_OUT_OF_RESOURCE;
    }
    ring->size = size;

    return PMIX_SUCCESS;
}

void* pmix_ring_buffer_push(pmix_ring_buffer_t *ring, void *ptr)
{
    char *p=NULL;

    if (NULL != ring->addr[ring->head]) {
        p = (char*)ring->addr[ring->head];
        if (ring->tail == ring->size - 1) {
            ring->tail = 0;
        } else {
            ring->tail = ring->head + 1;
        }
    }
    ring->addr[ring->head] = (char*)ptr;
    if (ring->tail < 0) {
        ring->tail = ring->head;
    }
    if (ring->head == ring->size - 1) {
        ring->head = 0;
    } else {
        ring->head++;
    }
    return (void*)p;
}

void* pmix_ring_buffer_pop(pmix_ring_buffer_t *ring)
{
    char *p=NULL;

    if (-1 == ring->tail) {
        /* nothing has been put on the ring yet */
        p = NULL;
    } else {
        p = (char*)ring->addr[ring->tail];
        ring->addr[ring->tail] = NULL;
        if (ring->tail == ring->size-1) {
            ring->tail = 0;
        } else {
            ring->tail++;
        }
        /* see if the ring is empty */
        if (ring->tail == ring->head) {
            ring->tail = -1;
        }
    }
    return (void*)p;
}

 void* pmix_ring_buffer_poke(pmix_ring_buffer_t *ring, int i)
 {
    char *p=NULL;
    int offset;

    if (ring->size <= i || -1 == ring->tail) {
        p = NULL;
    } else if (i < 0) {
        /* return the value at the head of the ring */
        if (ring->head == 0) {
            p = ring->addr[ring->size - 1];
        } else {
            p = ring->addr[ring->head - 1];
        }
    } else {
        /* calculate the offset of the tail in the ring */
        offset = ring->tail + i;
        /* correct for wrap-around */
        if (ring->size <= offset) {
            offset -= ring->size;
        }
        p = ring->addr[offset];
    }
    return (void*)p;
}
