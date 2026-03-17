/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2014 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2010      IBM Corporation.  All rights reserved.
 * Copyright (c) 2012-2013 Sandia National Laboratories.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#if !defined(OSC_RDMA_ACTIVE_TARGET_H)
#define OSC_RDMA_ACTIVE_TARGET_H

#include "osc_rdma.h"
#include "osc_rdma_sync.h"
#include "osc_rdma_lock.h"

int ompi_osc_rdma_fence_atomic (int assert, struct ompi_win_t *win);

int ompi_osc_rdma_start_atomic (struct ompi_group_t *group,
                                int assert, struct ompi_win_t *win);
int ompi_osc_rdma_complete_atomic (struct ompi_win_t *win);

int ompi_osc_rdma_post_atomic (struct ompi_group_t *group,
                               int assert, struct ompi_win_t *win);

int ompi_osc_rdma_wait_atomic (struct ompi_win_t *win);

int ompi_osc_rdma_test_atomic (struct ompi_win_t *win, int *flag);

#endif /* OSC_RDMA_ACTIVE_TARGET_H */
