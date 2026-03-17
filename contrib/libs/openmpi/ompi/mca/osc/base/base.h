/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2004-2006 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2016-2017 IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file:
 */

#ifndef OMPI_MCA_OSC_BASE_H
#define OMPI_MCA_OSC_BASE_H

#include "ompi_config.h"
#include "ompi/info/info.h"
#include "ompi/communicator/communicator.h"
#include "ompi/win/win.h"
#include "opal/mca/base/base.h"

/*
 * Global functions for MCA overall collective open and close
 */
BEGIN_C_DECLS

/*
 * function definitions
 */
int ompi_osc_base_find_available(bool enable_progress_threads,
                                 bool enable_mpi_threads);

int ompi_osc_base_select(ompi_win_t *win,
                         void **base,
                         size_t size,
                         int disp_unit,
                         ompi_communicator_t *comm,
                         opal_info_t *info,
                         int flavor,
                         int *model);

int ompi_osc_base_finalize(void);

OMPI_DECLSPEC extern mca_base_framework_t ompi_osc_base_framework;

END_C_DECLS

#endif
