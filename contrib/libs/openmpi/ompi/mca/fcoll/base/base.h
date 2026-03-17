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
 * Copyright (c) 2008      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2008-2011 University of Houston. All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 * MCA fcoll base framework public interface functions.
 */

#ifndef MCA_FCOLL_BASE_H
#define MCA_FCOLL_BASE_H

#include "ompi_config.h"

#include "mpi.h"
#include "opal/mca/base/base.h"
#include "ompi/mca/common/ompio/common_ompio.h"
#include "ompi/mca/fcoll/fcoll.h"

BEGIN_C_DECLS

OMPI_DECLSPEC int mca_fcoll_base_file_select(struct ompio_file_t *file,
                                             mca_base_component_t *preferred);
OMPI_DECLSPEC int mca_fcoll_base_query_table (struct ompio_file_t *file,
                                              char *name);
OMPI_DECLSPEC int mca_fcoll_base_file_unselect(struct ompio_file_t *file);

OMPI_DECLSPEC int mca_fcoll_base_find_available(bool enable_progress_threads,
                                                bool enable_mpi_threads);

OMPI_DECLSPEC int mca_fcoll_base_init_file (struct ompio_file_t *file);

OMPI_DECLSPEC int mca_fcoll_base_get_param (struct ompio_file_t *file, int keyval);
OMPI_DECLSPEC int ompi_fcoll_base_sort_iovec (struct iovec *iov, int num_entries, int *sorted);

OMPI_DECLSPEC mca_fcoll_base_component_t* mca_fcoll_base_component_lookup(const char* name);

/*
 * Globals
 */
OMPI_DECLSPEC extern mca_base_framework_t ompi_fcoll_base_framework;

END_C_DECLS

#endif /* MCA_BASE_FCOLL_H */
