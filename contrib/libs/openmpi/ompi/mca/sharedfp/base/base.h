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
 * Copyright (c) 2012-2013 Los Alamos National Security, Inc.  All rights reserved.
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
 * MCA sharedfp base framework public interface functions.
 */

#ifndef MCA_SHAREDFP_BASE_H
#define MCA_SHAREDFP_BASE_H

#include "ompi_config.h"

#include "mpi.h"
#include "opal/class/opal_list.h"
#include "ompi/mca/mca.h"
#include "ompi/mca/common/ompio/common_ompio.h"
#include "ompi/mca/sharedfp/sharedfp.h"


BEGIN_C_DECLS

/*
 * MCA Framework
 */
OMPI_DECLSPEC extern mca_base_framework_t ompi_sharedfp_base_framework;
/* select a component */
OMPI_DECLSPEC int mca_sharedfp_base_file_select(struct ompio_file_t *file,
                                                mca_base_component_t *preferred);

OMPI_DECLSPEC int mca_sharedfp_base_file_unselect(struct ompio_file_t *file);

OMPI_DECLSPEC int mca_sharedfp_base_find_available(bool enable_progress_threads,
                                                   bool enable_mpi_threads);

OMPI_DECLSPEC int mca_sharedfp_base_init_file (struct ompio_file_t *file);

OMPI_DECLSPEC int mca_sharedfp_base_get_param (struct ompio_file_t *file, int keyval);
/*
 * Globals
 */

END_C_DECLS

#endif /* MCA_BASE_SHAREDFP_H */
