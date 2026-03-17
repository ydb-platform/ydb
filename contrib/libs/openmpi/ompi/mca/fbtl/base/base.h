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
 * MCA fbtl base framework public interface functions.
 */

#ifndef MCA_FBTL_BASE_H
#define MCA_FBTL_BASE_H

#include "ompi_config.h"

#include "mpi.h"
#include "opal/mca/base/base.h"
#include "ompi/mca/common/ompio/common_ompio.h"
#include "ompi/mca/fbtl/fbtl.h"


BEGIN_C_DECLS

OMPI_DECLSPEC int mca_fbtl_base_file_select(struct ompio_file_t *file,
                            mca_base_component_t *preferred);

OMPI_DECLSPEC int mca_fbtl_base_file_unselect(struct ompio_file_t *file);

OMPI_DECLSPEC int mca_fbtl_base_find_available(bool enable_progress_threads,
                                               bool enable_mpi_threads);

OMPI_DECLSPEC int mca_fbtl_base_init_file (struct ompio_file_t *file);

OMPI_DECLSPEC int mca_fbtl_base_get_param (struct ompio_file_t *file, int keyval);
/*
 * Globals
 */

OMPI_DECLSPEC extern mca_base_framework_t ompi_fbtl_base_framework;

END_C_DECLS

#endif /* MCA_BASE_FBTL_H */
