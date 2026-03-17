/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_BML_BASE_BTL_H
#define MCA_BML_BASE_BTL_H

#include "ompi_config.h"

#include "ompi/mca/mca.h"


/*
 * Global functions for the BML
 */

BEGIN_C_DECLS

/* forward declarations */
struct mca_bml_base_btl_array_t;

OMPI_DECLSPEC int mca_bml_base_btl_array_reserve(struct mca_bml_base_btl_array_t* array, size_t size);


END_C_DECLS
#endif /* MCA_BML_BASE_H */

