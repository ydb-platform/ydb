/*
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

#ifndef OPAL_MEMCPY_BASE_H
#define OPAL_MEMCPY_BASE_H

#include "opal_config.h"
#include "opal/mca/base/mca_base_framework.h"
#include "opal/mca/memcpy/memcpy.h"


/*
 * Global functions for MCA overall memcpy open and close
 */

BEGIN_C_DECLS

/**
 * Framework declaration for the memcpy framework
 */
OPAL_DECLSPEC extern mca_base_framework_t opal_memcpy_base_framework;

END_C_DECLS

/* include implementation to call */
#include MCA_timer_IMPLEMENTATION_HEADER

#endif /* OPAL_BASE_MEMCPY_H */
