/*
 * Copyright (c) 2014      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file:
 */

#ifndef MCA_REACHABLE_BASE_H
#define MCA_REACHABLE_BASE_H

#include "opal_config.h"
#include "opal/types.h"

#include "opal/mca/mca.h"
#include "opal/mca/base/mca_base_framework.h"

#include "opal/mca/reachable/reachable.h"

BEGIN_C_DECLS

OPAL_DECLSPEC extern mca_base_framework_t opal_reachable_base_framework;

/**
 * Select a reachable module
 */
OPAL_DECLSPEC int opal_reachable_base_select(void);

OPAL_DECLSPEC opal_reachable_t * opal_reachable_allocate(unsigned int num_local,
							 unsigned int num_remote);


END_C_DECLS

#endif
