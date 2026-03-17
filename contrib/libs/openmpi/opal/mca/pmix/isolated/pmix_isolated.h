/*
 * Copyright (c) 2016      Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_PMIX_ISOLATED_H
#define MCA_PMIX_ISOLATED_H

#include "opal_config.h"

#include "opal/mca/mca.h"
#include "opal/mca/pmix/pmix.h"


BEGIN_C_DECLS

OPAL_DECLSPEC extern opal_pmix_base_component_t mca_pmix_isolated_component;

OPAL_DECLSPEC extern const opal_pmix_base_module_t opal_pmix_isolated_module;


END_C_DECLS

#endif /* MCA_PMIX_ISOLATED_H */
