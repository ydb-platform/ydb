/*
 * Copyright (c) 2016-2018 Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef _MCA_REGX_FwD_H_
#define _MCA_REGX_FwD_H_

#include "orte_config.h"

#include "orte/types.h"

#include "opal/mca/base/base.h"
#include "orte/mca/regx/regx.h"


BEGIN_C_DECLS

ORTE_MODULE_DECLSPEC extern orte_regx_base_component_t mca_regx_fwd_component;
extern orte_regx_base_module_t orte_regx_fwd_module;

END_C_DECLS

#endif /* MCA_REGX_FwD_H_ */
