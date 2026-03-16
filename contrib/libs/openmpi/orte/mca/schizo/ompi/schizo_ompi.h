/*
 * Copyright (c) 2015      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef _MCA_SCHIZO_OMPI_H_
#define _MCA_SCHIZO_OMPI_H_

#include "orte_config.h"

#include "orte/types.h"

#include "opal/mca/base/base.h"
#include "orte/mca/schizo/schizo.h"


BEGIN_C_DECLS

ORTE_MODULE_DECLSPEC extern orte_schizo_base_component_t mca_schizo_ompi_component;
extern orte_schizo_base_module_t orte_schizo_ompi_module;

END_C_DECLS

#endif /* MCA_SCHIZO_OMPI_H_ */

