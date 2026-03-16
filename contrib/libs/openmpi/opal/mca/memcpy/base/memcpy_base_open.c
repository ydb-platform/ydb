/*
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "opal_config.h"

#include "opal/constants.h"
#include "opal/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/mca/memcpy/memcpy.h"
#include "opal/mca/memcpy/base/base.h"


/*
 * The following file was created by configure.  It contains extern
 * statements and the definition of an array of pointers to each
 * component's public mca_base_component_t struct.
 */
#include "opal/mca/memcpy/base/static-components.h"

/*
 * Globals
 */
/* Use default register/open/close functions */
MCA_BASE_FRAMEWORK_DECLARE(opal, memcpy, NULL, NULL, NULL, NULL,
                           mca_memcpy_base_static_components, 0);
