/*
 * Copyright (c) 2014      Intel, Inc. All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "opal_config.h"
#include "opal/constants.h"

#include "opal/mca/mca.h"
#include "opal/util/output.h"
#include "opal/mca/base/base.h"

#include "opal/mca/reachable/base/base.h"


/*
 * The following file was created by configure.  It contains extern
 * components and the definition of an array of pointers to each
 * module's public mca_base_module_t struct.
 */

#include "opal/mca/reachable/base/static-components.h"

opal_reachable_base_module_t opal_reachable = {0};

static int opal_reachable_base_frame_register(mca_base_register_flag_t flags)
{
    return OPAL_SUCCESS;
}

static int opal_reachable_base_frame_close(void)
{
    return mca_base_framework_components_close(&opal_reachable_base_framework, NULL);
}

static int opal_reachable_base_frame_open(mca_base_open_flag_t flags)
{
    /* Open up all available components */
    return mca_base_framework_components_open(&opal_reachable_base_framework, flags);
}

MCA_BASE_FRAMEWORK_DECLARE(opal, reachable, "OPAL Reachability Framework",
                           opal_reachable_base_frame_register,
                           opal_reachable_base_frame_open,
                           opal_reachable_base_frame_close,
                           mca_reachable_base_static_components, 0);
