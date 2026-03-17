/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "ompi/mca/mca.h"
#include "opal/util/output.h"
#include "opal/mca/base/base.h"

#include "ompi/mca/crcp/crcp.h"
#include "ompi/mca/crcp/base/base.h"

#include "ompi/mca/crcp/base/static-components.h"

/*
 * Globals
 */
OMPI_DECLSPEC ompi_crcp_base_module_t ompi_crcp = {
    NULL, /* crcp_init               */
    NULL  /* crcp_finalize           */
};

ompi_crcp_base_component_t ompi_crcp_base_selected_component = {{0}};

static int ompi_crcp_base_close(void)
{
    /* Close the selected component */
    if( NULL != ompi_crcp.crcp_finalize ) {
        ompi_crcp.crcp_finalize();
    }

    /* Close all available modules that are open */
    return mca_base_framework_components_close(&ompi_crcp_base_framework, NULL);
}

MCA_BASE_FRAMEWORK_DECLARE(ompi, crcp, NULL, NULL, NULL, ompi_crcp_base_close,
                           mca_crcp_base_static_components, 0);
