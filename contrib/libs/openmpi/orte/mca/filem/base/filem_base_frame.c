/*
 * Copyright (c) 2004-2009 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2012-2013 Los Alamos National Security, LLC.
 *                         All rights reserved
 * Copyright (c) 2017      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"

#include "orte/constants.h"
#include "orte/mca/mca.h"
#include "opal/util/output.h"
#include "opal/mca/base/base.h"

#include "orte/mca/filem/filem.h"
#include "orte/mca/filem/base/base.h"

#include "orte/mca/filem/base/static-components.h"

/*
 * Globals
 */
ORTE_DECLSPEC orte_filem_base_module_t orte_filem = {
    .filem_init = orte_filem_base_module_init,
    .filem_finalize = orte_filem_base_module_finalize,
    .put = orte_filem_base_none_put,
    .put_nb = orte_filem_base_none_put_nb,
    .get = orte_filem_base_none_get,
    .get_nb = orte_filem_base_none_get_nb,
    .rm = orte_filem_base_none_rm,
    .rm_nb = orte_filem_base_none_rm_nb,
    .wait = orte_filem_base_none_wait,
    .wait_all = orte_filem_base_none_wait_all,
    .preposition_files = orte_filem_base_none_preposition_files,
    .link_local_files = orte_filem_base_none_link_local_files
};
bool orte_filem_base_is_active = false;

static int orte_filem_base_close(void)
{
    /* Close the selected component */
    if( NULL != orte_filem.filem_finalize ) {
        orte_filem.filem_finalize();
    }

    return mca_base_framework_components_close(&orte_filem_base_framework, NULL);
}

/**
 * Function for finding and opening either all MCA components,
 * or the one that was specifically requested via a MCA parameter.
 */
static int orte_filem_base_open(mca_base_open_flag_t flags)
{
     /* Open up all available components */
    return mca_base_framework_components_open(&orte_filem_base_framework, flags);
}

MCA_BASE_FRAMEWORK_DECLARE(orte, filem, NULL, NULL, orte_filem_base_open, orte_filem_base_close,
                           mca_filem_base_static_components, 0);
