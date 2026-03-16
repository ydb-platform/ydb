/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2015 Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include "opal/mca/mca.h"
#include "opal/mca/base/base.h"

#include "opal/mca/dl/dl.h"
#include "opal/mca/dl/base/base.h"


int opal_dl_base_close(void)
{
    /* Close all available modules that are open */
    return mca_base_framework_components_close(&opal_dl_base_framework, NULL);
}
