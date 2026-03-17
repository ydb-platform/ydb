/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2011 University of Houston. All rights reserved.
 * Copyright (c) 2012-2013 Los Alamos National Security, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "ompi_config.h"
#include <stdio.h>

#include "ompi/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/mca/base/mca_base_framework.h"

#include "ompi/mca/sharedfp/sharedfp.h"
#include "ompi/mca/sharedfp/base/base.h"


/*
 * The following file was created by configure.  It contains extern
 * statements and the definition of an array of pointers to each
 * component's public mca_base_component_t struct.
 */
#include "ompi/mca/sharedfp/base/static-components.h"

/*
 * Global variables
 */
mca_sharedfp_base_module_t mca_sharedfp = {0};

static int mca_sharedfp_base_close(void)
{
    return mca_base_framework_components_close(&ompi_sharedfp_base_framework, NULL);
}

/*
 * Function for finding and opening either all MCA components, or the one
 * that was specifically requested via a MCA parameter.
 */
static int mca_sharedfp_base_open(mca_base_open_flag_t flags)
{
    /* Open up all available components */
    return mca_base_framework_components_open(&ompi_sharedfp_base_framework, flags);
}

MCA_BASE_FRAMEWORK_DECLARE(ompi, sharedfp, "OMPI Shared Files", NULL,
                           mca_sharedfp_base_open, mca_sharedfp_base_close,
                           mca_sharedfp_base_static_components, 0);

