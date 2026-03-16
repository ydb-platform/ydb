/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2011 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2009 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2007 Voltaire. All rights reserved.
 * Copyright (c) 2009-2018 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2010-2015 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2011-2014 NVIDIA Corporation.  All rights reserved.
 * Copyright (c) 2010-2017 IBM Corporation.  All rights reserved.
 * Copyright (c) 2014-2016 Intel, Inc. All rights reserved.
 * Copyright (c) 2014-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#include "opal_config.h"

#include <string.h>

#include "opal/mca/btl/btl.h"
#include "opal/mca/btl/base/base.h"
#include "opal/util/output.h"
#include "opal/util/show_help.h"
#include "opal/util/argv.h"
#include "opal/constants.h"

static int mca_btl_sm_component_register(void);

/*
 * The "sm" BTL has been completely replaced by the "vader" BTL.
 *
 * The only purpose for this component is to print a show_help message
 * to inform the user that they should be using the vader BTL.
 */
mca_btl_base_component_3_0_0_t mca_btl_sm_component = {
    /* First, the mca_base_component_t struct containing meta information
       about the component itself */
    .btl_version = {
        MCA_BTL_DEFAULT_VERSION("sm"),
        .mca_register_component_params = mca_btl_sm_component_register,
    },
    .btl_data = {
        /* The component is checkpoint ready */
        .param_field = MCA_BASE_METADATA_PARAM_CHECKPOINT
    }
};


static int mca_btl_sm_component_register(void)
{
    // If the sm component was explicitly requested, print a show_help
    // message and return an error (which will cause the process to
    // abort).
    if (NULL != opal_btl_base_framework.framework_selection) {
        char **names;
        names = opal_argv_split(opal_btl_base_framework.framework_selection,
                                ',');
        if (NULL != names) {
            for (int i = 0; NULL != names[i]; ++i) {
                if (strcmp(names[i], "sm") == 0) {
                    opal_show_help("help-mpi-btl-sm.txt", "btl sm is dead",
                                   true);
                    opal_argv_free(names);
                    return OPAL_ERROR;
                }
            }
        }

        opal_argv_free(names);
    }

    // Tell the framework that we don't want this component to be
    // considered.
    return OPAL_ERR_NOT_AVAILABLE;
}
