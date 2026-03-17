/*
 * Copyright (c) 2004-2007 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
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
#include "base.h"
#include "ompi/mca/mca.h"
#include "opal/mca/base/base.h"
#include "ompi/mca/vprotocol/base/static-components.h"

char *mca_vprotocol_base_include_list = NULL;
mca_pml_v_t mca_pml_v = {-1, 0, 0};

/* Load any vprotocol MCA component and call open function of all those
 * components.
 *
 * Also fill the mca_vprotocol_base_include_list with components that exists
 */

static int mca_vprotocol_base_open(mca_base_open_flag_t flags)
{
    if (NULL == mca_vprotocol_base_include_list) {
        return OMPI_SUCCESS;
    }

    return mca_base_framework_components_open(&ompi_vprotocol_base_framework, 0);
}

void mca_vprotocol_base_set_include_list(char *vprotocol_include_list)
{
    mca_vprotocol_base_include_list = NULL;

    if (NULL != vprotocol_include_list && vprotocol_include_list[0] != '\0') {
        mca_vprotocol_base_include_list = strdup (vprotocol_include_list);
    }
}

/* Close and unload any vprotocol MCA component loaded.
 */
static int mca_vprotocol_base_close(void)
{
    if (NULL != mca_vprotocol_base_include_list) {
        free(mca_vprotocol_base_include_list);
    }

    return mca_base_framework_components_close(&ompi_vprotocol_base_framework, NULL);;
}

MCA_BASE_FRAMEWORK_DECLARE(ompi, vprotocol, "OMPI Vprotocol", NULL,
                           mca_vprotocol_base_open, mca_vprotocol_base_close,
                           mca_vprotocol_base_static_components, 0);

