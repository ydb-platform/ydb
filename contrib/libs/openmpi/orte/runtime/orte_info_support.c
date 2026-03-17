/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2014 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010-2015 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2011-2012 University of Houston. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/constants.h"
#include "orte/types.h"

#include "opal/class/opal_pointer_array.h"
#include "opal/util/argv.h"
#include "opal/runtime/opal_info_support.h"

#include "orte/include/orte/frameworks.h"
#include "orte/util/proc_info.h"
#include "orte/runtime/runtime.h"
#include "orte/runtime/orte_globals.h"

#include "orte/runtime/orte_info_support.h"

const char *orte_info_type_orte = "orte";

static int orte_info_registered = 0;

void orte_info_register_types(opal_pointer_array_t *mca_types)
{
    int i;

    /* add the top-level type */
    opal_pointer_array_add(mca_types, "orte");

    /* push all the types found by autogen */
    for (i=0; NULL != orte_frameworks[i]; i++) {
        opal_pointer_array_add(mca_types, orte_frameworks[i]->framework_name);
    }
}

int orte_info_register_framework_params(opal_pointer_array_t *component_map)
{
    int rc;

    if (orte_info_registered++) {
        return ORTE_SUCCESS;
    }

    /* Register the ORTE layer's MCA parameters */

    if (ORTE_SUCCESS != (rc = orte_register_params()) &&
        ORTE_ERR_BAD_PARAM != rc) {
        fprintf(stderr, "orte_info_register: orte_register_params failed\n");
        return rc;
    }

    rc = opal_info_register_framework_params(component_map);
    if (OPAL_SUCCESS != rc) {
        return rc;
    }

    return opal_info_register_project_frameworks("orte", orte_frameworks, component_map);
}

void orte_info_close_components(void)
{
    int i;

    assert (orte_info_registered);
    if (--orte_info_registered) {
        return;
    }

    for (i=0; NULL != orte_frameworks[i]; i++) {
        (void) mca_base_framework_close(orte_frameworks[i]);
    }

    opal_info_close_components ();
}

void orte_info_show_orte_version(const char *scope)
{
    char *tmp, *tmp2;

    asprintf(&tmp, "%s:version:full", orte_info_type_orte);
    tmp2 = opal_info_make_version_str(scope,
                                      ORTE_MAJOR_VERSION, ORTE_MINOR_VERSION,
                                      ORTE_RELEASE_VERSION,
                                      ORTE_GREEK_VERSION,
                                      ORTE_REPO_REV);
    opal_info_out("Open RTE", tmp, tmp2);
    free(tmp);
    free(tmp2);
    asprintf(&tmp, "%s:version:repo", orte_info_type_orte);
    opal_info_out("Open RTE repo revision", tmp, ORTE_REPO_REV);
    free(tmp);
    asprintf(&tmp, "%s:version:release_date", orte_info_type_orte);
    opal_info_out("Open RTE release date", tmp, ORTE_RELEASE_DATE);
    free(tmp);
}

