/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2007 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2014 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011-2012 University of Houston. All rights reserved.
 * Copyright (c) 2010-2015 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "ompi/include/ompi/constants.h"
#include "ompi/include/ompi/frameworks.h"
#include "ompi/communicator/communicator.h"

#include "ompi/runtime/params.h"

#include "opal/runtime/opal_info_support.h"
#include "ompi/runtime/ompi_info_support.h"

#if OMPI_RTE_ORTE
#include "orte/runtime/orte_info_support.h"
#endif

#include "opal/util/show_help.h"

const char *ompi_info_type_ompi = "ompi";
const char *ompi_info_type_base = "base";

static int ompi_info_registered = 0;

void ompi_info_register_types(opal_pointer_array_t *mca_types)
{
    int i;

    /* add the top-level type */
    opal_pointer_array_add(mca_types, (void *)ompi_info_type_ompi);
    opal_pointer_array_add(mca_types, "mpi");

    /* push all the types found by autogen */
    for (i=0; NULL != ompi_frameworks[i]; i++) {
        opal_pointer_array_add(mca_types, ompi_frameworks[i]->framework_name);
    }
}

int ompi_info_register_framework_params(opal_pointer_array_t *component_map)
{
    int rc;

    if (ompi_info_registered++) {
        return OMPI_SUCCESS;
    }

    /* Register the MPI layer's MCA parameters */
    if (OMPI_SUCCESS != (rc = ompi_mpi_register_params())) {
        fprintf(stderr, "ompi_info_register: ompi_mpi_register_params failed\n");
        return rc;
    }

    rc = opal_info_register_framework_params(component_map);
    if (OPAL_SUCCESS != rc) {
        return rc;
    }

#if OMPI_RTE_ORTE
    rc = orte_info_register_framework_params(component_map);
    if (ORTE_SUCCESS != rc) {
        return rc;
    }
#endif

    return opal_info_register_project_frameworks(ompi_info_type_ompi, ompi_frameworks, component_map);
}

void ompi_info_close_components(void)
{
    int i;

    assert(ompi_info_registered);
    if (--ompi_info_registered) {
        return;
    }

    /* Note that the order of shutdown here doesn't matter because
     * we aren't *using* any components -- none were selected, so
     * there are no dependencies between the frameworks.  We list
     * them generally "in order", but it doesn't really matter.

     * We also explicitly ignore the return values from the
     * close() functions -- what would we do if there was an
     * error?
     */
    for (i=0; NULL != ompi_frameworks[i]; i++) {
        (void) mca_base_framework_close(ompi_frameworks[i]);
    }

#if OMPI_RTE_ORTE
    /* close the ORTE components */
    (void) orte_info_close_components();
#endif

    (void) opal_info_close_components();
}

void ompi_info_show_ompi_version(const char *scope)
{
    char *tmp, *tmp2;

    (void)asprintf(&tmp, "%s:version:full", ompi_info_type_ompi);
    tmp2 = opal_info_make_version_str(scope,
                                      OMPI_MAJOR_VERSION, OMPI_MINOR_VERSION,
                                      OMPI_RELEASE_VERSION,
                                      OMPI_GREEK_VERSION,
                                      OMPI_REPO_REV);
    opal_info_out("Open MPI", tmp, tmp2);
    free(tmp);
    free(tmp2);
    (void)asprintf(&tmp, "%s:version:repo", ompi_info_type_ompi);
    opal_info_out("Open MPI repo revision", tmp, OMPI_REPO_REV);
    free(tmp);
    (void)asprintf(&tmp, "%s:version:release_date", ompi_info_type_ompi);
    opal_info_out("Open MPI release date", tmp, OMPI_RELEASE_DATE);
    free(tmp);

#if OMPI_RTE_ORTE
    /* show the orte version */
    orte_info_show_orte_version(scope);
#endif

    /* show the opal version */
    opal_info_show_opal_version(scope);

    tmp2 = opal_info_make_version_str(scope,
                                      MPI_VERSION, MPI_SUBVERSION,
                                      0, "", "");
    opal_info_out("MPI API", "mpi-api:version:full", tmp2);
    free(tmp2);

    opal_info_out("Ident string", "ident", OPAL_IDENT_STRING);
}
