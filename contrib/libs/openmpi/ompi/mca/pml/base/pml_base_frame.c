/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2007 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2013-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
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

#include <string.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif  /* HAVE_UNIST_H */
#include "ompi/mca/mca.h"
#include "opal/util/output.h"
#include "opal/mca/base/base.h"


#include "ompi/constants.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/mca/pml/base/base.h"
#include "ompi/mca/pml/base/pml_base_request.h"

/*
 * The following file was created by configure.  It contains extern
 * statements and the definition of an array of pointers to each
 * component's public mca_base_component_t struct.
 */

#include "ompi/mca/pml/base/static-components.h"

int mca_pml_base_progress(void)
{
    return OMPI_SUCCESS;
}

#define xstringify(pml) #pml
#define stringify(pml) xstringify(pml)

/*
 * Global variables
 */
mca_pml_base_module_t mca_pml = {
    NULL,                    /* pml_add_procs */
    NULL,                    /* pml_del_procs */
    NULL,                    /* pml_enable */
    mca_pml_base_progress,   /* pml_progress */
    NULL,                    /* pml_add_comm */
    NULL,                    /* pml_del_comm */
    NULL,                    /* pml_irecv_init */
    NULL,                    /* pml_irecv */
    NULL,                    /* pml_recv */
    NULL,                    /* pml_isend_init */
    NULL,                    /* pml_isend */
    NULL,                    /* pml_send */
    NULL,                    /* pml_iprobe */
    NULL,                    /* pml_probe */
    NULL,                    /* pml_start */
    NULL,                    /* pml_dump */
    NULL,                    /* pml_ft_event */
    0,                       /* pml_max_contextid */
    0                        /* pml_max_tag */
};

mca_pml_base_component_t mca_pml_base_selected_component = {{0}};
opal_pointer_array_t mca_pml_base_pml = {{0}};
char *ompi_pml_base_bsend_allocator_name = NULL;

#if !MCA_ompi_pml_DIRECT_CALL && OPAL_ENABLE_FT_CR == 1
static char *ompi_pml_base_wrapper = NULL;
#endif

static int mca_pml_base_register(mca_base_register_flag_t flags)
{
#if !MCA_ompi_pml_DIRECT_CALL && OPAL_ENABLE_FT_CR == 1
    int var_id;
#endif

    ompi_pml_base_bsend_allocator_name = "basic";
    (void) mca_base_var_register("ompi", "pml", "base", "bsend_allocator", NULL,
                                 MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &ompi_pml_base_bsend_allocator_name);

#if !MCA_ompi_pml_DIRECT_CALL && OPAL_ENABLE_FT_CR == 1
    ompi_pml_base_wrapper = NULL;
    var_id = mca_base_var_register("ompi", "pml", "base", "wrapper",
                                   "Use a Wrapper component around the selected PML component",
                                   MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                   OPAL_INFO_LVL_9,
                                   MCA_BASE_VAR_SCOPE_READONLY,
                                   &ompi_pml_base_wrapper);
    (void) mca_base_var_register_synonym(var_id, "ompi", "pml", NULL, "wrapper", 0);
#endif

    return OMPI_SUCCESS;
}

int mca_pml_base_finalize(void) {
  if (NULL != mca_pml_base_selected_component.pmlm_finalize) {
    return mca_pml_base_selected_component.pmlm_finalize();
  }
  return OMPI_SUCCESS;
}


static int mca_pml_base_close(void)
{
    int i, j;

    /* turn off the progress code for the pml */
    if( NULL != mca_pml.pml_progress ) {
        opal_progress_unregister(mca_pml.pml_progress);
    }

    /* Blatently ignore the return code (what would we do to recover,
       anyway?  This module is going away, so errors don't matter
       anymore) */

    /**
     * Destruct the send and receive queues. The opal_free_list_t destructor
     * will return the memory to the mpool, so this has to be done before the
     * mpool get released by the PML close function.
     */
    OBJ_DESTRUCT(&mca_pml_base_send_requests);
    OBJ_DESTRUCT(&mca_pml_base_recv_requests);

    mca_pml.pml_progress = mca_pml_base_progress;

    /* Free all the strings in the array */
    j = opal_pointer_array_get_size(&mca_pml_base_pml);
    for (i = 0; i < j; ++i) {
        char *str;
        str = (char*) opal_pointer_array_get_item(&mca_pml_base_pml, i);
        free(str);
    }
    OBJ_DESTRUCT(&mca_pml_base_pml);

    /* Close all remaining available components */
    return mca_base_framework_components_close(&ompi_pml_base_framework, NULL);
}

/**
 * Function for finding and opening either all MCA components, or the one
 * that was specifically requested via a MCA parameter.
 */
static int mca_pml_base_open(mca_base_open_flag_t flags)
{
    /**
     * Construct the send and receive request queues. There are 2 reasons to do it
     * here. First, as they are globals it's better to construct them in one common
     * place. Second, in order to be able to allow the external debuggers to show
     * their content, they should get constructed as soon as possible once the MPI
     * process is started.
     */
    OBJ_CONSTRUCT(&mca_pml_base_send_requests, opal_free_list_t);
    OBJ_CONSTRUCT(&mca_pml_base_recv_requests, opal_free_list_t);

    OBJ_CONSTRUCT(&mca_pml_base_pml, opal_pointer_array_t);

    /* Open up all available components */

    if (OPAL_SUCCESS !=
        mca_base_framework_components_open(&ompi_pml_base_framework, flags)) {
        return OMPI_ERROR;
    }

    /* Set a sentinel in case we don't select any components (e.g.,
       ompi_info) */

    mca_pml_base_selected_component.pmlm_finalize = NULL;

    /**
     * Right now our selection of BTLs is completely broken. If we have
     * multiple PMLs that use BTLs than we will open all BTLs several times, leading to
     * undefined behaviors. The simplest solution, at least until we
     * figure out the correct way to do it, is to force a default PML that
     * uses BTLs and any other PMLs that do not in the mca_pml_base_pml array.
     */

#if MCA_ompi_pml_DIRECT_CALL
    opal_pointer_array_add(&mca_pml_base_pml,
                           strdup(stringify(MCA_ompi_pml_DIRECT_CALL_COMPONENT)));
#else
    {
        const char **default_pml = NULL;
        int var_id;

        var_id = mca_base_var_find("ompi", "pml", NULL, NULL);
        mca_base_var_get_value(var_id, &default_pml, NULL, NULL);

        if( (NULL == default_pml || NULL == default_pml[0] ||
             0 == strlen(default_pml[0])) || (default_pml[0][0] == '^') ) {
            opal_pointer_array_add(&mca_pml_base_pml, strdup("ob1"));
            opal_pointer_array_add(&mca_pml_base_pml, strdup("yalla"));
            opal_pointer_array_add(&mca_pml_base_pml, strdup("ucx"));
            opal_pointer_array_add(&mca_pml_base_pml, strdup("cm"));
        } else {
            opal_pointer_array_add(&mca_pml_base_pml, strdup(default_pml[0]));
        }
    }
#if OPAL_ENABLE_FT_CR == 1
    /*
     * Which PML Wrapper component to use, if any
     *  - NULL or "" = No wrapper
     *  - ow. select that specific wrapper component
     */
    if( NULL != ompi_pml_base_wrapper) {
        opal_pointer_array_add(&mca_pml_base_pml, ompi_pml_base_wrapper);
    }
#endif

#endif

    return OMPI_SUCCESS;

}

MCA_BASE_FRAMEWORK_DECLARE(ompi, pml, "OMPI PML", mca_pml_base_register,
                           mca_pml_base_open, mca_pml_base_close,
                           mca_pml_base_static_components, 0);
