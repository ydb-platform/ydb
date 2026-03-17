/*
 * Copyright (c) 2014-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2015-2016 Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "opal_config.h"
#include "opal/constants.h"

#include "opal/mca/mca.h"
#include "opal/threads/thread_usage.h"
#include "opal/util/argv.h"
#include "opal/util/output.h"
#include "opal/mca/base/base.h"

#include "opal/mca/pmix/pmix.h"
#include "opal/mca/pmix/base/base.h"


/*
 * The following file was created by configure.  It contains extern
 * components and the definition of an array of pointers to each
 * module's public mca_base_module_t struct.
 */

#include "opal/mca/pmix/base/static-components.h"

/* Note that this initializer is important -- do not remove it!  See
   https://github.com/open-mpi/ompi/issues/375 for details. */
opal_pmix_base_module_t opal_pmix = { 0 };
bool opal_pmix_collect_all_data = true;
int opal_pmix_verbose_output = -1;
bool opal_pmix_base_async_modex = false;
opal_pmix_base_t opal_pmix_base = {
    .evbase = NULL,
    .timeout = 0,
    .initialized = 0,
    .lock = {
        .mutex = OPAL_MUTEX_STATIC_INIT,
        .cond = OPAL_PMIX_CONDITION_STATIC_INIT,
        .active = false
    }
};

static int opal_pmix_base_frame_register(mca_base_register_flag_t flags)
{
    opal_pmix_base_async_modex = false;
    (void) mca_base_var_register("opal", "pmix", "base", "async_modex", "Use asynchronous modex mode",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0, OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY, &opal_pmix_base_async_modex);
    opal_pmix_collect_all_data = true;
    (void) mca_base_var_register("opal", "pmix", "base", "collect_data", "Collect all data during modex",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0, OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY, &opal_pmix_collect_all_data);

    opal_pmix_base.timeout = -1;
    (void) mca_base_var_register("opal", "pmix", "base", "exchange_timeout",
                                 "Time (in seconds) to wait for a data exchange to complete",
                                 MCA_BASE_VAR_TYPE_INT, NULL, 0, 0, OPAL_INFO_LVL_3,
                                 MCA_BASE_VAR_SCOPE_READONLY, &opal_pmix_base.timeout);
    return OPAL_SUCCESS;
}

static int opal_pmix_base_frame_close(void)
{
    int rc;

    rc = mca_base_framework_components_close(&opal_pmix_base_framework, NULL);
    /* reset the opal_pmix function pointers to NULL */
    memset(&opal_pmix, 0, sizeof(opal_pmix));
    return rc;
}

static int opal_pmix_base_frame_open(mca_base_open_flag_t flags)
{
    int rc;

    /* Open up all available components */
    rc = mca_base_framework_components_open(&opal_pmix_base_framework, flags);
    /* ensure the function pointers are NULL */
    memset(&opal_pmix, 0, sizeof(opal_pmix));
    /* default to the OPAL event base */
    opal_pmix_base.evbase = opal_sync_event_base;
    /* pass across the verbosity */
    opal_pmix_verbose_output = opal_pmix_base_framework.framework_output;
    return rc;
}

MCA_BASE_FRAMEWORK_DECLARE(opal, pmix, "OPAL PMI Client Framework",
                           opal_pmix_base_frame_register,
                           opal_pmix_base_frame_open,
                           opal_pmix_base_frame_close,
                           mca_pmix_base_static_components, 0);

/****  PMIX FRAMEWORK OBJECTS  ****/
static void lkcon(opal_pmix_pdata_t *p)
{
    p->proc.jobid = OPAL_JOBID_INVALID;
    p->proc.vpid = OPAL_VPID_INVALID;
    OBJ_CONSTRUCT(&p->value, opal_value_t);
}
static void lkdes(opal_pmix_pdata_t *p)
{
    OBJ_DESTRUCT(&p->value);
}
OBJ_CLASS_INSTANCE(opal_pmix_pdata_t,
                   opal_list_item_t,
                   lkcon, lkdes);

static void mdcon(opal_pmix_modex_data_t *p)
{
    p->proc.jobid = OPAL_JOBID_INVALID;
    p->proc.vpid = OPAL_VPID_INVALID;
    p->blob = NULL;
    p->size = 0;
}
static void mddes(opal_pmix_modex_data_t *p)
{
    if (NULL != p->blob) {
        free(p->blob);
    }
}
OBJ_CLASS_INSTANCE(opal_pmix_modex_data_t,
                   opal_list_item_t,
                   mdcon, mddes);

static void apcon(opal_pmix_app_t *p)
{
    p->cmd = NULL;
    p->argv = NULL;
    p->env = NULL;
    p->cwd = NULL;
    p->maxprocs = 0;
    OBJ_CONSTRUCT(&p->info, opal_list_t);
}
static void apdes(opal_pmix_app_t *p)
{
    if (NULL != p->cmd) {
        free(p->cmd);
    }
    if (NULL != p->argv) {
        opal_argv_free(p->argv);
    }
    if (NULL != p->env) {
        opal_argv_free(p->env);
    }
    if (NULL != p->cwd) {
        free(p->cwd);
    }
    OPAL_LIST_DESTRUCT(&p->info);
}
OBJ_CLASS_INSTANCE(opal_pmix_app_t,
                   opal_list_item_t,
                   apcon, apdes);

static void qcon(opal_pmix_query_t *p)
{
    p->keys = NULL;
    OBJ_CONSTRUCT(&p->qualifiers, opal_list_t);
}
static void qdes(opal_pmix_query_t *p)
{
    if (NULL != p->keys) {
        opal_argv_free(p->keys);
    }
    OPAL_LIST_DESTRUCT(&p->qualifiers);
}
OBJ_CLASS_INSTANCE(opal_pmix_query_t,
                   opal_list_item_t,
                   qcon, qdes);
