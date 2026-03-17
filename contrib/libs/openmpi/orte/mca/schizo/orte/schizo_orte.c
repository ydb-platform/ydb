/*
 * Copyright (c) 2016-2017 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

#include "orte_config.h"
#include "orte/types.h"
#include "opal/types.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <ctype.h>

#include "opal/util/argv.h"
#include "opal/util/basename.h"
#include "opal/util/opal_environ.h"

#include "orte/runtime/orte_globals.h"
#include "orte/util/name_fns.h"
#include "orte/mca/schizo/base/base.h"

#include "schizo_orte.h"

static orte_schizo_launch_environ_t check_launch_environment(void);
static void finalize(void);

orte_schizo_base_module_t orte_schizo_orte_module = {
    .check_launch_environment = check_launch_environment,
    .finalize = finalize
};

static char **pushed_envs = NULL;
static char **pushed_vals = NULL;
static orte_schizo_launch_environ_t myenv;
static bool myenvdefined = false;

static orte_schizo_launch_environ_t check_launch_environment(void)
{
    int i;

    if (myenvdefined) {
        return myenv;
    }
    myenvdefined = true;

    /* we were only selected because we are an app,
     * so no need to further check that here. Instead,
     * see if we were direct launched vs launched via mpirun */
    if (NULL != orte_process_info.my_daemon_uri) {
        /* yes we were */
        myenv = ORTE_SCHIZO_NATIVE_LAUNCHED;
        opal_argv_append_nosize(&pushed_envs, OPAL_MCA_PREFIX"ess");
        opal_argv_append_nosize(&pushed_vals, "pmi");
        goto setup;
    }

    /* if nobody else has laid claim to this process,
     * then it must be a singleton */
    myenv = ORTE_SCHIZO_UNMANAGED_SINGLETON;
    opal_argv_append_nosize(&pushed_envs, OPAL_MCA_PREFIX"ess");
    opal_argv_append_nosize(&pushed_vals, "singleton");
    /* mark that we are in ORTE */
    opal_argv_append_nosize(&pushed_envs, "ORTE_SCHIZO_DETECTION");
    opal_argv_append_nosize(&pushed_vals, "ORTE");


  setup:
    opal_output_verbose(1, orte_schizo_base_framework.framework_output,
                        "schizo:orte DECLARED AS %s", orte_schizo_base_print_env(myenv));
    if (NULL != pushed_envs) {
        for (i=0; NULL != pushed_envs[i]; i++) {
            opal_setenv(pushed_envs[i], pushed_vals[i], true, &environ);
        }
    }
    return myenv;
}

static void finalize(void)
{
    int i;

    if (NULL != pushed_envs) {
        for (i=0; NULL != pushed_envs[i]; i++) {
            opal_unsetenv(pushed_envs[i], &environ);
        }
        opal_argv_free(pushed_envs);
        opal_argv_free(pushed_vals);
    }
}
