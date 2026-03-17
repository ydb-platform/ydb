/*
 * Copyright (c) 2016      Intel, Inc.  All rights reserved.
 * Copyright (c) 2016      Mellanox Technologies Ltd.  All rights reserved.
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

#include "schizo_flux.h"

static orte_schizo_launch_environ_t check_launch_environment(void);
static void finalize(void);

orte_schizo_base_module_t orte_schizo_flux_module = {
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

    /* we were only selected because FLUX was detected
     * and we are an app, so no need to further check
     * that here. Instead, see if we were direct launched
     * vs launched via mpirun */
    if (NULL != orte_process_info.my_daemon_uri) {
        /* nope */
        myenv = ORTE_SCHIZO_NATIVE_LAUNCHED;
        opal_argv_append_nosize(&pushed_envs, OPAL_MCA_PREFIX"ess");
        opal_argv_append_nosize(&pushed_vals, "pmi");
        goto setup;
    }

    myenv = ORTE_SCHIZO_DIRECT_LAUNCHED;
    opal_argv_append_nosize(&pushed_envs, OPAL_MCA_PREFIX"ess");
    opal_argv_append_nosize(&pushed_vals, "pmi");

    /* if we are direct launched by FLUX, then we want
     * to ensure that we do not override their binding
     * options, so set that envar */
    opal_argv_append_nosize(&pushed_envs, OPAL_MCA_PREFIX"hwloc_base_binding_policy");
    opal_argv_append_nosize(&pushed_vals, "none");
    /* indicate we are externally bound so we won't try to do it ourselves */
    opal_argv_append_nosize(&pushed_envs, OPAL_MCA_PREFIX"orte_externally_bound");
    opal_argv_append_nosize(&pushed_vals, "1");

  setup:
      opal_output_verbose(1, orte_schizo_base_framework.framework_output,
                          "schizo:flux DECLARED AS %s", orte_schizo_base_print_env(myenv));
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
