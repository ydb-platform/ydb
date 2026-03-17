/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2014-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2014-2015 Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "pmix_config.h"

#include <stdio.h>
#include <string.h>

#include "src/util/cmd_line.h"
#include "src/util/argv.h"
#include "src/util/pmix_environ.h"
#include "src/util/show_help.h"
#include "src/mca/base/base.h"
#include "pmix_common.h"


/*
 * Private variables
 */

/*
 * Private functions
 */
static int process_arg(const char *param, const char *value,
                       char ***params, char ***values);
static void add_to_env(char **params, char **values, char ***env);


/*
 * Add -mca to the possible command line options list
 */
int pmix_mca_base_cmd_line_setup(pmix_cmd_line_t *cmd)
{
    int ret = PMIX_SUCCESS;

    ret = pmix_cmd_line_make_opt3(cmd, '\0', PMIX_MCA_CMD_LINE_ID, PMIX_MCA_CMD_LINE_ID, 2,
                                  "Pass context-specific MCA parameters; they are considered global if --g"PMIX_MCA_CMD_LINE_ID" is not used and only one context is specified (arg0 is the parameter name; arg1 is the parameter value)");
    if (PMIX_SUCCESS != ret) {
        return ret;
    }

    ret = pmix_cmd_line_make_opt3(cmd, '\0', "g"PMIX_MCA_CMD_LINE_ID, "g"PMIX_MCA_CMD_LINE_ID, 2,
                                  "Pass global MCA parameters that are applicable to all contexts (arg0 is the parameter name; arg1 is the parameter value)");

    if (PMIX_SUCCESS != ret) {
        return ret;
    }

    {
        pmix_cmd_line_init_t entry =
            {"mca_base_param_file_prefix", '\0', "am", NULL, 1,
             NULL, PMIX_CMD_LINE_TYPE_STRING,
             "Aggregate MCA parameter set file list",
             PMIX_CMD_LINE_OTYPE_LAUNCH
            };
        ret = pmix_cmd_line_make_opt_mca(cmd, entry);
        if (PMIX_SUCCESS != ret) {
            return ret;
        }
    }

    {
        pmix_cmd_line_init_t entry =
            {"mca_base_envar_file_prefix", '\0', "tune", NULL, 1,
             NULL, PMIX_CMD_LINE_TYPE_STRING,
             "Application profile options file list",
             PMIX_CMD_LINE_OTYPE_DEBUG
            };
        ret = pmix_cmd_line_make_opt_mca(cmd, entry);
        if (PMIX_SUCCESS != ret) {
            return ret;
        }
    }

    return ret;
}


/*
 * Look for and handle any -mca options on the command line
 */
int pmix_mca_base_cmd_line_process_args(pmix_cmd_line_t *cmd,
                                        char ***context_env, char ***global_env)
{
    int i, num_insts, rc;
    char **params;
    char **values;

    /* If no relevant parameters were given, just return */

    if (!pmix_cmd_line_is_taken(cmd, PMIX_MCA_CMD_LINE_ID) &&
        !pmix_cmd_line_is_taken(cmd, "g"PMIX_MCA_CMD_LINE_ID)) {
        return PMIX_SUCCESS;
    }

    /* Handle app context-specific parameters */

    num_insts = pmix_cmd_line_get_ninsts(cmd, PMIX_MCA_CMD_LINE_ID);
    params = values = NULL;
    for (i = 0; i < num_insts; ++i) {
        if (PMIX_SUCCESS != (rc = process_arg(pmix_cmd_line_get_param(cmd, PMIX_MCA_CMD_LINE_ID, i, 0),
                                              pmix_cmd_line_get_param(cmd, PMIX_MCA_CMD_LINE_ID, i, 1),
                                              &params, &values))) {
            return rc;
        }
    }
    if (NULL != params) {
        add_to_env(params, values, context_env);
        pmix_argv_free(params);
        pmix_argv_free(values);
    }

    /* Handle global parameters */

    num_insts = pmix_cmd_line_get_ninsts(cmd, "g"PMIX_MCA_CMD_LINE_ID);
    params = values = NULL;
    for (i = 0; i < num_insts; ++i) {
        if (PMIX_SUCCESS != (rc = process_arg(pmix_cmd_line_get_param(cmd, "g"PMIX_MCA_CMD_LINE_ID, i, 0),
                                              pmix_cmd_line_get_param(cmd, "g"PMIX_MCA_CMD_LINE_ID, i, 1),
                                              &params, &values))) {
            return rc;
        }
    }
    if (NULL != params) {
        add_to_env(params, values, global_env);
        pmix_argv_free(params);
        pmix_argv_free(values);
    }

    /* All done */

    return PMIX_SUCCESS;
}



/*
 * Process a single MCA argument.
 */
static int process_arg(const char *param, const char *value,
                       char ***params, char ***values)
{
    int i;
    char *p1;

    /* check for quoted value */
    if ('\"' == value[0] && '\"' == value[strlen(value)-1]) {
        p1 = strdup(&value[1]);
        p1[strlen(p1)-1] = '\0';
    } else {
        p1 = strdup(value);
    }

    /* Look to see if we've already got an -mca argument for the same
       param.  Check against the list of MCA param's that we've
       already saved arguments for - if found, return an error. */

    for (i = 0; NULL != *params && NULL != (*params)[i]; ++i) {
        if (0 == strcmp(param, (*params)[i])) {
            /* cannot use show_help here as it may not get out prior
             * to the process exiting */
            fprintf(stderr,
                    "---------------------------------------------------------------------------\n"
                    "The following MCA parameter has been listed multiple times on the\n"
                    "command line:\n\n"
                    "  MCA param:   %s\n\n"
                    "MCA parameters can only be listed once on a command line to ensure there\n"
                    "is no ambiguity as to its value.  Please correct the situation and\n"
                    "try again.\n"
                    "---------------------------------------------------------------------------\n",
                    param);
            free(p1);
            return PMIX_ERROR;
        }
    }

    /* If we didn't already have an value for the same param, save
       this one away */
    pmix_argv_append_nosize(params, param);
    pmix_argv_append_nosize(values, p1);
    free(p1);

    return PMIX_SUCCESS;
}


static void add_to_env(char **params, char **values, char ***env)
{
    int i;
    char *name;

    /* Loop through all the args that we've gotten and make env
       vars of the form PMIX_MCA_PREFIX*=value. */

    for (i = 0; NULL != params && NULL != params[i]; ++i) {
        (void) pmix_mca_base_var_env_name (params[i], &name);
        pmix_setenv(name, values[i], true, env);
        free(name);
    }
}

void pmix_mca_base_cmd_line_wrap_args(char **args)
{
    int i;
    char *tstr;

    for (i=0; NULL != args && NULL != args[i]; i++) {
        if (0 == strcmp(args[i], "-"PMIX_MCA_CMD_LINE_ID) ||
            0 == strcmp(args[i], "--"PMIX_MCA_CMD_LINE_ID)) {
            if (NULL == args[i+1] || NULL == args[i+2]) {
                /* this should be impossible as the error would
                 * have been detected well before here, but just
                 * be safe */
                return;
            }
            i += 2;
            if (0 > asprintf(&tstr, "\"%s\"", args[i])) {
                return;
            }
            free(args[i]);
            args[i] = tstr;
        }
    }
}
