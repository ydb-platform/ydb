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
 * Copyright (c) 2014      Intel, Inc. All rights reserved.
 * Copyright (c) 2014-2015 Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include <stdio.h>
#include <string.h>

#include "opal/util/cmd_line.h"
#include "opal/util/argv.h"
#include "opal/util/opal_environ.h"
#include "opal/util/show_help.h"
#include "opal/mca/base/base.h"
#include "opal/constants.h"


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
int mca_base_cmd_line_setup(opal_cmd_line_t *cmd)
{
    int ret = OPAL_SUCCESS;

    ret = opal_cmd_line_make_opt3(cmd, '\0', OPAL_MCA_CMD_LINE_ID, OPAL_MCA_CMD_LINE_ID, 2,
                                  "Pass context-specific MCA parameters; they are considered global if --g"OPAL_MCA_CMD_LINE_ID" is not used and only one context is specified (arg0 is the parameter name; arg1 is the parameter value)");
    if (OPAL_SUCCESS != ret) {
        return ret;
    }

    ret = opal_cmd_line_make_opt3(cmd, '\0', "g"OPAL_MCA_CMD_LINE_ID, "g"OPAL_MCA_CMD_LINE_ID, 2,
                                  "Pass global MCA parameters that are applicable to all contexts (arg0 is the parameter name; arg1 is the parameter value)");

    if (OPAL_SUCCESS != ret) {
        return ret;
    }

    {
        opal_cmd_line_init_t entry =
            {"mca_base_param_file_prefix", '\0', "am", NULL, 1,
             NULL, OPAL_CMD_LINE_TYPE_STRING,
             "Aggregate MCA parameter set file list",
             OPAL_CMD_LINE_OTYPE_LAUNCH
            };
        ret = opal_cmd_line_make_opt_mca(cmd, entry);
        if (OPAL_SUCCESS != ret) {
            return ret;
        }
    }

    {
        opal_cmd_line_init_t entry =
            {"mca_base_envar_file_prefix", '\0', "tune", NULL, 1,
             NULL, OPAL_CMD_LINE_TYPE_STRING,
             "Application profile options file list",
             OPAL_CMD_LINE_OTYPE_DEBUG
            };
        ret = opal_cmd_line_make_opt_mca(cmd, entry);
        if (OPAL_SUCCESS != ret) {
            return ret;
        }
    }

    return ret;
}


/*
 * Look for and handle any -mca options on the command line
 */
int mca_base_cmd_line_process_args(opal_cmd_line_t *cmd,
                                   char ***context_env, char ***global_env)
{
    int i, num_insts, rc;
    char **params;
    char **values;

    /* If no relevant parameters were given, just return */

    if (!opal_cmd_line_is_taken(cmd, OPAL_MCA_CMD_LINE_ID) &&
        !opal_cmd_line_is_taken(cmd, "g"OPAL_MCA_CMD_LINE_ID)) {
        return OPAL_SUCCESS;
    }

    /* Handle app context-specific parameters */

    num_insts = opal_cmd_line_get_ninsts(cmd, OPAL_MCA_CMD_LINE_ID);
    params = values = NULL;
    for (i = 0; i < num_insts; ++i) {
        if (OPAL_SUCCESS != (rc = process_arg(opal_cmd_line_get_param(cmd, OPAL_MCA_CMD_LINE_ID, i, 0),
                                              opal_cmd_line_get_param(cmd, OPAL_MCA_CMD_LINE_ID, i, 1),
                                              &params, &values))) {
            return rc;
        }
    }
    if (NULL != params) {
        add_to_env(params, values, context_env);
        opal_argv_free(params);
        opal_argv_free(values);
    }

    /* Handle global parameters */

    num_insts = opal_cmd_line_get_ninsts(cmd, "g"OPAL_MCA_CMD_LINE_ID);
    params = values = NULL;
    for (i = 0; i < num_insts; ++i) {
        if (OPAL_SUCCESS != (rc = process_arg(opal_cmd_line_get_param(cmd, "g"OPAL_MCA_CMD_LINE_ID, i, 0),
                                              opal_cmd_line_get_param(cmd, "g"OPAL_MCA_CMD_LINE_ID, i, 1),
                                              &params, &values))) {
            return rc;
        }
    }
    if (NULL != params) {
        add_to_env(params, values, global_env);
        opal_argv_free(params);
        opal_argv_free(values);
    }

    /* All done */

    return OPAL_SUCCESS;
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
            return OPAL_ERROR;
        }
    }

    /* If we didn't already have an value for the same param, save
       this one away */
    opal_argv_append_nosize(params, param);
    opal_argv_append_nosize(values, p1);
    free(p1);

    return OPAL_SUCCESS;
}


static void add_to_env(char **params, char **values, char ***env)
{
    int i;
    char *name;

    /* Loop through all the args that we've gotten and make env
       vars of the form OPAL_MCA_PREFIX*=value. */

    for (i = 0; NULL != params && NULL != params[i]; ++i) {
        (void) mca_base_var_env_name (params[i], &name);
        opal_setenv(name, values[i], true, env);
        free(name);
    }
}

void mca_base_cmd_line_wrap_args(char **args)
{
    int i;
    char *tstr;

    for (i=0; NULL != args && NULL != args[i]; i++) {
        if (0 == strcmp(args[i], "-"OPAL_MCA_CMD_LINE_ID) ||
            0 == strcmp(args[i], "--"OPAL_MCA_CMD_LINE_ID)) {
            if (NULL == args[i+1] || NULL == args[i+2]) {
                /* this should be impossible as the error would
                 * have been detected well before here, but just
                 * be safe */
                return;
            }
            i += 2;
            asprintf(&tstr, "\"%s\"", args[i]);
            free(args[i]);
            args[i] = tstr;
        }
    }
}
