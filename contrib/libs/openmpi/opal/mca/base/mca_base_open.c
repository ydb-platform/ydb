/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2011-2017 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2017      IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include <stdio.h>
#include <string.h>
#ifdef HAVE_SYSLOG_H
#include <syslog.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "opal/mca/installdirs/installdirs.h"
#include "opal/util/output.h"
#include "opal/util/printf.h"
#include "opal/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/mca/base/mca_base_component_repository.h"
#include "opal/constants.h"
#include "opal/util/opal_environ.h"

/*
 * Public variables
 */
char *mca_base_component_path = NULL;
int mca_base_opened = 0;
char *mca_base_system_default_path = NULL;
char *mca_base_user_default_path = NULL;
bool mca_base_component_show_load_errors =
    (bool) OPAL_SHOW_LOAD_ERRORS_DEFAULT;
bool mca_base_component_track_load_errors = false;
bool mca_base_component_disable_dlopen = false;

static char *mca_base_verbose = NULL;

/*
 * Private functions
 */
static void set_defaults(opal_output_stream_t *lds);
static void parse_verbose(char *e, opal_output_stream_t *lds);


/*
 * Main MCA initialization.
 */
int mca_base_open(void)
{
    char *value;
    opal_output_stream_t lds;
    char hostname[OPAL_MAXHOSTNAMELEN];
    int var_id;

    if (mca_base_opened++) {
        return OPAL_SUCCESS;
    }

    /* define the system and user default paths */
#if OPAL_WANT_HOME_CONFIG_FILES
    mca_base_system_default_path = strdup(opal_install_dirs.opallibdir);
    asprintf(&mca_base_user_default_path, "%s"OPAL_PATH_SEP".openmpi"OPAL_PATH_SEP"components", opal_home_directory());
#else
    asprintf(&mca_base_system_default_path, "%s", opal_install_dirs.opallibdir);
#endif

    /* see if the user wants to override the defaults */
    if (NULL == mca_base_user_default_path) {
        value = strdup(mca_base_system_default_path);
    } else {
        asprintf(&value, "%s%c%s", mca_base_system_default_path,
                 OPAL_ENV_SEP, mca_base_user_default_path);
    }

    mca_base_component_path = value;
    var_id = mca_base_var_register("opal", "mca", "base", "component_path",
                                   "Path where to look for additional components",
                                   MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                   OPAL_INFO_LVL_9,
                                   MCA_BASE_VAR_SCOPE_READONLY,
                                   &mca_base_component_path);
    (void) mca_base_var_register_synonym(var_id, "opal", "mca", NULL, "component_path",
                                         MCA_BASE_VAR_SYN_FLAG_DEPRECATED);
    free(value);

    mca_base_component_show_load_errors =
        (bool) OPAL_SHOW_LOAD_ERRORS_DEFAULT;
    var_id = mca_base_var_register("opal", "mca", "base", "component_show_load_errors",
                                   "Whether to show errors for components that failed to load or not",
                                   MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                   OPAL_INFO_LVL_9,
                                   MCA_BASE_VAR_SCOPE_READONLY,
                                   &mca_base_component_show_load_errors);
    (void) mca_base_var_register_synonym(var_id, "opal", "mca", NULL, "component_show_load_errors",
                                         MCA_BASE_VAR_SYN_FLAG_DEPRECATED);

    mca_base_component_track_load_errors = false;
    var_id = mca_base_var_register("opal", "mca", "base", "component_track_load_errors",
                                   "Whether to track errors for components that failed to load or not",
                                   MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                   OPAL_INFO_LVL_9,
                                   MCA_BASE_VAR_SCOPE_READONLY,
                                   &mca_base_component_track_load_errors);

    mca_base_component_disable_dlopen = false;
    var_id = mca_base_var_register("opal", "mca", "base", "component_disable_dlopen",
                                   "Whether to attempt to disable opening dynamic components or not",
                                   MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                   OPAL_INFO_LVL_9,
                                   MCA_BASE_VAR_SCOPE_READONLY,
                                   &mca_base_component_disable_dlopen);
    (void) mca_base_var_register_synonym(var_id, "opal", "mca", NULL, "component_disable_dlopen",
                                         MCA_BASE_VAR_SYN_FLAG_DEPRECATED);

    /* What verbosity level do we want for the default 0 stream? */
    char *str = getenv("OPAL_OUTPUT_INTERNAL_TO_STDOUT");
    if (NULL != str && str[0] == '1') {
        mca_base_verbose = "stdout";
    }
    else {
        mca_base_verbose = "stderr";
    }
    var_id = mca_base_var_register("opal", "mca", "base", "verbose",
                                   "Specifies where the default error output stream goes (this is separate from distinct help messages).  Accepts a comma-delimited list of: stderr, stdout, syslog, syslogpri:<notice|info|debug>, syslogid:<str> (where str is the prefix string for all syslog notices), file[:filename] (if filename is not specified, a default filename is used), fileappend (if not specified, the file is opened for truncation), level[:N] (if specified, integer verbose level; otherwise, 0 is implied)",
                                   MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                   OPAL_INFO_LVL_9,
                                   MCA_BASE_VAR_SCOPE_READONLY,
                                   &mca_base_verbose);
    (void) mca_base_var_register_synonym(var_id, "opal", "mca", NULL, "verbose",
                                         MCA_BASE_VAR_SYN_FLAG_DEPRECATED);

    memset(&lds, 0, sizeof(lds));
    if (NULL != mca_base_verbose) {
        parse_verbose(mca_base_verbose, &lds);
    } else {
        set_defaults(&lds);
    }
    gethostname(hostname, sizeof(hostname));
    asprintf(&lds.lds_prefix, "[%s:%05d] ", hostname, getpid());
    opal_output_reopen(0, &lds);
    opal_output_verbose (MCA_BASE_VERBOSE_COMPONENT, 0, "mca: base: opening components");
    free(lds.lds_prefix);

    /* Open up the component repository */

    return mca_base_component_repository_init();
}


/*
 * Set sane default values for the lds
 */
static void set_defaults(opal_output_stream_t *lds)
{

    /* Load up defaults */

    OBJ_CONSTRUCT(lds, opal_output_stream_t);
#if defined(HAVE_SYSLOG) && defined(HAVE_SYSLOG_H)
    lds->lds_syslog_priority = LOG_INFO;
    lds->lds_syslog_ident = "ompi";
#endif
    lds->lds_want_stderr = true;
}


/*
 * Parse the value of an environment variable describing verbosity
 */
static void parse_verbose(char *e, opal_output_stream_t *lds)
{
    char *edup;
    char *ptr, *next;
    bool have_output = false;

    if (NULL == e) {
        return;
    }

    edup = strdup(e);
    ptr = edup;

    /* Now parse the environment variable */

    while (NULL != ptr && strlen(ptr) > 0) {
        next = strchr(ptr, ',');
        if (NULL != next) {
            *next = '\0';
        }

        if (0 == strcasecmp(ptr, "syslog")) {
#if defined(HAVE_SYSLOG) && defined(HAVE_SYSLOG_H)
            lds->lds_want_syslog = true;
            have_output = true;
#else
            opal_output(0, "syslog support requested but not available on this system");
#endif  /* defined(HAVE_SYSLOG) && defined(HAVE_SYSLOG_H) */
        }
        else if (strncasecmp(ptr, "syslogpri:", 10) == 0) {
#if defined(HAVE_SYSLOG) && defined(HAVE_SYSLOG_H)
            lds->lds_want_syslog = true;
            have_output = true;
            if (strcasecmp(ptr + 10, "notice") == 0)
                lds->lds_syslog_priority = LOG_NOTICE;
            else if (strcasecmp(ptr + 10, "INFO") == 0)
                lds->lds_syslog_priority = LOG_INFO;
            else if (strcasecmp(ptr + 10, "DEBUG") == 0)
                lds->lds_syslog_priority = LOG_DEBUG;
#else
            opal_output(0, "syslog support requested but not available on this system");
#endif  /* defined(HAVE_SYSLOG) && defined(HAVE_SYSLOG_H) */
        } else if (strncasecmp(ptr, "syslogid:", 9) == 0) {
#if defined(HAVE_SYSLOG) && defined(HAVE_SYSLOG_H)
            lds->lds_want_syslog = true;
            lds->lds_syslog_ident = ptr + 9;
#else
            opal_output(0, "syslog support requested but not available on this system");
#endif  /* defined(HAVE_SYSLOG) && defined(HAVE_SYSLOG_H) */
        }

        else if (strcasecmp(ptr, "stdout") == 0) {
            lds->lds_want_stdout = true;
            have_output = true;
        } else if (strcasecmp(ptr, "stderr") == 0) {
            lds->lds_want_stderr = true;
            have_output = true;
        }

        else if (strcasecmp(ptr, "file") == 0 || strcasecmp(ptr, "file:") == 0) {
            lds->lds_want_file = true;
            have_output = true;
        } else if (strncasecmp(ptr, "file:", 5) == 0) {
            lds->lds_want_file = true;
            lds->lds_file_suffix = strdup(ptr + 5);
            have_output = true;
        } else if (strcasecmp(ptr, "fileappend") == 0) {
            lds->lds_want_file = true;
            lds->lds_want_file_append = 1;
            have_output = true;
        }

        else if (strncasecmp(ptr, "level", 5) == 0) {
            lds->lds_verbose_level = 0;
            if (ptr[5] == OPAL_ENV_SEP)
                lds->lds_verbose_level = atoi(ptr + 6);
        }

        if (NULL == next) {
            break;
        }
        ptr = next + 1;
    }

    /* If we didn't get an output, default to stderr */

    if (!have_output) {
        lds->lds_want_stderr = true;
    }

    /* All done */

    free(edup);
}
