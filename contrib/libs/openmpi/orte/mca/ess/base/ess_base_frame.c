/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2011-2017 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2012      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2017      Intel, Inc. All rights reserved.
 * Copyright (c) 2017      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "orte_config.h"
#include "orte/constants.h"

#include <signal.h>

#include "orte/mca/mca.h"
#include "opal/util/argv.h"
#include "opal/util/output.h"
#include "opal/mca/base/base.h"
#include "orte/util/show_help.h"

#include "orte/mca/ess/base/base.h"

/*
 * The following file was created by configure.  It contains extern
 * statements and the definition of an array of pointers to each
 * module's public mca_base_module_t struct.
 */

#include "orte/mca/ess/base/static-components.h"

orte_ess_base_module_t orte_ess = {
    NULL,  /* init */
    NULL,  /* finalize */
    NULL,  /* abort */
    NULL   /* ft_event */
};
int orte_ess_base_std_buffering = -1;
int orte_ess_base_num_procs = -1;
char *orte_ess_base_jobid = NULL;
char *orte_ess_base_vpid = NULL;
opal_list_t orte_ess_base_signals = {{0}};

static mca_base_var_enum_value_t stream_buffering_values[] = {
  {-1, "default"},
  {0, "unbuffered"},
  {1, "line_buffered"},
  {2, "fully_buffered"},
  {0, NULL}
};

static int setup_signals(void);
static char *forwarded_signals = NULL;

static int orte_ess_base_register(mca_base_register_flag_t flags)
{
    mca_base_var_enum_t *new_enum;
    int ret;

    orte_ess_base_std_buffering = -1;
    (void) mca_base_var_enum_create("ess_base_stream_buffering", stream_buffering_values, &new_enum);
    (void) mca_base_var_register("orte", "ess", "base", "stream_buffering",
                                 "Adjust buffering for stdout/stderr "
                                "[-1 system default] [0 unbuffered] [1 line buffered] [2 fully buffered] "
                                 "(Default: -1)",
                                 MCA_BASE_VAR_TYPE_INT, new_enum, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY, &orte_ess_base_std_buffering);
    OBJ_RELEASE(new_enum);

    orte_ess_base_jobid = NULL;
    ret = mca_base_var_register("orte", "ess", "base", "jobid", "Process jobid",
                                MCA_BASE_VAR_TYPE_STRING, NULL, 0,
                                MCA_BASE_VAR_FLAG_INTERNAL,
                                OPAL_INFO_LVL_9,
                                MCA_BASE_VAR_SCOPE_READONLY, &orte_ess_base_jobid);
    mca_base_var_register_synonym(ret, "orte", "orte", "ess", "jobid", 0);

    orte_ess_base_vpid = NULL;
    ret = mca_base_var_register("orte", "ess", "base", "vpid", "Process vpid",
                                MCA_BASE_VAR_TYPE_STRING, NULL, 0,
                                MCA_BASE_VAR_FLAG_INTERNAL,
                                OPAL_INFO_LVL_9,
                                MCA_BASE_VAR_SCOPE_READONLY, &orte_ess_base_vpid);
    mca_base_var_register_synonym(ret, "orte", "orte", "ess", "vpid", 0);

    orte_ess_base_num_procs = -1;
    ret = mca_base_var_register("orte", "ess", "base", "num_procs",
                                "Used to discover the number of procs in the job",
                                MCA_BASE_VAR_TYPE_INT, NULL, 0,
                                MCA_BASE_VAR_FLAG_INTERNAL,
                                OPAL_INFO_LVL_9,
                                MCA_BASE_VAR_SCOPE_READONLY, &orte_ess_base_num_procs);
    mca_base_var_register_synonym(ret, "orte", "orte", "ess", "num_procs", 0);

    forwarded_signals = NULL;
    ret = mca_base_var_register ("orte", "ess", "base", "forward_signals",
                                 "Comma-delimited list of additional signals (names or integers) to forward to "
                                 "application processes [\"none\" => forward nothing]. Signals provided by "
                                 "default include SIGTSTP, SIGUSR1, SIGUSR2, SIGABRT, SIGALRM, and SIGCONT",
                                 MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                 OPAL_INFO_LVL_4, MCA_BASE_VAR_SCOPE_READONLY,
                                 &forwarded_signals);
    mca_base_var_register_synonym(ret, "orte", "ess", "hnp", "forward_signals", 0);


    return ORTE_SUCCESS;
}

static int orte_ess_base_close(void)
{
    OPAL_LIST_DESTRUCT(&orte_ess_base_signals);

    return mca_base_framework_components_close(&orte_ess_base_framework, NULL);
}

static int orte_ess_base_open(mca_base_open_flag_t flags)
{
    int rc;

    OBJ_CONSTRUCT(&orte_ess_base_signals, opal_list_t);

    if (ORTE_PROC_IS_HNP || ORTE_PROC_IS_DAEMON) {
        if (ORTE_SUCCESS != (rc = setup_signals())) {
            return rc;
        }
    }
    return mca_base_framework_components_open(&orte_ess_base_framework, flags);
}

MCA_BASE_FRAMEWORK_DECLARE(orte, ess, "ORTE Environmenal System Setup",
                           orte_ess_base_register, orte_ess_base_open, orte_ess_base_close,
                           mca_ess_base_static_components, 0);

/* signal forwarding */

/* setup signal forwarding list */
struct known_signal {
    /** signal number */
    int signal;
    /** signal name */
    char *signame;
    /** can this signal be forwarded */
    bool can_forward;
};

static struct known_signal known_signals[] = {
    {SIGTERM, "SIGTERM", false},
    {SIGHUP, "SIGHUP", false},
    {SIGINT, "SIGINT", false},
    {SIGKILL, "SIGKILL", false},
    {SIGPIPE, "SIGPIPE", false},
#ifdef SIGQUIT
    {SIGQUIT, "SIGQUIT", false},
#endif
#ifdef SIGTRAP
    {SIGTRAP, "SIGTRAP", true},
#endif
#ifdef SIGTSTP
    {SIGTSTP, "SIGTSTP", true},
#endif
#ifdef SIGABRT
    {SIGABRT, "SIGABRT", true},
#endif
#ifdef SIGCONT
    {SIGCONT, "SIGCONT", true},
#endif
#ifdef SIGSYS
    {SIGSYS, "SIGSYS", true},
#endif
#ifdef SIGXCPU
    {SIGXCPU, "SIGXCPU", true},
#endif
#ifdef SIGXFSZ
    {SIGXFSZ, "SIGXFSZ", true},
#endif
#ifdef SIGALRM
    {SIGALRM, "SIGALRM", true},
#endif
#ifdef SIGVTALRM
    {SIGVTALRM, "SIGVTALRM", true},
#endif
#ifdef SIGPROF
    {SIGPROF, "SIGPROF", true},
#endif
#ifdef SIGINFO
    {SIGINFO, "SIGINFO", true},
#endif
#ifdef SIGPWR
    {SIGPWR, "SIGPWR", true},
#endif
#ifdef SIGURG
    {SIGURG, "SIGURG", true},
#endif
#ifdef SIGUSR1
    {SIGUSR1, "SIGUSR1", true},
#endif
#ifdef SIGUSR2
    {SIGUSR2, "SIGUSR2", true},
#endif
    {0, NULL},
};

#define ESS_ADDSIGNAL(x, s)                                                 \
    do {                                                                    \
        orte_ess_base_signal_t *_sig;                                       \
        _sig = OBJ_NEW(orte_ess_base_signal_t);                             \
        _sig->signal = (x);                                                 \
        _sig->signame = strdup((s));                                        \
        opal_list_append(&orte_ess_base_signals, &_sig->super);             \
    } while(0)

static int setup_signals(void)
{
    int i, sval, nsigs;
    char **signals, *tmp;
    orte_ess_base_signal_t *sig;
    bool ignore, found;

    /* if they told us "none", then nothing to do */
    if (NULL != forwarded_signals &&
        0 == strcmp(forwarded_signals, "none")) {
        return ORTE_SUCCESS;
    }

    /* we know that some signals are (nearly) always defined, regardless
     * of environment, so add them here */
    nsigs = sizeof(known_signals) / sizeof(struct known_signal);
    for (i=0; i < nsigs; i++) {
        if (known_signals[i].can_forward) {
            ESS_ADDSIGNAL(known_signals[i].signal, known_signals[i].signame);
        }
    }

    /* see if they asked for anything beyond those - note that they may
     * have asked for some we already cover, and so we ignore any duplicates */
    if (NULL != forwarded_signals) {
        /* if they told us "none", then dump the list */
        signals = opal_argv_split(forwarded_signals, ',');
        for (i=0; NULL != signals[i]; i++) {
            sval = 0;
            if (0 != strncmp(signals[i], "SIG", 3)) {
                /* treat it like a number */
                errno = 0;
                sval = strtoul(signals[i], &tmp, 10);
                if (0 != errno || '\0' != *tmp) {
                    orte_show_help("help-ess-base.txt", "ess-base:unknown-signal",
                                   true, signals[i], forwarded_signals);
                    opal_argv_free(signals);
                    return OPAL_ERR_SILENT;
                }
            }

            /* see if it is one we already covered */
            ignore = false;
            OPAL_LIST_FOREACH(sig, &orte_ess_base_signals, orte_ess_base_signal_t) {
                if (0 == strcasecmp(signals[i], sig->signame) || sval == sig->signal) {
                    /* got it - we will ignore */
                    ignore = true;
                    break;
                }
            }

            if (ignore) {
                continue;
            }

            /* see if they gave us a signal name */
            found = false;
            for (int j = 0 ; known_signals[j].signame ; ++j) {
                if (0 == strcasecmp (signals[i], known_signals[j].signame) || sval == known_signals[j].signal) {
                    if (!known_signals[j].can_forward) {
                        orte_show_help("help-ess-base.txt", "ess-base:cannot-forward",
                                       true, known_signals[j].signame, forwarded_signals);
                        opal_argv_free(signals);
                        return OPAL_ERR_SILENT;
                    }
                    found = true;
                    ESS_ADDSIGNAL(known_signals[j].signal, known_signals[j].signame);
                    break;
                }
            }

            if (!found) {
                if (0 == strncmp(signals[i], "SIG", 3)) {
                    orte_show_help("help-ess-base.txt", "ess-base:unknown-signal",
                                   true, signals[i], forwarded_signals);
                    opal_argv_free(signals);
                    return OPAL_ERR_SILENT;
                }

                ESS_ADDSIGNAL(sval, signals[i]);
            }
        }
        opal_argv_free (signals);
    }
    return ORTE_SUCCESS;
}

/* instantiate the class */
static void scon(orte_ess_base_signal_t *t)
{
    t->signame = NULL;
}
static void sdes(orte_ess_base_signal_t *t)
{
    if (NULL != t->signame) {
        free(t->signame);
    }
}
OBJ_CLASS_INSTANCE(orte_ess_base_signal_t,
                   opal_list_item_t,
                   scon, sdes);
