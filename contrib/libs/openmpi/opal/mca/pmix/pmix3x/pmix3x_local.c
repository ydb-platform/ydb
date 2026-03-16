/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2017 Intel, Inc.  All rights reserved.
 * Copyright (c) 2014-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2014-2015 Mellanox Technologies, Inc.
 *                         All rights reserved.
 * Copyright (c) 2016      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2017      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"
#include "opal/constants.h"
#include "opal/types.h"

#ifdef HAVE_STRING_H
#include <string.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "opal/dss/dss.h"
#include "opal/mca/event/event.h"
#include "opal/mca/hwloc/base/base.h"
#include "opal/runtime/opal.h"
#include "opal/runtime/opal_progress_threads.h"
#include "opal/threads/threads.h"
#include "opal/util/argv.h"
#include "opal/util/error.h"
#include "opal/util/opal_environ.h"
#include "opal/util/output.h"
#include "opal/util/proc.h"
#include "opal/util/show_help.h"

#include "pmix3x.h"
#include "opal/mca/pmix/base/base.h"
#include "opal/mca/pmix/pmix_types.h"

#include <pmix_common.h>
#include <pmix.h>


typedef struct {
    opal_list_item_t super;
    char *opalname;
    char *opalvalue;
    char *pmixname;
    char *pmixvalue;
    bool mismatched;
} opal_pmix_evar_t;
static void econ(opal_pmix_evar_t *p)
{
    p->opalname = NULL;
    p->opalvalue = NULL;
    p->pmixname = NULL;
    p->pmixvalue = NULL;
    p->mismatched = false;
}
static OBJ_CLASS_INSTANCE(opal_pmix_evar_t,
                          opal_list_item_t,
                          econ, NULL);
struct known_value {
    char *opalname;
    char *pmixname;
};

static struct known_value known_values[] = {
    {"OPAL_PREFIX", "PMIX_INSTALL_PREFIX"},
    {"OPAL_EXEC_PREFIX", "PMIX_EXEC_PREFIX"},
    {"OPAL_BINDIR", "PMIX_BINDIR"},
    {"OPAL_SBINDIR", "PMIX_SBINDIR"},
    {"OPAL_LIBEXECDIR", "PMIX_LIBEXECDIR"},
    {"OPAL_DATAROOTDIR", "PMIX_DATAROOTDIR"},
    {"OPAL_DATADIR", "PMIX_DATADIR"},
    {"OPAL_SYSCONFDIR", "PMIX_SYSCONFDIR"},
    {"OPAL_SHAREDSTATEDIR", "PMIX_SHAREDSTATEDIR"},
    {"OPAL_LOCALSTATEDIR", "PMIX_LOCALSTATEDIR"},
    {"OPAL_LIBDIR", "PMIX_LIBDIR"},
    {"OPAL_INCLUDEDIR", "PMIX_INCLUDEDIR"},
    {"OPAL_INFODIR", "PMIX_INFODIR"},
    {"OPAL_MANDIR", "PMIX_MANDIR"},
    {"OPAL_PKGDATADIR", "PMIX_PKGDATADIR"},
    {"OPAL_PKGLIBDIR", "PMIX_PKGLIBDIR"},
    {"OPAL_PKGINCLUDEDIR", "PMIX_PKGINCLUDEDIR"}
};


int opal_pmix_pmix3x_check_evars(void)
{
    opal_list_t values;
    int nvals, i;
    opal_pmix_evar_t *evar;
    bool mismatched = false;
    char *tmp=NULL, *tmp2;

    OBJ_CONSTRUCT(&values, opal_list_t);
    nvals = sizeof(known_values) / sizeof(struct known_value);
    for (i=0; i < nvals; i++) {
        evar = OBJ_NEW(opal_pmix_evar_t);
        evar->opalname = known_values[i].opalname;
        evar->opalvalue = getenv(evar->opalname);
        evar->pmixname = known_values[i].pmixname;
        evar->pmixvalue = getenv(evar->pmixname);
        /* if the OPAL value is not set and the PMIx value is,
         * then that is a problem. Likewise, if both are set
         * and are different, then that is also a problem. Note that
         * it is okay for the OPAL value to be set and the PMIx
         * value to not be set */
        if ((NULL == evar->opalvalue && NULL != evar->pmixvalue) ||
            (NULL != evar->opalvalue && NULL != evar->pmixvalue &&
             0 != strcmp(evar->opalvalue, evar->pmixvalue))) {
            evar->mismatched = true;
            mismatched = true;
        }
        opal_list_append(&values, &evar->super);
    }
    if (!mismatched) {
        /* transfer any OPAL values that were set - we already verified
         * that the equivalent PMIx value, if present, matches, so
         * don't overwrite it */
        OPAL_LIST_FOREACH(evar, &values, opal_pmix_evar_t) {
            if (NULL != evar->opalvalue && NULL == evar->pmixvalue) {
                opal_setenv(evar->pmixname, evar->opalvalue, true, &environ);
            }
        }
        OPAL_LIST_DESTRUCT(&values);
        return OPAL_SUCCESS;
    }
    /* we have at least one mismatch somewhere, so print out the table */
    OPAL_LIST_FOREACH(evar, &values, opal_pmix_evar_t) {
        if (evar->mismatched) {
            if (NULL == tmp) {
                asprintf(&tmp, "  %s:  %s\n  %s:  %s",
                         evar->opalname, (NULL == evar->opalvalue) ? "NULL" : evar->opalvalue,
                         evar->pmixname, (NULL == evar->pmixvalue) ? "NULL" : evar->pmixvalue);
            } else {
                asprintf(&tmp2, "%s\n\n  %s:  %s\n  %s:  %s", tmp,
                         evar->opalname, (NULL == evar->opalvalue) ? "NULL" : evar->opalvalue,
                         evar->pmixname, (NULL == evar->pmixvalue) ? "NULL" : evar->pmixvalue);
                free(tmp);
                tmp = tmp2;
            }
        }
    }
    opal_show_help("help-pmix-pmix3x.txt", "evars", true, tmp);
    free(tmp);
    return OPAL_ERR_SILENT;
}
