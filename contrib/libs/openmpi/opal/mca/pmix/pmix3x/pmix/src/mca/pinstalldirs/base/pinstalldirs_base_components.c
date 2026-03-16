/*
 * Copyright (c) 2006-2012 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010      Sandia National Laboratories. All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

#include <src/include/pmix_config.h>

#include "pmix_common.h"
#include "src/mca/mca.h"
#include "src/mca/pinstalldirs/pinstalldirs.h"
#include "src/mca/pinstalldirs/base/base.h"
#include "src/mca/pinstalldirs/base/static-components.h"

pmix_pinstall_dirs_t pmix_pinstall_dirs = {0};

#define CONDITIONAL_COPY(target, origin, field)                 \
    do {                                                        \
        if (origin.field != NULL && target.field == NULL) {     \
            target.field = origin.field;                        \
        }                                                       \
    } while (0)

static int
pmix_pinstalldirs_base_open(pmix_mca_base_open_flag_t flags)
{
    pmix_mca_base_component_list_item_t *component_item;
    int ret;

    ret = pmix_mca_base_framework_components_open(&pmix_pinstalldirs_base_framework, flags);
    if (PMIX_SUCCESS != ret) {
        return ret;
    }

    PMIX_LIST_FOREACH(component_item, &pmix_pinstalldirs_base_framework.framework_components, pmix_mca_base_component_list_item_t) {
        const pmix_pinstalldirs_base_component_t *component =
            (const pmix_pinstalldirs_base_component_t *) component_item->cli_component;

        /* copy over the data, if something isn't already there */
        CONDITIONAL_COPY(pmix_pinstall_dirs, component->install_dirs_data,
                         prefix);
        CONDITIONAL_COPY(pmix_pinstall_dirs, component->install_dirs_data,
                         exec_prefix);
        CONDITIONAL_COPY(pmix_pinstall_dirs, component->install_dirs_data,
                         bindir);
        CONDITIONAL_COPY(pmix_pinstall_dirs, component->install_dirs_data,
                         sbindir);
        CONDITIONAL_COPY(pmix_pinstall_dirs, component->install_dirs_data,
                         libexecdir);
        CONDITIONAL_COPY(pmix_pinstall_dirs, component->install_dirs_data,
                         datarootdir);
        CONDITIONAL_COPY(pmix_pinstall_dirs, component->install_dirs_data,
                         datadir);
        CONDITIONAL_COPY(pmix_pinstall_dirs, component->install_dirs_data,
                         sysconfdir);
        CONDITIONAL_COPY(pmix_pinstall_dirs, component->install_dirs_data,
                         sharedstatedir);
        CONDITIONAL_COPY(pmix_pinstall_dirs, component->install_dirs_data,
                         localstatedir);
        CONDITIONAL_COPY(pmix_pinstall_dirs, component->install_dirs_data,
                         libdir);
        CONDITIONAL_COPY(pmix_pinstall_dirs, component->install_dirs_data,
                         includedir);
        CONDITIONAL_COPY(pmix_pinstall_dirs, component->install_dirs_data,
                         infodir);
        CONDITIONAL_COPY(pmix_pinstall_dirs, component->install_dirs_data,
                         mandir);
        CONDITIONAL_COPY(pmix_pinstall_dirs, component->install_dirs_data,
                         pmixdatadir);
        CONDITIONAL_COPY(pmix_pinstall_dirs, component->install_dirs_data,
                         pmixlibdir);
        CONDITIONAL_COPY(pmix_pinstall_dirs, component->install_dirs_data,
                         pmixincludedir);
    }

    /* expand out all the fields */
    pmix_pinstall_dirs.prefix =
        pmix_pinstall_dirs_expand_setup(pmix_pinstall_dirs.prefix);
    pmix_pinstall_dirs.exec_prefix =
        pmix_pinstall_dirs_expand_setup(pmix_pinstall_dirs.exec_prefix);
    pmix_pinstall_dirs.bindir =
        pmix_pinstall_dirs_expand_setup(pmix_pinstall_dirs.bindir);
    pmix_pinstall_dirs.sbindir =
        pmix_pinstall_dirs_expand_setup(pmix_pinstall_dirs.sbindir);
    pmix_pinstall_dirs.libexecdir =
        pmix_pinstall_dirs_expand_setup(pmix_pinstall_dirs.libexecdir);
    pmix_pinstall_dirs.datarootdir =
        pmix_pinstall_dirs_expand_setup(pmix_pinstall_dirs.datarootdir);
    pmix_pinstall_dirs.datadir =
        pmix_pinstall_dirs_expand_setup(pmix_pinstall_dirs.datadir);
    pmix_pinstall_dirs.sysconfdir =
        pmix_pinstall_dirs_expand_setup(pmix_pinstall_dirs.sysconfdir);
    pmix_pinstall_dirs.sharedstatedir =
        pmix_pinstall_dirs_expand_setup(pmix_pinstall_dirs.sharedstatedir);
    pmix_pinstall_dirs.localstatedir =
        pmix_pinstall_dirs_expand_setup(pmix_pinstall_dirs.localstatedir);
    pmix_pinstall_dirs.libdir =
        pmix_pinstall_dirs_expand_setup(pmix_pinstall_dirs.libdir);
    pmix_pinstall_dirs.includedir =
        pmix_pinstall_dirs_expand_setup(pmix_pinstall_dirs.includedir);
    pmix_pinstall_dirs.infodir =
        pmix_pinstall_dirs_expand_setup(pmix_pinstall_dirs.infodir);
    pmix_pinstall_dirs.mandir =
        pmix_pinstall_dirs_expand_setup(pmix_pinstall_dirs.mandir);
    pmix_pinstall_dirs.pmixdatadir =
        pmix_pinstall_dirs_expand_setup(pmix_pinstall_dirs.pmixdatadir);
    pmix_pinstall_dirs.pmixlibdir =
        pmix_pinstall_dirs_expand_setup(pmix_pinstall_dirs.pmixlibdir);
    pmix_pinstall_dirs.pmixincludedir =
        pmix_pinstall_dirs_expand_setup(pmix_pinstall_dirs.pmixincludedir);

#if 0
    fprintf(stderr, "prefix:         %s\n", pmix_pinstall_dirs.prefix);
    fprintf(stderr, "exec_prefix:    %s\n", pmix_pinstall_dirs.exec_prefix);
    fprintf(stderr, "bindir:         %s\n", pmix_pinstall_dirs.bindir);
    fprintf(stderr, "sbindir:        %s\n", pmix_pinstall_dirs.sbindir);
    fprintf(stderr, "libexecdir:     %s\n", pmix_pinstall_dirs.libexecdir);
    fprintf(stderr, "datarootdir:    %s\n", pmix_pinstall_dirs.datarootdir);
    fprintf(stderr, "datadir:        %s\n", pmix_pinstall_dirs.datadir);
    fprintf(stderr, "sysconfdir:     %s\n", pmix_pinstall_dirs.sysconfdir);
    fprintf(stderr, "sharedstatedir: %s\n", pmix_pinstall_dirs.sharedstatedir);
    fprintf(stderr, "localstatedir:  %s\n", pmix_pinstall_dirs.localstatedir);
    fprintf(stderr, "libdir:         %s\n", pmix_pinstall_dirs.libdir);
    fprintf(stderr, "includedir:     %s\n", pmix_pinstall_dirs.includedir);
    fprintf(stderr, "infodir:        %s\n", pmix_pinstall_dirs.infodir);
    fprintf(stderr, "mandir:         %s\n", pmix_pinstall_dirs.mandir);
    fprintf(stderr, "pkgdatadir:     %s\n", pmix_pinstall_dirs.pkgdatadir);
    fprintf(stderr, "pkglibdir:      %s\n", pmix_pinstall_dirs.pkglibdir);
    fprintf(stderr, "pkgincludedir:  %s\n", pmix_pinstall_dirs.pkgincludedir);
#endif

    /* NTH: Is it ok not to close the components? If not we can add a flag
       to mca_base_framework_components_close to indicate not to deregister
       variable groups */
    return PMIX_SUCCESS;
}


static int
pmix_pinstalldirs_base_close(void)
{
    free(pmix_pinstall_dirs.prefix);
    free(pmix_pinstall_dirs.exec_prefix);
    free(pmix_pinstall_dirs.bindir);
    free(pmix_pinstall_dirs.sbindir);
    free(pmix_pinstall_dirs.libexecdir);
    free(pmix_pinstall_dirs.datarootdir);
    free(pmix_pinstall_dirs.datadir);
    free(pmix_pinstall_dirs.sysconfdir);
    free(pmix_pinstall_dirs.sharedstatedir);
    free(pmix_pinstall_dirs.localstatedir);
    free(pmix_pinstall_dirs.libdir);
    free(pmix_pinstall_dirs.includedir);
    free(pmix_pinstall_dirs.infodir);
    free(pmix_pinstall_dirs.mandir);
    free(pmix_pinstall_dirs.pmixdatadir);
    free(pmix_pinstall_dirs.pmixlibdir);
    free(pmix_pinstall_dirs.pmixincludedir);
    memset (&pmix_pinstall_dirs, 0, sizeof (pmix_pinstall_dirs));

    return pmix_mca_base_framework_components_close (&pmix_pinstalldirs_base_framework, NULL);
}

/* Declare the pinstalldirs framework */
PMIX_MCA_BASE_FRAMEWORK_DECLARE(pmix, pinstalldirs, NULL, NULL, pmix_pinstalldirs_base_open,
                                pmix_pinstalldirs_base_close, mca_pinstalldirs_base_static_components,
                                PMIX_MCA_BASE_FRAMEWORK_FLAG_NOREGISTER | PMIX_MCA_BASE_FRAMEWORK_FLAG_NO_DSO);
