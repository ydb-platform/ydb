/*
 * Copyright (c) 2006-2007 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2016      Intel, Inc. All rights reserved
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>

#include <stdlib.h>
#include <string.h>

#include "pmix_common.h"
#include "src/mca/pinstalldirs/pinstalldirs.h"

static int pinstalldirs_env_open(void);


pmix_pinstalldirs_base_component_t mca_pinstalldirs_env_component = {
    /* First, the mca_component_t struct containing meta information
       about the component itself */
    {
        PMIX_PINSTALLDIRS_BASE_VERSION_1_0_0,

        /* Component name and version */
        "env",
        PMIX_MAJOR_VERSION,
        PMIX_MINOR_VERSION,
        PMIX_RELEASE_VERSION,

        /* Component open and close functions */
        pinstalldirs_env_open,
        NULL
    },
    {
        /* This component is checkpointable */
        PMIX_MCA_BASE_METADATA_PARAM_CHECKPOINT
    },

    /* Next the pmix_pinstall_dirs_t install_dirs_data information */
    {
        NULL,
    },
};


#define SET_FIELD(field, envname)                                         \
    do {                                                                  \
        char *tmp = getenv(envname);                                      \
         if (NULL != tmp && 0 == strlen(tmp)) {                           \
             tmp = NULL;                                                  \
         }                                                                \
         mca_pinstalldirs_env_component.install_dirs_data.field = tmp;     \
    } while (0)


static int
pinstalldirs_env_open(void)
{
    SET_FIELD(prefix, "PMIX_INSTALL_PREFIX");
    SET_FIELD(exec_prefix, "PMIX_EXEC_PREFIX");
    SET_FIELD(bindir, "PMIX_BINDIR");
    SET_FIELD(sbindir, "PMIX_SBINDIR");
    SET_FIELD(libexecdir, "PMIX_LIBEXECDIR");
    SET_FIELD(datarootdir, "PMIX_DATAROOTDIR");
    SET_FIELD(datadir, "PMIX_DATADIR");
    SET_FIELD(sysconfdir, "PMIX_SYSCONFDIR");
    SET_FIELD(sharedstatedir, "PMIX_SHAREDSTATEDIR");
    SET_FIELD(localstatedir, "PMIX_LOCALSTATEDIR");
    SET_FIELD(libdir, "PMIX_LIBDIR");
    SET_FIELD(includedir, "PMIX_INCLUDEDIR");
    SET_FIELD(infodir, "PMIX_INFODIR");
    SET_FIELD(mandir, "PMIX_MANDIR");
    SET_FIELD(pmixdatadir, "PMIX_PKGDATADIR");
    SET_FIELD(pmixlibdir, "PMIX_PKGLIBDIR");
    SET_FIELD(pmixincludedir, "PMIX_PKGINCLUDEDIR");

    return PMIX_SUCCESS;
}
