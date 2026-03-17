/*
 * Copyright (c) 2006-2007 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2016      Intel, Inc. All rights reserved
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>

#include "src/mca/pinstalldirs/pinstalldirs.h"
#include "src/mca/pinstalldirs/config/pinstall_dirs.h"

const pmix_pinstalldirs_base_component_t mca_pinstalldirs_config_component = {
    /* First, the mca_component_t struct containing meta information
       about the component itself */
    {
        PMIX_PINSTALLDIRS_BASE_VERSION_1_0_0,

        /* Component name and version */
        "config",
        PMIX_MAJOR_VERSION,
        PMIX_MINOR_VERSION,
        PMIX_RELEASE_VERSION,

        /* Component open and close functions */
        NULL,
        NULL
    },
    {
        /* This component is Checkpointable */
        PMIX_MCA_BASE_METADATA_PARAM_CHECKPOINT
    },

    {
        PMIX_INSTALL_PREFIX,
        PMIX_EXEC_PREFIX,
        PMIX_BINDIR,
        PMIX_SBINDIR,
        PMIX_LIBEXECDIR,
        PMIX_DATAROOTDIR,
        PMIX_DATADIR,
        PMIX_SYSCONFDIR,
        PMIX_SHAREDSTATEDIR,
        PMIX_LOCALSTATEDIR,
        PMIX_LIBDIR,
        PMIX_INCLUDEDIR,
        PMIX_INFODIR,
        PMIX_MANDIR,
        PMIX_PKGDATADIR,
        PMIX_PKGLIBDIR,
        PMIX_PKGINCLUDEDIR
    }
};
