/*
 * Copyright (c) 2006-2007 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include "opal/mca/installdirs/installdirs.h"
#include "opal/mca/installdirs/config/install_dirs.h"

const opal_installdirs_base_component_t mca_installdirs_config_component = {
    /* First, the mca_component_t struct containing meta information
       about the component itself */
    {
        OPAL_INSTALLDIRS_BASE_VERSION_2_0_0,

        /* Component name and version */
        "config",
        OPAL_MAJOR_VERSION,
        OPAL_MINOR_VERSION,
        OPAL_RELEASE_VERSION,

        /* Component open and close functions */
        NULL,
        NULL
    },
    {
        /* This component is Checkpointable */
        MCA_BASE_METADATA_PARAM_CHECKPOINT
    },

    {
        OPAL_PREFIX,
        OPAL_EXEC_PREFIX,
        OPAL_BINDIR,
        OPAL_SBINDIR,
        OPAL_LIBEXECDIR,
        OPAL_DATAROOTDIR,
        OPAL_DATADIR,
        OPAL_SYSCONFDIR,
        OPAL_SHAREDSTATEDIR,
        OPAL_LOCALSTATEDIR,
        OPAL_LIBDIR,
        OPAL_INCLUDEDIR,
        OPAL_INFODIR,
        OPAL_MANDIR,
        OPAL_PKGDATADIR,
        OPAL_PKGLIBDIR,
        OPAL_PKGINCLUDEDIR
    }
};
