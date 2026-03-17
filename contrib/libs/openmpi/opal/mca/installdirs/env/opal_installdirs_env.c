/*
 * Copyright (c) 2006-2007 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include <stdlib.h>
#include <string.h>

#include "opal/constants.h"
#include "opal/mca/installdirs/installdirs.h"

static int installdirs_env_open(void);


opal_installdirs_base_component_t mca_installdirs_env_component = {
    /* First, the mca_component_t struct containing meta information
       about the component itself */
    {
        OPAL_INSTALLDIRS_BASE_VERSION_2_0_0,

        /* Component name and version */
        "env",
        OPAL_MAJOR_VERSION,
        OPAL_MINOR_VERSION,
        OPAL_RELEASE_VERSION,

        /* Component open and close functions */
        installdirs_env_open,
        NULL
    },
    {
        /* This component is checkpointable */
        MCA_BASE_METADATA_PARAM_CHECKPOINT
    },

    /* Next the opal_install_dirs_t install_dirs_data information */
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
         mca_installdirs_env_component.install_dirs_data.field = tmp;     \
    } while (0)


static int
installdirs_env_open(void)
{
    SET_FIELD(prefix, "OPAL_PREFIX");
    SET_FIELD(exec_prefix, "OPAL_EXEC_PREFIX");
    SET_FIELD(bindir, "OPAL_BINDIR");
    SET_FIELD(sbindir, "OPAL_SBINDIR");
    SET_FIELD(libexecdir, "OPAL_LIBEXECDIR");
    SET_FIELD(datarootdir, "OPAL_DATAROOTDIR");
    SET_FIELD(datadir, "OPAL_DATADIR");
    SET_FIELD(sysconfdir, "OPAL_SYSCONFDIR");
    SET_FIELD(sharedstatedir, "OPAL_SHAREDSTATEDIR");
    SET_FIELD(localstatedir, "OPAL_LOCALSTATEDIR");
    SET_FIELD(libdir, "OPAL_LIBDIR");
    SET_FIELD(includedir, "OPAL_INCLUDEDIR");
    SET_FIELD(infodir, "OPAL_INFODIR");
    SET_FIELD(mandir, "OPAL_MANDIR");
    SET_FIELD(opaldatadir, "OPAL_PKGDATADIR");
    SET_FIELD(opallibdir, "OPAL_PKGLIBDIR");
    SET_FIELD(opalincludedir, "OPAL_PKGINCLUDEDIR");

    return OPAL_SUCCESS;
}
