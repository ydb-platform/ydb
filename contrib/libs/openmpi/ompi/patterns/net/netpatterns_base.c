/*
 *
 * Copyright (c) 2009-2012 Mellanox Technologies.  All rights reserved.
 * Copyright (c) 2009-2012 Oak Ridge National Laboratory.  All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#include <stdarg.h>

#include "ompi/include/ompi/constants.h"
#include "netpatterns.h"

int ompi_netpatterns_base_verbose = 0; /* disabled by default */

int ompi_netpatterns_register_mca_params(void)
{
    ompi_netpatterns_base_verbose = 0;
    mca_base_var_register("ompi", "common", "netpatterns", "base_verbose",
                          "Verbosity level of the NETPATTERNS framework",
                          MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                          OPAL_INFO_LVL_9,
                          MCA_BASE_VAR_SCOPE_READONLY,
                          &ompi_netpatterns_base_verbose);

    return OMPI_SUCCESS;
}

int ompi_netpatterns_base_err(const char* fmt, ...)
{
    va_list list;
    int ret;

    va_start(list, fmt);
    ret = vfprintf(stderr, fmt, list);
    va_end(list);
    return ret;
}

int ompi_netpatterns_init(void)
{
/* There is no component for common_netpatterns so every component that uses it
   should call ompi_netpatterns_init, still we want to run it only once */
static int was_called = 0;

    if (0 == was_called) {
        was_called = 1;

        return ompi_netpatterns_register_mca_params();
    }

    return OMPI_SUCCESS;
}
