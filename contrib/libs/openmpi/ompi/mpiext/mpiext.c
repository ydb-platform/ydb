#include "ompi_config.h"

#include <stdlib.h>

#include "ompi/constants.h"
#include "ompi/mpiext/mpiext.h"
#include "ompi/mpiext/static-components.h"


int
ompi_mpiext_init(void)
{
    const ompi_mpiext_component_t **tmp = ompi_mpiext_components;
    int ret;

    while (NULL != (*tmp)) {
        if (NULL != (*tmp)->init) {
            ret = (*tmp)->init();
            if (OMPI_SUCCESS != ret) return ret;
        }
        tmp++;
    }

    return OMPI_SUCCESS;
}


int
ompi_mpiext_fini(void)
{
    const ompi_mpiext_component_t **tmp = ompi_mpiext_components;
    int ret;

    while (NULL != (*tmp)) {
        if (NULL != (*tmp)->fini) {
            ret = (*tmp)->fini();
            if (OMPI_SUCCESS != ret) return ret;
        }
        tmp++;
    }

    return OMPI_SUCCESS;
}
