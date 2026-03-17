/*
 * Copyright (c) 2014      Mellanox Technologies, Inc.
 *                         All rights reserved.
 * Copyright (c) 2014      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014-2017 Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef PMIX_ALFG_H
#define PMIX_ALFG_H

#include <src/include/pmix_config.h>
#include <pmix_common.h>

#include "src/include/pmix_stdint.h"


struct pmix_rng_buff_t {
    uint32_t alfg[127];
    int tap1;
    int tap2;
};
typedef struct pmix_rng_buff_t pmix_rng_buff_t;


/* NOTE: UNLIKE OTHER PMIX FUNCTIONS, THIS FUNCTION RETURNS A 1 IF
 * SUCCESSFUL INSTEAD OF PMIX_SUCCESS */
PMIX_EXPORT int pmix_srand(pmix_rng_buff_t *buff, uint32_t seed);

PMIX_EXPORT uint32_t pmix_rand(pmix_rng_buff_t *buff);

PMIX_EXPORT int pmix_random(void);

#endif /* PMIX_ALFG_H */
