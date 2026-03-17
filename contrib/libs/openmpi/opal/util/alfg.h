/*
 * Copyright (c) 2014      Mellanox Technologies, Inc.
 *                         All rights reserved.
 * Copyright (c) 2014      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014-2015 Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_ALFG_H
#define OPAL_ALFG_H

#include "opal_config.h"

#include "opal_stdint.h"


struct opal_rng_buff_t {
    uint32_t alfg[127];
    int tap1;
    int tap2;
};
typedef struct opal_rng_buff_t opal_rng_buff_t;


/* NOTE: UNLIKE OTHER OPAL FUNCTIONS, THIS FUNCTION RETURNS A 1 IF
 * SUCCESSFUL INSTEAD OF OPAL_SUCCESS */
OPAL_DECLSPEC int opal_srand(opal_rng_buff_t *buff, uint32_t seed);

OPAL_DECLSPEC uint32_t opal_rand(opal_rng_buff_t *buff);

OPAL_DECLSPEC int opal_random(void);

#endif /* OPAL_ALFG_H */
