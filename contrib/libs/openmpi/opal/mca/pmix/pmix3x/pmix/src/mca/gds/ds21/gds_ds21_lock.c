/*
 * Copyright (c) 2018      Mellanox Technologies, Inc.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>
#include <pmix_common.h>

#include "src/mca/common/dstore/dstore_common.h"

#include "gds_ds21_lock.h"

pmix_common_lock_callbacks_t pmix_ds21_lock_module = {
    .init = pmix_gds_ds21_lock_init,
    .finalize = pmix_ds21_lock_finalize,
    .rd_lock = pmix_ds21_lock_rd_get,
    .rd_unlock = pmix_ds21_lock_rd_rel,
    .wr_lock = pmix_ds21_lock_wr_get,
    .wr_unlock = pmix_ds21_lock_wr_rel
};
