/*
 * Copyright (c) 2018      Mellanox Technologies, Inc.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef DS21_LOCK_H
#define DS21_LOCK_H

#include <src/include/pmix_config.h>
#include <pmix_common.h>

#include "src/mca/common/dstore/dstore_common.h"

pmix_status_t pmix_gds_ds21_lock_init(pmix_common_dstor_lock_ctx_t *lock_ctx,
                                      const char *base_path,  const char *name,
                                      uint32_t local_size, uid_t uid, bool setuid);
void pmix_ds21_lock_finalize(pmix_common_dstor_lock_ctx_t *lock_ctx);
pmix_status_t pmix_ds21_lock_rd_get(pmix_common_dstor_lock_ctx_t lock_ctx);
pmix_status_t pmix_ds21_lock_wr_get(pmix_common_dstor_lock_ctx_t lock_ctx);
pmix_status_t pmix_ds21_lock_rd_rel(pmix_common_dstor_lock_ctx_t lock_ctx);
pmix_status_t pmix_ds21_lock_wr_rel(pmix_common_dstor_lock_ctx_t lock_ctx);

extern pmix_common_lock_callbacks_t pmix_ds21_lock_module;

#endif // DS21_LOCK_H
