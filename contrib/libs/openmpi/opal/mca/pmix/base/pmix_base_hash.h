/*
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, Inc. All rights reserved.
 * Copyright (c) 2014-2015 Intel, Inc. All rights reserved.
 * Copyright (c) 2014      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_PMIX_HASH_H
#define OPAL_PMIX_HASH_H

#include "opal/class/opal_list.h"
#include "opal/class/opal_hash_table.h"
#include "opal/dss/dss.h"
#include "opal/util/proc.h"

BEGIN_C_DECLS

OPAL_DECLSPEC void opal_pmix_base_hash_init(void);
OPAL_DECLSPEC void opal_pmix_base_hash_finalize(void);

OPAL_DECLSPEC int opal_pmix_base_store(const opal_process_name_t *id,
                                       opal_value_t *val);

OPAL_DECLSPEC int opal_pmix_base_fetch(const opal_process_name_t *id,
                                       const char *key, opal_list_t *kvs);

OPAL_DECLSPEC int opal_pmix_base_remove(const opal_process_name_t *id, const char *key);

END_C_DECLS

#endif /* OPAL_DSTORE_HASH_H */
