/*
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, Inc. All rights reserved.
 * Copyright (c) 2014-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef PMIX_HASH_H
#define PMIX_HASH_H

#include <src/include/pmix_config.h>


#include "src/mca/bfrops/bfrops_types.h"
#include "src/class/pmix_hash_table.h"

BEGIN_C_DECLS

/* store a value in the given hash table for the specified
 * rank index.*/
PMIX_EXPORT pmix_status_t pmix_hash_store(pmix_hash_table_t *table,
                                          pmix_rank_t rank, pmix_kval_t *kv);

/* Fetch the value for a specified key and rank from within
 * the given hash_table */
PMIX_EXPORT pmix_status_t pmix_hash_fetch(pmix_hash_table_t *table, pmix_rank_t rank,
                                          const char *key, pmix_value_t **kvs);

/* Fetch the value for a specified key from within
 * the given hash_table
 * It gets the next portion of data from table, where matching key.
 * To get the first data from table, function is called with key parameter as string.
 * Remaining data from table are obtained by calling function with a null pointer for the key parameter.*/
PMIX_EXPORT pmix_status_t pmix_hash_fetch_by_key(pmix_hash_table_t *table, const char *key,
                                                 pmix_rank_t *rank, pmix_value_t **kvs, void **last);

/* remove the specified key-value from the given hash_table.
 * A NULL key will result in removal of all data for the
 * given rank. A rank of PMIX_RANK_WILDCARD indicates that
 * the specified key  is to be removed from the data for all
 * ranks in the table. Combining key=NULL with rank=PMIX_RANK_WILDCARD
 * will therefore result in removal of all data from the
 * table */
PMIX_EXPORT pmix_status_t pmix_hash_remove_data(pmix_hash_table_t *table,
                                                pmix_rank_t rank, const char *key);

END_C_DECLS

#endif /* PMIX_HASH_H */
