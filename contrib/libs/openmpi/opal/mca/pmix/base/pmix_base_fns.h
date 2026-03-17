/*
 * Copyright (c) 2014      Intel, Inc. All rights reserved.
 * Copyright (c) 2014      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_PMIX_BASE_FNS_H
#define MCA_PMIX_BASE_FNS_H

#include "opal_config.h"
#include "opal/util/error.h"
#include "opal/dss/dss_types.h"


BEGIN_C_DECLS

typedef int (*kvs_put_fn)(const char key[], const char value[]);
typedef int (*kvs_get_fn)(const char key[], char value [], int maxvalue);

OPAL_DECLSPEC int opal_pmix_base_store_encoded(const char *key, const void *data,
                                               opal_data_type_t type, char** buffer, int* length);
OPAL_DECLSPEC int opal_pmix_base_commit_packed(char** data, int* data_offset,
                                               char** enc_data, int* enc_data_offset,
                                               int max_key, int* pack_key, kvs_put_fn fn);
OPAL_DECLSPEC int opal_pmix_base_partial_commit_packed(char** data, int* data_offset,
                                                       char** enc_data, int* enc_data_offset,
                                                       int max_key, int* pack_key, kvs_put_fn fn);
OPAL_DECLSPEC int opal_pmix_base_cache_keys_locally(const opal_process_name_t* id, const char* key,
                                                    opal_value_t **out_kv, char* kvs_name, int vallen, kvs_get_fn fn);
OPAL_DECLSPEC int opal_pmix_base_get_packed(const opal_process_name_t* proc, char **packed_data,
                                            size_t *len, int vallen, kvs_get_fn fn);

END_C_DECLS

#endif
