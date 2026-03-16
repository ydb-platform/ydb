/* -*- C -*-
 *
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, Inc.  All rights reserved.
 * Copyright (c) 2014-2018 Intel, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016      IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */
#ifndef PMIX1_BFROP_INTERNAL_H_
#define PMIX1_BFROP_INTERNAL_H_

#include <src/include/pmix_config.h>


#ifdef HAVE_SYS_TIME_H
#include <sys/time.h> /* for struct timeval */
#endif
#ifdef HAVE_STRING_H
#include <string.h>
#endif

#include "src/class/pmix_pointer_array.h"

#include "src/mca/bfrops/base/base.h"

BEGIN_C_DECLS

/* DEPRECATED data type values */
#define PMIX_MODEX        29
#define PMIX_INFO_ARRAY   44

/****    PMIX MODEX STRUCT  -  DEPRECATED    ****/
typedef struct pmix_modex_data {
    char nspace[PMIX_MAX_NSLEN+1];
    int rank;
    uint8_t *blob;
    size_t size;
} pmix_modex_data_t;
/* utility macros for working with pmix_modex_t structs */
#define PMIX_MODEX_CREATE(m, n)                                             \
    do {                                                                    \
        (m) = (pmix_modex_data_t*)calloc((n) , sizeof(pmix_modex_data_t));  \
    } while (0)

#define PMIX_MODEX_RELEASE(m)                   \
    do {                                        \
        PMIX_MODEX_DESTRUCT((m));               \
        free((m));                              \
        (m) = NULL;                             \
    } while (0)

#define PMIX_MODEX_CONSTRUCT(m)                         \
    do {                                                \
        memset((m), 0, sizeof(pmix_modex_data_t));      \
    } while (0)

#define PMIX_MODEX_DESTRUCT(m)                  \
    do {                                        \
        if (NULL != (m)->blob) {                \
            free((m)->blob);                    \
            (m)->blob = NULL;                   \
        }                                       \
    } while (0)

#define PMIX_MODEX_FREE(m, n)                           \
    do {                                                \
        size_t _s;                                      \
        if (NULL != (m)) {                              \
            for (_s=0; _s < (n); _s++) {                \
                PMIX_MODEX_DESTRUCT(&((m)[_s]));        \
            }                                           \
            free((m));                                  \
            (m) = NULL;                                 \
        }                                               \
    } while (0)

/*
 * Implementations of API functions
 */

pmix_status_t pmix12_bfrop_pack(pmix_buffer_t *buffer, const void *src,
                               int32_t num_vals,
                               pmix_data_type_t type);
pmix_status_t pmix12_bfrop_unpack(pmix_buffer_t *buffer, void *dest,
                                 int32_t *max_num_vals,
                                 pmix_data_type_t type);

pmix_status_t pmix12_bfrop_copy(void **dest, void *src, pmix_data_type_t type);

pmix_status_t pmix12_bfrop_print(char **output, char *prefix, void *src, pmix_data_type_t type);

pmix_status_t pmix12_bfrop_copy_payload(pmix_buffer_t *dest, pmix_buffer_t *src);

pmix_status_t pmix12_bfrop_value_xfer(pmix_value_t *p, const pmix_value_t *src);

void pmix12_bfrop_value_load(pmix_value_t *v, const void *data,
                            pmix_data_type_t type);

pmix_status_t pmix12_bfrop_value_unload(pmix_value_t *kv,
                                       void **data,
                                       size_t *sz);

pmix_value_cmp_t pmix12_bfrop_value_cmp(pmix_value_t *p,
                                       pmix_value_t *p1);

/*
 * Specialized functions
 */
pmix_status_t pmix12_bfrop_pack_buffer(pmix_buffer_t *buffer, const void *src,
                                      int32_t num_vals, pmix_data_type_t type);

pmix_status_t pmix12_bfrop_unpack_buffer(pmix_buffer_t *buffer, void *dst,
                                        int32_t *num_vals, pmix_data_type_t type);

/*
 * Internal pack functions
 */

pmix_status_t pmix12_bfrop_pack_bool(pmix_buffer_t *buffer, const void *src,
                                    int32_t num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_pack_byte(pmix_buffer_t *buffer, const void *src,
                                    int32_t num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_pack_string(pmix_buffer_t *buffer, const void *src,
                                      int32_t num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_pack_sizet(pmix_buffer_t *buffer, const void *src,
                                     int32_t num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_pack_pid(pmix_buffer_t *buffer, const void *src,
                                   int32_t num_vals, pmix_data_type_t type);

pmix_status_t pmix12_bfrop_pack_int(pmix_buffer_t *buffer, const void *src,
                                   int32_t num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_pack_int16(pmix_buffer_t *buffer, const void *src,
                                     int32_t num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_pack_int32(pmix_buffer_t *buffer, const void *src,
                                     int32_t num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_pack_datatype(pmix_buffer_t *buffer, const void *src,
                                        int32_t num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_pack_int64(pmix_buffer_t *buffer, const void *src,
                                     int32_t num_vals, pmix_data_type_t type);

pmix_status_t pmix12_bfrop_pack_float(pmix_buffer_t *buffer, const void *src,
                                     int32_t num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_pack_double(pmix_buffer_t *buffer, const void *src,
                                      int32_t num_vals, pmix_data_type_t type);

pmix_status_t pmix12_bfrop_pack_timeval(pmix_buffer_t *buffer, const void *src,
                                       int32_t num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_pack_time(pmix_buffer_t *buffer, const void *src,
                                    int32_t num_vals, pmix_data_type_t type);

pmix_status_t pmix12_bfrop_pack_value(pmix_buffer_t *buffer, const void *src,
                                     int32_t num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_pack_array(pmix_buffer_t *buffer, const void *src,
                                     int32_t num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_pack_proc(pmix_buffer_t *buffer, const void *src,
                                    int32_t num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_pack_app(pmix_buffer_t *buffer, const void *src,
                                   int32_t num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_pack_info(pmix_buffer_t *buffer, const void *src,
                                    int32_t num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_pack_buf(pmix_buffer_t *buffer, const void *src,
                                   int32_t num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_pack_kval(pmix_buffer_t *buffer, const void *src,
                                    int32_t num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_pack_modex(pmix_buffer_t *buffer, const void *src,
                                     int32_t num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_pack_persist(pmix_buffer_t *buffer, const void *src,
                                       int32_t num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_pack_bo(pmix_buffer_t *buffer, const void *src,
                                  int32_t num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_pack_pdata(pmix_buffer_t *buffer, const void *src,
                                     int32_t num_vals, pmix_data_type_t type);
/* compatibility functions - no corresponding PMIx v1.x definitions */
pmix_status_t pmix12_bfrop_pack_ptr(pmix_buffer_t *buffer, const void *src,
                                   int32_t num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_pack_scope(pmix_buffer_t *buffer, const void *src,
                                     int32_t num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_pack_status(pmix_buffer_t *buffer, const void *src,
                                      int32_t num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_pack_range(pmix_buffer_t *buffer, const void *src,
                                     int32_t num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_pack_cmd(pmix_buffer_t *buffer, const void *src,
                                   int32_t num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_pack_info_directives(pmix_buffer_t *buffer, const void *src,
                                               int32_t num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_pack_proc_state(pmix_buffer_t *buffer, const void *src,
                                          int32_t num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_pack_darray(pmix_buffer_t *buffer, const void *src,
                                      int32_t num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_pack_proc_info(pmix_buffer_t *buffer, const void *src,
                                         int32_t num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_pack_query(pmix_buffer_t *buffer, const void *src,
                                     int32_t num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_pack_rank(pmix_buffer_t *buffer, const void *src,
                                    int32_t num_vals, pmix_data_type_t type);


/*
 * Internal unpack functions
 */
pmix_status_t pmix12_bfrop_unpack_bool(pmix_buffer_t *buffer, void *dest,
                                      int32_t *num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_unpack_byte(pmix_buffer_t *buffer, void *dest,
                                      int32_t *num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_unpack_string(pmix_buffer_t *buffer, void *dest,
                                        int32_t *num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_unpack_sizet(pmix_buffer_t *buffer, void *dest,
                                       int32_t *num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_unpack_pid(pmix_buffer_t *buffer, void *dest,
                                     int32_t *num_vals, pmix_data_type_t type);

pmix_status_t pmix12_bfrop_unpack_int(pmix_buffer_t *buffer, void *dest,
                                     int32_t *num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_unpack_int16(pmix_buffer_t *buffer, void *dest,
                                       int32_t *num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_unpack_int32(pmix_buffer_t *buffer, void *dest,
                                       int32_t *num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_unpack_datatype(pmix_buffer_t *buffer, void *dest,
                                          int32_t *num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_unpack_int64(pmix_buffer_t *buffer, void *dest,
                                       int32_t *num_vals, pmix_data_type_t type);

pmix_status_t pmix12_bfrop_unpack_float(pmix_buffer_t *buffer, void *dest,
                                       int32_t *num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_unpack_double(pmix_buffer_t *buffer, void *dest,
                                        int32_t *num_vals, pmix_data_type_t type);

pmix_status_t pmix12_bfrop_unpack_timeval(pmix_buffer_t *buffer, void *dest,
                                         int32_t *num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_unpack_time(pmix_buffer_t *buffer, void *dest,
                                      int32_t *num_vals, pmix_data_type_t type);

pmix_status_t pmix12_bfrop_unpack_value(pmix_buffer_t *buffer, void *dest,
                                       int32_t *num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_unpack_array(pmix_buffer_t *buffer, void *dest,
                                       int32_t *num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_unpack_proc(pmix_buffer_t *buffer, void *dest,
                                      int32_t *num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_unpack_app(pmix_buffer_t *buffer, void *dest,
                                     int32_t *num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_unpack_info(pmix_buffer_t *buffer, void *dest,
                                      int32_t *num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_unpack_buf(pmix_buffer_t *buffer, void *dest,
                                     int32_t *num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_unpack_kval(pmix_buffer_t *buffer, void *dest,
                                      int32_t *num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_unpack_modex(pmix_buffer_t *buffer, void *dest,
                                       int32_t *num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_unpack_persist(pmix_buffer_t *buffer, void *dest,
                                         int32_t *num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_unpack_bo(pmix_buffer_t *buffer, void *dest,
                                    int32_t *num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_unpack_pdata(pmix_buffer_t *buffer, void *dest,
                                       int32_t *num_vals, pmix_data_type_t type);
/* compatibility functions - no corresponding PMIx v1.x definitions */
pmix_status_t pmix12_bfrop_unpack_ptr(pmix_buffer_t *buffer, void *dest,
                                     int32_t *num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_unpack_scope(pmix_buffer_t *buffer, void *dest,
                                       int32_t *num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_unpack_status(pmix_buffer_t *buffer, void *dest,
                                        int32_t *num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_unpack_range(pmix_buffer_t *buffer, void *dest,
                                       int32_t *num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_unpack_cmd(pmix_buffer_t *buffer, void *dest,
                                     int32_t *num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_unpack_info_directives(pmix_buffer_t *buffer, void *dest,
                                                 int32_t *num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_unpack_proc_state(pmix_buffer_t *buffer, void *dest,
                                            int32_t *num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_unpack_darray(pmix_buffer_t *buffer, void *dest,
                                        int32_t *num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_unpack_proc_info(pmix_buffer_t *buffer, void *dest,
                                           int32_t *num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_unpack_query(pmix_buffer_t *buffer, void *dest,
                                       int32_t *num_vals, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_unpack_rank(pmix_buffer_t *buffer, void *dest,
                                      int32_t *num_vals, pmix_data_type_t type);


/*
 * Internal copy functions
 */

pmix_status_t pmix12_bfrop_std_copy(void **dest, void *src, pmix_data_type_t type);

pmix_status_t pmix12_bfrop_copy_string(char **dest, char *src, pmix_data_type_t type);

pmix_status_t pmix12_bfrop_copy_value(pmix_value_t **dest, pmix_value_t *src,
                                     pmix_data_type_t type);
pmix_status_t pmix12_bfrop_copy_array(pmix_info_array_t **dest, pmix_info_array_t *src,
                                     pmix_data_type_t type);
pmix_status_t pmix12_bfrop_copy_proc(pmix_proc_t **dest, pmix_proc_t *src,
                                    pmix_data_type_t type);
pmix_status_t pmix12_bfrop_copy_app(pmix_app_t **dest, pmix_app_t *src,
                                   pmix_data_type_t type);
pmix_status_t pmix12_bfrop_copy_info(pmix_info_t **dest, pmix_info_t *src,
                                    pmix_data_type_t type);
pmix_status_t pmix12_bfrop_copy_buf(pmix_buffer_t **dest, pmix_buffer_t *src,
                                   pmix_data_type_t type);
pmix_status_t pmix12_bfrop_copy_kval(pmix_kval_t **dest, pmix_kval_t *src,
                                    pmix_data_type_t type);
pmix_status_t pmix12_bfrop_copy_modex(pmix_modex_data_t **dest, pmix_modex_data_t *src,
                                     pmix_data_type_t type);
pmix_status_t pmix12_bfrop_copy_persist(pmix_persistence_t **dest, pmix_persistence_t *src,
                                       pmix_data_type_t type);
pmix_status_t pmix12_bfrop_copy_bo(pmix_byte_object_t **dest, pmix_byte_object_t *src,
                                  pmix_data_type_t type);
pmix_status_t pmix12_bfrop_copy_pdata(pmix_pdata_t **dest, pmix_pdata_t *src,
                                     pmix_data_type_t type);
/* compatibility functions - no corresponding PMIx v1.x definitions */
pmix_status_t pmix12_bfrop_copy_darray(pmix_pdata_t **dest, pmix_data_array_t *src,
                                      pmix_data_type_t type);
pmix_status_t pmix12_bfrop_copy_proc_info(pmix_pdata_t **dest, pmix_proc_info_t *src,
                                      pmix_data_type_t type);
pmix_status_t pmix12_bfrop_copy_query(pmix_pdata_t **dest, pmix_query_t *src,
                                      pmix_data_type_t type);


/*
 * Internal print functions
 */
pmix_status_t pmix12_bfrop_print_bool(char **output, char *prefix, bool *src, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_print_byte(char **output, char *prefix, uint8_t *src, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_print_string(char **output, char *prefix, char *src, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_print_size(char **output, char *prefix, size_t *src, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_print_pid(char **output, char *prefix, pid_t *src, pmix_data_type_t type);

pmix_status_t pmix12_bfrop_print_int(char **output, char *prefix, int *src, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_print_int8(char **output, char *prefix, int8_t *src, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_print_int16(char **output, char *prefix, int16_t *src, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_print_int32(char **output, char *prefix, int32_t *src, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_print_int64(char **output, char *prefix, int64_t *src, pmix_data_type_t type);

pmix_status_t pmix12_bfrop_print_uint(char **output, char *prefix, uint *src, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_print_uint8(char **output, char *prefix, uint8_t *src, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_print_uint16(char **output, char *prefix, uint16_t *src, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_print_uint32(char **output, char *prefix, uint32_t *src, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_print_uint64(char **output, char *prefix, uint64_t *src, pmix_data_type_t type);

pmix_status_t pmix12_bfrop_print_float(char **output, char *prefix, float *src, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_print_double(char **output, char *prefix, double *src, pmix_data_type_t type);

pmix_status_t pmix12_bfrop_print_timeval(char **output, char *prefix, struct timeval *src, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_print_time(char **output, char *prefix, time_t *src, pmix_data_type_t type);

pmix_status_t pmix12_bfrop_print_value(char **output, char *prefix, pmix_value_t *src, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_print_array(char **output, char *prefix,
                                      pmix_info_array_t *src, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_print_proc(char **output, char *prefix,
                                     pmix_proc_t *src, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_print_app(char **output, char *prefix,
                                    pmix_app_t *src, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_print_info(char **output, char *prefix,
                                     pmix_info_t *src, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_print_buf(char **output, char *prefix,
                                    pmix_buffer_t *src, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_print_kval(char **output, char *prefix,
                                     pmix_kval_t *src, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_print_modex(char **output, char *prefix,
                                      pmix_modex_data_t *src, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_print_persist(char **output, char *prefix,
                                        pmix_persistence_t *src, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_print_bo(char **output, char *prefix,
                                   pmix_byte_object_t *src, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_print_pdata(char **output, char *prefix,
                                      pmix_pdata_t *src, pmix_data_type_t type);
/* compatibility functions - no corresponding PMIx v1.x definitions */
pmix_status_t pmix12_bfrop_print_scope(char **output, char *prefix,
                                      pmix_scope_t *src, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_print_status(char **output, char *prefix,
                                       pmix_status_t *src, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_print_ptr(char **output, char *prefix,
                                    void *src, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_print_cmd(char **output, char *prefix,
                                    void *src, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_print_info_directives(char **output, char *prefix,
                                                void *src, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_print_datatype(char **output, char *prefix,
                                         pmix_data_type_t *src, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_print_proc_state(char **output, char *prefix,
                                           pmix_data_type_t *src, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_print_darray(char **output, char *prefix,
                                       pmix_data_array_t *src, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_print_proc_info(char **output, char *prefix,
                                          pmix_proc_info_t *src, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_print_query(char **output, char *prefix,
                                      pmix_query_t *src, pmix_data_type_t type);
pmix_status_t pmix12_bfrop_print_rank(char **output, char *prefix,
                                     pmix_rank_t *src, pmix_data_type_t type);


/*
 * Internal helper functions
 */
pmix_status_t pmix12_bfrop_store_data_type(pmix_buffer_t *buffer, pmix_data_type_t type);

pmix_status_t pmix12_bfrop_get_data_type(pmix_buffer_t *buffer, pmix_data_type_t *type);

int pmix12_v2_to_v1_datatype(pmix_data_type_t v2type);

pmix_data_type_t pmix12_v1_to_v2_datatype(int v1type);

END_C_DECLS

#endif
