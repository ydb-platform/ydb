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
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */
#ifndef PMIX_BFROP_BASE_H_
#define PMIX_BFROP_BASE_H_

#include <src/include/pmix_config.h>


#ifdef HAVE_SYS_TIME_H
#include <sys/time.h> /* for struct timeval */
#endif
#ifdef HAVE_STRING_H
#include <string.h>
#endif

#include "src/class/pmix_pointer_array.h"
#include "src/mca/mca.h"
#include "src/mca/base/pmix_mca_base_framework.h"
#include "src/include/pmix_globals.h"
#include "src/mca/bfrops/bfrops.h"


 BEGIN_C_DECLS

/*
 * MCA Framework
 */
 PMIX_EXPORT extern pmix_mca_base_framework_t pmix_bfrops_base_framework;
/**
 * BFROP select function
 *
 * Cycle across available components and construct the list
 * of active modules
 */
 PMIX_EXPORT pmix_status_t pmix_bfrop_base_select(void);

/**
 * Track an active component / module
 */
struct pmix_bfrops_base_active_module_t {
    pmix_list_item_t super;
    pmix_status_t pri;
    pmix_bfrops_module_t *module;
    pmix_bfrops_base_component_t *component;
};
typedef struct pmix_bfrops_base_active_module_t pmix_bfrops_base_active_module_t;
PMIX_CLASS_DECLARATION(pmix_bfrops_base_active_module_t);


/* framework globals */
struct pmix_bfrops_globals_t {
  pmix_list_t actives;
  bool initialized;
  size_t initial_size;
  size_t threshold_size;
  pmix_bfrop_buffer_type_t default_type;
};
typedef struct pmix_bfrops_globals_t pmix_bfrops_globals_t;

PMIX_EXPORT extern pmix_bfrops_globals_t pmix_bfrops_globals;

/*
 * The default starting chunk size
 */
#define PMIX_BFROP_DEFAULT_INITIAL_SIZE  128
/*
 * The default threshold size when we switch from doubling the
 * buffer size to additively increasing it
 */
#define PMIX_BFROP_DEFAULT_THRESHOLD_SIZE 1024

/*
 * Internal type corresponding to size_t.  Do not use this in
 * interface calls - use PMIX_SIZE instead.
 */
#if SIZEOF_SIZE_T == 1
#define BFROP_TYPE_SIZE_T PMIX_UINT8
#elif SIZEOF_SIZE_T == 2
#define BFROP_TYPE_SIZE_T PMIX_UINT16
#elif SIZEOF_SIZE_T == 4
#define BFROP_TYPE_SIZE_T PMIX_UINT32
#elif SIZEOF_SIZE_T == 8
#define BFROP_TYPE_SIZE_T PMIX_UINT64
#else
#error Unsupported size_t size!
#endif

/*
 * Internal type corresponding to bool.  Do not use this in interface
 * calls - use PMIX_BOOL instead.
 */
#if SIZEOF__BOOL == 1
#define BFROP_TYPE_BOOL PMIX_UINT8
#elif SIZEOF__BOOL == 2
#define BFROP_TYPE_BOOL PMIX_UINT16
#elif SIZEOF__BOOL == 4
#define BFROP_TYPE_BOOL PMIX_UINT32
#elif SIZEOF__BOOL == 8
#define BFROP_TYPE_BOOL PMIX_UINT64
#else
#error Unsupported bool size!
#endif

/*
 * Internal type corresponding to int and unsigned int.  Do not use
 * this in interface calls - use PMIX_INT / PMIX_UINT instead.
 */
#if SIZEOF_INT == 1
#define BFROP_TYPE_INT PMIX_INT8
#define BFROP_TYPE_UINT PMIX_UINT8
#elif SIZEOF_INT == 2
#define BFROP_TYPE_INT PMIX_INT16
#define BFROP_TYPE_UINT PMIX_UINT16
#elif SIZEOF_INT == 4
#define BFROP_TYPE_INT PMIX_INT32
#define BFROP_TYPE_UINT PMIX_UINT32
#elif SIZEOF_INT == 8
#define BFROP_TYPE_INT PMIX_INT64
#define BFROP_TYPE_UINT PMIX_UINT64
#else
#error Unsupported INT size!
#endif

/*
 * Internal type corresponding to pid_t.  Do not use this in interface
 * calls - use PMIX_PID instead.
 */
#if SIZEOF_PID_T == 1
#define BFROP_TYPE_PID_T PMIX_UINT8
#elif SIZEOF_PID_T == 2
#define BFROP_TYPE_PID_T PMIX_UINT16
#elif SIZEOF_PID_T == 4
#define BFROP_TYPE_PID_T PMIX_UINT32
#elif SIZEOF_PID_T == 8
#define BFROP_TYPE_PID_T PMIX_UINT64
#else
#error Unsupported pid_t size!
#endif

/* Unpack generic size macros */
#define PMIX_BFROP_UNPACK_SIZE_MISMATCH(unpack_type, remote_type, ret)                     \
    do {                                                                        \
        switch(remote_type) {                                                   \
            case PMIX_UINT8:                                                    \
                PMIX_BFROP_UNPACK_SIZE_MISMATCH_FOUND(unpack_type, uint8_t, remote_type);  \
                break;                                                          \
            case PMIX_INT8:                                                     \
                PMIX_BFROP_UNPACK_SIZE_MISMATCH_FOUND(unpack_type, int8_t, remote_type);   \
                break;                                                          \
            case PMIX_UINT16:                                                   \
                PMIX_BFROP_UNPACK_SIZE_MISMATCH_FOUND(unpack_type, uint16_t, remote_type); \
                break;                                                          \
            case PMIX_INT16:                                                    \
                PMIX_BFROP_UNPACK_SIZE_MISMATCH_FOUND(unpack_type, int16_t, remote_type);  \
                break;                                                          \
            case PMIX_UINT32:                                                   \
                PMIX_BFROP_UNPACK_SIZE_MISMATCH_FOUND(unpack_type, uint32_t, remote_type); \
                break;                                                          \
            case PMIX_INT32:                                                    \
                PMIX_BFROP_UNPACK_SIZE_MISMATCH_FOUND(unpack_type, int32_t, remote_type);  \
                break;                                                          \
            case PMIX_UINT64:                                                   \
                PMIX_BFROP_UNPACK_SIZE_MISMATCH_FOUND(unpack_type, uint64_t, remote_type); \
                break;                                                          \
            case PMIX_INT64:                                                    \
                PMIX_BFROP_UNPACK_SIZE_MISMATCH_FOUND(unpack_type, int64_t, remote_type);  \
                break;                                                          \
            default:                                                            \
                ret = PMIX_ERR_NOT_FOUND;                                       \
        }                                                                       \
    } while (0)

/* NOTE: do not need to deal with endianness here, as the unpacking of
   the underling sender-side type will do that for us.  Repeat: the
   data in tmpbuf[] is already in host byte order. */
#define PMIX_BFROP_UNPACK_SIZE_MISMATCH_FOUND(unpack_type, tmptype, tmpbfroptype)      \
    do {                                                                    \
        int32_t i;                                                          \
        tmptype *tmpbuf = (tmptype*)malloc(sizeof(tmptype) * (*num_vals));  \
        ret = unpack_gentype(buffer, tmpbuf, num_vals, tmpbfroptype);       \
        for (i = 0 ; i < *num_vals ; ++i) {                                 \
            ((unpack_type*) dest)[i] = (unpack_type)(tmpbuf[i]);            \
        }                                                                   \
        free(tmpbuf);                                                       \
    } while (0)

/* for backwards compatibility */
typedef struct pmix_info_array {
    size_t size;
    pmix_info_t *array;
} pmix_info_array_t;


/**
 * Internal struct used for holding registered bfrop functions
 */
 typedef struct {
    pmix_object_t super;
    /* type identifier */
    pmix_data_type_t odti_type;
    /** Debugging string name */
    char *odti_name;
    /** Pack function */
    pmix_bfrop_pack_fn_t odti_pack_fn;
    /** Unpack function */
    pmix_bfrop_unpack_fn_t odti_unpack_fn;
    /** copy function */
    pmix_bfrop_copy_fn_t odti_copy_fn;
    /** prpmix_status_t function */
    pmix_bfrop_print_fn_t odti_print_fn;
} pmix_bfrop_type_info_t;
PMIX_EXPORT PMIX_CLASS_DECLARATION(pmix_bfrop_type_info_t);

/* macro for registering data types - overwrite an existing
 * duplicate one based on type name */
#define PMIX_REGISTER_TYPE(n, t, p, u, c, pr, arr)                  \
 do {                                                               \
    pmix_bfrop_type_info_t *_info;                                  \
    _info = PMIX_NEW(pmix_bfrop_type_info_t);                       \
    _info->odti_name = strdup((n));                                 \
    _info->odti_type = (t);                                         \
    _info->odti_pack_fn = (pmix_bfrop_pack_fn_t)(p);                \
    _info->odti_unpack_fn = (pmix_bfrop_unpack_fn_t)(u);            \
    _info->odti_copy_fn = (pmix_bfrop_copy_fn_t)(c) ;               \
    _info->odti_print_fn = (pmix_bfrop_print_fn_t)(pr) ;            \
    pmix_pointer_array_set_item((arr), (t), _info);                 \
} while (0)

/* API Stub functions */
PMIX_EXPORT char* pmix_bfrops_stub_get_available_modules(void);
PMIX_EXPORT pmix_status_t pmix_bfrops_stub_assign_module(struct pmix_peer_t *peer,
                                             const char *version);
PMIX_EXPORT pmix_buffer_t* pmix_bfrops_stub_create_buffer(struct pmix_peer_t *pr);
PMIX_EXPORT void pmix_bfrops_construct_buffer(struct pmix_peer_t *pr,
                                              pmix_buffer_t *buf);
PMIX_EXPORT pmix_status_t pmix_bfrops_stub_pack(struct pmix_peer_t *peer,
                                                pmix_buffer_t *buffer,
                                                const void *src,
                                                int32_t num_values,
                                                pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_stub_unpack(struct pmix_peer_t *peer,
                                                  pmix_buffer_t *buffer, void *dest,
                                                  int32_t *max_num_values,
                                                  pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_stub_copy(struct pmix_peer_t *peer,
                                                void **dest, void *src,
                                                pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_stub_print(struct pmix_peer_t *peer,
                                                 char **output, char *prefix,
                                                 void *src, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_stub_copy_payload(struct pmix_peer_t *peer,
                                                        pmix_buffer_t *dest,
                                                        pmix_buffer_t *src);
PMIX_EXPORT pmix_status_t pmix_bfrops_stub_value_xfer(struct pmix_peer_t *peer,
                                                      pmix_value_t *dest,
                                                      const pmix_value_t *src);
PMIX_EXPORT void pmix_bfrops_stub_value_load(struct pmix_peer_t *peer,
                                             pmix_value_t *v, void *data,
                                             pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_stub_value_unload(struct pmix_peer_t *peer,
                                                        pmix_value_t *kv,
                                                        void **data, size_t *sz);
PMIX_EXPORT pmix_value_cmp_t pmix_bfrops_stub_value_cmp(struct pmix_peer_t *peer,
                                                        pmix_value_t *p1, pmix_value_t *p2);
PMIX_EXPORT pmix_status_t pmix_bfrops_stub_register_type(struct pmix_peer_t *peer,
                                                         const char *name, pmix_data_type_t type,
                                                         pmix_bfrop_pack_fn_t pack,
                                                         pmix_bfrop_unpack_fn_t unpack,
                                                         pmix_bfrop_copy_fn_t copy,
                                                         pmix_bfrop_print_fn_t print);

/* data type string function */
PMIX_EXPORT const char* pmix_bfrops_base_data_type_string(pmix_pointer_array_t *regtypes,
                                                          pmix_data_type_t type);

/*
 * "Standard" pack functions
 */
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack(pmix_pointer_array_t *regtypes,
                                                pmix_buffer_t *buffer,
                                                const void *src, int num_vals,
                                                pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_buffer(pmix_pointer_array_t *regtypes,
                                                       pmix_buffer_t *buffer,
                                                       const void *src, int32_t num_vals,
                                                       pmix_data_type_t type);

PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_bool(pmix_buffer_t *buffer, const void *src,
                                                     int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_int(pmix_buffer_t *buffer, const void *src,
                                                    int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_sizet(pmix_buffer_t *buffer, const void *src,
                                                      int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_byte(pmix_buffer_t *buffer, const void *src,
                                                     int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_string(pmix_buffer_t *buffer, const void *src,
                                                       int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_pid(pmix_buffer_t *buffer, const void *src,
                                                    int32_t num_vals, pmix_data_type_t type);

PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_int16(pmix_buffer_t *buffer, const void *src,
                                                      int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_int32(pmix_buffer_t *buffer, const void *src,
                                                      int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_int64(pmix_buffer_t *buffer, const void *src,
                                                      int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_string(pmix_buffer_t *buffer, const void *src,
                                                       int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_float(pmix_buffer_t *buffer, const void *src,
                                                      int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_double(pmix_buffer_t *buffer, const void *src,
                                                       int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_timeval(pmix_buffer_t *buffer, const void *src,
                                                        int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_time(pmix_buffer_t *buffer, const void *src,
                                                     int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_status(pmix_buffer_t *buffer, const void *src,
                                                       int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_buf(pmix_buffer_t *buffer, const void *src,
                                                    int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_bo(pmix_buffer_t *buffer, const void *src,
                                                   int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_proc(pmix_buffer_t *buffer, const void *src,
                                                     int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_value(pmix_buffer_t *buffer, const void *src,
                                                      int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_info(pmix_buffer_t *buffer, const void *src,
                                                     int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_pdata(pmix_buffer_t *buffer, const void *src,
                                                      int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_app(pmix_buffer_t *buffer, const void *src,
                                                    int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_kval(pmix_buffer_t *buffer, const void *src,
                                                     int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_array(pmix_buffer_t *buffer, const void *src,
                                                      int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_modex(pmix_buffer_t *buffer, const void *src,
                                                      int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_persist(pmix_buffer_t *buffer, const void *src,
                                                        int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_datatype(pmix_buffer_t *buffer, const void *src,
                                                         int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_ptr(pmix_buffer_t *buffer, const void *src,
                                                    int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_scope(pmix_buffer_t *buffer, const void *src,
                                                      int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_range(pmix_buffer_t *buffer, const void *src,
                                                      int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_cmd(pmix_buffer_t *buffer, const void *src,
                                                    int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_info_directives(pmix_buffer_t *buffer, const void *src,
                                                                int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_pstate(pmix_buffer_t *buffer, const void *src,
                                                       int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_pinfo(pmix_buffer_t *buffer, const void *src,
                                                      int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_darray(pmix_buffer_t *buffer, const void *src,
                                                       int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_rank(pmix_buffer_t *buffer, const void *src,
                                                     int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_query(pmix_buffer_t *buffer, const void *src,
                                                      int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_val(pmix_buffer_t *buffer,
                                                    pmix_value_t *p);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_alloc_directive(pmix_buffer_t *buffer, const void *src,
                                                                int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_iof_channel(pmix_buffer_t *buffer, const void *src,
                                                            int32_t num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_pack_envar(pmix_buffer_t *buffer, const void *src,
                                                      int32_t num_vals, pmix_data_type_t type);

/*
* "Standard" unpack functions
*/
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack(pmix_pointer_array_t *regtypes,
                                                  pmix_buffer_t *buffer,
                                                  void *dst, int32_t *num_vals,
                                                  pmix_data_type_t type);

PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_bool(pmix_buffer_t *buffer, void *dest,
                                                       int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_byte(pmix_buffer_t *buffer, void *dest,
                                                       int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_string(pmix_buffer_t *buffer, void *dest,
                                                         int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_int(pmix_buffer_t *buffer, void *dest,
                                                      int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_sizet(pmix_buffer_t *buffer, void *dest,
                                                        int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_pid(pmix_buffer_t *buffer, void *dest,
                                                      int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_int16(pmix_buffer_t *buffer, void *dest,
                                                        int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_int32(pmix_buffer_t *buffer, void *dest,
                                                        int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_datatype(pmix_buffer_t *buffer, void *dest,
                                                           int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_int64(pmix_buffer_t *buffer, void *dest,
                                                        int32_t *num_vals, pmix_data_type_t type);

PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_float(pmix_buffer_t *buffer, void *dest,
                                                        int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_double(pmix_buffer_t *buffer, void *dest,
                                                         int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_timeval(pmix_buffer_t *buffer, void *dest,
                                                          int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_time(pmix_buffer_t *buffer, void *dest,
                                                       int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_status(pmix_buffer_t *buffer, void *dest,
                                                         int32_t *num_vals, pmix_data_type_t type);

PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_val(pmix_buffer_t *buffer,
                                                      pmix_value_t *val);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_value(pmix_buffer_t *buffer, void *dest,
                                                        int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_info(pmix_buffer_t *buffer, void *dest,
                                                       int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_pdata(pmix_buffer_t *buffer, void *dest,
                                                        int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_buf(pmix_buffer_t *buffer, void *dest,
                                                      int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_proc(pmix_buffer_t *buffer, void *dest,
                                                       int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_app(pmix_buffer_t *buffer, void *dest,
                                                      int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_kval(pmix_buffer_t *buffer, void *dest,
                                                       int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_modex(pmix_buffer_t *buffer, void *dest,
                                                        int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_persist(pmix_buffer_t *buffer, void *dest,
                                                          int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_bo(pmix_buffer_t *buffer, void *dest,
                                                     int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_ptr(pmix_buffer_t *buffer, void *dest,
                                                      int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_scope(pmix_buffer_t *buffer, void *dest,
                                                        int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_range(pmix_buffer_t *buffer, void *dest,
                                                        int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_cmd(pmix_buffer_t *buffer, void *dest,
                                                      int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_info_directives(pmix_buffer_t *buffer, void *dest,
                                                                  int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_datatype(pmix_buffer_t *buffer, void *dest,
                                                           int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_pstate(pmix_buffer_t *buffer, void *dest,
                                                         int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_pinfo(pmix_buffer_t *buffer, void *dest,
                                                        int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_darray(pmix_buffer_t *buffer, void *dest,
                                                         int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_rank(pmix_buffer_t *buffer, void *dest,
                                                       int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_query(pmix_buffer_t *buffer, void *dest,
                                                        int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_alloc_directive(pmix_buffer_t *buffer, void *dest,
                                                                  int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_iof_channel(pmix_buffer_t *buffer, void *dest,
                                                              int32_t *num_vals, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_envar(pmix_buffer_t *buffer, void *dest,
                                                        int32_t *num_vals, pmix_data_type_t type);
/**** DEPRECATED ****/
PMIX_EXPORT pmix_status_t pmix_bfrops_base_unpack_array(pmix_buffer_t *buffer, void *dest,
                                                        int32_t *num_vals, pmix_data_type_t type);

/*
* "Standard" copy functions
*/
PMIX_EXPORT pmix_status_t pmix_bfrops_base_copy(pmix_pointer_array_t *regtypes,
                                                void **dest, void *src,
                                                pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_copy_payload(pmix_buffer_t *dest,
                                                        pmix_buffer_t *src);

PMIX_EXPORT pmix_status_t pmix_bfrops_base_std_copy(void **dest, void *src,
                                                    pmix_data_type_t type);

PMIX_EXPORT pmix_status_t pmix_bfrops_base_copy_string(char **dest, char *src,
                                                       pmix_data_type_t type);

PMIX_EXPORT pmix_status_t pmix_bfrops_base_copy_value(pmix_value_t **dest,
                                                      pmix_value_t *src,
                                                      pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_copy_proc(pmix_proc_t **dest,
                                                     pmix_proc_t *src,
                                                     pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_copy_app(pmix_app_t **dest,
                                                    pmix_app_t *src,
                                                    pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_copy_info(pmix_info_t **dest,
                                                     pmix_info_t *src,
                                                     pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_copy_buf(pmix_buffer_t **dest,
                                                    pmix_buffer_t *src,
                                                    pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_copy_kval(pmix_kval_t **dest,
                                                     pmix_kval_t *src,
                                                     pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrop_base_copy_persist(pmix_persistence_t **dest,
                                                       pmix_persistence_t *src,
                                                       pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_copy_bo(pmix_byte_object_t **dest,
                                                   pmix_byte_object_t *src,
                                                   pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_copy_pdata(pmix_pdata_t **dest,
                                                      pmix_pdata_t *src,
                                                      pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_copy_pinfo(pmix_proc_info_t **dest,
                                                      pmix_proc_info_t *src,
                                                      pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_copy_darray(pmix_data_array_t **dest,
                                                       pmix_data_array_t *src,
                                                       pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_copy_query(pmix_query_t **dest,
                                                      pmix_query_t *src,
                                                      pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_copy_envar(pmix_envar_t **dest,
                                                      pmix_envar_t *src,
                                                      pmix_data_type_t type);

/*
* "Standard" print functions
*/
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print(pmix_pointer_array_t *regtypes,
                                                 char **output, char *prefix,
                                                 void *src, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_bool(char **output, char *prefix,
                                                      bool *src, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_byte(char **output, char *prefix,
                                                      uint8_t *src, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_string(char **output, char *prefix,
                                                        char *src, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_size(char **output, char *prefix,
                                                      size_t *src, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_pid(char **output, char *prefix,
                                                     pid_t *src, pmix_data_type_t type);

PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_int(char **output, char *prefix,
                                                     int *src, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_int8(char **output, char *prefix,
                                                      int8_t *src, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_int16(char **output, char *prefix,
                                                       int16_t *src, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_int32(char **output, char *prefix,
                                                       int32_t *src, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_int64(char **output, char *prefix,
                                                       int64_t *src, pmix_data_type_t type);

PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_uint(char **output, char *prefix,
                                                      uint *src, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_uint8(char **output, char *prefix,
                                                       uint8_t *src, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_uint16(char **output, char *prefix,
                                                        uint16_t *src, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_uint32(char **output, char *prefix,
                                                        uint32_t *src, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_uint64(char **output, char *prefix,
                                                        uint64_t *src, pmix_data_type_t type);

PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_float(char **output, char *prefix,
                                                       float *src, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_double(char **output, char *prefix,
                                                        double *src, pmix_data_type_t type);

PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_timeval(char **output, char *prefix,
                                                         struct timeval *src, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_time(char **output, char *prefix,
                                                      time_t *src, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_status(char **output, char *prefix,
                                                        pmix_status_t *src, pmix_data_type_t type);

PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_value(char **output, char *prefix,
                                                       pmix_value_t *src, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_proc(char **output, char *prefix,
                                                      pmix_proc_t *src, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_app(char **output, char *prefix,
                                                     pmix_app_t *src, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_info(char **output, char *prefix,
                                                      pmix_info_t *src, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_buf(char **output, char *prefix,
                                                     pmix_buffer_t *src, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_kval(char **output, char *prefix,
                                                      pmix_kval_t *src, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_persist(char **output, char *prefix,
                                                         pmix_persistence_t *src, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_bo(char **output, char *prefix,
                                                    pmix_byte_object_t *src, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_pdata(char **output, char *prefix,
                                                       pmix_pdata_t *src, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_ptr(char **output, char *prefix,
                                                     void *src, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_scope(char **output, char *prefix,
                                                       pmix_scope_t *src, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_range(char **output, char *prefix,
                                                       pmix_data_range_t *src, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_cmd(char **output, char *prefix,
                                                     pmix_cmd_t *src, pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_info_directives(char **output, char *prefix,
                                                                 pmix_info_directives_t *src,
                                                                 pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_datatype(char **output, char *prefix,
                                                          pmix_data_type_t *src,
                                                          pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_pstate(char **output, char *prefix,
                                                        pmix_proc_state_t *src,
                                                        pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_pinfo(char **output, char *prefix,
                                                       pmix_proc_info_t *src,
                                                       pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_darray(char **output, char *prefix,
                                                        pmix_data_array_t *src,
                                                        pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_query(char **output, char *prefix,
                                                       pmix_query_t *src,
                                                       pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_rank(char **output, char *prefix,
                                                      pmix_rank_t *src,
                                                      pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_alloc_directive(char **output, char *prefix,
                                                                 pmix_alloc_directive_t *src,
                                                                 pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_iof_channel(char **output, char *prefix,
                                                            pmix_iof_channel_t *src,
                                                            pmix_data_type_t type);
PMIX_EXPORT pmix_status_t pmix_bfrops_base_print_envar(char **output, char *prefix,
                                                       pmix_envar_t *src,
                                                       pmix_data_type_t type);

/*
 * Common helper functions
 */

PMIX_EXPORT char* pmix_bfrop_buffer_extend(pmix_buffer_t *bptr, size_t bytes_to_add);

PMIX_EXPORT bool pmix_bfrop_too_small(pmix_buffer_t *buffer, size_t bytes_reqd);

PMIX_EXPORT pmix_status_t pmix_bfrop_store_data_type(pmix_buffer_t *buffer, pmix_data_type_t type);

PMIX_EXPORT pmix_status_t pmix_bfrop_get_data_type(pmix_buffer_t *buffer, pmix_data_type_t *type);

PMIX_EXPORT pmix_status_t pmix_bfrops_base_copy_payload(pmix_buffer_t *dest,
                                                        pmix_buffer_t *src);

PMIX_EXPORT void pmix_bfrops_base_value_load(pmix_value_t *v, const void *data,
                                             pmix_data_type_t type);

PMIX_EXPORT pmix_status_t pmix_bfrops_base_value_unload(pmix_value_t *kv,
                                                        void **data,
                                                        size_t *sz);

PMIX_EXPORT pmix_status_t pmix_bfrops_base_value_xfer(pmix_value_t *p,
                                                      const pmix_value_t *src);

PMIX_EXPORT pmix_value_cmp_t pmix_bfrops_base_value_cmp(pmix_value_t *p,
                                                        pmix_value_t *p1);

END_C_DECLS

#endif
