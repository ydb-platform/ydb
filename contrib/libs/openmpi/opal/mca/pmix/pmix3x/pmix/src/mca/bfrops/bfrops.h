/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, Inc. All rights reserved.
 * Copyright (c) 2013-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016      Mellanox Technologies, Inc.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 *
 * Data packing subsystem.
 */

#ifndef PMIX_BFROP_H_
#define PMIX_BFROP_H_

#include <src/include/pmix_config.h>
#include <pmix_common.h>
#include <src/include/types.h>

#include "src/mca/mca.h"

#include "bfrops_types.h"

BEGIN_C_DECLS

/* The overall objective of this framework is to provide seamless
 * cross-version support for communications by allowing a process
 * to communicate with a peer:
 *
 * (a) using a different version of the buffer operations. We are
 *     allowing changes in the structure compositions and/or data
 *     type definitions between versions. This specifically affects
 *     our ability to pack/unpack across versions.
 *
 * (b) using a different buffer type (described vs non-described).
 *     This resolves conflicts when one side was compiled with a
 *     debug option, while the other side has been "optimized".
 *
 * This is a mult-select framework - i.e., multiple components
 * are selected and "active" at the same time. The intent is
 * to have one component for each data type variation, with the
 * expectation that the community will do its best not to revise
 * existing data type definitions. Thus, new variations should be
 * rare, and only a few components will exist.
 *
 * The framework itself reflects the fact that any given peer
 * will utilize only one variation of the data type definitions.
 * Thus, once a peer is identified, it will pass its version string
 * to this framework's "assign_module" function, which will then
 * pass it to each component until one returns a module capable of
 * processing the given version. This module is then "attached" to
 * the pmix_peer_t object so it can be used for all subsequent
 * communication to/from that peer.
 *
 * Buffer type is included in the buffer metadata. Unfortunately,
 * the metadata is not communicated at each exchange. Thus, the
 * peers will indicate during the connection handshake the type
 * of buffer they will use for all subsequent communications. The
 * peer must then utilize that same buffer type for all messages
 * sent to that remote proc, so we provide new macros for creating
 * and constructing buffers that ensures the correct buffer type
 * is marked.
 *
 * Accordingly, there are two levels of APIs defined for this
 * framework:
 *
 * (a) component level - these allow for init/finalize of the
 *     component, and assignment of a module to a given peer
 *     based on the version that peer is using
 *
 * (b) module level - implement pack/unpack/copy/recv/etc. of
 *     the various datatypes. Note that the module only needs
 *     to provide those functions that differ from the base
 *     functions - they don't need to duplicate all that code!
 */


/* The following functions are exposed to the user - they
 * therefore are implemented in the bfrops/base functions
 * as wrappers to the real functions.
 *
 * NOTE: THESE FUNCTIONS ARE NOT TO BE USED INTERNALLY -
 * USE THE MACROS INSTEAD
 */
bool pmix_value_cmp(pmix_value_t *p, pmix_value_t *p1);



/****    MODULE INTERFACE DEFINITION    ****/

/* initialize the module - the module is expected
 * to register its datatype functions at this time */
typedef pmix_status_t (*pmix_bfrop_init_fn_t)(void);

/* finalize the module */
typedef void (*pmix_bfrop_finalize_fn_t)(void);

/**
 * Pack one or more values into a buffer.
 *
 * The pack function packs one or more values of a specified type into
 * the specified buffer.  The buffer must have already been
 * initialized via an PMIX_NEW or PMIX_CONSTRUCT call - otherwise, the
 * pack_value function will return an error. Providing an unsupported
 * type flag will likewise be reported as an error.
 *
 * Note that any data to be packed that is not hard type cast (i.e.,
 * not type cast to a specific size) may lose precision when unpacked
 * by a non-homogeneous recipient.  The BFROP will do its best to deal
 * with heterogeneity issues between the packer and unpacker in such
 * cases. Sending a number larger than can be handled by the recipient
 * will return an error code (generated by the BFROP upon unpacking) -
 * the BFROP cannot detect such errors during packing.
 *
 * @param *buffer A pointer to the buffer into which the value is to
 * be packed.
 *
 * @param *src A void* pointer to the data that is to be packed. This
 * is interpreted as a pointer to an array of that data type containing
 * length num_values. Note that strings are of data type char*, and so
 * they are to be passed as (char **) - i.e., the caller must
 * pass the address of the pointer to the string as the void*. This
 * allows the BFROP to use a single interface function, but still allow
 * the caller to pass multiple strings in a single call.
 *
 * @param num_values An int32_t indicating the number of values that are
 * to be packed, beginning at the location pointed to by src. A string
 * value is counted as a single value regardless of length. The values
 * must be contiguous in memory. Arrays of pointers (e.g., string
 * arrays) should be contiguous, although (obviously) the data pointed
 * to need not be contiguous across array entries.
 *
 * @param type The type of the data to be packed - must be one of the
 * PMIX defined data types.
 *
 * @retval PMIX_SUCCESS The data was packed as requested.
 *
 * @retval PMIX_ERROR(s) An appropriate PMIX error code indicating the
 * problem encountered. This error code should be handled
 * appropriately.
 *
 * @code
 * pmix_buffer_t *buffer;
 * int32_t src;
 *
 * status_code = pmix_bfrop.pack(buffer, &src, 1, PMIX_INT32);
 * @endcode
 */
typedef pmix_status_t (*pmix_bfrop_pack_fn_t)(pmix_buffer_t *buffer,
                                              const void *src,
                                              int32_t num_values,
                                              pmix_data_type_t type);

/**
 * Unpack values from a buffer.
 *
 * The unpack function unpacks the next value (or values) of a
 * specified type from the specified buffer.
 *
 * The buffer must have already been initialized via an PMIX_NEW or
 * PMIX_CONSTRUCT call (and assumedly filled with some data) -
 * otherwise, the unpack_value function will return an
 * error. Providing an unsupported type flag will likewise be reported
 * as an error, as will specifying a data type that DOES NOT match the
 * type of the next item in the buffer. An attempt to read beyond the
 * end of the stored data held in the buffer will also return an
 * error.
 *
 * NOTE: it is possible for the buffer to be corrupted and that
 * the BFROP will *think* there is a proper variable type at the
 * beginning of an unpack region - but that the value is bogus (e.g., just
 * a byte field in a string array that so happens to have a value that
 * matches the specified data type flag). Therefore, the data type error check
 * is NOT completely safe. This is true for ALL unpack functions.
 *
 * Warning: The caller is responsible for providing adequate memory
 * storage for the requested data. As noted below, the user
 * must provide a parameter indicating the maximum number of values that
 * can be unpacked into the allocated memory. If more values exist in the
 * buffer than can fit into the memory storage, then the bfrop will unpack
 * what it can fit into that location and return an error code indicating
 * that the buffer was only partially unpacked.
 *
 * Note that any data that was not hard type cast (i.e., not type cast
 * to a specific size) when packed may lose precision when unpacked by
 * a non-homogeneous recipient.  The BFROP will do its best to deal with
 * heterogeneity issues between the packer and unpacker in such
 * cases. Sending a number larger than can be handled by the recipient
 * will return an error code generated by the BFROP upon unpacking - the
 * BFROP cannot detect such errors during packing.
 *
 * @param *buffer A pointer to the buffer from which the value will be
 * extracted.
 *
 * @param *dest A void* pointer to the memory location into which the
 * data is to be stored. Note that these values will be stored
 * contiguously in memory. For strings, this pointer must be to (char
 * **) to provide a means of supporting multiple string
 * operations. The BFROP unpack function will allocate memory for each
 * string in the array - the caller must only provide adequate memory
 * for the array of pointers.
 *
 * @param type The type of the data to be unpacked - must be one of
 * the BFROP defined data types.
 *
 * @retval *max_num_values The number of values actually unpacked. In
 * most cases, this should match the maximum number provided in the
 * parameters - but in no case will it exceed the value of this
 * parameter.  Note that if you unpack fewer values than are actually
 * available, the buffer will be in an unpackable state - the bfrop will
 * return an error code to warn of this condition.
 *
 * @note The unpack function will return the actual number of values
 * unpacked in this location.
 *
 * @retval PMIX_SUCCESS The next item in the buffer was successfully
 * unpacked.
 *
 * @retval PMIX_ERROR(s) The unpack function returns an error code
 * under one of several conditions: (a) the number of values in the
 * item exceeds the max num provided by the caller; (b) the type of
 * the next item in the buffer does not match the type specified by
 * the caller; or (c) the unpack failed due to either an error in the
 * buffer or an attempt to read past the end of the buffer.
 *
 * @code
 * pmix_buffer_t *buffer;
 * int32_t dest;
 * char **string_array;
 * int32_t num_values;
 *
 * num_values = 1;
 * status_code = pmix_bfrop.unpack(buffer, (void*)&dest, &num_values, PMIX_INT32);
 *
 * num_values = 5;
 * string_array = malloc(num_values*sizeof(char *));
 * status_code = pmix_bfrop.unpack(buffer, (void*)(string_array), &num_values, PMIX_STRING);
 *
 * @endcode
 */
typedef pmix_status_t (*pmix_bfrop_unpack_fn_t)(pmix_buffer_t *buffer, void *dest,
                                                int32_t *max_num_values,
                                                pmix_data_type_t type);
/**
 * Copy a payload from one buffer to another
 * This function will append a copy of the payload in one buffer into
 * another buffer. If the destination buffer is NOT empty, then the
 * type of the two buffers MUST match or else an
 * error will be returned. If the destination buffer IS empty, then
 * its type will be set to that of the source buffer.
 * NOTE: This is NOT a destructive procedure - the
 * source buffer's payload will remain intact, as will any pre-existing
 * payload in the destination's buffer.
 */
typedef pmix_status_t (*pmix_bfrop_copy_payload_fn_t)(pmix_buffer_t *dest,
                                                      pmix_buffer_t *src);

/**
 * Copy a data value from one location to another.
 *
 * Since registered data types can be complex structures, the system
 * needs some way to know how to copy the data from one location to
 * another (e.g., for storage in the registry). This function, which
 * can call other copy functions to build up complex data types, defines
 * the method for making a copy of the specified data type.
 *
 * @param **dest The address of a pointer into which the
 * address of the resulting data is to be stored.
 *
 * @param *src A pointer to the memory location from which the
 * data is to be copied.
 *
 * @param type The type of the data to be copied - must be one of
 * the BFROP defined data types.
 *
 * @retval PMIX_SUCCESS The value was successfully copied.
 *
 * @retval PMIX_ERROR(s) An appropriate error code.
 *
 */
typedef pmix_status_t (*pmix_bfrop_copy_fn_t)(void **dest, void *src,
                                              pmix_data_type_t type);

/**
 * Print a data value.
 *
 * Since registered data types can be complex structures, the system
 * needs some way to know how to print them (i.e., convert them to a string
 * representation). Provided for debug purposes.
 *
 * @retval PMIX_SUCCESS The value was successfully printed.
 *
 * @retval PMIX_ERROR(s) An appropriate error code.
 */
typedef pmix_status_t (*pmix_bfrop_print_fn_t)(char **output, char *prefix,
                                               void *src, pmix_data_type_t type);

/**
 * Transfer a value from one pmix_value_t to another. Ordinarily,
 * this would be executed as a base function. However, it is
 * possible that future versions may add new data types, and
 * thus the xfer function may differ
 *
 * @retval PMIX_SUCCESS The value was successfully transferred
 *
 * @retval PMIX_ERROR(s) An appropriate error code
 */
typedef pmix_status_t (*pmix_bfrop_value_xfer_fn_t)(pmix_value_t *dest,
                                                    const pmix_value_t *src);


/**
 * Load data into a pmix_value_t object. Again, this is provided
 * as a component function to support different data types
 */
typedef void (*pmix_bfrop_value_load_fn_t)(pmix_value_t *v, const void *data,
                                           pmix_data_type_t type);

/**
 * Unload data from a pmix_value_t object
 *
 * @retval PMIX_SUCCESS The value was successfully unloaded
 *
 * @retval PMIX_ERROR(s) An appropriate error code
 */
typedef pmix_status_t (*pmix_bfrop_value_unload_fn_t)(pmix_value_t *kv,
                                                      void **data, size_t *sz);

/**
 * Compare two pmix_value_t structs
 */
typedef pmix_value_cmp_t (*pmix_bfrop_value_cmp_fn_t)(pmix_value_t *p1, pmix_value_t *p2);

/* define a component-level API for registering a new
 * datatype, providing all the required functions */
typedef pmix_status_t (*pmix_bfrop_base_register_fn_t)(const char *name, pmix_data_type_t type,
                                                       pmix_bfrop_pack_fn_t pack,
                                                       pmix_bfrop_unpack_fn_t unpack,
                                                       pmix_bfrop_copy_fn_t copy,
                                                       pmix_bfrop_print_fn_t print);

/* return the string name of a provided data type */
typedef const char* (*pmix_bfrop_data_type_string_fn_t)(pmix_data_type_t type);

/**
 * Base structure for a BFROP module
 */
typedef struct {
    char *version;
    pmix_bfrop_init_fn_t              init;
    pmix_bfrop_finalize_fn_t          finalize;
    pmix_bfrop_pack_fn_t              pack;
    pmix_bfrop_unpack_fn_t            unpack;
    pmix_bfrop_copy_fn_t              copy;
    pmix_bfrop_print_fn_t             print;
    pmix_bfrop_copy_payload_fn_t      copy_payload;
    pmix_bfrop_value_xfer_fn_t        value_xfer;
    pmix_bfrop_value_load_fn_t        value_load;
    pmix_bfrop_value_unload_fn_t      value_unload;
    pmix_bfrop_value_cmp_fn_t         value_cmp;
    pmix_bfrop_base_register_fn_t     register_type;
    pmix_bfrop_data_type_string_fn_t  data_type_string;
} pmix_bfrops_module_t;


/* get a list of available versions - caller must free results
 * when done */
PMIX_EXPORT char* pmix_bfrops_base_get_available_modules(void);

/* Select a bfrops module for a given version */
PMIX_EXPORT pmix_bfrops_module_t* pmix_bfrops_base_assign_module(const char *version);

/* provide a backdoor to access the framework debug output */
PMIX_EXPORT extern int pmix_bfrops_base_output;

/* MACROS FOR EXECUTING BFROPS FUNCTIONS */
#define PMIX_BFROPS_ASSIGN_TYPE(p, b)               \
    (b)->type = (p)->nptr->compat.type

#define PMIX_BFROPS_PACK(r, p, b, s, n, t)                          \
    do {                                                            \
        pmix_output_verbose(2, pmix_bfrops_base_output,             \
                            "[%s:%d] PACK version %s",              \
                            __FILE__, __LINE__,                     \
                            (p)->nptr->compat.bfrops->version);     \
        if (PMIX_BFROP_BUFFER_UNDEF == (b)->type) {                 \
            (b)->type = (p)->nptr->compat.type;                     \
            (r) = (p)->nptr->compat.bfrops->pack(b, s, n, t);       \
        } else if ((b)->type == (p)->nptr->compat.type) {           \
            (r) = (p)->nptr->compat.bfrops->pack(b, s, n, t);       \
        } else {                                                    \
            (r) = PMIX_ERR_PACK_MISMATCH;                           \
        }                                                           \
    } while(0)

#define PMIX_BFROPS_UNPACK(r, p, b, d, m, t)                        \
    do {                                                            \
        pmix_output_verbose(2, pmix_bfrops_base_output,             \
                            "[%s:%d] UNPACK version %s",            \
                            __FILE__, __LINE__,                     \
                            (p)->nptr->compat.bfrops->version);     \
        if ((b)->type == (p)->nptr->compat.type) {                  \
            (r) = (p)->nptr->compat.bfrops->unpack(b, d, m, t);     \
        } else {                                                    \
            (r) = PMIX_ERR_UNPACK_FAILURE;                          \
        }                                                           \
    } while(0)

#define PMIX_BFROPS_COPY(r, p, d, s, t)             \
    (r) = (p)->nptr->compat.bfrops->copy(d, s, t)

#define PMIX_BFROPS_PRINT(r, p, o, pr, s, t)        \
    (r) = (p)->nptr->compat.bfrops->print(o, pr, s, t)

#define PMIX_BFROPS_COPY_PAYLOAD(r, p, d, s)                    \
    do {                                                        \
        if (PMIX_BFROP_BUFFER_UNDEF == (d)->type) {             \
            (d)->type = (p)->nptr->compat.type;                 \
            (r) = (p)->nptr->compat.bfrops->copy_payload(d, s); \
        } else if ((d)->type == (p)->nptr->compat.type) {       \
            (r) = (p)->nptr->compat.bfrops->copy_payload(d, s); \
        } else {                                                \
            (r) = PMIX_ERR_PACK_MISMATCH;                       \
        }                                                       \
    } while(0)

#define PMIX_BFROPS_VALUE_XFER(r, p, d, s)          \
    (r) = (p)->nptr->compat.bfrops->value_xfer(d, s)

#define PMIX_BFROPS_VALUE_LOAD(p, v, d, t)          \
    (p)->nptr->compat.bfrops->value_load(v, d, t)

#define PMIX_BFROPS_VALUE_UNLOAD(r, p, k, d, s)     \
    (r) = (p)->nptr->compat.bfrops->value_unload(k,, d, s)

#define PMIX_BFROPS_VALUE_CMP(r, p, q, s)           \
    (r) = (p)->nptr->compat.bfrops->value_cmp(q, s)

#define PMIX_BFROPS_REGISTER(r, p, n, t, pk, u, c, pr)   \
    (r) = (p)->nptr->compat.bfrops->register_type(n, t, pk, u, c, pr)

#define PMIX_BFROPS_PRINT_TYPE(c, p, t)             \
    (c) = (p)->nptr->compat.bfrops->data_type_string(t)


/****    COMPONENT STRUCTURE DEFINITION    ****/

/* define a component-level API for getting a module */
typedef pmix_bfrops_module_t* (*pmix_bfrop_base_component_assign_module_fn_t)(void);

/*
 * the standard component data structure
 */
struct pmix_bfrops_base_component_t {
    pmix_mca_base_component_t                           base;
    pmix_mca_base_component_data_t                      data;
    int                                                 priority;
    pmix_pointer_array_t                                types;
    pmix_bfrop_base_component_assign_module_fn_t        assign_module;
};
typedef struct pmix_bfrops_base_component_t pmix_bfrops_base_component_t;

/*
 * Macro for use in components that are of type bfrops
 */
#define PMIX_BFROPS_BASE_VERSION_1_0_0 \
    PMIX_MCA_BASE_VERSION_1_0_0("bfrops", 1, 0, 0)

END_C_DECLS

#endif /* PMIX_BFROP_H */
