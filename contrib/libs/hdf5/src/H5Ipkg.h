/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://www.hdfgroup.org/licenses.               *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 * Purpose: This file contains declarations which are visible only within
 *          the H5I package.  Source files outside the H5I package should
 *          include H5Iprivate.h instead.
 */
#if !(defined H5I_FRIEND || defined H5I_MODULE)
#error "Do not include this file outside the H5I package!"
#endif

#ifndef H5Ipkg_H
#define H5Ipkg_H

/* Get package's private header */
#include "H5Iprivate.h"

/**************************/
/* Package Private Macros */
/**************************/

/*
 * Number of bits to use for ID Type in each ID. Increase if more types
 * are needed (though this will decrease the number of available IDs per
 * type). This is the only number that must be changed since all other bit
 * field sizes and masks are calculated from TYPE_BITS.
 */
#define TYPE_BITS 7
#define TYPE_MASK (((hid_t)1 << TYPE_BITS) - 1)

#define H5I_MAX_NUM_TYPES TYPE_MASK

/*
 * Number of bits to use for the ID index in each ID (assumes 8-bit
 * bytes). We don't use the sign bit.
 */
#define ID_BITS ((sizeof(hid_t) * 8) - (TYPE_BITS + 1))
#define ID_MASK (((hid_t)1 << ID_BITS) - 1)

/* Map an ID to an ID type number */
#define H5I_TYPE(a) ((H5I_type_t)(((hid_t)(a) >> ID_BITS) & TYPE_MASK))

/****************************/
/* Package Private Typedefs */
/****************************/

/* ID information structure used */
typedef struct H5I_id_info_t {
    hid_t       id;        /* ID for this info */
    unsigned    count;     /* Ref. count for this ID */
    unsigned    app_count; /* Ref. count of application visible IDs */
    const void *object;    /* Pointer associated with the ID */

    /* Future ID info */
    bool                      is_future;  /* Whether this ID represents a future object */
    H5I_future_realize_func_t realize_cb; /* 'realize' callback for future object */
    H5I_future_discard_func_t discard_cb; /* 'discard' callback for future object */

    /* Hash table ID fields */
    bool           marked; /* Marked for deletion */
    UT_hash_handle hh;     /* Hash table handle (must be LAST) */
} H5I_id_info_t;

/* Type information structure used */
typedef struct H5I_type_info_t {
    const H5I_class_t *cls;          /* Pointer to ID class */
    unsigned           init_count;   /* # of times this type has been initialized */
    uint64_t           id_count;     /* Current number of IDs held */
    uint64_t           nextid;       /* ID to use for the next object */
    H5I_id_info_t     *last_id_info; /* Info for most recent ID looked up */
    H5I_id_info_t     *hash_table;   /* Hash table pointer for this ID type */
} H5I_type_info_t;

/*****************************/
/* Package Private Variables */
/*****************************/

/* Array of pointers to ID types */
H5_DLLVAR H5I_type_info_t *H5I_type_info_array_g[H5I_MAX_NUM_TYPES];

/* Variable to keep track of the number of types allocated.  Its value is the
 * next type ID to be handed out, so it is always one greater than the number
 * of types.
 * Starts at 1 instead of 0 because it makes trace output look nicer.  If more
 * types (or IDs within a type) are needed, adjust TYPE_BITS in H5Ipkg.h
 * and/or increase size of hid_t
 */
H5_DLLVAR int H5I_next_type_g;

/******************************/
/* Package Private Prototypes */
/******************************/

H5_DLL hid_t          H5I__register(H5I_type_t type, const void *object, bool app_ref,
                                    H5I_future_realize_func_t realize_cb, H5I_future_discard_func_t discard_cb);
H5_DLL int            H5I__destroy_type(H5I_type_t type);
H5_DLL void          *H5I__remove_verify(hid_t id, H5I_type_t type);
H5_DLL int            H5I__inc_type_ref(H5I_type_t type);
H5_DLL int            H5I__get_type_ref(H5I_type_t type);
H5_DLL H5I_id_info_t *H5I__find_id(hid_t id);

/* Testing functions */
#ifdef H5I_TESTING
H5_DLL ssize_t H5I__get_name_test(hid_t id, char *name /*out*/, size_t size, bool *cached);
#endif /* H5I_TESTING */

#endif /*H5Ipkg_H*/
