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
 * This file contains private information about the H5SL module
 */
#ifndef H5SLprivate_H
#define H5SLprivate_H

/**************************************/
/* Public headers needed by this file */
/**************************************/

/***************************************/
/* Private headers needed by this file */
/***************************************/
#include "H5private.h"

/************/
/* Typedefs */
/************/

/* Typedefs for skip list struct (defined in H5SL.c) */
typedef struct H5SL_t      H5SL_t;
typedef struct H5SL_node_t H5SL_node_t;

/* Typedef for kinds of skip lists supported */
typedef enum {
    H5SL_TYPE_INT,      /* Skip list keys are 'int's */
    H5SL_TYPE_HADDR,    /* Skip list keys are 'haddr_t's */
    H5SL_TYPE_STR,      /* Skip list keys are 'char *'s (ie. strings) */
    H5SL_TYPE_HSIZE,    /* Skip list keys are 'hsize_t's */
    H5SL_TYPE_UNSIGNED, /* Skip list keys are 'unsigned's */
    H5SL_TYPE_SIZE,     /* Skip list keys are 'size_t's */
    H5SL_TYPE_OBJ,      /* Skip list keys are 'H5_obj_t's */
    H5SL_TYPE_HID,      /* Skip list keys are 'hid_t's */
    H5SL_TYPE_GENERIC   /* Skip list keys are unknown, comparison callback supplied */
} H5SL_type_t;

/**********/
/* Macros */
/**********/

/* Typedef for comparison operations */
typedef int (*H5SL_cmp_t)(const void *key1, const void *key2);

/* Typedef for iteration operations */
typedef herr_t (*H5SL_operator_t)(void *item, void *key, void *operator_data /*in,out*/);

/********************/
/* Private routines */
/********************/
H5_DLL H5SL_t      *H5SL_create(H5SL_type_t type, H5SL_cmp_t cmp);
H5_DLL size_t       H5SL_count(H5SL_t *slist);
H5_DLL herr_t       H5SL_insert(H5SL_t *slist, void *item, const void *key);
H5_DLL H5SL_node_t *H5SL_add(H5SL_t *slist, void *item, const void *key);
H5_DLL void        *H5SL_remove(H5SL_t *slist, const void *key);
H5_DLL void        *H5SL_remove_first(H5SL_t *slist);
H5_DLL void        *H5SL_search(H5SL_t *slist, const void *key);
H5_DLL void        *H5SL_less(H5SL_t *slist, const void *key);
H5_DLL void        *H5SL_greater(H5SL_t *slist, const void *key);
H5_DLL H5SL_node_t *H5SL_find(H5SL_t *slist, const void *key);
H5_DLL H5SL_node_t *H5SL_below(H5SL_t *slist, const void *key);
H5_DLL H5SL_node_t *H5SL_above(H5SL_t *slist, const void *key);
H5_DLL H5SL_node_t *H5SL_first(H5SL_t *slist);
H5_DLL H5SL_node_t *H5SL_next(H5SL_node_t *slist_node);
H5_DLL H5SL_node_t *H5SL_prev(H5SL_node_t *slist_node);
H5_DLL H5SL_node_t *H5SL_last(H5SL_t *slist);
H5_DLL void        *H5SL_item(H5SL_node_t *slist_node);
H5_DLL herr_t       H5SL_iterate(H5SL_t *slist, H5SL_operator_t op, void *op_data);
H5_DLL herr_t       H5SL_release(H5SL_t *slist);
H5_DLL herr_t       H5SL_free(H5SL_t *slist, H5SL_operator_t op, void *op_data);
H5_DLL herr_t       H5SL_close(H5SL_t *slist);
H5_DLL herr_t       H5SL_destroy(H5SL_t *slist, H5SL_operator_t op, void *op_data);
H5_DLL herr_t       H5SL_init(void);
H5_DLL int          H5SL_term_interface(void);

#endif /* H5SLprivate_H */
