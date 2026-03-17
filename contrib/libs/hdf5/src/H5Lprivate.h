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
 * This file contains private information about the H5L module
 * for dealing with links in an HDF5 file.
 */
#ifndef H5Lprivate_H
#define H5Lprivate_H

/* Include package's public headers */
#include "H5Lpublic.h"
#include "H5Ldevelop.h"

/* Private headers needed by this file */
#include "H5Gprivate.h" /* Groups                */
#include "H5Oprivate.h" /* Object headers            */

/**************************/
/* Library Private Macros */
/**************************/

/* Default number of soft links to traverse */
#define H5L_NUM_LINKS 16

/* ========  Link creation property names ======== */
#define H5L_CRT_INTERMEDIATE_GROUP_NAME "intermediate_group" /* Create intermediate groups flag */

/* ========  Link access property names ======== */
/* Number of soft links to traverse */
#define H5L_ACS_NLINKS_NAME "max soft links"
/* External link prefix */
#define H5L_ACS_ELINK_PREFIX_NAME "external link prefix"
/* file access property list for external link access */
#define H5L_ACS_ELINK_FAPL_NAME "external link fapl"
/* file access flags for external link traversal */
#define H5L_ACS_ELINK_FLAGS_NAME "external link flags"
/*  callback function for external link traversal */
#define H5L_ACS_ELINK_CB_NAME "external link callback"

/****************************/
/* Library Private Typedefs */
/****************************/

/* Structure for external link traversal callback property */
typedef struct H5L_elink_cb_t {
    H5L_elink_traverse_t func;
    void                *user_data;
} H5L_elink_cb_t;

/*****************************/
/* Library Private Variables */
/*****************************/

/******************************/
/* Library Private Prototypes */
/******************************/

/* General operations on links */
H5_DLL herr_t H5L_init(void);
H5_DLL herr_t H5L_link(const H5G_loc_t *new_loc, const char *new_name, H5G_loc_t *obj_loc, hid_t lcpl_id);
H5_DLL herr_t H5L_link_object(const H5G_loc_t *new_loc, const char *new_name, H5O_obj_create_t *ocrt_info,
                              hid_t lcpl_id);
H5_DLL herr_t H5L_exists_tolerant(const H5G_loc_t *loc, const char *name, bool *exists);
H5_DLL herr_t H5L_get_info(const H5G_loc_t *loc, const char *name, H5L_info2_t *linkbuf /*out*/);
H5_DLL herr_t H5L_register_external(void);
H5_DLL herr_t H5L_iterate(H5G_loc_t *loc, const char *group_name, H5_index_t idx_type, H5_iter_order_t order,
                          hsize_t *idx_p, H5L_iterate2_t op, void *op_data);

/* User-defined link functions */
H5_DLL herr_t             H5L_register(const H5L_class_t *cls);
H5_DLL herr_t             H5L_unregister(H5L_type_t id);
H5_DLL herr_t             H5L_is_registered(H5L_type_t id, bool *is_registered);
H5_DLL const H5L_class_t *H5L_find_class(H5L_type_t id);

#endif /* H5Lprivate_H */
