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

/*-----------------------------------------------------------------------------
 * File:    H5Iprivate.h
 * Purpose: header file for ID API
 *---------------------------------------------------------------------------*/

/* avoid re-inclusion */
#ifndef H5Iprivate_H
#define H5Iprivate_H

/* Include package's public headers */
#include "H5Ipublic.h"
#include "H5Idevelop.h"

/* Private headers needed by this file */
#include "H5private.h"
#include "H5VLprivate.h"

/**************************/
/* Library Private Macros */
/**************************/

/* Macro to determine if a H5I_type_t is a "library type" */
#define H5I_IS_LIB_TYPE(type) (type > 0 && type < H5I_NTYPES)

/* Flags for ID class */
#define H5I_CLASS_IS_APPLICATION 0x01

/****************************/
/* Library Private Typedefs */
/****************************/

typedef struct H5I_class_t {
    H5I_type_t type;      /* Class "value" for the type */
    unsigned   flags;     /* Class behavior flags */
    unsigned   reserved;  /* Number of reserved IDs for this type */
                          /* [A specific number of type entries may be
                           * reserved to enable "constant" values to be
                           * handed out which are valid IDs in the type,
                           * but which do not map to any data structures
                           * and are not allocated dynamically later.]
                           */
    H5I_free_t free_func; /* Free function for object's of this type */
} H5I_class_t;

/*****************************/
/* Library-private Variables */
/*****************************/

/***************************************/
/* Library-private Function Prototypes */
/***************************************/
H5_DLL herr_t     H5I_register_type(const H5I_class_t *cls);
H5_DLL int64_t    H5I_nmembers(H5I_type_t type);
H5_DLL herr_t     H5I_clear_type(H5I_type_t type, bool force, bool app_ref);
H5_DLL H5I_type_t H5I_get_type(hid_t id);
H5_DLL herr_t     H5I_iterate(H5I_type_t type, H5I_search_func_t func, void *udata, bool app_ref);
H5_DLL int        H5I_get_ref(hid_t id, bool app_ref);
H5_DLL int        H5I_inc_ref(hid_t id, bool app_ref);
H5_DLL int        H5I_dec_ref(hid_t id);
H5_DLL int        H5I_dec_app_ref(hid_t id);
H5_DLL int        H5I_dec_app_ref_async(hid_t id, void **token);
H5_DLL int        H5I_dec_app_ref_always_close(hid_t id);
H5_DLL int        H5I_dec_app_ref_always_close_async(hid_t id, void **token);
H5_DLL int        H5I_dec_type_ref(H5I_type_t type);
H5_DLL herr_t     H5I_find_id(const void *object, H5I_type_t type, hid_t *id /*out*/);

/* NOTE:    The object and ID functions below deal in non-VOL objects (i.e.;
 *          H5S_t, etc.). Similar VOL calls exist in H5VLprivate.h. Use
 *          the H5VL calls with objects that go through the VOL, such as
 *          datasets and groups, and the H5I calls with objects
 *          that do not, such as property lists and dataspaces. Datatypes
 *          are can be either named, where they will use the VOL, or not,
 *          and thus require special treatment. See the datatype docs for
 *          how to handle this.
 */

/* Functions that manipulate objects */
H5_DLL void  *H5I_object(hid_t id);
H5_DLL void  *H5I_object_verify(hid_t id, H5I_type_t type);
H5_DLL void  *H5I_remove(hid_t id);
H5_DLL void  *H5I_subst(hid_t id, const void *new_object);
H5_DLL htri_t H5I_is_file_object(hid_t id);

/* ID registration functions */
H5_DLL hid_t  H5I_register(H5I_type_t type, const void *object, bool app_ref);
H5_DLL herr_t H5I_register_using_existing_id(H5I_type_t type, void *object, bool app_ref, hid_t existing_id);

/* Debugging functions */
H5_DLL herr_t H5I_dump_ids_for_type(H5I_type_t type);

#endif /* H5Iprivate_H */
