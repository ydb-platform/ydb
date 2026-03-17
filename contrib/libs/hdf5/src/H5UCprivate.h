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
 * This file contains private information about the H5UC module
 * The module used to be H5RC, but changed to H5UC because of
 * conflicting requirement for the use of H5RC.
 */

#ifndef H5UCprivate_H
#define H5UCprivate_H

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

/* Typedef for function to release object when reference count drops to zero */
typedef herr_t (*H5UC_free_func_t)(void *o);

/* Typedef for reference counted objects */
typedef struct H5UC_t {
    void            *o;         /* Object to be reference counted */
    size_t           n;         /* Reference count of number of pointers sharing object */
    H5UC_free_func_t free_func; /* Function to free object */
} H5UC_t;

/**********/
/* Macros */
/**********/
#define H5UC_INC(rc)     ((rc)->n++)
#define H5UC_DEC(rc)     (H5UC_decr(rc))
#define H5UC_GET_OBJ(rc) ((rc)->o)

/********************/
/* Private routines */
/********************/
H5_DLL H5UC_t *H5UC_create(void *s, H5UC_free_func_t free_func);
H5_DLL herr_t  H5UC_decr(H5UC_t *rc);

#endif /* H5UCprivate_H */
