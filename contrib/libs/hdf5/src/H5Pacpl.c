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

/*-------------------------------------------------------------------------
 *
 * Created:     H5Pacpl.c
 *
 * Purpose:     Attribute creation property list class routines
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5Pmodule.h" /* This source code file is part of the H5P module */

/***********/
/* Headers */
/***********/
#include "H5private.h"  /* Generic Functions                        */
#include "H5Eprivate.h" /* Error handling                           */
#include "H5Ppkg.h"     /* Property lists                           */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/

/*********************/
/* Package Variables */
/*********************/

/* Attribute creation property list class library initialization object */
const H5P_libclass_t H5P_CLS_ACRT[1] = {{
    "attribute create",        /* Class name for debugging                 */
    H5P_TYPE_ATTRIBUTE_CREATE, /* Class type                               */

    &H5P_CLS_STRING_CREATE_g,       /* Parent class                             */
    &H5P_CLS_ATTRIBUTE_CREATE_g,    /* Pointer to class                         */
    &H5P_CLS_ATTRIBUTE_CREATE_ID_g, /* Pointer to class ID                      */
    &H5P_LST_ATTRIBUTE_CREATE_ID_g, /* Pointer to default property list ID      */
    NULL,                           /* Default property registration routine    */

    NULL, /* Class creation callback                  */
    NULL, /* Class creation callback info             */
    NULL, /* Class copy callback                      */
    NULL, /* Class copy callback info                 */
    NULL, /* Class close callback                     */
    NULL  /* Class close callback info                */
}};

/*****************************/
/* Library Private Variables */
/*****************************/
