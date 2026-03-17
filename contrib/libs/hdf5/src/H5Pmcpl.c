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
 * Created:     H5Pmcpl.c
 *
 * Purpose:     Map creation property list class routines
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
#include "H5Mprivate.h" /* Maps                                     */
#include "H5Eprivate.h" /* Error handling                           */
#include "H5Iprivate.h" /* IDs                                      */
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

/* Property class callbacks */
static herr_t H5P__mcrt_reg_prop(H5P_genclass_t *pclass);

/*********************/
/* Package Variables */
/*********************/

/* Map create property list class library initialization object */
const H5P_libclass_t H5P_CLS_MCRT[1] = {{
    "map create",        /* Class name for debugging     */
    H5P_TYPE_MAP_CREATE, /* Class type                   */

    &H5P_CLS_OBJECT_CREATE_g, /* Parent class                 */
    &H5P_CLS_MAP_CREATE_g,    /* Pointer to class             */
    &H5P_CLS_MAP_CREATE_ID_g, /* Pointer to class ID          */
    &H5P_LST_MAP_CREATE_ID_g, /* Pointer to default property list ID */
    H5P__mcrt_reg_prop,       /* Default property registration routine */

    NULL, /* Class creation callback      */
    NULL, /* Class creation callback info */
    NULL, /* Class copy callback          */
    NULL, /* Class copy callback info     */
    NULL, /* Class close callback         */
    NULL  /* Class close callback info    */
}};

/*****************************/
/* Library Private Variables */
/*****************************/

/***************************/
/* Local Private Variables */
/***************************/

/*-------------------------------------------------------------------------
 * Function:    H5P__mcrt_reg_prop
 *
 * Purpose:     Register the map creation property list class's properties
 *
 * Return:      Non-negative on success/Negative on failure
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__mcrt_reg_prop(H5P_genclass_t H5_ATTR_UNUSED *pclass)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__mcrt_reg_prop() */
