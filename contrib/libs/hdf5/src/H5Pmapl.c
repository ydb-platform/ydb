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
 * Purpose:        Map access property list class routines
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

/* ========= Map Access properties ============ */
/* Definitions for key prefetch size */
#define H5M_ACS_KEY_PREFETCH_SIZE_SIZE sizeof(size_t)
#define H5M_ACS_KEY_PREFETCH_SIZE_DEF  (size_t)(16 * 1024)
#define H5M_ACS_KEY_PREFETCH_SIZE_ENC  H5P__encode_size_t
#define H5M_ACS_KEY_PREFETCH_SIZE_DEC  H5P__decode_size_t
/* Definition for key prefetch buffer size */
#define H5M_ACS_KEY_ALLOC_SIZE_SIZE sizeof(size_t)
#define H5M_ACS_KEY_ALLOC_SIZE_DEF  (size_t)(1024 * 1024)
#define H5M_ACS_KEY_ALLOC_SIZE_ENC  H5P__encode_size_t
#define H5M_ACS_KEY_ALLOC_SIZE_DEC  H5P__decode_size_t

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
static herr_t H5P__macc_reg_prop(H5P_genclass_t *pclass);

/*********************/
/* Package Variables */
/*********************/

/* Map access property list class library initialization object */
const H5P_libclass_t H5P_CLS_MACC[1] = {{
    "map access",        /* Class name for debugging     */
    H5P_TYPE_MAP_ACCESS, /* Class type                   */

    &H5P_CLS_LINK_ACCESS_g,   /* Parent class                 */
    &H5P_CLS_MAP_ACCESS_g,    /* Pointer to class             */
    &H5P_CLS_MAP_ACCESS_ID_g, /* Pointer to class ID          */
    &H5P_LST_MAP_ACCESS_ID_g, /* Pointer to default property list ID */
    H5P__macc_reg_prop,       /* Default property registration routine */

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

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:    H5P__macc_reg_prop
 *
 * Purpose:     Register the map access property list class's
 *              properties
 *
 * Return:      Non-negative on success/Negative on failure
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__macc_reg_prop(H5P_genclass_t *pclass)
{
    size_t key_prefetch_size = H5M_ACS_KEY_PREFETCH_SIZE_DEF; /* Default key prefetch size for iteration */
    size_t key_alloc_size =
        H5M_ACS_KEY_ALLOC_SIZE_DEF; /* Default key prefetch allocation size for iteration */
    herr_t ret_value = SUCCEED;     /* Return value */

    FUNC_ENTER_PACKAGE

    /* Register the key prefetch size for iteration */
    if (H5P__register_real(pclass, H5M_ACS_KEY_PREFETCH_SIZE_NAME, H5M_ACS_KEY_PREFETCH_SIZE_SIZE,
                           &key_prefetch_size, NULL, NULL, NULL, H5M_ACS_KEY_PREFETCH_SIZE_ENC,
                           H5M_ACS_KEY_PREFETCH_SIZE_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the key prefetch allocation size for iteration */
    if (H5P__register_real(pclass, H5M_ACS_KEY_ALLOC_SIZE_NAME, H5M_ACS_KEY_ALLOC_SIZE_SIZE, &key_alloc_size,
                           NULL, NULL, NULL, H5M_ACS_KEY_ALLOC_SIZE_ENC, H5M_ACS_KEY_ALLOC_SIZE_DEC, NULL,
                           NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__macc_reg_prop() */

#ifdef H5_HAVE_MAP_API

/*-------------------------------------------------------------------------
 * Function:    H5Pset_map_iterate_hints
 *
 * Purpose:     H5Pset_map_iterate_hints adjusts the behavior of
 *              H5Miterate when prefetching keys for iteration. The
 *              KEY_PREFETCH_SIZE parameter specifies the number of keys
 *              to prefetch at a time during iteration. The KEY_ALLOC_SIZE
 *              parameter specifies the initial size of the buffer
 *              allocated to hold these prefetched keys. If this buffer is
 *              too small it will be reallocated to a larger size, though
 *              this may result in an additional I/O.
 *
 *              Move to DAOS VOL code? DSINC
 *
 * Return:      Non-negative on success/Negative on failure
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_map_iterate_hints(hid_t mapl_id, size_t key_prefetch_size, size_t key_alloc_size)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "izz", mapl_id, key_prefetch_size, key_alloc_size);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(mapl_id, H5P_MAP_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Set sizes */
    if (H5P_set(plist, H5M_ACS_KEY_PREFETCH_SIZE_NAME, &key_prefetch_size) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set key prefetch size");
    if (H5P_set(plist, H5M_ACS_KEY_ALLOC_SIZE_NAME, &key_alloc_size) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set key allocation size");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_map_iterate_hints() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_map_iterate_hints
 *
 * Purpose:     Returns the map iterate hints, KEY_PREFETCH_SIZE and
 *              KEY_ALLOC_SIZE, as set by H5Pset_map_iterate_hints.
 *
 * Return:      Non-negative on success/Negative on failure
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_map_iterate_hints(hid_t mapl_id, size_t *key_prefetch_size /*out*/, size_t *key_alloc_size /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "ixx", mapl_id, key_prefetch_size, key_alloc_size);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(mapl_id, H5P_MAP_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get the properties */
    if (key_prefetch_size) {
        if (H5P_get(plist, H5M_ACS_KEY_PREFETCH_SIZE_NAME, key_prefetch_size) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get key prefetch size");
    } /* end if */
    if (key_alloc_size) {
        if (H5P_get(plist, H5M_ACS_KEY_ALLOC_SIZE_NAME, key_alloc_size) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get key allocation size");
    } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_map_iterate_hints() */
#endif
