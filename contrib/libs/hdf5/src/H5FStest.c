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
 * Purpose:	Free-space manager testing functions.
 *
 */

/****************/
/* Module Setup */
/****************/

#include "H5FSmodule.h" /* This source code file is part of the H5FS module */
#define H5FS_TESTING    /* Suppress warning about H5FS testing funcs */

/***********/
/* Headers */
/***********/
#include "H5private.h"  /* Generic Functions                            */
#include "H5Eprivate.h" /* Error handling                               */
#include "H5FSpkg.h"    /* Free-space manager                           */

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

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:    H5FS__get_cparam_test
 *
 * Purpose:     Retrieve the parameters used to create the free-space manager
 *              similar to H5HF_get_cparam_test()
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FS__get_cparam_test(const H5FS_t *frsp, H5FS_create_t *cparam)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments. */
    assert(frsp);
    assert(cparam);

    cparam->client         = frsp->client;
    cparam->shrink_percent = frsp->shrink_percent;
    cparam->expand_percent = frsp->expand_percent;
    cparam->max_sect_addr  = frsp->max_sect_addr;
    cparam->max_sect_size  = frsp->max_sect_size;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5FS__get_cparam_test() */

/*-------------------------------------------------------------------------
 * Function:    H5FS__cmp_cparam_test
 *
 * Purpose:     Compare the parameters used to create the free space manager
 *              similar to H5HF_cmp_cparam_test()
 *
 * Return:      A value like strcmp()
 *
 *-------------------------------------------------------------------------
 */
int
H5FS__cmp_cparam_test(const H5FS_create_t *cparam1, const H5FS_create_t *cparam2)
{
    int ret_value = 0; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments. */
    assert(cparam1);
    assert(cparam2);

    if (cparam1->client < cparam2->client)
        HGOTO_DONE(-1);
    else if (cparam1->client > cparam2->client)
        HGOTO_DONE(1);

    if (cparam1->shrink_percent < cparam2->shrink_percent)
        HGOTO_DONE(-1);
    else if (cparam1->shrink_percent > cparam2->shrink_percent)
        HGOTO_DONE(1);

    if (cparam1->expand_percent < cparam2->expand_percent)
        HGOTO_DONE(-1);
    else if (cparam1->expand_percent > cparam2->expand_percent)
        HGOTO_DONE(1);

    if (cparam1->max_sect_size < cparam2->max_sect_size)
        HGOTO_DONE(-1);
    else if (cparam1->max_sect_size > cparam2->max_sect_size)
        HGOTO_DONE(1);

    if (cparam1->max_sect_addr < cparam2->max_sect_addr)
        HGOTO_DONE(-1);
    else if (cparam1->max_sect_addr > cparam2->max_sect_addr)
        HGOTO_DONE(1);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS__cmp_cparam_test */
