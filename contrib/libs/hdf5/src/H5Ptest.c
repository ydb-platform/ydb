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
 * Purpose:	Generic Property Testing Functions
 */

#include "H5Pmodule.h" /* This source code file is part of the H5P module */
#define H5P_TESTING    /*suppress warning about H5P testing funcs*/

/* Private header files */
#include "H5private.h"  /* Generic Functions			*/
#include "H5Eprivate.h" /* Error handling		  	*/
#include "H5Iprivate.h" /* IDs			  		*/
#include "H5Ppkg.h"     /* Property lists		  	*/
#include "H5Dprivate.h" /* Dataset		  		*/

/* Local variables */

/* Local typedefs */

/*--------------------------------------------------------------------------
 NAME
    H5P__get_class_path_test
 PURPOSE
    Routine to query the full path of a generic property list class
 USAGE
    char *H5P__get_class_name_test(pclass_id)
        hid_t pclass_id;         IN: Property class to query
 RETURNS
    Success: Pointer to a malloc'ed string containing the full path of class
    Failure: NULL
 DESCRIPTION
        This routine retrieves the full path name of a generic property list
    class, starting with the root of the class hierarchy.
    The pointer to the name must be free'd by the user for successful calls.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    DO NOT USE THIS FUNCTION FOR ANYTHING EXCEPT TESTING H5P__get_class_path()
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
char *
H5P__get_class_path_test(hid_t pclass_id)
{
    H5P_genclass_t *pclass;           /* Property class to query */
    char           *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    if (NULL == (pclass = (H5P_genclass_t *)H5I_object_verify(pclass_id, H5I_GENPROP_CLS)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a property class");

    /* Get the property list class path */
    if (NULL == (ret_value = H5P__get_class_path(pclass)))
        HGOTO_ERROR(H5E_PLIST, H5E_NOTFOUND, NULL, "unable to query full path of class");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__get_class_path_test() */

/*--------------------------------------------------------------------------
 NAME
    H5P__open_class_path_test
 PURPOSE
    Routine to open a [copy of] a class with its full path name
 USAGE
    hid_t H5P__open_class_path_test(path)
        const char *path;       IN: Full path name of class to open [copy of]
 RETURNS
    Success: ID of generic property class
    Failure: H5I_INVALID_HID
 DESCRIPTION
    This routine opens [a copy] of the class indicated by the full path.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    DO NOT USE THIS FUNCTION FOR ANYTHING EXCEPT TESTING H5P__open_class_path()
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
hid_t
H5P__open_class_path_test(const char *path)
{
    H5P_genclass_t *pclass    = NULL;            /* Property class to query */
    hid_t           ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    if (NULL == path || *path == '\0')
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "invalid class path");

    /* Open the property list class */
    if (NULL == (pclass = H5P__open_class_path(path)))
        HGOTO_ERROR(H5E_PLIST, H5E_NOTFOUND, H5I_INVALID_HID, "unable to find class with full path");

    /* Get an ID for the class */
    if ((ret_value = H5I_register(H5I_GENPROP_CLS, pclass, true)) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register property list class");

done:
    if (H5I_INVALID_HID == ret_value && pclass)
        H5P__close_class(pclass);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__open_class_path_test() */
