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

/****************/
/* Module Setup */
/****************/

#define H5O_FRIEND     /*suppress error about including H5Opkg	  */
#include "H5Tmodule.h" /* This source code file is part of the H5T module */

/***********/
/* Headers */
/***********/
#include "H5private.h"  /* Generic Functions			*/
#include "H5Eprivate.h" /* Error handling		  	*/
#include "H5Iprivate.h" /* IDs			  		*/
#include "H5Opkg.h"     /* Object headers			*/
#include "H5Tpkg.h"     /* Datatypes				*/

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Local Prototypes */
/********************/

static htri_t     H5O__dtype_isa(const H5O_t *loc);
static void      *H5O__dtype_open(const H5G_loc_t *obj_loc, H5I_type_t *opened_type);
static void      *H5O__dtype_create(H5F_t *f, void *_crt_info, H5G_loc_t *obj_loc);
static H5O_loc_t *H5O__dtype_get_oloc(hid_t obj_id);

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/* This message derives from H5O object class */
const H5O_obj_class_t H5O_OBJ_DATATYPE[1] = {{
    H5O_TYPE_NAMED_DATATYPE, /* object type			*/
    "named datatype",        /* object name, for debugging	*/
    NULL,                    /* get 'copy file' user data	*/
    NULL,                    /* free 'copy file' user data	*/
    H5O__dtype_isa,          /* "isa" 			*/
    H5O__dtype_open,         /* open an object of this class */
    H5O__dtype_create,       /* create an object of this class */
    H5O__dtype_get_oloc,     /* get an object header location for an object */
    NULL,                    /* get the index & heap info for an object */
    NULL                     /* flush an opened object of this class */
}};

/*-------------------------------------------------------------------------
 * Function:	H5O__dtype_isa
 *
 * Purpose:	Determines if an object has the requisite messages for being
 *		a datatype.
 *
 * Return:	Success:	true if the required data type messages are
 *				present; false otherwise.
 *
 *		Failure:	FAIL if the existence of certain messages
 *				cannot be determined.
 *
 *-------------------------------------------------------------------------
 */
static htri_t
H5O__dtype_isa(const H5O_t *oh)
{
    htri_t ret_value = FAIL; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(oh);

    if ((ret_value = H5O_msg_exists_oh(oh, H5O_DTYPE_ID)) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to read object header");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__dtype_isa() */

/*-------------------------------------------------------------------------
 * Function:	H5O__dtype_open
 *
 * Purpose:	Open a datatype at a particular location
 *
 * Return:	Success:	Open object identifier
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static void *
H5O__dtype_open(const H5G_loc_t *obj_loc, H5I_type_t *opened_type)
{
    H5T_t *type      = NULL; /* Datatype opened */
    void  *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(obj_loc);

    *opened_type = H5I_DATATYPE;

    /* Open the datatype */
    if (NULL == (type = H5T_open(obj_loc)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTOPENOBJ, NULL, "unable to open datatype");

    ret_value = (void *)type;

done:
    if (NULL == ret_value)
        if (type && H5T_close(type) < 0)
            HDONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, NULL, "unable to release datatype");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__dtype_open() */

/*-------------------------------------------------------------------------
 * Function:	H5O__dtype_create
 *
 * Purpose:	Create a named datatype in a file
 *
 * Return:	Success:	Pointer to the named datatype data structure
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5O__dtype_create(H5F_t *f, void *_crt_info, H5G_loc_t *obj_loc)
{
    H5T_obj_create_t *crt_info  = (H5T_obj_create_t *)_crt_info; /* Named datatype creation parameters */
    void             *ret_value = NULL;                          /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(crt_info);
    assert(obj_loc);

    /* Commit the type to the file */
    if (H5T__commit(f, crt_info->dt, crt_info->tcpl_id) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "unable to commit datatype");

    /* Set up the new named datatype's location */
    if (NULL == (obj_loc->oloc = H5T_oloc(crt_info->dt)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "unable to get object location of named datatype");
    if (NULL == (obj_loc->path = H5T_nameof(crt_info->dt)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "unable to get path of named datatype");

    /* Set the return value */
    ret_value = crt_info->dt;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__dtype_create() */

/*-------------------------------------------------------------------------
 * Function:	H5O__dtype_get_oloc
 *
 * Purpose:	Retrieve the object header location for an open object
 *
 * Return:	Success:	Pointer to object header location
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
static H5O_loc_t *
H5O__dtype_get_oloc(hid_t obj_id)
{
    H5T_t     *type      = NULL; /* Datatype opened */
    H5T_t     *dt        = NULL;
    H5O_loc_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Get the datatype */
    if (NULL == (dt = (H5T_t *)H5I_object(obj_id)))
        HGOTO_ERROR(H5E_OHDR, H5E_BADID, NULL, "couldn't get object from ID");

    /* If this is a named datatype, get the VOL driver pointer to the datatype */
    type = (H5T_t *)H5T_get_actual_type(dt);

    /* Get the datatype's object header location */
    if (NULL == (ret_value = H5T_oloc(type)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, NULL, "unable to get object location from object");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__dtype_get_oloc() */
