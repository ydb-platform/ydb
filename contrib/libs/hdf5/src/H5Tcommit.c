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
 * Module Info: This module contains the functionality for committing datatypes
 *      to a file for the H5T interface.
 */

/****************/
/* Module Setup */
/****************/

#include "H5Tmodule.h" /* This source code file is part of the H5T module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5ACprivate.h" /* Metadata cache                           */
#include "H5CXprivate.h" /* API Contexts                             */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5ESprivate.h" /* Event Sets                               */
#include "H5FLprivate.h" /* Free lists                               */
#include "H5FOprivate.h" /* File objects                             */
#include "H5Iprivate.h"  /* IDs                                      */
#include "H5Lprivate.h"  /* Links                                    */
#include "H5MMprivate.h" /* Memory Management                        */
#include "H5Oprivate.h"  /* Object headers                           */
#include "H5Pprivate.h"  /* Property lists                           */
#include "H5Tpkg.h"      /* Datatypes                                */
#include "H5VLprivate.h" /* Virtual Object Layer                     */

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
static herr_t H5T__commit_api_common(hid_t loc_id, const char *name, hid_t type_id, hid_t lcpl_id,
                                     hid_t tcpl_id, hid_t tapl_id, void **token_ptr,
                                     H5VL_object_t **_vol_obj_ptr);
static hid_t  H5T__open_api_common(hid_t loc_id, const char *name, hid_t tapl_id, void **token_ptr,
                                   H5VL_object_t **_vol_obj_ptr);
static H5T_t *H5T__open_oid(const H5G_loc_t *loc);

/*********************/
/* Public Variables */
/*********************/

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/* Declare a free list to manage the H5VL_t struct */
H5FL_EXTERN(H5VL_t);

/* Declare a free list to manage the H5VL_object_t struct */
H5FL_EXTERN(H5VL_object_t);

/*-------------------------------------------------------------------------
 * Function:    H5T__commit_api_common
 *
 * Purpose:     This is the common function for committing a datytype.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__commit_api_common(hid_t loc_id, const char *name, hid_t type_id, hid_t lcpl_id, hid_t tcpl_id,
                       hid_t tapl_id, void **token_ptr, H5VL_object_t **_vol_obj_ptr)
{
    void           *data        = NULL; /* VOL-managed datatype data */
    H5VL_object_t  *new_obj     = NULL; /* VOL object that holds the datatype object and the VOL info */
    H5T_t          *dt          = NULL; /* High level datatype object that wraps the VOL object */
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    H5VL_loc_params_t loc_params;                     /* Location parameters */
    herr_t            ret_value = SUCCEED;            /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    if (!name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "name parameter cannot be NULL");
    if (!*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "name parameter cannot be an empty string");
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");
    if (H5T_is_named(dt))
        HGOTO_ERROR(H5E_ARGS, H5E_CANTSET, FAIL, "datatype is already committed");

    /* Get correct property list */
    if (H5P_DEFAULT == lcpl_id)
        lcpl_id = H5P_LINK_CREATE_DEFAULT;
    else if (true != H5P_isa_class(lcpl_id, H5P_LINK_CREATE))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not link creation property list");

    /* Get correct property list */
    if (H5P_DEFAULT == tcpl_id)
        tcpl_id = H5P_DATATYPE_CREATE_DEFAULT;
    else if (true != H5P_isa_class(tcpl_id, H5P_DATATYPE_CREATE))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not datatype creation property list");

    /* Set the LCPL for the API context */
    H5CX_set_lcpl(lcpl_id);

    /* Set up object access arguments */
    if (H5VL_setup_acc_args(loc_id, H5P_CLS_TACC, true, &tapl_id, vol_obj_ptr, &loc_params) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTSET, FAIL, "can't set object access arguments");

    /* Commit the type */
    if (NULL == (data = H5VL_datatype_commit(*vol_obj_ptr, &loc_params, name, type_id, lcpl_id, tcpl_id,
                                             tapl_id, H5P_DATASET_XFER_DEFAULT, token_ptr)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to commit datatype");

    /* Set up VOL object */
    if (NULL == (new_obj = H5VL_create_object(data, (*vol_obj_ptr)->connector)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTALLOC, FAIL, "can't create VOL object for committed datatype");

    /* Set the committed type object to the VOL connector pointer in the H5T_t struct */
    dt->vol_obj = new_obj;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__commit_api_common() */

/*-------------------------------------------------------------------------
 * Function:    H5Tcommit2
 *
 * Purpose:     Save a transient datatype to a file and turn the type handle
 *              into a "named", immutable type.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Tcommit2(hid_t loc_id, const char *name, hid_t type_id, hid_t lcpl_id, hid_t tcpl_id, hid_t tapl_id)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE6("e", "i*siiii", loc_id, name, type_id, lcpl_id, tcpl_id, tapl_id);

    /* Commit the dataset synchronously */
    if ((ret_value = H5T__commit_api_common(loc_id, name, type_id, lcpl_id, tcpl_id, tapl_id, NULL, NULL)) <
        0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTOPENOBJ, FAIL, "unable to commit datatype synchronously");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tcommit2() */

/*-------------------------------------------------------------------------
 * Function:    H5Tcommit_async
 *
 * Purpose:     Asynchronous version of H5Tcommit2
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Tcommit_async(const char *app_file, const char *app_func, unsigned app_line, hid_t loc_id, const char *name,
                hid_t type_id, hid_t lcpl_id, hid_t tcpl_id, hid_t tapl_id, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    herr_t         ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE10("e", "*s*sIui*siiiii", app_file, app_func, app_line, loc_id, name, type_id, lcpl_id, tcpl_id,
              tapl_id, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token;

    /* Commit the datatype asynchronously */
    if ((ret_value = H5T__commit_api_common(loc_id, name, type_id, lcpl_id, tcpl_id, tapl_id, token_ptr,
                                            &vol_obj)) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTOPENOBJ, FAIL, "unable to commit datatype asynchronously");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE10(__func__, "*s*sIui*siiiii", app_file, app_func, app_line, loc_id, name, type_id, lcpl_id, tcpl_id, tapl_id, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tcommit_async() */

/*-------------------------------------------------------------------------
 * Function:	H5T__commit_named
 *
 * Purpose:	Internal routine to save a transient datatype to a file and
 *              turn the type ID into a "named", immutable type.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5T__commit_named(const H5G_loc_t *loc, const char *name, H5T_t *dt, hid_t lcpl_id, hid_t tcpl_id)
{
    H5O_obj_create_t ocrt_info;           /* Information for object creation */
    H5T_obj_create_t tcrt_info;           /* Information for named datatype creation */
    H5T_state_t      old_state;           /* The state of the datatype before H5T__commit. */
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(loc);
    assert(name && *name);
    assert(dt);
    assert(lcpl_id != H5P_DEFAULT);
    assert(tcpl_id != H5P_DEFAULT);

    /* Record the type's state so that we can revert to it if linking fails */
    old_state = dt->shared->state;

    /* Set up named datatype creation info */
    tcrt_info.dt      = dt;
    tcrt_info.tcpl_id = tcpl_id;

    /* Set up object creation information */
    ocrt_info.obj_type = H5O_TYPE_NAMED_DATATYPE;
    ocrt_info.crt_info = &tcrt_info;
    ocrt_info.new_obj  = NULL;

    /* Create the new named datatype and link it to its parent group */
    if (H5L_link_object(loc, name, &ocrt_info, lcpl_id) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to create and link to named datatype");
    assert(ocrt_info.new_obj);

done:
    /* If the datatype was committed but something failed after that, we need
     * to return it to the state it was in before it was committed.
     */
    if (ret_value < 0 && (NULL != ocrt_info.new_obj)) {
        if (dt->shared->state == H5T_STATE_OPEN && dt->sh_loc.type == H5O_SHARE_TYPE_COMMITTED) {
            /* Remove the datatype from the list of opened objects in the file */
            if (H5FO_top_decr(dt->sh_loc.file, dt->sh_loc.u.loc.oh_addr) < 0)
                HDONE_ERROR(H5E_DATASET, H5E_CANTRELEASE, FAIL, "can't decrement count for object");
            if (H5FO_delete(dt->sh_loc.file, dt->sh_loc.u.loc.oh_addr) < 0)
                HDONE_ERROR(H5E_DATASET, H5E_CANTRELEASE, FAIL,
                            "can't remove dataset from list of open objects");

            /* Close the datatype object */
            if (H5O_close(&(dt->oloc), NULL) < 0)
                HDONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "unable to release object header");

            /* Remove the datatype's object header from the file */
            if (H5O_delete(dt->sh_loc.file, dt->sh_loc.u.loc.oh_addr) < 0)
                HDONE_ERROR(H5E_DATATYPE, H5E_CANTDELETE, FAIL, "unable to delete object header");

            /* Mark datatype as being back in memory */
            if (H5T_set_loc(dt, NULL, H5T_LOC_MEMORY))
                HDONE_ERROR(H5E_DATATYPE, H5E_CANTDELETE, FAIL, "unable to return datatype to memory");
            dt->sh_loc.type   = H5O_SHARE_TYPE_UNSHARED;
            dt->shared->state = old_state;
        } /* end if */
    }     /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__commit_named() */

/*-------------------------------------------------------------------------
 * Function:    H5Tcommit_anon
 *
 * Purpose:     Save a transient datatype to a file and turn the type handle
 *              into a "named", immutable type.
 *
 *              The resulting ID should be linked into the file with
 *              H5Olink or it will be deleted when closed.
 *
 * Note:        The datatype access property list is unused currently, but
 *              is checked for sanity anyway.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Tcommit_anon(hid_t loc_id, hid_t type_id, hid_t tcpl_id, hid_t tapl_id)
{
    void             *dt      = NULL; /* datatype object created by VOL connector */
    H5VL_object_t    *new_obj = NULL; /* VOL object that holds the datatype object and the VOL info */
    H5T_t            *type    = NULL; /* Datatype created */
    H5VL_object_t    *vol_obj = NULL; /* object of loc_id */
    H5VL_loc_params_t loc_params;
    herr_t            ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "iiii", loc_id, type_id, tcpl_id, tapl_id);

    /* Check arguments */
    if (NULL == (type = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");
    if (H5T_is_named(type))
        HGOTO_ERROR(H5E_ARGS, H5E_CANTSET, FAIL, "datatype is already committed");

    /* Get correct property list */
    if (H5P_DEFAULT == tcpl_id)
        tcpl_id = H5P_DATATYPE_CREATE_DEFAULT;
    else if (true != H5P_isa_class(tcpl_id, H5P_DATATYPE_CREATE))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not datatype creation property list");

    if (H5P_DEFAULT == tapl_id)
        tapl_id = H5P_DATATYPE_ACCESS_DEFAULT;
    else if (true != H5P_isa_class(tapl_id, H5P_DATATYPE_ACCESS))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not datatype access property list");

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&tapl_id, H5P_CLS_TACC, loc_id, true) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTSET, FAIL, "can't set access property list info");

    /* Fill in location struct fields */
    loc_params.type     = H5VL_OBJECT_BY_SELF;
    loc_params.obj_type = H5I_get_type(loc_id);

    /* Get the file object */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid file identifier");

    /* Commit the datatype */
    if (NULL == (dt = H5VL_datatype_commit(vol_obj, &loc_params, NULL, type_id, H5P_LINK_CREATE_DEFAULT,
                                           tcpl_id, tapl_id, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to commit datatype");

    /* Setup VOL object */
    if (NULL == (new_obj = H5VL_create_object(dt, vol_obj->connector)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTALLOC, FAIL, "can't create VOL object for committed datatype");

    /* Set the committed type object to the VOL connector pointer in the H5T_t struct */
    type->vol_obj = new_obj;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tcommit_anon() */

/*-------------------------------------------------------------------------
 * Function:    H5T__commit_anon
 *
 * Purpose:     Create an anonymous committed datatype.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5T__commit_anon(H5F_t *file, H5T_t *type, hid_t tcpl_id)
{
    H5O_loc_t *oloc;                /* Object location for datatype */
    herr_t     ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(file);
    assert(type);
    assert(tcpl_id != H5P_DEFAULT);

    /* Commit the type */
    if (H5T__commit(file, type, tcpl_id) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to commit datatype");

    /* Release the datatype's object header */

    /* Get the new committed datatype's object location */
    if (NULL == (oloc = H5T_oloc(type)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "unable to get object location of committed datatype");

    /* Decrement refcount on committed datatype's object header in memory */
    if (H5O_dec_rc_by_loc(oloc) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTDEC, FAIL, "unable to decrement refcount on newly created object");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5T__commit_anon() */

/*-------------------------------------------------------------------------
 * Function:	H5T__commit
 *
 * Purpose:	Commit a type, giving it a name and causing it to become
 *		immutable.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5T__commit(H5F_t *file, H5T_t *type, hid_t tcpl_id)
{
    H5O_loc_t  temp_oloc;           /* Temporary object header location */
    H5G_name_t temp_path;           /* Temporary path */
    bool       loc_init = false;    /* Have temp_oloc and temp_path been initialized? */
    size_t     dtype_size;          /* Size of the datatype message */
    herr_t     ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(file);
    assert(type);
    assert(tcpl_id != H5P_DEFAULT);

    /* Check if we are allowed to write to this file */
    if (0 == (H5F_INTENT(file) & H5F_ACC_RDWR))
        HGOTO_ERROR(H5E_DATATYPE, H5E_WRITEERROR, FAIL, "no write intent on file");

    /*
     * Check arguments.  We cannot commit an immutable type because H5Tclose()
     * normally fails on such types (try H5Tclose(H5T_NATIVE_INT)) but closing
     * a named type should always succeed.
     */
    if (H5T_STATE_NAMED == type->shared->state || H5T_STATE_OPEN == type->shared->state)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "datatype is already committed");
    if (H5T_STATE_IMMUTABLE == type->shared->state)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "datatype is immutable");

    /* Check for a "sensible" datatype to store on disk */
    if (H5T_is_sensible(type) <= 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "datatype is not sensible");

    /* Mark datatype as being on disk now.  This step changes the size of
     *  datatype as stored on disk.
     */
    if (H5T_set_loc(type, H5F_VOL_OBJ(file), H5T_LOC_DISK) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "cannot mark datatype on disk");

    /* Reset datatype location and path */
    if (H5O_loc_reset(&temp_oloc) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTRESET, FAIL, "unable to initialize location");
    if (H5G_name_reset(&temp_path) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTRESET, FAIL, "unable to initialize path");
    loc_init = true;

    /* Set the version for datatype */
    if (H5T_set_version(file, type) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "can't set version of datatype");

    /* Calculate message size information, for creating object header */
    dtype_size = H5O_msg_size_f(file, tcpl_id, H5O_DTYPE_ID, type, (size_t)0);
    assert(dtype_size);

    /*
     * Create the object header and open it for write access. Insert the data
     * type message and then give the object header a name.
     */
    if (H5O_create(file, dtype_size, (size_t)1, tcpl_id, &temp_oloc) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to create datatype object header");
    if (H5O_msg_create(&temp_oloc, H5O_DTYPE_ID, H5O_MSG_FLAG_CONSTANT | H5O_MSG_FLAG_DONTSHARE,
                       H5O_UPDATE_TIME, type) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to update type header message");

    /* Copy the new object header's location into the datatype, taking ownership of it */
    if (H5O_loc_copy_shallow(&(type->oloc), &temp_oloc) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to copy datatype location");
    if (H5G_name_copy(&(type->path), &temp_path, H5_COPY_SHALLOW) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to copy datatype location");
    loc_init = false;

    /* Set the shared info fields */
    H5T_update_shared(type);
    type->shared->state    = H5T_STATE_OPEN;
    type->shared->fo_count = 1;

    /* Add datatype to the list of open objects in the file */
    if (H5FO_top_incr(type->sh_loc.file, type->sh_loc.u.loc.oh_addr) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINC, FAIL, "can't incr object ref. count");
    if (H5FO_insert(type->sh_loc.file, type->sh_loc.u.loc.oh_addr, type->shared, true) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINSERT, FAIL, "can't insert datatype into list of open objects");

    /* Mark datatype as being on memory again.  Since this datatype may still be
     *  used in memory after committed to disk, change its size back as in memory.
     */
    if (H5T_set_loc(type, NULL, H5T_LOC_MEMORY) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "cannot mark datatype in memory");

done:
    if (ret_value < 0) {
        if (loc_init) {
            H5O_loc_free(&temp_oloc);
            H5G_name_free(&temp_path);
        } /* end if */
        if ((type->shared->state == H5T_STATE_TRANSIENT || type->shared->state == H5T_STATE_RDONLY) &&
            (type->sh_loc.type == H5O_SHARE_TYPE_COMMITTED)) {
            if (H5O_dec_rc_by_loc(&(type->oloc)) < 0)
                HDONE_ERROR(H5E_DATATYPE, H5E_CANTDEC, FAIL,
                            "unable to decrement refcount on newly created object");
            if (H5O_close(&(type->oloc), NULL) < 0)
                HDONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "unable to release object header");
            if (H5O_delete(file, type->sh_loc.u.loc.oh_addr) < 0)
                HDONE_ERROR(H5E_DATATYPE, H5E_CANTDELETE, FAIL, "unable to delete object header");
            type->sh_loc.type = H5O_SHARE_TYPE_UNSHARED;
        } /* end if */
    }     /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5T__commit() */

/*-------------------------------------------------------------------------
 * Function:    H5Tcommitted
 *
 * Purpose:     Determines if a datatype is committed or not.
 *
 * Return:      true/false/Negative
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5Tcommitted(hid_t type_id)
{
    H5T_t *type;      /* Datatype to query */
    htri_t ret_value; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("t", "i", type_id);

    /* Check arguments */
    if (NULL == (type = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");

    /* Set return value */
    ret_value = H5T_is_named(type);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tcommitted() */

/*-------------------------------------------------------------------------
 * Function:	H5T_link
 *
 * Purpose:	Adjust the link count for an object header by adding
 *		ADJUST to the link count.
 *
 * Return:	Success:	New link count
 *		Failure:	-1
 *
 *-------------------------------------------------------------------------
 */
int
H5T_link(const H5T_t *type, int adjust)
{
    int ret_value = -1; /* Return value */

    FUNC_ENTER_NOAPI((-1))

    assert(type);
    assert(type->sh_loc.type == H5O_SHARE_TYPE_COMMITTED);

    /* Adjust the link count on the named datatype */
    if ((ret_value = H5O_link(&type->oloc, adjust)) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_LINKCOUNT, (-1), "unable to adjust named datatype link count");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_link() */

/*-------------------------------------------------------------------------
 * Function:    H5T__open_api_common
 *
 * Purpose:     This is the common function for opening a datatype.
 *
 * Return:      Success:    Object ID of the named datatype
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
static hid_t
H5T__open_api_common(hid_t loc_id, const char *name, hid_t tapl_id, void **token_ptr,
                     H5VL_object_t **_vol_obj_ptr)
{
    void           *dt          = NULL; /* datatype object created by VOL connector */
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    H5VL_loc_params_t loc_params;
    hid_t             ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    if (!name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "name parameter cannot be NULL");
    if (!*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "name parameter cannot be an empty string");

    /* Set up object access arguments */
    if (H5VL_setup_acc_args(loc_id, H5P_CLS_TACC, false, &tapl_id, vol_obj_ptr, &loc_params) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTSET, H5I_INVALID_HID, "can't set object access arguments");

    /* Open the datatype */
    if (NULL == (dt = H5VL_datatype_open(*vol_obj_ptr, &loc_params, name, tapl_id, H5P_DATASET_XFER_DEFAULT,
                                         token_ptr)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTOPENOBJ, H5I_INVALID_HID, "unable to open named datatype");

    /* Register the type and return the ID */
    if ((ret_value = H5VL_register(H5I_DATATYPE, dt, (*vol_obj_ptr)->connector, true)) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register named datatype");

done:
    /* Cleanup on error */
    if (H5I_INVALID_HID == ret_value)
        if (dt && H5VL_datatype_close(*vol_obj_ptr, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HDONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, H5I_INVALID_HID, "unable to release datatype");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__open_api_common() */

/*-------------------------------------------------------------------------
 * Function:    H5Topen2
 *
 * Purpose:     Opens a named datatype using a Datatype Access Property
 *              List.
 *
 * Return:      Success:    Object ID of the named datatype
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Topen2(hid_t loc_id, const char *name, hid_t tapl_id)
{
    hid_t ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE3("i", "i*si", loc_id, name, tapl_id);

    /* Open the datatype synchronously */
    if ((ret_value = H5T__open_api_common(loc_id, name, tapl_id, NULL, NULL)) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTOPENOBJ, H5I_INVALID_HID,
                    "unable to open named datatype synchronously");
done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Topen2() */

/*-------------------------------------------------------------------------
 * Function:    H5Topen_async
 *
 * Purpose:     Asynchronous version of H5Topen2.
 *
 * Return:      Success:    Object ID of the named datatype
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Topen_async(const char *app_file, const char *app_func, unsigned app_line, hid_t loc_id, const char *name,
              hid_t tapl_id, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation */
    hid_t          ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE7("i", "*s*sIui*sii", app_file, app_func, app_line, loc_id, name, tapl_id, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Open the datatype asynchronously */
    if ((ret_value = H5T__open_api_common(loc_id, name, tapl_id, token_ptr, &vol_obj)) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTOPENOBJ, H5I_INVALID_HID,
                    "unable to open named datatype asynchronously");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE7(__func__, "*s*sIui*sii", app_file, app_func, app_line, loc_id, name, tapl_id, es_id)) < 0) {
            /* clang-format on */
            if (H5I_dec_app_ref_always_close(ret_value) < 0)
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTDEC, H5I_INVALID_HID,
                            "can't decrement count on datatype ID");
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINSERT, H5I_INVALID_HID, "can't insert token into event set");
        } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Topen_async() */

/*-------------------------------------------------------------------------
 * Function:    H5Tget_create_plist
 *
 * Purpose:     Returns a copy of the datatype creation property list.
 *
 * Note:        There are no datatype creation properties currently, just
 *              object creation ones.
 *
 * Return:      Success:    ID for a copy of the datatype creation
 *                          property list.  The property list ID should be
 *                          released by calling H5Pclose().
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Tget_create_plist(hid_t dtype_id)
{
    H5T_t *type      = NULL;            /* Datatype object for ID */
    htri_t is_named  = FAIL;            /* Is the datatype named? */
    hid_t  ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE1("i", "i", dtype_id);

    /* Check arguments */
    if (NULL == (type = (H5T_t *)H5I_object_verify(dtype_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not a datatype");

    /* Check if the datatype is committed */
    if (FAIL == (is_named = H5T_is_named(type)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, H5I_INVALID_HID, "can't check whether datatype is committed");

    /* If the datatype is not committed/named, just copy the default
     * creation property list and return that.
     */
    if (false == is_named) {
        H5P_genplist_t *tcpl_plist = NULL;

        /* Copy the default datatype creation property list */
        if (NULL == (tcpl_plist = (H5P_genplist_t *)H5I_object(H5P_LST_DATATYPE_CREATE_ID_g)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "can't get default creation property list");
        if ((ret_value = H5P_copy_plist(tcpl_plist, true)) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, H5I_INVALID_HID,
                        "unable to copy the creation property list");
    } /* end if */
    /* If the datatype is committed, retrieve further information */
    else {
        H5VL_object_t           *vol_obj = type->vol_obj;
        H5VL_datatype_get_args_t vol_cb_args; /* Arguments to VOL callback */

        /* Set up VOL callback arguments */
        vol_cb_args.op_type               = H5VL_DATATYPE_GET_TCPL;
        vol_cb_args.args.get_tcpl.tcpl_id = H5I_INVALID_HID;

        /* Get the property list through the VOL */
        if (H5VL_datatype_get(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, H5I_INVALID_HID, "can't get object creation info");

        /* Set return value */
        ret_value = vol_cb_args.args.get_tcpl.tcpl_id;
    } /* end else */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tget_create_plist() */

/*-------------------------------------------------------------------------
 * Function:    H5Tflush
 *
 * Purpose:     Flushes all buffers associated with a named datatype to disk.
 *
 * Return:      Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Tflush(hid_t type_id)
{
    H5T_t *dt;                  /* Datatype for this operation */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", type_id);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");
    if (!H5T_is_named(dt))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a committed datatype");

    /* Flush metadata for named datatype */
    if (dt->vol_obj) {
        H5VL_datatype_specific_args_t vol_cb_args; /* Arguments to VOL callback */

        /* Set up collective metadata if appropriate */
        if (H5CX_set_loc(type_id) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTSET, FAIL, "can't set access property list info");

        /* Set up VOL callback arguments */
        vol_cb_args.op_type            = H5VL_DATATYPE_FLUSH;
        vol_cb_args.args.flush.type_id = type_id;

        if (H5VL_datatype_specific(dt->vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTFLUSH, FAIL, "unable to flush datatype");
    }

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Tflush */

/*-------------------------------------------------------------------------
 * Function:    H5Trefresh
 *
 * Purpose:     Refreshes all buffers associated with a named datatype.
 *
 * Return:      Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Trefresh(hid_t type_id)
{
    H5T_t *dt;                  /* Datatype for this operation */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", type_id);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");
    if (!H5T_is_named(dt))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a committed datatype");

    /* Refresh the datatype's metadata */
    if (dt->vol_obj) {
        H5VL_datatype_specific_args_t vol_cb_args; /* Arguments to VOL callback */

        /* Set up collective metadata if appropriate */
        if (H5CX_set_loc(type_id) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTSET, FAIL, "can't set access property list info");

        /* Set up VOL callback arguments */
        vol_cb_args.op_type              = H5VL_DATATYPE_REFRESH;
        vol_cb_args.args.refresh.type_id = type_id;

        if (H5VL_datatype_specific(dt->vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTLOAD, FAIL, "unable to refresh datatype");
    }

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Trefresh */

/*-------------------------------------------------------------------------
 * Function:    H5T__get_create_plist
 *
 * Purpose:     Returns a copy of the datatype creation property list.
 *
 * Note:        There are no datatype creation properties currently, just
 *              object creation ones.
 *
 * Return:      Success:    ID for a copy of the datatype creation
 *                          property list.  The property list ID should be
 *                          released by calling H5Pclose().
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5T__get_create_plist(const H5T_t *type)
{
    H5P_genplist_t *tcpl_plist;                    /* Existing datatype creation propertty list */
    H5P_genplist_t *new_plist;                     /* New datatype creation property list */
    hid_t           new_tcpl_id = FAIL;            /* New datatype creation property list */
    hid_t           ret_value   = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(type);

    /* Copy the default datatype creation property list */
    if (NULL == (tcpl_plist = (H5P_genplist_t *)H5I_object(H5P_LST_DATATYPE_CREATE_ID_g)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_BADTYPE, H5I_INVALID_HID, "can't get default creation property list");
    if ((new_tcpl_id = H5P_copy_plist(tcpl_plist, true)) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, H5I_INVALID_HID, "unable to copy the creation property list");

    /* Get property list object for new TCPL */
    if (NULL == (new_plist = (H5P_genplist_t *)H5I_object(new_tcpl_id)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_BADTYPE, H5I_INVALID_HID, "can't get property list");

    /* Retrieve any object creation properties */
    if (H5O_get_create_plist(&type->oloc, new_plist) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, H5I_INVALID_HID, "can't get object creation info");

    /* Set the return value */
    ret_value = new_tcpl_id;

done:
    if (ret_value < 0)
        if (new_tcpl_id > 0)
            if (H5I_dec_app_ref(new_tcpl_id) < 0)
                HDONE_ERROR(H5E_DATATYPE, H5E_CANTDEC, H5I_INVALID_HID, "unable to close temporary object");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__get_create_plist() */

/*-------------------------------------------------------------------------
 * Function:	H5T__open_name
 *
 * Purpose:	Open a named datatype.
 *
 * Return:	Success:	Ptr to a new datatype.
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
H5T_t *
H5T__open_name(const H5G_loc_t *loc, const char *name)
{
    H5T_t     *dt = NULL;         /* Datatype opened in file */
    H5G_name_t path;              /* Datatype group hier. path */
    H5O_loc_t  oloc;              /* Datatype object location */
    H5G_loc_t  type_loc;          /* Group object for datatype */
    H5O_type_t obj_type;          /* Type of object at location */
    bool       obj_found = false; /* Object at 'name' found */
    H5T_t     *ret_value = NULL;  /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(loc);
    assert(name);

    /* Set up datatype location to fill in */
    type_loc.oloc = &oloc;
    type_loc.path = &path;
    H5G_loc_reset(&type_loc);

    /*
     * Find the named datatype object header and read the datatype message
     * from it.
     */
    if (H5G_loc_find(loc, name, &type_loc /*out*/) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_NOTFOUND, NULL, "not found");
    obj_found = true;

    /* Check that the object found is the correct type */
    if (H5O_obj_type(&oloc, &obj_type) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, NULL, "can't get object type");
    if (obj_type != H5O_TYPE_NAMED_DATATYPE)
        HGOTO_ERROR(H5E_DATATYPE, H5E_BADTYPE, NULL, "not a named datatype");

    /* Open it */
    if (NULL == (dt = H5T_open(&type_loc)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTOPENOBJ, NULL, "unable to open named datatype");

    ret_value = dt;

done:
    /* Error cleanup */
    if (NULL == ret_value)
        if (obj_found && H5_addr_defined(type_loc.oloc->addr))
            if (H5G_loc_free(&type_loc) < 0)
                HDONE_ERROR(H5E_DATATYPE, H5E_CANTRELEASE, NULL, "can't free location");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__open_name() */

/*-------------------------------------------------------------------------
 * Function:	H5T_open
 *
 * Purpose:	Open a named datatype.
 *
 * Return:	Success:	Ptr to a new datatype.
 *
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
H5T_t *
H5T_open(const H5G_loc_t *loc)
{
    H5T_shared_t *shared_fo = NULL;
    H5T_t        *dt        = NULL;
    H5T_t        *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI(NULL)

    assert(loc);

    /* Check if datatype was already open */
    if (NULL == (shared_fo = (H5T_shared_t *)H5FO_opened(loc->oloc->file, loc->oloc->addr))) {
        /* Clear any errors from H5FO_opened() */
        H5E_clear_stack(NULL);

        /* Open the datatype object */
        if (NULL == (dt = H5T__open_oid(loc)))
            HGOTO_ERROR(H5E_DATATYPE, H5E_NOTFOUND, NULL, "not found");

        /* Add the datatype to the list of opened objects in the file */
        if (H5FO_insert(dt->sh_loc.file, dt->sh_loc.u.loc.oh_addr, dt->shared, false) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINSERT, NULL,
                        "can't insert datatype into list of open objects");

        /* Increment object count for the object in the top file */
        if (H5FO_top_incr(dt->sh_loc.file, dt->sh_loc.u.loc.oh_addr) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINC, NULL, "can't increment object count");

        /* Mark any datatypes as being in memory now */
        if (H5T_set_loc(dt, NULL, H5T_LOC_MEMORY) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "invalid datatype location");

        dt->shared->fo_count = 1;
    } /* end if */
    else {
        if (NULL == (dt = H5FL_MALLOC(H5T_t)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "can't allocate space for datatype");
        dt->vol_obj = NULL;

#if defined(H5_USING_MEMCHECKER) || !defined(NDEBUG)
        /* Clear object location */
        if (H5O_loc_reset(&(dt->oloc)) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTOPENOBJ, NULL, "unable to reset location");

        /* Clear path name */
        if (H5G_name_reset(&(dt->path)) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTOPENOBJ, NULL, "unable to reset path");
#endif /* H5_USING_MEMCHECKER */

        /* Shallow copy (take ownership) of the object location object */
        if (H5O_loc_copy_shallow(&dt->oloc, loc->oloc) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, NULL, "can't copy object location");

        /* Shallow copy (take ownership) of the group hier. path */
        if (H5G_name_copy(&(dt->path), loc->path, H5_COPY_SHALLOW) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, NULL, "can't copy path");

        /* Set the shared component info */
        H5T_update_shared(dt);

        /* Point to shared datatype info */
        dt->shared = shared_fo;

        /* Mark any datatypes as being in memory now */
        if (H5T_set_loc(dt, NULL, H5T_LOC_MEMORY) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "invalid datatype location");

        /* Increment ref. count on shared info */
        shared_fo->fo_count++;

        /* Check if the object has been opened through the top file yet */
        if (H5FO_top_count(dt->sh_loc.file, dt->sh_loc.u.loc.oh_addr) == 0) {
            /* Open the object through this top file */
            if (H5O_open(&(dt->oloc)) < 0)
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTOPENOBJ, NULL, "unable to open object header");
        } /* end if */

        /* Increment object count for the object in the top file */
        if (H5FO_top_incr(dt->sh_loc.file, dt->sh_loc.u.loc.oh_addr) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINC, NULL, "can't increment object count");
    } /* end else */

    ret_value = dt;

done:
    if (ret_value == NULL) {
        if (dt) {
            if (shared_fo == NULL) { /* Need to free shared file object */
                if (dt->shared->owned_vol_obj && H5VL_free_object(dt->shared->owned_vol_obj) < 0)
                    HDONE_ERROR(H5E_DATATYPE, H5E_CANTCLOSEOBJ, NULL, "unable to close owned VOL object");
                dt->shared = H5FL_FREE(H5T_shared_t, dt->shared);
            } /* end if */

            H5O_loc_free(&(dt->oloc));
            H5G_name_free(&(dt->path));

            dt = H5FL_FREE(H5T_t, dt);
        } /* end if */

        if (shared_fo)
            shared_fo->fo_count--;
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_open() */

/*-------------------------------------------------------------------------
 * Function:	H5T__open_oid
 *
 * Purpose:	Open a named datatype.
 *
 * Return:	Success:	Ptr to a new datatype.
 *
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
static H5T_t *
H5T__open_oid(const H5G_loc_t *loc)
{
    H5T_t *dt        = NULL; /* Datatype from the file */
    H5T_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE_TAG(loc->oloc->addr)

    assert(loc);

    /* Open named datatype object in file */
    if (H5O_open(loc->oloc) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTOPENOBJ, NULL, "unable to open named datatype");

    /* Deserialize the datatype message into a datatype in memory */
    if (NULL == (dt = (H5T_t *)H5O_msg_read(loc->oloc, H5O_DTYPE_ID, NULL)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "unable to load type message from object header");

    /* Mark the type as named and open */
    dt->shared->state = H5T_STATE_OPEN;

    /* Shallow copy (take ownership) of the object location object */
    if (H5O_loc_copy_shallow(&dt->oloc, loc->oloc) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, NULL, "can't copy object location");

    /* Shallow copy (take ownership) of the group hier. path */
    if (H5G_name_copy(&(dt->path), loc->path, H5_COPY_SHALLOW) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, NULL, "can't copy path");

    /* Set the shared component info */
    H5T_update_shared(dt);

    /* Set return value */
    ret_value = dt;

done:
    if (ret_value == NULL)
        if (dt == NULL)
            H5O_close(loc->oloc, NULL);

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5T__open_oid() */

/*-------------------------------------------------------------------------
 * Function:	H5T_update_shared
 *
 * Purpose:	Update the shared location information from the object location
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5T_update_shared(H5T_t *dt)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(dt);

    /* Set the shared location fields from the named datatype info */
    H5O_UPDATE_SHARED(&(dt->sh_loc), H5O_SHARE_TYPE_COMMITTED, dt->oloc.file, H5O_DTYPE_ID, 0, dt->oloc.addr)

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5T_update_shared() */

/*-------------------------------------------------------------------------
 * Function:    H5T_construct_datatype
 *
 * Purpose:     Create a Library datatype with a connector specific datatype object
 *
 * Return:      Success:    A type structure
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
H5T_t *
H5T_construct_datatype(H5VL_object_t *vol_obj)
{
    H5T_t                   *dt = NULL;        /* Datatype object from VOL connector */
    H5VL_datatype_get_args_t vol_cb_args;      /* Arguments to VOL callback */
    size_t                   nalloc    = 0;    /* Size required to store serialized form of datatype */
    void                    *buf       = NULL; /* Buffer to store serialized datatype */
    H5T_t                   *ret_value = NULL;

    FUNC_ENTER_NOAPI(NULL)

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                   = H5VL_DATATYPE_GET_BINARY_SIZE;
    vol_cb_args.args.get_binary_size.size = &nalloc;

    /* Get required buf size for encoding the datatype */
    if (H5VL_datatype_get(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "unable to get datatype serialized size");

    /* Allocate buffer to store binary description of the datatype */
    if (NULL == (buf = (void *)H5MM_calloc(nalloc)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "can't allocate space for datatype");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                  = H5VL_DATATYPE_GET_BINARY;
    vol_cb_args.args.get_binary.buf      = buf;
    vol_cb_args.args.get_binary.buf_size = nalloc;

    /* get binary description of the datatype */
    if (H5VL_datatype_get(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "unable to get serialized datatype");

    /* Construct datatype, from serialized form in buffer */
    if (NULL == (dt = H5T_decode(nalloc, buf)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "can't decode datatype");
    dt->vol_obj = vol_obj;

    /* Set return value */
    ret_value = dt;

done:
    if (buf)
        buf = H5MM_xfree(buf);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_construct_datatype() */

/*-------------------------------------------------------------------------
 * Function:    H5T_get_named_type
 *
 * Purpose:     Returns the VOL object for the named datatype if it exists
 *
 * Return:      Success:    Pointer to the VOL object for the datatype
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
H5VL_object_t *
H5T_get_named_type(const H5T_t *dt)
{
    H5VL_object_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    if (NULL != dt->vol_obj)
        ret_value = dt->vol_obj;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_get_named_type() */

/*-------------------------------------------------------------------------
 * Function:    H5T_get_actual_type
 *
 * Purpose:     Returns underlying native datatype created by native connector
 *              if datatype is committed, otherwise return the datatype
 *              object associate with the ID.
 *
 * Return:      Success:    Pointer to the VOL-managed data for this datatype
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
H5T_t *
H5T_get_actual_type(H5T_t *dt)
{
    H5T_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check if the datatype is committed */
    if (NULL == dt->vol_obj)
        ret_value = dt;
    else
        ret_value = (H5T_t *)H5VL_object_data(dt->vol_obj);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_get_actual_type() */

/*-------------------------------------------------------------------------
 * Function:    H5T_save_refresh_state
 *
 * Purpose:     Save state for datatype reconstruction after a refresh.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5T_save_refresh_state(hid_t tid, H5O_shared_t *cached_H5O_shared)
{
    H5T_t *dt        = NULL;    /* High level datatype object that wraps the VOL object */
    H5T_t *vol_dt    = NULL;    /* H5T_t pointer stored in the datatype's vol_obj field */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    assert(cached_H5O_shared);

    if (NULL == (dt = (H5T_t *)H5I_object_verify(tid, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "tid is not a datatype ID");
    vol_dt = H5T_get_actual_type(dt);
    if (NULL == vol_dt)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "tid is not not a named datatype ID");

    /* Increase the count on the file object */
    vol_dt->shared->fo_count += 1;

    /* Increment object count for the object in the top file */
    if (H5FO_top_incr(vol_dt->sh_loc.file, vol_dt->sh_loc.u.loc.oh_addr) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINC, FAIL, "can't increment object count");

    /* Cache the H5O_shared_t data */
    H5MM_memcpy(cached_H5O_shared, &(vol_dt->sh_loc), sizeof(H5O_shared_t));

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_save_refresh_state() */

/*-------------------------------------------------------------------------
 * Function:    H5T_restore_refresh_state
 *
 * Purpose:     Restore state for datatype reconstruction after a refresh.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5T_restore_refresh_state(hid_t tid, H5O_shared_t *cached_H5O_shared)
{
    H5T_t *dt        = NULL;    /* High level datatype object that wraps the VOL object */
    H5T_t *vol_dt    = NULL;    /* H5T_t pointer stored in the datatype's vol_obj field */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    assert(cached_H5O_shared);

    if (NULL == (dt = (H5T_t *)H5I_object_verify(tid, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "tid not a datatype ID");
    vol_dt = H5T_get_actual_type(dt);
    if (NULL == vol_dt)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "tid is not not a named datatype ID");

    /* Restore the H5O_shared_t data */
    H5MM_memcpy(&(vol_dt->sh_loc), cached_H5O_shared, sizeof(H5O_shared_t));

    /* Decrement the ref. count for this object in the top file */
    if (H5FO_top_decr(vol_dt->sh_loc.file, vol_dt->sh_loc.u.loc.oh_addr) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTDEC, FAIL, "can't decrement object count");

    /* Decrease the count on the file object */
    vol_dt->shared->fo_count -= 1;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_restore_refresh_state() */

/*-------------------------------------------------------------------------
 * Function:    H5T_already_vol_managed
 *
 * Purpose:     Check if the committed datatype is already VOL managed
 *
 * Return:      true / false
 *
 *-------------------------------------------------------------------------
 */
bool
H5T_already_vol_managed(const H5T_t *dt)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check */
    assert(dt);

    FUNC_LEAVE_NOAPI(dt->vol_obj != NULL)
} /* end H5T_already_vol_managed() */

/*-------------------------------------------------------------------------
 * Function:    H5T_invoke_vol_optional
 *
 * Purpose:     Invokes an optional VOL connector-specific operation on a named datatype
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5T_invoke_vol_optional(H5T_t *dt, H5VL_optional_args_t *args, hid_t dxpl_id, void **req,
                        H5VL_object_t **vol_obj_ptr)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check that datatype is committed */
    if (!H5T_is_named(dt))
        HGOTO_ERROR(H5E_DATATYPE, H5E_BADTYPE, FAIL, "not a committed datatype");

    /* Only invoke callback if VOL object is set for the datatype */
    if (dt->vol_obj)
        if (H5VL_datatype_optional_op(dt->vol_obj, args, dxpl_id, req, vol_obj_ptr) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTOPERATE, FAIL, "unable to execute datatype optional callback");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_invoke_vol_optional() */
