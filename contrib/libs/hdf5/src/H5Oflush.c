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
 * Created:     H5Oflush.c
 *
 * Purpose:     Object flush/refresh routines.
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5Omodule.h" /* This source code file is part of the H5O module */
#define H5T_FRIEND     /* Suppress error about including H5Tpkg */

/***********/
/* Headers */
/***********/

#include "H5private.h"   /* Generic Functions */
#include "H5CXprivate.h" /* API Contexts */
#include "H5Dprivate.h"  /* Datasets */
#include "H5Eprivate.h"  /* Errors   */
#include "H5Fprivate.h"  /* Files    */
#include "H5Gprivate.h"  /* Groups   */
#include "H5Iprivate.h"  /* IDs      */
#include "H5Opkg.h"      /* Objects  */
#include "H5Tpkg.h"      /* Datatypes */

/********************/
/* Local Prototypes */
/********************/
static herr_t H5O__oh_tag(const H5O_loc_t *oloc, haddr_t *tag);
static herr_t H5O__refresh_metadata_close(H5O_loc_t *oloc, H5G_loc_t *obj_loc, hid_t oid);

/*************/
/* Functions */
/*************/

/*-------------------------------------------------------------------------
 * Function:    H5O_flush
 *
 * Purpose:     Internal routine to flush an object
 *
 * Return:	Success:	Non-negative
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O_flush(H5O_loc_t *oloc, hid_t obj_id)
{
    void                  *obj_ptr;             /* Pointer to object */
    const H5O_obj_class_t *obj_class;           /* Class of object */
    herr_t                 ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Currently, H5Oflush causes H5Fclose to trigger an assertion failure in metadata cache.
     * Leave this situation for the future solution */
    if (H5F_HAS_FEATURE(oloc->file, H5FD_FEAT_HAS_MPI))
        HGOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, FAIL, "H5Oflush isn't supported for parallel");

    /* Get the object pointer */
    if (NULL == (obj_ptr = H5VL_object(obj_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid object identifier");

    /* Get the object class */
    if (NULL == (obj_class = H5O__obj_class(oloc)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTINIT, FAIL, "unable to determine object class");

    /* Flush the object of this class */
    if (obj_class->flush && obj_class->flush(obj_ptr) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTFLUSH, FAIL, "unable to flush object");

    /* Flush the object metadata and invoke flush callback */
    if (H5O_flush_common(oloc, obj_id) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTFLUSH, FAIL, "unable to flush object and object flush callback");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O_flush() */

/*-------------------------------------------------------------------------
 * Function:    H5O_flush_common
 *
 * Purpose:    	Flushes the object's metadata
 *		Invokes the user-defined callback if there is one.
 *
 * Return:  	Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O_flush_common(H5O_loc_t *oloc, hid_t obj_id)
{
    haddr_t tag       = 0;
    herr_t  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Retrieve tag for object */
    if (H5O__oh_tag(oloc, &tag) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTFLUSH, FAIL, "unable to flush object metadata");

    /* Flush metadata based on tag value of the object */
    if (H5F_flush_tagged_metadata(oloc->file, tag) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTFLUSH, FAIL, "unable to flush tagged metadata");

    /* Check to invoke callback */
    if (H5F_object_flush_cb(oloc->file, obj_id) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTFLUSH, FAIL, "unable to do object flush callback");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O_flush_common() */

/*-------------------------------------------------------------------------
 * Function:    H5O__oh_tag
 *
 * Purpose:     Get object header's address--tag value for the object
 *
 * Return:  	Success:    Non-negative
 *          	Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__oh_tag(const H5O_loc_t *oloc, haddr_t *tag)
{
    H5O_t *oh        = NULL;    /* Object header */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(oloc);

    /* Get object header for object */
    if (NULL == (oh = H5O_protect(oloc, H5AC__READ_ONLY_FLAG, false)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTPROTECT, FAIL, "unable to protect object's object header");

    /* Get object header's address (i.e. the tag value for this object) */
    if (HADDR_UNDEF == (*tag = H5O_OH_GET_ADDR(oh)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "unable to get address of object header");

done:
    /* Unprotect object header on failure */
    if (oh && H5O_unprotect(oloc, oh, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTUNPROTECT, FAIL, "unable to release object header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__oh_tag() */

/*-------------------------------------------------------------------------
 * Function:    H5O_refresh_metadata
 *
 * Purpose:     Refreshes all buffers associated with an object.
 *
 * Note:	This is based on the original H5O_refresh_metadata() but
 *	        is split into 2 routines.
 *	        This is done so that H5Fstart_swmr_write() can use these
 *	        2 routines to refresh opened objects.  This may be
 *	        restored back to the original code when H5Fstart_swmr_write()
 *	        uses a different approach to handle issues with opened objects.
 *	 	H5Fstart_swmr_write() no longer calls the 1st routine.	(12/24/15)
 *
 * Return:    	Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O_refresh_metadata(H5O_loc_t *oloc, hid_t oid)
{
    H5VL_object_t *vol_obj   = NULL;  /* VOL object associated with the ID */
    bool           objs_incr = false; /* Whether the object count in the file was incremented */
    H5F_t         *file      = NULL;
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* If the file is opened with write access, no need to perform refresh actions. */
    if (!(H5F_INTENT(oloc->file) & H5F_ACC_RDWR)) {
        H5G_loc_t    obj_loc;
        H5O_loc_t    obj_oloc;
        H5G_name_t   obj_path;
        H5O_shared_t cached_H5O_shared;
        H5VL_t      *connector = NULL;

        /* Hold a copy of the object's file pointer, since closing the object will
         * invalidate the file pointer in the oloc.
         */
        file = oloc->file;

        /* Create empty object location */
        obj_loc.oloc = &obj_oloc;
        obj_loc.path = &obj_path;
        H5G_loc_reset(&obj_loc);

        /* "Fake" another open object in the file, so that it doesn't get closed
         *  if this object is the only thing holding the file open.
         */
        H5F_incr_nopen_objs(oloc->file);
        objs_incr = true;

        /* Save important datatype state */
        if (H5I_get_type(oid) == H5I_DATATYPE)
            if (H5T_save_refresh_state(oid, &cached_H5O_shared) < 0)
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTOPENOBJ, FAIL, "unable to save datatype state");

        /* Get the VOL object from the ID and cache a pointer to the connector.
         * The vol_obj will disappear when the underlying object is closed, so
         * we can't use that directly.
         */
        if (NULL == (vol_obj = H5VL_vol_object(oid)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid object identifier");
        connector = vol_obj->connector;

        /* Bump the number of references on the VOL connector.
         * If you don't do this, VDS refreshes can accidentally close the connector.
         */
        connector->nrefs++;

        /* Close object & evict its metadata */
        if (H5O__refresh_metadata_close(oloc, &obj_loc, oid) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, FAIL, "unable to refresh object");

        /* Re-open the object, re-fetching its metadata */
        if (H5O_refresh_metadata_reopen(oid, H5P_DEFAULT, &obj_loc, connector, false) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, FAIL, "unable to refresh object");

        /* Restore the number of references on the VOL connector */
        connector->nrefs--;

        /* Restore important datatype state */
        if (H5I_get_type(oid) == H5I_DATATYPE)
            if (H5T_restore_refresh_state(oid, &cached_H5O_shared) < 0)
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTOPENOBJ, FAIL, "unable to restore datatype state");

    } /* end if */

done:
    if (objs_incr && file)
        H5F_decr_nopen_objs(file);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O_refresh_metadata() */

/*-------------------------------------------------------------------------
 * Function:    H5O__refresh_metadata_close
 *
 * Purpose:     This is the first part of the original routine H5O_refresh_metadata().
 *		(1) Save object location information.
 *		(2) Handle multiple dataset opens
 *		(3) Get object cork status
 *		(4) Close the object
 *		(5) Flush and evict object metadata
 *		(6) Re-cork the object if needed
 *
 * Return:  Success:    Non-negative
 *          Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__refresh_metadata_close(H5O_loc_t *oloc, H5G_loc_t *obj_loc, hid_t oid)
{
    H5F_t  *file;                /* Local copy of the object's file pointer */
    haddr_t tag       = 0;       /* Tag for object */
    bool    corked    = false;   /* Whether object's metadata is corked */
    herr_t  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Make deep local copy of object's location information */
    if (obj_loc) {
        H5G_loc_t tmp_loc;

        H5G_loc(oid, &tmp_loc);
        H5G_loc_copy(obj_loc, &tmp_loc, H5_COPY_DEEP);
    } /* end if */

    /* Handle close for multiple dataset opens */
    if (H5I_get_type(oid) == H5I_DATASET)
        if (H5D_mult_refresh_close(oid) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, FAIL, "unable to prepare refresh for dataset");

    /* Retrieve tag for object */
    if (H5O__oh_tag(oloc, &tag) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTFLUSH, FAIL, "unable to get object header address");

    /* Get cork status of the object with tag */
    if (H5AC_cork(oloc->file, tag, H5AC__GET_CORKED, &corked) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_SYSTEM, FAIL, "unable to retrieve an object's cork status");

    /* Hold a copy of the object's file pointer, since closing the object will
     * invalidate the file pointer in the oloc.
     */
    file = oloc->file;
    /* Close the object */
    if (H5I_dec_ref(oid) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTINIT, FAIL, "unable to close object");

    /* Flush metadata based on tag value of the object */
    if (H5F_flush_tagged_metadata(file, tag) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTFLUSH, FAIL, "unable to flush tagged metadata");

    /* Evict the object's tagged metadata */
    if (H5AC_evict_tagged_metadata(file, tag, true) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTFLUSH, FAIL, "unable to evict metadata");

    /* Re-cork object with tag */
    if (corked)
        if (H5AC_cork(file, tag, H5AC__SET_CORK, &corked) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_SYSTEM, FAIL, "unable to cork the object");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__refresh_metadata_close() */

/*-------------------------------------------------------------------------
 * Function:    H5O_refresh_metadata_reopen
 *
 * Purpose:     This is the second part of the original routine H5O_refresh_metadata().
 *		  (1) Re-open object with the saved object location information.
 *		  (2) Re-register object ID with the re-opened object.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O_refresh_metadata_reopen(hid_t oid, hid_t apl_id, H5G_loc_t *obj_loc, H5VL_t *vol_connector,
                            bool start_swmr)
{
    void      *object = NULL;       /* Object for this operation */
    H5I_type_t type;                /* Type of object for the ID */
    herr_t     ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(obj_loc);
    assert(vol_connector);

    /* Get object's type */
    type = H5I_get_type(oid);

    switch (type) {
        case H5I_GROUP:
            /* Re-open the group */
            if (NULL == (object = H5G_open(obj_loc)))
                HGOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, FAIL, "unable to open group");
            break;

        case H5I_DATATYPE:
            /* Re-open the named datatype */
            if (NULL == (object = H5T_open(obj_loc)))
                HGOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, FAIL, "unable to open named datatype");
            break;

        case H5I_DATASET:
            /* Set dataset access property list in API context if appropriate */
            if (H5CX_set_apl(&apl_id, H5P_CLS_DACC, oid, true) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL, "can't set access property list info");

            /* Re-open the dataset */
            if (NULL ==
                (object = H5D_open(obj_loc, apl_id == H5P_DEFAULT ? H5P_DATASET_ACCESS_DEFAULT : apl_id)))
                HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, FAIL, "unable to open dataset");
            if (!start_swmr) /* No need to handle multiple opens when H5Fstart_swmr_write() */
                if (H5D_mult_refresh_reopen((H5D_t *)object) < 0)
                    HGOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, FAIL, "unable to finish refresh for dataset");
            break;

        case H5I_MAP:
            HGOTO_ERROR(H5E_OHDR, H5E_BADTYPE, FAIL, "maps not supported in native VOL connector");

        case H5I_UNINIT:
        case H5I_BADID:
        case H5I_FILE:
        case H5I_DATASPACE:
        case H5I_ATTR:
        case H5I_VFL:
        case H5I_VOL:
        case H5I_GENPROP_CLS:
        case H5I_GENPROP_LST:
        case H5I_ERROR_CLASS:
        case H5I_ERROR_MSG:
        case H5I_ERROR_STACK:
        case H5I_SPACE_SEL_ITER:
        case H5I_EVENTSET:
        case H5I_NTYPES:
        default:
            HGOTO_ERROR(H5E_OHDR, H5E_BADTYPE, FAIL,
                        "not a valid file object ID (dataset, group, or datatype)");
            break;
    } /* end switch */

    /* Re-register ID for the object */
    if ((H5VL_register_using_existing_id(type, object, vol_connector, true, oid)) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTREGISTER, FAIL, "unable to re-register object ID after refresh");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O_refresh_metadata_reopen() */
