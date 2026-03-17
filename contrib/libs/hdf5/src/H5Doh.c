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

#include "H5Dmodule.h" /* This source code file is part of the H5D module */
#define H5O_FRIEND     /*suppress error about including H5Opkg	  */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5CXprivate.h" /* API Contexts                         */
#include "H5Dpkg.h"      /* Datasets				*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5FLprivate.h" /* Free lists                           */
#include "H5Iprivate.h"  /* IDs			  		*/
#include "H5Opkg.h"      /* Object headers			*/
#include "H5VLprivate.h" /* Virtual Object Layer                     */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Local Prototypes */
/********************/
static void      *H5O__dset_get_copy_file_udata(void);
static void       H5O__dset_free_copy_file_udata(void *);
static htri_t     H5O__dset_isa(const H5O_t *loc);
static void      *H5O__dset_open(const H5G_loc_t *obj_loc, H5I_type_t *opened_type);
static void      *H5O__dset_create(H5F_t *f, void *_crt_info, H5G_loc_t *obj_loc);
static H5O_loc_t *H5O__dset_get_oloc(hid_t obj_id);
static herr_t     H5O__dset_bh_info(const H5O_loc_t *loc, H5O_t *oh, H5_ih_info_t *bh_info);
static herr_t     H5O__dset_flush(void *_obj_ptr);

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
const H5O_obj_class_t H5O_OBJ_DATASET[1] = {{
    H5O_TYPE_DATASET,               /* object type			*/
    "dataset",                      /* object name, for debugging	*/
    H5O__dset_get_copy_file_udata,  /* get 'copy file' user data	*/
    H5O__dset_free_copy_file_udata, /* free 'copy file' user data	*/
    H5O__dset_isa,                  /* "isa" message		*/
    H5O__dset_open,                 /* open an object of this class */
    H5O__dset_create,               /* create an object of this class */
    H5O__dset_get_oloc,             /* get an object header location for an object */
    H5O__dset_bh_info,              /* get the index & heap info for an object */
    H5O__dset_flush                 /* flush an opened object of this class */
}};

/* Declare a free list to manage the H5D_copy_file_ud_t struct */
H5FL_DEFINE(H5D_copy_file_ud_t);

/*-------------------------------------------------------------------------
 * Function:	H5O__dset_get_copy_file_udata
 *
 * Purpose:	Allocates the user data needed for copying a dataset's
 *		object header from file to file.
 *
 * Return:	Success:	Non-NULL pointer to user data
 *
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5O__dset_get_copy_file_udata(void)
{
    void *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Allocate space for the 'copy file' user data for copying datasets */
    if (NULL == (ret_value = H5FL_CALLOC(H5D_copy_file_ud_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__dset_get_copy_file_udata() */

/*-------------------------------------------------------------------------
 * Function:	H5O__dset_free_copy_file_udata
 *
 * Purpose:	Release the user data needed for copying a dataset's
 *		object header from file to file.
 *
 * Return:	<none>
 *
 *-------------------------------------------------------------------------
 */
static void
H5O__dset_free_copy_file_udata(void *_udata)
{
    H5D_copy_file_ud_t *udata = (H5D_copy_file_ud_t *)_udata;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(udata);

    /* Release copy of dataset's dataspace extent, if it was set */
    if (udata->src_space_extent)
        H5O_msg_free(H5O_SDSPACE_ID, udata->src_space_extent);

    /* Release copy of dataset's datatype, if it was set */
    if (udata->src_dtype)
        H5T_close_real(udata->src_dtype);

    /* Release copy of dataset's filter pipeline, if it was set */
    if (udata->common.src_pline)
        H5O_msg_free(H5O_PLINE_ID, udata->common.src_pline);

    /* Release space for 'copy file' user data */
    udata = H5FL_FREE(H5D_copy_file_ud_t, udata);

    FUNC_LEAVE_NOAPI_VOID
} /* end H5O__dset_free_copy_file_udata() */

/*-------------------------------------------------------------------------
 * Function:	H5O__dset_isa
 *
 * Purpose:	Determines if an object has the requisite messages for being
 *		a dataset.
 *
 * Return:	Success:	true if the required dataset messages are
 *				present; false otherwise.
 *
 *		Failure:	FAIL if the existence of certain messages
 *				cannot be determined.
 *
 *-------------------------------------------------------------------------
 */
static htri_t
H5O__dset_isa(const H5O_t *oh)
{
    htri_t exists;           /* Flag if header message of interest exists */
    htri_t ret_value = true; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(oh);

    /* Datatype */
    if ((exists = H5O_msg_exists_oh(oh, H5O_DTYPE_ID)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to read object header");
    else if (!exists)
        HGOTO_DONE(false);

    /* Layout */
    if ((exists = H5O_msg_exists_oh(oh, H5O_SDSPACE_ID)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to read object header");
    else if (!exists)
        HGOTO_DONE(false);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__dset_isa() */

/*-------------------------------------------------------------------------
 * Function:	H5O__dset_open
 *
 * Purpose:	Open a dataset at a particular location
 *
 * Return:	Success:	Open object identifier
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static void *
H5O__dset_open(const H5G_loc_t *obj_loc, H5I_type_t *opened_type)
{
    H5D_t *dset = NULL;      /* Dataset opened */
    hid_t  dapl_id;          /* dapl to use to open this dataset */
    void  *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(obj_loc);

    *opened_type = H5I_DATASET;

    /* Get the LAPL (which is a superclass of DAPLs) from the API context, but
     * if it's the default link access property list or a custom link access
     * property list but not a dataset access property list, use the default
     * dataset access property list instead.  (Since LAPLs don't have the
     * additional properties that DAPLs have)
     */
    dapl_id = H5CX_get_lapl();
    if (dapl_id == H5P_LINK_ACCESS_DEFAULT)
        dapl_id = H5P_DATASET_ACCESS_DEFAULT;
    else {
        htri_t is_lapl, is_dapl; /* Class of LAPL from API context */

        /* Check class of LAPL from API context */
        if ((is_lapl = H5P_isa_class(dapl_id, H5P_LINK_ACCESS)) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, NULL, "unable to get LAPL status");
        if ((is_dapl = H5P_isa_class(dapl_id, H5P_DATASET_ACCESS)) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, NULL, "unable to get DAPL status");

        /* Switch to default DAPL if not an actual DAPL in the API context */
        if (!is_dapl && is_lapl)
            dapl_id = H5P_DATASET_ACCESS_DEFAULT;
    } /* end else */

    /* Open the dataset */
    if (NULL == (dset = H5D_open(obj_loc, dapl_id)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, NULL, "unable to open dataset");

    ret_value = (void *)dset;

done:
    if (NULL == ret_value)
        if (dset && H5D_close(dset) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, NULL, "unable to release dataset");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__dset_open() */

/*-------------------------------------------------------------------------
 * Function:	H5O__dset_create
 *
 * Purpose:	Create a dataset in a file
 *
 * Return:	Success:	Pointer to the dataset data structure
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5O__dset_create(H5F_t *f, void *_crt_info, H5G_loc_t *obj_loc)
{
    H5D_obj_create_t *crt_info  = (H5D_obj_create_t *)_crt_info; /* Dataset creation parameters */
    H5D_t            *dset      = NULL;                          /* New dataset created */
    void             *ret_value = NULL;                          /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(crt_info);
    assert(obj_loc);

    /* Create the dataset */
    if (NULL ==
        (dset = H5D__create(f, crt_info->type_id, crt_info->space, crt_info->dcpl_id, crt_info->dapl_id)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "unable to create dataset");

    /* Set up the new dataset's location */
    if (NULL == (obj_loc->oloc = H5D_oloc(dset)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "unable to get object location of dataset");
    if (NULL == (obj_loc->path = H5D_nameof(dset)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "unable to get path of dataset");

    /* Set the return value */
    ret_value = dset;

done:
    if (ret_value == NULL)
        if (dset && H5D_close(dset) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, NULL, "unable to release dataset");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__dset_create() */

/*-------------------------------------------------------------------------
 * Function:	H5O__dset_get_oloc
 *
 * Purpose:	Retrieve the object header location for an open object
 *
 * Return:	Success:	Pointer to object header location
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
static H5O_loc_t *
H5O__dset_get_oloc(hid_t obj_id)
{
    H5D_t     *dset;             /* Dataset opened */
    H5O_loc_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Get the dataset */
    if (NULL == (dset = (H5D_t *)H5VL_object(obj_id)))
        HGOTO_ERROR(H5E_OHDR, H5E_BADID, NULL, "couldn't get object from ID");

    /* Get the dataset's object header location */
    if (NULL == (ret_value = H5D_oloc(dset)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, NULL, "unable to get object location from object");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__dset_get_oloc() */

/*-------------------------------------------------------------------------
 * Function:    H5O__dset_bh_info
 *
 * Purpose:     Returns the amount of btree storage that is used for chunked
 *              dataset.
 *
 * Return:      Success:        non-negative
 *              Failure:        negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__dset_bh_info(const H5O_loc_t *loc, H5O_t *oh, H5_ih_info_t *bh_info)
{
    H5O_layout_t layout;              /* Data storage layout message */
    H5O_efl_t    efl;                 /* External File List message */
    bool         layout_read = false; /* Whether the layout message was read */
    bool         efl_read    = false; /* Whether the external file list message was read */
    htri_t       exists;              /* Flag if header message of interest exists */
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(loc);
    assert(loc->file);
    assert(H5_addr_defined(loc->addr));
    assert(oh);
    assert(bh_info);

    /* Get the layout message from the object header */
    if (NULL == H5O_msg_read_oh(loc->file, oh, H5O_LAYOUT_ID, &layout))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "can't find layout message");
    layout_read = true;

    /* Check for chunked dataset storage */
    if (layout.type == H5D_CHUNKED && H5D__chunk_is_space_alloc(&layout.storage)) {
        /* Get size of chunk index */
        if (H5D__chunk_bh_info(loc, oh, &layout, &(bh_info->index_size)) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "can't determine chunked dataset btree info");
    } /* end if */
    else if (layout.type == H5D_VIRTUAL && (layout.storage.u.virt.serial_list_hobjid.addr != HADDR_UNDEF)) {
        size_t virtual_heap_size;

        /* Get size of global heap object for virtual dataset */
        if (H5HG_get_obj_size(loc->file, &(layout.storage.u.virt.serial_list_hobjid), &virtual_heap_size) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL,
                        "can't get global heap size for virtual dataset mapping");

        /* Return heap size */
        bh_info->heap_size = (hsize_t)virtual_heap_size;
    } /* end if */

    /* Check for External File List message in the object header */
    if ((exists = H5O_msg_exists_oh(oh, H5O_EFL_ID)) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_NOTFOUND, FAIL, "unable to check for EFL message");

    if (exists && H5D__efl_is_space_alloc(&layout.storage)) {
        /* Start with clean EFL info */
        memset(&efl, 0, sizeof(efl));

        /* Get External File List message from the object header */
        if (NULL == H5O_msg_read_oh(loc->file, oh, H5O_EFL_ID, &efl))
            HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "can't find EFL message");
        efl_read = true;

        /* Get size of local heap for EFL message's file list */
        if (H5D__efl_bh_info(loc->file, &efl, &(bh_info->heap_size)) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "can't determine EFL heap info");
    } /* end if */

done:
    /* Free messages, if they've been read in */
    if (layout_read && H5O_msg_reset(H5O_LAYOUT_ID, &layout) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTRESET, FAIL, "unable to reset data storage layout message");
    if (efl_read && H5O_msg_reset(H5O_EFL_ID, &efl) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTRESET, FAIL, "unable to reset external file list message");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__dset_bh_info() */

/*-------------------------------------------------------------------------
 * Function:    H5O__dset_flush
 *
 * Purpose:     To flush any dataset information cached in memory
 *
 * Return:      Success:        non-negative
 *              Failure:        negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__dset_flush(void *_obj_ptr)
{
    H5D_t     *dset = (H5D_t *)_obj_ptr; /* Pointer to dataset object */
    H5O_type_t obj_type;                 /* Type of object at location */
    herr_t     ret_value = SUCCEED;      /* Return value */

    FUNC_ENTER_PACKAGE

    assert(dset);
    assert(&dset->oloc);

    /* Check that the object found is the correct type */
    if (H5O_obj_type(&dset->oloc, &obj_type) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get object type");
    if (obj_type != H5O_TYPE_DATASET)
        HGOTO_ERROR(H5E_DATASET, H5E_BADTYPE, FAIL, "not a dataset");

    if (H5D__flush_real(dset) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "unable to flush cached dataset info");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__dset_flush() */
