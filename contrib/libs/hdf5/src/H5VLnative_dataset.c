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
 * Purpose:     Dataset callbacks for the native VOL connector
 *
 */

/****************/
/* Module Setup */
/****************/

#define H5D_FRIEND /* Suppress error about including H5Dpkg    */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5CXprivate.h" /* API Contexts                             */
#include "H5Dpkg.h"      /* Datasets                                 */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5Fprivate.h"  /* Files                                    */
#include "H5Gprivate.h"  /* Groups                                   */
#include "H5Iprivate.h"  /* IDs                                      */
#include "H5MMprivate.h" /* Memory management                        */
#include "H5Pprivate.h"  /* Property lists                           */
#include "H5Sprivate.h"  /* Dataspaces                               */
#include "H5VLprivate.h" /* Virtual Object Layer                     */

#include "H5VLnative_private.h" /* Native VOL connector                     */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Local Prototypes */
/********************/

/* Helper routines for read/write API calls */
static herr_t H5VL__native_dataset_io_setup(size_t count, void *obj[], hid_t mem_type_id[],
                                            hid_t mem_space_id[], hid_t file_space_id[], hid_t dxpl_id,
                                            H5_flexible_const_ptr_t buf[], H5D_dset_io_info_t *dinfo);
static herr_t H5VL__native_dataset_io_cleanup(size_t count, hid_t mem_space_id[], hid_t file_space_id[],
                                              H5D_dset_io_info_t *dinfo);

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
 * Function:    H5VL__native_dataset_io_setup
 *
 * Purpose:     Set up file and memory dataspaces for dataset I/O operation
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL__native_dataset_io_setup(size_t count, void *obj[], hid_t mem_type_id[], hid_t mem_space_id[],
                              hid_t file_space_id[], hid_t dxpl_id, H5_flexible_const_ptr_t buf[],
                              H5D_dset_io_info_t *dinfo)
{
    H5F_shared_t *f_sh;
    size_t        i;
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(dinfo);

    /* Get shared file */
    f_sh = H5F_SHARED(((H5D_t *)obj[0])->oloc.file);

    /* Iterate over datasets */
    for (i = 0; i < count; i++) {
        /* Initialize fields not set here to prevent use of uninitialized */
        memset(&dinfo[i].layout_ops, 0, sizeof(dinfo[i].layout_ops));
        memset(&dinfo[i].io_ops, 0, sizeof(dinfo[i].io_ops));
        memset(&dinfo[i].layout_io_info, 0, sizeof(dinfo[i].layout_io_info));
        memset(&dinfo[i].type_info, 0, sizeof(dinfo[i].type_info));
        dinfo[i].store   = NULL;
        dinfo[i].layout  = NULL;
        dinfo[i].nelmts  = 0;
        dinfo[i].skip_io = false;

        /* Set up dset */
        dinfo[i].dset = (H5D_t *)obj[i];
        assert(dinfo[i].dset);

        /* Check dataset's file pointer is valid */
        if (NULL == dinfo[i].dset->oloc.file)
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "dataset is not associated with a file");
        if (f_sh != H5F_SHARED(dinfo[i].dset->oloc.file))
            HGOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, FAIL,
                        "different files detected in multi dataset I/O request");

        /* Set up memory type */
        dinfo[i].mem_type_id = mem_type_id[i];

        /* Set up file dataspace */
        if (H5S_ALL == file_space_id[i])
            /* Use dataspace for dataset */
            dinfo[i].file_space = dinfo[i].dset->shared->space;
        else if (H5S_BLOCK == file_space_id[i])
            HGOTO_ERROR(H5E_DATASET, H5E_BADTYPE, FAIL, "H5S_BLOCK is not allowed for file dataspace");
        else if (H5S_PLIST == file_space_id[i]) {
            H5P_genplist_t *plist; /* Property list pointer */
            H5S_t          *space; /* Dataspace to hold selection */

            /* Get the plist structure */
            if (NULL == (plist = H5P_object_verify(dxpl_id, H5P_DATASET_XFER)))
                HGOTO_ERROR(H5E_DATASET, H5E_BADID, FAIL, "bad dataset transfer property list");

            /* Get a pointer to the file space in the property list */
            if (H5P_peek(plist, H5D_XFER_DSET_IO_SEL_NAME, &space) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "error getting dataset I/O selection");

            /* Use dataspace for dataset */
            dinfo[i].file_space = dinfo[i].dset->shared->space;

            /* Copy, but share, selection from property list to dataset's dataspace */
            if (H5S_SELECT_COPY(dinfo[i].file_space, space, true) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "can't copy dataset I/O selection");
        } /* end else-if */
        else {
            /* Get the dataspace pointer */
            if (NULL == (dinfo[i].file_space = (H5S_t *)H5I_object_verify(file_space_id[i], H5I_DATASPACE)))
                HGOTO_ERROR(H5E_DATASET, H5E_BADTYPE, FAIL, "file_space_id is not a dataspace ID");
        } /* end else */

        /* Get dataspace for memory buffer */
        if (H5S_ALL == mem_space_id[i])
            dinfo[i].mem_space = dinfo[i].file_space;
        else if (H5S_BLOCK == mem_space_id[i]) {
            hsize_t nelmts; /* # of selected elements in file */

            /* Get the # of elements selected */
            nelmts = H5S_GET_SELECT_NPOINTS(dinfo[i].file_space);

            /* Check for any elements */
            if (nelmts > 0) {
                /* Create a 1-D dataspace of the same # of elements */
                if (NULL == (dinfo[i].mem_space = H5S_create_simple(1, &nelmts, NULL)))
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTCREATE, FAIL,
                                "unable to create simple memory dataspace");
            } /* end if */
            else {
                /* Create a NULL dataspace of the same # of elements */
                if (NULL == (dinfo[i].mem_space = H5S_create(H5S_NULL)))
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTCREATE, FAIL, "unable to create NULL memory dataspace");
            } /* end else */
        }     /* end if */
        else if (H5S_PLIST == mem_space_id[i])
            HGOTO_ERROR(H5E_DATASET, H5E_BADTYPE, FAIL, "H5S_PLIST is not allowed for memory dataspace");
        else {
            /* Get the dataspace pointer */
            if (NULL == (dinfo[i].mem_space = (H5S_t *)H5I_object_verify(mem_space_id[i], H5I_DATASPACE)))
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "mem_space_id is not a dataspace ID");
        } /* end else */

        /* Check for valid selections */
        if (H5S_SELECT_VALID(dinfo[i].file_space) != true)
            HGOTO_ERROR(H5E_DATASPACE, H5E_BADRANGE, FAIL,
                        "selection + offset not within extent for file dataspace");
        if (H5S_SELECT_VALID(dinfo[i].mem_space) != true)
            HGOTO_ERROR(H5E_DATASPACE, H5E_BADRANGE, FAIL,
                        "selection + offset not within extent for memory dataspace");

        /* Set up buf */
        dinfo[i].buf = buf[i];
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_dataset_io_setup() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__native_dataset_io_cleanup
 *
 * Purpose:     Frees memory allocated by H5VL__native_dataset_io_setup()
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL__native_dataset_io_cleanup(size_t count, hid_t mem_space_id[], hid_t file_space_id[],
                                H5D_dset_io_info_t *dinfo)
{
    size_t i;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(dinfo);

    /* Iterate over datasets */
    for (i = 0; i < count; i++) {
        /* Free memory dataspace if it was created.  Use HDONE_ERROR in this function so we always
         * try to free everything we can. */
        if (H5S_BLOCK == mem_space_id[i] && dinfo[i].mem_space)
            if (H5S_close(dinfo[i].mem_space) < 0)
                HDONE_ERROR(H5E_DATASET, H5E_CANTRELEASE, FAIL,
                            "unable to release temporary memory dataspace for H5S_BLOCK");

        /* Reset file dataspace selection if it was copied from the property list */
        if (H5S_PLIST == file_space_id[i] && dinfo[i].file_space)
            if (H5S_select_all(dinfo[i].file_space, true) < 0)
                HDONE_ERROR(H5E_DATASET, H5E_CANTRELEASE, FAIL,
                            "unable to release file dataspace selection for H5S_PLIST");
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_dataset_io_cleanup() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__native_dataset_create
 *
 * Purpose:     Handles the dataset create callback
 *
 * Return:      Success:    dataset pointer
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5VL__native_dataset_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id,
                            hid_t type_id, hid_t space_id, hid_t dcpl_id, hid_t dapl_id,
                            hid_t H5_ATTR_UNUSED dxpl_id, void H5_ATTR_UNUSED **req)
{
    H5G_loc_t    loc;         /* Object location to insert dataset into */
    H5D_t       *dset = NULL; /* New dataset's info */
    const H5S_t *space;       /* Dataspace for dataset */
    void        *ret_value;

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    if (H5G_loc_real(obj, loc_params->obj_type, &loc) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a file or file object");
    if (H5I_DATATYPE != H5I_get_type(type_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a datatype ID");
    if (NULL == (space = (const H5S_t *)H5I_object_verify(space_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a dataspace ID");

    /* H5Dcreate_anon */
    if (NULL == name) {
        /* build and open the new dataset */
        if (NULL == (dset = H5D__create(loc.oloc->file, type_id, space, dcpl_id, dapl_id)))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "unable to create dataset");
    } /* end if */
    /* H5Dcreate2 */
    else {
        /* Create the new dataset & get its ID */
        if (NULL == (dset = H5D__create_named(&loc, name, type_id, space, lcpl_id, dcpl_id, dapl_id)))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "unable to create dataset");
    } /* end else */

    ret_value = (void *)dset;

done:
    if (NULL == name) {
        /* Release the dataset's object header, if it was created */
        if (dset) {
            H5O_loc_t *oloc; /* Object location for dataset */

            /* Get the new dataset's object location */
            if (NULL == (oloc = H5D_oloc(dset)))
                HDONE_ERROR(H5E_DATASET, H5E_CANTGET, NULL, "unable to get object location of dataset");

            /* Decrement refcount on dataset's object header in memory */
            if (H5O_dec_rc_by_loc(oloc) < 0)
                HDONE_ERROR(H5E_DATASET, H5E_CANTDEC, NULL,
                            "unable to decrement refcount on newly created object");
        } /* end if */
    }     /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_dataset_create() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__native_dataset_open
 *
 * Purpose:     Handles the dataset open callback
 *
 * Return:      Success:    dataset pointer
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5VL__native_dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id,
                          hid_t H5_ATTR_UNUSED dxpl_id, void H5_ATTR_UNUSED **req)
{
    H5D_t    *dset = NULL;
    H5G_loc_t loc; /* Object location of group */
    void     *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    if (H5G_loc_real(obj, loc_params->obj_type, &loc) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a file or file object");

    /* Open the dataset */
    if (NULL == (dset = H5D__open_name(&loc, name, dapl_id)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, NULL, "unable to open dataset");

    ret_value = (void *)dset;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_dataset_open() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__native_dataset_read
 *
 * Purpose:     Handles the dataset read callback
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL__native_dataset_read(size_t count, void *obj[], hid_t mem_type_id[], hid_t mem_space_id[],
                          hid_t file_space_id[], hid_t dxpl_id, void *buf[], void H5_ATTR_UNUSED **req)
{
    H5D_dset_io_info_t  dinfo_local;
    H5D_dset_io_info_t *dinfo     = &dinfo_local;
    herr_t              ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Allocate dataset info array if necessary */
    if (count > 1)
        if (NULL == (dinfo = (H5D_dset_io_info_t *)H5MM_malloc(count * sizeof(H5D_dset_io_info_t))))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "couldn't allocate dset info array buffer");

    /* Get file & memory dataspaces */
    if (H5VL__native_dataset_io_setup(count, obj, mem_type_id, mem_space_id, file_space_id, dxpl_id,
                                      (H5_flexible_const_ptr_t *)buf, dinfo) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to set up file and memory dataspaces");

    /* Set DXPL for operation */
    H5CX_set_dxpl(dxpl_id);

    /* Read raw data.  Call H5D__read directly in single dset case. */
    if (H5D__read(count, dinfo) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "can't read data");

done:
    /* Clean up */
    if (H5VL__native_dataset_io_cleanup(count, mem_space_id, file_space_id, dinfo) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTRELEASE, FAIL, "unable to release dataset info");

    if (dinfo != &dinfo_local)
        H5MM_xfree(dinfo);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_dataset_read() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__native_dataset_write
 *
 * Purpose:     Handles the dataset write callback
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL__native_dataset_write(size_t count, void *obj[], hid_t mem_type_id[], hid_t mem_space_id[],
                           hid_t file_space_id[], hid_t dxpl_id, const void *buf[], void H5_ATTR_UNUSED **req)
{
    H5D_dset_io_info_t  dinfo_local;
    H5D_dset_io_info_t *dinfo     = &dinfo_local;
    herr_t              ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Allocate dataset info array if necessary */
    if (count > 1)
        if (NULL == (dinfo = (H5D_dset_io_info_t *)H5MM_malloc(count * sizeof(H5D_dset_io_info_t))))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "couldn't allocate dset info array buffer");

    /* Get file & memory dataspaces */
    if (H5VL__native_dataset_io_setup(count, obj, mem_type_id, mem_space_id, file_space_id, dxpl_id,
                                      (H5_flexible_const_ptr_t *)buf, dinfo) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to set up file and memory dataspaces");

    /* Set DXPL for operation */
    H5CX_set_dxpl(dxpl_id);

    /* Write raw data.  Call H5D__write directly in single dset case. */
    if (H5D__write(count, dinfo) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "can't write data");

done:
    /* Clean up */
    if (H5VL__native_dataset_io_cleanup(count, mem_space_id, file_space_id, dinfo) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTRELEASE, FAIL, "unable to release dataset info");

    if (dinfo != &dinfo_local)
        H5MM_xfree(dinfo);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_dataset_write() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__native_dataset_get
 *
 * Purpose:     Handles the dataset get callback
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL__native_dataset_get(void *obj, H5VL_dataset_get_args_t *args, hid_t H5_ATTR_UNUSED dxpl_id,
                         void H5_ATTR_UNUSED **req)
{
    H5D_t *dset      = (H5D_t *)obj;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    switch (args->op_type) {
        /* H5Dget_space */
        case H5VL_DATASET_GET_SPACE: {
            if ((args->args.get_space.space_id = H5D__get_space(dset)) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get space ID of dataset");

            break;
        }

        /* H5Dget_space_status */
        case H5VL_DATASET_GET_SPACE_STATUS: {
            if (H5D__get_space_status(dset, args->args.get_space_status.status) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to get space status");

            break;
        }

        /* H5Dget_type */
        case H5VL_DATASET_GET_TYPE: {
            if ((args->args.get_type.type_id = H5D__get_type(dset)) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get datatype ID of dataset");

            break;
        }

        /* H5Dget_create_plist */
        case H5VL_DATASET_GET_DCPL: {
            if ((args->args.get_dcpl.dcpl_id = H5D_get_create_plist(dset)) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get creation property list for dataset");

            break;
        }

        /* H5Dget_access_plist */
        case H5VL_DATASET_GET_DAPL: {
            if ((args->args.get_dapl.dapl_id = H5D_get_access_plist(dset)) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get access property list for dataset");

            break;
        }

        /* H5Dget_storage_size */
        case H5VL_DATASET_GET_STORAGE_SIZE: {
            if (H5D__get_storage_size(dset, args->args.get_storage_size.storage_size) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get size of dataset's storage");
            break;
        }

        default:
            HGOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't get this type of information from dataset");
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_dataset_get() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__native_dataset_specific
 *
 * Purpose:     Handles the dataset specific callback
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL__native_dataset_specific(void *obj, H5VL_dataset_specific_args_t *args, hid_t H5_ATTR_UNUSED dxpl_id,
                              void H5_ATTR_UNUSED **req)
{
    H5D_t *dset      = (H5D_t *)obj;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    switch (args->op_type) {
        /* H5Dset_extent (H5Dextend - deprecated) */
        case H5VL_DATASET_SET_EXTENT: {
            if (H5D__set_extent(dset, args->args.set_extent.size) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "unable to set extent of dataset");
            break;
        }

        /* H5Dflush */
        case H5VL_DATASET_FLUSH: {
            if (H5D__flush(dset, args->args.flush.dset_id) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTFLUSH, FAIL, "unable to flush dataset");

            break;
        }

        /* H5Drefresh */
        case H5VL_DATASET_REFRESH: {
            if (H5D__refresh(dset, args->args.refresh.dset_id) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTLOAD, FAIL, "unable to refresh dataset");

            break;
        }

        default:
            HGOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid specific operation");
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_dataset_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__native_dataset_optional
 *
 * Purpose:     Handles the dataset optional callback
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL__native_dataset_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void H5_ATTR_UNUSED **req)
{
    H5D_t                               *dset      = (H5D_t *)obj; /* Dataset */
    H5VL_native_dataset_optional_args_t *opt_args  = args->args; /* Pointer to native operation's arguments */
    herr_t                               ret_value = SUCCEED;    /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(dset);

    /* Set DXPL for operation */
    H5CX_set_dxpl(dxpl_id);

    switch (args->op_type) {
        /* H5Dformat_convert */
        case H5VL_NATIVE_DATASET_FORMAT_CONVERT: {
            switch (dset->shared->layout.type) {
                case H5D_CHUNKED:
                    /* Convert the chunk indexing type to version 1 B-tree if not */
                    if (dset->shared->layout.u.chunk.idx_type != H5D_CHUNK_IDX_BTREE)
                        if (H5D__format_convert(dset) < 0)
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTLOAD, FAIL,
                                        "unable to downgrade chunk indexing type for dataset");
                    break;

                case H5D_CONTIGUOUS:
                case H5D_COMPACT:
                    /* Downgrade the layout version to 3 if greater than 3 */
                    if (dset->shared->layout.version > H5O_LAYOUT_VERSION_DEFAULT)
                        if (H5D__format_convert(dset) < 0)
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTLOAD, FAIL,
                                        "unable to downgrade layout version for dataset");
                    break;

                case H5D_VIRTUAL:
                    /* Nothing to do even though layout is version 4 */
                    break;

                case H5D_LAYOUT_ERROR:
                case H5D_NLAYOUTS:
                    HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid dataset layout type");

                default:
                    HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "unknown dataset layout type");
            } /* end switch */

            break;
        }

        /* H5Dget_chunk_index_type */
        case H5VL_NATIVE_DATASET_GET_CHUNK_INDEX_TYPE: {
            /* Make sure the dataset is chunked */
            if (H5D_CHUNKED != dset->shared->layout.type)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a chunked dataset");

            /* Get the chunk indexing type */
            *opt_args->get_chunk_idx_type.idx_type = dset->shared->layout.u.chunk.idx_type;

            break;
        }

        /* H5Dget_chunk_storage_size */
        case H5VL_NATIVE_DATASET_GET_CHUNK_STORAGE_SIZE: {
            H5VL_native_dataset_get_chunk_storage_size_t *gcss_args = &opt_args->get_chunk_storage_size;

            /* Make sure the dataset is chunked */
            if (H5D_CHUNKED != dset->shared->layout.type)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a chunked dataset");

            /* Call private function */
            if (H5D__get_chunk_storage_size(dset, gcss_args->offset, gcss_args->size) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get storage size of chunk");

            break;
        }

        /* H5Dget_num_chunks */
        case H5VL_NATIVE_DATASET_GET_NUM_CHUNKS: {
            H5VL_native_dataset_get_num_chunks_t *gnc_args = &opt_args->get_num_chunks;
            const H5S_t                          *space    = NULL;

            assert(dset->shared);
            assert(dset->shared->space);

            /* When default dataspace is given, use the dataset's dataspace */
            if (gnc_args->space_id == H5S_ALL)
                space = dset->shared->space;
            else /*  otherwise, use the given space ID */
                if (NULL == (space = (const H5S_t *)H5I_object_verify(gnc_args->space_id, H5I_DATASPACE)))
                    HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a valid dataspace ID");

            /* Make sure the dataset is chunked */
            if (H5D_CHUNKED != dset->shared->layout.type)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a chunked dataset");

            /* Call private function */
            if (H5D__get_num_chunks(dset, space, gnc_args->nchunks) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of chunks");

            break;
        }

        /* H5Dget_chunk_info */
        case H5VL_NATIVE_DATASET_GET_CHUNK_INFO_BY_IDX: {
            H5VL_native_dataset_get_chunk_info_by_idx_t *gcibi_args = &opt_args->get_chunk_info_by_idx;
            const H5S_t                                 *space;

            assert(dset->shared);
            assert(dset->shared->space);

            /* When default dataspace is given, use the dataset's dataspace */
            if (gcibi_args->space_id == H5S_ALL)
                space = dset->shared->space;
            else /*  otherwise, use the given space ID */
                if (NULL == (space = (const H5S_t *)H5I_object_verify(gcibi_args->space_id, H5I_DATASPACE)))
                    HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a valid dataspace ID");

            /* Make sure the dataset is chunked */
            if (H5D_CHUNKED != dset->shared->layout.type)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a chunked dataset");

            /* Call private function */
            if (H5D__get_chunk_info(dset, space, gcibi_args->chk_index, gcibi_args->offset,
                                    gcibi_args->filter_mask, gcibi_args->addr, gcibi_args->size) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get chunk info by index");

            break;
        }

        /* H5Dget_chunk_info_by_coord */
        case H5VL_NATIVE_DATASET_GET_CHUNK_INFO_BY_COORD: {
            H5VL_native_dataset_get_chunk_info_by_coord_t *gcibc_args = &opt_args->get_chunk_info_by_coord;

            assert(dset->shared);

            /* Make sure the dataset is chunked */
            if (H5D_CHUNKED != dset->shared->layout.type)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a chunked dataset");

            /* Call private function */
            if (H5D__get_chunk_info_by_coord(dset, gcibc_args->offset, gcibc_args->filter_mask,
                                             gcibc_args->addr, gcibc_args->size) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL,
                            "can't get chunk info by its logical coordinates");

            break;
        }

        /* H5Dread_chunk */
        case H5VL_NATIVE_DATASET_CHUNK_READ: {
            H5VL_native_dataset_chunk_read_t *chunk_read_args = &opt_args->chunk_read;
            hsize_t offset_copy[H5O_LAYOUT_NDIMS]; /* Internal copy of chunk offset */

            /* Check arguments */
            if (NULL == dset->oloc.file)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "dataset is not associated with a file");
            if (H5D_CHUNKED != dset->shared->layout.type)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a chunked dataset");

            /* Copy the user's offset array so we can be sure it's terminated properly.
             * (we don't want to mess with the user's buffer).
             */
            if (H5D__chunk_get_offset_copy(dset, chunk_read_args->offset, offset_copy) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "failure to copy offset array");

            /* Read the raw chunk */
            if (H5D__chunk_direct_read(dset, offset_copy, &chunk_read_args->filters, chunk_read_args->buf) <
                0)
                HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "can't read unprocessed chunk data");

            break;
        }

        /* H5Dwrite_chunk */
        case H5VL_NATIVE_DATASET_CHUNK_WRITE: {
            H5VL_native_dataset_chunk_write_t *chunk_write_args = &opt_args->chunk_write;
            hsize_t offset_copy[H5O_LAYOUT_NDIMS]; /* Internal copy of chunk offset */

            /* Check arguments */
            if (NULL == dset->oloc.file)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "dataset is not associated with a file");
            if (H5D_CHUNKED != dset->shared->layout.type)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a chunked dataset");

            /* Copy the user's offset array so we can be sure it's terminated properly.
             * (we don't want to mess with the user's buffer).
             */
            if (H5D__chunk_get_offset_copy(dset, chunk_write_args->offset, offset_copy) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "failure to copy offset array");

            /* Write chunk */
            if (H5D__chunk_direct_write(dset, chunk_write_args->filters, offset_copy, chunk_write_args->size,
                                        chunk_write_args->buf) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "can't write unprocessed chunk data");

            break;
        }

        /* H5Dvlen_get_buf_size */
        case H5VL_NATIVE_DATASET_GET_VLEN_BUF_SIZE: {
            H5VL_native_dataset_get_vlen_buf_size_t *gvbs_args = &opt_args->get_vlen_buf_size;

            if (H5D__vlen_get_buf_size(dset, gvbs_args->type_id, gvbs_args->space_id, gvbs_args->size) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get size of vlen buf needed");
            break;
        }

        /* H5Dget_offset */
        case H5VL_NATIVE_DATASET_GET_OFFSET: {
            /* Get offset */
            *opt_args->get_offset.offset = H5D__get_offset(dset);

            break;
        }

        /* H5Dchunk_iter */
        case H5VL_NATIVE_DATASET_CHUNK_ITER: {
            /* Sanity check */
            assert(dset->shared);

            /* Make sure the dataset is chunked */
            if (H5D_CHUNKED != dset->shared->layout.type)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a chunked dataset");

            /* Call private function */
            if ((ret_value = H5D__chunk_iter(dset, opt_args->chunk_iter.op, opt_args->chunk_iter.op_data)) <
                0)
                HERROR(H5E_DATASET, H5E_BADITER, "chunk iteration failed");

            break;
        }

        default:
            HGOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid optional operation");
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_dataset_optional() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__native_dataset_close
 *
 * Purpose:     Handles the dataset close callback
 *
 * Return:      Success:    SUCCEED
 *              Failure:    FAIL (dataset will not be closed)
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL__native_dataset_close(void *dset, hid_t H5_ATTR_UNUSED dxpl_id, void H5_ATTR_UNUSED **req)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    if (H5D_close((H5D_t *)dset) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "can't close dataset");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_dataset_close() */
