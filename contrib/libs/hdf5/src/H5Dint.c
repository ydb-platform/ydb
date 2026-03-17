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

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5CXprivate.h" /* API Contexts                             */
#include "H5Dpkg.h"      /* Datasets                                 */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5Fprivate.h"  /* Files                                    */
#include "H5FLprivate.h" /* Free Lists                               */
#include "H5FOprivate.h" /* File objects                             */
#include "H5Iprivate.h"  /* IDs                                      */
#include "H5Lprivate.h"  /* Links                                    */
#include "H5MMprivate.h" /* Memory management                        */
#include "H5VLprivate.h" /* Virtual Object Layer                     */
#include "H5VMprivate.h" /* Vector Functions                         */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/* Shared data structure for computing variable-length dataset's total size */
/* (Used for both native and generic 'get vlen buf size' operation) */
typedef struct {
    void   *fl_tbuf;      /* Ptr to the temporary buffer we are using for fixed-length data */
    void   *vl_tbuf;      /* Ptr to the temporary buffer we are using for VL data */
    size_t  vl_tbuf_size; /* Current size of the temp. buffer for VL data */
    hsize_t size;         /* Accumulated number of bytes for the selection */
} H5D_vlen_bufsize_common_t;

/* Internal data structure for computing variable-length dataset's total size */
/* (Used for native 'get vlen buf size' operation) */
typedef struct {
    H5D_t                    *dset;   /* Dataset for operation */
    H5S_t                    *fspace; /* Dataset's dataspace for operation */
    H5S_t                    *mspace; /* Memory dataspace for operation */
    H5D_vlen_bufsize_common_t common; /* VL data buffers & accumulatd size */
} H5D_vlen_bufsize_native_t;

/* Internal data structure for computing variable-length dataset's total size */
/* (Used for generic 'get vlen buf size' operation) */
typedef struct {
    const H5VL_object_t      *dset_vol_obj; /* VOL object for the dataset */
    hid_t                     fspace_id;    /* Dataset dataspace ID of the dataset we are working on */
    H5S_t                    *fspace;       /* Dataset's dataspace for operation */
    hid_t                     mspace_id;    /* Memory dataspace ID of the dataset we are working on */
    hid_t                     dxpl_id;      /* Dataset transfer property list to pass to dataset read */
    H5D_vlen_bufsize_common_t common;       /* VL data buffers & accumulatd size */
} H5D_vlen_bufsize_generic_t;

/********************/
/* Local Prototypes */
/********************/

/* General stuff */
static H5D_shared_t *H5D__new(hid_t dcpl_id, hid_t dapl_id, bool creating, bool vl_type);
static herr_t        H5D__init_type(H5F_t *file, const H5D_t *dset, hid_t type_id, H5T_t *type);
static herr_t        H5D__cache_dataspace_info(const H5D_t *dset);
static herr_t        H5D__init_space(H5F_t *file, const H5D_t *dset, const H5S_t *space);
static herr_t        H5D__update_oh_info(H5F_t *file, H5D_t *dset, hid_t dapl_id);
static herr_t H5D__build_file_prefix(const H5D_t *dset, H5F_prefix_open_t prefix_type, char **file_prefix);
static herr_t H5D__open_oid(H5D_t *dataset, hid_t dapl_id);
static herr_t H5D__init_storage(H5D_t *dset, bool full_overwrite, hsize_t old_dim[]);
static herr_t H5D__append_flush_setup(H5D_t *dset, hid_t dapl_id);
static herr_t H5D__close_cb(H5VL_object_t *dset_vol_obj, void **request);
static herr_t H5D__use_minimized_dset_headers(H5F_t *file, bool *minimize);
static herr_t H5D__prepare_minimized_oh(H5F_t *file, H5D_t *dset, H5O_loc_t *oloc);
static size_t H5D__calculate_minimum_header_size(H5F_t *file, H5D_t *dset, H5O_t *ohdr);
static void  *H5D__vlen_get_buf_size_alloc(size_t size, void *info);
static herr_t H5D__vlen_get_buf_size_cb(void *elem, hid_t type_id, unsigned ndim, const hsize_t *point,
                                        void *op_data);
static herr_t H5D__vlen_get_buf_size_gen_cb(void *elem, hid_t type_id, unsigned ndim, const hsize_t *point,
                                            void *op_data);
static herr_t H5D__check_filters(H5D_t *dataset);

/*********************/
/* Package Variables */
/*********************/

/* Declare a free list to manage blocks of VL data */
H5FL_BLK_DEFINE(vlen_vl_buf);

/* Declare a free list to manage other blocks of VL data */
H5FL_BLK_DEFINE(vlen_fl_buf);

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/* Declare a free list to manage the H5D_t and H5D_shared_t structs */
H5FL_DEFINE_STATIC(H5D_t);
H5FL_DEFINE_STATIC(H5D_shared_t);

/* Declare the external PQ free list for the sieve buffer information */
H5FL_BLK_EXTERN(sieve_buf);

/* Declare the external free list to manage the H5D_piece_info_t struct */
H5FL_EXTERN(H5D_piece_info_t);

/* Declare extern the free list to manage blocks of type conversion data */
H5FL_BLK_EXTERN(type_conv);

/* Disable warning for intentional identical branches here -QAK */
H5_GCC_DIAG_OFF("larger-than=")
/* Define a static "default" dataset structure to use to initialize new datasets */
static H5D_shared_t H5D_def_dset;
H5_GCC_DIAG_ON("larger-than=")

/* Dataset ID class */
static const H5I_class_t H5I_DATASET_CLS[1] = {{
    H5I_DATASET,              /* ID class value */
    0,                        /* Class flags */
    0,                        /* # of reserved IDs for class */
    (H5I_free_t)H5D__close_cb /* Callback routine for closing objects of this class */
}};

/* Prefixes of VDS and external file from the environment variables
 * HDF5_EXTFILE_PREFIX and HDF5_VDS_PREFIX */
static const char *H5D_prefix_ext_env = NULL;
static const char *H5D_prefix_vds_env = NULL;

/*-------------------------------------------------------------------------
 * Function: H5D_init
 *
 * Purpose:  Initialize the interface from some other layer.
 *
 * Return:   Success:    non-negative
 *
 *           Failure:    negative
 *-------------------------------------------------------------------------
 */
herr_t
H5D_init(void)
{
    H5P_genplist_t *def_dcpl;            /* Default Dataset Creation Property list */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Initialize the ID group for the dataset IDs */
    if (H5I_register_type(H5I_DATASET_CLS) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to initialize interface");

    /* Reset the "default dataset" information */
    memset(&H5D_def_dset, 0, sizeof(H5D_shared_t));
    H5D_def_dset.type_id = H5I_INVALID_HID;
    H5D_def_dset.dapl_id = H5I_INVALID_HID;
    H5D_def_dset.dcpl_id = H5I_INVALID_HID;

    /* Get the default dataset creation property list values and initialize the
     * default dataset with them.
     */
    if (NULL == (def_dcpl = (H5P_genplist_t *)H5I_object(H5P_LST_DATASET_CREATE_ID_g)))
        HGOTO_ERROR(H5E_DATASET, H5E_BADTYPE, FAIL, "can't get default dataset creation property list");

    /* Get the default data storage layout */
    if (H5P_get(def_dcpl, H5D_CRT_LAYOUT_NAME, &H5D_def_dset.layout) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't retrieve layout");

    /* Get the default dataset creation properties */
    if (H5P_get(def_dcpl, H5D_CRT_EXT_FILE_LIST_NAME, &H5D_def_dset.dcpl_cache.efl) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't retrieve external file list");
    if (H5P_get(def_dcpl, H5D_CRT_FILL_VALUE_NAME, &H5D_def_dset.dcpl_cache.fill) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't retrieve fill value");
    if (H5P_get(def_dcpl, H5O_CRT_PIPELINE_NAME, &H5D_def_dset.dcpl_cache.pline) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't retrieve pipeline filter");

    /* Retrieve the prefixes of VDS and external file from the environment variable */
    H5D_prefix_vds_env = getenv("HDF5_VDS_PREFIX");
    H5D_prefix_ext_env = getenv("HDF5_EXTFILE_PREFIX");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D_init() */

/*-------------------------------------------------------------------------
 * Function: H5D_top_term_package
 *
 * Purpose:  Close the "top" of the interface, releasing IDs, etc.
 *
 * Return:   Success:    Positive if anything was done that might
 *                affect other interfaces; zero otherwise.
 *           Failure:    Negative.
 *-------------------------------------------------------------------------
 */
int
H5D_top_term_package(void)
{
    int n = 0;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    if (H5I_nmembers(H5I_DATASET) > 0) {
        /* The dataset API uses the "force" flag set to true because it
         * is using the "file objects" (H5FO) API functions to track open
         * objects in the file.  Using the H5FO code means that dataset
         * IDs can have reference counts >1, when an existing dataset is
         * opened more than once.  However, the H5I code does not attempt
         * to close objects with reference counts>1 unless the "force" flag
         * is set to true.
         *
         * At some point (probably after the group and datatypes use the
         * the H5FO code), the H5FO code might need to be switched around
         * to storing pointers to the objects being tracked (H5D_t, H5G_t,
         * etc) and reference count those itself instead of relying on the
         * reference counting in the H5I layer.  Then, the "force" flag can
         * be put back to false.
         *
         * Setting the "force" flag to true for all the interfaces won't
         * work because the "file driver" (H5FD) APIs use the H5I reference
         * counting to avoid closing a file driver out from underneath an
         * open file...
         *
         * QAK - 5/13/03
         */
        (void)H5I_clear_type(H5I_DATASET, true, false);
        n++; /*H5I*/
    }

    FUNC_LEAVE_NOAPI(n)
} /* end H5D_top_term_package() */

/*-------------------------------------------------------------------------
 * Function: H5D_term_package
 *
 * Purpose:  Terminate this interface.
 *
 * Note:     Finishes shutting down the interface, after
 *           H5D_top_term_package() is called
 *
 * Return:   Success:    Positive if anything was done that might
 *                affect other interfaces; zero otherwise.
 *            Failure:    Negative.
 *-------------------------------------------------------------------------
 */
int
H5D_term_package(void)
{
    int n = 0;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity checks */
    assert(0 == H5I_nmembers(H5I_DATASET));

    /* Destroy the dataset object id group */
    n += (H5I_dec_type_ref(H5I_DATASET) > 0);

    FUNC_LEAVE_NOAPI(n)
} /* end H5D_term_package() */

/*-------------------------------------------------------------------------
 * Function:    H5D__close_cb
 *
 * Purpose:     Called when the ref count reaches zero on the dataset's ID
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__close_cb(H5VL_object_t *dset_vol_obj, void **request)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(dset_vol_obj);

    /* Close the dataset */
    if (H5VL_dataset_close(dset_vol_obj, H5P_DATASET_XFER_DEFAULT, request) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "unable to close dataset");

done:
    /* Free the VOL object */
    if (H5VL_free_object(dset_vol_obj) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "unable to free VOL object");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__close_cb() */

/*-------------------------------------------------------------------------
 * Function: H5D__create_named
 *
 * Purpose:  Internal routine to create a new dataset.
 *
 * Return:   Success:    Non-NULL, pointer to new dataset object.
 *
 *           Failure:    NULL
 *-------------------------------------------------------------------------
 */
H5D_t *
H5D__create_named(const H5G_loc_t *loc, const char *name, hid_t type_id, const H5S_t *space, hid_t lcpl_id,
                  hid_t dcpl_id, hid_t dapl_id)
{
    H5O_obj_create_t ocrt_info;        /* Information for object creation */
    H5D_obj_create_t dcrt_info;        /* Information for dataset creation */
    H5D_t           *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(loc);
    assert(name && *name);
    assert(type_id != H5P_DEFAULT);
    assert(space);
    assert(lcpl_id != H5P_DEFAULT);
    assert(dcpl_id != H5P_DEFAULT);
    assert(dapl_id != H5P_DEFAULT);

    /* Set up dataset creation info */
    dcrt_info.type_id = type_id;
    dcrt_info.space   = space;
    dcrt_info.dcpl_id = dcpl_id;
    dcrt_info.dapl_id = dapl_id;

    /* Set up object creation information */
    ocrt_info.obj_type = H5O_TYPE_DATASET;
    ocrt_info.crt_info = &dcrt_info;
    ocrt_info.new_obj  = NULL;

    /* Create the new dataset and link it to its parent group */
    if (H5L_link_object(loc, name, &ocrt_info, lcpl_id) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "unable to create and link to dataset");
    assert(ocrt_info.new_obj);

    /* Set the return value */
    ret_value = (H5D_t *)ocrt_info.new_obj;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__create_named() */

/*-------------------------------------------------------------------------
 * Function:    H5D__get_space_status
 *
 * Purpose:     Returns the status of dataspace allocation.
 *
 * Return:
 *              Success:        Non-negative
 *              Failure:       Negative
 *-------------------------------------------------------------------------
 */
herr_t
H5D__get_space_status(const H5D_t *dset, H5D_space_status_t *allocation)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(dset);

    /* Check for chunked layout */
    if (dset->shared->layout.type == H5D_CHUNKED) {
        hsize_t n_chunks_total = dset->shared->layout.u.chunk.nchunks;
        hsize_t n_chunks_alloc = 0;

        if (H5D__get_num_chunks(dset, dset->shared->space, &n_chunks_alloc) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL,
                        "unable to retrieve number of allocated chunks in dataset");

        assert(n_chunks_alloc <= n_chunks_total);

        if (n_chunks_alloc == 0)
            *allocation = H5D_SPACE_STATUS_NOT_ALLOCATED;
        else if (n_chunks_alloc == n_chunks_total)
            *allocation = H5D_SPACE_STATUS_ALLOCATED;
        else
            *allocation = H5D_SPACE_STATUS_PART_ALLOCATED;
    } /* end if */
    else {
        /* For non-chunked layouts set space status by result of is_space_alloc
         * function */
        if (dset->shared->layout.ops->is_space_alloc(&dset->shared->layout.storage))
            *allocation = H5D_SPACE_STATUS_ALLOCATED;
        else
            *allocation = H5D_SPACE_STATUS_NOT_ALLOCATED;
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__get_space_status() */

/*-------------------------------------------------------------------------
 * Function: H5D__new
 *
 * Purpose:  Creates a new, empty dataset structure
 *
 * Return:   Success:    Pointer to a new dataset descriptor.
 *           Failure:    NULL
 *-------------------------------------------------------------------------
 */
static H5D_shared_t *
H5D__new(hid_t dcpl_id, hid_t dapl_id, bool creating, bool vl_type)
{
    H5D_shared_t   *new_dset = NULL;  /* New dataset object */
    H5P_genplist_t *plist;            /* Property list created */
    H5D_shared_t   *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Allocate new shared dataset structure */
    if (NULL == (new_dset = H5FL_MALLOC(H5D_shared_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Copy the default dataset information */
    H5MM_memcpy(new_dset, &H5D_def_dset, sizeof(H5D_shared_t));

    /* If we are using the default dataset creation property list, during creation
     * don't bother to copy it, just increment the reference count
     */
    if (!vl_type && creating && dcpl_id == H5P_DATASET_CREATE_DEFAULT) {
        if (H5I_inc_ref(dcpl_id, false) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINC, NULL, "can't increment default DCPL ID");
        new_dset->dcpl_id = dcpl_id;
    } /* end if */
    else {
        /* Get the property list */
        if (NULL == (plist = (H5P_genplist_t *)H5I_object(dcpl_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a property list");

        new_dset->dcpl_id = H5P_copy_plist(plist, false);
    } /* end else */

    if (!vl_type && creating && dapl_id == H5P_DATASET_ACCESS_DEFAULT) {
        if (H5I_inc_ref(dapl_id, false) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINC, NULL, "can't increment default DAPL ID");
        new_dset->dapl_id = dapl_id;
    } /* end if */
    else {
        /* Get the property list */
        if (NULL == (plist = (H5P_genplist_t *)H5I_object(dapl_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a property list");

        new_dset->dapl_id = H5P_copy_plist(plist, false);
    } /* end else */

    /* Set return value */
    ret_value = new_dset;

done:
    if (ret_value == NULL)
        if (new_dset != NULL) {
            if (new_dset->dcpl_id != 0 && H5I_dec_ref(new_dset->dcpl_id) < 0)
                HDONE_ERROR(H5E_DATASET, H5E_CANTDEC, NULL, "can't decrement temporary datatype ID");
            if (new_dset->dapl_id != 0 && H5I_dec_ref(new_dset->dapl_id) < 0)
                HDONE_ERROR(H5E_DATASET, H5E_CANTDEC, NULL, "can't decrement temporary datatype ID");
            new_dset = H5FL_FREE(H5D_shared_t, new_dset);
        } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__new() */

/*-------------------------------------------------------------------------
 * Function: H5D__init_type
 *
 * Purpose:  Copy a datatype for a dataset's use, performing all the
 *              necessary adjustments, etc.
 *
 * Return:   Success:    SUCCEED
 *           Failure:    FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__init_type(H5F_t *file, const H5D_t *dset, hid_t type_id, H5T_t *type)
{
    htri_t relocatable;         /* Flag whether the type is relocatable */
    htri_t immutable;           /* Flag whether the type is immutable */
    bool   use_at_least_v18;    /* Flag indicating to use at least v18 format versions */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checking */
    assert(file);
    assert(dset);
    assert(type);

    /* Check whether the datatype is relocatable */
    if ((relocatable = H5T_is_relocatable(type)) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't check datatype?");

    /* Check whether the datatype is immutable */
    if ((immutable = H5T_is_immutable(type)) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't check datatype?");

    /* To use at least v18 format versions or not */
    use_at_least_v18 = (H5F_LOW_BOUND(file) >= H5F_LIBVER_V18);

    /* Copy the datatype if it's a custom datatype or if it'll change when its location is changed */
    if (!immutable || relocatable || use_at_least_v18) {
        /* Copy datatype for dataset */
        if ((dset->shared->type = H5T_copy(type, H5T_COPY_ALL)) == NULL)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "can't copy datatype");

        /* Convert a datatype (if committed) to a transient type if the committed datatype's file
         * location is different from the file location where the dataset will be created.
         */
        if (H5T_convert_committed_datatype(dset->shared->type, file) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't get shared datatype info");

        /* Mark any datatypes as being on disk now */
        if (H5T_set_loc(dset->shared->type, H5F_VOL_OBJ(file), H5T_LOC_DISK) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't set datatype location");

        /* Set the version for datatype */
        if (H5T_set_version(file, dset->shared->type) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "can't set version of datatype");

        /* Get a datatype ID for the dataset's datatype */
        if ((dset->shared->type_id = H5I_register(H5I_DATATYPE, dset->shared->type, false)) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTREGISTER, FAIL, "unable to register type");
    } /* end if */
    /* Not a custom datatype, just use it directly */
    else {
        if (H5I_inc_ref(type_id, false) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINC, FAIL, "Can't increment datatype ID");

        /* Use existing datatype */
        dset->shared->type_id = type_id;
        dset->shared->type    = type;
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__init_type() */

/*-------------------------------------------------------------------------
 * Function: H5D__cache_dataspace_info
 *
 * Purpose:  Cache dataspace info for a dataset
 *
 * Return:   Success:    SUCCEED
 *           Failure:    FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__cache_dataspace_info(const H5D_t *dset)
{
    int      sndims;              /* Signed number of dimensions of dataspace rank */
    unsigned u;                   /* Local index value */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checking */
    assert(dset);

    /* Cache info for dataset's dataspace */
    if ((sndims = H5S_get_simple_extent_dims(dset->shared->space, dset->shared->curr_dims,
                                             dset->shared->max_dims)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't cache dataspace dimensions");
    dset->shared->ndims = (unsigned)sndims;

    /* Compute the initial 'power2up' values */
    for (u = 0; u < dset->shared->ndims; u++) {
        hsize_t scaled_power2up; /* Scaled value, rounded to next power of 2 */

        if (!(scaled_power2up = H5VM_power2up(dset->shared->curr_dims[u])))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "unable to get the next power of 2");
        dset->shared->curr_power2up[u] = scaled_power2up;
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__cache_dataspace_info() */

/*-------------------------------------------------------------------------
 * Function: H5D__init_space
 *
 * Purpose:  Copy a dataspace for a dataset's use, performing all the
 *              necessary adjustments, etc.
 *
 * Return:   Success:    SUCCEED
 *           Failure:    FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__init_space(H5F_t *file, const H5D_t *dset, const H5S_t *space)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checking */
    assert(file);
    assert(dset);
    assert(space);

    /* Copy dataspace for dataset */
    if (NULL == (dset->shared->space = H5S_copy(space, false, true)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "can't copy dataspace");

    /* Cache the dataset's dataspace info */
    if (H5D__cache_dataspace_info(dset) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "can't cache dataspace info");

    /* Set the version for dataspace */
    if (H5S_set_version(file, dset->shared->space) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "can't set latest version of datatype");

    /* Set the dataset's dataspace to 'all' selection */
    if (H5S_select_all(dset->shared->space, true) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "unable to set all selection");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__init_space() */

/*-------------------------------------------------------------------------
 * Function:   H5D__use_minimized_dset_headers
 *
 * Purpose:    Compartmentalize check for file- or dcpl-set values indicating
 *             to create a "minimized" dataset object header.
 *             Upon success, write resulting value to out pointer `minimize`.
 *
 * Return:     Success: SUCCEED (0) (non-negative value)
 *             Failure: FAIL (-1) (negative value)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__use_minimized_dset_headers(H5F_t *file, bool *minimize)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(file);
    assert(minimize);

    /* Get the dataset object header minimize flag for this call */
    if (H5CX_get_dset_min_ohdr_flag(minimize) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL,
                    "can't get dataset object header minimize flag from API context");

    if (false == *minimize)
        *minimize = H5F_get_min_dset_ohdr(file);

done:
    if (FAIL == ret_value)
        *minimize = false;
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__use_minimized_dset_headers */

/*-------------------------------------------------------------------------
 * Function:   H5D__calculate_minimium_header_size
 *
 * Purpose:    Calculate the size required for the minimized object header.
 *
 * Return:     Success: Positive value > 0
 *             Failure: 0
 *
 *-------------------------------------------------------------------------
 */
static size_t
H5D__calculate_minimum_header_size(H5F_t *file, H5D_t *dset, H5O_t *ohdr)
{
    H5T_t      *type             = NULL;
    H5O_fill_t *fill_prop        = NULL;
    bool        use_at_least_v18 = false;
    const char  continuation[1]  = ""; /* required for work-around */
    size_t      get_value        = 0;
    size_t      ret_value        = 0;

    FUNC_ENTER_PACKAGE

    assert(file);
    assert(dset);
    assert(ohdr);

    type             = dset->shared->type;
    fill_prop        = &(dset->shared->dcpl_cache.fill);
    use_at_least_v18 = (H5F_LOW_BOUND(file) >= H5F_LIBVER_V18);

    /* Datatype message size */
    get_value = H5O_msg_size_oh(file, ohdr, H5O_DTYPE_ID, type, 0);
    if (get_value == 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, 0, "Can't get size of datatype message");
    ret_value += get_value;

    /* Shared Dataspace message size */
    get_value = H5O_msg_size_oh(file, ohdr, H5O_SDSPACE_ID, dset->shared->space, 0);
    if (get_value == 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, 0, "can't get size of dataspace message");
    ret_value += get_value;

    /* "Layout" message size */
    get_value = H5O_msg_size_oh(file, ohdr, H5O_LAYOUT_ID, &dset->shared->layout, 0);
    if (get_value == 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, 0, "can't get size of layout message");
    ret_value += get_value;

    /* Fill Value message size */
    get_value = H5O_msg_size_oh(file, ohdr, H5O_FILL_NEW_ID, fill_prop, 0);
    if (get_value == 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, 0, "can't get size of fill value message");
    ret_value += get_value;

    /* "Continuation" message size */
    /* message pointer "continuation" is unused by raw get function, however,
     * a null pointer would be intercepted by an assert in H5O_msg_size_oh().
     */
    get_value = H5O_msg_size_oh(file, ohdr, H5O_CONT_ID, continuation, 0);
    if (get_value == 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, 0, "can't get size of continuation message");
    ret_value += get_value;

    /* Fill Value (backwards compatibility) message size */
    if (fill_prop->buf && !use_at_least_v18) {
        H5O_fill_t old_fill_prop; /* Copy for writing "old" fill value */

        /* Shallow copy the fill value property */
        /* guards against shared component modification */
        H5MM_memcpy(&old_fill_prop, fill_prop, sizeof(old_fill_prop));

        if (H5O_msg_reset_share(H5O_FILL_ID, &old_fill_prop) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, 0, "can't reset the copied fill property");

        get_value = H5O_msg_size_oh(file, ohdr, H5O_FILL_ID, &old_fill_prop, 0);
        if (get_value == 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, 0,
                        "can't get size of fill value (backwards compat) message");
        ret_value += get_value;
    }

    /* Filter/Pipeline message size */
    if (H5D_CHUNKED == dset->shared->layout.type) {
        H5O_pline_t *pline = &dset->shared->dcpl_cache.pline;
        if (pline->nused > 0) {
            get_value = H5O_msg_size_oh(file, ohdr, H5O_PLINE_ID, pline, 0);
            if (get_value == 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, 0, "can't get size of filter message");
            ret_value += get_value;
        }
    }

    /* External File Link message size */
    if (dset->shared->dcpl_cache.efl.nused > 0) {
        get_value = H5O_msg_size_oh(file, ohdr, H5O_EFL_ID, &dset->shared->dcpl_cache.efl, 0);
        if (get_value == 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, 0, "can't get size of external file link message");
        ret_value += get_value;
    }

    /* Modification Time message size */
    if (H5O_HDR_STORE_TIMES & H5O_OH_GET_FLAGS(ohdr)) {
        assert(H5O_OH_GET_VERSION(ohdr) >= 1); /* 1 :: H5O_VERSION_1 (H5Opkg.h) */

        if (H5O_OH_GET_VERSION(ohdr) == 1) {
            /* v1 object headers store modification time as a message */
            time_t mtime;
            get_value = H5O_msg_size_oh(file, ohdr, H5O_MTIME_NEW_ID, &mtime, 0);
            if (get_value == 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, 0, "can't get size of modification time message");
            ret_value += get_value;
        }
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__calculate_minimum_header_size */

/*-------------------------------------------------------------------------
 * Function:   H5D__prepare_minimized_oh
 *
 * Purpose:    Create an object header (H5O_t) allocated with the smallest
 *             possible size.
 *
 * Return:     Success: SUCCEED (0) (non-negative value)
 *             Failure: FAIL (-1) (negative value)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__prepare_minimized_oh(H5F_t *file, H5D_t *dset, H5O_loc_t *oloc)
{
    H5O_t *oh        = NULL;
    size_t ohdr_size = 0;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(file);
    assert(dset);
    assert(oloc);

    oh = H5O_create_ohdr(file, dset->shared->dcpl_id);
    if (NULL == oh)
        HGOTO_ERROR(H5E_OHDR, H5E_BADVALUE, FAIL, "can't instantiate object header");

    ohdr_size = H5D__calculate_minimum_header_size(file, dset, oh);
    if (ohdr_size == 0)
        HGOTO_ERROR(H5E_OHDR, H5E_BADVALUE, FAIL, "computed header size is invalid");

    /* Special allocation of space for compact datasets is handled by the call here. */
    if (H5O_apply_ohdr(file, oh, dset->shared->dcpl_id, ohdr_size, (size_t)1, oloc) == FAIL)
        HGOTO_ERROR(H5E_OHDR, H5E_BADVALUE, FAIL, "can't apply object header to file");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__prepare_minimized_oh */

/*-------------------------------------------------------------------------
 * Function: H5D__update_oh_info
 *
 * Purpose:  Create and fill object header for dataset
 *
 * Return:   Success:    SUCCEED
 *           Failure:    FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__update_oh_info(H5F_t *file, H5D_t *dset, hid_t dapl_id)
{
    H5O_t           *oh        = NULL;            /* Pointer to dataset's object header */
    size_t           ohdr_size = H5D_MINHDR_SIZE; /* Size of dataset's object header */
    H5O_loc_t       *oloc      = NULL;            /* Dataset's object location */
    H5O_layout_t    *layout;                      /* Dataset's layout information */
    H5T_t           *type;                        /* Dataset's datatype */
    H5O_fill_t      *fill_prop;                   /* Pointer to dataset's fill value information */
    H5D_fill_value_t fill_status;                 /* Fill value status */
    bool             fill_changed = false;        /* Flag indicating the fill value was changed */
    bool             layout_init  = false; /* Flag to indicate that chunk information was initialized */
    bool             use_at_least_v18;     /* Flag indicating to use at least v18 format versions */
    bool             use_minimized_header = false;   /* Flag to use minimized dataset object headers */
    herr_t           ret_value            = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checking */
    assert(file);
    assert(dset);

    /* Set some local variables, for convenience */
    oloc      = &dset->oloc;
    layout    = &dset->shared->layout;
    type      = dset->shared->type;
    fill_prop = &dset->shared->dcpl_cache.fill;

    /* To use at least v18 format versions or not */
    use_at_least_v18 = (H5F_LOW_BOUND(file) >= H5F_LIBVER_V18);

    /* Retrieve "defined" status of fill value */
    if (H5P_is_fill_value_defined(fill_prop, &fill_status) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't tell if fill value defined");

    /* Special case handling for variable-length types */
    if (H5T_detect_class(type, H5T_VLEN, false)) {
        /* If the default fill value is chosen for variable-length types, always write it */
        if (fill_prop->fill_time == H5D_FILL_TIME_IFSET && fill_status == H5D_FILL_VALUE_DEFAULT) {
            /* Update dataset creation property */
            fill_prop->fill_time = H5D_FILL_TIME_ALLOC;

            /* Note that the fill value changed */
            fill_changed = true;
        } /* end if */

        /* Don't allow never writing fill values with variable-length types */
        if (fill_prop->fill_time == H5D_FILL_TIME_NEVER)
            HGOTO_ERROR(H5E_DATASET, H5E_UNSUPPORTED, FAIL,
                        "Dataset doesn't support VL datatype when fill value is not defined");
    } /* end if */

    /* Determine whether fill value is defined or not */
    if (fill_status == H5D_FILL_VALUE_DEFAULT || fill_status == H5D_FILL_VALUE_USER_DEFINED) {
        /* Convert fill value buffer to dataset's datatype */
        if (fill_prop->buf && fill_prop->size > 0 && H5O_fill_convert(fill_prop, type, &fill_changed) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to convert fill value to dataset type");

        fill_prop->fill_defined = true;
    }
    else if (fill_status == H5D_FILL_VALUE_UNDEFINED)
        fill_prop->fill_defined = false;
    else
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "unable to determine if fill value is defined");

    /* Check for invalid fill & allocation time setting */
    if (fill_prop->fill_defined == false && fill_prop->fill_time == H5D_FILL_TIME_ALLOC)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL,
                    "fill value writing on allocation set, but no fill value defined");

    /* Check if the fill value info changed */
    if (fill_changed) {
        H5P_genplist_t *dc_plist; /* Dataset's creation property list */

        /* Get dataset's property list object */
        assert(dset->shared->dcpl_id != H5P_DATASET_CREATE_DEFAULT);
        if (NULL == (dc_plist = (H5P_genplist_t *)H5I_object(dset->shared->dcpl_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "can't get dataset creation property list");

        /* Update dataset creation property */
        if (H5P_set(dc_plist, H5D_CRT_FILL_VALUE_NAME, fill_prop) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set fill value info");
    } /* end if */

    if (H5D__use_minimized_dset_headers(file, &use_minimized_header) == FAIL)
        HGOTO_ERROR(H5E_ARGS, H5E_CANTGET, FAIL, "can't get minimize settings");

    if (true == use_minimized_header) {
        if (H5D__prepare_minimized_oh(file, dset, oloc) == FAIL)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create minimized dataset object header");
    } /* end if */
    else {
        /* Add the dataset's raw data size to the size of the header, if the
         * raw data will be stored as compact
         */
        if (H5D_COMPACT == layout->type)
            ohdr_size += layout->storage.u.compact.size;

        /* Create an object header for the dataset */
        if (H5O_create(file, ohdr_size, (size_t)1, dset->shared->dcpl_id, oloc /*out*/) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to create dataset object header");
    } /* if using default/minimized object headers */

    assert(file == dset->oloc.file);

    /* Pin the object header */
    if (NULL == (oh = H5O_pin(oloc)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTPIN, FAIL, "unable to pin dataset object header");

    /* Write the dataspace header message */
    if (H5S_append(file, oh, dset->shared->space) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to update dataspace header message");

    /* Write the datatype header message */
    if (H5O_msg_append_oh(file, oh, H5O_DTYPE_ID, H5O_MSG_FLAG_CONSTANT, 0, type) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to update datatype header message");

    /* Write new fill value message */
    if (H5O_msg_append_oh(file, oh, H5O_FILL_NEW_ID, H5O_MSG_FLAG_CONSTANT, 0, fill_prop) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to update new fill value header message");

    /* If there is valid information for the old fill value struct, add it */
    /* (only if we aren't using v18 format versions and above */
    if (fill_prop->buf && !use_at_least_v18) {
        H5O_fill_t old_fill_prop; /* Copy of fill value property, for writing as "old" fill value */

        /* Shallow copy the fill value property */
        /* (we only want to make certain that the shared component isn't modified) */
        H5MM_memcpy(&old_fill_prop, fill_prop, sizeof(old_fill_prop));

        /* Reset shared component info */
        H5O_msg_reset_share(H5O_FILL_ID, &old_fill_prop);

        /* Write old fill value */
        if (H5O_msg_append_oh(file, oh, H5O_FILL_ID, H5O_MSG_FLAG_CONSTANT, 0, &old_fill_prop) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to update old fill value header message");
    } /* end if */

    /* Update/create the layout (and I/O pipeline & EFL) messages */
    if (H5D__layout_oh_create(file, oh, dset, dapl_id) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to update layout/pline/efl header message");

    /* Indicate that the layout information was initialized */
    layout_init = true;

#ifdef H5O_ENABLE_BOGUS
    {
        H5P_genplist_t *dc_plist; /* Dataset's creation property list */

        /* Get dataset's property list object */
        if (NULL == (dc_plist = (H5P_genplist_t *)H5I_object(dset->shared->dcpl_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "can't get dataset creation property list");

        /* Check whether to add a "bogus" message */
        if ((H5P_exist_plist(dc_plist, H5O_BOGUS_MSG_FLAGS_NAME) > 0) &&
            (H5P_exist_plist(dc_plist, H5O_BOGUS_MSG_ID_NAME) > 0)) {

            uint8_t  bogus_flags = 0; /* Flags for creating "bogus" message */
            unsigned bogus_id;        /* "bogus" ID */

            /* Retrieve "bogus" message ID */
            if (H5P_get(dc_plist, H5O_BOGUS_MSG_ID_NAME, &bogus_id) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get bogus ID options");
            /* Retrieve "bogus" message flags */
            if (H5P_get(dc_plist, H5O_BOGUS_MSG_FLAGS_NAME, &bogus_flags) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get bogus message options");

            /* Add a "bogus" message (for error testing). */
            if (H5O_bogus_oh(file, oh, bogus_id, (unsigned)bogus_flags) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to create 'bogus' message");
        } /* end if */
    }
#endif /* H5O_ENABLE_BOGUS */

    /* Add a modification time message, if using older format. */
    /* (If using v18 format versions and above, the modification time is part of the object
     *  header and doesn't use a separate message -QAK)
     */
    if (!use_at_least_v18)
        if (H5O_touch_oh(file, oh, true) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to update modification time message");

done:
    /* Release pointer to object header itself */
    if (oh != NULL)
        if (H5O_unpin(oh) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTUNPIN, FAIL, "unable to unpin dataset object header");

    /* Error cleanup */
    if (ret_value < 0)
        if (layout_init)
            /* Destroy the layout information for the dataset */
            if (dset->shared->layout.ops->dest && (dset->shared->layout.ops->dest)(dset) < 0)
                HDONE_ERROR(H5E_DATASET, H5E_CANTRELEASE, FAIL, "unable to destroy layout info");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__update_oh_info() */

/*--------------------------------------------------------------------------
 * Function:    H5D__build_file_prefix
 *
 * Purpose:     Determine the file prefix to be used and store
 *              it in file_prefix. Stores an empty string if no prefix
 *              should be used.
 *
 * Return:      SUCCEED/FAIL
 *--------------------------------------------------------------------------
 */
static herr_t
H5D__build_file_prefix(const H5D_t *dset, H5F_prefix_open_t prefix_type, char **file_prefix /*out*/)
{
    const char *prefix   = NULL;     /* prefix used to look for the file               */
    char       *filepath = NULL;     /* absolute path of directory the HDF5 file is in */
    size_t      filepath_len;        /* length of file path                            */
    size_t      prefix_len;          /* length of prefix                               */
    size_t      file_prefix_len;     /* length of expanded prefix                      */
    herr_t      ret_value = SUCCEED; /* Return value                                   */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(dset);
    assert(dset->oloc.file);
    filepath = H5F_EXTPATH(dset->oloc.file);
    assert(filepath);

    /* XXX: Future thread-safety note - getenv is not required
     *      to be reentrant.
     */
    if (H5F_PREFIX_VDS == prefix_type) {
        prefix = H5D_prefix_vds_env;

        if (prefix == NULL || *prefix == '\0') {
            if (H5CX_get_vds_prefix(&prefix) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get the prefix for vds file");
        }
    }
    else if (H5F_PREFIX_EFILE == prefix_type) {
        prefix = H5D_prefix_ext_env;

        if (prefix == NULL || *prefix == '\0') {
            if (H5CX_get_ext_file_prefix(&prefix) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get the prefix for the external file");
        }
    }
    else
        HGOTO_ERROR(H5E_DATASET, H5E_BADTYPE, FAIL, "prefix name is not sensible");

    /* Prefix has to be checked for NULL / empty string again because the
     * code above might have updated it.
     */
    if (prefix == NULL || *prefix == '\0' || strcmp(prefix, ".") == 0) {
        /* filename is interpreted as relative to the current directory,
         * does not need to be expanded
         */
        *file_prefix = NULL;
    } /* end if */
    else {
        if (strncmp(prefix, "${ORIGIN}", strlen("${ORIGIN}")) == 0) {
            /* Replace ${ORIGIN} at beginning of prefix by directory of HDF5 file */
            filepath_len    = strlen(filepath);
            prefix_len      = strlen(prefix);
            file_prefix_len = filepath_len + prefix_len - strlen("${ORIGIN}") + 1;

            if (NULL == (*file_prefix = (char *)H5MM_malloc(file_prefix_len)))
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "unable to allocate buffer");
            snprintf(*file_prefix, file_prefix_len, "%s%s", filepath, prefix + strlen("${ORIGIN}"));
        } /* end if */
        else {
            if (NULL == (*file_prefix = (char *)H5MM_strdup(prefix)))
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed");
        } /* end else */
    }     /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__build_file_prefix() */

/*-------------------------------------------------------------------------
 * Function:    H5D__create
 *
 * Purpose:    Creates a new dataset with name NAME in file F and associates
 *        with it a datatype TYPE for each element as stored in the
 *        file, dimensionality information or dataspace SPACE, and
 *        other miscellaneous properties CREATE_PARMS.  All arguments
 *        are deep-copied before being associated with the new dataset,
 *        so the caller is free to subsequently modify them without
 *        affecting the dataset.
 *
 * Return:    Success:    Pointer to a new dataset
 *            Failure:    NULL
 *-------------------------------------------------------------------------
 */
H5D_t *
H5D__create(H5F_t *file, hid_t type_id, const H5S_t *space, hid_t dcpl_id, hid_t dapl_id)
{
    H5T_t          *type          = NULL; /* Datatype for dataset (VOL pointer) */
    H5T_t          *dt            = NULL; /* Datatype for dataset (non-VOL pointer) */
    H5D_t          *new_dset      = NULL;
    H5P_genplist_t *dc_plist      = NULL;  /* New Property list */
    bool            has_vl_type   = false; /* Flag to indicate a VL-type for dataset */
    bool            layout_init   = false; /* Flag to indicate that chunk information was initialized */
    bool            layout_copied = false; /* Flag to indicate that layout message was copied */
    bool            fill_copied   = false; /* Flag to indicate that fill-value message was copied */
    bool            pline_copied  = false; /* Flag to indicate that pipeline message was copied */
    bool            efl_copied    = false; /* Flag to indicate that external file list message was copied */
    H5G_loc_t       dset_loc;              /* Dataset location */
    H5D_t          *ret_value = NULL;      /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(file);
    assert(H5I_DATATYPE == H5I_get_type(type_id));
    assert(space);
    assert(H5I_GENPROP_LST == H5I_get_type(dcpl_id));

    /* Get the dataset's datatype */
    if (NULL == (dt = (H5T_t *)H5I_object(type_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a datatype");

    /* If this is a named datatype, get the pointer via the VOL plugin */
    type = H5T_get_actual_type(dt);

    /* Check if the datatype is "sensible" for use in a dataset */
    if (H5T_is_sensible(type) != true)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "datatype is not sensible");

    /* Check if the datatype is/contains a VL-type */
    if (H5T_detect_class(type, H5T_VLEN, false))
        has_vl_type = true;

    /* Check if the dataspace has an extent set (or is NULL) */
    if (!H5S_has_extent(space))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "dataspace extent has not been set.");

    /* Initialize the dataset object */
    if (NULL == (new_dset = H5FL_CALLOC(H5D_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Set up & reset dataset location */
    dset_loc.oloc = &(new_dset->oloc);
    dset_loc.path = &(new_dset->path);
    H5G_loc_reset(&dset_loc);

    /* Initialize the shared dataset space */
    if (NULL == (new_dset->shared = H5D__new(dcpl_id, dapl_id, true, has_vl_type)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Copy & initialize datatype for dataset */
    if (H5D__init_type(file, new_dset, type_id, type) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't copy datatype");

    /* Copy & initialize dataspace for dataset */
    if (H5D__init_space(file, new_dset, space) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't copy dataspace");

    /* Set the dataset's checked_filters flag to enable writing */
    new_dset->shared->checked_filters = true;

    /* Check if the dataset has a non-default DCPL & get important values, if so */
    if (new_dset->shared->dcpl_id != H5P_DATASET_CREATE_DEFAULT) {
        H5O_layout_t *layout;                 /* Dataset's layout information */
        H5O_pline_t  *pline;                  /* Dataset's I/O pipeline information */
        H5O_fill_t   *fill;                   /* Dataset's fill value info */
        H5O_efl_t    *efl;                    /* Dataset's external file list info */
        htri_t        ignore_filters = false; /* Ignore optional filters or not */

        if ((ignore_filters = H5Z_ignore_filters(new_dset->shared->dcpl_id, dt, space)) < 0)
            HGOTO_ERROR(H5E_ARGS, H5E_CANTINIT, NULL, "H5Z_has_optional_filter() failed");

        if (false == ignore_filters) {
            /* Check if the filters in the DCPL can be applied to this dataset */
            if (H5Z_can_apply(new_dset->shared->dcpl_id, new_dset->shared->type_id) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_CANTINIT, NULL, "I/O filters can't operate on this dataset");

            /* Make the "set local" filter callbacks for this dataset */
            if (H5Z_set_local(new_dset->shared->dcpl_id, new_dset->shared->type_id) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "unable to set local filter parameters");
        } /* ignore_filters */

        /* Get new dataset's property list object */
        if (NULL == (dc_plist = (H5P_genplist_t *)H5I_object(new_dset->shared->dcpl_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't get dataset creation property list");

        /* Retrieve the properties we need */
        pline = &new_dset->shared->dcpl_cache.pline;
        if (H5P_get(dc_plist, H5O_CRT_PIPELINE_NAME, pline) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, NULL, "can't retrieve pipeline filter");
        pline_copied = true;
        layout       = &new_dset->shared->layout;
        if (H5P_get(dc_plist, H5D_CRT_LAYOUT_NAME, layout) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, NULL, "can't retrieve layout");
        layout_copied = true;
        fill          = &new_dset->shared->dcpl_cache.fill;
        if (H5P_get(dc_plist, H5D_CRT_FILL_VALUE_NAME, fill) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, NULL, "can't retrieve fill value info");
        fill_copied = true;
        efl         = &new_dset->shared->dcpl_cache.efl;
        if (H5P_get(dc_plist, H5D_CRT_EXT_FILE_LIST_NAME, efl) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, NULL, "can't retrieve external file list");
        efl_copied = true;

        if (false == ignore_filters) {
            /* Check that chunked layout is used if filters are enabled */
            if (pline->nused > 0 && H5D_CHUNKED != layout->type)
                HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, NULL, "filters can only be used with chunked layout");
        }

        /* Check if the alloc_time is the default and error out */
        if (fill->alloc_time == H5D_ALLOC_TIME_DEFAULT)
            HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, NULL, "invalid space allocation state");

        /* Don't allow compact datasets to allocate space later */
        if (layout->type == H5D_COMPACT && fill->alloc_time != H5D_ALLOC_TIME_EARLY)
            HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, NULL, "compact dataset must have early space allocation");
    } /* end if */

    /* Set the version for the I/O pipeline message */
    if (H5O_pline_set_version(file, &new_dset->shared->dcpl_cache.pline) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, NULL, "can't set latest version of I/O filter pipeline");

    /* Set the version for the fill message */
    if (H5O_fill_set_version(file, &new_dset->shared->dcpl_cache.fill) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, NULL, "can't set latest version of fill value");

    /* Set the latest version for the layout message */
    if (H5D__layout_set_version(file, &new_dset->shared->layout) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, NULL, "can't set latest version of layout");

    if (new_dset->shared->layout.version >= H5O_LAYOUT_VERSION_4) {
        /* Use latest indexing type for layout message version >= 4 */
        if (H5D__layout_set_latest_indexing(&new_dset->shared->layout, new_dset->shared->space,
                                            &new_dset->shared->dcpl_cache) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, NULL, "can't set latest indexing");
    } /* end if */

    /* Check if the file driver would like to force early space allocation */
    if (H5F_HAS_FEATURE(file, H5FD_FEAT_ALLOCATE_EARLY))
        new_dset->shared->dcpl_cache.fill.alloc_time = H5D_ALLOC_TIME_EARLY;

    /*
     * Check if this dataset is going into a parallel file and set space allocation time.
     * If the dataset has filters applied to it, writes to the dataset must be collective,
     * so we don't need to force early space allocation. Otherwise, we force early space
     * allocation to facilitate independent raw data operations.
     */
    if (H5F_HAS_FEATURE(file, H5FD_FEAT_HAS_MPI) && (new_dset->shared->dcpl_cache.pline.nused == 0))
        new_dset->shared->dcpl_cache.fill.alloc_time = H5D_ALLOC_TIME_EARLY;

    /* Set the dataset's I/O operations */
    if (H5D__layout_set_io_ops(new_dset) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "unable to initialize I/O operations");

    /* Create the layout information for the new dataset */
    if (new_dset->shared->layout.ops->construct &&
        (new_dset->shared->layout.ops->construct)(file, new_dset) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "unable to construct layout information");

    /* Update the dataset's object header info. */
    if (H5D__update_oh_info(file, new_dset, new_dset->shared->dapl_id) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't update the metadata cache");

    /* Indicate that the layout information was initialized */
    layout_init = true;

    /* Set up append flush parameters for the dataset */
    if (H5D__append_flush_setup(new_dset, new_dset->shared->dapl_id) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "unable to set up flush append property");

    /* Set the external file prefix */
    if (H5D__build_file_prefix(new_dset, H5F_PREFIX_EFILE, &new_dset->shared->extfile_prefix) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "unable to initialize external file prefix");

    /* Set the VDS file prefix */
    if (H5D__build_file_prefix(new_dset, H5F_PREFIX_VDS, &new_dset->shared->vds_prefix) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "unable to initialize VDS prefix");

    /* Add the dataset to the list of opened objects in the file */
    if (H5FO_top_incr(new_dset->oloc.file, new_dset->oloc.addr) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINC, NULL, "can't incr object ref. count");
    if (H5FO_insert(new_dset->oloc.file, new_dset->oloc.addr, new_dset->shared, true) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINSERT, NULL, "can't insert dataset into list of open objects");
    new_dset->shared->fo_count = 1;

    /* Success */
    ret_value = new_dset;

done:
    if (!ret_value && new_dset) {
        if (new_dset->shared) {
            if (layout_init)
                if (new_dset->shared->layout.ops->dest && (new_dset->shared->layout.ops->dest)(new_dset) < 0)
                    HDONE_ERROR(H5E_DATASET, H5E_CANTRELEASE, NULL, "unable to destroy layout info");
            if (pline_copied)
                if (H5O_msg_reset(H5O_PLINE_ID, &new_dset->shared->dcpl_cache.pline) < 0)
                    HDONE_ERROR(H5E_DATASET, H5E_CANTRESET, NULL, "unable to reset I/O pipeline info");
            if (layout_copied)
                if (H5O_msg_reset(H5O_LAYOUT_ID, &new_dset->shared->layout) < 0)
                    HDONE_ERROR(H5E_DATASET, H5E_CANTRESET, NULL, "unable to reset layout info");
            if (fill_copied)
                if (H5O_msg_reset(H5O_FILL_ID, &new_dset->shared->dcpl_cache.fill) < 0)
                    HDONE_ERROR(H5E_DATASET, H5E_CANTRESET, NULL, "unable to reset fill-value info");
            if (efl_copied)
                if (H5O_msg_reset(H5O_EFL_ID, &new_dset->shared->dcpl_cache.efl) < 0)
                    HDONE_ERROR(H5E_DATASET, H5E_CANTRESET, NULL, "unable to reset external file list info");
            if (new_dset->shared->space && H5S_close(new_dset->shared->space) < 0)
                HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, NULL, "unable to release dataspace");

            if (new_dset->shared->type) {
                if (new_dset->shared->type_id > 0) {
                    if (H5I_dec_ref(new_dset->shared->type_id) < 0)
                        HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, NULL, "unable to release datatype");
                } /* end if */
                else {
                    if (H5T_close_real(new_dset->shared->type) < 0)
                        HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, NULL, "unable to release datatype");
                } /* end else */
            }     /* end if */

            if (H5_addr_defined(new_dset->oloc.addr)) {
                if (H5O_dec_rc_by_loc(&(new_dset->oloc)) < 0)
                    HDONE_ERROR(H5E_DATASET, H5E_CANTDEC, NULL,
                                "unable to decrement refcount on newly created object");
                if (H5O_close(&(new_dset->oloc), NULL) < 0)
                    HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, NULL, "unable to release object header");
                if (file) {
                    if (H5O_delete(file, new_dset->oloc.addr) < 0)
                        HDONE_ERROR(H5E_DATASET, H5E_CANTDELETE, NULL, "unable to delete object header");
                } /* end if */
            }     /* end if */
            if (new_dset->shared->dcpl_id != 0 && H5I_dec_ref(new_dset->shared->dcpl_id) < 0)
                HDONE_ERROR(H5E_DATASET, H5E_CANTDEC, NULL, "unable to decrement ref count on property list");
            if (new_dset->shared->dapl_id != 0 && H5I_dec_ref(new_dset->shared->dapl_id) < 0)
                HDONE_ERROR(H5E_DATASET, H5E_CANTDEC, NULL, "unable to decrement ref count on property list");
            new_dset->shared->extfile_prefix = (char *)H5MM_xfree(new_dset->shared->extfile_prefix);
            new_dset->shared->vds_prefix     = (char *)H5MM_xfree(new_dset->shared->vds_prefix);
            new_dset->shared                 = H5FL_FREE(H5D_shared_t, new_dset->shared);
        } /* end if */
        new_dset->oloc.file = NULL;
        new_dset            = H5FL_FREE(H5D_t, new_dset);
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__create() */

/*-------------------------------------------------------------------------
 * Function:    H5D__open_name
 *
 * Purpose:     Opens an existing dataset by name.
 *
 * Return:      Success:        Ptr to a new dataset.
 *              Failure:        NULL
 *-------------------------------------------------------------------------
 */
H5D_t *
H5D__open_name(const H5G_loc_t *loc, const char *name, hid_t dapl_id)
{
    H5D_t     *dset = NULL;
    H5G_loc_t  dset_loc;          /* Object location of dataset */
    H5G_name_t path;              /* Dataset group hier. path */
    H5O_loc_t  oloc;              /* Dataset object location */
    H5O_type_t obj_type;          /* Type of object at location */
    bool       loc_found = false; /* Location at 'name' found */
    H5D_t     *ret_value = NULL;  /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(loc);
    assert(name);

    /* Set up dataset location to fill in */
    dset_loc.oloc = &oloc;
    dset_loc.path = &path;
    H5G_loc_reset(&dset_loc);

    /* Find the dataset object */
    if (H5G_loc_find(loc, name, &dset_loc) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_NOTFOUND, NULL, "not found");
    loc_found = true;

    /* Check that the object found is the correct type */
    if (H5O_obj_type(&oloc, &obj_type) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, NULL, "can't get object type");
    if (obj_type != H5O_TYPE_DATASET)
        HGOTO_ERROR(H5E_DATASET, H5E_BADTYPE, NULL, "not a dataset");

    /* Open the dataset */
    if (NULL == (dset = H5D_open(&dset_loc, dapl_id)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't open dataset");

    /* Set return value */
    ret_value = dset;

done:
    if (!ret_value)
        if (loc_found && H5G_loc_free(&dset_loc) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTRELEASE, NULL, "can't free location");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__open_name() */

/*
 *-------------------------------------------------------------------------
 * Function: H5D_open
 *
 * Purpose:  Checks if dataset is already open, or opens a dataset for
 *              access.
 *
 * Return:   Success:    Dataset ID
 *           Failure:    FAIL
 *-------------------------------------------------------------------------
 */
H5D_t *
H5D_open(const H5G_loc_t *loc, hid_t dapl_id)
{
    H5D_shared_t *shared_fo      = NULL;
    H5D_t        *dataset        = NULL;
    char         *extfile_prefix = NULL; /* Expanded external file prefix */
    char         *vds_prefix     = NULL; /* Expanded vds prefix */
    H5D_t        *ret_value      = NULL; /* Return value */

    FUNC_ENTER_NOAPI(NULL)

    /* check args */
    assert(loc);

    /* Allocate the dataset structure */
    if (NULL == (dataset = H5FL_CALLOC(H5D_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Shallow copy (take ownership) of the object location object */
    if (H5O_loc_copy_shallow(&(dataset->oloc), loc->oloc) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, NULL, "can't copy object location");

    /* Shallow copy (take ownership) of the group hier. path */
    if (H5G_name_copy(&(dataset->path), loc->path, H5_COPY_SHALLOW) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, NULL, "can't copy path");

    /* Get the external file prefix */
    if (H5D__build_file_prefix(dataset, H5F_PREFIX_EFILE, &extfile_prefix) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "unable to initialize external file prefix");

    /* Get the VDS prefix */
    if (H5D__build_file_prefix(dataset, H5F_PREFIX_VDS, &vds_prefix) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "unable to initialize VDS prefix");

    /* Check if dataset was already open */
    if (NULL == (shared_fo = (H5D_shared_t *)H5FO_opened(dataset->oloc.file, dataset->oloc.addr))) {
        /* Clear any errors from H5FO_opened() */
        H5E_clear_stack(NULL);

        /* Open the dataset object */
        if (H5D__open_oid(dataset, dapl_id) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_NOTFOUND, NULL, "not found");

        /* Add the dataset to the list of opened objects in the file */
        if (H5FO_insert(dataset->oloc.file, dataset->oloc.addr, dataset->shared, false) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINSERT, NULL, "can't insert dataset into list of open objects");

        /* Increment object count for the object in the top file */
        if (H5FO_top_incr(dataset->oloc.file, dataset->oloc.addr) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINC, NULL, "can't increment object count");

        /* We're the first dataset to use the shared info */
        dataset->shared->fo_count = 1;

        /* Set the external file prefix */
        dataset->shared->extfile_prefix = extfile_prefix;
        /* Prevent string from being freed during done: */
        extfile_prefix = NULL;

        /* Set the vds file prefix */
        dataset->shared->vds_prefix = vds_prefix;
        /* Prevent string from being freed during done: */
        vds_prefix = NULL;

    } /* end if */
    else {
        /* Point to shared info */
        dataset->shared = shared_fo;

        /* Increment # of datasets using shared information */
        shared_fo->fo_count++;

        /* Check whether the external file prefix of the already open dataset
         * matches the new external file prefix
         */
        if (extfile_prefix && dataset->shared->extfile_prefix) {
            if (strcmp(extfile_prefix, dataset->shared->extfile_prefix) != 0)
                HGOTO_ERROR(
                    H5E_DATASET, H5E_CANTOPENOBJ, NULL,
                    "new external file prefix does not match external file prefix of already open dataset");
        }
        else {
            if (extfile_prefix || dataset->shared->extfile_prefix)
                HGOTO_ERROR(
                    H5E_DATASET, H5E_CANTOPENOBJ, NULL,
                    "new external file prefix does not match external file prefix of already open dataset");
        }

        /* Check if the object has been opened through the top file yet */
        if (H5FO_top_count(dataset->oloc.file, dataset->oloc.addr) == 0) {
            /* Open the object through this top file */
            if (H5O_open(&(dataset->oloc)) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, NULL, "unable to open object header");
        } /* end if */

        /* Increment object count for the object in the top file */
        if (H5FO_top_incr(dataset->oloc.file, dataset->oloc.addr) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINC, NULL, "can't increment object count");
    } /* end else */

    /* Set the dataset to return */
    ret_value = dataset;

done:
    extfile_prefix = (char *)H5MM_xfree(extfile_prefix);
    vds_prefix     = (char *)H5MM_xfree(vds_prefix);

    if (ret_value == NULL) {
        /* Free the location */
        if (dataset) {
            if (shared_fo == NULL && dataset->shared) { /* Need to free shared fo */
                dataset->shared->extfile_prefix = (char *)H5MM_xfree(dataset->shared->extfile_prefix);
                dataset->shared->vds_prefix     = (char *)H5MM_xfree(dataset->shared->vds_prefix);
                dataset->shared                 = H5FL_FREE(H5D_shared_t, dataset->shared);
            }

            H5O_loc_free(&(dataset->oloc));
            H5G_name_free(&(dataset->path));

            dataset = H5FL_FREE(H5D_t, dataset);
        } /* end if */
        if (shared_fo)
            shared_fo->fo_count--;
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D_open() */

/*
 *-------------------------------------------------------------------------
 * Function: H5D__flush_append_setup
 *
 * Purpose:  Set the append flush parameters for a dataset
 *
 * Return:   Non-negative on success/Negative on failure
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__append_flush_setup(H5D_t *dset, hid_t dapl_id)
{
    herr_t ret_value = SUCCEED; /* return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(dset);
    assert(dset->shared);

    /* Set default append flush values */
    memset(&dset->shared->append_flush, 0, sizeof(dset->shared->append_flush));

    /* If the dataset is chunked and there is a non-default DAPL */
    if (dapl_id != H5P_DATASET_ACCESS_DEFAULT && dset->shared->layout.type == H5D_CHUNKED) {
        H5P_genplist_t *dapl; /* data access property list object pointer */

        /* Get dataset access property list */
        if (NULL == (dapl = (H5P_genplist_t *)H5I_object(dapl_id)))
            HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for dapl ID");

        /* Check if append flush property exists */
        if (H5P_exist_plist(dapl, H5D_ACS_APPEND_FLUSH_NAME) > 0) {
            H5D_append_flush_t info;

            /* Get append flush property */
            if (H5P_get(dapl, H5D_ACS_APPEND_FLUSH_NAME, &info) < 0)
                HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get append flush info");
            if (info.ndims > 0) {
                hsize_t  curr_dims[H5S_MAX_RANK]; /* current dimension sizes */
                hsize_t  max_dims[H5S_MAX_RANK];  /* current dimension sizes */
                int      rank;                    /* dataspace # of dimensions */
                unsigned u;                       /* local index variable */

                /* Get dataset rank */
                if ((rank = H5S_get_simple_extent_dims(dset->shared->space, curr_dims, max_dims)) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get dataset dimensions");
                if (info.ndims != (unsigned)rank)
                    HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL,
                                "boundary dimension rank does not match dataset rank");

                /* Validate boundary sizes */
                for (u = 0; u < info.ndims; u++)
                    if (info.boundary[u] != 0) /* when a non-zero boundary is set */
                        /* the dimension is extendible? */
                        if (max_dims[u] != H5S_UNLIMITED && max_dims[u] == curr_dims[u])
                            break;

                /* At least one boundary dimension is not extendible */
                if (u != info.ndims)
                    HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "boundary dimension is not valid");

                /* Copy append flush settings */
                dset->shared->append_flush.ndims = info.ndims;
                dset->shared->append_flush.func  = info.func;
                dset->shared->append_flush.udata = info.udata;
                H5MM_memcpy(dset->shared->append_flush.boundary, info.boundary, sizeof(info.boundary));
            } /* end if */
        }     /* end if */
    }         /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__append_flush_setup() */

/*-------------------------------------------------------------------------
 * Function: H5D__open_oid
 *
 * Purpose:  Opens a dataset for access.
 *
 * Return:   Dataset pointer on success, NULL on failure
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__open_oid(H5D_t *dataset, hid_t dapl_id)
{
    H5P_genplist_t *plist;                     /* Property list */
    H5O_fill_t     *fill_prop;                 /* Pointer to dataset's fill value info */
    unsigned        alloc_time_state;          /* Allocation time state */
    htri_t          msg_exists;                /* Whether a particular type of message exists */
    bool            layout_init       = false; /* Flag to indicate that chunk information was initialized */
    bool            must_init_storage = false;
    herr_t          ret_value         = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE_TAG(dataset->oloc.addr)

    /* check args */
    assert(dataset);

    /* (Set the 'vl_type' parameter to false since it doesn't matter from here) */
    if (NULL == (dataset->shared = H5D__new(H5P_DATASET_CREATE_DEFAULT, dapl_id, false, false)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed");

    /* Open the dataset object */
    if (H5O_open(&(dataset->oloc)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, FAIL, "unable to open");

    /* Get the type and space */
    if (NULL == (dataset->shared->type = (H5T_t *)H5O_msg_read(&(dataset->oloc), H5O_DTYPE_ID, NULL)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to load type info from dataset header");

    if (H5T_set_loc(dataset->shared->type, H5F_VOL_OBJ(dataset->oloc.file), H5T_LOC_DISK) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "invalid datatype location");

    if (NULL == (dataset->shared->space = H5S_read(&(dataset->oloc))))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to load dataspace info from dataset header");

    /* Cache the dataset's dataspace info */
    if (H5D__cache_dataspace_info(dataset) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "can't cache dataspace info");

    /* Get a datatype ID for the dataset's datatype */
    if ((dataset->shared->type_id = H5I_register(H5I_DATATYPE, dataset->shared->type, false)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTREGISTER, FAIL, "unable to register type");

    /* Get dataset creation property list object */
    if (NULL == (plist = (H5P_genplist_t *)H5I_object(dataset->shared->dcpl_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "can't get dataset creation property list");

    /* Get the layout/pline/efl message information */
    if (H5D__layout_oh_read(dataset, dapl_id, plist) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get layout/pline/efl info");

    /* Indicate that the layout information was initialized */
    layout_init = true;

    /*
     * Now that we've read the dataset's datatype, dataspace and
     * layout information, perform a quick check for compact datasets
     * to ensure that the size of the internal buffer that was
     * allocated for the dataset's raw data matches the size of
     * the data. A corrupted file can cause a mismatch between the
     * two, which might result in buffer overflows during future
     * I/O to the dataset.
     */
    if (H5D_COMPACT == dataset->shared->layout.type) {
        hssize_t dset_nelemts   = 0;
        size_t   dset_type_size = H5T_GET_SIZE(dataset->shared->type);
        size_t   dset_data_size = 0;

        assert(H5D_COMPACT == dataset->shared->layout.storage.type);

        if ((dset_nelemts = H5S_GET_EXTENT_NPOINTS(dataset->shared->space)) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL,
                        "can't get number of elements in dataset's dataspace");

        dset_data_size = (size_t)dset_nelemts * dset_type_size;

        if (dataset->shared->layout.storage.u.compact.size != dset_data_size)
            HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL,
                        "bad value from dataset header - size of compact dataset's data buffer doesn't match "
                        "size of dataset data");
    }

    /* Set up flush append property */
    if (H5D__append_flush_setup(dataset, dapl_id))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "unable to set up flush append property");

    /* Point at dataset's copy, to cache it for later */
    fill_prop = &dataset->shared->dcpl_cache.fill;

    /* Try to get the new fill value message from the object header */
    if ((msg_exists = H5O_msg_exists(&(dataset->oloc), H5O_FILL_NEW_ID)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't check if message exists");
    if (msg_exists) {
        if (NULL == H5O_msg_read(&(dataset->oloc), H5O_FILL_NEW_ID, fill_prop))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't retrieve message");
    } /* end if */
    else {
        /* For backward compatibility, try to retrieve the old fill value message */
        if ((msg_exists = H5O_msg_exists(&(dataset->oloc), H5O_FILL_ID)) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't check if message exists");
        if (msg_exists) {
            if (NULL == H5O_msg_read(&(dataset->oloc), H5O_FILL_ID, fill_prop))
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't retrieve message");
        } /* end if */
        else {
            /* Set the space allocation time appropriately, based on the type of dataset storage */
            switch (dataset->shared->layout.type) {
                case H5D_COMPACT:
                    fill_prop->alloc_time = H5D_ALLOC_TIME_EARLY;
                    break;

                case H5D_CONTIGUOUS:
                    fill_prop->alloc_time = H5D_ALLOC_TIME_LATE;
                    break;

                case H5D_CHUNKED:
                    fill_prop->alloc_time = H5D_ALLOC_TIME_INCR;
                    break;

                case H5D_VIRTUAL:
                    fill_prop->alloc_time = H5D_ALLOC_TIME_INCR;
                    break;

                case H5D_LAYOUT_ERROR:
                case H5D_NLAYOUTS:
                default:
                    HGOTO_ERROR(H5E_DATASET, H5E_UNSUPPORTED, FAIL, "not implemented yet");
            } /* end switch */ /*lint !e788 All appropriate cases are covered */
        }                      /* end else */

        /* If "old" fill value size is 0 (undefined), map it to -1 */
        if (fill_prop->size == 0)
            fill_prop->size = (ssize_t)-1;
    } /* end if */
    alloc_time_state = 0;
    if ((dataset->shared->layout.type == H5D_COMPACT && fill_prop->alloc_time == H5D_ALLOC_TIME_EARLY) ||
        (dataset->shared->layout.type == H5D_CONTIGUOUS && fill_prop->alloc_time == H5D_ALLOC_TIME_LATE) ||
        (dataset->shared->layout.type == H5D_CHUNKED && fill_prop->alloc_time == H5D_ALLOC_TIME_INCR) ||
        (dataset->shared->layout.type == H5D_VIRTUAL && fill_prop->alloc_time == H5D_ALLOC_TIME_INCR))
        alloc_time_state = 1;

    /* Set revised fill value properties, if they are different from the defaults */
    if (H5P_fill_value_cmp(&H5D_def_dset.dcpl_cache.fill, fill_prop, sizeof(H5O_fill_t))) {
        if (H5P_set(plist, H5D_CRT_FILL_VALUE_NAME, fill_prop) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "can't set fill value");
        if (H5P_set(plist, H5D_CRT_ALLOC_TIME_STATE_NAME, &alloc_time_state) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "can't set allocation time state");
    } /* end if */

    /*
     * Make sure all storage is properly initialized.
     * This is important only for parallel I/O where the space must
     * be fully allocated before I/O can happen.
     *
     * Storage will be initialized here if either the VFD being used
     * has set the H5FD_FEAT_ALLOCATE_EARLY flag to indicate that it
     * wishes to force early space allocation OR a parallel VFD is
     * being used and the dataset in question doesn't have any filters
     * applied to it. If filters are applied to the dataset, collective
     * I/O will be required when writing to the dataset, so we don't
     * need to initialize storage here, as the collective I/O process
     * will coordinate that.
     */
    must_init_storage = (H5F_INTENT(dataset->oloc.file) & H5F_ACC_RDWR) &&
                        !(*dataset->shared->layout.ops->is_space_alloc)(&dataset->shared->layout.storage);
    must_init_storage = must_init_storage && (H5F_HAS_FEATURE(dataset->oloc.file, H5FD_FEAT_ALLOCATE_EARLY) ||
                                              (H5F_HAS_FEATURE(dataset->oloc.file, H5FD_FEAT_HAS_MPI) &&
                                               dataset->shared->dcpl_cache.pline.nused == 0));

    if (must_init_storage && (H5D__alloc_storage(dataset, H5D_ALLOC_OPEN, false, NULL) < 0))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to initialize file storage");

done:
    if (ret_value < 0) {
        if (H5_addr_defined(dataset->oloc.addr) && H5O_close(&(dataset->oloc), NULL) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "unable to release object header");
        if (dataset->shared) {
            if (layout_init)
                if (dataset->shared->layout.ops->dest && (dataset->shared->layout.ops->dest)(dataset) < 0)
                    HDONE_ERROR(H5E_DATASET, H5E_CANTRELEASE, FAIL, "unable to destroy layout info");
            if (dataset->shared->space && H5S_close(dataset->shared->space) < 0)
                HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "unable to release dataspace");
            if (dataset->shared->type) {
                if (dataset->shared->type_id > 0) {
                    if (H5I_dec_ref(dataset->shared->type_id) < 0)
                        HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "unable to release datatype");
                } /* end if */
                else {
                    if (H5T_close_real(dataset->shared->type) < 0)
                        HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "unable to release datatype");
                } /* end else */
            }     /* end if */
        }         /* end if */
    }             /* end if */

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5D__open_oid() */

/*-------------------------------------------------------------------------
 * Function: H5D_close
 *
 * Purpose:  Insures that all data has been saved to the file, closes the
 *           dataset object header, and frees all resources used by the
 *           descriptor.
 *
 * Return:   Non-negative on success/Negative on failure
 *-------------------------------------------------------------------------
 */
herr_t
H5D_close(H5D_t *dataset)
{
    bool   free_failed = false;   /* Set if freeing sub-components failed */
    bool   corked;                /* Whether the dataset is corked or not */
    bool   file_closed = true;    /* H5O_close also closed the file?      */
    herr_t ret_value   = SUCCEED; /* Return value                         */

    FUNC_ENTER_NOAPI(FAIL)

    /* check args */
    assert(dataset && dataset->oloc.file && dataset->shared);
    assert(dataset->shared->fo_count > 0);

    /* Dump debugging info */
#ifdef H5D_CHUNK_DEBUG
    H5D__chunk_stats(dataset, false);
#endif /* H5D_CHUNK_DEBUG */

    dataset->shared->fo_count--;
    if (dataset->shared->fo_count == 0) {

        /* Flush the dataset's information.  Continue to close even if it fails. */
        if (H5D__flush_real(dataset) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "unable to flush cached dataset info");

        /* Set a flag to indicate the dataset is closing, before we start freeing things */
        /* (Avoids problems with flushing datasets twice, when one is holding
         *      the file open and it iterates through dataset to flush them -QAK)
         */
        dataset->shared->closing = true;

        /* Free cached information for each kind of dataset */
        switch (dataset->shared->layout.type) {
            case H5D_CONTIGUOUS:
                /* Free the data sieve buffer, if it's been allocated */
                if (dataset->shared->cache.contig.sieve_buf)
                    dataset->shared->cache.contig.sieve_buf =
                        (unsigned char *)H5FL_BLK_FREE(sieve_buf, dataset->shared->cache.contig.sieve_buf);
                break;

            case H5D_CHUNKED:
                /* Check for skip list for iterating over chunks during I/O to close */
                if (dataset->shared->cache.chunk.sel_chunks) {
                    assert(H5SL_count(dataset->shared->cache.chunk.sel_chunks) == 0);
                    H5SL_close(dataset->shared->cache.chunk.sel_chunks);
                    dataset->shared->cache.chunk.sel_chunks = NULL;
                } /* end if */

                /* Check for cached single chunk dataspace */
                if (dataset->shared->cache.chunk.single_space) {
                    (void)H5S_close(dataset->shared->cache.chunk.single_space);
                    dataset->shared->cache.chunk.single_space = NULL;
                } /* end if */

                /* Check for cached single element chunk info */
                if (dataset->shared->cache.chunk.single_piece_info) {
                    dataset->shared->cache.chunk.single_piece_info =
                        H5FL_FREE(H5D_piece_info_t, dataset->shared->cache.chunk.single_piece_info);
                    dataset->shared->cache.chunk.single_piece_info = NULL;
                } /* end if */
                break;

            case H5D_COMPACT:
                /* Nothing special to do (info freed in the layout destroy) */
                break;

            case H5D_VIRTUAL: {
                size_t i, j;

                assert(dataset->shared->layout.storage.u.virt.list ||
                       (dataset->shared->layout.storage.u.virt.list_nused == 0));

                /* Close source datasets */
                for (i = 0; i < dataset->shared->layout.storage.u.virt.list_nused; i++) {
                    /* Close source dataset */
                    if (dataset->shared->layout.storage.u.virt.list[i].source_dset.dset) {
                        assert(dataset->shared->layout.storage.u.virt.list[i].source_dset.dset != dataset);
                        if (H5D_close(dataset->shared->layout.storage.u.virt.list[i].source_dset.dset) < 0)
                            HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "unable to close source dataset");
                        dataset->shared->layout.storage.u.virt.list[i].source_dset.dset = NULL;
                    } /* end if */

                    /* Close sub datasets */
                    for (j = 0; j < dataset->shared->layout.storage.u.virt.list[i].sub_dset_nused; j++)
                        if (dataset->shared->layout.storage.u.virt.list[i].sub_dset[j].dset) {
                            assert(dataset->shared->layout.storage.u.virt.list[i].sub_dset[j].dset !=
                                   dataset);
                            if (H5D_close(dataset->shared->layout.storage.u.virt.list[i].sub_dset[j].dset) <
                                0)
                                HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL,
                                            "unable to close source dataset");
                            dataset->shared->layout.storage.u.virt.list[i].sub_dset[j].dset = NULL;
                        } /* end if */
                }         /* end for */
            }             /* end block */
            break;

            case H5D_LAYOUT_ERROR:
            case H5D_NLAYOUTS:
            default:
                assert("not implemented yet" && 0);
#ifdef NDEBUG
                HGOTO_ERROR(H5E_IO, H5E_UNSUPPORTED, FAIL, "unsupported storage layout");
#endif                     /* NDEBUG */
        } /* end switch */ /*lint !e788 All appropriate cases are covered */

        /* Destroy any cached layout information for the dataset */
        if (dataset->shared->layout.ops->dest && (dataset->shared->layout.ops->dest)(dataset) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTRELEASE, FAIL, "unable to destroy layout info");

        /* Free the external file prefix */
        dataset->shared->extfile_prefix = (char *)H5MM_xfree(dataset->shared->extfile_prefix);

        /* Free the vds file prefix */
        dataset->shared->vds_prefix = (char *)H5MM_xfree(dataset->shared->vds_prefix);

        /* Release layout, fill-value, efl & pipeline messages */
        if (dataset->shared->dcpl_id != H5P_DATASET_CREATE_DEFAULT)
            free_failed |= (H5O_msg_reset(H5O_PLINE_ID, &dataset->shared->dcpl_cache.pline) < 0) ||
                           (H5O_msg_reset(H5O_LAYOUT_ID, &dataset->shared->layout) < 0) ||
                           (H5O_msg_reset(H5O_FILL_ID, &dataset->shared->dcpl_cache.fill) < 0) ||
                           (H5O_msg_reset(H5O_EFL_ID, &dataset->shared->dcpl_cache.efl) < 0);

        /* Uncork cache entries with object address tag */
        if (H5AC_cork(dataset->oloc.file, dataset->oloc.addr, H5AC__GET_CORKED, &corked) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "unable to retrieve an object's cork status");
        if (corked)
            if (H5AC_cork(dataset->oloc.file, dataset->oloc.addr, H5AC__UNCORK, NULL) < 0)
                HDONE_ERROR(H5E_DATASET, H5E_CANTUNCORK, FAIL, "unable to uncork an object");

        /* Release datatype, dataspace, and creation and access property lists -- there isn't
         * much we can do if one of these fails, so we just continue.
         */
        free_failed |=
            (H5I_dec_ref(dataset->shared->type_id) < 0) || (H5S_close(dataset->shared->space) < 0) ||
            (H5I_dec_ref(dataset->shared->dcpl_id) < 0) || (H5I_dec_ref(dataset->shared->dapl_id) < 0);

        /* Remove the dataset from the list of opened objects in the file */
        if (H5FO_top_decr(dataset->oloc.file, dataset->oloc.addr) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTRELEASE, FAIL, "can't decrement count for object");
        if (H5FO_delete(dataset->oloc.file, dataset->oloc.addr) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTRELEASE, FAIL, "can't remove dataset from list of open objects");

        /* Close the dataset object */
        /* (This closes the file, if this is the last object open) */
        if (H5O_close(&(dataset->oloc), &file_closed) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "unable to release object header");

        /* Evict dataset metadata if evicting on close */
        if (!file_closed && H5F_SHARED(dataset->oloc.file) && H5F_EVICT_ON_CLOSE(dataset->oloc.file)) {
            if (H5AC_flush_tagged_metadata(dataset->oloc.file, dataset->oloc.addr) < 0)
                HDONE_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "unable to flush tagged metadata");
            if (H5AC_evict_tagged_metadata(dataset->oloc.file, dataset->oloc.addr, false) < 0)
                HDONE_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "unable to evict tagged metadata");
        } /* end if */

        /*
         * Free memory.  Before freeing the memory set the file pointer to NULL.
         * We always check for a null file pointer in other H5D functions to be
         * sure we're not accessing an already freed dataset (see the assert()
         * above).
         */
        dataset->oloc.file = NULL;
        dataset->shared    = H5FL_FREE(H5D_shared_t, dataset->shared);

    } /* end if */
    else {
        /* Decrement the ref. count for this object in the top file */
        if (H5FO_top_decr(dataset->oloc.file, dataset->oloc.addr) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTRELEASE, FAIL, "can't decrement count for object");

        /* Check reference count for this object in the top file */
        if (H5FO_top_count(dataset->oloc.file, dataset->oloc.addr) == 0) {
            if (H5O_close(&(dataset->oloc), NULL) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to close");
        } /* end if */
        else
            /* Free object location (i.e. "unhold" the file if appropriate) */
            if (H5O_loc_free(&(dataset->oloc)) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTRELEASE, FAIL, "problem attempting to free location");
    } /* end else */

    /* Release the dataset's path info */
    if (H5G_name_free(&(dataset->path)) < 0)
        free_failed = true;

    /* Free the dataset's memory structure */
    dataset = H5FL_FREE(H5D_t, dataset);

    /* Check if anything failed in the middle... */
    if (free_failed)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL,
                    "couldn't free a component of the dataset, but the dataset was freed anyway.");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D_close() */

/*-------------------------------------------------------------------------
 * Function: H5D_mult_refresh_close
 *
 * Purpose:  Closing down the needed information when the dataset has
 *           multiple opens.  (From H5O__refresh_metadata_close())
 *
 * Return:   Non-negative on success/Negative on failure
 *-------------------------------------------------------------------------
 */
herr_t
H5D_mult_refresh_close(hid_t dset_id)
{
    H5D_t *dataset;             /* Dataset to refresh */
    herr_t ret_value = SUCCEED; /* return value */

    FUNC_ENTER_NOAPI(FAIL)

    if (NULL == (dataset = (H5D_t *)H5VL_object_verify(dset_id, H5I_DATASET)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataset");

    /* check args */
    assert(dataset);
    assert(dataset->oloc.file);
    assert(dataset->shared);
    assert(dataset->shared->fo_count > 0);

    if (dataset->shared->fo_count > 1) {
        /* Free cached information for each kind of dataset */
        switch (dataset->shared->layout.type) {
            case H5D_CONTIGUOUS:
                /* Free the data sieve buffer, if it's been allocated */
                if (dataset->shared->cache.contig.sieve_buf)
                    dataset->shared->cache.contig.sieve_buf =
                        (unsigned char *)H5FL_BLK_FREE(sieve_buf, dataset->shared->cache.contig.sieve_buf);
                break;

            case H5D_CHUNKED:
                /* Check for skip list for iterating over chunks during I/O to close */
                if (dataset->shared->cache.chunk.sel_chunks) {
                    assert(H5SL_count(dataset->shared->cache.chunk.sel_chunks) == 0);
                    H5SL_close(dataset->shared->cache.chunk.sel_chunks);
                    dataset->shared->cache.chunk.sel_chunks = NULL;
                } /* end if */

                /* Check for cached single chunk dataspace */
                if (dataset->shared->cache.chunk.single_space) {
                    (void)H5S_close(dataset->shared->cache.chunk.single_space);
                    dataset->shared->cache.chunk.single_space = NULL;
                } /* end if */

                /* Check for cached single element chunk info */
                if (dataset->shared->cache.chunk.single_piece_info) {
                    dataset->shared->cache.chunk.single_piece_info =
                        H5FL_FREE(H5D_piece_info_t, dataset->shared->cache.chunk.single_piece_info);
                    dataset->shared->cache.chunk.single_piece_info = NULL;
                } /* end if */
                break;

            case H5D_COMPACT:
            case H5D_VIRTUAL:
                /* Nothing special to do (info freed in the layout destroy) */
                break;

            case H5D_LAYOUT_ERROR:
            case H5D_NLAYOUTS:
            default:
                assert("not implemented yet" && 0);
#ifdef NDEBUG
                HGOTO_ERROR(H5E_IO, H5E_UNSUPPORTED, FAIL, "unsupported storage layout");
#endif                     /* NDEBUG */
        } /* end switch */ /*lint !e788 All appropriate cases are covered */

        /* Destroy any cached layout information for the dataset */
        if (dataset->shared->layout.ops->dest && (dataset->shared->layout.ops->dest)(dataset) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTRELEASE, FAIL, "unable to destroy layout info");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D_mult_refresh_close() */

/*-------------------------------------------------------------------------
 * Function: H5D_mult_refresh_reopen
 *
 * Purpose:  Re-initialize the needed info when the dataset has multiple
 *           opens.
 *
 * Return:   Non-negative on success/Negative on failure
 *-------------------------------------------------------------------------
 */
herr_t
H5D_mult_refresh_reopen(H5D_t *dataset)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* check args */
    assert(dataset && dataset->oloc.file && dataset->shared);
    assert(dataset->shared->fo_count > 0);

    if (dataset->shared->fo_count > 1) {
        /* Release dataspace info */
        if (H5S_close(dataset->shared->space) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTRELEASE, FAIL, "unable to release dataspace");

        /* Re-load dataspace info */
        if (NULL == (dataset->shared->space = H5S_read(&(dataset->oloc))))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to load dataspace info from dataset header");

        /* Cache the dataset's dataspace info */
        if (H5D__cache_dataspace_info(dataset) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "can't cache dataspace info");

        /* Release layout info */
        if (H5O_msg_reset(H5O_LAYOUT_ID, &dataset->shared->layout) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTRESET, FAIL, "unable to reset layout info");

        /* Re-load layout message info */
        if (NULL == H5O_msg_read(&(dataset->oloc), H5O_LAYOUT_ID, &(dataset->shared->layout)))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to read data layout message");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D_mult_refresh_reopen() */

/*-------------------------------------------------------------------------
 * Function: H5D_oloc
 *
 * Purpose:  Returns a pointer to the object location for a dataset.
 *
 * Return:   Success:    Ptr to location
 *           Failure:    NULL
 *-------------------------------------------------------------------------
 */
H5O_loc_t *
H5D_oloc(H5D_t *dataset)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    FUNC_LEAVE_NOAPI(dataset ? &(dataset->oloc) : (H5O_loc_t *)NULL)
} /* end H5D_oloc() */

/*-------------------------------------------------------------------------
 * Function: H5D_nameof
 *
 * Purpose:  Returns a pointer to the group hier. path for a dataset.
 *
 * Return:   Success:    Ptr to entry
 *           Failure:    NULL
 *-------------------------------------------------------------------------
 */
H5G_name_t *
H5D_nameof(H5D_t *dataset)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    FUNC_LEAVE_NOAPI(dataset ? &(dataset->path) : NULL)
} /* end H5D_nameof() */

/*-------------------------------------------------------------------------
 * Function: H5D__alloc_storage
 *
 * Purpose:  Allocate storage for the raw data of a dataset.
 *
 * Return:   Non-negative on success/Negative on failure
 *-------------------------------------------------------------------------
 */
herr_t
H5D__alloc_storage(H5D_t *dset, H5D_time_alloc_t time_alloc, bool full_overwrite, hsize_t old_dim[])
{
    H5F_t        *f;                         /* The dataset's file pointer */
    H5O_layout_t *layout;                    /* The dataset's layout information */
    bool          must_init_space = false;   /* Flag to indicate that space should be initialized */
    bool          addr_set        = false;   /* Flag to indicate that the dataset's storage address was set */
    herr_t        ret_value       = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(dset);
    f = dset->oloc.file;
    assert(f);

    /* If the data is stored in external files, don't set an address for the layout
     * We assume that external storage is already
     * allocated by the caller, or at least will be before I/O is performed.
     */
    if (!(0 == H5S_GET_EXTENT_NPOINTS(dset->shared->space) || dset->shared->dcpl_cache.efl.nused > 0)) {
        /* Get a pointer to the dataset's layout information */
        layout = &(dset->shared->layout);

        switch (layout->type) {
            case H5D_CONTIGUOUS:
                if (!(*dset->shared->layout.ops->is_space_alloc)(&dset->shared->layout.storage)) {
                    /* Check if we have a zero-sized dataset */
                    if (layout->storage.u.contig.size > 0) {
                        /* Reserve space in the file for the entire array */
                        if (H5D__contig_alloc(f, &layout->storage.u.contig /*out*/) < 0)
                            HGOTO_ERROR(H5E_IO, H5E_CANTINIT, FAIL,
                                        "unable to initialize contiguous storage");

                        /* Indicate that we should initialize storage space */
                        must_init_space = true;
                    } /* end if */
                    else
                        layout->storage.u.contig.addr = HADDR_UNDEF;

                    /* Indicate that we set the storage addr */
                    addr_set = true;
                } /* end if */
                break;

            case H5D_CHUNKED:
                if (!(*dset->shared->layout.ops->is_space_alloc)(&dset->shared->layout.storage)) {
                    /* Create the root of the index that manages chunked storage */
                    if (H5D__chunk_create(dset /*in,out*/) < 0)
                        HGOTO_ERROR(H5E_IO, H5E_CANTINIT, FAIL, "unable to initialize chunked storage");

                    /* Indicate that we set the storage addr */
                    addr_set = true;

                    /* Indicate that we should initialize storage space */
                    must_init_space = true;
                } /* end if */

                /* If space allocation is set to 'early' and we are extending
                 * the dataset, indicate that space should be allocated, so the
                 * index gets expanded. -QAK
                 */
                if (dset->shared->dcpl_cache.fill.alloc_time == H5D_ALLOC_TIME_EARLY &&
                    time_alloc == H5D_ALLOC_EXTEND)
                    must_init_space = true;
                break;

            case H5D_COMPACT:
                /* Check if space is already allocated */
                if (NULL == layout->storage.u.compact.buf) {
                    /* Reserve space in layout header message for the entire array.
                     * Starting from the 1.8.7 release, we allow dataspace to have
                     * zero dimension size.  So the storage size can be zero.
                     * SLU 2011/4/4 */
                    if (layout->storage.u.compact.size > 0) {
                        if (NULL ==
                            (layout->storage.u.compact.buf = H5MM_malloc(layout->storage.u.compact.size)))
                            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL,
                                        "unable to allocate memory for compact dataset");
                        if (!full_overwrite)
                            memset(layout->storage.u.compact.buf, 0, layout->storage.u.compact.size);
                        layout->storage.u.compact.dirty = true;

                        /* Indicate that we should initialize storage space */
                        must_init_space = true;
                    }
                    else {
                        layout->storage.u.compact.dirty = false;
                        must_init_space                 = false;
                    }
                } /* end if */
                break;

            case H5D_VIRTUAL:
                /* No-op, as the raw data is stored elsewhere and the global
                 * heap object containing the mapping information is created
                 * when the layout message is encoded.  We may wish to move the
                 * creation of the global heap object here at some point, but we
                 * will have to make sure is it always created before the
                 * dataset is closed. */
                break;

            case H5D_LAYOUT_ERROR:
            case H5D_NLAYOUTS:
            default:
                assert("not implemented yet" && 0);
#ifdef NDEBUG
                HGOTO_ERROR(H5E_IO, H5E_UNSUPPORTED, FAIL, "unsupported storage layout");
#endif                     /* NDEBUG */
        } /* end switch */ /*lint !e788 All appropriate cases are covered */

        /* Check if we need to initialize the space */
        if (must_init_space) {
            if (layout->type == H5D_CHUNKED) {
                /* If we are doing incremental allocation and the index got
                 * created during a H5Dwrite call, don't initialize the storage
                 * now, wait for the actual writes to each block and let the
                 * low-level chunking routines handle initialize the fill-values.
                 * Otherwise, pass along the space initialization call and let
                 * the low-level chunking routines sort out whether to write
                 * fill values to the chunks they allocate space for.  Yes,
                 * this is icky. -QAK
                 */
                if (!(dset->shared->dcpl_cache.fill.alloc_time == H5D_ALLOC_TIME_INCR &&
                      time_alloc == H5D_ALLOC_WRITE))
                    if (H5D__init_storage(dset, full_overwrite, old_dim) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL,
                                    "unable to initialize dataset with fill value");
            } /* end if */
            else {
                H5D_fill_value_t fill_status; /* The fill value status */

                /* Check the dataset's fill-value status */
                if (H5P_is_fill_value_defined(&dset->shared->dcpl_cache.fill, &fill_status) < 0)
                    HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't tell if fill value defined");

                /* If we are filling the dataset on allocation or "if set" and
                 * the fill value _is_ set, do that now */
                if (dset->shared->dcpl_cache.fill.fill_time == H5D_FILL_TIME_ALLOC ||
                    (dset->shared->dcpl_cache.fill.fill_time == H5D_FILL_TIME_IFSET &&
                     fill_status == H5D_FILL_VALUE_USER_DEFINED))
                    if (H5D__init_storage(dset, full_overwrite, old_dim) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL,
                                    "unable to initialize dataset with fill value");
            } /* end else */
        }     /* end if */

        /* If we set the address (and aren't in the middle of creating the
         *      dataset), mark the layout header message for later writing to
         *      the file.  (this improves forward compatibility).
         */
        /* (The layout message is already in the dataset's object header, this
         *      operation just sets the address and makes it constant)
         */
        if (time_alloc != H5D_ALLOC_CREATE && addr_set)
            /* Mark the layout as dirty, for later writing to the file */
            if (H5D__mark(dset, H5D_MARK_LAYOUT) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "unable to mark dataspace as dirty");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__alloc_storage() */

/*-------------------------------------------------------------------------
 * Function: H5D__init_storage
 *
 * Purpose:  Initialize the data for a new dataset.  If a selection is
 *           defined for SPACE then initialize only that part of the
 *           dataset.
 *
 * Return:   Non-negative on success/Negative on failure
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__init_storage(H5D_t *dset, bool full_overwrite, hsize_t old_dim[])
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(dset);

    switch (dset->shared->layout.type) {
        case H5D_COMPACT:
            /* If we will be immediately overwriting the values, don't bother to clear them */
            if (!full_overwrite) {
                /* Fill the compact dataset storage */
                if (H5D__compact_fill(dset) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL,
                                "unable to initialize compact dataset storage");
            } /* end if */
            break;

        case H5D_CONTIGUOUS:
            /* Don't write default fill values to external files */
            /* If we will be immediately overwriting the values, don't bother to clear them */
            if ((dset->shared->dcpl_cache.efl.nused == 0 || dset->shared->dcpl_cache.fill.buf) &&
                !full_overwrite)
                if (H5D__contig_fill(dset) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to allocate all chunks of dataset");
            break;

        case H5D_CHUNKED:
            /*
             * Allocate file space
             * for all chunks now and initialize each chunk with the fill value.
             */
            {
                hsize_t zero_dim[H5O_LAYOUT_NDIMS] = {0};

                /* Use zeros for old dimensions if not specified */
                if (old_dim == NULL)
                    old_dim = zero_dim;

                if (H5D__chunk_allocate(dset, full_overwrite, old_dim) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to allocate all chunks of dataset");
                break;
            } /* end block */

        case H5D_VIRTUAL:
            /* No-op, as the raw data is stored elsewhere */

        case H5D_LAYOUT_ERROR:
        case H5D_NLAYOUTS:
        default:
            assert("not implemented yet" && 0);
#ifdef NDEBUG
            HGOTO_ERROR(H5E_IO, H5E_UNSUPPORTED, FAIL, "unsupported storage layout");
#endif                 /* NDEBUG */
    } /* end switch */ /*lint !e788 All appropriate cases are covered */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__init_storage() */

/*-------------------------------------------------------------------------
 * Function: H5D__get_storage_size
 *
 * Purpose:  Determines how much space has been reserved to store the raw
 *           data of a dataset.
 *
 * Return:   Non-negative on success, negative on failure
 *-------------------------------------------------------------------------
 */
herr_t
H5D__get_storage_size(const H5D_t *dset, hsize_t *storage_size)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE_TAG(dset->oloc.addr)

    switch (dset->shared->layout.type) {
        case H5D_CHUNKED:
            if ((*dset->shared->layout.ops->is_space_alloc)(&dset->shared->layout.storage)) {
                if (H5D__chunk_allocated(dset, storage_size) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL,
                                "can't retrieve chunked dataset allocated size");
            } /* end if */
            else
                *storage_size = 0;
            break;

        case H5D_CONTIGUOUS:
            /* Datasets which are not allocated yet are using no space on disk */
            if ((*dset->shared->layout.ops->is_space_alloc)(&dset->shared->layout.storage))
                *storage_size = dset->shared->layout.storage.u.contig.size;
            else
                *storage_size = 0;
            break;

        case H5D_COMPACT:
            *storage_size = dset->shared->layout.storage.u.compact.size;
            break;

        case H5D_VIRTUAL:
            /* Just set to 0, as virtual datasets do not actually store raw data
             */
            *storage_size = 0;
            break;

        case H5D_LAYOUT_ERROR:
        case H5D_NLAYOUTS:
        default:
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataset type");
    } /*lint !e788 All appropriate cases are covered */

done:
    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5D__get_storage_size() */

/*-------------------------------------------------------------------------
 * Function:    H5D__get_offset
 *
 * Purpose:     Private function for H5Dget_offset().  Returns the address
 *              of dataset in file.
 *
 * Return:      Success:    The address of dataset
 *
 *              Failure:    HADDR_UNDEF (but also a valid value)
 *
 *-------------------------------------------------------------------------
 */
haddr_t
H5D__get_offset(const H5D_t *dset)
{
    haddr_t ret_value = HADDR_UNDEF;

    FUNC_ENTER_PACKAGE

    assert(dset);

    switch (dset->shared->layout.type) {
        case H5D_VIRTUAL:
        case H5D_CHUNKED:
        case H5D_COMPACT:
            break;

        case H5D_CONTIGUOUS:
            /* If dataspace hasn't been allocated or dataset is stored in
             * an external file, the value will be HADDR_UNDEF.
             */
            if (dset->shared->dcpl_cache.efl.nused == 0 ||
                H5_addr_defined(dset->shared->layout.storage.u.contig.addr))
                /* Return the absolute dataset offset from the beginning of file. */
                ret_value = dset->shared->layout.storage.u.contig.addr + H5F_BASE_ADDR(dset->oloc.file);
            break;

        case H5D_LAYOUT_ERROR:
        case H5D_NLAYOUTS:
        default:
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, HADDR_UNDEF, "unknown dataset layout type");
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__get_offset() */

/*-------------------------------------------------------------------------
 * Function: H5D__vlen_get_buf_size_alloc
 *
 * Purpose:  This routine makes certain there is enough space in the temporary
 *           buffer for the new data to read in.  All the VL data read in is actually
 *           placed in this buffer, overwriting the previous data.  Needless to say,
 *           this data is not actually usable.
 *
 * Return:   Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
static void *
H5D__vlen_get_buf_size_alloc(size_t size, void *info)
{
    H5D_vlen_bufsize_common_t *vlen_bufsize_com = (H5D_vlen_bufsize_common_t *)info;
    void                      *ret_value        = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check for increasing the size of the temporary space for VL data */
    if (size > vlen_bufsize_com->vl_tbuf_size) {
        if (NULL ==
            (vlen_bufsize_com->vl_tbuf = H5FL_BLK_REALLOC(vlen_vl_buf, vlen_bufsize_com->vl_tbuf, size)))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, NULL, "can't reallocate temporary VL data buffer");
        vlen_bufsize_com->vl_tbuf_size = size;
    } /* end if */

    /* Increment size of VL data buffer needed */
    vlen_bufsize_com->size += size;

    /* Set return value */
    ret_value = vlen_bufsize_com->vl_tbuf;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__vlen_get_buf_size_alloc() */

/*-------------------------------------------------------------------------
 * Function: H5D__vlen_get_buf_size_cb
 *
 * Purpose:  Dataspace selection iteration callback for H5Dvlen_get_buf_size.
 *
 * Return:   Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__vlen_get_buf_size_cb(void H5_ATTR_UNUSED *elem, hid_t type_id, unsigned H5_ATTR_UNUSED ndim,
                          const hsize_t *point, void *op_data)
{
    H5D_vlen_bufsize_native_t *vlen_bufsize = (H5D_vlen_bufsize_native_t *)op_data;
    H5D_dset_io_info_t         dset_info;                /* Internal multi-dataset info placeholder */
    herr_t                     ret_value = H5_ITER_CONT; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(H5I_DATATYPE == H5I_get_type(type_id));
    assert(point);
    assert(op_data);

    /* Select point to read in */
    if (H5S_select_elements(vlen_bufsize->fspace, H5S_SELECT_SET, (size_t)1, point) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTCREATE, H5_ITER_ERROR, "can't select point");

    {
        dset_info.dset        = vlen_bufsize->dset;
        dset_info.mem_space   = vlen_bufsize->mspace;
        dset_info.file_space  = vlen_bufsize->fspace;
        dset_info.buf.vp      = vlen_bufsize->common.fl_tbuf;
        dset_info.mem_type_id = type_id;

        /* Read in the point (with the custom VL memory allocator) */
        if (H5D__read(1, &dset_info) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "can't read data");
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__vlen_get_buf_size_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5D__vlen_get_buf_size
 *
 * Purpose: This routine checks the number of bytes required to store the VL
 *      data from the dataset, using the space_id for the selection in the
 *      dataset on disk and the type_id for the memory representation of the
 *      VL data, in memory.  The *size value is modified according to how many
 *      bytes are required to store the VL data in memory.
 *
 * Implementation: This routine actually performs the read with a custom
 *      memory manager which basically just counts the bytes requested and
 *      uses a temporary memory buffer (through the H5FL API) to make certain
 *      enough space is available to perform the read.  Then the temporary
 *      buffer is released and the number of bytes allocated is returned.
 *      Kinda kludgy, but easier than the other method of trying to figure out
 *      the sizes without actually reading the data in... - QAK
 *
 * Return:  Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__vlen_get_buf_size(H5D_t *dset, hid_t type_id, hid_t space_id, hsize_t *size)
{
    H5D_vlen_bufsize_native_t vlen_bufsize = {NULL, NULL, NULL, {NULL, NULL, 0, 0}};
    H5S_t                    *fspace       = NULL; /* Dataset's dataspace */
    H5S_t                    *mspace       = NULL; /* Memory dataspace */
    char                      bogus;               /* bogus value to pass to H5Diterate() */
    H5S_t                    *space;               /* Dataspace for iteration */
    H5T_t                    *type;                /* Datatype */
    H5S_sel_iter_op_t         dset_op;             /* Operator for iteration */
    herr_t                    ret_value = FAIL;    /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    if (NULL == (type = (H5T_t *)H5I_object(type_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not an valid base datatype");
    if (NULL == (space = (H5S_t *)H5I_object(space_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid dataspace");
    if (!(H5S_has_extent(space)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "dataspace does not have extent set");

    /* Save the dataset */
    vlen_bufsize.dset = dset;

    /* Get a copy of the dataset's dataspace */
    if (NULL == (fspace = H5S_copy(dset->shared->space, false, true)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "unable to get dataspace");
    vlen_bufsize.fspace = fspace;

    /* Create a scalar for the memory dataspace */
    if (NULL == (mspace = H5S_create(H5S_SCALAR)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCREATE, FAIL, "can't create dataspace");
    vlen_bufsize.mspace = mspace;

    /* Grab the temporary buffers required */
    if (NULL == (vlen_bufsize.common.fl_tbuf = H5FL_BLK_MALLOC(vlen_fl_buf, H5T_get_size(type))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "no temporary buffers available");
    if (NULL == (vlen_bufsize.common.vl_tbuf = H5FL_BLK_MALLOC(vlen_vl_buf, (size_t)1)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "no temporary buffers available");
    vlen_bufsize.common.vl_tbuf_size = 1;

    /* Set the memory manager to the special allocation routine */
    if (H5CX_set_vlen_alloc_info(H5D__vlen_get_buf_size_alloc, &vlen_bufsize.common, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "can't set VL data allocation routine");

    /* Set the initial number of bytes required */
    vlen_bufsize.common.size = 0;

    /* Call H5S_select_iterate with args, etc. */
    dset_op.op_type          = H5S_SEL_ITER_OP_APP;
    dset_op.u.app_op.op      = H5D__vlen_get_buf_size_cb;
    dset_op.u.app_op.type_id = type_id;

    ret_value = H5S_select_iterate(&bogus, type, space, &dset_op, &vlen_bufsize);

    /* Get the size if we succeeded */
    if (ret_value >= 0)
        *size = vlen_bufsize.common.size;

done:
    if (fspace && H5S_close(fspace) < 0)
        HDONE_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL, "unable to release dataspace");
    if (mspace && H5S_close(mspace) < 0)
        HDONE_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL, "unable to release dataspace");
    if (vlen_bufsize.common.fl_tbuf != NULL)
        vlen_bufsize.common.fl_tbuf = H5FL_BLK_FREE(vlen_fl_buf, vlen_bufsize.common.fl_tbuf);
    if (vlen_bufsize.common.vl_tbuf != NULL)
        vlen_bufsize.common.vl_tbuf = H5FL_BLK_FREE(vlen_vl_buf, vlen_bufsize.common.vl_tbuf);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__vlen_get_buf_size() */

/*-------------------------------------------------------------------------
 * Function: H5D__vlen_get_buf_size_gen_cb
 *
 * Purpose:  This routine checks the number of bytes required to store a single
 *           element from a dataset in memory, creating a selection with just the
 *           single element selected to read in the element and using a custom memory
 *           allocator for any VL data encountered.
 *           The *size value is modified according to how many bytes are
 *           required to store the element in memory.
 *
 * Implementation: This routine actually performs the read with a custom
 *      memory manager which basically just counts the bytes requested and
 *      uses a temporary memory buffer (through the H5FL API) to make certain
 *      enough space is available to perform the read.  Then the temporary
 *      buffer is released and the number of bytes allocated is returned.
 *      Kinda kludgy, but easier than the other method of trying to figure out
 *      the sizes without actually reading the data in... - QAK
 *
 * Return:   Non-negative on success, negative on failure
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__vlen_get_buf_size_gen_cb(void H5_ATTR_UNUSED *elem, hid_t type_id, unsigned H5_ATTR_UNUSED ndim,
                              const hsize_t *point, void *op_data)
{
    H5D_vlen_bufsize_generic_t *vlen_bufsize = (H5D_vlen_bufsize_generic_t *)op_data;
    H5T_t                      *dt;                  /* Datatype for operation */
    herr_t                      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(point);
    assert(op_data);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object(type_id)))
        HGOTO_ERROR(H5E_DATASET, H5E_BADTYPE, FAIL, "not a datatype");

    /* Make certain there is enough fixed-length buffer available */
    if (NULL == (vlen_bufsize->common.fl_tbuf =
                     H5FL_BLK_REALLOC(vlen_fl_buf, vlen_bufsize->common.fl_tbuf, H5T_get_size(dt))))
        HGOTO_ERROR(H5E_DATASET, H5E_NOSPACE, FAIL, "can't resize tbuf");

    /* Select point to read in */
    if (H5S_select_elements(vlen_bufsize->fspace, H5S_SELECT_SET, (size_t)1, point) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTCREATE, FAIL, "can't select point");

    /* Read in the point (with the custom VL memory allocator) */
    if (H5VL_dataset_read(1, &vlen_bufsize->dset_vol_obj, &type_id, &vlen_bufsize->mspace_id,
                          &vlen_bufsize->fspace_id, vlen_bufsize->dxpl_id, &vlen_bufsize->common.fl_tbuf,
                          H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "can't read point");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__vlen_get_buf_size_gen_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5D__vlen_get_buf_size_gen
 *
 * Purpose: Generic routine to checks the number of bytes required to store the
 *      VL data from the dataset.
 *
 * Return:  Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__vlen_get_buf_size_gen(H5VL_object_t *vol_obj, hid_t type_id, hid_t space_id, hsize_t *size)
{
    H5D_vlen_bufsize_generic_t vlen_bufsize = {
        NULL, H5I_INVALID_HID, NULL, H5I_INVALID_HID, H5I_INVALID_HID, {NULL, NULL, 0, 0}};
    H5P_genplist_t         *dxpl   = NULL;       /* DXPL for operation */
    H5S_t                  *mspace = NULL;       /* Memory dataspace */
    char                    bogus;               /* Bogus value to pass to H5Diterate() */
    H5S_t                  *space;               /* Dataspace for iteration */
    H5T_t                  *type;                /* Datatype */
    H5S_sel_iter_op_t       dset_op;             /* Operator for iteration */
    H5VL_dataset_get_args_t vol_cb_args;         /* Arguments to VOL callback */
    herr_t                  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    if (NULL == (type = (H5T_t *)H5I_object(type_id)))
        HGOTO_ERROR(H5E_DATASET, H5E_BADTYPE, FAIL, "not an valid datatype");
    if (NULL == (space = (H5S_t *)H5I_object(space_id)))
        HGOTO_ERROR(H5E_DATASET, H5E_BADTYPE, FAIL, "invalid dataspace");
    if (!(H5S_has_extent(space)))
        HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "dataspace does not have extent set");

    /* Save the dataset */
    vlen_bufsize.dset_vol_obj = (const H5VL_object_t *)vol_obj;

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                 = H5VL_DATASET_GET_SPACE;
    vol_cb_args.args.get_space.space_id = H5I_INVALID_HID;

    /* Get a copy of the dataset's dataspace */
    if (H5VL_dataset_get(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get dataspace");
    vlen_bufsize.fspace_id = vol_cb_args.args.get_space.space_id;
    if (NULL == (vlen_bufsize.fspace = (H5S_t *)H5I_object(vlen_bufsize.fspace_id)))
        HGOTO_ERROR(H5E_DATASET, H5E_BADTYPE, FAIL, "not a dataspace");

    /* Create a scalar for the memory dataspace */
    if (NULL == (mspace = H5S_create(H5S_SCALAR)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTCREATE, FAIL, "can't create dataspace");
    if ((vlen_bufsize.mspace_id = H5I_register(H5I_DATASPACE, mspace, true)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTREGISTER, FAIL, "unable to register dataspace ID");

    /* Grab the temporary buffers required */
    if (NULL == (vlen_bufsize.common.fl_tbuf = H5FL_BLK_MALLOC(vlen_fl_buf, H5T_get_size(type))))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "no temporary buffers available");
    if (NULL == (vlen_bufsize.common.vl_tbuf = H5FL_BLK_MALLOC(vlen_vl_buf, (size_t)1)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "no temporary buffers available");
    vlen_bufsize.common.vl_tbuf_size = 1;

    /* Set the VL allocation callbacks on a DXPL */
    if (NULL == (dxpl = (H5P_genplist_t *)H5I_object(H5P_DATASET_XFER_DEFAULT)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get default DXPL");
    if ((vlen_bufsize.dxpl_id = H5P_copy_plist(dxpl, true)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "can't copy property list");
    if (NULL == (dxpl = (H5P_genplist_t *)H5I_object(vlen_bufsize.dxpl_id)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get copied DXPL");
    if (H5P_set_vlen_mem_manager(dxpl, H5D__vlen_get_buf_size_alloc, &vlen_bufsize.common, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "can't set VL data allocation routine on DXPL");

    /* Set the initial number of bytes required */
    vlen_bufsize.common.size = 0;

    /* Call H5S_select_iterate with args, etc. */
    dset_op.op_type          = H5S_SEL_ITER_OP_APP;
    dset_op.u.app_op.op      = H5D__vlen_get_buf_size_gen_cb;
    dset_op.u.app_op.type_id = type_id;

    ret_value = H5S_select_iterate(&bogus, type, space, &dset_op, &vlen_bufsize);

    /* Get the size if we succeeded */
    if (ret_value >= 0)
        *size = vlen_bufsize.common.size;

done:
    if (vlen_bufsize.fspace_id >= 0) {
        if (H5I_dec_app_ref(vlen_bufsize.fspace_id) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "problem freeing id");
        vlen_bufsize.fspace = NULL;
    } /* end if */
    if (vlen_bufsize.fspace && H5S_close(vlen_bufsize.fspace) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTRELEASE, FAIL, "unable to release dataspace");
    if (vlen_bufsize.mspace_id >= 0) {
        if (H5I_dec_app_ref(vlen_bufsize.mspace_id) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "problem freeing id");
        mspace = NULL;
    } /* end if */
    if (mspace && H5S_close(mspace) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTRELEASE, FAIL, "unable to release dataspace");
    if (vlen_bufsize.common.fl_tbuf != NULL)
        vlen_bufsize.common.fl_tbuf = H5FL_BLK_FREE(vlen_fl_buf, vlen_bufsize.common.fl_tbuf);
    if (vlen_bufsize.common.vl_tbuf != NULL)
        vlen_bufsize.common.vl_tbuf = H5FL_BLK_FREE(vlen_vl_buf, vlen_bufsize.common.vl_tbuf);
    if (vlen_bufsize.dxpl_id != H5I_INVALID_HID) {
        if (H5I_dec_app_ref(vlen_bufsize.dxpl_id) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "can't close property list");
        dxpl = NULL;
    } /* end if */
    if (dxpl && H5P_close(dxpl) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTRELEASE, FAIL, "unable to release DXPL");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__vlen_get_buf_size_gen() */

/*-------------------------------------------------------------------------
 * Function: H5D__check_filters
 *
 * Purpose:  Check if the filters have be initialized for the dataset
 *
 * Return:   Non-negative on success/Negative on failure
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__check_filters(H5D_t *dataset)
{
    H5O_fill_t *fill;                /* Dataset's fill value */
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(dataset);

    /* Check if the filters in the DCPL will need to encode, and if so, can they?
     *
     * Filters need encoding if fill value is defined and a fill policy is set
     * that requires writing on an extend.
     */
    fill = &dataset->shared->dcpl_cache.fill;
    if (!dataset->shared->checked_filters) {
        H5D_fill_value_t fill_status; /* Whether the fill value is defined */

        /* Retrieve the "defined" status of the fill value */
        if (H5P_is_fill_value_defined(fill, &fill_status) < 0)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Couldn't retrieve fill value from dataset.");

        /* See if we can check the filter status */
        if (fill_status == H5D_FILL_VALUE_DEFAULT || fill_status == H5D_FILL_VALUE_USER_DEFINED) {
            if (fill->fill_time == H5D_FILL_TIME_ALLOC ||
                (fill->fill_time == H5D_FILL_TIME_IFSET && fill_status == H5D_FILL_VALUE_USER_DEFINED)) {
                /* Filters must have encoding enabled. Ensure that all filters can be applied */
                if (H5Z_can_apply(dataset->shared->dcpl_id, dataset->shared->type_id) < 0)
                    HGOTO_ERROR(H5E_PLINE, H5E_CANAPPLY, FAIL, "can't apply filters");

                dataset->shared->checked_filters = true;
            } /* end if */
        }     /* end if */
    }         /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__check_filters() */

/*-------------------------------------------------------------------------
 * Function: H5D__set_extent
 *
 * Purpose:  Based on H5D_extend, allows change to a lower dimension,
 *           calls H5S_set_extent and H5D__chunk_prune_by_extent instead
 *
 * Return:   Non-negative on success, negative on failure
 *-------------------------------------------------------------------------
 */
herr_t
H5D__set_extent(H5D_t *dset, const hsize_t *size)
{
    hsize_t  curr_dims[H5S_MAX_RANK]; /* Current dimension sizes */
    htri_t   changed;                 /* Whether the dataspace changed size */
    size_t   u, v;                    /* Local index variable */
    unsigned dim_idx;                 /* Dimension index */
    herr_t   ret_value = SUCCEED;     /* Return value */

    FUNC_ENTER_PACKAGE_TAG(dset->oloc.addr)

    /* Check args */
    assert(dset);
    assert(size);

    /* Check if we are allowed to modify this file */
    if (0 == (H5F_INTENT(dset->oloc.file) & H5F_ACC_RDWR))
        HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "no write intent on file");

    /* Check if we are allowed to modify the space; only datasets with chunked and external storage are
     * allowed to be modified */
    if (H5D_COMPACT == dset->shared->layout.type)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "dataset has compact storage");
    if (H5D_CONTIGUOUS == dset->shared->layout.type && 0 == dset->shared->dcpl_cache.efl.nused)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "dataset has contiguous storage");

    /* Check if the filters in the DCPL will need to encode, and if so, can they? */
    if (H5D__check_filters(dset) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't apply filters");

    /* Keep the current dataspace dimensions for later */
    HDcompile_assert(sizeof(curr_dims) == sizeof(dset->shared->curr_dims));
    H5MM_memcpy(curr_dims, dset->shared->curr_dims, H5S_MAX_RANK * sizeof(curr_dims[0]));

    /* Modify the size of the dataspace */
    if ((changed = H5S_set_extent(dset->shared->space, size)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to modify size of dataspace");

    /* Don't bother updating things, unless they've changed */
    if (changed) {
        bool shrink        = false; /* Flag to indicate a dimension has shrank */
        bool expand        = false; /* Flag to indicate a dimension has grown */
        bool update_chunks = false; /* Flag to indicate chunk cache update is needed */

        /* Determine if we are shrinking and/or expanding any dimensions */
        for (dim_idx = 0; dim_idx < dset->shared->ndims; dim_idx++) {
            /* Check for various status changes */
            if (size[dim_idx] < curr_dims[dim_idx])
                shrink = true;
            if (size[dim_idx] > curr_dims[dim_idx])
                expand = true;

            /* Chunked storage specific checks */
            if (H5D_CHUNKED == dset->shared->layout.type && dset->shared->ndims > 1) {
                hsize_t scaled; /* Scaled value */

                /* Compute the scaled dimension size value */
                if (dset->shared->layout.u.chunk.dim[dim_idx] == 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "chunk size must be > 0, dim = %u ",
                                dim_idx);

                scaled = size[dim_idx] / dset->shared->layout.u.chunk.dim[dim_idx];

                /* Check if scaled dimension size changed */
                if (scaled != dset->shared->cache.chunk.scaled_dims[dim_idx]) {
                    hsize_t scaled_power2up; /* Scaled value, rounded to next power of 2 */

                    /* Update the scaled dimension size value for the current dimension */
                    dset->shared->cache.chunk.scaled_dims[dim_idx] = scaled;

                    /* Check if algorithm for computing hash values will change */
                    if ((scaled > dset->shared->cache.chunk.nslots &&
                         dset->shared->cache.chunk.scaled_dims[dim_idx] <=
                             dset->shared->cache.chunk.nslots) ||
                        (scaled <= dset->shared->cache.chunk.nslots &&
                         dset->shared->cache.chunk.scaled_dims[dim_idx] > dset->shared->cache.chunk.nslots))
                        update_chunks = true;

                    if (!(scaled_power2up = H5VM_power2up(scaled)))
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "unable to get the next power of 2");

                    /* Check if the number of bits required to encode the scaled size value changed */
                    if (dset->shared->cache.chunk.scaled_power2up[dim_idx] != scaled_power2up) {
                        /* Update the 'power2up' & 'encode_bits' values for the current dimension */
                        dset->shared->cache.chunk.scaled_power2up[dim_idx] = scaled_power2up;
                        dset->shared->cache.chunk.scaled_encode_bits[dim_idx] =
                            H5VM_log2_gen(scaled_power2up);

                        /* Indicate that the cached chunk indices need to be updated */
                        update_chunks = true;
                    } /* end if */
                }     /* end if */
            }         /* end if */

            /* Update the cached copy of the dataset's dimensions */
            dset->shared->curr_dims[dim_idx] = size[dim_idx];
        } /* end for */

        /*-------------------------------------------------------------------------
         * Modify the dataset storage
         *-------------------------------------------------------------------------
         */
        /* Update the index values for the cached chunks for this dataset */
        if (H5D_CHUNKED == dset->shared->layout.type) {
            /* Set the cached chunk info */
            if (H5D__chunk_set_info(dset) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "unable to update # of chunks");

            /* Check if updating the chunk cache indices is necessary */
            if (update_chunks)
                /* Update the chunk cache indices */
                if (H5D__chunk_update_cache(dset) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "unable to update cached chunk indices");
        } /* end if */

        /* Operations for virtual datasets */
        if (H5D_VIRTUAL == dset->shared->layout.type) {
            /* Check that the dimensions of the VDS are large enough */
            if (H5D_virtual_check_min_dims(dset) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                            "virtual dataset dimensions not large enough to contain all limited dimensions "
                            "in all selections");

            /* Patch the virtual selection dataspaces */
            for (u = 0; u < dset->shared->layout.storage.u.virt.list_nused; u++) {
                /* Patch extent */
                if (H5S_set_extent(dset->shared->layout.storage.u.virt.list[u].source_dset.virtual_select,
                                   size) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to modify size of dataspace");
                dset->shared->layout.storage.u.virt.list[u].virtual_space_status = H5O_VIRTUAL_STATUS_CORRECT;

                /* Patch sub-source datasets */
                for (v = 0; v < dset->shared->layout.storage.u.virt.list[u].sub_dset_nused; v++)
                    if (H5S_set_extent(dset->shared->layout.storage.u.virt.list[u].sub_dset[v].virtual_select,
                                       size) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to modify size of dataspace");
            } /* end for */

            /* Mark virtual datasets as not fully initialized so internal
             * selections are recalculated (at next I/O operation) */
            dset->shared->layout.storage.u.virt.init = false;
        } /* end if */

        /* Allocate space for the new parts of the dataset, if appropriate */
        if (expand && dset->shared->dcpl_cache.fill.alloc_time == H5D_ALLOC_TIME_EARLY)
            if (H5D__alloc_storage(dset, H5D_ALLOC_EXTEND, false, curr_dims) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to extend dataset storage");

        /*-------------------------------------------------------------------------
         * Remove chunk information in the case of chunked datasets
         * This removal takes place only in case we are shrinking the dataset
         * and if the chunks are written
         *-------------------------------------------------------------------------
         */
        if (H5D_CHUNKED == dset->shared->layout.type) {
            if (shrink && ((*dset->shared->layout.ops->is_space_alloc)(&dset->shared->layout.storage) ||
                           (dset->shared->layout.ops->is_data_cached &&
                            (*dset->shared->layout.ops->is_data_cached)(dset->shared))))
                /* Remove excess chunks */
                if (H5D__chunk_prune_by_extent(dset, curr_dims) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "unable to remove chunks");

            /* Update chunks that are no longer edge chunks as a result of
             * expansion */
            if (expand &&
                (dset->shared->layout.u.chunk.flags & H5O_LAYOUT_CHUNK_DONT_FILTER_PARTIAL_BOUND_CHUNKS) &&
                (dset->shared->dcpl_cache.pline.nused > 0))
                if (H5D__chunk_update_old_edge_chunks(dset, curr_dims) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "unable to do update old edge chunks");
        } /* end if */

        /* Mark the dataspace as dirty, for later writing to the file */
        if (H5D__mark(dset, H5D_MARK_SPACE) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "unable to mark dataspace as dirty");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5D__set_extent() */

/*-------------------------------------------------------------------------
 * Function: H5D__flush_sieve_buf
 *
 * Purpose:  Flush any dataset sieve buffer info cached in memory
 *
 * Return:   Success:    Non-negative
 *           Failure:    Negative
 *-------------------------------------------------------------------------
 */
herr_t
H5D__flush_sieve_buf(H5D_t *dataset)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(dataset);

    /* Flush the raw data buffer, if we have a dirty one */
    if (dataset->shared->cache.contig.sieve_buf && dataset->shared->cache.contig.sieve_dirty) {
        assert(dataset->shared->layout.type !=
               H5D_COMPACT); /* We should never have a sieve buffer for compact storage */

        /* Write dirty data sieve buffer to file */
        if (H5F_shared_block_write(
                H5F_SHARED(dataset->oloc.file), H5FD_MEM_DRAW, dataset->shared->cache.contig.sieve_loc,
                dataset->shared->cache.contig.sieve_size, dataset->shared->cache.contig.sieve_buf) < 0)
            HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "block write failed");

        /* Reset sieve buffer dirty flag */
        dataset->shared->cache.contig.sieve_dirty = false;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__flush_sieve_buf() */

/*-------------------------------------------------------------------------
 * Function: H5D__flush_real
 *
 * Purpose:  Flush any dataset information cached in memory
 *
 * Return:   Success:    Non-negative
 *           Failure:    Negative
 *-------------------------------------------------------------------------
 */
herr_t
H5D__flush_real(H5D_t *dataset)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE_TAG(dataset->oloc.addr)

    /* Check args */
    assert(dataset);
    assert(dataset->shared);

    /* Avoid flushing the dataset (again) if it's closing */
    if (!dataset->shared->closing)
        /* Flush cached raw data for each kind of dataset layout */
        if (dataset->shared->layout.ops->flush && (dataset->shared->layout.ops->flush)(dataset) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTFLUSH, FAIL, "unable to flush raw data");

done:
    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5D__flush_real() */

/*-------------------------------------------------------------------------
 * Function: H5D__flush
 *
 * Purpose:  Flush dataset information cached in memory
 *
 * Return:   Success:    Non-negative
 *           Failure:    Negative
 *-------------------------------------------------------------------------
 */
herr_t
H5D__flush(H5D_t *dset, hid_t dset_id)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(dset);
    assert(dset->shared);

    /* Currently, H5Oflush causes H5Fclose to trigger an assertion failure in metadata cache.
     * Leave this situation for the future solution */
    if (H5F_HAS_FEATURE(dset->oloc.file, H5FD_FEAT_HAS_MPI))
        HGOTO_ERROR(H5E_DATASET, H5E_UNSUPPORTED, FAIL, "H5Oflush isn't supported for parallel");

    /* Flush any dataset information still cached in memory */
    if (H5D__flush_real(dset) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTFLUSH, FAIL, "unable to flush cached dataset info");

    /* Flush object's metadata to file */
    if (H5O_flush_common(&dset->oloc, dset_id) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTFLUSH, FAIL, "unable to flush dataset and object flush callback");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__flush() */

/*-------------------------------------------------------------------------
 * Function: H5D__format_convert
 *
 * Purpose:  For chunked: downgrade the chunk indexing type to version 1 B-tree
 *           For compact/contiguous: downgrade layout version to 3
 *
 * Return:   Success:    Non-negative
 *           Failure:    Negative
 *-------------------------------------------------------------------------
 */
herr_t
H5D__format_convert(H5D_t *dataset)
{
    H5D_chk_idx_info_t new_idx_info;                /* Index info for the new layout */
    H5D_chk_idx_info_t idx_info;                    /* Index info for the current layout */
    H5O_layout_t      *newlayout         = NULL;    /* The new layout */
    bool               init_new_index    = false;   /* Indicate that the new chunk index is initialized */
    bool               delete_old_layout = false;   /* Indicate that the old layout message is deleted */
    bool               add_new_layout    = false;   /* Indicate that the new layout message is added */
    herr_t             ret_value         = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE_TAG(dataset->oloc.addr)

    /* Check args */
    assert(dataset);

    switch (dataset->shared->layout.type) {
        case H5D_CHUNKED:
            assert(dataset->shared->layout.u.chunk.idx_type != H5D_CHUNK_IDX_BTREE);

            if (NULL == (newlayout = (H5O_layout_t *)H5MM_calloc(sizeof(H5O_layout_t))))
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "unable to allocate buffer");

            /* Set up the current index info */
            idx_info.f       = dataset->oloc.file;
            idx_info.pline   = &dataset->shared->dcpl_cache.pline;
            idx_info.layout  = &dataset->shared->layout.u.chunk;
            idx_info.storage = &dataset->shared->layout.storage.u.chunk;

            /* Copy the current layout info to the new layout */
            H5MM_memcpy(newlayout, &dataset->shared->layout, sizeof(H5O_layout_t));

            /* Set up info for version 1 B-tree in the new layout */
            newlayout->version                        = H5O_LAYOUT_VERSION_3;
            newlayout->storage.u.chunk.idx_type       = H5D_CHUNK_IDX_BTREE;
            newlayout->storage.u.chunk.idx_addr       = HADDR_UNDEF;
            newlayout->storage.u.chunk.ops            = H5D_COPS_BTREE;
            newlayout->storage.u.chunk.u.btree.shared = NULL;

            /* Set up the index info to version 1 B-tree */
            new_idx_info.f       = dataset->oloc.file;
            new_idx_info.pline   = &dataset->shared->dcpl_cache.pline;
            new_idx_info.layout  = &(newlayout->u).chunk;
            new_idx_info.storage = &(newlayout->storage).u.chunk;

            /* Initialize version 1 B-tree */
            if (new_idx_info.storage->ops->init &&
                (new_idx_info.storage->ops->init)(&new_idx_info, dataset->shared->space, dataset->oloc.addr) <
                    0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't initialize indexing information");
            init_new_index = true;

            /* If the current chunk index exists */
            if (H5_addr_defined(idx_info.storage->idx_addr)) {

                /* Create v1 B-tree chunk index */
                if ((new_idx_info.storage->ops->create)(&new_idx_info) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create chunk index");

                /* Iterate over the chunks in the current index and insert the chunk addresses
                 * into the version 1 B-tree chunk index
                 */
                if (H5D__chunk_format_convert(dataset, &idx_info, &new_idx_info) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_BADITER, FAIL, "unable to iterate/convert chunk index");
            } /* end if */

            /* Delete the old "current" layout message */
            if (H5O_msg_remove(&dataset->oloc, H5O_LAYOUT_ID, H5O_ALL, false) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_CANTDELETE, FAIL, "unable to delete layout message");

            delete_old_layout = true;

            /* Append the new layout message to the object header */
            if (H5O_msg_create(&dataset->oloc, H5O_LAYOUT_ID, 0, H5O_UPDATE_TIME, newlayout) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to update layout header message");

            add_new_layout = true;

            /* Release the old (current) chunk index */
            if (idx_info.storage->ops->dest && (idx_info.storage->ops->dest)(&idx_info) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "unable to release chunk index info");

            /* Copy the new layout to the dataset's layout */
            H5MM_memcpy(&dataset->shared->layout, newlayout, sizeof(H5O_layout_t));

            break;

        case H5D_CONTIGUOUS:
        case H5D_COMPACT:
            assert(dataset->shared->layout.version > H5O_LAYOUT_VERSION_DEFAULT);
            dataset->shared->layout.version = H5O_LAYOUT_VERSION_DEFAULT;
            if (H5O_msg_write(&(dataset->oloc), H5O_LAYOUT_ID, 0, H5O_UPDATE_TIME,
                              &(dataset->shared->layout)) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "unable to update layout message");
            break;

        case H5D_VIRTUAL:
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "virtual dataset layout not supported");

        case H5D_LAYOUT_ERROR:
        case H5D_NLAYOUTS:
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid dataset layout type");

        default:
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "unknown dataset layout type");
    } /* end switch */

done:
    if (ret_value < 0 && dataset->shared->layout.type == H5D_CHUNKED) {
        /* Remove new layout message */
        if (add_new_layout)
            if (H5O_msg_remove(&dataset->oloc, H5O_LAYOUT_ID, H5O_ALL, false) < 0)
                HDONE_ERROR(H5E_SYM, H5E_CANTDELETE, FAIL, "unable to delete layout message");

        /* Add back old layout message */
        if (delete_old_layout)
            if (H5O_msg_create(&dataset->oloc, H5O_LAYOUT_ID, 0, H5O_UPDATE_TIME, &dataset->shared->layout) <
                0)
                HDONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to add layout header message");

        /* Clean up v1 b-tree chunk index */
        if (init_new_index) {
            if (H5_addr_defined(new_idx_info.storage->idx_addr)) {
                /* Check for valid address i.e. tag */
                if (!H5_addr_defined(dataset->oloc.addr))
                    HDONE_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "address undefined");

                /* Expunge from cache all v1 B-tree type entries associated with tag */
                if (H5AC_expunge_tag_type_metadata(dataset->oloc.file, dataset->oloc.addr, H5AC_BT_ID,
                                                   H5AC__NO_FLAGS_SET))
                    HDONE_ERROR(H5E_DATASET, H5E_CANTEXPUNGE, FAIL, "unable to expunge index metadata");
            } /* end if */

            /* Delete v1 B-tree chunk index */
            if (new_idx_info.storage->ops->dest && (new_idx_info.storage->ops->dest)(&new_idx_info) < 0)
                HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "unable to release chunk index info");
        } /* end if */
    }     /* end if */

    if (newlayout != NULL)
        newlayout = (H5O_layout_t *)H5MM_xfree(newlayout);

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5D__format_convert() */

/*-------------------------------------------------------------------------
 * Function: H5D__mark
 *
 * Purpose:  Mark some aspect of a dataset as dirty
 *
 * Return:   Success:    Non-negative
 *           Failure:    Negative
 *-------------------------------------------------------------------------
 */
herr_t
H5D__mark(const H5D_t *dataset, unsigned flags)
{
    H5O_t *oh        = NULL;    /* Pointer to dataset's object header */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(dataset);
    assert(!(flags & (unsigned)~(H5D_MARK_SPACE | H5D_MARK_LAYOUT)));

    /* Mark aspects of the dataset as dirty */
    if (flags) {
        unsigned update_flags = H5O_UPDATE_TIME; /* Modification time flag */

        /* Pin the object header */
        if (NULL == (oh = H5O_pin(&dataset->oloc)))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTPIN, FAIL, "unable to pin dataset object header");

        /* Update the layout on disk, if it's been changed */
        if (flags & H5D_MARK_LAYOUT) {
            if (H5D__layout_oh_write(dataset, oh, update_flags) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "unable to update layout info");

            /* Reset the "update the modification time" flag, so we only do it once */
            update_flags = 0;
        } /* end if */

        /* Update the dataspace on disk, if it's been changed */
        if (flags & H5D_MARK_SPACE) {
            if (H5S_write(dataset->oloc.file, oh, update_flags, dataset->shared->space) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "unable to update file with new dataspace");

            /* Reset the "update the modification time" flag, so we only do it once */
            update_flags = 0;
        } /* end if */

        /* _Somebody_ should have update the modification time! */
        assert(update_flags == 0);
    } /* end if */

done:
    /* Release pointer to object header */
    if (oh != NULL)
        if (H5O_unpin(oh) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTUNPIN, FAIL, "unable to unpin dataset object header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__mark() */

/*-------------------------------------------------------------------------
 * Function: H5D__flush_all_cb
 *
 * Purpose:  Flush any dataset information cached in memory
 *
 * Return:   Success:    Non-negative
 *           Failure:    Negative
 *-------------------------------------------------------------------------
 */
static int
H5D__flush_all_cb(void *_dataset, hid_t H5_ATTR_UNUSED id, void *_udata)
{
    H5D_t *dataset   = (H5D_t *)_dataset; /* Dataset pointer */
    H5F_t *f         = (H5F_t *)_udata;   /* User data for callback */
    int    ret_value = H5_ITER_CONT;      /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(dataset);
    assert(f);

    /* Check for dataset in same file */
    if (f == dataset->oloc.file)
        /* Flush the dataset's information */
        if (H5D__flush_real(dataset) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, H5_ITER_ERROR, "unable to flush cached dataset info");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__flush_all_cb() */

/*-------------------------------------------------------------------------
 * Function: H5D_flush_all
 *
 * Purpose:  Flush any dataset information cached in memory
 *
 * Return:   Success:    Non-negative
 *           Failure:    Negative
 *-------------------------------------------------------------------------
 */
herr_t
H5D_flush_all(H5F_t *f)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args */
    assert(f);

    /* Iterate over all the open datasets */
    if (H5I_iterate(H5I_DATASET, H5D__flush_all_cb, f, false) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_BADITER, FAIL, "unable to flush cached dataset info");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D_flush_all() */

/*-------------------------------------------------------------------------
 * Function: H5D_get_create_plist
 *
 * Purpose:  Private function for H5Dget_create_plist
 *
 * Return:   Success:    ID for a copy of the dataset creation
 *                property list.  The template should be
 *                released by calling H5P_close().
 *           Failure:    FAIL
 *-------------------------------------------------------------------------
 */
hid_t
H5D_get_create_plist(const H5D_t *dset)
{
    H5P_genplist_t *dcpl_plist;        /* Dataset's DCPL */
    H5P_genplist_t *new_plist;         /* Copy of dataset's DCPL */
    H5O_layout_t    copied_layout;     /* Layout to tweak */
    H5O_fill_t      copied_fill = {0}; /* Fill value to tweak */
    H5O_efl_t       copied_efl;        /* External file list to tweak */
    hid_t           new_dcpl_id = FAIL;
    hid_t           ret_value   = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args */
    if (NULL == (dcpl_plist = (H5P_genplist_t *)H5I_object(dset->shared->dcpl_id)))
        HGOTO_ERROR(H5E_DATASET, H5E_BADTYPE, FAIL, "can't get property list");

    /* Copy the creation property list */
    if ((new_dcpl_id = H5P_copy_plist(dcpl_plist, true)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "unable to copy the creation property list");
    if (NULL == (new_plist = (H5P_genplist_t *)H5I_object(new_dcpl_id)))
        HGOTO_ERROR(H5E_DATASET, H5E_BADTYPE, FAIL, "can't get property list");

    /* Retrieve any object creation properties */
    if (H5O_get_create_plist(&dset->oloc, new_plist) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get object creation info");

    /* Get the layout property */
    if (H5P_peek(new_plist, H5D_CRT_LAYOUT_NAME, &copied_layout) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get layout");

    /* Reset layout values set when dataset is created */
    copied_layout.ops = NULL;
    switch (copied_layout.type) {
        case H5D_COMPACT:
            copied_layout.storage.u.compact.buf = H5MM_xfree(copied_layout.storage.u.compact.buf);
            memset(&copied_layout.storage.u.compact, 0, sizeof(copied_layout.storage.u.compact));
            break;

        case H5D_CONTIGUOUS:
            copied_layout.storage.u.contig.addr = HADDR_UNDEF;
            copied_layout.storage.u.contig.size = 0;
            break;

        case H5D_CHUNKED:
            /* Reset chunk size */
            copied_layout.u.chunk.size = 0;

            /* Reset index info, if the chunk ops are set */
            if (copied_layout.storage.u.chunk.ops)
                /* Reset address and pointer of the array struct for the chunked storage index */
                if (H5D_chunk_idx_reset(&copied_layout.storage.u.chunk, true) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL,
                                "unable to reset chunked storage index in dest");

            /* Reset chunk index ops */
            copied_layout.storage.u.chunk.ops = NULL;
            break;

        case H5D_VIRTUAL:
            copied_layout.storage.u.virt.serial_list_hobjid.addr = HADDR_UNDEF;
            copied_layout.storage.u.virt.serial_list_hobjid.idx  = 0;
            break;

        case H5D_LAYOUT_ERROR:
        case H5D_NLAYOUTS:
        default:
            assert(0 && "Unknown layout type!");
    } /* end switch */

    /* Set back the (possibly modified) layout property to property list */
    if (H5P_poke(new_plist, H5D_CRT_LAYOUT_NAME, &copied_layout) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "unable to set layout");

    /* Get the fill value property */
    if (H5P_peek(new_plist, H5D_CRT_FILL_VALUE_NAME, &copied_fill) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get fill value");

    /* Check if there is a fill value, but no type yet */
    if (copied_fill.buf != NULL && copied_fill.type == NULL) {
        H5T_path_t *tpath; /* Conversion information*/

        /* Copy the dataset type into the fill value message */
        if (NULL == (copied_fill.type = H5T_copy(dset->shared->type, H5T_COPY_TRANSIENT)))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to copy dataset datatype for fill value");

        /* Set up type conversion function */
        if (NULL == (tpath = H5T_path_find(dset->shared->type, copied_fill.type)))
            HGOTO_ERROR(H5E_DATASET, H5E_UNSUPPORTED, FAIL,
                        "unable to convert between src and dest data types");

        /* Convert disk form of fill value into memory form */
        if (!H5T_path_noop(tpath)) {
            hid_t    dst_id, src_id; /* Source & destination datatypes for type conversion */
            uint8_t *bkg_buf = NULL; /* Background conversion buffer */
            size_t   bkg_size;       /* Size of background buffer */

            /* Wrap copies of types to convert */
            dst_id = H5I_register(H5I_DATATYPE, H5T_copy(copied_fill.type, H5T_COPY_TRANSIENT), false);
            if (dst_id < 0)
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to copy/register datatype");
            src_id = H5I_register(H5I_DATATYPE, H5T_copy(dset->shared->type, H5T_COPY_ALL), false);
            if (src_id < 0) {
                H5I_dec_ref(dst_id);
                HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to copy/register datatype");
            } /* end if */

            /* Allocate a background buffer */
            bkg_size = MAX(H5T_GET_SIZE(copied_fill.type), H5T_GET_SIZE(dset->shared->type));
            if (H5T_path_bkg(tpath) && NULL == (bkg_buf = H5FL_BLK_CALLOC(type_conv, bkg_size))) {
                H5I_dec_ref(src_id);
                H5I_dec_ref(dst_id);
                HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "memory allocation failed");
            } /* end if */

            /* Convert fill value */
            if (H5T_convert(tpath, src_id, dst_id, (size_t)1, (size_t)0, (size_t)0, copied_fill.buf,
                            bkg_buf) < 0) {
                H5I_dec_ref(src_id);
                H5I_dec_ref(dst_id);
                if (bkg_buf)
                    bkg_buf = H5FL_BLK_FREE(type_conv, bkg_buf);
                HGOTO_ERROR(H5E_DATASET, H5E_CANTCONVERT, FAIL, "datatype conversion failed");
            } /* end if */

            /* Release local resources */
            if (H5I_dec_ref(src_id) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "unable to close temporary object");
            if (H5I_dec_ref(dst_id) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "unable to close temporary object");
            if (bkg_buf)
                bkg_buf = H5FL_BLK_FREE(type_conv, bkg_buf);
        } /* end if */
    }     /* end if */

    /* Set back the (possibly modified) fill value property to property list */
    if (H5P_poke(new_plist, H5D_CRT_FILL_VALUE_NAME, &copied_fill) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "unable to set fill value");

    /* Get the fill value property */
    if (H5P_peek(new_plist, H5D_CRT_EXT_FILE_LIST_NAME, &copied_efl) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get external file list");

    /* Reset efl name_offset and heap_addr, these are the values when the dataset is created */
    if (copied_efl.slot) {
        unsigned u;

        copied_efl.heap_addr = HADDR_UNDEF;
        for (u = 0; u < copied_efl.nused; u++)
            copied_efl.slot[u].name_offset = 0;
    } /* end if */

    /* Set back the (possibly modified) external file list property to property list */
    if (H5P_poke(new_plist, H5D_CRT_EXT_FILE_LIST_NAME, &copied_efl) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "unable to set external file list");

    /* Set the return value */
    ret_value = new_dcpl_id;

done:
    if (ret_value < 0) {
        if (new_dcpl_id > 0)
            if (H5I_dec_app_ref(new_dcpl_id) < 0)
                HDONE_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "unable to close temporary object");

        if (copied_fill.type && (H5T_close_real(copied_fill.type) < 0))
            HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "Can't free temporary datatype");
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D_get_create_plist() */

/*-------------------------------------------------------------------------
 * Function: H5D_get_access_plist
 *
 * Purpose:  Returns a copy of the dataset access property list.
 *
 * Return:   Success:    ID for a copy of the dataset access
 *                       property list.
 *           Failure:    FAIL
 *-------------------------------------------------------------------------
 */
hid_t
H5D_get_access_plist(const H5D_t *dset)
{
    H5P_genplist_t    *old_plist;                    /* Stored DAPL from dset */
    H5P_genplist_t    *new_plist;                    /* New DAPL */
    H5P_genplist_t    *def_dapl              = NULL; /* Default DAPL */
    H5D_append_flush_t def_append_flush_info = {0};  /* Default append flush property */
    H5D_rdcc_t         def_chunk_info;               /* Default chunk cache property */
    H5D_vds_view_t     def_vds_view;                 /* Default virtual view property */
    hsize_t            def_vds_gap;                  /* Default virtual printf gap property */
    hid_t              new_dapl_id = FAIL;
    hid_t              ret_value   = FAIL;

    FUNC_ENTER_NOAPI_NOINIT

    /* Make a copy of the dataset's dataset access property list */
    if (NULL == (old_plist = (H5P_genplist_t *)H5I_object(dset->shared->dapl_id)))
        HGOTO_ERROR(H5E_DATASET, H5E_BADTYPE, FAIL, "can't get property list");
    if ((new_dapl_id = H5P_copy_plist(old_plist, true)) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINIT, FAIL, "can't copy dataset access property list");
    if (NULL == (new_plist = (H5P_genplist_t *)H5I_object(new_dapl_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a property list");

    /* If the dataset is chunked then copy the rdcc & append flush parameters.
     * Otherwise, use the default values. */
    if (dset->shared->layout.type == H5D_CHUNKED) {
        if (H5P_set(new_plist, H5D_ACS_DATA_CACHE_NUM_SLOTS_NAME, &(dset->shared->cache.chunk.nslots)) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set data cache number of slots");
        if (H5P_set(new_plist, H5D_ACS_DATA_CACHE_BYTE_SIZE_NAME, &(dset->shared->cache.chunk.nbytes_max)) <
            0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set data cache byte size");
        if (H5P_set(new_plist, H5D_ACS_PREEMPT_READ_CHUNKS_NAME, &(dset->shared->cache.chunk.w0)) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set preempt read chunks");
        if (H5P_set(new_plist, H5D_ACS_APPEND_FLUSH_NAME, &dset->shared->append_flush) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set append flush property");
    }
    else {
        /* Get the default FAPL */
        if (NULL == (def_dapl = (H5P_genplist_t *)H5I_object(H5P_LST_DATASET_ACCESS_ID_g)))
            HGOTO_ERROR(H5E_DATASET, H5E_BADTYPE, FAIL, "not a property list");

        /* Set the data cache number of slots to the value of the default FAPL */
        if (H5P_get(def_dapl, H5D_ACS_DATA_CACHE_NUM_SLOTS_NAME, &def_chunk_info.nslots) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get data number of slots");
        if (H5P_set(new_plist, H5D_ACS_DATA_CACHE_NUM_SLOTS_NAME, &def_chunk_info.nslots) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "can't set data cache number of slots");

        /* Set the data cache byte size to the value of the default FAPL */
        if (H5P_get(def_dapl, H5D_ACS_DATA_CACHE_BYTE_SIZE_NAME, &def_chunk_info.nbytes_max) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get data cache byte size");
        if (H5P_set(new_plist, H5D_ACS_DATA_CACHE_BYTE_SIZE_NAME, &def_chunk_info.nbytes_max) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "can't set data cache byte size");

        /* Set the preempt read chunks property to the value of the default FAPL */
        if (H5P_get(def_dapl, H5D_ACS_PREEMPT_READ_CHUNKS_NAME, &def_chunk_info.w0) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get preempt read chunks");
        if (H5P_set(new_plist, H5D_ACS_PREEMPT_READ_CHUNKS_NAME, &def_chunk_info.w0) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "can't set preempt read chunks");

        /* Set the append flush property to its default value */
        if (H5P_set(new_plist, H5D_ACS_APPEND_FLUSH_NAME, &def_append_flush_info) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "can't set append flush property");
    } /* end if-else */

    /* If the dataset is virtual then copy the VDS view & printf gap options.
     * Otherwise, use the default values. */
    if (dset->shared->layout.type == H5D_VIRTUAL) {
        if (H5P_set(new_plist, H5D_ACS_VDS_VIEW_NAME, &(dset->shared->layout.storage.u.virt.view)) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set VDS view");
        if (H5P_set(new_plist, H5D_ACS_VDS_PRINTF_GAP_NAME,
                    &(dset->shared->layout.storage.u.virt.printf_gap)) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set VDS printf gap");
    }
    else {
        /* Get the default FAPL if necessary */
        if (!def_dapl && NULL == (def_dapl = (H5P_genplist_t *)H5I_object(H5P_LST_DATASET_ACCESS_ID_g)))
            HGOTO_ERROR(H5E_DATASET, H5E_BADTYPE, FAIL, "not a property list");

        /* Set the data cache number of slots to the value of the default FAPL */
        if (H5P_get(def_dapl, H5D_ACS_VDS_VIEW_NAME, &def_vds_view) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get VDS view");
        if (H5P_set(new_plist, H5D_ACS_VDS_VIEW_NAME, &def_vds_view) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "can't set VDS view");

        /* Set the data cache byte size to the value of the default FAPL */
        if (H5P_get(def_dapl, H5D_ACS_VDS_PRINTF_GAP_NAME, &def_vds_gap) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get VDS printf gap");
        if (H5P_set(new_plist, H5D_ACS_VDS_PRINTF_GAP_NAME, &def_vds_gap) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "can't set VDS printf gap");
    }

    /* Set the vds prefix option */
    if (H5P_set(new_plist, H5D_ACS_VDS_PREFIX_NAME, &(dset->shared->vds_prefix)) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set vds prefix");

    /* Set the external file prefix option */
    if (H5P_set(new_plist, H5D_ACS_EFILE_PREFIX_NAME, &(dset->shared->extfile_prefix)) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set external file prefix");

    /* Set the return value */
    ret_value = new_dapl_id;

done:
    if (ret_value < 0)
        if (new_dapl_id > 0)
            if (H5I_dec_app_ref(new_dapl_id) < 0)
                HDONE_ERROR(H5E_SYM, H5E_CANTDEC, FAIL, "can't free");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D_get_access_plist() */

/*-------------------------------------------------------------------------
 * Function: H5D__get_space
 *
 * Purpose:  Returns and ID for the dataspace of the dataset.
 *
 * Return:   Success:    ID for dataspace
 *           Failure:    FAIL
 *-------------------------------------------------------------------------
 */
hid_t
H5D__get_space(const H5D_t *dset)
{
    H5S_t *space     = NULL;
    hid_t  ret_value = H5I_INVALID_HID;

    FUNC_ENTER_PACKAGE

    /* If the layout is virtual, update the extent */
    if (dset->shared->layout.type == H5D_VIRTUAL)
        if (H5D__virtual_set_extent_unlim(dset) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to update virtual dataset extent");

    /* Read the dataspace message and return a dataspace object */
    if (NULL == (space = H5S_copy(dset->shared->space, false, true)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to get dataspace");

    /* Create an ID */
    if ((ret_value = H5I_register(H5I_DATASPACE, space, true)) < 0)
        HGOTO_ERROR(H5E_ID, H5E_CANTREGISTER, FAIL, "unable to register dataspace");

done:
    if (ret_value < 0)
        if (space != NULL)
            if (H5S_close(space) < 0)
                HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "unable to release dataspace");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__get_space() */

/*-------------------------------------------------------------------------
 * Function: H5D__get_type
 *
 * Purpose:  Returns and ID for the datatype of the dataset.
 *
 * Return:   Success:    ID for datatype
 *           Failure:    FAIL
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5D__get_type(const H5D_t *dset)
{
    H5T_t *dt        = NULL;
    hid_t  ret_value = FAIL;

    FUNC_ENTER_PACKAGE

    /* Patch the datatype's "top level" file pointer */
    if (H5T_patch_file(dset->shared->type, dset->oloc.file) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to patch datatype's file pointer");

    /* Copy the dataset's datatype */
    if (NULL == (dt = H5T_copy_reopen(dset->shared->type)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to copy datatype");

    /* Mark any datatypes as being in memory now */
    if (H5T_set_loc(dt, NULL, H5T_LOC_MEMORY) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "invalid datatype location");

    /* Lock copied type */
    if (H5T_lock(dt, false) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to lock transient datatype");

    /* Create an ID */
    if (H5T_is_named(dt)) {
        /* If this is a committed datatype, we need to recreate the
         * two-level IDs, where the VOL object is a copy of the
         * returned datatype.
         */
        if ((ret_value = H5VL_wrap_register(H5I_DATATYPE, dt, true)) < 0)
            HGOTO_ERROR(H5E_ID, H5E_CANTREGISTER, FAIL, "unable to register datatype");
    } /* end if */
    else if ((ret_value = H5I_register(H5I_DATATYPE, dt, true)) < 0)
        HGOTO_ERROR(H5E_ID, H5E_CANTREGISTER, FAIL, "unable to register datatype");

done:
    if (ret_value < 0)
        if (dt && H5T_close(dt) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "unable to release datatype");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__get_type() */

/*-------------------------------------------------------------------------
 * Function: H5D__refresh
 *
 * Purpose:  Refreshes all buffers associated with a dataset.
 *
 * Return:   SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
herr_t
H5D__refresh(H5D_t *dset, hid_t dset_id)
{
    H5D_virtual_held_file_t *head            = NULL;    /* Pointer to list of files held open */
    bool                     virt_dsets_held = false;   /* Whether virtual datasets' files are held open */
    herr_t                   ret_value       = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(dset);
    assert(dset->shared);

    /* If the layout is virtual... */
    if (dset->shared->layout.type == H5D_VIRTUAL) {
        /* Hold open the source datasets' files */
        if (H5D__virtual_hold_source_dset_files(dset, &head) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINC, FAIL, "unable to hold VDS source files open");
        virt_dsets_held = true;

        /* Refresh source datasets for virtual dataset */
        if (H5D__virtual_refresh_source_dsets(dset) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTFLUSH, FAIL, "unable to refresh VDS source datasets");
    } /* end if */

    /* Refresh dataset object */
    if ((H5O_refresh_metadata(&dset->oloc, dset_id)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTFLUSH, FAIL, "unable to refresh dataset");

done:
    /* Release hold on (source) virtual datasets' files */
    if (virt_dsets_held)
        if (H5D__virtual_release_source_dset_files(head) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "can't release VDS source files held open");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__refresh() */
