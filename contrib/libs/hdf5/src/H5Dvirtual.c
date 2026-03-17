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
 * Purpose:
 *      Virtual Dataset (VDS) functions.  Creates a layout type which allows
 *      definition of a virtual dataset, where the actual dataset is stored in
 *      other datasets (called source datasets).  The mappings between the
 *      virtual and source datasets are specified by hyperslab or "all"
 *      dataspace selections.  Point selections are not currently supported.
 *      Overlaps in the mappings in the virtual dataset result in undefined
 *      behaviour.
 *
 *      Mapping selections may be unlimited, in which case the size of the
 *      virtual dataset is determined by the size of the source dataset(s).
 *      Names for the source datasets may also be generated procedurally, in
 *      which case the virtual selection should be unlimited with an unlimited
 *      count and the source selection should be limited with a size equal to
 *      that of the virtual selection with the unlimited count set to 1.
 *
 *      Source datasets are opened lazily (only when needed for I/O or to
 *      determine the size of the virtual dataset), and are currently held open
 *      until the virtual dataset is closed.
 */

/*
 * Note: H5S_select_project_intersection has been updated to no longer require
 * that the source and source intersect spaces have the same extent.  This file
 * should therefore be updated to remove code that ensures this condition, which
 * should improve both maintainability and performance.
 */

/****************/
/* Module Setup */
/****************/

#include "H5Dmodule.h" /* This source code file is part of the H5D module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                    */
#include "H5CXprivate.h" /* API Contexts                         */
#include "H5Dpkg.h"      /* Dataset functions                    */
#include "H5Eprivate.h"  /* Error handling                       */
#include "H5Fprivate.h"  /* Files                                */
#include "H5FLprivate.h" /* Free Lists                           */
#include "H5Gprivate.h"  /* Groups                               */
#include "H5HGprivate.h" /* Global Heaps                         */
#include "H5Iprivate.h"  /* IDs                                  */
#include "H5MMprivate.h" /* Memory management                    */
#include "H5Oprivate.h"  /* Object headers                       */
#include "H5Pprivate.h"  /* Property Lists                       */
#include "H5Sprivate.h"  /* Dataspaces                           */
#include "H5VLprivate.h" /* Virtual Object Layer                 */

/****************/
/* Local Macros */
/****************/

/* Default size for sub_dset array */
#define H5D_VIRTUAL_DEF_SUB_DSET_SIZE 128

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Local Prototypes */
/********************/

/* Layout operation callbacks */
static bool   H5D__virtual_is_data_cached(const H5D_shared_t *shared_dset);
static herr_t H5D__virtual_io_init(H5D_io_info_t *io_info, H5D_dset_io_info_t *dinfo);
static herr_t H5D__virtual_read(H5D_io_info_t *io_info, H5D_dset_io_info_t *dinfo);
static herr_t H5D__virtual_write(H5D_io_info_t *io_info, H5D_dset_io_info_t *dinfo);
static herr_t H5D__virtual_flush(H5D_t *dset);

/* Other functions */
static herr_t H5D__virtual_open_source_dset(const H5D_t *vdset, H5O_storage_virtual_ent_t *virtual_ent,
                                            H5O_storage_virtual_srcdset_t *source_dset);
static herr_t H5D__virtual_reset_source_dset(H5O_storage_virtual_ent_t     *virtual_ent,
                                             H5O_storage_virtual_srcdset_t *source_dset);
static herr_t H5D__virtual_str_append(const char *src, size_t src_len, char **p, char **buf,
                                      size_t *buf_size);
static herr_t H5D__virtual_copy_parsed_name(H5O_storage_virtual_name_seg_t **dst,
                                            H5O_storage_virtual_name_seg_t  *src);
static herr_t H5D__virtual_build_source_name(char                                 *source_name,
                                             const H5O_storage_virtual_name_seg_t *parsed_name,
                                             size_t static_strlen, size_t nsubs, hsize_t blockno,
                                             char **built_name);
static herr_t H5D__virtual_init_all(const H5D_t *dset);
static herr_t H5D__virtual_pre_io(H5D_dset_io_info_t *dset_info, H5O_storage_virtual_t *storage,
                                  H5S_t *file_space, H5S_t *mem_space, hsize_t *tot_nelmts);
static herr_t H5D__virtual_post_io(H5O_storage_virtual_t *storage);
static herr_t H5D__virtual_read_one(H5D_dset_io_info_t            *dset_info,
                                    H5O_storage_virtual_srcdset_t *source_dset);
static herr_t H5D__virtual_write_one(H5D_dset_io_info_t            *dset_info,
                                     H5O_storage_virtual_srcdset_t *source_dset);

/*********************/
/* Package Variables */
/*********************/

/* Contiguous storage layout I/O ops */
const H5D_layout_ops_t H5D_LOPS_VIRTUAL[1] = {{
    NULL,                        /* construct */
    H5D__virtual_init,           /* init */
    H5D__virtual_is_space_alloc, /* is_space_alloc */
    H5D__virtual_is_data_cached, /* is_data_cached */
    H5D__virtual_io_init,        /* io_init */
    NULL,                        /* mdio_init */
    H5D__virtual_read,           /* ser_read */
    H5D__virtual_write,          /* ser_write */
    NULL,                        /* readvv */
    NULL,                        /* writevv */
    H5D__virtual_flush,          /* flush */
    NULL,                        /* io_term */
    NULL                         /* dest */
}};

/*******************/
/* Local Variables */
/*******************/

/* Declare a free list to manage the H5O_storage_virtual_name_seg_t struct */
H5FL_DEFINE(H5O_storage_virtual_name_seg_t);

/* Declare a static free list to manage H5D_virtual_file_list_t structs */
H5FL_DEFINE_STATIC(H5D_virtual_held_file_t);

/*-------------------------------------------------------------------------
 * Function:    H5D_virtual_check_mapping_pre
 *
 * Purpose:     Checks that the provided virtual and source selections are
 *              legal for use as a VDS mapping, prior to creating the rest
 *              of the mapping entry.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D_virtual_check_mapping_pre(const H5S_t *vspace, const H5S_t *src_space,
                              H5O_virtual_space_status_t space_status)
{
    H5S_sel_type select_type;         /* Selection type */
    hsize_t      nelmts_vs;           /* Number of elements in virtual selection */
    hsize_t      nelmts_ss;           /* Number of elements in source selection */
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check for point selections (currently unsupported) */
    if (H5S_SEL_ERROR == (select_type = H5S_GET_SELECT_TYPE(vspace)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get selection type");
    if (select_type == H5S_SEL_POINTS)
        HGOTO_ERROR(H5E_DATASET, H5E_UNSUPPORTED, FAIL,
                    "point selections not currently supported with virtual datasets");
    if (H5S_SEL_ERROR == (select_type = H5S_GET_SELECT_TYPE(src_space)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get selection type");
    if (select_type == H5S_SEL_POINTS)
        HGOTO_ERROR(H5E_DATASET, H5E_UNSUPPORTED, FAIL,
                    "point selections not currently supported with virtual datasets");

    /* Get number of elements in spaces */
    nelmts_vs = (hsize_t)H5S_GET_SELECT_NPOINTS(vspace);
    nelmts_ss = (hsize_t)H5S_GET_SELECT_NPOINTS(src_space);

    /* Check for unlimited vspace */
    if (nelmts_vs == H5S_UNLIMITED) {
        /* Check for unlimited src_space */
        if (nelmts_ss == H5S_UNLIMITED) {
            hsize_t nenu_vs; /* Number of elements in the non-unlimited dimensions of vspace */
            hsize_t nenu_ss; /* Number of elements in the non-unlimited dimensions of src_space */

            /* Non-printf unlimited selection.  Make sure both selections have
             * the same number of elements in the non-unlimited dimension.  Note
             * we can always check this even if the space status is invalid
             * because unlimited selections are never dependent on the extent.
             */
            if (H5S_get_select_num_elem_non_unlim(vspace, &nenu_vs) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTCOUNT, FAIL,
                            "can't get number of elements in non-unlimited dimension");
            if (H5S_get_select_num_elem_non_unlim(src_space, &nenu_ss) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTCOUNT, FAIL,
                            "can't get number of elements in non-unlimited dimension");
            if (nenu_vs != nenu_ss)
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                            "numbers of elements in the non-unlimited dimensions is different for source and "
                            "virtual spaces");
        } /* end if */
        /* We will handle the printf case after parsing the source names */
    } /* end if */
    else if (space_status != H5O_VIRTUAL_STATUS_INVALID)
        /* Limited selections.  Check number of points is the same. */
        if (nelmts_vs != nelmts_ss)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                        "virtual and source space selections have different numbers of elements");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D_virtual_check_mapping_pre() */

/*-------------------------------------------------------------------------
 * Function:    H5D_virtual_check_mapping_post
 *
 * Purpose:     Checks that the provided virtual dataset mapping entry is
 *              legal, after the mapping is otherwise complete.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D_virtual_check_mapping_post(const H5O_storage_virtual_ent_t *ent)
{
    hsize_t nelmts_vs;           /* Number of elements in virtual selection */
    hsize_t nelmts_ss;           /* Number of elements in source selection */
    H5S_t  *tmp_space = NULL;    /* Temporary dataspace */
    herr_t  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Get number of elements in spaces */
    nelmts_vs = (hsize_t)H5S_GET_SELECT_NPOINTS(ent->source_dset.virtual_select);
    nelmts_ss = (hsize_t)H5S_GET_SELECT_NPOINTS(ent->source_select);

    /* Check for printf selection */
    if ((nelmts_vs == H5S_UNLIMITED) && (nelmts_ss != H5S_UNLIMITED)) {
        /* Make sure there at least one %b substitution in the source file or
         * dataset name */
        if ((ent->psfn_nsubs == 0) && (ent->psdn_nsubs == 0))
            HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL,
                        "unlimited virtual selection, limited source selection, and no printf specifiers in "
                        "source names");

        /* Make sure virtual space uses hyperslab selection */
        if (H5S_GET_SELECT_TYPE(ent->source_dset.virtual_select) != H5S_SEL_HYPERSLABS)
            HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL,
                        "virtual selection with printf mapping must be hyperslab");

        /* Check that the number of elements in one block in the virtual
         * selection matches the total number of elements in the source
         * selection, if the source space status is not invalid (virtual space
         * status does not matter here because it is unlimited) */
        if (ent->source_space_status != H5O_VIRTUAL_STATUS_INVALID) {
            /* Get first block in virtual selection */
            if (NULL == (tmp_space = H5S_hyper_get_unlim_block(ent->source_dset.virtual_select, (hsize_t)0)))
                HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get first block in virtual selection");

            /* Check number of points */
            nelmts_vs = (hsize_t)H5S_GET_SELECT_NPOINTS(tmp_space);
            if (nelmts_vs != nelmts_ss)
                HGOTO_ERROR(
                    H5E_ARGS, H5E_BADVALUE, FAIL,
                    "virtual (single block) and source space selections have different numbers of elements");
        } /* end if */
    }     /* end if */
    else
        /* Make sure there are no printf substitutions */
        if ((ent->psfn_nsubs > 0) || (ent->psdn_nsubs > 0))
            HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL,
                        "printf specifier(s) in source name(s) without an unlimited virtual selection and "
                        "limited source selection");

done:
    /* Free temporary space */
    if (tmp_space)
        if (H5S_close(tmp_space) < 0)
            HDONE_ERROR(H5E_PLIST, H5E_CLOSEERROR, FAIL, "can't close dataspace");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D_virtual_check_mapping_post() */

/*-------------------------------------------------------------------------
 * Function:    H5D_virtual_update_min_dims
 *
 * Purpose:     Updates the virtual layout's "min_dims" field to take into
 *              account the "idx"th entry in the mapping list.  The entry
 *              must be complete, though top level field list_nused (and
 *              of course min_dims) does not need to take it into account.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D_virtual_update_min_dims(H5O_layout_t *layout, size_t idx)
{
    H5O_storage_virtual_t     *virt = &layout->storage.u.virt;
    H5O_storage_virtual_ent_t *ent  = &virt->list[idx];
    H5S_sel_type               sel_type;
    int                        rank;
    hsize_t                    bounds_start[H5S_MAX_RANK];
    hsize_t                    bounds_end[H5S_MAX_RANK];
    int                        i;
    herr_t                     ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    assert(layout);
    assert(layout->type == H5D_VIRTUAL);
    assert(idx < virt->list_nalloc);

    /* Get type of selection */
    if (H5S_SEL_ERROR == (sel_type = H5S_GET_SELECT_TYPE(ent->source_dset.virtual_select)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "unable to get selection type");

    /* Do not update min_dims for "all" or "none" selections */
    if ((sel_type == H5S_SEL_ALL) || (sel_type == H5S_SEL_NONE))
        HGOTO_DONE(SUCCEED);

    /* Get rank of vspace */
    if ((rank = H5S_GET_EXTENT_NDIMS(ent->source_dset.virtual_select)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "unable to get number of dimensions");

    /* Get selection bounds */
    if (H5S_SELECT_BOUNDS(ent->source_dset.virtual_select, bounds_start, bounds_end) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "unable to get selection bounds");

    /* Update min_dims */
    for (i = 0; i < rank; i++)
        /* Don't check unlimited dimensions in the selection */
        if ((i != ent->unlim_dim_virtual) && (bounds_end[i] >= virt->min_dims[i]))
            virt->min_dims[i] = bounds_end[i] + (hsize_t)1;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D_virtual_update_min_dims() */

/*-------------------------------------------------------------------------
 * Function:    H5D_virtual_check_min_dims
 *
 * Purpose:     Checks if the dataset's dimensions are at least the
 *              calculated minimum dimensions from the mappings.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D_virtual_check_min_dims(const H5D_t *dset)
{
    int     rank;
    hsize_t dims[H5S_MAX_RANK];
    int     i;
    herr_t  ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    assert(dset);
    assert(dset->shared);
    assert(dset->shared->layout.type == H5D_VIRTUAL);

    /* Get rank of dataspace */
    if ((rank = H5S_GET_EXTENT_NDIMS(dset->shared->space)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "unable to get number of dimensions");

    /* Get VDS dimensions */
    if (H5S_get_simple_extent_dims(dset->shared->space, dims, NULL) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get VDS dimensions");

    /* Verify that dimensions are larger than min_dims */
    for (i = 0; i < rank; i++)
        if (dims[i] < dset->shared->layout.storage.u.virt.min_dims[i])
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                        "virtual dataset dimensions not large enough to contain all limited dimensions in "
                        "all selections");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D_virtual_check_min_dims() */

/*-------------------------------------------------------------------------
 * Function:    H5D__virtual_store_layout
 *
 * Purpose:     Store virtual dataset layout information, for new dataset
 *
 * Note:        We assume here that the contents of the heap block cannot
 *        change!  If this ever stops being the case we must change
 *        this code to allow overwrites of the heap block.  -NAF
 *
 * Return:      Success:    SUCCEED
 *              Failure:    FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__virtual_store_layout(H5F_t *f, H5O_layout_t *layout)
{
    H5O_storage_virtual_t *virt       = &layout->storage.u.virt;
    uint8_t               *heap_block = NULL;   /* Block to add to heap */
    size_t                *str_size   = NULL;   /* Array for VDS entry string lengths */
    uint8_t               *heap_block_p;        /* Pointer into the heap block, while encoding */
    size_t                 block_size;          /* Total size of block needed */
    hsize_t                tmp_nentries;        /* Temp. variable for # of VDS entries */
    uint32_t               chksum;              /* Checksum for heap data */
    size_t                 i;                   /* Local index variable */
    herr_t                 ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checking */
    assert(f);
    assert(layout);
    assert(virt->serial_list_hobjid.addr == HADDR_UNDEF);

    /* Create block if # of used entries > 0 */
    if (virt->list_nused > 0) {

        /* Set the low/high bounds according to 'f' for the API context */
        H5CX_set_libver_bounds(f);

        /* Allocate array for caching results of strlen */
        if (NULL == (str_size = (size_t *)H5MM_malloc(2 * virt->list_nused * sizeof(size_t))))
            HGOTO_ERROR(H5E_OHDR, H5E_RESOURCE, FAIL, "unable to allocate string length array");

        /*
         * Calculate heap block size
         */

        /* Version and number of entries */
        block_size = (size_t)1 + H5F_SIZEOF_SIZE(f);

        /* Calculate size of each entry */
        for (i = 0; i < virt->list_nused; i++) {
            H5O_storage_virtual_ent_t *ent = &virt->list[i];
            hssize_t                   select_serial_size; /* Size of serialized selection */

            assert(ent->source_file_name);
            assert(ent->source_dset_name);
            assert(ent->source_select);
            assert(ent->source_dset.virtual_select);

            /* Source file name */
            str_size[2 * i] = strlen(ent->source_file_name) + (size_t)1;
            block_size += str_size[2 * i];

            /* Source dset name */
            str_size[(2 * i) + 1] = strlen(ent->source_dset_name) + (size_t)1;
            block_size += str_size[(2 * i) + 1];

            /* Source selection */
            if ((select_serial_size = H5S_SELECT_SERIAL_SIZE(ent->source_select)) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTENCODE, FAIL, "unable to check dataspace selection size");
            block_size += (size_t)select_serial_size;

            /* Virtual dataset selection */
            if ((select_serial_size = H5S_SELECT_SERIAL_SIZE(ent->source_dset.virtual_select)) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTENCODE, FAIL, "unable to check dataspace selection size");
            block_size += (size_t)select_serial_size;
        } /* end for */

        /* Checksum */
        block_size += 4;

        /* Allocate heap block */
        if (NULL == (heap_block = (uint8_t *)H5MM_malloc(block_size)))
            HGOTO_ERROR(H5E_OHDR, H5E_RESOURCE, FAIL, "unable to allocate heap block");

        /*
         * Encode heap block
         */
        heap_block_p = heap_block;

        /* Encode heap block encoding version */
        *heap_block_p++ = (uint8_t)H5O_LAYOUT_VDS_GH_ENC_VERS;

        /* Number of entries */
        tmp_nentries = (hsize_t)virt->list_nused;
        H5F_ENCODE_LENGTH(f, heap_block_p, tmp_nentries);

        /* Encode each entry */
        for (i = 0; i < virt->list_nused; i++) {
            H5O_storage_virtual_ent_t *ent = &virt->list[i];
            /* Source file name */
            H5MM_memcpy((char *)heap_block_p, ent->source_file_name, str_size[2 * i]);
            heap_block_p += str_size[2 * i];

            /* Source dataset name */
            H5MM_memcpy((char *)heap_block_p, ent->source_dset_name, str_size[(2 * i) + 1]);
            heap_block_p += str_size[(2 * i) + 1];

            /* Source selection */
            if (H5S_SELECT_SERIALIZE(ent->source_select, &heap_block_p) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTCOPY, FAIL, "unable to serialize source selection");

            /* Virtual selection */
            if (H5S_SELECT_SERIALIZE(ent->source_dset.virtual_select, &heap_block_p) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTCOPY, FAIL, "unable to serialize virtual selection");
        } /* end for */

        /* Checksum */
        chksum = H5_checksum_metadata(heap_block, block_size - (size_t)4, 0);
        UINT32ENCODE(heap_block_p, chksum);

        /* Insert block into global heap */
        if (H5HG_insert(f, block_size, heap_block, &(virt->serial_list_hobjid)) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTINSERT, FAIL, "unable to insert virtual dataset heap block");
    } /* end if */

done:
    heap_block = (uint8_t *)H5MM_xfree(heap_block);
    str_size   = (size_t *)H5MM_xfree(str_size);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__virtual_store_layout() */

/*-------------------------------------------------------------------------
 * Function:    H5D__virtual_copy_layout
 *
 * Purpose:     Deep copies virtual storage layout message in memory.
 *              This function assumes that the top-level struct has
 *              already been copied (so the source struct retains
 *              ownership of the fields passed to this function).
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__virtual_copy_layout(H5O_layout_t *layout)
{
    H5O_storage_virtual_ent_t *orig_list = NULL;
    H5O_storage_virtual_t     *virt      = &layout->storage.u.virt;
    hid_t                      orig_source_fapl;
    hid_t                      orig_source_dapl;
    H5P_genplist_t            *plist;
    size_t                     i;
    herr_t                     ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(layout);
    assert(layout->type == H5D_VIRTUAL);

    /* Save original entry list and top-level property lists and reset in layout
     * so the originals aren't closed on error */
    orig_source_fapl  = virt->source_fapl;
    virt->source_fapl = -1;
    orig_source_dapl  = virt->source_dapl;
    virt->source_dapl = -1;
    orig_list         = virt->list;
    virt->list        = NULL;

    /* Copy entry list */
    if (virt->list_nused > 0) {
        assert(orig_list);

        /* Allocate memory for the list */
        if (NULL == (virt->list = H5MM_calloc(virt->list_nused * sizeof(virt->list[0]))))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL,
                        "unable to allocate memory for virtual dataset entry list");
        virt->list_nalloc = virt->list_nused;

        /* Copy the list entries, though set source_dset.dset and sub_dset to
         * NULL */
        for (i = 0; i < virt->list_nused; i++) {
            H5O_storage_virtual_ent_t *ent = &virt->list[i];

            /* Copy virtual selection */
            if (NULL == (ent->source_dset.virtual_select =
                             H5S_copy(orig_list[i].source_dset.virtual_select, false, true)))
                HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "unable to copy virtual selection");

            /* Copy original source names */
            if (NULL == (ent->source_file_name = H5MM_strdup(orig_list[i].source_file_name)))
                HGOTO_ERROR(H5E_DATASET, H5E_RESOURCE, FAIL, "unable to duplicate source file name");
            if (NULL == (ent->source_dset_name = H5MM_strdup(orig_list[i].source_dset_name)))
                HGOTO_ERROR(H5E_DATASET, H5E_RESOURCE, FAIL, "unable to duplicate source dataset name");

            /* Copy source selection */
            if (NULL == (ent->source_select = H5S_copy(orig_list[i].source_select, false, true)))
                HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "unable to copy source selection");

            /* Initialize clipped selections */
            if (orig_list[i].unlim_dim_virtual < 0) {
                ent->source_dset.clipped_source_select  = ent->source_select;
                ent->source_dset.clipped_virtual_select = ent->source_dset.virtual_select;
            } /* end if */

            /* Copy parsed names */
            if (H5D__virtual_copy_parsed_name(&ent->parsed_source_file_name,
                                              orig_list[i].parsed_source_file_name) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "unable to copy parsed source file name");
            ent->psfn_static_strlen = orig_list[i].psfn_static_strlen;
            ent->psfn_nsubs         = orig_list[i].psfn_nsubs;
            if (H5D__virtual_copy_parsed_name(&ent->parsed_source_dset_name,
                                              orig_list[i].parsed_source_dset_name) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "unable to copy parsed source dataset name");
            ent->psdn_static_strlen = orig_list[i].psdn_static_strlen;
            ent->psdn_nsubs         = orig_list[i].psdn_nsubs;

            /* Copy source names in source dset or add reference as appropriate
             */
            if (orig_list[i].source_dset.file_name) {
                if (orig_list[i].source_dset.file_name == orig_list[i].source_file_name)
                    ent->source_dset.file_name = ent->source_file_name;
                else if (orig_list[i].parsed_source_file_name &&
                         (orig_list[i].source_dset.file_name !=
                          orig_list[i].parsed_source_file_name->name_segment)) {
                    assert(ent->parsed_source_file_name);
                    assert(ent->parsed_source_file_name->name_segment);
                    ent->source_dset.file_name = ent->parsed_source_file_name->name_segment;
                } /* end if */
                else if (NULL ==
                         (ent->source_dset.file_name = H5MM_strdup(orig_list[i].source_dset.file_name)))
                    HGOTO_ERROR(H5E_DATASET, H5E_RESOURCE, FAIL, "unable to duplicate source file name");
            } /* end if */
            if (orig_list[i].source_dset.dset_name) {
                if (orig_list[i].source_dset.dset_name == orig_list[i].source_dset_name)
                    ent->source_dset.dset_name = ent->source_dset_name;
                else if (orig_list[i].parsed_source_dset_name &&
                         (orig_list[i].source_dset.dset_name !=
                          orig_list[i].parsed_source_dset_name->name_segment)) {
                    assert(ent->parsed_source_dset_name);
                    assert(ent->parsed_source_dset_name->name_segment);
                    ent->source_dset.dset_name = ent->parsed_source_dset_name->name_segment;
                } /* end if */
                else if (NULL ==
                         (ent->source_dset.dset_name = H5MM_strdup(orig_list[i].source_dset.dset_name)))
                    HGOTO_ERROR(H5E_DATASET, H5E_RESOURCE, FAIL, "unable to duplicate source dataset name");
            } /* end if */

            /* Copy other fields in entry */
            ent->unlim_dim_source     = orig_list[i].unlim_dim_source;
            ent->unlim_dim_virtual    = orig_list[i].unlim_dim_virtual;
            ent->unlim_extent_source  = orig_list[i].unlim_extent_source;
            ent->unlim_extent_virtual = orig_list[i].unlim_extent_virtual;
            ent->clip_size_source     = orig_list[i].clip_size_source;
            ent->clip_size_virtual    = orig_list[i].clip_size_virtual;
            ent->source_space_status  = orig_list[i].source_space_status;
            ent->virtual_space_status = orig_list[i].virtual_space_status;
        } /* end for */
    }     /* end if */
    else {
        /* Zero out other fields related to list, just to be sure */
        virt->list        = NULL;
        virt->list_nalloc = 0;
    } /* end else */

    /* Copy property lists */
    if (orig_source_fapl >= 0) {
        if (NULL == (plist = (H5P_genplist_t *)H5I_object_verify(orig_source_fapl, H5I_GENPROP_LST)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a property list");
        if ((virt->source_fapl = H5P_copy_plist(plist, false)) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "can't copy fapl");
    } /* end if */
    if (orig_source_dapl >= 0) {
        if (NULL == (plist = (H5P_genplist_t *)H5I_object_verify(orig_source_dapl, H5I_GENPROP_LST)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a property list");
        if ((virt->source_dapl = H5P_copy_plist(plist, false)) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "can't copy dapl");
    } /* end if */

    /* New layout is not fully initialized */
    virt->init = false;

done:
    /* Release allocated resources on failure */
    if (ret_value < 0)
        if (H5D__virtual_reset_layout(layout) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "unable to reset virtual layout");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__virtual_copy_layout() */

/*-------------------------------------------------------------------------
 * Function:    H5D__virtual_reset_layout
 *
 * Purpose:     Frees internal structures in a virtual storage layout
 *              message in memory.  This function is safe to use on
 *              incomplete structures (for recovery from failure) provided
 *              the internal structures are initialized with all bytes set
 *              to 0.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__virtual_reset_layout(H5O_layout_t *layout)
{
    size_t                 i, j;
    H5O_storage_virtual_t *virt      = &layout->storage.u.virt;
    herr_t                 ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(layout);
    assert(layout->type == H5D_VIRTUAL);

    /* Free the list entries.  Note we always attempt to free everything even in
     * the case of a failure.  Because of this, and because we free the list
     * afterwards, we do not need to zero out the memory in the list. */
    for (i = 0; i < virt->list_nused; i++) {
        H5O_storage_virtual_ent_t *ent = &virt->list[i];
        /* Free source_dset */
        if (H5D__virtual_reset_source_dset(ent, &ent->source_dset) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "unable to reset source dataset");

        /* Free original source names */
        (void)H5MM_xfree(ent->source_file_name);
        (void)H5MM_xfree(ent->source_dset_name);

        /* Free sub_dset */
        for (j = 0; j < ent->sub_dset_nalloc; j++)
            if (H5D__virtual_reset_source_dset(ent, &ent->sub_dset[j]) < 0)
                HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "unable to reset source dataset");
        ent->sub_dset = H5MM_xfree(ent->sub_dset);

        /* Free source_select */
        if (ent->source_select)
            if (H5S_close(ent->source_select) < 0)
                HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "unable to release source selection");

        /* Free parsed_source_file_name */
        H5D_virtual_free_parsed_name(ent->parsed_source_file_name);

        /* Free parsed_source_dset_name */
        H5D_virtual_free_parsed_name(ent->parsed_source_dset_name);
    }

    /* Free the list */
    virt->list        = H5MM_xfree(virt->list);
    virt->list_nalloc = (size_t)0;
    virt->list_nused  = (size_t)0;
    (void)memset(virt->min_dims, 0, sizeof(virt->min_dims));

    /* Close access property lists */
    if (virt->source_fapl >= 0) {
        if (H5I_dec_ref(virt->source_fapl) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "can't close source fapl");
        virt->source_fapl = -1;
    }
    if (virt->source_dapl >= 0) {
        if (H5I_dec_ref(virt->source_dapl) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "can't close source dapl");
        virt->source_dapl = -1;
    }

    /* The list is no longer initialized */
    virt->init = false;

    /* Note the lack of a done: label.  This is because there are no HGOTO_ERROR
     * calls.  If one is added, a done: label must also be added */
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__virtual_reset_layout() */

/*-------------------------------------------------------------------------
 * Function:    H5D__virtual_copy
 *
 * Purpose:     Copy virtual storage raw data from SRC file to DST file.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__virtual_copy(H5F_t *f_dst, H5O_layout_t *layout_dst)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

#ifdef NOT_YET
    /* Check for copy to the same file */
    if (f_dst == f_src) {
        /* Increase reference count on global heap object */
        if ((heap_rc = H5HG_link(f_dst, (H5HG_t *)&(layout_dst->u.virt.serial_list_hobjid), 1)) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTMODIFY, FAIL, "unable to adjust global heap reference count");
    } /* end if */
    else
#endif /* NOT_YET */
    {
        /* Reset global heap id */
        layout_dst->storage.u.virt.serial_list_hobjid.addr = HADDR_UNDEF;
        layout_dst->storage.u.virt.serial_list_hobjid.idx  = (size_t)0;

        /* Write the VDS data to destination file's heap */
        if (H5D__virtual_store_layout(f_dst, layout_dst) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "unable to store VDS info");
    } /* end block/else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__virtual_copy() */

/*-------------------------------------------------------------------------
 * Function:    H5D__virtual_delete
 *
 * Purpose:     Delete the file space for a virtual dataset
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__virtual_delete(H5F_t *f, H5O_storage_t *storage)
{
#ifdef NOT_YET
    int heap_rc;                /* Reference count of global heap object */
#endif                          /* NOT_YET */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(f);
    assert(storage);
    assert(storage->type == H5D_VIRTUAL);

    /* Check for global heap block */
    if (storage->u.virt.serial_list_hobjid.addr != HADDR_UNDEF) {
#ifdef NOT_YET
        /* Unlink the global heap block */
        if ((heap_rc = H5HG_link(f, (H5HG_t *)&(storage->u.virt.serial_list_hobjid), -1)) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTMODIFY, FAIL, "unable to adjust global heap reference count");
        if (heap_rc == 0)
#endif /* NOT_YET */
            /* Delete the global heap block */
            if (H5HG_remove(f, (H5HG_t *)&(storage->u.virt.serial_list_hobjid)) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTREMOVE, FAIL, "unable to remove heap object");
    } /* end if */

    /* Clear global heap ID in storage */
    storage->u.virt.serial_list_hobjid.addr = HADDR_UNDEF;
    storage->u.virt.serial_list_hobjid.idx  = 0;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__virtual_delete */

/*-------------------------------------------------------------------------
 * Function:    H5D__virtual_open_source_dset
 *
 * Purpose:     Attempts to open a source dataset.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__virtual_open_source_dset(const H5D_t *vdset, H5O_storage_virtual_ent_t *virtual_ent,
                              H5O_storage_virtual_srcdset_t *source_dset)
{
    H5F_t *src_file      = NULL;    /* Source file */
    bool   src_file_open = false;   /* Whether we have opened and need to close src_file */
    herr_t ret_value     = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(vdset);
    assert(source_dset);
    assert(!source_dset->dset);
    assert(source_dset->file_name);
    assert(source_dset->dset_name);

    /* Check if we need to open the source file */
    if (strcmp(source_dset->file_name, ".") != 0) {
        unsigned intent; /* File access permissions */

        /* Get the virtual dataset's file open flags ("intent") */
        intent = H5F_INTENT(vdset->oloc.file);

        /* Try opening the file */
        src_file = H5F_prefix_open_file(vdset->oloc.file, H5F_PREFIX_VDS, vdset->shared->vds_prefix,
                                        source_dset->file_name, intent,
                                        vdset->shared->layout.storage.u.virt.source_fapl);

        /* If we opened the source file here, we should close it when leaving */
        if (src_file)
            src_file_open = true;
        else
            /* Reset the error stack */
            H5E_clear_stack(NULL);
    } /* end if */
    else
        /* Source file is ".", use the virtual dataset's file */
        src_file = vdset->oloc.file;

    if (src_file) {
        H5G_loc_t src_root_loc; /* Object location of source file root group */

        /* Set up the root group in the destination file */
        if (NULL == (src_root_loc.oloc = H5G_oloc(H5G_rootof(src_file))))
            HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "unable to get object location for root group");
        if (NULL == (src_root_loc.path = H5G_nameof(H5G_rootof(src_file))))
            HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "unable to get path for root group");

        /* Try opening the source dataset */
        source_dset->dset = H5D__open_name(&src_root_loc, source_dset->dset_name,
                                           vdset->shared->layout.storage.u.virt.source_dapl);

        /* Dataset does not exist */
        if (NULL == source_dset->dset) {
            /* Reset the error stack */
            H5E_clear_stack(NULL);

            source_dset->dset_exists = false;
        } /* end if */
        else {
            /* Dataset exists */
            source_dset->dset_exists = true;

            /* Patch the source selection if necessary */
            if (virtual_ent->source_space_status != H5O_VIRTUAL_STATUS_CORRECT) {
                if (H5S_extent_copy(virtual_ent->source_select, source_dset->dset->shared->space) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "can't copy source dataspace extent");
                virtual_ent->source_space_status = H5O_VIRTUAL_STATUS_CORRECT;
            } /* end if */
        }     /* end else */
    }         /* end if */

done:
    /* Release resources */
    if (src_file_open)
        if (H5F_efc_close(vdset->oloc.file, src_file) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTCLOSEFILE, FAIL, "can't close source file");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__virtual_open_source_dset() */

/*-------------------------------------------------------------------------
 * Function:    H5D__virtual_reset_source_dset
 *
 * Purpose:     Frees space referenced by a source dataset struct.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__virtual_reset_source_dset(H5O_storage_virtual_ent_t     *virtual_ent,
                               H5O_storage_virtual_srcdset_t *source_dset)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(source_dset);

    /* Free dataset */
    if (source_dset->dset) {
        if (H5D_close(source_dset->dset) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "unable to close source dataset");
        source_dset->dset = NULL;
    } /* end if */

    /* Free file name */
    if (virtual_ent->parsed_source_file_name &&
        (source_dset->file_name != virtual_ent->parsed_source_file_name->name_segment))
        source_dset->file_name = (char *)H5MM_xfree(source_dset->file_name);
    else
        assert((source_dset->file_name == virtual_ent->source_file_name) ||
               (virtual_ent->parsed_source_file_name &&
                (source_dset->file_name == virtual_ent->parsed_source_file_name->name_segment)) ||
               !source_dset->file_name);

    /* Free dataset name */
    if (virtual_ent->parsed_source_dset_name &&
        (source_dset->dset_name != virtual_ent->parsed_source_dset_name->name_segment))
        source_dset->dset_name = (char *)H5MM_xfree(source_dset->dset_name);
    else
        assert((source_dset->dset_name == virtual_ent->source_dset_name) ||
               (virtual_ent->parsed_source_dset_name &&
                (source_dset->dset_name == virtual_ent->parsed_source_dset_name->name_segment)) ||
               !source_dset->dset_name);

    /* Free clipped virtual selection */
    if (source_dset->clipped_virtual_select) {
        if (source_dset->clipped_virtual_select != source_dset->virtual_select)
            if (H5S_close(source_dset->clipped_virtual_select) < 0)
                HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "unable to release clipped virtual selection");
        source_dset->clipped_virtual_select = NULL;
    } /* end if */

    /* Free virtual selection */
    if (source_dset->virtual_select) {
        if (H5S_close(source_dset->virtual_select) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "unable to release virtual selection");
        source_dset->virtual_select = NULL;
    } /* end if */

    /* Free clipped source selection */
    if (source_dset->clipped_source_select) {
        if (source_dset->clipped_source_select != virtual_ent->source_select)
            if (H5S_close(source_dset->clipped_source_select) < 0)
                HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "unable to release clipped source selection");
        source_dset->clipped_source_select = NULL;
    } /* end if */

    /* The projected memory space should never exist when this function is
     * called */
    assert(!source_dset->projected_mem_space);

    /* Note the lack of a done: label.  This is because there are no HGOTO_ERROR
     * calls.  If one is added, a done: label must also be added */
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__virtual_reset_source_dset() */

/*-------------------------------------------------------------------------
 * Function:    H5D__virtual_str_append
 *
 * Purpose:     Appends src_len bytes of the string src to the position *p
 *              in the buffer *buf (allocating *buf if necessary).
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__virtual_str_append(const char *src, size_t src_len, char **p, char **buf, size_t *buf_size)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(src);
    assert(src_len > 0);
    assert(p);
    assert(buf);
    assert(*p >= *buf);
    assert(buf_size);

    /* Allocate or extend buffer if necessary */
    if (!*buf) {
        assert(!*p);
        assert(*buf_size == 0);

        /* Allocate buffer */
        if (NULL == (*buf = (char *)H5MM_malloc(src_len + (size_t)1)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "unable to allocate name segment struct");
        *buf_size = src_len + (size_t)1;
        *p        = *buf;
    } /* end if */
    else {
        size_t p_offset = (size_t)(*p - *buf); /* Offset of p within buf */

        /* Extend buffer if necessary */
        if ((p_offset + src_len + (size_t)1) > *buf_size) {
            char  *tmp_buf;
            size_t tmp_buf_size;

            /* Calculate new size of buffer */
            tmp_buf_size = MAX(p_offset + src_len + (size_t)1, *buf_size * (size_t)2);

            /* Reallocate buffer */
            if (NULL == (tmp_buf = (char *)H5MM_realloc(*buf, tmp_buf_size)))
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "unable to reallocate name segment buffer");
            *buf      = tmp_buf;
            *buf_size = tmp_buf_size;
            *p        = *buf + p_offset;
        } /* end if */
    }     /* end else */

    /* Copy string to *p.  Note that since src in not NULL terminated, we must
     * use memcpy */
    H5MM_memcpy(*p, src, src_len);

    /* Advance *p */
    *p += src_len;

    /* Add NULL terminator */
    **p = '\0';

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__virtual_str_append() */

/*-------------------------------------------------------------------------
 * Function:    H5D_virtual_parse_source_name
 *
 * Purpose:     Parses a source file or dataset name.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D_virtual_parse_source_name(const char *source_name, H5O_storage_virtual_name_seg_t **parsed_name,
                              size_t *static_strlen, size_t *nsubs)
{
    H5O_storage_virtual_name_seg_t  *tmp_parsed_name   = NULL;
    H5O_storage_virtual_name_seg_t **tmp_parsed_name_p = &tmp_parsed_name;
    size_t                           tmp_static_strlen;
    size_t                           tmp_strlen;
    size_t                           tmp_nsubs = 0;
    const char                      *p;
    const char                      *pct;
    char                            *name_seg_p    = NULL;
    size_t                           name_seg_size = 0;
    herr_t                           ret_value     = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(source_name);
    assert(parsed_name);
    assert(static_strlen);
    assert(nsubs);

    /* Initialize p and tmp_static_strlen */
    p                 = source_name;
    tmp_static_strlen = tmp_strlen = strlen(source_name);

    /* Iterate over name */
    /* Note this will not work with UTF-8!  We should support this eventually
     * -NAF 5/18/2015 */
    while ((pct = strchr(p, '%'))) {
        assert(pct >= p);

        /* Allocate name segment struct if necessary */
        if (!*tmp_parsed_name_p)
            if (NULL == (*tmp_parsed_name_p = H5FL_CALLOC(H5O_storage_virtual_name_seg_t)))
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "unable to allocate name segment struct");

        /* Check for type of format specifier */
        if (pct[1] == 'b') {
            /* Check for blank string before specifier */
            if (pct != p)
                /* Append string to name segment */
                if (H5D__virtual_str_append(p, (size_t)(pct - p), &name_seg_p,
                                            &(*tmp_parsed_name_p)->name_segment, &name_seg_size) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "unable to append name segment");

            /* Update other variables */
            tmp_parsed_name_p = &(*tmp_parsed_name_p)->next;
            tmp_static_strlen -= 2;
            tmp_nsubs++;
            name_seg_p    = NULL;
            name_seg_size = 0;
        } /* end if */
        else if (pct[1] == '%') {
            /* Append string to name segment (include first '%') */
            if (H5D__virtual_str_append(p, (size_t)(pct - p) + (size_t)1, &name_seg_p,
                                        &(*tmp_parsed_name_p)->name_segment, &name_seg_size) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "unable to append name segment");

            /* Update other variables */
            tmp_static_strlen -= 1;
        } /* end else */
        else
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid format specifier");

        p = pct + 2;
    } /* end while */

    /* Copy last segment of name, if any, unless the parsed name was not
     * allocated */
    if (tmp_parsed_name) {
        assert(p >= source_name);
        if (*p == '\0')
            assert((size_t)(p - source_name) == tmp_strlen);
        else {
            assert((size_t)(p - source_name) < tmp_strlen);

            /* Allocate name segment struct if necessary */
            if (!*tmp_parsed_name_p)
                if (NULL == (*tmp_parsed_name_p = H5FL_CALLOC(H5O_storage_virtual_name_seg_t)))
                    HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "unable to allocate name segment struct");

            /* Append string to name segment */
            if (H5D__virtual_str_append(p, tmp_strlen - (size_t)(p - source_name), &name_seg_p,
                                        &(*tmp_parsed_name_p)->name_segment, &name_seg_size) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "unable to append name segment");
        } /* end else */
    }     /* end if */

    /* Set return values */
    *parsed_name    = tmp_parsed_name;
    tmp_parsed_name = NULL;
    *static_strlen  = tmp_static_strlen;
    *nsubs          = tmp_nsubs;

done:
    if (tmp_parsed_name) {
        assert(ret_value < 0);
        H5D_virtual_free_parsed_name(tmp_parsed_name);
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D_virtual_parse_source_name() */

/*-------------------------------------------------------------------------
 * Function:    H5D__virtual_copy_parsed_name
 *
 * Purpose:     Deep copies a parsed source file or dataset name.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__virtual_copy_parsed_name(H5O_storage_virtual_name_seg_t **dst, H5O_storage_virtual_name_seg_t *src)
{
    H5O_storage_virtual_name_seg_t  *tmp_dst   = NULL;
    H5O_storage_virtual_name_seg_t  *p_src     = src;
    H5O_storage_virtual_name_seg_t **p_dst     = &tmp_dst;
    herr_t                           ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(dst);

    /* Walk over parsed name, duplicating it */
    while (p_src) {
        /* Allocate name segment struct */
        if (NULL == (*p_dst = H5FL_CALLOC(H5O_storage_virtual_name_seg_t)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "unable to allocate name segment struct");

        /* Duplicate name segment */
        if (p_src->name_segment) {
            if (NULL == ((*p_dst)->name_segment = H5MM_strdup(p_src->name_segment)))
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "unable to duplicate name segment");
        } /* end if */

        /* Advance pointers */
        p_src = p_src->next;
        p_dst = &(*p_dst)->next;
    } /* end while */

    /* Set dst */
    *dst    = tmp_dst;
    tmp_dst = NULL;

done:
    if (tmp_dst) {
        assert(ret_value < 0);
        H5D_virtual_free_parsed_name(tmp_dst);
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__virtual_copy_parsed_name() */

/*-------------------------------------------------------------------------
 * Function:    H5D_virtual_free_parsed_name
 *
 * Purpose:     Frees the provided parsed name.
 *
 * Return:      void
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D_virtual_free_parsed_name(H5O_storage_virtual_name_seg_t *name_seg)
{
    H5O_storage_virtual_name_seg_t *next_seg;
    herr_t                          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    /* Walk name segments, freeing them */
    while (name_seg) {
        (void)H5MM_xfree(name_seg->name_segment);
        next_seg = name_seg->next;
        (void)H5FL_FREE(H5O_storage_virtual_name_seg_t, name_seg);
        name_seg = next_seg;
    } /* end while */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D_virtual_free_parsed_name() */

/*-------------------------------------------------------------------------
 * Function:    H5D__virtual_build_source_name
 *
 * Purpose:     Builds a source file or dataset name from a parsed name.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__virtual_build_source_name(char *source_name, const H5O_storage_virtual_name_seg_t *parsed_name,
                               size_t static_strlen, size_t nsubs, hsize_t blockno, char **built_name)
{
    char  *tmp_name  = NULL;    /* Name buffer */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(source_name);
    assert(built_name);

    /* Check for static name */
    if (nsubs == 0) {
        if (parsed_name)
            *built_name = parsed_name->name_segment;
        else
            *built_name = source_name;
    } /* end if */
    else {
        const H5O_storage_virtual_name_seg_t *name_seg = parsed_name;
        char                                 *p;
        hsize_t                               blockno_down = blockno;
        size_t                                blockno_len  = 1;
        size_t                                name_len;
        size_t                                name_len_rem;
        size_t                                seg_len;
        size_t                                nsubs_rem = nsubs;

        assert(parsed_name);

        /* Calculate length of printed block number */
        do {
            blockno_down /= (hsize_t)10;
            if (blockno_down == 0)
                break;
            blockno_len++;
        } while (1);

        /* Calculate length of name buffer */
        name_len_rem = name_len = static_strlen + (nsubs * blockno_len) + (size_t)1;

        /* Allocate name buffer */
        if (NULL == (tmp_name = (char *)H5MM_malloc(name_len)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "unable to allocate name buffer");
        p = tmp_name;

        /* Build name */
        do {
            /* Add name segment */
            if (name_seg->name_segment) {
                seg_len = strlen(name_seg->name_segment);
                assert(seg_len > 0);
                assert(seg_len < name_len_rem);
                strncpy(p, name_seg->name_segment, name_len_rem);
                name_len_rem -= seg_len;
                p += seg_len;
            } /* end if */

            /* Add block number */
            if (nsubs_rem > 0) {
                assert(blockno_len < name_len_rem);
                if (snprintf(p, name_len_rem, "%llu", (long long unsigned)blockno) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "unable to write block number to string");
                name_len_rem -= blockno_len;
                p += blockno_len;
                nsubs_rem--;
            } /* end if */

            /* Advance name_seg */
            name_seg = name_seg->next;
        } while (name_seg);

        /* Assign built_name */
        *built_name = tmp_name;
        tmp_name    = NULL;
    } /* end else */

done:
    if (tmp_name) {
        assert(ret_value < 0);
        H5MM_free(tmp_name);
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__virtual_build_source_name() */

/*-------------------------------------------------------------------------
 * Function:    H5D__virtual_set_extent_unlim
 *
 * Purpose:     Sets the extent of the virtual dataset by checking the
 *              extents of source datasets where an unlimited selection
 *              matching.  Dimensions that are not unlimited in any
 *              virtual mapping selections are not affected.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__virtual_set_extent_unlim(const H5D_t *dset)
{
    H5O_storage_virtual_t *storage;
    hsize_t                new_dims[H5S_MAX_RANK];
    hsize_t                curr_dims[H5S_MAX_RANK];
    hsize_t                clip_size;
    int                    rank;
    bool                   changed = false; /* Whether the VDS extent changed */
    size_t                 i, j;
    herr_t                 ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(dset);
    assert(dset->shared->layout.storage.type == H5D_VIRTUAL);
    storage = &dset->shared->layout.storage.u.virt;
    assert((storage->view == H5D_VDS_FIRST_MISSING) || (storage->view == H5D_VDS_LAST_AVAILABLE));

    /* Get rank of VDS */
    if ((rank = H5S_GET_EXTENT_NDIMS(dset->shared->space)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "unable to get number of dimensions");

    /* Initialize new_dims to HSIZE_UNDEF */
    for (i = 0; i < (size_t)rank; i++)
        new_dims[i] = HSIZE_UNDEF;

    /* Iterate over mappings */
    for (i = 0; i < storage->list_nused; i++)
        /* Check for unlimited dimension */
        if (storage->list[i].unlim_dim_virtual >= 0) {
            /* Check for "printf" source dataset resolution */
            if (storage->list[i].unlim_dim_source >= 0) {
                /* Non-printf mapping */
                /* Open source dataset */
                if (!storage->list[i].source_dset.dset)
                    if (H5D__virtual_open_source_dset(dset, &storage->list[i],
                                                      &storage->list[i].source_dset) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, FAIL, "unable to open source dataset");

                /* Check if source dataset is open */
                if (storage->list[i].source_dset.dset) {
                    /* Retrieve current source dataset extent and patch mapping
                     */
                    if (H5S_extent_copy(storage->list[i].source_select,
                                        storage->list[i].source_dset.dset->shared->space) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "can't copy source dataspace extent");

                    /* Get source space dimensions */
                    if (H5S_get_simple_extent_dims(storage->list[i].source_select, curr_dims, NULL) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get source space dimensions");

                    /* Check if the source extent in the unlimited dimension
                     * changed since the last time the VDS extent/mapping
                     * was updated */
                    if (curr_dims[storage->list[i].unlim_dim_source] == storage->list[i].unlim_extent_source)
                        /* Use cached result for clip size */
                        clip_size = storage->list[i].clip_size_virtual;
                    else {
                        /* Get size that virtual selection would be clipped to
                         * to match size of source selection within source
                         * extent */
                        clip_size = H5S_hyper_get_clip_extent_match(
                            storage->list[i].source_dset.virtual_select, storage->list[i].source_select,
                            curr_dims[storage->list[i].unlim_dim_source],
                            storage->view == H5D_VDS_FIRST_MISSING);

                        /* If we are setting the extent by the last available
                         * data, clip virtual_select and source_select.  Note
                         * that if we used the cached clip_size above or it
                         * happens to be the same, the virtual selection will
                         * already be clipped to the correct size.  Likewise,
                         * if we used the cached clip_size the source selection
                         * will already be correct. */
                        if (storage->view == H5D_VDS_LAST_AVAILABLE) {
                            if (clip_size != storage->list[i].clip_size_virtual) {
                                /* Close previous clipped virtual selection, if
                                 * any */
                                if (storage->list[i].source_dset.clipped_virtual_select) {
                                    assert(storage->list[i].source_dset.clipped_virtual_select !=
                                           storage->list[i].source_dset.virtual_select);
                                    if (H5S_close(storage->list[i].source_dset.clipped_virtual_select) < 0)
                                        HGOTO_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL,
                                                    "unable to release clipped virtual dataspace");
                                } /* end if */

                                /* Copy virtual selection */
                                if (NULL == (storage->list[i].source_dset.clipped_virtual_select = H5S_copy(
                                                 storage->list[i].source_dset.virtual_select, false, true)))
                                    HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL,
                                                "unable to copy virtual selection");

                                /* Clip virtual selection */
                                if (H5S_hyper_clip_unlim(storage->list[i].source_dset.clipped_virtual_select,
                                                         clip_size))
                                    HGOTO_ERROR(H5E_DATASET, H5E_CANTCLIP, FAIL,
                                                "failed to clip unlimited selection");
                            } /* end if */

                            /* Close previous clipped source selection, if any
                             */
                            if (storage->list[i].source_dset.clipped_source_select) {
                                assert(storage->list[i].source_dset.clipped_source_select !=
                                       storage->list[i].source_select);
                                if (H5S_close(storage->list[i].source_dset.clipped_source_select) < 0)
                                    HGOTO_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL,
                                                "unable to release clipped source dataspace");
                            } /* end if */

                            /* Copy source selection */
                            if (NULL == (storage->list[i].source_dset.clipped_source_select =
                                             H5S_copy(storage->list[i].source_select, false, true)))
                                HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL,
                                            "unable to copy source selection");

                            /* Clip source selection */
                            if (H5S_hyper_clip_unlim(storage->list[i].source_dset.clipped_source_select,
                                                     curr_dims[storage->list[i].unlim_dim_source]))
                                HGOTO_ERROR(H5E_DATASET, H5E_CANTCLIP, FAIL,
                                            "failed to clip unlimited selection");
                        } /* end if */

                        /* Update cached values unlim_extent_source and
                         * clip_size_virtual */
                        storage->list[i].unlim_extent_source = curr_dims[storage->list[i].unlim_dim_source];
                        storage->list[i].clip_size_virtual   = clip_size;
                    } /* end else */
                }     /* end if */
                else
                    clip_size = 0;
            } /* end if */
            else {
                /* printf mapping */
                hsize_t first_missing =
                    0; /* First missing dataset in the current block of missing datasets */

                /* Search for source datasets */
                assert(storage->printf_gap != HSIZE_UNDEF);
                for (j = 0; j <= (storage->printf_gap + first_missing); j++) {
                    /* Check for running out of space in sub_dset array */
                    if (j >= (hsize_t)storage->list[i].sub_dset_nalloc) {
                        if (storage->list[i].sub_dset_nalloc == 0) {
                            /* Allocate sub_dset */
                            if (NULL ==
                                (storage->list[i].sub_dset = (H5O_storage_virtual_srcdset_t *)H5MM_calloc(
                                     H5D_VIRTUAL_DEF_SUB_DSET_SIZE * sizeof(H5O_storage_virtual_srcdset_t))))
                                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                                            "unable to allocate sub dataset array");
                            storage->list[i].sub_dset_nalloc = H5D_VIRTUAL_DEF_SUB_DSET_SIZE;
                        } /* end if */
                        else {
                            H5O_storage_virtual_srcdset_t *tmp_sub_dset;

                            /* Extend sub_dset */
                            if (NULL ==
                                (tmp_sub_dset = (H5O_storage_virtual_srcdset_t *)H5MM_realloc(
                                     storage->list[i].sub_dset, 2 * storage->list[i].sub_dset_nalloc *
                                                                    sizeof(H5O_storage_virtual_srcdset_t))))
                                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                                            "unable to extend sub dataset array");
                            storage->list[i].sub_dset = tmp_sub_dset;

                            /* Clear new space in sub_dset */
                            (void)memset(&storage->list[i].sub_dset[storage->list[i].sub_dset_nalloc], 0,
                                         storage->list[i].sub_dset_nalloc *
                                             sizeof(H5O_storage_virtual_srcdset_t));

                            /* Update sub_dset_nalloc */
                            storage->list[i].sub_dset_nalloc *= 2;
                        } /* end else */
                    }     /* end if */

                    /* Check if the dataset was already opened */
                    if (storage->list[i].sub_dset[j].dset_exists)
                        first_missing = j + 1;
                    else {
                        /* Resolve file name */
                        if (!storage->list[i].sub_dset[j].file_name)
                            if (H5D__virtual_build_source_name(storage->list[i].source_file_name,
                                                               storage->list[i].parsed_source_file_name,
                                                               storage->list[i].psfn_static_strlen,
                                                               storage->list[i].psfn_nsubs, j,
                                                               &storage->list[i].sub_dset[j].file_name) < 0)
                                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL,
                                            "unable to build source file name");

                        /* Resolve dset name */
                        if (!storage->list[i].sub_dset[j].dset_name)
                            if (H5D__virtual_build_source_name(storage->list[i].source_dset_name,
                                                               storage->list[i].parsed_source_dset_name,
                                                               storage->list[i].psdn_static_strlen,
                                                               storage->list[i].psdn_nsubs, j,
                                                               &storage->list[i].sub_dset[j].dset_name) < 0)
                                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL,
                                            "unable to build source dataset name");

                        /* Resolve virtual selection for block */
                        if (!storage->list[i].sub_dset[j].virtual_select)
                            if (NULL ==
                                (storage->list[i].sub_dset[j].virtual_select = H5S_hyper_get_unlim_block(
                                     storage->list[i].source_dset.virtual_select, j)))
                                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL,
                                            "unable to get block in unlimited selection");

                        /* Initialize clipped selections */
                        if (!storage->list[i].sub_dset[j].clipped_source_select)
                            storage->list[i].sub_dset[j].clipped_source_select =
                                storage->list[i].source_select;
                        if (!storage->list[i].sub_dset[j].clipped_virtual_select)
                            storage->list[i].sub_dset[j].clipped_virtual_select =
                                storage->list[i].sub_dset[j].virtual_select;

                        /* Open source dataset */
                        if (H5D__virtual_open_source_dset(dset, &storage->list[i],
                                                          &storage->list[i].sub_dset[j]) < 0)
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, FAIL, "unable to open source dataset");

                        if (storage->list[i].sub_dset[j].dset) {
                            /* Update first_missing */
                            first_missing = j + 1;

                            /* Close source dataset so we don't have huge
                             * numbers of datasets open */
                            if (H5D_close(storage->list[i].sub_dset[j].dset) < 0)
                                HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL,
                                            "unable to close source dataset");
                            storage->list[i].sub_dset[j].dset = NULL;
                        } /* end if */
                    }     /* end else */
                }         /* end for */

                /* Check if the size changed */
                if ((first_missing == (hsize_t)storage->list[i].sub_dset_nused) &&
                    (storage->list[i].clip_size_virtual != HSIZE_UNDEF))
                    /* Use cached clip_size */
                    clip_size = storage->list[i].clip_size_virtual;
                else {
                    /* Check for no datasets */
                    if (first_missing == 0)
                        /* Set clip size to 0 */
                        clip_size = (hsize_t)0;
                    else {
                        hsize_t bounds_start[H5S_MAX_RANK];
                        hsize_t bounds_end[H5S_MAX_RANK];

                        /* Get clip size from selection */
                        if (storage->view == H5D_VDS_LAST_AVAILABLE) {
                            /* Get bounds from last valid virtual selection */
                            if (H5S_SELECT_BOUNDS(
                                    storage->list[i].sub_dset[first_missing - (hsize_t)1].virtual_select,
                                    bounds_start, bounds_end) < 0)
                                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "unable to get selection bounds");

                            /* Set clip_size to bounds_end in unlimited
                             * dimension */
                            clip_size = bounds_end[storage->list[i].unlim_dim_virtual] + (hsize_t)1;
                        } /* end if */
                        else {
                            /* Get bounds from first missing virtual selection
                             */
                            if (H5S_SELECT_BOUNDS(storage->list[i].sub_dset[first_missing].virtual_select,
                                                  bounds_start, bounds_end) < 0)
                                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "unable to get selection bounds");

                            /* Set clip_size to bounds_start in unlimited
                             * dimension */
                            clip_size = bounds_start[storage->list[i].unlim_dim_virtual];
                        } /* end else */
                    }     /* end else */

                    /* Set sub_dset_nused and clip_size_virtual */
                    storage->list[i].sub_dset_nused    = (size_t)first_missing;
                    storage->list[i].clip_size_virtual = clip_size;
                } /* end else */
            }     /* end else */

            /* Update new_dims */
            if ((new_dims[storage->list[i].unlim_dim_virtual] == HSIZE_UNDEF) ||
                (storage->view == H5D_VDS_FIRST_MISSING
                     ? (clip_size < (hsize_t)new_dims[storage->list[i].unlim_dim_virtual])
                     : (clip_size > (hsize_t)new_dims[storage->list[i].unlim_dim_virtual])))
                new_dims[storage->list[i].unlim_dim_virtual] = clip_size;
        } /* end if */

    /* Get current VDS dimensions */
    if (H5S_get_simple_extent_dims(dset->shared->space, curr_dims, NULL) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get VDS dimensions");

    /* Calculate new extent */
    for (i = 0; i < (size_t)rank; i++) {
        if (new_dims[i] == HSIZE_UNDEF)
            new_dims[i] = curr_dims[i];
        else if (new_dims[i] < storage->min_dims[i])
            new_dims[i] = storage->min_dims[i];
        if (new_dims[i] != curr_dims[i])
            changed = true;
    } /* end for */

    /* Update extent if it changed */
    if (changed) {
        /* Update VDS extent */
        if (H5S_set_extent(dset->shared->space, new_dims) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to modify size of dataspace");

        /* Mark the space as dirty, for later writing to the file */
        if (H5F_INTENT(dset->oloc.file) & H5F_ACC_RDWR)
            if (H5D__mark(dset, H5D_MARK_SPACE) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "unable to mark dataspace as dirty");
    } /* end if */

    /* If we did not change the VDS dimensions, there is nothing more to update
     */
    if (changed || (!storage->init && (storage->view == H5D_VDS_FIRST_MISSING))) {
        /* Iterate over mappings again to update source selections and virtual
         * mapping extents */
        for (i = 0; i < storage->list_nused; i++) {
            /* If there is an unlimited dimension, we are setting extent by the
             * minimum of mappings, and the virtual extent in the unlimited
             * dimension has changed since the last time the VDS extent/mapping
             * was updated, we must adjust the selections */
            if ((storage->list[i].unlim_dim_virtual >= 0) && (storage->view == H5D_VDS_FIRST_MISSING) &&
                (new_dims[storage->list[i].unlim_dim_virtual] != storage->list[i].unlim_extent_virtual)) {
                /* Check for "printf" style mapping */
                if (storage->list[i].unlim_dim_source >= 0) {
                    /* Non-printf mapping */
                    /* Close previous clipped virtual selection, if any */
                    if (storage->list[i].source_dset.clipped_virtual_select) {
                        assert(storage->list[i].source_dset.clipped_virtual_select !=
                               storage->list[i].source_dset.virtual_select);
                        if (H5S_close(storage->list[i].source_dset.clipped_virtual_select) < 0)
                            HGOTO_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL,
                                        "unable to release clipped virtual dataspace");
                    } /* end if */

                    /* Copy virtual selection */
                    if (NULL == (storage->list[i].source_dset.clipped_virtual_select =
                                     H5S_copy(storage->list[i].source_dset.virtual_select, false, true)))
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "unable to copy virtual selection");

                    /* Clip space to virtual extent */
                    if (H5S_hyper_clip_unlim(storage->list[i].source_dset.clipped_virtual_select,
                                             new_dims[storage->list[i].unlim_dim_source]))
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTCLIP, FAIL, "failed to clip unlimited selection");

                    /* Get size that source selection will be clipped to to
                     * match size of virtual selection */
                    clip_size =
                        H5S_hyper_get_clip_extent(storage->list[i].source_select,
                                                  storage->list[i].source_dset.clipped_virtual_select, false);

                    /* Check if the clip size changed */
                    if (clip_size != storage->list[i].clip_size_source) {
                        /* Close previous clipped source selection, if any */
                        if (storage->list[i].source_dset.clipped_source_select) {
                            assert(storage->list[i].source_dset.clipped_source_select !=
                                   storage->list[i].source_select);
                            if (H5S_close(storage->list[i].source_dset.clipped_source_select) < 0)
                                HGOTO_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL,
                                            "unable to release clipped source dataspace");
                        } /* end if */

                        /* Copy source selection */
                        if (NULL == (storage->list[i].source_dset.clipped_source_select =
                                         H5S_copy(storage->list[i].source_select, false, true)))
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "unable to copy source selection");

                        /* Clip source selection */
                        if (H5S_hyper_clip_unlim(storage->list[i].source_dset.clipped_source_select,
                                                 clip_size))
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTCLIP, FAIL,
                                        "failed to clip unlimited selection");

                        /* Update cached value clip_size_source */
                        storage->list[i].clip_size_source = clip_size;
                    } /* end if */
                }     /* end if */
                else {
                    /* printf mapping */
                    hsize_t first_inc_block;
                    bool    partial_block;

                    /* Get index of first incomplete block in virtual
                     * selection */
                    first_inc_block = H5S_hyper_get_first_inc_block(
                        storage->list[i].source_dset.virtual_select,
                        new_dims[storage->list[i].unlim_dim_virtual], &partial_block);

                    /* Iterate over sub datasets */
                    for (j = 0; j < storage->list[i].sub_dset_nalloc; j++) {
                        /* Close previous clipped source selection, if any */
                        if (storage->list[i].sub_dset[j].clipped_source_select !=
                            storage->list[i].source_select) {
                            if (storage->list[i].sub_dset[j].clipped_source_select)
                                if (H5S_close(storage->list[i].sub_dset[j].clipped_source_select) < 0)
                                    HGOTO_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL,
                                                "unable to release clipped source dataspace");

                            /* Initialize clipped source selection to point to
                             * base source selection */
                            storage->list[i].sub_dset[j].clipped_source_select =
                                storage->list[i].source_select;
                        } /* end if */

                        /* Close previous clipped virtual selection, if any */
                        if (storage->list[i].sub_dset[j].clipped_virtual_select !=
                            storage->list[i].sub_dset[j].virtual_select) {
                            if (storage->list[i].sub_dset[j].clipped_virtual_select)
                                if (H5S_close(storage->list[i].sub_dset[j].clipped_virtual_select) < 0)
                                    HGOTO_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL,
                                                "unable to release clipped virtual dataspace");

                            /* Initialize clipped virtual selection to point to
                             * unclipped virtual selection */
                            storage->list[i].sub_dset[j].clipped_virtual_select =
                                storage->list[i].sub_dset[j].virtual_select;
                        } /* end if */

                        /* Only initialize clipped selections if it is a
                         * complete block, for incomplete blocks defer to
                         * H5D__virtual_pre_io() as we may not have a valid
                         * source extent here.  For unused blocks we will never
                         * need clipped selections (until the extent is
                         * recalculated in this function). */
                        if (j >= (size_t)first_inc_block) {
                            /* Clear clipped source and virtual selections */
                            storage->list[i].sub_dset[j].clipped_source_select  = NULL;
                            storage->list[i].sub_dset[j].clipped_virtual_select = NULL;
                        } /* end if */
                    }     /* end for */
                }         /* end else */

                /* Update cached value unlim_extent_virtual */
                storage->list[i].unlim_extent_virtual = new_dims[storage->list[i].unlim_dim_virtual];
            } /* end if */

            /* Update top level virtual_select and clipped_virtual_select
             * extents */
            if (H5S_set_extent(storage->list[i].source_dset.virtual_select, new_dims) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to modify size of dataspace");
            if ((storage->list[i].source_dset.clipped_virtual_select !=
                 storage->list[i].source_dset.virtual_select) &&
                storage->list[i].source_dset.clipped_virtual_select)
                if (H5S_set_extent(storage->list[i].source_dset.clipped_virtual_select, new_dims) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to modify size of dataspace");

            /* Update sub dataset virtual_select and clipped_virtual_select
             * extents */
            for (j = 0; j < storage->list[i].sub_dset_nalloc; j++)
                if (storage->list[i].sub_dset[j].virtual_select) {
                    if (H5S_set_extent(storage->list[i].sub_dset[j].virtual_select, new_dims) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to modify size of dataspace");
                    if ((storage->list[i].sub_dset[j].clipped_virtual_select !=
                         storage->list[i].sub_dset[j].virtual_select) &&
                        storage->list[i].sub_dset[j].clipped_virtual_select)
                        if (H5S_set_extent(storage->list[i].sub_dset[j].clipped_virtual_select, new_dims) < 0)
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL,
                                        "unable to modify size of dataspace");
                } /* end if */
                else
                    assert(!storage->list[i].sub_dset[j].clipped_virtual_select);
        } /* end for */
    }     /* end if */

    /* Mark layout as fully initialized */
    storage->init = true;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__virtual_set_extent_unlim() */

/*-------------------------------------------------------------------------
 * Function:    H5D__virtual_init_all
 *
 * Purpose:     Finishes initializing layout in preparation for I/O.
 *              Only necessary if H5D__virtual_set_extent_unlim() has not
 *              been called yet.  Initializes clipped_virtual_select and
 *              clipped_source_select for all mappings in this layout.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__virtual_init_all(const H5D_t *dset)
{
    H5O_storage_virtual_t *storage;
    hsize_t                virtual_dims[H5S_MAX_RANK];
    hsize_t                source_dims[H5S_MAX_RANK];
    hsize_t                clip_size;
    size_t                 i, j;
    herr_t                 ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(dset);
    assert(dset->shared->layout.storage.type == H5D_VIRTUAL);
    storage = &dset->shared->layout.storage.u.virt;
    assert((storage->view == H5D_VDS_FIRST_MISSING) || (storage->view == H5D_VDS_LAST_AVAILABLE));

    /* Get current VDS dimensions */
    if (H5S_get_simple_extent_dims(dset->shared->space, virtual_dims, NULL) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get VDS dimensions");

    /* Iterate over mappings */
    for (i = 0; i < storage->list_nused; i++)
        /* Check for unlimited dimension */
        if (storage->list[i].unlim_dim_virtual >= 0) {
            /* Check for "printf" source dataset resolution */
            if (storage->list[i].unlim_dim_source >= 0) {
                /* Non-printf mapping */
                /* Open source dataset */
                if (!storage->list[i].source_dset.dset)
                    if (H5D__virtual_open_source_dset(dset, &storage->list[i],
                                                      &storage->list[i].source_dset) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, FAIL, "unable to open source dataset");

                /* Check if source dataset is open */
                if (storage->list[i].source_dset.dset) {
                    /* Retrieve current source dataset extent and patch mapping
                     */
                    if (H5S_extent_copy(storage->list[i].source_select,
                                        storage->list[i].source_dset.dset->shared->space) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "can't copy source dataspace extent");

                    /* Get source space dimensions */
                    if (H5S_get_simple_extent_dims(storage->list[i].source_select, source_dims, NULL) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get source space dimensions");

                    /* Get size that source selection would be clipped to to
                     * match size of virtual selection */
                    clip_size = H5S_hyper_get_clip_extent_match(
                        storage->list[i].source_select, storage->list[i].source_dset.virtual_select,
                        virtual_dims[storage->list[i].unlim_dim_virtual], false);

                    /* Close previous clipped virtual selection, if any */
                    if (storage->list[i].source_dset.clipped_virtual_select) {
                        assert(storage->list[i].source_dset.clipped_virtual_select !=
                               storage->list[i].source_dset.virtual_select);
                        if (H5S_close(storage->list[i].source_dset.clipped_virtual_select) < 0)
                            HGOTO_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL,
                                        "unable to release clipped virtual dataspace");
                    } /* end if */

                    /* Copy virtual selection */
                    if (NULL == (storage->list[i].source_dset.clipped_virtual_select =
                                     H5S_copy(storage->list[i].source_dset.virtual_select, false, true)))
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "unable to copy virtual selection");

                    /* Close previous clipped source selection, if any */
                    if (storage->list[i].source_dset.clipped_source_select) {
                        assert(storage->list[i].source_dset.clipped_source_select !=
                               storage->list[i].source_select);
                        if (H5S_close(storage->list[i].source_dset.clipped_source_select) < 0)
                            HGOTO_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL,
                                        "unable to release clipped source dataspace");
                    } /* end if */

                    /* Copy source selection */
                    if (NULL == (storage->list[i].source_dset.clipped_source_select =
                                     H5S_copy(storage->list[i].source_select, false, true)))
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "unable to copy source selection");

                    /* Check if the clip size is within the current extent of
                     * the source dataset */
                    if (clip_size <= source_dims[storage->list[i].unlim_dim_source]) {
                        /* Clip virtual selection to extent */
                        if (H5S_hyper_clip_unlim(storage->list[i].source_dset.clipped_virtual_select,
                                                 virtual_dims[storage->list[i].unlim_dim_virtual]))
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTCLIP, FAIL,
                                        "failed to clip unlimited selection");

                        /* Clip source selection to clip_size */
                        if (H5S_hyper_clip_unlim(storage->list[i].source_dset.clipped_source_select,
                                                 clip_size))
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTCLIP, FAIL,
                                        "failed to clip unlimited selection");
                    } /* end if */
                    else {
                        /* Get size that virtual selection will be clipped to to
                         * match size of source selection within source extent
                         */
                        clip_size = H5S_hyper_get_clip_extent_match(
                            storage->list[i].source_dset.virtual_select, storage->list[i].source_select,
                            source_dims[storage->list[i].unlim_dim_source], false);

                        /* Clip virtual selection to clip_size */
                        if (H5S_hyper_clip_unlim(storage->list[i].source_dset.clipped_virtual_select,
                                                 clip_size))
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTCLIP, FAIL,
                                        "failed to clip unlimited selection");

                        /* Clip source selection to extent */
                        if (H5S_hyper_clip_unlim(storage->list[i].source_dset.clipped_source_select,
                                                 source_dims[storage->list[i].unlim_dim_source]))
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTCLIP, FAIL,
                                        "failed to clip unlimited selection");
                    } /* end else */
                }     /* end if */
                else {
                    assert(!storage->list[i].source_dset.clipped_virtual_select);
                    assert(!storage->list[i].source_dset.clipped_source_select);
                } /* end else */
            }     /* end if */
            else {
                /* printf mapping */
                size_t sub_dset_max;
                bool   partial_block;

                /* Get number of sub-source datasets in current extent */
                sub_dset_max = (size_t)H5S_hyper_get_first_inc_block(
                    storage->list[i].source_dset.virtual_select,
                    virtual_dims[storage->list[i].unlim_dim_virtual], &partial_block);
                if (partial_block)
                    sub_dset_max++;

                /* Allocate or grow the sub_dset array if necessary */
                if (!storage->list[i].sub_dset) {
                    /* Allocate sub_dset array */
                    if (NULL == (storage->list[i].sub_dset = (H5O_storage_virtual_srcdset_t *)H5MM_calloc(
                                     sub_dset_max * sizeof(H5O_storage_virtual_srcdset_t))))
                        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                                    "unable to allocate sub dataset array");

                    /* Update sub_dset_nalloc */
                    storage->list[i].sub_dset_nalloc = sub_dset_max;
                } /* end if */
                else if (sub_dset_max > storage->list[i].sub_dset_nalloc) {
                    H5O_storage_virtual_srcdset_t *tmp_sub_dset;

                    /* Extend sub_dset array */
                    if (NULL == (tmp_sub_dset = (H5O_storage_virtual_srcdset_t *)H5MM_realloc(
                                     storage->list[i].sub_dset,
                                     sub_dset_max * sizeof(H5O_storage_virtual_srcdset_t))))
                        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "unable to extend sub dataset array");
                    storage->list[i].sub_dset = tmp_sub_dset;

                    /* Clear new space in sub_dset */
                    (void)memset(&storage->list[i].sub_dset[storage->list[i].sub_dset_nalloc], 0,
                                 (sub_dset_max - storage->list[i].sub_dset_nalloc) *
                                     sizeof(H5O_storage_virtual_srcdset_t));

                    /* Update sub_dset_nalloc */
                    storage->list[i].sub_dset_nalloc = sub_dset_max;
                } /* end if */

                /* Iterate over sub dsets */
                for (j = 0; j < sub_dset_max; j++) {
                    /* Resolve file name */
                    if (!storage->list[i].sub_dset[j].file_name)
                        if (H5D__virtual_build_source_name(
                                storage->list[i].source_file_name, storage->list[i].parsed_source_file_name,
                                storage->list[i].psfn_static_strlen, storage->list[i].psfn_nsubs, j,
                                &storage->list[i].sub_dset[j].file_name) < 0)
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "unable to build source file name");

                    /* Resolve dset name */
                    if (!storage->list[i].sub_dset[j].dset_name)
                        if (H5D__virtual_build_source_name(
                                storage->list[i].source_dset_name, storage->list[i].parsed_source_dset_name,
                                storage->list[i].psdn_static_strlen, storage->list[i].psdn_nsubs, j,
                                &storage->list[i].sub_dset[j].dset_name) < 0)
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL,
                                        "unable to build source dataset name");

                    /* Resolve virtual selection for block */
                    if (!storage->list[i].sub_dset[j].virtual_select)
                        if (NULL == (storage->list[i].sub_dset[j].virtual_select = H5S_hyper_get_unlim_block(
                                         storage->list[i].source_dset.virtual_select, j)))
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL,
                                        "unable to get block in unlimited selection");

                    /* Close previous clipped source selection, if any */
                    if (storage->list[i].sub_dset[j].clipped_source_select !=
                        storage->list[i].source_select) {
                        if (storage->list[i].sub_dset[j].clipped_source_select)
                            if (H5S_close(storage->list[i].sub_dset[j].clipped_source_select) < 0)
                                HGOTO_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL,
                                            "unable to release clipped source dataspace");

                        /* Initialize clipped source selection to point to base
                         * source selection */
                        storage->list[i].sub_dset[j].clipped_source_select = storage->list[i].source_select;
                    } /* end if */

                    /* Close previous clipped virtual selection, if any */
                    if (storage->list[i].sub_dset[j].clipped_virtual_select !=
                        storage->list[i].sub_dset[j].virtual_select) {
                        if (storage->list[i].sub_dset[j].clipped_virtual_select)
                            if (H5S_close(storage->list[i].sub_dset[j].clipped_virtual_select) < 0)
                                HGOTO_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL,
                                            "unable to release clipped virtual dataspace");

                        /* Initialize clipped virtual selection to point to
                         * unclipped virtual selection */
                        storage->list[i].sub_dset[j].clipped_virtual_select =
                            storage->list[i].sub_dset[j].virtual_select;
                    } /* end if */

                    /* Clear clipped selections if this is a partial block,
                     * defer calculation of real clipped selections to
                     * H5D__virtual_pre_io() as we may not have a valid source
                     * extent here */
                    if ((j == (sub_dset_max - 1)) && partial_block) {
                        /* Clear clipped source and virtual selections */
                        storage->list[i].sub_dset[j].clipped_source_select  = NULL;
                        storage->list[i].sub_dset[j].clipped_virtual_select = NULL;
                    } /* end else */
                    /* Note we do not need to open the source file, this will
                     * happen later in H5D__virtual_pre_io() */
                } /* end for */

                /* Update sub_dset_nused */
                storage->list[i].sub_dset_nused = sub_dset_max;
            } /* end else */
        }     /* end if */
        else {
            /* Limited mapping, just make sure the clipped selections were
             * already set.  Again, no need to open the source file. */
            assert(storage->list[i].source_dset.clipped_virtual_select);
            assert(storage->list[i].source_dset.clipped_source_select);
        } /* end else */

    /* Mark layout as fully initialized */
    storage->init = true;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__virtual_init_all() */

/*-------------------------------------------------------------------------
 * Function:    H5D__virtual_init
 *
 * Purpose:     Initialize the virtual layout information for a dataset.
 *              This is called when the dataset is initialized.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__virtual_init(H5F_t *f, const H5D_t *dset, hid_t dapl_id)
{
    H5O_storage_virtual_t *storage;                      /* Convenience pointer */
    H5P_genplist_t        *dapl;                         /* Data access property list object pointer */
    hssize_t               old_offset[H5O_LAYOUT_NDIMS]; /* Old selection offset (unused) */
    size_t                 i;                            /* Local index variables */
    herr_t                 ret_value = SUCCEED;          /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(dset);
    storage = &dset->shared->layout.storage.u.virt;
    assert(storage->list || (storage->list_nused == 0));

    /* Check that the dimensions of the VDS are large enough */
    if (H5D_virtual_check_min_dims(dset) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                    "virtual dataset dimensions not large enough to contain all limited dimensions in all "
                    "selections");

    /* Patch the virtual selection dataspaces.  Note we always patch the space
     * status because this layout could be from an old version held in the
     * object header message code.  We cannot update that held message because
     * the layout message is constant, so just overwrite the values here (and
     * invalidate other fields by setting storage->init to false below).  Also
     * remove offset from selections.  We only have to update
     * source_space_status and virtual_space_status because others will be based
     * on these and should therefore already have been normalized. */
    for (i = 0; i < storage->list_nused; i++) {
        assert(storage->list[i].sub_dset_nalloc == 0);

        /* Patch extent */
        if (H5S_extent_copy(storage->list[i].source_dset.virtual_select, dset->shared->space) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "can't copy virtual dataspace extent");
        storage->list[i].virtual_space_status = H5O_VIRTUAL_STATUS_CORRECT;

        /* Mark source extent as invalid */
        storage->list[i].source_space_status = H5O_VIRTUAL_STATUS_INVALID;

        /* Normalize offsets, toss out old offset values */
        if (H5S_hyper_normalize_offset(storage->list[i].source_dset.virtual_select, old_offset) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_BADSELECT, FAIL, "unable to normalize dataspace by offset");
        if (H5S_hyper_normalize_offset(storage->list[i].source_select, old_offset) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_BADSELECT, FAIL, "unable to normalize dataspace by offset");
    } /* end for */

    /* Get dataset access property list */
    if (NULL == (dapl = (H5P_genplist_t *)H5I_object(dapl_id)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for dapl ID");

    /* Get view option */
    if (H5P_get(dapl, H5D_ACS_VDS_VIEW_NAME, &storage->view) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get virtual view option");

    /* Get printf gap if view is H5D_VDS_LAST_AVAILABLE, otherwise set to 0 */
    if (storage->view == H5D_VDS_LAST_AVAILABLE) {
        if (H5P_get(dapl, H5D_ACS_VDS_PRINTF_GAP_NAME, &storage->printf_gap) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get virtual printf gap");
    } /* end if */
    else
        storage->printf_gap = (hsize_t)0;

    /* Retrieve VDS file FAPL to layout */
    if (storage->source_fapl <= 0) {
        H5P_genplist_t    *source_fapl  = NULL;           /* Source file FAPL */
        H5F_close_degree_t close_degree = H5F_CLOSE_WEAK; /* Close degree for source files */

        if ((storage->source_fapl = H5F_get_access_plist(f, false)) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get fapl");

        /* Get property list pointer */
        if (NULL == (source_fapl = (H5P_genplist_t *)H5I_object(storage->source_fapl)))
            HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, H5I_INVALID_HID, "not a property list");

        /* Source files must always be opened with H5F_CLOSE_WEAK close degree */
        if (H5P_set(source_fapl, H5F_ACS_CLOSE_DEGREE_NAME, &close_degree) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set file close degree");
    } /* end if */
#ifndef NDEBUG
    else {
        H5P_genplist_t    *source_fapl = NULL; /* Source file FAPL */
        H5F_close_degree_t close_degree;       /* Close degree for source files */

        /* Get property list pointer */
        if (NULL == (source_fapl = (H5P_genplist_t *)H5I_object(storage->source_fapl)))
            HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, H5I_INVALID_HID, "not a property list");

        /* Verify H5F_CLOSE_WEAK close degree is set */
        if (H5P_get(source_fapl, H5F_ACS_CLOSE_DEGREE_NAME, &close_degree) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get file close degree");

        assert(close_degree == H5F_CLOSE_WEAK);
    }  /* end else */
#endif /* NDEBUG */

    /* Copy DAPL to layout */
    if (storage->source_dapl <= 0)
        if ((storage->source_dapl = H5P_copy_plist(dapl, false)) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "can't copy dapl");

    /* Mark layout as not fully initialized (must be done prior to I/O for
     * unlimited/printf selections) */
    storage->init = false;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__virtual_init() */

/*-------------------------------------------------------------------------
 * Function:    H5D__virtual_is_space_alloc
 *
 * Purpose:     Query if space is allocated for layout
 *
 * Return:      true if space is allocated
 *              false if it is not
 *              Negative on failure
 *
 *-------------------------------------------------------------------------
 */
bool
H5D__virtual_is_space_alloc(const H5O_storage_t H5_ATTR_UNUSED *storage)
{
    bool ret_value = false; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Just return true, since the global heap object containing the mappings is
     * created when the layout message is encoded, and nothing else needs to be
     * allocated for virtual datasets.  This also ensures that the library never
     * assumes (falsely) that no data is present in the dataset, causing errors.
     */
    ret_value = true;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__virtual_is_space_alloc() */

/*-------------------------------------------------------------------------
 * Function:    H5D__virtual_is_data_cached
 *
 * Purpose:     Query if raw data is cached for dataset
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static bool
H5D__virtual_is_data_cached(const H5D_shared_t *shared_dset)
{
    const H5O_storage_virtual_t *storage;           /* Convenience pointer */
    size_t                       i, j;              /* Local index variables */
    bool                         ret_value = false; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(shared_dset);
    storage = &shared_dset->layout.storage.u.virt;

    /* Iterate over mappings */
    for (i = 0; i < storage->list_nused; i++)
        /* Check for "printf" source dataset resolution */
        if (storage->list[i].psfn_nsubs || storage->list[i].psdn_nsubs) {
            /* Iterate over sub-source dsets */
            for (j = storage->list[i].sub_dset_io_start; j < storage->list[i].sub_dset_io_end; j++)
                /* Check for cached data in source dset */
                if (storage->list[i].sub_dset[j].dset &&
                    storage->list[i].sub_dset[j].dset->shared->layout.ops->is_data_cached &&
                    storage->list[i].sub_dset[j].dset->shared->layout.ops->is_data_cached(
                        storage->list[i].sub_dset[j].dset->shared))
                    HGOTO_DONE(true);
        } /* end if */
        else if (storage->list[i].source_dset.dset &&
                 storage->list[i].source_dset.dset->shared->layout.ops->is_data_cached &&
                 storage->list[i].source_dset.dset->shared->layout.ops->is_data_cached(
                     storage->list[i].source_dset.dset->shared))
            HGOTO_DONE(true);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__virtual_is_data_cached() */

/*-------------------------------------------------------------------------
 * Function:    H5D__virtual_io_init
 *
 * Purpose:     Performs initialization before any sort of I/O on the raw data
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__virtual_io_init(H5D_io_info_t *io_info, H5D_dset_io_info_t H5_ATTR_UNUSED *dinfo)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Disable selection I/O */
    io_info->use_select_io = H5D_SELECTION_IO_MODE_OFF;
    io_info->no_selection_io_cause |= H5D_SEL_IO_NOT_CONTIGUOUS_OR_CHUNKED_DATASET;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__virtual_io_init() */

/*-------------------------------------------------------------------------
 * Function:    H5D__virtual_pre_io
 *
 * Purpose:     Project all virtual mappings onto mem_space, with the
 *              results stored in projected_mem_space for each mapping.
 *              Opens all source datasets if possible.  The total number
 *              of elements is stored in tot_nelmts.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__virtual_pre_io(H5D_dset_io_info_t *dset_info, H5O_storage_virtual_t *storage, H5S_t *file_space,
                    H5S_t *mem_space, hsize_t *tot_nelmts)
{
    const H5D_t *dset = dset_info->dset;     /* Local pointer to dataset info */
    hssize_t     select_nelmts;              /* Number of elements in selection */
    hsize_t      bounds_start[H5S_MAX_RANK]; /* Selection bounds start */
    hsize_t      bounds_end[H5S_MAX_RANK];   /* Selection bounds end */
    int          rank        = 0;
    bool         bounds_init = false; /* Whether bounds_start, bounds_end, and rank are valid */
    size_t       i, j, k;             /* Local index variables */
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(storage);
    assert(mem_space);
    assert(file_space);
    assert(tot_nelmts);

    /* Initialize layout if necessary */
    if (!storage->init)
        if (H5D__virtual_init_all(dset) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't initialize virtual layout");

    /* Initialize tot_nelmts */
    *tot_nelmts = 0;

    /* Iterate over mappings */
    for (i = 0; i < storage->list_nused; i++) {
        /* Sanity check that the virtual space has been patched by now */
        assert(storage->list[i].virtual_space_status == H5O_VIRTUAL_STATUS_CORRECT);

        /* Check for "printf" source dataset resolution */
        if (storage->list[i].psfn_nsubs || storage->list[i].psdn_nsubs) {
            bool partial_block;

            assert(storage->list[i].unlim_dim_virtual >= 0);

            /* Get selection bounds if necessary */
            if (!bounds_init) {
                /* Get rank of VDS */
                if ((rank = H5S_GET_EXTENT_NDIMS(dset->shared->space)) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "unable to get number of dimensions");

                /* Get selection bounds */
                if (H5S_SELECT_BOUNDS(file_space, bounds_start, bounds_end) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "unable to get selection bounds");

                /* Adjust bounds_end to represent the extent just enclosing them
                 * (add 1) */
                for (j = 0; j < (size_t)rank; j++)
                    bounds_end[j]++;

                /* Bounds are now initialized */
                bounds_init = true;
            } /* end if */

            /* Get index of first block in virtual selection */
            storage->list[i].sub_dset_io_start =
                (size_t)H5S_hyper_get_first_inc_block(storage->list[i].source_dset.virtual_select,
                                                      bounds_start[storage->list[i].unlim_dim_virtual], NULL);

            /* Get index of first block outside of virtual selection */
            storage->list[i].sub_dset_io_end = (size_t)H5S_hyper_get_first_inc_block(
                storage->list[i].source_dset.virtual_select, bounds_end[storage->list[i].unlim_dim_virtual],
                &partial_block);
            if (partial_block)
                storage->list[i].sub_dset_io_end++;
            if (storage->list[i].sub_dset_io_end > storage->list[i].sub_dset_nused)
                storage->list[i].sub_dset_io_end = storage->list[i].sub_dset_nused;

            /* Iterate over sub-source dsets */
            for (j = storage->list[i].sub_dset_io_start; j < storage->list[i].sub_dset_io_end; j++) {
                /* Check for clipped virtual selection */
                if (!storage->list[i].sub_dset[j].clipped_virtual_select) {
                    hsize_t start[H5S_MAX_RANK];
                    /* This should only be NULL if this is a partial block */
                    assert((j == (storage->list[i].sub_dset_io_end - 1)) && partial_block);

                    /* If the source space status is not correct, we must try to
                     * open the source dataset to patch it */
                    if (storage->list[i].source_space_status != H5O_VIRTUAL_STATUS_CORRECT) {
                        assert(!storage->list[i].sub_dset[j].dset);
                        if (H5D__virtual_open_source_dset(dset, &storage->list[i],
                                                          &storage->list[i].sub_dset[j]) < 0)
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, FAIL, "unable to open source dataset");
                    } /* end if */

                    /* If we obtained a valid source space, we must create
                     * clipped source and virtual selections, otherwise we
                     * cannot do this and we will leave them NULL.  This doesn't
                     * hurt anything because we can't do I/O because the dataset
                     * must not have been found. */
                    if (storage->list[i].source_space_status == H5O_VIRTUAL_STATUS_CORRECT) {
                        hsize_t tmp_dims[H5S_MAX_RANK];
                        hsize_t vbounds_end[H5S_MAX_RANK];

                        /* Get bounds of virtual selection */
                        if (H5S_SELECT_BOUNDS(storage->list[i].sub_dset[j].virtual_select, tmp_dims,
                                              vbounds_end) < 0)
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "unable to get selection bounds");

                        assert(bounds_init);

                        /* Convert bounds to extent (add 1) */
                        for (k = 0; k < (size_t)rank; k++)
                            vbounds_end[k]++;

                        /* Temporarily set extent of virtual selection to bounds */
                        if (H5S_set_extent(storage->list[i].sub_dset[j].virtual_select, vbounds_end) < 0)
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL,
                                        "unable to modify size of dataspace");

                        /* Get current VDS dimensions */
                        if (H5S_get_simple_extent_dims(dset->shared->space, tmp_dims, NULL) < 0)
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get VDS dimensions");

                        /* Copy virtual selection */
                        if (NULL == (storage->list[i].sub_dset[j].clipped_virtual_select =
                                         H5S_copy(storage->list[i].sub_dset[j].virtual_select, false, true)))
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "unable to copy virtual selection");

                        /* Clip virtual selection to real virtual extent */
                        (void)memset(start, 0, sizeof(start));
                        if (H5S_select_hyperslab(storage->list[i].sub_dset[j].clipped_virtual_select,
                                                 H5S_SELECT_AND, start, NULL, tmp_dims, NULL) < 0)
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTSELECT, FAIL, "unable to clip hyperslab");

                        /* Project intersection of virtual space and clipped
                         * virtual space onto source space (create
                         * clipped_source_select) */
                        if (H5S_select_project_intersection(
                                storage->list[i].sub_dset[j].virtual_select, storage->list[i].source_select,
                                storage->list[i].sub_dset[j].clipped_virtual_select,
                                &storage->list[i].sub_dset[j].clipped_source_select, true) < 0)
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTCLIP, FAIL,
                                        "can't project virtual intersection onto memory space");

                        /* Set extents of virtual_select and
                         * clipped_virtual_select to virtual extent */
                        if (H5S_set_extent(storage->list[i].sub_dset[j].virtual_select, tmp_dims) < 0)
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL,
                                        "unable to modify size of dataspace");
                        if (H5S_set_extent(storage->list[i].sub_dset[j].clipped_virtual_select, tmp_dims) < 0)
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL,
                                        "unable to modify size of dataspace");
                    } /* end if */
                }     /* end if */

                /* Only continue if we managed to obtain a
                 * clipped_virtual_select */
                if (storage->list[i].sub_dset[j].clipped_virtual_select) {
                    /* Project intersection of file space and mapping virtual space
                     * onto memory space */
                    if (H5S_select_project_intersection(
                            file_space, mem_space, storage->list[i].sub_dset[j].clipped_virtual_select,
                            &storage->list[i].sub_dset[j].projected_mem_space, true) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTCLIP, FAIL,
                                    "can't project virtual intersection onto memory space");

                    /* Check number of elements selected */
                    if ((select_nelmts = (hssize_t)H5S_GET_SELECT_NPOINTS(
                             storage->list[i].sub_dset[j].projected_mem_space)) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTCOUNT, FAIL,
                                    "unable to get number of elements in selection");

                    /* Check if anything is selected */
                    if (select_nelmts > (hssize_t)0) {
                        /* Open source dataset */
                        if (!storage->list[i].sub_dset[j].dset)
                            /* Try to open dataset */
                            if (H5D__virtual_open_source_dset(dset, &storage->list[i],
                                                              &storage->list[i].sub_dset[j]) < 0)
                                HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, FAIL,
                                            "unable to open source dataset");

                        /* If the source dataset is not open, mark the selected
                         * elements as zero so projected_mem_space is freed */
                        if (!storage->list[i].sub_dset[j].dset)
                            select_nelmts = (hssize_t)0;
                    } /* end if */

                    /* If there are not elements selected in this mapping, free
                     * projected_mem_space, otherwise update tot_nelmts */
                    if (select_nelmts == (hssize_t)0) {
                        if (H5S_close(storage->list[i].sub_dset[j].projected_mem_space) < 0)
                            HGOTO_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL,
                                        "can't close projected memory space");
                        storage->list[i].sub_dset[j].projected_mem_space = NULL;
                    } /* end if */
                    else
                        *tot_nelmts += (hsize_t)select_nelmts;
                } /* end if */
            }     /* end for */
        }         /* end if */
        else {
            if (storage->list[i].source_dset.clipped_virtual_select) {
                /* Project intersection of file space and mapping virtual space onto
                 * memory space */
                if (H5S_select_project_intersection(
                        file_space, mem_space, storage->list[i].source_dset.clipped_virtual_select,
                        &storage->list[i].source_dset.projected_mem_space, true) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTCLIP, FAIL,
                                "can't project virtual intersection onto memory space");

                /* Check number of elements selected, add to tot_nelmts */
                if ((select_nelmts = (hssize_t)H5S_GET_SELECT_NPOINTS(
                         storage->list[i].source_dset.projected_mem_space)) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTCOUNT, FAIL,
                                "unable to get number of elements in selection");

                /* Check if anything is selected */
                if (select_nelmts > (hssize_t)0) {
                    /* Open source dataset */
                    if (!storage->list[i].source_dset.dset)
                        /* Try to open dataset */
                        if (H5D__virtual_open_source_dset(dset, &storage->list[i],
                                                          &storage->list[i].source_dset) < 0)
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, FAIL, "unable to open source dataset");

                    /* If the source dataset is not open, mark the selected elements
                     * as zero so projected_mem_space is freed */
                    if (!storage->list[i].source_dset.dset)
                        select_nelmts = (hssize_t)0;
                } /* end if */

                /* If there are not elements selected in this mapping, free
                 * projected_mem_space, otherwise update tot_nelmts */
                if (select_nelmts == (hssize_t)0) {
                    if (H5S_close(storage->list[i].source_dset.projected_mem_space) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close projected memory space");
                    storage->list[i].source_dset.projected_mem_space = NULL;
                } /* end if */
                else
                    *tot_nelmts += (hsize_t)select_nelmts;
            } /* end if */
            else {
                /* If there is no clipped_dim_virtual, this must be an unlimited
                 * selection whose dataset was not found in the last call to
                 * H5Dget_space().  Do not attempt to open it as this might
                 * affect the extent and we are not going to recalculate it
                 * here. */
                assert(storage->list[i].unlim_dim_virtual >= 0);
                assert(!storage->list[i].source_dset.dset);
            } /* end else */
        }     /* end else */
    }         /* end for */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__virtual_pre_io() */

/*-------------------------------------------------------------------------
 * Function:    H5D__virtual_post_io
 *
 * Purpose:     Frees memory structures allocated by H5D__virtual_pre_io.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__virtual_post_io(H5O_storage_virtual_t *storage)
{
    size_t i, j;                /* Local index variables */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(storage);

    /* Iterate over mappings */
    for (i = 0; i < storage->list_nused; i++)
        /* Check for "printf" source dataset resolution */
        if (storage->list[i].psfn_nsubs || storage->list[i].psdn_nsubs) {
            /* Iterate over sub-source dsets */
            for (j = storage->list[i].sub_dset_io_start; j < storage->list[i].sub_dset_io_end; j++)
                /* Close projected memory space */
                if (storage->list[i].sub_dset[j].projected_mem_space) {
                    if (H5S_close(storage->list[i].sub_dset[j].projected_mem_space) < 0)
                        HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close temporary space");
                    storage->list[i].sub_dset[j].projected_mem_space = NULL;
                } /* end if */
        }         /* end if */
        else
            /* Close projected memory space */
            if (storage->list[i].source_dset.projected_mem_space) {
                if (H5S_close(storage->list[i].source_dset.projected_mem_space) < 0)
                    HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close temporary space");
                storage->list[i].source_dset.projected_mem_space = NULL;
            } /* end if */

    /* Note the lack of a done: label.  This is because there are no HGOTO_ERROR
     * calls.  If one is added, a done: label must also be added */
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__virtual_post_io() */

/*-------------------------------------------------------------------------
 * Function:    H5D__virtual_read_one
 *
 * Purpose:     Read from a single source dataset in a virtual dataset.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__virtual_read_one(H5D_dset_io_info_t *dset_info, H5O_storage_virtual_srcdset_t *source_dset)
{
    H5S_t             *projected_src_space = NULL; /* File space for selection in a single source dataset */
    H5D_dset_io_info_t source_dinfo;               /* Dataset info for source dataset read */
    herr_t             ret_value = SUCCEED;        /* Return value */

    FUNC_ENTER_PACKAGE

    assert(source_dset);

    /* Only perform I/O if there is a projected memory space, otherwise there
     * were no elements in the projection or the source dataset could not be
     * opened */
    if (source_dset->projected_mem_space) {
        assert(source_dset->dset);
        assert(source_dset->clipped_source_select);

        /* Project intersection of file space and mapping virtual space onto
         * mapping source space */
        if (H5S_select_project_intersection(source_dset->clipped_virtual_select,
                                            source_dset->clipped_source_select, dset_info->file_space,
                                            &projected_src_space, true) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTCLIP, FAIL,
                        "can't project virtual intersection onto source space");

        {
            /* Initialize source_dinfo */
            source_dinfo.dset        = source_dset->dset;
            source_dinfo.mem_space   = source_dset->projected_mem_space;
            source_dinfo.file_space  = projected_src_space;
            source_dinfo.buf.vp      = dset_info->buf.vp;
            source_dinfo.mem_type_id = dset_info->type_info.dst_type_id;

            /* Read in the point (with the custom VL memory allocator) */
            if (H5D__read(1, &source_dinfo) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "can't read source dataset");
        }

        /* Close projected_src_space */
        if (H5S_close(projected_src_space) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close projected source space");
        projected_src_space = NULL;
    } /* end if */

done:
    /* Release allocated resources */
    if (projected_src_space) {
        assert(ret_value < 0);
        if (H5S_close(projected_src_space) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close projected source space");
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__virtual_read_one() */

/*-------------------------------------------------------------------------
 * Function:    H5D__virtual_read
 *
 * Purpose:     Read from a virtual dataset.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__virtual_read(H5D_io_info_t H5_ATTR_NDEBUG_UNUSED *io_info, H5D_dset_io_info_t *dset_info)
{
    H5O_storage_virtual_t *storage;             /* Convenient pointer into layout struct */
    hsize_t                tot_nelmts;          /* Total number of elements mapped to mem_space */
    H5S_t                 *fill_space = NULL;   /* Space to fill with fill value */
    size_t                 nelmts;              /* Number of elements to process */
    size_t                 i, j;                /* Local index variables */
    herr_t                 ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(io_info);
    assert(dset_info);
    assert(dset_info->buf.vp);
    assert(dset_info->mem_space);
    assert(dset_info->file_space);

    storage = &(dset_info->dset->shared->layout.storage.u.virt);
    assert((storage->view == H5D_VDS_FIRST_MISSING) || (storage->view == H5D_VDS_LAST_AVAILABLE));

    /* Initialize nelmts */
    nelmts = H5S_GET_SELECT_NPOINTS(dset_info->file_space);

#ifdef H5_HAVE_PARALLEL
    /* Parallel reads are not supported (yet) */
    if (H5F_HAS_FEATURE(dset_info->dset->oloc.file, H5FD_FEAT_HAS_MPI))
        HGOTO_ERROR(H5E_DATASET, H5E_UNSUPPORTED, FAIL, "parallel reads not supported on virtual datasets");
#endif /* H5_HAVE_PARALLEL */

    /* Prepare for I/O operation */
    if (H5D__virtual_pre_io(dset_info, storage, dset_info->file_space, dset_info->mem_space, &tot_nelmts) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTCLIP, FAIL, "unable to prepare for I/O operation");

    /* Iterate over mappings */
    for (i = 0; i < storage->list_nused; i++) {
        /* Sanity check that the virtual space has been patched by now */
        assert(storage->list[i].virtual_space_status == H5O_VIRTUAL_STATUS_CORRECT);

        /* Check for "printf" source dataset resolution */
        if (storage->list[i].psfn_nsubs || storage->list[i].psdn_nsubs) {
            /* Iterate over sub-source dsets */
            for (j = storage->list[i].sub_dset_io_start; j < storage->list[i].sub_dset_io_end; j++)
                if (H5D__virtual_read_one(dset_info, &storage->list[i].sub_dset[j]) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "unable to read source dataset");
        } /* end if */
        else
            /* Read from source dataset */
            if (H5D__virtual_read_one(dset_info, &storage->list[i].source_dset) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "unable to read source dataset");
    } /* end for */

    /* Fill unmapped part of buffer with fill value */
    if (tot_nelmts < nelmts) {
        H5D_fill_value_t fill_status; /* Fill value status */

        /* Check the fill value status */
        if (H5P_is_fill_value_defined(&dset_info->dset->shared->dcpl_cache.fill, &fill_status) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't tell if fill value defined");

        /* Always write fill value to memory buffer unless it is undefined */
        if (fill_status != H5D_FILL_VALUE_UNDEFINED) {
            /* Start with fill space equal to memory space */
            if (NULL == (fill_space = H5S_copy(dset_info->mem_space, false, true)))
                HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "unable to copy memory selection");

            /* Iterate over mappings */
            for (i = 0; i < storage->list_nused; i++)
                /* Check for "printf" source dataset resolution */
                if (storage->list[i].psfn_nsubs || storage->list[i].psdn_nsubs) {
                    /* Iterate over sub-source dsets */
                    for (j = storage->list[i].sub_dset_io_start; j < storage->list[i].sub_dset_io_end; j++)
                        if (storage->list[i].sub_dset[j].projected_mem_space)
                            if (H5S_select_subtract(fill_space,
                                                    storage->list[i].sub_dset[j].projected_mem_space) < 0)
                                HGOTO_ERROR(H5E_DATASET, H5E_CANTCLIP, FAIL, "unable to clip fill selection");
                } /* end if */
                else if (storage->list[i].source_dset.projected_mem_space)
                    /* Subtract projected memory space from fill space */
                    if (H5S_select_subtract(fill_space, storage->list[i].source_dset.projected_mem_space) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTCLIP, FAIL, "unable to clip fill selection");

            /* Write fill values to memory buffer */
            if (H5D__fill(dset_info->dset->shared->dcpl_cache.fill.buf, dset_info->dset->shared->type,
                          dset_info->buf.vp, dset_info->type_info.mem_type, fill_space) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "filling buf failed");

#ifndef NDEBUG
            /* Make sure the total number of elements written (including fill
             * values) >= nelmts */
            {
                hssize_t select_nelmts; /* Number of elements in selection */

                /* Get number of elements in fill dataspace */
                if ((select_nelmts = (hssize_t)H5S_GET_SELECT_NPOINTS(fill_space)) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTCOUNT, FAIL,
                                "unable to get number of elements in selection");

                /* Verify number of elements is correct.  Note that since we
                 * don't check for overlap we can't assert that these are equal
                 */
                assert((tot_nelmts + (hsize_t)select_nelmts) >= nelmts);
            } /* end block */
#endif        /* NDEBUG */
        }     /* end if */
    }         /* end if */

done:
    /* Cleanup I/O operation */
    if (H5D__virtual_post_io(storage) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't cleanup I/O operation");

    /* Close fill space */
    if (fill_space)
        if (H5S_close(fill_space) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close fill space");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__virtual_read() */

/*-------------------------------------------------------------------------
 * Function:    H5D__virtual_write_one
 *
 * Purpose:     Write to a single source dataset in a virtual dataset.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__virtual_write_one(H5D_dset_io_info_t *dset_info, H5O_storage_virtual_srcdset_t *source_dset)
{
    H5S_t             *projected_src_space = NULL; /* File space for selection in a single source dataset */
    H5D_dset_io_info_t source_dinfo;               /* Dataset info for source dataset write */
    herr_t             ret_value = SUCCEED;        /* Return value */

    FUNC_ENTER_PACKAGE

    assert(source_dset);

    /* Only perform I/O if there is a projected memory space, otherwise there
     * were no elements in the projection */
    if (source_dset->projected_mem_space) {
        assert(source_dset->dset);
        assert(source_dset->clipped_source_select);

        /* In the future we may wish to extent this implementation to extend
         * source datasets if a write to a virtual dataset goes past the current
         * extent in the unlimited dimension.  -NAF */
        /* Project intersection of file space and mapping virtual space onto
         * mapping source space */
        if (H5S_select_project_intersection(source_dset->clipped_virtual_select,
                                            source_dset->clipped_source_select, dset_info->file_space,
                                            &projected_src_space, true) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTCLIP, FAIL,
                        "can't project virtual intersection onto source space");

        {
            /* Initialize source_dinfo */
            source_dinfo.dset        = source_dset->dset;
            source_dinfo.mem_space   = source_dset->projected_mem_space;
            source_dinfo.file_space  = projected_src_space;
            source_dinfo.buf.cvp     = dset_info->buf.cvp;
            source_dinfo.mem_type_id = dset_info->type_info.dst_type_id;

            /* Read in the point (with the custom VL memory allocator) */
            if (H5D__write(1, &source_dinfo) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "can't read source dataset");
        }

        /* Close projected_src_space */
        if (H5S_close(projected_src_space) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close projected source space");
        projected_src_space = NULL;
    } /* end if */

done:
    /* Release allocated resources */
    if (projected_src_space) {
        assert(ret_value < 0);
        if (H5S_close(projected_src_space) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close projected source space");
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__virtual_write_one() */

/*-------------------------------------------------------------------------
 * Function:    H5D__virtual_write
 *
 * Purpose:     Write to a virtual dataset.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__virtual_write(H5D_io_info_t H5_ATTR_NDEBUG_UNUSED *io_info, H5D_dset_io_info_t *dset_info)
{
    H5O_storage_virtual_t *storage;             /* Convenient pointer into layout struct */
    hsize_t                tot_nelmts;          /* Total number of elements mapped to mem_space */
    size_t                 nelmts;              /* Number of elements to process */
    size_t                 i, j;                /* Local index variables */
    herr_t                 ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(io_info);
    assert(dset_info);
    assert(dset_info->buf.cvp);
    assert(dset_info->mem_space);
    assert(dset_info->file_space);

    storage = &(dset_info->dset->shared->layout.storage.u.virt);
    assert((storage->view == H5D_VDS_FIRST_MISSING) || (storage->view == H5D_VDS_LAST_AVAILABLE));

    /* Initialize nelmts */
    nelmts = H5S_GET_SELECT_NPOINTS(dset_info->file_space);

#ifdef H5_HAVE_PARALLEL
    /* Parallel writes are not supported (yet) */
    if (H5F_HAS_FEATURE(dset_info->dset->oloc.file, H5FD_FEAT_HAS_MPI))
        HGOTO_ERROR(H5E_DATASET, H5E_UNSUPPORTED, FAIL, "parallel writes not supported on virtual datasets");
#endif /* H5_HAVE_PARALLEL */

    /* Prepare for I/O operation */
    if (H5D__virtual_pre_io(dset_info, storage, dset_info->file_space, dset_info->mem_space, &tot_nelmts) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTCLIP, FAIL, "unable to prepare for I/O operation");

    /* Fail if there are unmapped parts of the selection as they would not be
     * written */
    if (tot_nelmts != nelmts)
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL,
                    "write requested to unmapped portion of virtual dataset");

    /* Iterate over mappings */
    for (i = 0; i < storage->list_nused; i++) {
        /* Sanity check that virtual space has been patched by now */
        assert(storage->list[i].virtual_space_status == H5O_VIRTUAL_STATUS_CORRECT);

        /* Check for "printf" source dataset resolution */
        if (storage->list[i].psfn_nsubs || storage->list[i].psdn_nsubs) {
            /* Iterate over sub-source dsets */
            for (j = storage->list[i].sub_dset_io_start; j < storage->list[i].sub_dset_io_end; j++)
                if (H5D__virtual_write_one(dset_info, &storage->list[i].sub_dset[j]) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "unable to write to source dataset");
        } /* end if */
        else
            /* Write to source dataset */
            if (H5D__virtual_write_one(dset_info, &storage->list[i].source_dset) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "unable to write to source dataset");
    } /* end for */

done:
    /* Cleanup I/O operation */
    if (H5D__virtual_post_io(storage) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't cleanup I/O operation");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__virtual_write() */

/*-------------------------------------------------------------------------
 * Function:    H5D__virtual_flush
 *
 * Purpose:     Writes all dirty data to disk.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__virtual_flush(H5D_t *dset)
{
    H5O_storage_virtual_t *storage;             /* Convenient pointer into layout struct */
    size_t                 i, j;                /* Local index variables */
    herr_t                 ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(dset);

    storage = &dset->shared->layout.storage.u.virt;

    /* Flush only open datasets */
    for (i = 0; i < storage->list_nused; i++)
        /* Check for "printf" source dataset resolution */
        if (storage->list[i].psfn_nsubs || storage->list[i].psdn_nsubs) {
            /* Iterate over sub-source dsets */
            for (j = 0; j < storage->list[i].sub_dset_nused; j++)
                if (storage->list[i].sub_dset[j].dset)
                    /* Flush source dataset */
                    if (H5D__flush_real(storage->list[i].sub_dset[j].dset) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "unable to flush source dataset");
        } /* end if */
        else if (storage->list[i].source_dset.dset)
            /* Flush source dataset */
            if (H5D__flush_real(storage->list[i].source_dset.dset) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "unable to flush source dataset");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__virtual_flush() */

/*-------------------------------------------------------------------------
 * Function:    H5D__virtual_hold_source_dset_files
 *
 * Purpose:     Hold open the source files that are open, during a refresh event
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__virtual_hold_source_dset_files(const H5D_t *dset, H5D_virtual_held_file_t **head)
{
    H5O_storage_virtual_t   *storage;             /* Convenient pointer into layout struct */
    H5D_virtual_held_file_t *tmp;                 /* Temporary held file node */
    size_t                   i;                   /* Local index variable */
    herr_t                   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(dset);
    assert(head && NULL == *head);

    /* Set the convenience pointer */
    storage = &dset->shared->layout.storage.u.virt;

    /* Hold only files for open datasets */
    for (i = 0; i < storage->list_nused; i++)
        /* Check for "printf" source dataset resolution */
        if (storage->list[i].psfn_nsubs || storage->list[i].psdn_nsubs) {
            size_t j; /* Local index variable */

            /* Iterate over sub-source dsets */
            for (j = 0; j < storage->list[i].sub_dset_nused; j++)
                if (storage->list[i].sub_dset[j].dset) {
                    /* Hold open the file */
                    H5F_INCR_NOPEN_OBJS(storage->list[i].sub_dset[j].dset->oloc.file);

                    /* Allocate a node for this file */
                    if (NULL == (tmp = H5FL_MALLOC(H5D_virtual_held_file_t)))
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate held file node");

                    /* Set up node & connect to list */
                    tmp->file = storage->list[i].sub_dset[j].dset->oloc.file;
                    tmp->next = *head;
                    *head     = tmp;
                } /* end if */
        }         /* end if */
        else if (storage->list[i].source_dset.dset) {
            /* Hold open the file */
            H5F_INCR_NOPEN_OBJS(storage->list[i].source_dset.dset->oloc.file);

            /* Allocate a node for this file */
            if (NULL == (tmp = H5FL_MALLOC(H5D_virtual_held_file_t)))
                HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate held file node");

            /* Set up node & connect to list */
            tmp->file = storage->list[i].source_dset.dset->oloc.file;
            tmp->next = *head;
            *head     = tmp;
        } /* end if */

done:
    if (ret_value < 0)
        /* Release hold on files and delete list on error */
        if (*head && H5D__virtual_release_source_dset_files(*head) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "can't release source datasets' files held open");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__virtual_hold_source_dset_files() */

/*-------------------------------------------------------------------------
 * Function:    H5D__virtual_refresh_source_dset
 *
 * Purpose:     Refresh a source dataset
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__virtual_refresh_source_dset(H5D_t **dset)
{
    hid_t          temp_id   = H5I_INVALID_HID; /* Temporary dataset identifier */
    H5VL_object_t *vol_obj   = NULL;            /* VOL object stored with the ID */
    herr_t         ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(dset && *dset);

    /* Get a temporary identifier for this source dataset */
    if ((temp_id = H5VL_wrap_register(H5I_DATASET, *dset, false)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTREGISTER, FAIL, "can't register (temporary) source dataset ID");

    /* Refresh source dataset */
    if (H5D__refresh(*dset, temp_id) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTFLUSH, FAIL, "unable to refresh source dataset");

    /* Discard the identifier & replace the dataset */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_remove(temp_id)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTREMOVE, FAIL, "can't unregister source dataset ID");
    if (NULL == (*dset = (H5D_t *)H5VL_object_unwrap(vol_obj)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't retrieve library object from VOL object");
    vol_obj->data = NULL;

done:
    if (vol_obj && H5VL_free_object(vol_obj) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "unable to free VOL object");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__virtual_refresh_source_dset() */

/*-------------------------------------------------------------------------
 * Function:    H5D__virtual_refresh_source_dsets
 *
 * Purpose:     Refresh the source datasets
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__virtual_refresh_source_dsets(H5D_t *dset)
{
    H5O_storage_virtual_t *storage;             /* Convenient pointer into layout struct */
    size_t                 i;                   /* Local index variable */
    herr_t                 ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(dset);

    /* Set convenience pointer */
    storage = &dset->shared->layout.storage.u.virt;

    /* Refresh only open datasets */
    for (i = 0; i < storage->list_nused; i++)
        /* Check for "printf" source dataset resolution */
        if (storage->list[i].psfn_nsubs || storage->list[i].psdn_nsubs) {
            size_t j; /* Local index variable */

            /* Iterate over sub-source datasets */
            for (j = 0; j < storage->list[i].sub_dset_nused; j++)
                /* Check if sub-source dataset is open */
                if (storage->list[i].sub_dset[j].dset)
                    /* Refresh sub-source dataset */
                    if (H5D__virtual_refresh_source_dset(&storage->list[i].sub_dset[j].dset) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTFLUSH, FAIL, "unable to refresh source dataset");
        } /* end if */
        else
            /* Check if source dataset is open */
            if (storage->list[i].source_dset.dset)
                /* Refresh source dataset */
                if (H5D__virtual_refresh_source_dset(&storage->list[i].source_dset.dset) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTFLUSH, FAIL, "unable to refresh source dataset");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__virtual_refresh_source_dsets() */

/*-------------------------------------------------------------------------
 * Function:    H5D__virtual_release_source_dset_files
 *
 * Purpose:     Release the hold on source files that are open, during a refresh event
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__virtual_release_source_dset_files(H5D_virtual_held_file_t *head)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Release hold on files and delete list */
    while (head) {
        H5D_virtual_held_file_t *tmp = head->next; /* Temporary pointer to next node */

        /* Release hold on file */
        H5F_DECR_NOPEN_OBJS(head->file);

        /* Attempt to close the file */
        /* (Should always succeed, since the 'top' source file pointer is
         *      essentially "private" to the virtual dataset, since it wasn't
         *      opened through an API routine -QAK)
         */
        if (H5F_try_close(head->file, NULL) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTCLOSEFILE, FAIL, "problem attempting file close");

        /* Delete node */
        (void)H5FL_FREE(H5D_virtual_held_file_t, head);

        /* Advance to next node */
        head = tmp;
    } /* end while */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__virtual_release_source_dset_files() */
