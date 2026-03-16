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
 * Purpose:     File callbacks for the native VOL connector
 *
 */

/****************/
/* Module Setup */
/****************/

#define H5F_FRIEND /* Suppress error about including H5Fpkg    */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5ACprivate.h" /* Metadata cache                           */
#include "H5Cprivate.h"  /* Cache                                    */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5Fpkg.h"      /* Files                                    */
#include "H5Gprivate.h"  /* Groups                                   */
#include "H5Iprivate.h"  /* IDs                                      */
#include "H5MFprivate.h" /* File memory management                   */
#include "H5Pprivate.h"  /* Property lists                           */
#include "H5PBprivate.h" /* Page buffering                           */
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
 * Function:    H5VL__native_file_create
 *
 * Purpose:     Handles the file create callback
 *
 * Return:      Success:    file pointer
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5VL__native_file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id,
                         hid_t H5_ATTR_UNUSED dxpl_id, void H5_ATTR_UNUSED **req)
{
    H5F_t *new_file  = NULL;
    void  *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    /* Adjust bit flags by turning on the creation bit and making sure that
     * the EXCL or TRUNC bit is set.  All newly-created files are opened for
     * reading and writing.
     */
    if (0 == (flags & (H5F_ACC_EXCL | H5F_ACC_TRUNC)))
        flags |= H5F_ACC_EXCL; /* default */
    flags |= H5F_ACC_RDWR | H5F_ACC_CREAT;

    /* Create the file */
    if (NULL == (new_file = H5F_open(name, flags, fcpl_id, fapl_id)))
        HGOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, NULL, "unable to create file");
    new_file->id_exists = true;

    ret_value = (void *)new_file;

done:
    if (NULL == ret_value && new_file)
        if (H5F__close(new_file) < 0)
            HDONE_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, NULL, "problems closing file");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_file_create() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__native_file_open
 *
 * Purpose:     Handles the file open callback
 *
 * Return:      Success:    file pointer
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5VL__native_file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t H5_ATTR_UNUSED dxpl_id,
                       void H5_ATTR_UNUSED **req)
{
    H5F_t *new_file  = NULL;
    void  *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    /* Open the file */
    if (NULL == (new_file = H5F_open(name, flags, H5P_FILE_CREATE_DEFAULT, fapl_id)))
        HGOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, NULL, "unable to open file");
    new_file->id_exists = true;

    ret_value = (void *)new_file;

done:
    if (NULL == ret_value && new_file && H5F_try_close(new_file, NULL) < 0)
        HDONE_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, NULL, "problems closing file");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_file_open() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__native_file_get
 *
 * Purpose:     Handles the file get callback
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL__native_file_get(void *obj, H5VL_file_get_args_t *args, hid_t H5_ATTR_UNUSED dxpl_id,
                      void H5_ATTR_UNUSED **req)
{
    H5F_t *f         = NULL;    /* File struct */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    switch (args->op_type) {
        /* "get container info" */
        case H5VL_FILE_GET_CONT_INFO: {
            if (H5F__get_cont_info((H5F_t *)obj, args->args.get_cont_info.info) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "can't get file container info");

            break;
        }

        /* H5Fget_access_plist */
        case H5VL_FILE_GET_FAPL: {
            if ((args->args.get_fapl.fapl_id = H5F_get_access_plist((H5F_t *)obj, true)) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "can't get file access property list");

            break;
        }

        /* H5Fget_create_plist */
        case H5VL_FILE_GET_FCPL: {
            H5P_genplist_t *plist; /* Property list */

            f = (H5F_t *)obj;
            if (NULL == (plist = (H5P_genplist_t *)H5I_object(f->shared->fcpl_id)))
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a property list");

            /* Create the property list object to return */
            if ((args->args.get_fcpl.fcpl_id = H5P_copy_plist(plist, true)) < 0)
                HGOTO_ERROR(H5E_PLIST, H5E_CANTINIT, FAIL, "unable to copy file creation properties");

            break;
        }

        /* H5Fget_intent */
        case H5VL_FILE_GET_INTENT: {
            f = (H5F_t *)obj;

            /* HDF5 uses some flags internally that users don't know about.
             * Simplify things for them so that they only get either H5F_ACC_RDWR
             * or H5F_ACC_RDONLY and any SWMR flags.
             */
            if (H5F_INTENT(f) & H5F_ACC_RDWR) {
                *args->args.get_intent.flags = H5F_ACC_RDWR;

                /* Check for SWMR write access on the file */
                if (H5F_INTENT(f) & H5F_ACC_SWMR_WRITE)
                    *args->args.get_intent.flags |= H5F_ACC_SWMR_WRITE;
            } /* end if */
            else {
                *args->args.get_intent.flags = H5F_ACC_RDONLY;

                /* Check for SWMR read access on the file */
                if (H5F_INTENT(f) & H5F_ACC_SWMR_READ)
                    *args->args.get_intent.flags |= H5F_ACC_SWMR_READ;
            } /* end else */

            break;
        }

        /* H5Fget_fileno */
        case H5VL_FILE_GET_FILENO: {
            unsigned long fileno = 0;

            H5F_GET_FILENO((H5F_t *)obj, fileno);
            *args->args.get_fileno.fileno = fileno;

            break;
        }

        /* H5Fget_name */
        case H5VL_FILE_GET_NAME: {
            H5VL_file_get_name_args_t *file_args = &args->args.get_name;

            if (H5VL_native_get_file_struct(obj, file_args->type, &f) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file or file object");

            /* Get length of file name */
            *file_args->file_name_len = strlen(H5F_OPEN_NAME(f));

            /* Populate buffer with name, if given */
            if (file_args->buf) {
                strncpy(file_args->buf, H5F_OPEN_NAME(f),
                        MIN(*file_args->file_name_len + 1, file_args->buf_size));
                if (*file_args->file_name_len >= file_args->buf_size)
                    file_args->buf[file_args->buf_size - 1] = '\0';
            } /* end if */

            break;
        }

        /* H5Fget_obj_count */
        case H5VL_FILE_GET_OBJ_COUNT: {
            if (H5F_get_obj_count((H5F_t *)obj, args->args.get_obj_count.types, true,
                                  args->args.get_obj_count.count) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "can't retrieve object count");

            break;
        }

        /* H5Fget_obj_ids */
        case H5VL_FILE_GET_OBJ_IDS: {
            H5VL_file_get_obj_ids_args_t *file_args = &args->args.get_obj_ids;

            if (H5F_get_obj_ids((H5F_t *)obj, file_args->types, file_args->max_objs, file_args->oid_list,
                                true, file_args->count) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "can't retrieve object IDs");

            break;
        }

        default:
            HGOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't get this type of information");
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_file_get() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__native_file_specific
 *
 * Purpose:     Handles the file specific callback
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL__native_file_specific(void *obj, H5VL_file_specific_args_t *args, hid_t H5_ATTR_UNUSED dxpl_id,
                           void H5_ATTR_UNUSED **req)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    switch (args->op_type) {
        /* H5Fflush */
        case H5VL_FILE_FLUSH: {
            H5F_t *f = NULL; /* File to flush */

            /* Get the file for the object */
            if (H5VL_native_get_file_struct(obj, args->args.flush.obj_type, &f) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file or file object");

            /* Nothing to do if the file is read only. This determination is
             * made at the shared open(2) flags level, implying that opening a
             * file twice, once for read-only and once for read-write, and then
             * calling H5Fflush() with the read-only handle, still causes data
             * to be flushed.
             */
            if (H5F_ACC_RDWR & H5F_INTENT(f)) {
                /* Flush other files, depending on scope */
                if (H5F_SCOPE_GLOBAL == args->args.flush.scope) {
                    /* Call the flush routine for mounted file hierarchies */
                    if (H5F_flush_mounts(f) < 0)
                        HGOTO_ERROR(H5E_FILE, H5E_CANTFLUSH, FAIL, "unable to flush mounted file hierarchy");
                } /* end if */
                else {
                    /* Call the flush routine, for this file */
                    if (H5F__flush(f) < 0)
                        HGOTO_ERROR(H5E_FILE, H5E_CANTFLUSH, FAIL,
                                    "unable to flush file's cached information");
                } /* end else */
            }     /* end if */

            break;
        }

        /* H5Freopen */
        case H5VL_FILE_REOPEN: {
            H5F_t *new_file;

            /* Reopen the file through the VOL connector */
            if (NULL == (new_file = H5F__reopen((H5F_t *)obj)))
                HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "unable to reopen file");
            new_file->id_exists = true;

            /* Set 'out' value */
            *args->args.reopen.file = new_file;

            break;
        }

        /* H5Fis_accessible */
        case H5VL_FILE_IS_ACCESSIBLE: {
            htri_t result;

            if ((result = H5F__is_hdf5(args->args.is_accessible.filename, args->args.is_accessible.fapl_id)) <
                0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "error in HDF5 file check");

            /* Set 'out' value */
            *args->args.is_accessible.accessible = (bool)result;

            break;
        }

        /* H5Fdelete */
        case H5VL_FILE_DELETE: {
            if (H5F__delete(args->args.del.filename, args->args.del.fapl_id) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTDELETEFILE, FAIL, "error in HDF5 file deletion");

            break;
        }

        /* Check if two files are the same */
        case H5VL_FILE_IS_EQUAL: {
            if (!obj || !args->args.is_equal.obj2)
                *args->args.is_equal.same_file = false;
            else
                *args->args.is_equal.same_file =
                    (((H5F_t *)obj)->shared == ((H5F_t *)args->args.is_equal.obj2)->shared);

            break;
        }

        default:
            HGOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid specific operation");
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_file_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__native_file_optional
 *
 * Purpose:     Handles the file optional callback
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL__native_file_optional(void *obj, H5VL_optional_args_t *args, hid_t H5_ATTR_UNUSED dxpl_id,
                           void H5_ATTR_UNUSED **req)
{
    H5F_t                            *f         = (H5F_t *)obj; /* File */
    H5VL_native_file_optional_args_t *opt_args  = args->args;   /* Pointer to native operation's arguments */
    herr_t                            ret_value = SUCCEED;      /* Return value */

    FUNC_ENTER_PACKAGE

    switch (args->op_type) {
        /* H5Fget_filesize */
        case H5VL_NATIVE_FILE_GET_SIZE: {
            haddr_t max_eof_eoa; /* Maximum of the EOA & EOF */
            haddr_t base_addr;   /* Base address for the file */

            /* Get the actual file size & base address */
            if (H5F__get_max_eof_eoa(f, &max_eof_eoa) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "file can't get max eof/eoa ");
            base_addr = H5FD_get_base_addr(f->shared->lf);

            /* Convert relative base address for file to absolute address */
            *opt_args->get_size.size = (hsize_t)(max_eof_eoa + base_addr);

            break;
        }

        /* H5Fget_file_image */
        case H5VL_NATIVE_FILE_GET_FILE_IMAGE: {
            H5VL_native_file_get_file_image_t *gfi_args = &opt_args->get_file_image;

            /* Get file image */
            if (H5F__get_file_image(f, gfi_args->buf, gfi_args->buf_size, gfi_args->image_len) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "get file image failed");

            break;
        }

        /* H5Fget_freespace */
        case H5VL_NATIVE_FILE_GET_FREE_SPACE: {
            H5VL_native_file_get_freespace_t *gfs_args = &opt_args->get_freespace;

            /* Get the actual amount of free space in the file */
            if (H5MF_get_freespace(f, gfs_args->size, NULL) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "unable to check free space for file");

            break;
        }

        /* H5Fget_free_sections */
        case H5VL_NATIVE_FILE_GET_FREE_SECTIONS: {
            H5VL_native_file_get_free_sections_t *gfs_args = &opt_args->get_free_sections;

            /* Go get the free-space section information in the file */
            if (H5MF_get_free_sections(f, gfs_args->type, gfs_args->nsects, gfs_args->sect_info,
                                       gfs_args->sect_count) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "unable to check free space for file");

            break;
        }

        /* H5Fget_info1/2 */
        case H5VL_NATIVE_FILE_GET_INFO: {
            H5VL_native_file_get_info_t *gfi_args = &opt_args->get_info;

            /* Get the file struct. This call is careful to not return the file pointer
             * for the top file in a mount hierarchy.
             */
            if (H5VL_native_get_file_struct(obj, gfi_args->type, &f) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "could not get a file struct");

            /* Get the file info */
            if (H5F__get_info(f, gfi_args->finfo) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "unable to retrieve file info");

            break;
        }

        /* H5Fget_mdc_config */
        case H5VL_NATIVE_FILE_GET_MDC_CONF: {
            /* Get the metadata cache configuration */
            if (H5AC_get_cache_auto_resize_config(f->shared->cache, opt_args->get_mdc_config.config) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "can't get metadata cache configuration");

            break;
        }

        /* H5Fget_mdc_hit_rate */
        case H5VL_NATIVE_FILE_GET_MDC_HR: {
            /* Get the current hit rate */
            if (H5AC_get_cache_hit_rate(f->shared->cache, opt_args->get_mdc_hit_rate.hit_rate) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "can't get metadata cache hit rate");

            break;
        }

        /* H5Fget_mdc_size */
        case H5VL_NATIVE_FILE_GET_MDC_SIZE: {
            H5VL_native_file_get_mdc_size_t *gms_args = &opt_args->get_mdc_size;

            /* Get the size data */
            if (H5AC_get_cache_size(f->shared->cache, gms_args->max_size, gms_args->min_clean_size,
                                    gms_args->cur_size, gms_args->cur_num_entries) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "can't get metadata cache size");

            break;
        }

        /* H5Fget_vfd_handle */
        case H5VL_NATIVE_FILE_GET_VFD_HANDLE: {
            H5VL_native_file_get_vfd_handle_t *gvh_args = &opt_args->get_vfd_handle;

            /* Retrieve the VFD handle for the file */
            if (H5F_get_vfd_handle(f, gvh_args->fapl_id, gvh_args->file_handle) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "can't retrieve VFD handle");

            break;
        }

        /* H5Fclear_elink_file_cache */
        case H5VL_NATIVE_FILE_CLEAR_ELINK_CACHE: {
            /* Release the EFC */
            if (f->shared->efc)
                if (H5F__efc_release(f->shared->efc) < 0)
                    HGOTO_ERROR(H5E_FILE, H5E_CANTRELEASE, FAIL, "can't release external file cache");

            break;
        }

        /* H5Freset_mdc_hit_rate_stats */
        case H5VL_NATIVE_FILE_RESET_MDC_HIT_RATE: {
            /* Reset the hit rate statistic */
            if (H5AC_reset_cache_hit_rate_stats(f->shared->cache) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTSET, FAIL, "can't reset cache hit rate");

            break;
        }

        /* H5Fset_mdc_config */
        case H5VL_NATIVE_FILE_SET_MDC_CONFIG: {
            /* Set the metadata cache configuration  */
            if (H5AC_set_cache_auto_resize_config(f->shared->cache, opt_args->set_mdc_config.config) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTSET, FAIL, "can't set metadata cache configuration");

            break;
        }

        /* H5Fget_metadata_read_retry_info */
        case H5VL_NATIVE_FILE_GET_METADATA_READ_RETRY_INFO: {
            if (H5F_get_metadata_read_retry_info(f, opt_args->get_metadata_read_retry_info.info) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "can't get metadata read retry info");

            break;
        }

        /* H5Fstart_swmr_write */
        case H5VL_NATIVE_FILE_START_SWMR_WRITE: {
            if (H5F__start_swmr_write(f) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTSET, FAIL, "can't start SWMR write");

            break;
        }

        /* H5Fstart_mdc_logging */
        case H5VL_NATIVE_FILE_START_MDC_LOGGING: {
            /* Call mdc logging function */
            if (H5C_start_logging(f->shared->cache) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_LOGGING, FAIL, "unable to start mdc logging");

            break;
        }

        /* H5Fstop_mdc_logging */
        case H5VL_NATIVE_FILE_STOP_MDC_LOGGING: {
            /* Call mdc logging function */
            if (H5C_stop_logging(f->shared->cache) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_LOGGING, FAIL, "unable to stop mdc logging");

            break;
        }

        /* H5Fget_mdc_logging_status */
        case H5VL_NATIVE_FILE_GET_MDC_LOGGING_STATUS: {
            H5VL_native_file_get_mdc_logging_status_t *gmls_args = &opt_args->get_mdc_logging_status;

            /* Call mdc logging function */
            if (H5C_get_logging_status(f->shared->cache, gmls_args->is_enabled,
                                       gmls_args->is_currently_logging) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_LOGGING, FAIL, "unable to get logging status");

            break;
        }

        /* H5Fformat_convert */
        case H5VL_NATIVE_FILE_FORMAT_CONVERT: {
            /* Convert the format */
            if (H5F__format_convert(f) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTCONVERT, FAIL, "can't convert file format");

            break;
        }

        /* H5Freset_page_buffering_stats */
        case H5VL_NATIVE_FILE_RESET_PAGE_BUFFERING_STATS: {
            /* Sanity check */
            if (NULL == f->shared->page_buf)
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "page buffering not enabled on file");

            /* Reset the statistics */
            if (H5PB_reset_stats(f->shared->page_buf) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "can't reset stats for page buffering");

            break;
        }

        /* H5Fget_page_buffering_stats */
        case H5VL_NATIVE_FILE_GET_PAGE_BUFFERING_STATS: {
            H5VL_native_file_get_page_buffering_stats_t *gpbs_args = &opt_args->get_page_buffering_stats;

            /* Sanity check */
            if (NULL == f->shared->page_buf)
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "page buffering not enabled on file");

            /* Get the statistics */
            if (H5PB_get_stats(f->shared->page_buf, gpbs_args->accesses, gpbs_args->hits, gpbs_args->misses,
                               gpbs_args->evictions, gpbs_args->bypasses) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "can't retrieve stats for page buffering");

            break;
        }

        /* H5Fget_mdc_image_info */
        case H5VL_NATIVE_FILE_GET_MDC_IMAGE_INFO: {
            H5VL_native_file_get_mdc_image_info_t *gmii_args = &opt_args->get_mdc_image_info;

            /* Go get the address and size of the cache image */
            if (H5AC_get_mdc_image_info(f->shared->cache, gmii_args->addr, gmii_args->len) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "can't retrieve cache image info");

            break;
        }

        /* H5Fget_eoa */
        case H5VL_NATIVE_FILE_GET_EOA: {
            haddr_t rel_eoa; /* Relative address of EOA */

            /* This routine will work only for drivers with this feature enabled.*/
            /* We might introduce a new feature flag in the future */
            if (!H5F_HAS_FEATURE(f, H5FD_FEAT_SUPPORTS_SWMR_IO))
                HGOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL,
                            "must use a SWMR-compatible VFD for this public routine");

            /* The real work */
            if (HADDR_UNDEF == (rel_eoa = H5F_get_eoa(f, H5FD_MEM_DEFAULT)))
                HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "get_eoa request failed");

            /* Set return value */
            /* (Note compensating for base address subtraction in internal routine) */
            *opt_args->get_eoa.eoa = rel_eoa + H5F_get_base_addr(f);

            break;
        }

        /* H5Fincrement_filesize */
        case H5VL_NATIVE_FILE_INCR_FILESIZE: {
            haddr_t max_eof_eoa; /* Maximum of the relative EOA & EOF */

            /* This public routine will work only for drivers with this feature enabled.*/
            /* We might introduce a new feature flag in the future */
            if (!H5F_HAS_FEATURE(f, H5FD_FEAT_SUPPORTS_SWMR_IO))
                HGOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL,
                            "must use a SWMR-compatible VFD for this public routine");

            /* Get the maximum of EOA and EOF */
            if (H5F__get_max_eof_eoa(f, &max_eof_eoa) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "file can't get max eof/eoa ");

            /* Set EOA to the maximum value + increment */
            if (H5F__set_eoa(f, H5FD_MEM_DEFAULT, max_eof_eoa + opt_args->increment_filesize.increment) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTSET, FAIL, "driver set_eoa request failed");

            break;
        }

        /* H5Fset_latest_format, H5Fset_libver_bounds */
        case H5VL_NATIVE_FILE_SET_LIBVER_BOUNDS: {
            H5VL_native_file_set_libver_bounds_t *slb_args = &opt_args->set_libver_bounds;

            /* Call internal set_libver_bounds function */
            if (H5F__set_libver_bounds(f, slb_args->low, slb_args->high) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTSET, FAIL, "cannot set low/high bounds");

            break;
        }

        /* H5Fget_dset_no_attrs_hint */
        case H5VL_NATIVE_FILE_GET_MIN_DSET_OHDR_FLAG: {
            *opt_args->get_min_dset_ohdr_flag.minimize = H5F_GET_MIN_DSET_OHDR(f);

            break;
        }

        /* H5Fset_dset_no_attrs_hint */
        case H5VL_NATIVE_FILE_SET_MIN_DSET_OHDR_FLAG: {
            if (H5F_set_min_dset_ohdr(f, opt_args->set_min_dset_ohdr_flag.minimize) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTSET, FAIL,
                            "cannot set file's dataset object header minimization flag");

            break;
        }

#ifdef H5_HAVE_PARALLEL
        /* H5Fget_mpi_atomicity */
        case H5VL_NATIVE_FILE_GET_MPI_ATOMICITY: {
            if (H5F__get_mpi_atomicity(f, opt_args->get_mpi_atomicity.flag) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "cannot get MPI atomicity");

            break;
        }

        /* H5Fset_mpi_atomicity */
        case H5VL_NATIVE_FILE_SET_MPI_ATOMICITY: {
            if (H5F__set_mpi_atomicity(f, opt_args->set_mpi_atomicity.flag) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTSET, FAIL, "cannot set MPI atomicity");

            break;
        }
#endif /* H5_HAVE_PARALLEL */

        /* Finalize H5Fopen */
        case H5VL_NATIVE_FILE_POST_OPEN: {
            /* Call package routine */
            if (H5F__post_open(f) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't finish opening file");
            break;
        }

        default:
            HGOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid optional operation");
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_file_optional() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__native_file_close
 *
 * Purpose:     Handles the file close callback
 *
 * Return:      Success:    SUCCEED
 *              Failure:    FAIL (file will not be closed)
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL__native_file_close(void *file, hid_t H5_ATTR_UNUSED dxpl_id, void H5_ATTR_UNUSED **req)
{
    int    nref;
    H5F_t *f         = (H5F_t *)file;
    hid_t  file_id   = H5I_INVALID_HID;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* This routine should only be called when a file ID's ref count drops to zero */
    assert(f->shared == NULL || H5F_ID_EXISTS(f));

    if (f->shared == NULL)
        f = H5FL_FREE(H5F_t, f);

    else {

        /* Flush file if this is the last reference to this id and we have write
         * intent, unless it will be flushed by the "shared" file being closed.
         * This is only necessary to replicate previous behaviour, and could be
         * disabled by an option/property to improve performance.
         */
        if ((H5F_NREFS(f) > 1) && (H5F_INTENT(f) & H5F_ACC_RDWR)) {
            /* Get the file ID corresponding to the H5F_t struct */
            if (H5I_find_id(f, H5I_FILE, &file_id) < 0 || H5I_INVALID_HID == file_id)
                HGOTO_ERROR(H5E_ID, H5E_CANTGET, FAIL, "invalid ID");

            /* Get the number of references outstanding for this file ID */
            if ((nref = H5I_get_ref(file_id, false)) < 0)
                HGOTO_ERROR(H5E_ID, H5E_CANTGET, FAIL, "can't get ID ref count");
            if (nref == 1)
                if (H5F__flush(f) < 0)
                    HGOTO_ERROR(H5E_FILE, H5E_CANTFLUSH, FAIL, "unable to flush cache");
        } /* end if */

        /* Close the file */
        if (H5F__close(f) < 0)
            HGOTO_ERROR(H5E_FILE, H5E_CANTDEC, FAIL, "can't close file");
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_file_close() */
