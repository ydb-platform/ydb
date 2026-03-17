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
 * Onion Virtual File Driver (VFD)
 *
 * Purpose:     Provide in-file provenance and revision/version control.
 */

/* This source code file is part of the H5FD driver module */
#include "H5FDdrvr_module.h"

#include "H5private.h"      /* Generic Functions           */
#include "H5Eprivate.h"     /* Error handling              */
#include "H5Fprivate.h"     /* Files                       */
#include "H5FDprivate.h"    /* File drivers                */
#include "H5FDonion.h"      /* Onion file driver           */
#include "H5FDonion_priv.h" /* Onion file driver internals */
#include "H5FDsec2.h"       /* Sec2 file driver         */
#include "H5FLprivate.h"    /* Free Lists                  */
#include "H5Iprivate.h"     /* IDs                         */
#include "H5MMprivate.h"    /* Memory management           */

/* The driver identification number, initialized at runtime */
static hid_t H5FD_ONION_g = 0;

/******************************************************************************
 *
 * Structure:   H5FD_onion_t
 *
 * Purpose:     Store information required to manage an onionized file.
 *              This structure is created when such a file is "opened" and
 *              discarded when it is "closed".
 *
 * pu
 *
 *      Instance of H5FD_t which contains fields common to all VFDs.
 *      It must be the first item in this structure, since at higher levels,
 *      this structure will be treated as an instance of H5FD_t.
 *
 * fa
 *
 *      Instance of `H5FD_onion_fapl_info_t` containing the configuration data
 *      needed to "open" the HDF5 file.
 *
 * original_file
 *
 *      VFD handle for the original HDF5 file.
 *
 * onion_file
 *
 *      VFD handle for the onion file.
 *      NULL if not set to use the single, separate storage target.
 *
 * recovery_file
 *
 *      VFD handle for the history recovery file. This file is a backup of
 *      the existing history when an existing onion file is opened in RW mode.
 *
 * recovery_file_name
 *
 *      String allocated and populated on file-open in write mode and freed on
 *      file-close, stores the path/name of the 'recovery' file. The file
 *      created at this location is to be removed upon successful file-close
 *      from write mode.
 *
 * is_open_rw
 *
 *      Remember whether the file was opened in a read-write mode.
 *
 * align_history_on_pages
 *
 *      Remember whether onion-writes must be aligned to page boundaries.
 *
 * header
 *
 *      In-memory copy of the onion history data header.
 *
 * history
 *
 *      In-memory copy of the onion history.
 *
 * curr_rev_record
 *
 *      Record for the currently open revision.
 *
 * rev_index
 *
 *      Index for maintaining modified pages (RW mode only).
 *      Pointer is NULL when the file is not opened in write mode.
 *      Pointer is allocated on open and must be freed on close.
 *      Contents must be merged with the revision record's archival index prior
 *      to commitment of history to backing store.
 *
 * onion_eof
 *
 *      Last byte in the onion file.
 *
 * origin_eof
 *
 *     Size of the original HDF5 file.
 *
 * logical_eoa
 *
 *     Address of first byte past addressed space in the logical 'file'
 *     presented by this VFD.
 *
 * logical_eof
 *
 *     Address of first byte past last byte in the logical 'file' presented
 *     by this VFD.
 *     Must be copied into the revision record on close to write onion data.
 *
 ******************************************************************************
 */
typedef struct H5FD_onion_t {
    H5FD_t                 pub;
    H5FD_onion_fapl_info_t fa;
    bool                   is_open_rw;
    bool                   align_history_on_pages;

    /* Onion-related files */
    H5FD_t *original_file;
    H5FD_t *onion_file;
    H5FD_t *recovery_file;
    char   *recovery_file_name;

    /* Onion data structures */
    H5FD_onion_header_t          header;
    H5FD_onion_history_t         history;
    H5FD_onion_revision_record_t curr_rev_record;
    H5FD_onion_revision_index_t *rev_index;

    /* End of addresses and files */
    haddr_t onion_eof;
    haddr_t origin_eof;
    haddr_t logical_eoa;
    haddr_t logical_eof;
} H5FD_onion_t;

H5FL_DEFINE_STATIC(H5FD_onion_t);

#define MAXADDR (((haddr_t)1 << (8 * sizeof(HDoff_t) - 1)) - 1)

#define H5FD_CTL_GET_NUM_REVISIONS 20001

/* Prototypes */
static herr_t  H5FD__onion_close(H5FD_t *);
static haddr_t H5FD__onion_get_eoa(const H5FD_t *, H5FD_mem_t);
static haddr_t H5FD__onion_get_eof(const H5FD_t *, H5FD_mem_t);
static H5FD_t *H5FD__onion_open(const char *, unsigned int, hid_t, haddr_t);
static herr_t  H5FD__onion_read(H5FD_t *, H5FD_mem_t, hid_t, haddr_t, size_t, void *);
static herr_t  H5FD__onion_set_eoa(H5FD_t *, H5FD_mem_t, haddr_t);
static herr_t  H5FD__onion_term(void);
static herr_t  H5FD__onion_write(H5FD_t *, H5FD_mem_t, hid_t, haddr_t, size_t, const void *);

static herr_t  H5FD__onion_open_rw(H5FD_onion_t *, unsigned int, haddr_t, bool new_open);
static herr_t  H5FD__onion_sb_encode(H5FD_t *_file, char *name /*out*/, unsigned char *buf /*out*/);
static herr_t  H5FD__onion_sb_decode(H5FD_t *_file, const char *name, const unsigned char *buf);
static hsize_t H5FD__onion_sb_size(H5FD_t *_file);
static herr_t  H5FD__onion_ctl(H5FD_t *_file, uint64_t op_code, uint64_t flags,
                               const void H5_ATTR_UNUSED *input, void H5_ATTR_UNUSED **output);
static herr_t  H5FD__get_onion_revision_count(H5FD_t *file, uint64_t *revision_count);

/* Temporary */
H5_DLL herr_t H5FD__onion_write_final_history(H5FD_onion_t *file);

static const H5FD_class_t H5FD_onion_g = {
    H5FD_CLASS_VERSION,             /* struct version       */
    H5FD_ONION_VALUE,               /* value                */
    "onion",                        /* name                 */
    MAXADDR,                        /* maxaddr              */
    H5F_CLOSE_WEAK,                 /* fc_degree            */
    H5FD__onion_term,               /* terminate            */
    H5FD__onion_sb_size,            /* sb_size              */
    H5FD__onion_sb_encode,          /* sb_encode            */
    H5FD__onion_sb_decode,          /* sb_decode            */
    sizeof(H5FD_onion_fapl_info_t), /* fapl_size            */
    NULL,                           /* fapl_get             */
    NULL,                           /* fapl_copy            */
    NULL,                           /* fapl_free            */
    0,                              /* dxpl_size            */
    NULL,                           /* dxpl_copy            */
    NULL,                           /* dxpl_free            */
    H5FD__onion_open,               /* open                 */
    H5FD__onion_close,              /* close                */
    NULL,                           /* cmp                  */
    NULL,                           /* query                */
    NULL,                           /* get_type_map         */
    NULL,                           /* alloc                */
    NULL,                           /* free                 */
    H5FD__onion_get_eoa,            /* get_eoa              */
    H5FD__onion_set_eoa,            /* set_eoa              */
    H5FD__onion_get_eof,            /* get_eof              */
    NULL,                           /* get_handle           */
    H5FD__onion_read,               /* read                 */
    H5FD__onion_write,              /* write                */
    NULL,                           /* read_vector          */
    NULL,                           /* write_vector         */
    NULL,                           /* read_selection       */
    NULL,                           /* write_selection      */
    NULL,                           /* flush                */
    NULL,                           /* truncate             */
    NULL,                           /* lock                 */
    NULL,                           /* unlock               */
    NULL,                           /* del                  */
    H5FD__onion_ctl,                /* ctl                  */
    H5FD_FLMAP_DICHOTOMY            /* fl_map               */
};

/*-----------------------------------------------------------------------------
 * Function:    H5FD_onion_init
 *
 * Purpose:     Initialize this driver by registering the driver with the
 *              library.
 *
 * Return:      Success:    The driver ID for the onion driver.
 *              Failure:    Negative
 *-----------------------------------------------------------------------------
 */
hid_t
H5FD_onion_init(void)
{
    hid_t ret_value = H5I_INVALID_HID;

    FUNC_ENTER_NOAPI_NOERR

    if (H5I_VFL != H5I_get_type(H5FD_ONION_g))
        H5FD_ONION_g = H5FD_register(&H5FD_onion_g, sizeof(H5FD_class_t), false);

    /* Set return value */
    ret_value = H5FD_ONION_g;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_onion_init() */

/*-----------------------------------------------------------------------------
 * Function:    H5FD__onion_term
 *
 * Purpose:     Shut down the Onion VFD.
 *
 * Returns:     SUCCEED (Can't fail)
 *-----------------------------------------------------------------------------
 */
static herr_t
H5FD__onion_term(void)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Reset VFL ID */
    H5FD_ONION_g = 0;

    FUNC_LEAVE_NOAPI(SUCCEED)

} /* end H5FD__onion_term() */

/*-----------------------------------------------------------------------------
 * Function:    H5Pget_fapl_onion
 *
 * Purpose:     Copy the Onion configuration information from the FAPL at
 *              `fapl_id` to the destination pointer `fa_out`.
 *
 * Return:      Success: Non-negative value (SUCCEED).
 *              Failure: Negative value (FAIL).
 *-----------------------------------------------------------------------------
 */
herr_t
H5Pget_fapl_onion(hid_t fapl_id, H5FD_onion_fapl_info_t *fa_out)
{
    const H5FD_onion_fapl_info_t *info_ptr  = NULL;
    H5P_genplist_t               *plist     = NULL;
    herr_t                        ret_value = SUCCEED;

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "i*!", fapl_id, fa_out);

    if (NULL == fa_out)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "NULL info-out pointer");

    if (NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Not a valid FAPL ID");

    if (H5FD_ONION != H5P_peek_driver(plist))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Incorrect VFL driver");

    if (NULL == (info_ptr = (const H5FD_onion_fapl_info_t *)H5P_peek_driver_info(plist)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "bad VFL driver info");

    H5MM_memcpy(fa_out, info_ptr, sizeof(H5FD_onion_fapl_info_t));

done:
    FUNC_LEAVE_API(ret_value)

} /* end H5Pget_fapl_onion() */

/*-----------------------------------------------------------------------------
 * Function:    H5Pset_fapl_onion
 *
 * Purpose      Set the file access property list at `fapl_id` to use the
 *              Onion virtual file driver with the given configuration.
 *              The info structure may be modified or deleted after this call,
 *              as its contents are copied into the FAPL.
 *
 * Return:      Success: Non-negative value (SUCCEED).
 *              Failure: Negative value (FAIL).
 *-----------------------------------------------------------------------------
 */
herr_t
H5Pset_fapl_onion(hid_t fapl_id, const H5FD_onion_fapl_info_t *fa)
{
    H5P_genplist_t *fapl           = NULL;
    H5P_genplist_t *backing_fapl   = NULL;
    hid_t           backing_vfd_id = H5I_INVALID_HID;
    herr_t          ret_value      = SUCCEED;

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "i*!", fapl_id, fa);

    if (NULL == (fapl = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Not a valid FAPL ID");
    if (NULL == fa)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "NULL info pointer");
    if (H5FD_ONION_FAPL_INFO_VERSION_CURR != fa->version)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid info version");
    if (!POWER_OF_TWO(fa->page_size))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid info page size");
    if (fa->page_size < 1)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid info page size");

    if (H5P_DEFAULT == fa->backing_fapl_id) {
        if (NULL == (backing_fapl = H5P_object_verify(H5P_FILE_ACCESS_DEFAULT, H5P_FILE_ACCESS)))
            HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "invalid backing fapl id");
    }
    else {
        if (NULL == (backing_fapl = H5P_object_verify(fa->backing_fapl_id, H5P_FILE_ACCESS)))
            HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "invalid backing fapl id");
    }

    /* The only backing fapl that is currently supported is sec2 */
    if ((backing_vfd_id = H5P_peek_driver(backing_fapl)) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "Can't get VFD from fapl");
    if (backing_vfd_id != H5FD_SEC2)
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "Onion VFD only supports sec2 backing store");

    if (H5P_set_driver(fapl, H5FD_ONION, (const void *)fa, NULL) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTSET, FAIL, "Can't set the onion VFD");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_fapl_onion() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__onion_sb_size
 *
 * Purpose:     Returns the size of the private information to be stored in
 *              the superblock.
 *
 * Return:      Success:    The super block driver data size
 *              Failure:    never fails
 *-------------------------------------------------------------------------
 */
static hsize_t
H5FD__onion_sb_size(H5FD_t *_file)
{
    H5FD_onion_t *file      = (H5FD_onion_t *)_file;
    hsize_t       ret_value = 0;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(file);
    assert(file->original_file);

    if (file->original_file)
        ret_value = H5FD_sb_size(file->original_file);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__onion_sb_size */

/*-------------------------------------------------------------------------
 * Function:    H5FD__onion_sb_encode
 *
 * Purpose:     Encodes the superblock information for this driver
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__onion_sb_encode(H5FD_t *_file, char *name /*out*/, unsigned char *buf /*out*/)
{
    H5FD_onion_t *file      = (H5FD_onion_t *)_file;
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(file);
    assert(file->original_file);

    if (file->original_file && H5FD_sb_encode(file->original_file, name, buf) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTENCODE, FAIL, "unable to encode the superblock in R/W file");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__onion_sb_encode */

/*-------------------------------------------------------------------------
 * Function:    H5FD__onion_sb_decode
 *
 * Purpose:     Decodes the superblock information for this driver
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__onion_sb_decode(H5FD_t *_file, const char *name, const unsigned char *buf)
{
    H5FD_onion_t *file      = (H5FD_onion_t *)_file;
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(file);
    assert(file->original_file);

    if (H5FD_sb_load(file->original_file, name, buf) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTDECODE, FAIL, "unable to decode the superblock in R/W file");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__onion_sb_decode */

/*-----------------------------------------------------------------------------
 * Write in-memory revision record to appropriate backing file.
 * Update information in other in-memory components.
 *-----------------------------------------------------------------------------
 */
static herr_t
H5FD__onion_commit_new_revision_record(H5FD_onion_t *file)
{
    uint32_t                      checksum  = 0; /* required */
    size_t                        size      = 0;
    haddr_t                       phys_addr = 0; /* offset in history file to record start */
    unsigned char                *buf       = NULL;
    herr_t                        ret_value = SUCCEED;
    H5FD_onion_revision_record_t *rec       = &file->curr_rev_record;
    H5FD_onion_history_t         *history   = &file->history;
    H5FD_onion_record_loc_t      *new_list  = NULL;

    time_t     rawtime;
    struct tm *info;

    FUNC_ENTER_PACKAGE

    HDtime(&rawtime);
    info = HDgmtime(&rawtime);
    strftime(rec->time_of_creation, sizeof(rec->time_of_creation), "%Y%m%dT%H%M%SZ", info);

    rec->logical_eof = file->logical_eof;

    if ((true == file->is_open_rw) && (H5FD__onion_merge_revision_index_into_archival_index(
                                           file->rev_index, &file->curr_rev_record.archival_index) < 0))
        HGOTO_ERROR(H5E_VFL, H5E_INTERNAL, FAIL, "unable to update index to write");

    if (NULL == (buf = H5MM_malloc(H5FD_ONION_ENCODED_SIZE_REVISION_RECORD + (size_t)rec->comment_size +
                                   (H5FD_ONION_ENCODED_SIZE_INDEX_ENTRY * rec->archival_index.n_entries))))
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL, "can't allocate buffer for encoded revision record");

    if (0 == (size = H5FD__onion_revision_record_encode(rec, buf, &checksum)))
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "problem encoding revision record");

    phys_addr = file->onion_eof;
    if (H5FD_set_eoa(file->onion_file, H5FD_MEM_DRAW, phys_addr + size) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTSET, FAIL, "can't modify EOA for new revision record");
    if (H5FD_write(file->onion_file, H5FD_MEM_DRAW, phys_addr, size, buf) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, FAIL, "can't write new revision record");

    file->onion_eof = phys_addr + size;
    if (true == file->align_history_on_pages)
        file->onion_eof = (file->onion_eof + (file->header.page_size - 1)) & (~(file->header.page_size - 1));

    /* Update history info to accommodate new revision */

    if (history->n_revisions == 0) {
        unsigned char *ptr = buf; /* reuse buffer space to compute checksum */

        assert(history->record_locs == NULL);
        history->n_revisions = 1;
        if (NULL == (history->record_locs = H5MM_calloc(sizeof(H5FD_onion_record_loc_t))))
            HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL, "can't allocate temporary record pointer list");

        history->record_locs[0].phys_addr   = phys_addr;
        history->record_locs[0].record_size = size;
        UINT64ENCODE(ptr, phys_addr);
        UINT64ENCODE(ptr, size);
        history->record_locs[0].checksum = H5_checksum_fletcher32(buf, (size_t)(ptr - buf));
        /* TODO: size-reset belongs where? */
        file->header.history_size += H5FD_ONION_ENCODED_SIZE_RECORD_POINTER;
    } /* end if no extant revisions in history */
    else {
        unsigned char *ptr = buf; /* reuse buffer space to compute checksum */

        assert(history->record_locs != NULL);

        if (NULL == (new_list = H5MM_calloc((history->n_revisions + 1) * sizeof(H5FD_onion_record_loc_t))))
            HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL, "unable to resize record pointer list");
        H5MM_memcpy(new_list, history->record_locs, sizeof(H5FD_onion_record_loc_t) * history->n_revisions);
        H5MM_xfree(history->record_locs);
        history->record_locs                                   = new_list;
        new_list                                               = NULL;
        history->record_locs[history->n_revisions].phys_addr   = phys_addr;
        history->record_locs[history->n_revisions].record_size = size;
        UINT64ENCODE(ptr, phys_addr);
        UINT64ENCODE(ptr, size);
        history->record_locs[history->n_revisions].checksum =
            H5_checksum_fletcher32(buf, (size_t)(ptr - buf));

        file->header.history_size += H5FD_ONION_ENCODED_SIZE_RECORD_POINTER;
        history->n_revisions += 1;
    } /* end if one or more revisions present in history */

    file->header.history_addr = file->onion_eof;

done:
    H5MM_xfree(buf);
    H5MM_xfree(new_list);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__onion_commit_new_revision_record() */

/*-----------------------------------------------------------------------------
 * Function:    H5FD__onion_close
 *
 * Purpose:     Close an onionized file
 *
 * Return:      SUCCEED/FAIL
 *-----------------------------------------------------------------------------
 */
static herr_t
H5FD__onion_close(H5FD_t *_file)
{
    H5FD_onion_t *file      = (H5FD_onion_t *)_file;
    herr_t        ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(file);

    if (H5FD_ONION_STORE_TARGET_ONION == file->fa.store_target) {

        assert(file->onion_file);

        if (file->is_open_rw) {

            assert(file->recovery_file);

            if (H5FD__onion_commit_new_revision_record(file) < 0)
                HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, FAIL, "Can't write revision record to backing store");

            if (H5FD__onion_write_final_history(file) < 0)
                HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, FAIL, "Can't write history to backing store");

            /* Unset write-lock flag and write header */
            if (file->is_open_rw)
                file->header.flags &= (uint32_t)~H5FD_ONION_HEADER_FLAG_WRITE_LOCK;
            if (H5FD__onion_write_header(&(file->header), file->onion_file) < 0)
                HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, FAIL, "Can't write updated header to backing store");
        }
    }
    else
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "invalid history target");

done:

    /* Destroy things as best we can, even if there were earlier errors */
    if (file->original_file)
        if (H5FD_close(file->original_file) < 0)
            HDONE_ERROR(H5E_VFL, H5E_CANTRELEASE, FAIL, "can't close backing canon file");
    if (file->onion_file)
        if (H5FD_close(file->onion_file) < 0)
            HDONE_ERROR(H5E_VFL, H5E_CANTRELEASE, FAIL, "can't close backing onion file");
    if (file->recovery_file) {
        if (H5FD_close(file->recovery_file) < 0)
            HDONE_ERROR(H5E_VFL, H5E_CANTRELEASE, FAIL, "can't close backing recovery file");
        /* TODO: Use the VFD's del callback instead of remove (this requires
         *       storing a copy of the fapl that was used to open it)
         */
        HDremove(file->recovery_file_name);
    }
    if (file->rev_index)
        if (H5FD__onion_revision_index_destroy(file->rev_index) < 0)
            HDONE_ERROR(H5E_VFL, H5E_CANTRELEASE, FAIL, "can't close revision index");

    H5MM_xfree(file->recovery_file_name);
    H5MM_xfree(file->history.record_locs);
    H5MM_xfree(file->curr_rev_record.comment);
    H5MM_xfree(file->curr_rev_record.archival_index.list);

    file = H5FL_FREE(H5FD_onion_t, file);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__onion_close() */

/*-----------------------------------------------------------------------------
 * Function:    H5FD__onion_get_eoa
 *
 * Purpose:     Get end-of-address address.
 *
 * Return:      Address of first byte past the addressed space
 *-----------------------------------------------------------------------------
 */
static haddr_t
H5FD__onion_get_eoa(const H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type)
{
    const H5FD_onion_t *file = (const H5FD_onion_t *)_file;

    FUNC_ENTER_PACKAGE_NOERR

    FUNC_LEAVE_NOAPI(file->logical_eoa)
} /* end H5FD__onion_get_eoa() */

/*-----------------------------------------------------------------------------
 * Function:    H5FD__onion_get_eof
 *
 * Purpose:     Get end-of-file address.
 *
 * Return:      Address of first byte past the file-end.
 *-----------------------------------------------------------------------------
 */
static haddr_t
H5FD__onion_get_eof(const H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type)
{
    const H5FD_onion_t *file = (const H5FD_onion_t *)_file;

    FUNC_ENTER_PACKAGE_NOERR

    FUNC_LEAVE_NOAPI(file->logical_eof)
} /* end H5FD__onion_get_eof() */

/*-----------------------------------------------------------------------------
 * Sanitize the backing FAPL ID
 *-----------------------------------------------------------------------------
 */
static inline hid_t
H5FD__onion_get_legit_fapl_id(hid_t fapl_id)
{
    if (H5P_DEFAULT == fapl_id)
        return H5P_FILE_ACCESS_DEFAULT;
    else if (true == H5P_isa_class(fapl_id, H5P_FILE_ACCESS))
        return fapl_id;
    else
        return H5I_INVALID_HID;
}

/*-----------------------------------------------------------------------------
 * Function:    H5FD_onion_create_truncate_onion
 *
 * Purpose:     Create/truncate HDF5 and onion data for a fresh file
 *
 *      Special open operation required to instantiate the canonical file and
 *      history simultaneously. If successful, the required backing files are
 *      craeated and given initial population on the backing store, and the Onion
 *      virtual file handle is set; open effects a write-mode open.
 *
 *      Cannot create 'template' history and proceed with normal write-mode open,
 *      as this would in effect create an empty first revision, making the history
 *      unintuitive. (create file -> initialize and commit empty first revision
 *      (revision 0); any data written to file during the 'create' open, as seen by
 *      the user, would be in the second revision (revision 1).)
 *
 * Return:      SUCCEED/FAIL
 *-----------------------------------------------------------------------------
 */
static herr_t
H5FD__onion_create_truncate_onion(H5FD_onion_t *file, const char *filename, const char *name_onion,
                                  const char *recovery_file_nameery, unsigned int flags, haddr_t maxaddr)
{
    hid_t                         backing_fapl_id = H5I_INVALID_HID;
    H5FD_onion_header_t          *hdr             = NULL;
    H5FD_onion_history_t         *history         = NULL;
    H5FD_onion_revision_record_t *rec             = NULL;
    unsigned char                *buf             = NULL;
    size_t                        size            = 0;
    herr_t                        ret_value       = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(file != NULL);

    hdr     = &file->header;
    history = &file->history;
    rec     = &file->curr_rev_record;

    hdr->flags = H5FD_ONION_HEADER_FLAG_WRITE_LOCK;
    if (H5FD_ONION_FAPL_INFO_CREATE_FLAG_ENABLE_PAGE_ALIGNMENT & file->fa.creation_flags)
        hdr->flags |= H5FD_ONION_HEADER_FLAG_PAGE_ALIGNMENT;

    hdr->origin_eof = 0;

    backing_fapl_id = H5FD__onion_get_legit_fapl_id(file->fa.backing_fapl_id);
    if (H5I_INVALID_HID == backing_fapl_id)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid backing FAPL ID");

    /* Create backing files for onion history */

    if (NULL == (file->original_file = H5FD_open(filename, flags, backing_fapl_id, maxaddr)))
        HGOTO_ERROR(H5E_VFL, H5E_CANTOPENFILE, FAIL, "cannot open the backing file");

    if (NULL == (file->onion_file = H5FD_open(name_onion, flags, backing_fapl_id, maxaddr)))
        HGOTO_ERROR(H5E_VFL, H5E_CANTOPENFILE, FAIL, "cannot open the backing onion file");

    if (NULL == (file->recovery_file = H5FD_open(recovery_file_nameery, flags, backing_fapl_id, maxaddr)))
        HGOTO_ERROR(H5E_VFL, H5E_CANTOPENFILE, FAIL, "cannot open the backing file");

    /* Write "empty" .h5 file contents (signature ONIONEOF) */

    if (H5FD_set_eoa(file->original_file, H5FD_MEM_DRAW, 8) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTSET, FAIL, "can't extend EOA");
    if (H5FD_write(file->original_file, H5FD_MEM_DRAW, 0, 8, "ONIONEOF") < 0)
        HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, FAIL, "cannot write header to the backing h5 file");

    /* Write nascent history (with no revisions) to "recovery" */

    if (NULL == (buf = H5MM_malloc(H5FD_ONION_ENCODED_SIZE_HISTORY)))
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL, "can't allocate buffer");
    size = H5FD__onion_history_encode(history, buf, &history->checksum);
    if (H5FD_ONION_ENCODED_SIZE_HISTORY != size)
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "can't encode history");
    if (H5FD_set_eoa(file->recovery_file, H5FD_MEM_DRAW, size) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTSET, FAIL, "can't extend EOA");
    if (H5FD_write(file->recovery_file, H5FD_MEM_DRAW, 0, size, buf) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, FAIL, "cannot write history to the backing recovery file");
    hdr->history_size = size; /* record for later use */
    H5MM_xfree(buf);
    buf = NULL;

    /* Write history header with "no" history.
     * Size of the "recovery" history recorded for later use on close.
     */

    if (NULL == (buf = H5MM_malloc(H5FD_ONION_ENCODED_SIZE_HEADER)))
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL, "can't allocate buffer");
    size = H5FD__onion_header_encode(hdr, buf, &hdr->checksum);
    if (H5FD_ONION_ENCODED_SIZE_HEADER != size)
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "can't encode history header");
    if (H5FD_set_eoa(file->onion_file, H5FD_MEM_DRAW, size) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTSET, FAIL, "can't extend EOA");
    if (H5FD_write(file->onion_file, H5FD_MEM_DRAW, 0, size, buf) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, FAIL, "cannot write header to the backing onion file");
    file->onion_eof = (haddr_t)size;
    if (true == file->align_history_on_pages)
        file->onion_eof = (file->onion_eof + (hdr->page_size - 1)) & (~(hdr->page_size - 1));

    rec->archival_index.list = NULL;

    if (NULL == (file->rev_index = H5FD__onion_revision_index_init(file->fa.page_size)))
        HGOTO_ERROR(H5E_VFL, H5E_CANTINIT, FAIL, "can't initialize revision index");

done:
    H5MM_xfree(buf);

    if (FAIL == ret_value)
        HDremove(recovery_file_nameery); /* destroy new temp file, if 'twas created */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__onion_create_truncate_onion() */

static herr_t
H5FD__onion_remove_unused_symbols(char *s)
{
    char *d = s;

    FUNC_ENTER_PACKAGE_NOERR

    do {
        while (*d == '{' || *d == '}' || *d == ' ') {
            ++d;
        }
    } while ((*s++ = *d++));

    FUNC_LEAVE_NOAPI(SUCCEED)
}

static herr_t
H5FD__onion_parse_config_str(const char *config_str, H5FD_onion_fapl_info_t *fa)
{
    char  *config_str_copy = NULL;
    herr_t ret_value       = SUCCEED;

    FUNC_ENTER_PACKAGE

    if (!strcmp(config_str, ""))
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "configure string can't be empty");

    /* Initialize to the default values */
    fa->version          = H5FD_ONION_FAPL_INFO_VERSION_CURR;
    fa->backing_fapl_id  = H5P_DEFAULT;
    fa->page_size        = 4;
    fa->store_target     = H5FD_ONION_STORE_TARGET_ONION;
    fa->revision_num     = H5FD_ONION_FAPL_INFO_REVISION_ID_LATEST;
    fa->force_write_open = 0;
    fa->creation_flags   = 0;
    strcpy(fa->comment, "initial comment");

    /* If a single integer is passed in as a string, it's a shortcut for the tools
     * (h5repack, h5diff, h5dump).  Otherwise, the string should have curly brackets,
     * e.g. {revision_num: 2; page_size: 4;}
     */
    if (config_str[0] != '{')
        fa->revision_num = (uint64_t)strtoull(config_str, NULL, 10);
    else {
        char *token1 = NULL, *token2 = NULL;

        /* Duplicate the configure string since strtok will mess with it */
        if (NULL == (config_str_copy = H5MM_strdup(config_str)))
            HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL, "can't duplicate configure string");

        /* Remove the curly brackets and space from the configure string */
        H5FD__onion_remove_unused_symbols(config_str_copy);

        /* The configure string can't be empty after removing the curly brackets */
        if (!strcmp(config_str_copy, ""))
            HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "configure string can't be empty");

        token1 = strtok(config_str_copy, ":");
        token2 = strtok(NULL, ";");

        do {
            if (token1 && token2) {
                if (!strcmp(token1, "version")) {
                    if (!strcmp(token2, "H5FD_ONION_FAPL_INFO_VERSION_CURR"))
                        fa->version = H5FD_ONION_FAPL_INFO_VERSION_CURR;
                }
                else if (!strcmp(token1, "backing_fapl_id")) {
                    if (!strcmp(token2, "H5P_DEFAULT"))
                        fa->backing_fapl_id = H5P_DEFAULT;
                    else if (!strcmp(token2, "H5I_INVALID_HID"))
                        fa->backing_fapl_id = H5I_INVALID_HID;
                    else
                        fa->backing_fapl_id = strtoll(token2, NULL, 10);
                }
                else if (!strcmp(token1, "page_size")) {
                    fa->page_size = (uint32_t)strtoul(token2, NULL, 10);
                }
                else if (!strcmp(token1, "revision_num")) {
                    if (!strcmp(token2, "H5FD_ONION_FAPL_INFO_REVISION_ID_LATEST"))
                        fa->revision_num = H5FD_ONION_FAPL_INFO_REVISION_ID_LATEST;
                    else
                        fa->revision_num = (uint64_t)strtoull(token2, NULL, 10);
                }
                else if (!strcmp(token1, "force_write_open")) {
                    fa->force_write_open = (uint8_t)strtoul(token2, NULL, 10);
                }
                else if (!strcmp(token1, "creation_flags")) {
                    fa->creation_flags = (uint8_t)strtoul(token2, NULL, 10);
                }
                else if (!strcmp(token1, "comment")) {
                    strcpy(fa->comment, token2);
                }
                else
                    HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "unknown token in the configure string: %s",
                                token1);
            }

            token1 = strtok(NULL, ":");
            token2 = strtok(NULL, ";");
        } while (token1);
    }

    if (H5P_DEFAULT == fa->backing_fapl_id || H5I_INVALID_HID == fa->backing_fapl_id) {
        H5P_genclass_t *pclass; /* Property list class to modify */

        if (NULL == (pclass = (H5P_genclass_t *)H5I_object_verify(H5P_FILE_ACCESS, H5I_GENPROP_CLS)))
            HGOTO_ERROR(H5E_VFL, H5E_BADTYPE, FAIL, "not a property list class");

        /* Create the new property list */
        if ((fa->backing_fapl_id = H5P_create_id(pclass, true)) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTCREATE, FAIL, "unable to create property list");
    }

done:
    H5MM_free(config_str_copy);

    FUNC_LEAVE_NOAPI(ret_value)
}

/*-----------------------------------------------------------------------------
 * Function:    H5FD__onion_open
 *
 * Purpose:     Open an onionized file
 *
 * Return:      Success:    A pointer to a new file data structure
 *              Failure:    NULL
 *-----------------------------------------------------------------------------
 */
static H5FD_t *
H5FD__onion_open(const char *filename, unsigned flags, hid_t fapl_id, haddr_t maxaddr)
{
    H5P_genplist_t               *plist                 = NULL;
    H5FD_onion_t                 *file                  = NULL;
    const H5FD_onion_fapl_info_t *fa                    = NULL;
    H5FD_onion_fapl_info_t       *new_fa                = NULL;
    const char                   *config_str            = NULL;
    double                        log2_page_size        = 0.0;
    hid_t                         backing_fapl_id       = H5I_INVALID_HID;
    char                         *name_onion            = NULL;
    char                         *recovery_file_nameery = NULL;
    bool                          new_open              = false;
    haddr_t                       canon_eof             = 0;
    H5FD_t                       *ret_value             = NULL;

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    if (!filename || !*filename)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "invalid file name");
    if (0 == maxaddr || HADDR_UNDEF == maxaddr)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, NULL, "bogus maxaddr");
    assert(H5P_DEFAULT != fapl_id);
    if (NULL == (plist = (H5P_genplist_t *)H5I_object(fapl_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a file access property list");

    /* This VFD can be invoked by either H5Pset_fapl_onion() or
     * H5Pset_driver_by_name(). When invoked by the former, there will be
     * driver info to peek at.
     */
    fa = (const H5FD_onion_fapl_info_t *)H5P_peek_driver_info(plist);

    if (NULL == fa) {
        if (NULL == (config_str = H5P_peek_driver_config_str(plist)))
            HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, NULL, "missing VFL driver configure string");

        /* Allocate a new onion fapl info struct and fill it from the
         * configuration string
         */
        if (NULL == (new_fa = H5MM_calloc(sizeof(H5FD_onion_fapl_info_t))))
            HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, NULL, "can't allocate memory for onion fapl info struct");
        if (H5FD__onion_parse_config_str(config_str, new_fa) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, NULL, "failed to parse configure string");

        fa = new_fa;
    }

    /* Check for unsupported target values */
    if (H5FD_ONION_STORE_TARGET_ONION != fa->store_target)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "invalid store target");

    /* Allocate space for the file struct */
    if (NULL == (file = H5FL_CALLOC(H5FD_onion_t)))
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, NULL, "unable to allocate file struct");

    /* Allocate space for onion VFD file names */
    if (NULL == (name_onion = H5MM_malloc(sizeof(char) * (strlen(filename) + 7))))
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, NULL, "unable to allocate onion name string");
    snprintf(name_onion, strlen(filename) + 7, "%s.onion", filename);

    if (NULL == (recovery_file_nameery = H5MM_malloc(sizeof(char) * (strlen(name_onion) + 10))))
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, NULL, "unable to allocate recovery name string");
    snprintf(recovery_file_nameery, strlen(name_onion) + 10, "%s.recovery", name_onion);
    file->recovery_file_name = recovery_file_nameery;

    if (NULL == (file->recovery_file_name = H5MM_malloc(sizeof(char) * (strlen(name_onion) + 10))))
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, NULL, "unable to allocate recovery name string");
    snprintf(file->recovery_file_name, strlen(name_onion) + 10, "%s.recovery", name_onion);

    /* Translate H5P_DEFAULT to a real fapl ID, if necessary */
    backing_fapl_id = H5FD__onion_get_legit_fapl_id(file->fa.backing_fapl_id);
    if (H5I_INVALID_HID == backing_fapl_id)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "invalid backing FAPL ID");

    /* Initialize file structure fields */

    H5MM_memcpy(&(file->fa), fa, sizeof(H5FD_onion_fapl_info_t));

    file->header.version   = H5FD_ONION_HEADER_VERSION_CURR;
    file->header.page_size = file->fa.page_size; /* guarded on FAPL-set */

    file->history.version = H5FD_ONION_HISTORY_VERSION_CURR;

    file->curr_rev_record.version                = H5FD_ONION_REVISION_RECORD_VERSION_CURR;
    file->curr_rev_record.archival_index.version = H5FD_ONION_ARCHIVAL_INDEX_VERSION_CURR;

    /* Check that the page size is a power of two */
    if ((fa->page_size == 0) || ((fa->page_size & (fa->page_size - 1)) != 0))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "page size is not a power of two");

    /* Assign the page size */
    log2_page_size                                      = log2((double)(fa->page_size));
    file->curr_rev_record.archival_index.page_size_log2 = (uint32_t)log2_page_size;

    /* Proceed with open. */

    if ((H5F_ACC_CREAT | H5F_ACC_TRUNC) & flags) {

        /* Create a new onion file from scratch */

        /* Set flags */
        if (fa->creation_flags & H5FD_ONION_FAPL_INFO_CREATE_FLAG_ENABLE_PAGE_ALIGNMENT) {
            file->header.flags |= H5FD_ONION_HEADER_FLAG_PAGE_ALIGNMENT;
            file->align_history_on_pages = true;
        }

        /* Truncate and create everything as necessary */
        if (H5FD__onion_create_truncate_onion(file, filename, name_onion, file->recovery_file_name, flags,
                                              maxaddr) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTCREATE, NULL, "unable to create/truncate onionized files");
        file->is_open_rw = true;
    }
    else {

        /* Opening an existing onion file */

        /* Open the existing file using the specified fapl */
        if (NULL == (file->original_file = H5FD_open(filename, flags, backing_fapl_id, maxaddr)))
            HGOTO_ERROR(H5E_VFL, H5E_CANTOPENFILE, NULL, "unable to open canonical file (does not exist?)");

        /* Try to open any existing onion file */
        H5E_BEGIN_TRY
        {
            file->onion_file = H5FD_open(name_onion, flags, backing_fapl_id, maxaddr);
        }
        H5E_END_TRY

        /* If that didn't work, create a new onion file */
        /* TODO: Move to a new function */
        if (NULL == file->onion_file) {
            if (H5F_ACC_RDWR & flags) {
                H5FD_onion_header_t          *hdr        = NULL;
                H5FD_onion_history_t         *history    = NULL;
                H5FD_onion_revision_record_t *rec        = NULL;
                unsigned char                *head_buf   = NULL;
                unsigned char                *hist_buf   = NULL;
                size_t                        size       = 0;
                size_t                        saved_size = 0;

                assert(file != NULL);

                hdr     = &file->header;
                history = &file->history;
                rec     = &file->curr_rev_record;

                new_open = true;

                if (H5FD_ONION_FAPL_INFO_CREATE_FLAG_ENABLE_PAGE_ALIGNMENT & file->fa.creation_flags) {
                    hdr->flags |= H5FD_ONION_HEADER_FLAG_PAGE_ALIGNMENT;
                    file->align_history_on_pages = true;
                }

                if (HADDR_UNDEF == (canon_eof = H5FD_get_eof(file->original_file, H5FD_MEM_DEFAULT))) {
                    HGOTO_ERROR(H5E_VFL, H5E_CANTINIT, NULL, "cannot get size of canonical file");
                }
                if (H5FD_set_eoa(file->original_file, H5FD_MEM_DRAW, canon_eof) < 0)
                    HGOTO_ERROR(H5E_VFL, H5E_CANTSET, NULL, "can't extend EOA");
                hdr->origin_eof   = canon_eof;
                file->logical_eof = canon_eof;

                backing_fapl_id = H5FD__onion_get_legit_fapl_id(file->fa.backing_fapl_id);

                if (H5I_INVALID_HID == backing_fapl_id)
                    HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "invalid backing FAPL ID");

                /* Create backing files for onion history */

                if ((file->onion_file = H5FD_open(name_onion, (H5F_ACC_RDWR | H5F_ACC_CREAT | H5F_ACC_TRUNC),
                                                  backing_fapl_id, maxaddr)) == NULL) {
                    HGOTO_ERROR(H5E_VFL, H5E_CANTOPENFILE, NULL, "cannot open the backing onion file");
                }

                /* Write history header with "no" history */
                hdr->history_size = H5FD_ONION_ENCODED_SIZE_HISTORY; /* record for later use */
                hdr->history_addr =
                    H5FD_ONION_ENCODED_SIZE_HEADER + 1; /* TODO: comment these 2 or do some other way */
                head_buf = H5MM_malloc(H5FD_ONION_ENCODED_SIZE_HEADER);
                if (NULL == head_buf)
                    HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, NULL, "can't allocate buffer");
                size = H5FD__onion_header_encode(hdr, head_buf, &hdr->checksum);
                if (H5FD_ONION_ENCODED_SIZE_HEADER != size)
                    HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, NULL, "can't encode history header");

                hist_buf = H5MM_malloc(H5FD_ONION_ENCODED_SIZE_HISTORY);
                if (NULL == hist_buf)
                    HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, NULL, "can't allocate buffer");
                saved_size                = size;
                history->n_revisions      = 0;
                size                      = H5FD__onion_history_encode(history, hist_buf, &history->checksum);
                file->header.history_size = size; /* record for later use */
                if (H5FD_ONION_ENCODED_SIZE_HISTORY != size) {
                    HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, NULL, "can't encode history");
                }
                if (H5FD_set_eoa(file->onion_file, H5FD_MEM_DRAW, saved_size + size + 1) < 0)
                    HGOTO_ERROR(H5E_VFL, H5E_CANTSET, NULL, "can't extend EOA");

                if (H5FD_write(file->onion_file, H5FD_MEM_DRAW, 0, saved_size, head_buf) < 0) {
                    HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, NULL,
                                "cannot write header to the backing onion file");
                }

                file->onion_eof = (haddr_t)saved_size;
                if (true == file->align_history_on_pages)
                    file->onion_eof = (file->onion_eof + (hdr->page_size - 1)) & (~(hdr->page_size - 1));

                rec->archival_index.list = NULL;

                file->header.history_addr = file->onion_eof;

                /* Write nascent history (with no revisions) to the backing onion file */
                if (H5FD_write(file->onion_file, H5FD_MEM_DRAW, saved_size + 1, size, hist_buf) < 0) {
                    HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, NULL,
                                "cannot write history to the backing onion file");
                }

                file->header.history_size = size; /* record for later use */

                H5MM_xfree(head_buf);
                H5MM_xfree(hist_buf);
            }
            else {
                HGOTO_ERROR(H5E_VFL, H5E_CANTOPENFILE, NULL, "unable to open onion file (does not exist?).");
            }
        }

        if (HADDR_UNDEF == (canon_eof = H5FD_get_eof(file->original_file, H5FD_MEM_DEFAULT))) {
            HGOTO_ERROR(H5E_VFL, H5E_CANTINIT, NULL, "cannot get size of canonical file");
        }
        if (H5FD_set_eoa(file->original_file, H5FD_MEM_DRAW, canon_eof) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTSET, NULL, "can't extend EOA");

        /* Get the history header from the onion file */
        if (H5FD__onion_ingest_header(&file->header, file->onion_file, 0) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTDECODE, NULL, "can't get history header from backing store");
        file->align_history_on_pages =
            (file->header.flags & H5FD_ONION_HEADER_FLAG_PAGE_ALIGNMENT) ? true : false;

        if (H5FD_ONION_HEADER_FLAG_WRITE_LOCK & file->header.flags) {
            /* Opening a file twice in write mode is an error */
            HGOTO_ERROR(H5E_VFL, H5E_UNSUPPORTED, NULL, "Can't open file already opened in write-mode");
        }
        else {
            /* Read in the history from the onion file */
            if (H5FD__onion_ingest_history(&file->history, file->onion_file, file->header.history_addr,
                                           file->header.history_size) < 0)
                HGOTO_ERROR(H5E_VFL, H5E_CANTDECODE, NULL, "can't get history from backing store");

            /* Sanity check on revision ID */
            if (fa->revision_num > file->history.n_revisions &&
                fa->revision_num != H5FD_ONION_FAPL_INFO_REVISION_ID_LATEST)
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "target revision ID out of range");

            if (fa->revision_num == 0) {
                file->curr_rev_record.logical_eof = canon_eof;
            }
            else if (file->history.n_revisions > 0 &&
                     H5FD__onion_ingest_revision_record(
                         &file->curr_rev_record, file->onion_file, &file->history,
                         MIN(fa->revision_num - 1, (file->history.n_revisions - 1))) < 0) {
                HGOTO_ERROR(H5E_VFL, H5E_CANTDECODE, NULL, "can't get revision record from backing store");
            }

            if (H5F_ACC_RDWR & flags)
                if (H5FD__onion_open_rw(file, flags, maxaddr, new_open) < 0)
                    HGOTO_ERROR(H5E_VFL, H5E_CANTOPENFILE, NULL, "can't write-open write-locked file");
        }

    } /* End if opening existing file */

    /* Copy comment from FAPL info, if one is given */
    if ((H5F_ACC_RDWR | H5F_ACC_CREAT | H5F_ACC_TRUNC) & flags) {
        /* Free the old comment */
        file->curr_rev_record.comment = H5MM_xfree(file->curr_rev_record.comment);

        /* The buffer is of size H5FD_ONION_FAPL_INFO_COMMENT_MAX_LEN + 1
         *
         * We're getting this buffer from a fixed-size array in a struct, which
         * will be garbage and not null-terminated if the user isn't careful.
         * Be careful of this and do strndup first to ensure strdup gets a
         * null-termianted string (HDF5 doesn't provide a strnlen call if you
         * don't have one).
         */
        if (NULL ==
            (file->curr_rev_record.comment = H5MM_strndup(fa->comment, H5FD_ONION_FAPL_INFO_COMMENT_MAX_LEN)))
            HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, NULL, "unable to duplicate comment string");

        /* TODO: Lengths of strings should be size_t */
        file->curr_rev_record.comment_size = (uint32_t)strlen(fa->comment) + 1;
    }
    file->origin_eof  = file->header.origin_eof;
    file->logical_eof = MAX(file->curr_rev_record.logical_eof, file->logical_eof);
    file->logical_eoa = 0;

    file->onion_eof = H5FD_get_eoa(file->onion_file, H5FD_MEM_DRAW);
    if (true == file->align_history_on_pages)
        file->onion_eof = (file->onion_eof + (file->header.page_size - 1)) & (~(file->header.page_size - 1));

    ret_value = (H5FD_t *)file;

done:
    H5MM_xfree(name_onion);
    H5MM_xfree(recovery_file_nameery);

    if (config_str && new_fa)
        if (fa && fa->backing_fapl_id)
            if (H5I_GENPROP_LST == H5I_get_type(fa->backing_fapl_id))
                H5I_dec_app_ref(fa->backing_fapl_id);

    if ((NULL == ret_value) && file) {

        if (file->original_file)
            if (H5FD_close(file->original_file) < 0)
                HDONE_ERROR(H5E_VFL, H5E_CANTRELEASE, NULL, "can't destroy backing canon");
        if (file->onion_file)
            if (H5FD_close(file->onion_file) < 0)
                HDONE_ERROR(H5E_VFL, H5E_CANTRELEASE, NULL, "can't destroy backing onion");
        if (file->recovery_file)
            if (H5FD_close(file->recovery_file) < 0)
                HDONE_ERROR(H5E_VFL, H5E_CANTRELEASE, NULL, "can't destroy backing recov");

        if (file->rev_index)
            if (H5FD__onion_revision_index_destroy(file->rev_index) < 0)
                HDONE_ERROR(H5E_VFL, H5E_CANTRELEASE, NULL, "can't destroy revision index");

        H5MM_xfree(file->history.record_locs);

        H5MM_xfree(file->recovery_file_name);
        H5MM_xfree(file->curr_rev_record.comment);

        H5FL_FREE(H5FD_onion_t, file);
    }

    H5MM_xfree(new_fa);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__onion_open() */

/*-----------------------------------------------------------------------------
 * Function:    H5FD__onion_open_rw
 *
 * Purpose:     Complete onion file-open, handling process for write mode.
 *
 *              Creates recovery file if one does not exist.
 *              Initializes 'live' revision index.
 *              Force write-open is not yet supported (recovery provision) TODO
 *              Establishes write-lock in history header (sets lock flag).
 *
 * Return:      SUCCEED/FAIL
 *-----------------------------------------------------------------------------
 */
static herr_t
H5FD__onion_open_rw(H5FD_onion_t *file, unsigned int flags, haddr_t maxaddr, bool new_open)
{
    unsigned char *buf       = NULL;
    size_t         size      = 0;
    uint32_t       checksum  = 0;
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Guard against simultaneous write-open.
     * TODO: support recovery open with force-write-open flag in FAPL info.
     */

    if (file->header.flags & H5FD_ONION_HEADER_FLAG_WRITE_LOCK)
        HGOTO_ERROR(H5E_VFL, H5E_UNSUPPORTED, FAIL, "can't write-open write-locked file");

    /* Copy history to recovery file */

    if (NULL ==
        (file->recovery_file = H5FD_open(file->recovery_file_name, (flags | H5F_ACC_CREAT | H5F_ACC_TRUNC),
                                         file->fa.backing_fapl_id, maxaddr)))
        HGOTO_ERROR(H5E_VFL, H5E_CANTOPENFILE, FAIL, "unable to create recovery file");

    if (0 == (size = H5FD__onion_write_history(&file->history, file->recovery_file, 0, 0)))
        HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, FAIL, "can't write history to recovery file");
    if (size != file->header.history_size)
        HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, FAIL, "written history differed from expected size");

    /* Set write-lock flag in onion header */

    if (NULL == (buf = H5MM_malloc(H5FD_ONION_ENCODED_SIZE_HEADER)))
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL, "can't allocate space for encoded buffer");

    file->header.flags |= H5FD_ONION_HEADER_FLAG_WRITE_LOCK;

    if (0 == (size = H5FD__onion_header_encode(&file->header, buf, &checksum)))
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "problem encoding history header");

    if (H5FD_write(file->onion_file, H5FD_MEM_DRAW, 0, size, buf) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, FAIL, "can't write updated history header");

    /* Prepare revision index and finalize write-mode open */

    if (NULL == (file->rev_index = H5FD__onion_revision_index_init(file->fa.page_size)))
        HGOTO_ERROR(H5E_VFL, H5E_CANTINIT, FAIL, "can't initialize revision index");
    file->curr_rev_record.parent_revision_num = file->curr_rev_record.revision_num;
    if (!new_open)
        file->curr_rev_record.revision_num += 1;
    file->is_open_rw = true;

done:
    if (FAIL == ret_value) {
        if (file->recovery_file != NULL) {
            if (H5FD_close(file->recovery_file) < 0)
                HDONE_ERROR(H5E_VFL, H5E_CANTCLOSEFILE, FAIL, "can't close recovery file");
            file->recovery_file = NULL;
        }

        if (file->rev_index != NULL) {
            if (H5FD__onion_revision_index_destroy(file->rev_index) < 0)
                HDONE_ERROR(H5E_VFL, H5E_CANTRELEASE, FAIL, "can't destroy revision index");
            file->rev_index = NULL;
        }
    }

    H5MM_xfree(buf);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__onion_open_rw() */

/*-----------------------------------------------------------------------------
 * Function:    H5FD__onion_read
 *
 * Purpose:     Read bytes from an onionized file
 *
 * Return:      SUCCEED/FAIL
 *-----------------------------------------------------------------------------
 */
static herr_t
H5FD__onion_read(H5FD_t *_file, H5FD_mem_t type, hid_t H5_ATTR_UNUSED dxpl_id, haddr_t offset, size_t len,
                 void *_buf_out)
{
    H5FD_onion_t  *file           = (H5FD_onion_t *)_file;
    uint64_t       page_0         = 0;
    size_t         n_pages        = 0;
    uint32_t       page_size      = 0;
    uint32_t       page_size_log2 = 0;
    size_t         bytes_to_read  = len;
    unsigned char *buf_out        = (unsigned char *)_buf_out;
    herr_t         ret_value      = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(file != NULL);
    assert(buf_out != NULL);

    if ((uint64_t)(offset + len) > file->logical_eoa)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Read extends beyond addressed space");

    if (0 == len)
        goto done;

    page_size      = file->header.page_size;
    page_size_log2 = file->curr_rev_record.archival_index.page_size_log2;
    page_0         = offset >> page_size_log2;
    n_pages        = (len + page_size - 1) >> page_size_log2;

    /* Read, page-by-page */
    for (size_t i = 0; i < n_pages; i++) {
        const H5FD_onion_index_entry_t *entry_out     = NULL;
        haddr_t                         page_gap_head = 0; /* start of page to start of buffer */
        haddr_t                         page_gap_tail = 0; /* end of buffer to end of page */
        size_t                          page_readsize = 0;
        uint64_t                        page_i        = page_0 + i;

        if (0 == i) {
            page_gap_head = offset & (((uint32_t)1 << page_size_log2) - 1);
            /* Check if we need to add an additional page to make up for the page_gap_head */
            if (page_gap_head > 0 &&
                (page_gap_head + (bytes_to_read % page_size) > page_size || bytes_to_read % page_size == 0)) {
                n_pages++;
            }
        }

        if (n_pages - 1 == i)
            page_gap_tail = page_size - bytes_to_read - page_gap_head;

        page_readsize = (size_t)page_size - page_gap_head - page_gap_tail;

        if (true == file->is_open_rw && file->fa.revision_num != 0 &&
            H5FD__onion_revision_index_find(file->rev_index, page_i, &entry_out)) {
            /* Page exists in 'live' revision index */
            if (H5FD_read(file->onion_file, H5FD_MEM_DRAW, entry_out->phys_addr + page_gap_head,
                          page_readsize, buf_out) < 0)
                HGOTO_ERROR(H5E_VFL, H5E_READERROR, FAIL, "can't get working file data");
        }
        else if (file->fa.revision_num != 0 &&
                 H5FD__onion_archival_index_find(&file->curr_rev_record.archival_index, page_i, &entry_out)) {
            /* Page exists in archival index */
            if (H5FD_read(file->onion_file, H5FD_MEM_DRAW, entry_out->phys_addr + page_gap_head,
                          page_readsize, buf_out) < 0)
                HGOTO_ERROR(H5E_VFL, H5E_READERROR, FAIL, "can't get previously-amended file data");
        }
        else {
            /* Page does not exist in either index */

            /* Casts prevent truncation */
            haddr_t addr_start   = (haddr_t)page_i * (haddr_t)page_size + (haddr_t)page_gap_head;
            haddr_t overlap_size = (addr_start > file->origin_eof) ? 0 : file->origin_eof - addr_start;
            haddr_t read_size    = MIN(overlap_size, page_readsize);

            /* Get all original bytes in page range */
            if ((read_size > 0) && H5FD_read(file->original_file, type, addr_start, read_size, buf_out) < 0) {
                HGOTO_ERROR(H5E_VFL, H5E_READERROR, FAIL, "can't get original file data");
            }

            /* Fill with 0s any gaps after end of original bytes
             * and before end of page.
             */
            for (size_t j = read_size; j < page_readsize; j++)
                buf_out[j] = 0;
        }

        buf_out += page_readsize;
        bytes_to_read -= page_readsize;
    } /* end for each page in range */

    assert(0 == bytes_to_read);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__onion_read() */

/*-----------------------------------------------------------------------------
 * Function:    H5FD__onion_set_eoa
 *
 * Purpose:     Set end-of-address marker of the logical file.
 *
 * Return:      SUCCEED/FAIL
 *-----------------------------------------------------------------------------
 */
static herr_t
H5FD__onion_set_eoa(H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type, haddr_t addr)
{
    H5FD_onion_t *file = (H5FD_onion_t *)_file;

    FUNC_ENTER_PACKAGE_NOERR

    file->logical_eoa = addr;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FD__onion_set_eoa() */

/*-----------------------------------------------------------------------------
 * Function:    H5FD__onion_write
 *
 * Purpose:     Write bytes to an onionized file
 *
 * Return:      SUCCEED/FAIL
 *-----------------------------------------------------------------------------
 */
static herr_t
H5FD__onion_write(H5FD_t *_file, H5FD_mem_t type, hid_t H5_ATTR_UNUSED dxpl_id, haddr_t offset, size_t len,
                  const void *_buf)
{
    H5FD_onion_t        *file           = (H5FD_onion_t *)_file;
    uint64_t             page_0         = 0;
    size_t               n_pages        = 0;
    unsigned char       *page_buf       = NULL;
    uint32_t             page_size      = 0;
    uint32_t             page_size_log2 = 0;
    size_t               bytes_to_write = len;
    const unsigned char *buf            = (const unsigned char *)_buf;
    herr_t               ret_value      = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(file != NULL);
    assert(buf != NULL);
    assert(file->rev_index != NULL);
    assert((uint64_t)(offset + len) <= file->logical_eoa);

    if (false == file->is_open_rw)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Write not allowed if file not opened in write mode");

    if (0 == len)
        goto done;

    page_size      = file->header.page_size;
    page_size_log2 = file->curr_rev_record.archival_index.page_size_log2;
    page_0         = offset >> page_size_log2;
    n_pages        = (len + page_size - 1) >> page_size_log2;

    if (NULL == (page_buf = H5MM_calloc(page_size * sizeof(unsigned char))))
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL, "cannot allocate temporary buffer");

    /* Write, page-by-page */
    for (size_t i = 0; i < n_pages; i++) {
        const unsigned char            *write_buf = buf;
        H5FD_onion_index_entry_t        new_entry;
        const H5FD_onion_index_entry_t *entry_out     = NULL;
        haddr_t                         page_gap_head = 0; /* start of page to start of buffer */
        haddr_t                         page_gap_tail = 0; /* end of buffer to end of page */
        size_t                          page_n_used   = 0; /* nbytes from buffer for this page-write */
        uint64_t                        page_i        = page_0 + i;

        if (0 == i) {
            page_gap_head = offset & (((uint32_t)1 << page_size_log2) - 1);
            /* If we have a page_gap_head and the number of bytes to write is
             * evenly divisible by the page size we need to add an additional
             * page to make up for the page_gap_head
             */
            if (page_gap_head > 0 && (page_gap_head + (bytes_to_write % page_size) > page_size ||
                                      bytes_to_write % page_size == 0)) {
                n_pages++;
            }
        }
        if (n_pages - 1 == i)
            page_gap_tail = page_size - bytes_to_write - page_gap_head;
        page_n_used = page_size - page_gap_head - page_gap_tail;

        /* Modify page in revision index, if present */
        if (H5FD__onion_revision_index_find(file->rev_index, page_i, &entry_out)) {
            if (page_gap_head | page_gap_tail) {
                /* Copy existing page verbatim. */
                if (H5FD_read(file->onion_file, H5FD_MEM_DRAW, entry_out->phys_addr, page_size, page_buf) < 0)
                    HGOTO_ERROR(H5E_VFL, H5E_READERROR, FAIL, "can't get working file data");
                /* Overlay delta from input buffer onto page buffer. */
                H5MM_memcpy(page_buf + page_gap_head, buf, page_n_used);
                write_buf = page_buf;
            } /* end if partial page */

            if (H5FD_write(file->onion_file, H5FD_MEM_DRAW, entry_out->phys_addr, page_size, write_buf) < 0)
                HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, FAIL, "write amended page data to backing file");

            buf += page_n_used; /* overflow never touched */
            bytes_to_write -= page_n_used;

            continue;
        } /* end if page exists in 'live' revision index */

        if (page_gap_head || page_gap_tail) {
            /* Fill gaps with existing data or zeroes. */
            if (H5FD__onion_archival_index_find(&file->curr_rev_record.archival_index, page_i, &entry_out)) {
                /* Page exists in archival index */

                /* Copy existing page verbatim */
                if (H5FD_read(file->onion_file, H5FD_MEM_DRAW, entry_out->phys_addr, page_size, page_buf) < 0)
                    HGOTO_ERROR(H5E_VFL, H5E_READERROR, FAIL, "can't get previously-amended data");
            }
            else {
                haddr_t addr_start   = (haddr_t)(page_i * page_size);
                haddr_t overlap_size = (addr_start > file->origin_eof) ? 0 : file->origin_eof - addr_start;
                haddr_t read_size    = MIN(overlap_size, page_size);

                /* Get all original bytes in page range */
                if ((read_size > 0) &&
                    H5FD_read(file->original_file, type, addr_start, read_size, page_buf) < 0) {
                    HGOTO_ERROR(H5E_VFL, H5E_READERROR, FAIL, "can't get original file data");
                }

                /* Fill with 0s any gaps after end of original bytes
                 * or start of page and before start of new data.
                 */
                for (size_t j = read_size; j < page_gap_head; j++)
                    page_buf[j] = 0;

                /* Fill with 0s any gaps after end of original bytes
                 * or end of new data and before end of page.
                 */
                for (size_t j = MAX(read_size, page_size - page_gap_tail); j < page_size; j++)
                    page_buf[j] = 0;
            } /* end if page exists in neither index */

            /* Copy input buffer to temporary page buffer */
            assert((page_size - page_gap_head) >= page_n_used);
            H5MM_memcpy(page_buf + page_gap_head, buf, page_n_used);
            write_buf = page_buf;

        } /* end if data range does not span entire page */

        new_entry.logical_page = page_i;
        new_entry.phys_addr    = file->onion_eof;

        if (H5FD_set_eoa(file->onion_file, H5FD_MEM_DRAW, file->onion_eof + page_size) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTSET, FAIL, "can't modify EOA for new page amendment");

        if (H5FD_write(file->onion_file, H5FD_MEM_DRAW, file->onion_eof, page_size, write_buf) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, FAIL, "write amended page data to backing file");

        if (H5FD__onion_revision_index_insert(file->rev_index, &new_entry) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTINSERT, FAIL, "can't insert new index entry into revision index");

        file->onion_eof += page_size;
        buf += page_n_used; /* possible overflow never touched */
        bytes_to_write -= page_n_used;

    } /* end for each page to write */

    assert(0 == bytes_to_write);

    file->logical_eof = MAX(file->logical_eof, (offset + len));

done:
    H5MM_xfree(page_buf);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__onion_write() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__onion_ctl
 *
 * Purpose:     Onion VFD version of the ctl callback.
 *
 *              The desired operation is specified by the op_code
 *              parameter.
 *
 *              The flags parameter controls management of op_codes that
 *              are unknown to the callback
 *
 *              The input and output parameters allow op_code specific
 *              input and output
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__onion_ctl(H5FD_t *_file, uint64_t op_code, uint64_t flags, const void H5_ATTR_UNUSED *input,
                void **output)
{
    H5FD_onion_t *file      = (H5FD_onion_t *)_file;
    herr_t        ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(file);

    switch (op_code) {
        case H5FD_CTL_GET_NUM_REVISIONS:
            if (!output || !*output)
                HGOTO_ERROR(H5E_VFL, H5E_FCNTL, FAIL, "the output parameter is null");

            **((uint64_t **)output) = file->history.n_revisions;
            break;
        /* Unknown op code */
        default:
            if (flags & H5FD_CTL_FAIL_IF_UNKNOWN_FLAG)
                HGOTO_ERROR(H5E_VFL, H5E_FCNTL, FAIL, "unknown op_code and fail if unknown flag is set");
            break;
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__onion_ctl() */

/*-------------------------------------------------------------------------
 * Function:    H5FDget_onion_revision_count
 *
 * Purpose:     Get the number of revisions in an onion file
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
herr_t
H5FDonion_get_revision_count(const char *filename, hid_t fapl_id, uint64_t *revision_count /*out*/)
{
    H5P_genplist_t *plist     = NULL;
    H5FD_t         *file      = NULL;
    herr_t          ret_value = SUCCEED;

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "*six", filename, fapl_id, revision_count);

    /* Check args */
    if (!filename || !strcmp(filename, ""))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not a valid file name");
    if (!revision_count)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "revision count can't be null");

    /* Make sure using the correct driver */
    if (NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not a valid FAPL ID");
    if (H5FD_ONION != H5P_peek_driver(plist))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not a Onion VFL driver");

    /* Open the file with the driver */
    if (NULL == (file = H5FD_open(filename, H5F_ACC_RDONLY, fapl_id, HADDR_UNDEF)))
        HGOTO_ERROR(H5E_VFL, H5E_CANTOPENFILE, FAIL, "unable to open file with onion driver");

    /* Call the private function */
    if (H5FD__get_onion_revision_count(file, revision_count) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "failed to get the number of revisions");

done:
    /* Close H5FD_t structure pointer */
    if (file && H5FD_close(file) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTCLOSEFILE, FAIL, "unable to close file");

    FUNC_LEAVE_API(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5FD__get_onion_revision_count
 *
 * Purpose:     Private version of H5FDget_onion_revision_count()
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__get_onion_revision_count(H5FD_t *file, uint64_t *revision_count)
{
    uint64_t op_code;
    uint64_t flags;
    herr_t   ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(file);
    assert(revision_count);

    op_code = H5FD_CTL_GET_NUM_REVISIONS;
    flags   = H5FD_CTL_FAIL_IF_UNKNOWN_FLAG;

    /* Get the number of revisions via the ctl callback */
    if (H5FD_ctl(file, op_code, flags, NULL, (void **)&revision_count) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_FCNTL, FAIL, "VFD ctl request failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

/*-----------------------------------------------------------------------------
 * Function:    H5FD__onion_write_final_history
 *
 * Purpose:     Write final history to appropriate backing file on file close
 *
 * Return:      SUCCEED/FAIL
 *-----------------------------------------------------------------------------
 */
herr_t
H5FD__onion_write_final_history(H5FD_onion_t *file)
{
    size_t size      = 0;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* TODO: history EOF may not be correct (under what circumstances?) */
    if (0 == (size = H5FD__onion_write_history(&(file->history), file->onion_file, file->onion_eof,
                                               file->onion_eof)))
        HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, FAIL, "can't write final history");

    if (size != file->header.history_size)
        HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, FAIL, "written history differed from expected size");

    /* Is last write operation to history file; no need to extend to page
     * boundary if set to page-align.
     */
    file->onion_eof += size;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__onion_write_final_history() */
