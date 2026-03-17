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
 * Purpose:     A driver which stores the HDF5 data in main memory  using
 *              only the HDF5 public API. This driver is useful for fast
 *              access to small, temporary hdf5 files.
 */

#include "H5FDdrvr_module.h" /* This source code file is part of the H5FD driver module */

#include "H5private.h"   /* Generic Functions            */
#include "H5Eprivate.h"  /* Error handling               */
#include "H5Fprivate.h"  /* File access                  */
#include "H5FDprivate.h" /* File drivers                 */
#include "H5FDcore.h"    /* Core file driver             */
#include "H5FLprivate.h" /* Free lists                   */
#include "H5Iprivate.h"  /* IDs                          */
#include "H5MMprivate.h" /* Memory management            */
#include "H5Pprivate.h"  /* Property lists               */
#include "H5SLprivate.h" /* Skip lists                   */

/* The driver identification number, initialized at runtime */
static hid_t H5FD_CORE_g = 0;

/* Whether to ignore file locks when disabled (env var value) */
static htri_t ignore_disabled_file_locks_s = FAIL;

/* The skip list node type.  Represents a region in the file. */
typedef struct H5FD_core_region_t {
    haddr_t start; /* Start address of the region          */
    haddr_t end;   /* End address of the region            */
} H5FD_core_region_t;

/* The description of a file belonging to this driver. The 'eoa' and 'eof'
 * determine the amount of hdf5 address space in use and the high-water mark
 * of the file (the current size of the underlying memory).
 */
typedef struct H5FD_core_t {
    H5FD_t         pub;              /* public stuff, must be first          */
    char          *name;             /* for equivalence testing              */
    unsigned char *mem;              /* the underlying memory                */
    haddr_t        eoa;              /* end of allocated region              */
    haddr_t        eof;              /* current allocated size               */
    size_t         increment;        /* multiples for mem allocation         */
    bool           backing_store;    /* write to file name on flush          */
    bool           write_tracking;   /* Whether to track writes              */
    size_t         bstore_page_size; /* backing store page size              */
    bool           ignore_disabled_file_locks;
    int            fd; /* backing store file descriptor        */
    /* Information for determining uniqueness of a file with a backing store */
#ifndef H5_HAVE_WIN32_API
    /* On most systems the combination of device and i-node number uniquely
     * identify a file.
     */
    dev_t device; /*file device number            */
    ino_t inode;  /*file i-node number            */
#else
    /* Files in windows are uniquely identified by the volume serial
     * number and the file index (both low and high parts).
     *
     * There are caveats where these numbers can change, especially
     * on FAT file systems.  On NTFS, however, a file should keep
     * those numbers the same until renamed or deleted (though you
     * can use ReplaceFile() on NTFS to keep the numbers the same
     * while renaming).
     *
     * See the MSDN "BY_HANDLE_FILE_INFORMATION Structure" entry for
     * more information.
     *
     * http://msdn.microsoft.com/en-us/library/aa363788(v=VS.85).aspx
     */
    DWORD nFileIndexLow;
    DWORD nFileIndexHigh;
    DWORD dwVolumeSerialNumber;

    HANDLE hFile; /* Native windows file handle */
#endif                                        /* H5_HAVE_WIN32_API */
    bool                        dirty;        /* changes not saved?       */
    H5FD_file_image_callbacks_t fi_callbacks; /* file image callbacks     */
    H5SL_t                     *dirty_list;   /* dirty parts of the file  */
} H5FD_core_t;

/* Driver-specific file access properties */
typedef struct H5FD_core_fapl_t {
    size_t increment;      /* how much to grow memory */
    bool   backing_store;  /* write to file name on flush */
    bool   write_tracking; /* Whether to track writes */
    size_t page_size;      /* Page size for tracked writes */
} H5FD_core_fapl_t;

/* Allocate memory in multiples of this size by default */
#define H5FD_CORE_INCREMENT                8192
#define H5FD_CORE_WRITE_TRACKING_FLAG      false
#define H5FD_CORE_WRITE_TRACKING_PAGE_SIZE 524288

/* These macros check for overflow of various quantities.  These macros
 * assume that file_offset_t is signed and haddr_t and size_t are unsigned.
 *
 * ADDR_OVERFLOW:   Checks whether a file address of type `haddr_t'
 *                  is too large to be represented by the second argument
 *                  of the file seek function.
 *
 * SIZE_OVERFLOW:   Checks whether a buffer size of type `hsize_t' is too
 *                  large to be represented by the `size_t' type.
 *
 * REGION_OVERFLOW: Checks whether an address and size pair describe data
 *                  which can be addressed entirely in memory.
 */
#define MAXADDR          ((haddr_t)((~(size_t)0) - 1))
#define ADDR_OVERFLOW(A) (HADDR_UNDEF == (A) || (A) > (haddr_t)MAXADDR)
#define SIZE_OVERFLOW(Z) ((Z) > (hsize_t)MAXADDR)
#define REGION_OVERFLOW(A, Z)                                                                                \
    (ADDR_OVERFLOW(A) || SIZE_OVERFLOW(Z) || HADDR_UNDEF == (A) + (Z) || (size_t)((A) + (Z)) < (size_t)(A))

/* Prototypes */
static herr_t  H5FD__core_add_dirty_region(H5FD_core_t *file, haddr_t start, haddr_t end);
static herr_t  H5FD__core_destroy_dirty_list(H5FD_core_t *file);
static herr_t  H5FD__core_write_to_bstore(H5FD_core_t *file, haddr_t addr, size_t size);
static herr_t  H5FD__core_term(void);
static void   *H5FD__core_fapl_get(H5FD_t *_file);
static H5FD_t *H5FD__core_open(const char *name, unsigned flags, hid_t fapl_id, haddr_t maxaddr);
static herr_t  H5FD__core_close(H5FD_t *_file);
static int     H5FD__core_cmp(const H5FD_t *_f1, const H5FD_t *_f2);
static herr_t  H5FD__core_query(const H5FD_t *_f1, unsigned long *flags);
static haddr_t H5FD__core_get_eoa(const H5FD_t *_file, H5FD_mem_t type);
static herr_t  H5FD__core_set_eoa(H5FD_t *_file, H5FD_mem_t type, haddr_t addr);
static haddr_t H5FD__core_get_eof(const H5FD_t *_file, H5FD_mem_t type);
static herr_t  H5FD__core_get_handle(H5FD_t *_file, hid_t fapl, void **file_handle);
static herr_t  H5FD__core_read(H5FD_t *_file, H5FD_mem_t type, hid_t fapl_id, haddr_t addr, size_t size,
                               void *buf);
static herr_t  H5FD__core_write(H5FD_t *_file, H5FD_mem_t type, hid_t fapl_id, haddr_t addr, size_t size,
                                const void *buf);
static herr_t  H5FD__core_flush(H5FD_t *_file, hid_t dxpl_id, bool closing);
static herr_t  H5FD__core_truncate(H5FD_t *_file, hid_t dxpl_id, bool closing);
static herr_t  H5FD__core_lock(H5FD_t *_file, bool rw);
static herr_t  H5FD__core_unlock(H5FD_t *_file);
static herr_t  H5FD__core_delete(const char *filename, hid_t fapl_id);
static inline const H5FD_core_fapl_t *H5FD__core_get_default_config(void);

static const H5FD_class_t H5FD_core_g = {
    H5FD_CLASS_VERSION,       /* struct version       */
    H5FD_CORE_VALUE,          /* value                */
    "core",                   /* name                 */
    MAXADDR,                  /* maxaddr              */
    H5F_CLOSE_WEAK,           /* fc_degree            */
    H5FD__core_term,          /* terminate            */
    NULL,                     /* sb_size              */
    NULL,                     /* sb_encode            */
    NULL,                     /* sb_decode            */
    sizeof(H5FD_core_fapl_t), /* fapl_size            */
    H5FD__core_fapl_get,      /* fapl_get             */
    NULL,                     /* fapl_copy            */
    NULL,                     /* fapl_free            */
    0,                        /* dxpl_size            */
    NULL,                     /* dxpl_copy            */
    NULL,                     /* dxpl_free            */
    H5FD__core_open,          /* open                 */
    H5FD__core_close,         /* close                */
    H5FD__core_cmp,           /* cmp                  */
    H5FD__core_query,         /* query                */
    NULL,                     /* get_type_map         */
    NULL,                     /* alloc                */
    NULL,                     /* free                 */
    H5FD__core_get_eoa,       /* get_eoa              */
    H5FD__core_set_eoa,       /* set_eoa              */
    H5FD__core_get_eof,       /* get_eof              */
    H5FD__core_get_handle,    /* get_handle           */
    H5FD__core_read,          /* read                 */
    H5FD__core_write,         /* write                */
    NULL,                     /* read_vector          */
    NULL,                     /* write_vector         */
    NULL,                     /* read_selection       */
    NULL,                     /* write_selection      */
    H5FD__core_flush,         /* flush                */
    H5FD__core_truncate,      /* truncate             */
    H5FD__core_lock,          /* lock                 */
    H5FD__core_unlock,        /* unlock               */
    H5FD__core_delete,        /* del                  */
    NULL,                     /* ctl                  */
    H5FD_FLMAP_DICHOTOMY      /* fl_map               */
};

/* Default configurations, if none provided */
static const H5FD_core_fapl_t H5FD_core_default_config_g = {
    (size_t)H5_MB, true, H5FD_CORE_WRITE_TRACKING_FLAG, H5FD_CORE_WRITE_TRACKING_PAGE_SIZE};
static const H5FD_core_fapl_t H5FD_core_default_paged_config_g = {(size_t)H5_MB, true, true, (size_t)4096};

/* Define a free list to manage the region type */
H5FL_DEFINE(H5FD_core_region_t);

/*-------------------------------------------------------------------------
 * Function:    H5FD__core_add_dirty_region
 *
 * Purpose:     Add a new dirty region to the list for later flushing
 *              to the backing store.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__core_add_dirty_region(H5FD_core_t *file, haddr_t start, haddr_t end)
{
    H5FD_core_region_t *b_item          = NULL;
    H5FD_core_region_t *a_item          = NULL;
    H5FD_core_region_t *item            = NULL;
    haddr_t             b_addr          = 0;
    haddr_t             a_addr          = 0;
    bool                create_new_node = true;
    herr_t              ret_value       = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(file);
    assert(file->dirty_list);
    assert(start <= end);

    /* Adjust the dirty region to the nearest block boundaries */
    if (start % file->bstore_page_size != 0)
        start = (start / file->bstore_page_size) * file->bstore_page_size;

    if (end % file->bstore_page_size != (file->bstore_page_size - 1)) {
        end = (((end / file->bstore_page_size) + 1) * file->bstore_page_size) - 1;
        if (end > file->eof)
            end = file->eof - 1;
    } /* end if */

    /* Get the regions before and after the intended insertion point */
    b_addr = start + 1;
    a_addr = end + 2;
    b_item = (H5FD_core_region_t *)H5SL_less(file->dirty_list, &b_addr);
    a_item = (H5FD_core_region_t *)H5SL_less(file->dirty_list, &a_addr);

    /* Check to see if we need to extend the upper end of the NEW region */
    if (a_item)
        if (start < a_item->start && end < a_item->end) {
            /* Extend the end of the NEW region to match the existing AFTER region */
            end = a_item->end;
        } /* end if */
    /* Attempt to extend the PREV region */
    if (b_item)
        if (start <= b_item->end + 1) {

            /* Need to set this for the delete algorithm */
            start = b_item->start;

            /* We won't need to insert a new node since we can
             * just update an existing one instead.
             */
            create_new_node = false;
        } /* end if */

    /* Remove any old nodes that are no longer needed */
    while (a_item && a_item->start > start) {

        H5FD_core_region_t *less;
        haddr_t             key = a_item->start - 1;

        /* Save the previous node before we trash this one */
        less = (H5FD_core_region_t *)H5SL_less(file->dirty_list, &key);

        /* Delete this node */
        a_item = (H5FD_core_region_t *)H5SL_remove(file->dirty_list, &a_item->start);
        a_item = H5FL_FREE(H5FD_core_region_t, a_item);

        /* Set up to check the next node */
        if (less)
            a_item = less;
    } /* end while */

    /* Insert the new node */
    if (create_new_node) {
        if (NULL == (item = (H5FD_core_region_t *)H5SL_search(file->dirty_list, &start))) {
            /* Ok to insert.  No pre-existing node with that key. */
            item        = (H5FD_core_region_t *)H5FL_CALLOC(H5FD_core_region_t);
            item->start = start;
            item->end   = end;
            if (H5SL_insert(file->dirty_list, item, &item->start) < 0)
                HGOTO_ERROR(H5E_SLIST, H5E_CANTINSERT, FAIL, "can't insert new dirty region: (%llu, %llu)\n",
                            (unsigned long long)start, (unsigned long long)end);
        } /* end if */
        else {
            /* Store the new item endpoint if it's bigger */
            item->end = (item->end < end) ? end : item->end;
        }
    }
    else {
        /* Update the size of the before region */
        if (b_item->end < end)
            b_item->end = end;
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__core_add_dirty_region() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__core_destroy_dirty_list
 *
 * Purpose:     Completely destroy the dirty list.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__core_destroy_dirty_list(H5FD_core_t *file)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(file);

    /* Destroy the list, including any remaining list elements */
    if (file->dirty_list) {
        H5FD_core_region_t *region = NULL;

        while (NULL != (region = (H5FD_core_region_t *)H5SL_remove_first(file->dirty_list)))
            region = H5FL_FREE(H5FD_core_region_t, region);

        if (H5SL_close(file->dirty_list) < 0)
            HGOTO_ERROR(H5E_SLIST, H5E_CLOSEERROR, FAIL, "can't close core vfd dirty list");
        file->dirty_list = NULL;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__core_destroy_dirty_list() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__core_write_to_bstore
 *
 * Purpose:     Write data to the backing store.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__core_write_to_bstore(H5FD_core_t *file, haddr_t addr, size_t size)
{
    unsigned char *ptr = file->mem + addr; /* mutable pointer into the
                                            * buffer (can't change mem)
                                            */
    HDoff_t offset    = (HDoff_t)addr;     /* Offset to write at */
    herr_t  ret_value = SUCCEED;           /* Return value */

    FUNC_ENTER_PACKAGE

    assert(file);

#ifndef H5_HAVE_PREADWRITE
    /* Seek to the correct location (if we don't have pwrite) */
    if ((HDoff_t)addr != HDlseek(file->fd, (HDoff_t)addr, SEEK_SET))
        HGOTO_ERROR(H5E_IO, H5E_SEEKERROR, FAIL, "error seeking in backing store");
#endif /* H5_HAVE_PREADWRITE */

    while (size > 0) {

        h5_posix_io_t     bytes_in    = 0;  /* # of bytes to write  */
        h5_posix_io_ret_t bytes_wrote = -1; /* # of bytes written   */

        /* Trying to write more bytes than the return type can handle is
         * undefined behavior in POSIX.
         */
        if (size > H5_POSIX_MAX_IO_BYTES)
            bytes_in = H5_POSIX_MAX_IO_BYTES;
        else
            bytes_in = (h5_posix_io_t)size;

        do {
#ifdef H5_HAVE_PREADWRITE
            bytes_wrote = HDpwrite(file->fd, ptr, bytes_in, offset);
            if (bytes_wrote > 0)
                offset += bytes_wrote;
#else
            bytes_wrote = HDwrite(file->fd, ptr, bytes_in);
#endif /* H5_HAVE_PREADWRITE */
        } while (-1 == bytes_wrote && EINTR == errno);

        if (-1 == bytes_wrote) { /* error */
            int    myerrno = errno;
            time_t mytime  = HDtime(NULL);

            offset = HDlseek(file->fd, (HDoff_t)0, SEEK_CUR);

            HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL,
                        "write to backing store failed: time = %s, filename = '%s', file descriptor = %d, "
                        "errno = %d, error message = '%s', ptr = %p, total write size = %llu, bytes this "
                        "sub-write = %llu, bytes actually written = %llu, offset = %llu",
                        HDctime(&mytime), file->name, file->fd, myerrno, strerror(myerrno), (void *)ptr,
                        (unsigned long long)size, (unsigned long long)bytes_in,
                        (unsigned long long)bytes_wrote, (unsigned long long)offset);
        } /* end if */

        assert(bytes_wrote > 0);
        assert((size_t)bytes_wrote <= size);

        size -= (size_t)bytes_wrote;
        ptr = (unsigned char *)ptr + bytes_wrote;

    } /* end while */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__core_write_to_bstore() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__core_get_default_config
 *
 * Purpose:     Retrieves a default configuration for this VFD when no
 *              configuration information has been provided.
 *
 * Return:      Valid Core VFD configuration information pointer (can't
 *                  fail)
 *
 *-------------------------------------------------------------------------
 */
static inline const H5FD_core_fapl_t *
H5FD__core_get_default_config(void)
{
    char *driver = getenv(HDF5_DRIVER);

    if (driver) {
        if (!strcmp(driver, "core"))
            return &H5FD_core_default_config_g;
        else if (!strcmp(driver, "core_paged"))
            return &H5FD_core_default_paged_config_g;
    }

    return &H5FD_core_default_config_g;
} /* end H5FD__core_get_default_config() */

/*-------------------------------------------------------------------------
 * Function:    H5FD_core_init
 *
 * Purpose:     Initialize this driver by registering the driver with the
 *              library.
 *
 * Return:      Success:    The driver ID for the core driver
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5FD_core_init(void)
{
    char *lock_env_var = NULL;            /* Environment variable pointer */
    hid_t ret_value    = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    /* Check the use disabled file locks environment variable */
    lock_env_var = getenv(HDF5_USE_FILE_LOCKING);
    if (lock_env_var && !strcmp(lock_env_var, "BEST_EFFORT"))
        ignore_disabled_file_locks_s = true; /* Override: Ignore disabled locks */
    else if (lock_env_var && (!strcmp(lock_env_var, "TRUE") || !strcmp(lock_env_var, "1")))
        ignore_disabled_file_locks_s = false; /* Override: Don't ignore disabled locks */
    else
        ignore_disabled_file_locks_s = FAIL; /* Environment variable not set, or not set correctly */

    if (H5I_VFL != H5I_get_type(H5FD_CORE_g))
        H5FD_CORE_g = H5FD_register(&H5FD_core_g, sizeof(H5FD_class_t), false);

    /* Set return value */
    ret_value = H5FD_CORE_g;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_core_init() */

/*---------------------------------------------------------------------------
 * Function:    H5FD__core_term
 *
 * Purpose:     Shut down the VFD
 *
 * Returns:     SUCCEED (Can't fail)
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5FD__core_term(void)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Reset VFL ID */
    H5FD_CORE_g = 0;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FD__core_term() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_core_write_tracking
 *
 * Purpose:    Enables/disables core VFD write tracking and page
 *              aggregation size.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_core_write_tracking(hid_t plist_id, hbool_t is_enabled, size_t page_size)
{
    H5P_genplist_t         *plist;               /* Property list pointer */
    H5FD_core_fapl_t        fa;                  /* Core VFD info */
    const H5FD_core_fapl_t *old_fa;              /* Old core VFD info */
    herr_t                  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "ibz", plist_id, is_enabled, page_size);

    /* The page size cannot be zero */
    if (page_size == 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "page_size cannot be zero");

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_PLIST, H5E_BADID, FAIL, "can't find object for ID");
    if (H5FD_CORE != H5P_peek_driver(plist))
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "incorrect VFL driver");
    if (NULL == (old_fa = (const H5FD_core_fapl_t *)H5P_peek_driver_info(plist)))
        old_fa = H5FD__core_get_default_config();

    /* Set VFD info values */
    memset(&fa, 0, sizeof(H5FD_core_fapl_t));
    fa.increment      = old_fa->increment;
    fa.backing_store  = old_fa->backing_store;
    fa.write_tracking = is_enabled;
    fa.page_size      = page_size;

    /* Set the property values & the driver for the FAPL */
    if (H5P_set_driver(plist, H5FD_CORE, &fa, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set core VFD as driver");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_core_write_tracking() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_core_write_tracking
 *
 * Purpose:    Gets information about core VFD write tracking and page
 *              aggregation size.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_core_write_tracking(hid_t plist_id, hbool_t *is_enabled /*out*/, size_t *page_size /*out*/)
{
    H5P_genplist_t         *plist;               /* Property list pointer */
    const H5FD_core_fapl_t *fa;                  /* Core VFD info */
    herr_t                  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "ixx", plist_id, is_enabled, page_size);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_PLIST, H5E_BADID, FAIL, "can't find object for ID");
    if (H5FD_CORE != H5P_peek_driver(plist))
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "incorrect VFL driver");
    if (NULL == (fa = (const H5FD_core_fapl_t *)H5P_peek_driver_info(plist)))
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "bad VFL driver info");

    /* Get values */
    if (is_enabled)
        *is_enabled = fa->write_tracking;
    if (page_size)
        *page_size = fa->page_size;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_core_write_tracking() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_fapl_core
 *
 * Purpose:     Modify the file access property list to use the H5FD_CORE
 *              driver defined in this source file.  The INCREMENT specifies
 *              how much to grow the memory each time we need more.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_fapl_core(hid_t fapl_id, size_t increment, hbool_t backing_store)
{
    H5P_genplist_t  *plist;               /* Property list pointer */
    H5FD_core_fapl_t fa;                  /* Core VFD info */
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "izb", fapl_id, increment, backing_store);

    /* Check argument */
    if (NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list");

    /* Set VFD info values */
    memset(&fa, 0, sizeof(H5FD_core_fapl_t));
    fa.increment      = increment;
    fa.backing_store  = backing_store;
    fa.write_tracking = H5FD_CORE_WRITE_TRACKING_FLAG;
    fa.page_size      = H5FD_CORE_WRITE_TRACKING_PAGE_SIZE;

    /* Set the property values & the driver for the FAPL */
    if (H5P_set_driver(plist, H5FD_CORE, &fa, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set core VFD as driver");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_fapl_core() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_fapl_core
 *
 * Purpose:     Queries properties set by the H5Pset_fapl_core() function.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_fapl_core(hid_t fapl_id, size_t *increment /*out*/, hbool_t *backing_store /*out*/)
{
    H5P_genplist_t         *plist;               /* Property list pointer */
    const H5FD_core_fapl_t *fa;                  /* Core VFD info */
    herr_t                  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "ixx", fapl_id, increment, backing_store);

    if (NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list");
    if (H5FD_CORE != H5P_peek_driver(plist))
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "incorrect VFL driver");
    if (NULL == (fa = (const H5FD_core_fapl_t *)H5P_peek_driver_info(plist)))
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "bad VFL driver info");

    if (increment)
        *increment = fa->increment;
    if (backing_store)
        *backing_store = fa->backing_store;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_fapl_core() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__core_fapl_get
 *
 * Purpose:     Returns a copy of the file access properties.
 *
 * Return:      Success:    Ptr to new file access properties.
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5FD__core_fapl_get(H5FD_t *_file)
{
    H5FD_core_t      *file = (H5FD_core_t *)_file;
    H5FD_core_fapl_t *fa;               /* Core VFD info */
    void             *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    if (NULL == (fa = (H5FD_core_fapl_t *)H5MM_calloc(sizeof(H5FD_core_fapl_t))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    fa->increment      = file->increment;
    fa->backing_store  = (bool)(file->fd >= 0);
    fa->write_tracking = file->write_tracking;
    fa->page_size      = file->bstore_page_size;

    /* Set return value */
    ret_value = fa;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__core_fapl_get() */

/*-------------------------------------------------------------------------
 * Function:    H5FD___core_open
 *
 * Purpose:     Create memory as an HDF5 file.
 *
 * Return:      Success:    A pointer to a new file data structure. The
 *                          public fields will be initialized by the
 *                          caller, which is always H5FD_open().
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static H5FD_t *
H5FD__core_open(const char *name, unsigned flags, hid_t fapl_id, haddr_t maxaddr)
{
    int                     o_flags;
    H5FD_core_t            *file = NULL;
    const H5FD_core_fapl_t *fa   = NULL;
    H5P_genplist_t         *plist; /* Property list pointer */
#ifdef H5_HAVE_WIN32_API
    struct _BY_HANDLE_FILE_INFORMATION fileinfo;
#endif
    h5_stat_t              sb;
    int                    fd = -1;
    H5FD_file_image_info_t file_image_info;
    H5FD_t                *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    if (!name || !*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "invalid file name");
    if (0 == maxaddr || HADDR_UNDEF == maxaddr)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, NULL, "bogus maxaddr");
    if (ADDR_OVERFLOW(maxaddr))
        HGOTO_ERROR(H5E_ARGS, H5E_OVERFLOW, NULL, "maxaddr overflow");
    assert(H5P_DEFAULT != fapl_id);
    if (NULL == (plist = (H5P_genplist_t *)H5I_object(fapl_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a file access property list");
    if (NULL == (fa = (const H5FD_core_fapl_t *)H5P_peek_driver_info(plist)))
        fa = H5FD__core_get_default_config();

    /* Build the open flags */
    o_flags = (H5F_ACC_RDWR & flags) ? O_RDWR : O_RDONLY;
    if (H5F_ACC_TRUNC & flags)
        o_flags |= O_TRUNC;
    if (H5F_ACC_CREAT & flags)
        o_flags |= O_CREAT;
    if (H5F_ACC_EXCL & flags)
        o_flags |= O_EXCL;

    /* Retrieve initial file image info */
    if (H5P_peek(plist, H5F_ACS_FILE_IMAGE_INFO_NAME, &file_image_info) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, NULL, "can't get initial file image info");

    /* If the file image exists and this is an open, make sure the file doesn't exist */
    assert(((file_image_info.buffer != NULL) && (file_image_info.size > 0)) ||
           ((file_image_info.buffer == NULL) && (file_image_info.size == 0)));
    memset(&sb, 0, sizeof(sb));
    if ((file_image_info.buffer != NULL) && !(H5F_ACC_CREAT & flags)) {
        if (HDopen(name, o_flags, H5_POSIX_CREATE_MODE_RW) >= 0)
            HGOTO_ERROR(H5E_FILE, H5E_FILEEXISTS, NULL, "file already exists");

        /* If backing store is requested, create and stat the file
         * Note: We are forcing the O_CREAT flag here, even though this is
         * technically an open.
         */
        if (fa->backing_store) {
            if ((fd = HDopen(name, o_flags | O_CREAT, H5_POSIX_CREATE_MODE_RW)) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, NULL, "unable to create file");
            if (HDfstat(fd, &sb) < 0)
                HSYS_GOTO_ERROR(H5E_FILE, H5E_BADFILE, NULL, "unable to fstat file")
        } /* end if */
    }     /* end if */
    /* Open backing store, and get stat() from file.  The only case that backing
     * store is off is when  the backing_store flag is off and H5F_ACC_CREAT is
     * on. */
    else if (fa->backing_store || !(H5F_ACC_CREAT & flags)) {
        if ((fd = HDopen(name, o_flags, H5_POSIX_CREATE_MODE_RW)) < 0)
            HGOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, NULL, "unable to open file");
        if (HDfstat(fd, &sb) < 0)
            HSYS_GOTO_ERROR(H5E_FILE, H5E_BADFILE, NULL, "unable to fstat file")
    } /* end if */

    /* Create the new file struct */
    if (NULL == (file = (H5FD_core_t *)H5MM_calloc(sizeof(H5FD_core_t))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "unable to allocate file struct");
    file->fd = fd;
    if (name && *name)
        file->name = H5MM_xstrdup(name);

    /* The increment comes from either the file access property list or the
     * default value. But if the file access property list was zero then use
     * the default value instead.
     */
    file->increment = (fa->increment > 0) ? fa->increment : H5FD_CORE_INCREMENT;

    /* If save data in backing store. */
    file->backing_store = fa->backing_store;

    /* Save file image callbacks */
    file->fi_callbacks = file_image_info.callbacks;

    /* Check the file locking flags in the fapl */
    if (ignore_disabled_file_locks_s != FAIL)
        /* The environment variable was set, so use that preferentially */
        file->ignore_disabled_file_locks = ignore_disabled_file_locks_s;
    else {
        /* Use the value in the property list */
        if (H5P_get(plist, H5F_ACS_IGNORE_DISABLED_FILE_LOCKS_NAME, &file->ignore_disabled_file_locks) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTGET, NULL, "can't get ignore disabled file locks property");
    }

    if (fd >= 0) {
        /* Retrieve information for determining uniqueness of file */
#ifdef H5_HAVE_WIN32_API
        file->hFile = (HANDLE)_get_osfhandle(fd);
        if (INVALID_HANDLE_VALUE == file->hFile)
            HGOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, NULL, "unable to get Windows file handle");

        if (!GetFileInformationByHandle((HANDLE)file->hFile, &fileinfo))
            HGOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, NULL, "unable to get Windows file information");

        file->nFileIndexHigh       = fileinfo.nFileIndexHigh;
        file->nFileIndexLow        = fileinfo.nFileIndexLow;
        file->dwVolumeSerialNumber = fileinfo.dwVolumeSerialNumber;
#else  /* H5_HAVE_WIN32_API */
        file->device = sb.st_dev;
        file->inode  = sb.st_ino;
#endif /* H5_HAVE_WIN32_API */
    }  /* end if */

    /* If an existing file is opened, load the whole file into memory. */
    if (!(H5F_ACC_CREAT & flags)) {
        size_t size;

        /* Retrieve file size */
        if (file_image_info.buffer && file_image_info.size > 0)
            size = file_image_info.size;
        else
            size = (size_t)sb.st_size;

        /* Check if we should allocate the memory buffer and read in existing data */
        if (size) {
            /* Allocate memory for the file's data, using the file image callback if available. */
            if (file->fi_callbacks.image_malloc) {
                if (NULL == (file->mem = (unsigned char *)file->fi_callbacks.image_malloc(
                                 size, H5FD_FILE_IMAGE_OP_FILE_OPEN, file->fi_callbacks.udata)))
                    HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "image malloc callback failed");
            } /* end if */
            else {
                if (NULL == (file->mem = (unsigned char *)H5MM_malloc(size)))
                    HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "unable to allocate memory block");
            } /* end else */

            /* Set up data structures */
            file->eof = size;

            /* If there is an initial file image, copy it, using the callback if possible */
            if (file_image_info.buffer && file_image_info.size > 0) {
                if (file->fi_callbacks.image_memcpy) {
                    if (file->mem != file->fi_callbacks.image_memcpy(file->mem, file_image_info.buffer, size,
                                                                     H5FD_FILE_IMAGE_OP_FILE_OPEN,
                                                                     file->fi_callbacks.udata))
                        HGOTO_ERROR(H5E_FILE, H5E_CANTCOPY, NULL, "image_memcpy callback failed");
                } /* end if */
                else
                    H5MM_memcpy(file->mem, file_image_info.buffer, size);
            } /* end if */
            /* Read in existing data from the file if there is no image */
            else {
                /* Read in existing data, being careful of interrupted system calls,
                 * partial results, and the end of the file.
                 */

                uint8_t *mem    = file->mem;  /* memory pointer for writes */
                HDoff_t  offset = (HDoff_t)0; /* offset for reading */

                while (size > 0) {
                    h5_posix_io_t     bytes_in   = 0;  /* # of bytes to read       */
                    h5_posix_io_ret_t bytes_read = -1; /* # of bytes actually read */

                    /* Trying to read more bytes than the return type can handle is
                     * undefined behavior in POSIX.
                     */
                    if (size > H5_POSIX_MAX_IO_BYTES)
                        bytes_in = H5_POSIX_MAX_IO_BYTES;
                    else
                        bytes_in = (h5_posix_io_t)size;

                    do {
#ifdef H5_HAVE_PREADWRITE
                        bytes_read = HDpread(file->fd, mem, bytes_in, offset);
                        if (bytes_read > 0)
                            offset += bytes_read;
#else
                        bytes_read = HDread(file->fd, mem, bytes_in);
#endif /* H5_HAVE_PREADWRITE */
                    } while (-1 == bytes_read && EINTR == errno);

                    if (-1 == bytes_read) { /* error */
                        int    myerrno = errno;
                        time_t mytime  = HDtime(NULL);

                        offset = HDlseek(file->fd, (HDoff_t)0, SEEK_CUR);

                        HGOTO_ERROR(
                            H5E_IO, H5E_READERROR, NULL,
                            "file read failed: time = %s, filename = '%s', file descriptor = %d, errno = %d, "
                            "error message = '%s', file->mem = %p, total read size = %llu, bytes this "
                            "sub-read = %llu, bytes actually read = %llu, offset = %llu",
                            HDctime(&mytime), file->name, file->fd, myerrno, strerror(myerrno),
                            (void *)file->mem, (unsigned long long)size, (unsigned long long)bytes_in,
                            (unsigned long long)bytes_read, (unsigned long long)offset);
                    } /* end if */

                    assert(bytes_read >= 0);
                    assert((size_t)bytes_read <= size);

                    mem += bytes_read;
                    size -= (size_t)bytes_read;
                } /* end while */
            }     /* end else */
        }         /* end if */
    }             /* end if */

    /* Get the write tracking & page size */
    file->write_tracking   = fa->write_tracking;
    file->bstore_page_size = fa->page_size;

    /* Set up write tracking if the backing store is on */
    file->dirty_list = NULL;
    if (fa->backing_store) {
        bool use_write_tracking = false; /* what we're actually doing */

        /* default is to have write tracking OFF for create (hence the check to see
         * if the user explicitly set a page size) and ON with the default page size
         * on open (when not read-only).
         */
        /* Only use write tracking if the file is open for writing */
        use_write_tracking = (true == fa->write_tracking) /* user asked for write tracking */
                             && !(o_flags & O_RDONLY)     /* file is open for writing (i.e. not read-only) */
                             && (file->bstore_page_size != 0); /* page size is not zero */

        /* initialize the dirty list */
        if (use_write_tracking)
            if (NULL == (file->dirty_list = H5SL_create(H5SL_TYPE_HADDR, NULL)))
                HGOTO_ERROR(H5E_SLIST, H5E_CANTCREATE, NULL, "can't create core vfd dirty region list");
    } /* end if */

    /* Set return value */
    ret_value = (H5FD_t *)file;

done:
    if (!ret_value && file) {
        if (file->fd >= 0)
            HDclose(file->fd);
        H5MM_xfree(file->name);
        H5MM_xfree(file->mem);
        H5MM_xfree(file);
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__core_open() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__core_close
 *
 * Purpose:     Closes the file.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__core_close(H5FD_t *_file)
{
    H5FD_core_t *file      = (H5FD_core_t *)_file;
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Flush any changed buffers */
    if (H5FD__core_flush(_file, (hid_t)-1, true) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTFLUSH, FAIL, "unable to flush core vfd backing store");

    /* Destroy the dirty region list */
    if (file->dirty_list)
        if (H5FD__core_destroy_dirty_list(file) != SUCCEED)
            HGOTO_ERROR(H5E_VFL, H5E_CANTFREE, FAIL, "unable to free core vfd dirty region list");

    /* Release resources */
    if (file->fd >= 0)
        HDclose(file->fd);
    if (file->name)
        H5MM_xfree(file->name);
    if (file->mem) {
        /* Use image callback if available */
        if (file->fi_callbacks.image_free) {
            if (file->fi_callbacks.image_free(file->mem, H5FD_FILE_IMAGE_OP_FILE_CLOSE,
                                              file->fi_callbacks.udata) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTFREE, FAIL, "image_free callback failed");
        } /* end if */
        else
            H5MM_xfree(file->mem);
    } /* end if */
    memset(file, 0, sizeof(H5FD_core_t));
    H5MM_xfree(file);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__core_close() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__core_cmp
 *
 * Purpose:     Compares two files belonging to this driver by name. If one
 *              file doesn't have a name then it is less than the other file.
 *              If neither file has a name then the comparison is by file
 *              address.
 *
 * Return:      Success:    A value like strcmp()
 *              Failure:    never fails (arguments were checked by the
 *                          caller).
 *
 *-------------------------------------------------------------------------
 */
static int
H5FD__core_cmp(const H5FD_t *_f1, const H5FD_t *_f2)
{
    const H5FD_core_t *f1        = (const H5FD_core_t *)_f1;
    const H5FD_core_t *f2        = (const H5FD_core_t *)_f2;
    int                ret_value = 0;

    FUNC_ENTER_PACKAGE_NOERR

    if (f1->fd >= 0 && f2->fd >= 0) {
        /* Compare low level file information for backing store */
#ifdef H5_HAVE_WIN32_API
        if (f1->dwVolumeSerialNumber < f2->dwVolumeSerialNumber)
            HGOTO_DONE(-1);
        if (f1->dwVolumeSerialNumber > f2->dwVolumeSerialNumber)
            HGOTO_DONE(1);

        if (f1->nFileIndexHigh < f2->nFileIndexHigh)
            HGOTO_DONE(-1);
        if (f1->nFileIndexHigh > f2->nFileIndexHigh)
            HGOTO_DONE(1);

        if (f1->nFileIndexLow < f2->nFileIndexLow)
            HGOTO_DONE(-1);
        if (f1->nFileIndexLow > f2->nFileIndexLow)
            HGOTO_DONE(1);

#else
#ifdef H5_DEV_T_IS_SCALAR
        if (f1->device < f2->device)
            HGOTO_DONE(-1);
        if (f1->device > f2->device)
            HGOTO_DONE(1);
#else  /* H5_DEV_T_IS_SCALAR */
        /* If dev_t isn't a scalar value on this system, just use memcmp to
         * determine if the values are the same or not.  The actual return value
         * shouldn't really matter...
         */
        if (memcmp(&(f1->device), &(f2->device), sizeof(dev_t)) < 0)
            HGOTO_DONE(-1);
        if (memcmp(&(f1->device), &(f2->device), sizeof(dev_t)) > 0)
            HGOTO_DONE(1);
#endif /* H5_DEV_T_IS_SCALAR */

        if (f1->inode < f2->inode)
            HGOTO_DONE(-1);
        if (f1->inode > f2->inode)
            HGOTO_DONE(1);

#endif /*H5_HAVE_WIN32_API*/
    }  /* end if */
    else {
        if (NULL == f1->name && NULL == f2->name) {
            if (f1 < f2)
                HGOTO_DONE(-1);
            if (f1 > f2)
                HGOTO_DONE(1);
            HGOTO_DONE(0);
        } /* end if */

        if (NULL == f1->name)
            HGOTO_DONE(-1);
        if (NULL == f2->name)
            HGOTO_DONE(1);

        ret_value = strcmp(f1->name, f2->name);
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__core_cmp() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__core_query
 *
 * Purpose:     Set the flags that this VFL driver is capable of supporting.
 *              (listed in H5FDpublic.h)
 *
 * Return:      SUCCEED (Can't fail)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__core_query(const H5FD_t *_file, unsigned long *flags /* out */)
{
    const H5FD_core_t *file = (const H5FD_core_t *)_file;

    FUNC_ENTER_PACKAGE_NOERR

    /* clang-format off */
    /* Set the VFL feature flags that this driver supports */
    if(flags) {
        *flags = 0;
        *flags |= H5FD_FEAT_AGGREGATE_METADATA;             /* OK to aggregate metadata allocations                             */
        *flags |= H5FD_FEAT_ACCUMULATE_METADATA;            /* OK to accumulate metadata for faster writes                      */
        *flags |= H5FD_FEAT_DATA_SIEVE;                     /* OK to perform data sieving for faster raw data reads & writes    */
        *flags |= H5FD_FEAT_AGGREGATE_SMALLDATA;            /* OK to aggregate "small" raw data allocations                     */
        *flags |= H5FD_FEAT_ALLOW_FILE_IMAGE;               /* OK to use file image feature with this VFD                       */
        *flags |= H5FD_FEAT_CAN_USE_FILE_IMAGE_CALLBACKS;   /* OK to use file image callbacks with this VFD                     */

        /* These feature flags are only applicable if the backing store is enabled */
        if(file && file->fd >= 0 && file->backing_store) {
            *flags |= H5FD_FEAT_POSIX_COMPAT_HANDLE;        /* get_handle callback returns a POSIX file descriptor              */
            *flags |= H5FD_FEAT_DEFAULT_VFD_COMPATIBLE;     /* VFD creates a file which can be opened with the default VFD      */
        }
    } /* end if */
    /* clang-format on */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FD__core_query() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__core_get_eoa
 *
 * Purpose:     Gets the end-of-address marker for the file. The EOA marker
 *              is the first address past the last byte allocated in the
 *              format address space.
 *
 * Return:      The end-of-address marker. (Can't fail)
 *
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD__core_get_eoa(const H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type)
{
    const H5FD_core_t *file = (const H5FD_core_t *)_file;

    FUNC_ENTER_PACKAGE_NOERR

    FUNC_LEAVE_NOAPI(file->eoa)
} /* end H5FD__core_get_eoa() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__core_set_eoa
 *
 * Purpose:     Set the end-of-address marker for the file. This function is
 *              called shortly after an existing HDF5 file is opened in order
 *              to tell the driver where the end of the HDF5 data is located.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__core_set_eoa(H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type, haddr_t addr)
{
    H5FD_core_t *file      = (H5FD_core_t *)_file;
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    if (ADDR_OVERFLOW(addr))
        HGOTO_ERROR(H5E_ARGS, H5E_OVERFLOW, FAIL, "address overflow");

    file->eoa = addr;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__core_set_eoa() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__core_get_eof
 *
 * Purpose:     Returns the end-of-file marker, which is the greater of
 *              either the size of the underlying memory or the HDF5
 *              end-of-address markers.
 *
 * Return:      End of file address, the first address past
 *              the end of the "file", either the memory
 *              or the HDF5 file. (Can't fail)
 *
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD__core_get_eof(const H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type)
{
    const H5FD_core_t *file = (const H5FD_core_t *)_file;

    FUNC_ENTER_PACKAGE_NOERR

    FUNC_LEAVE_NOAPI(file->eof)
} /* end H5FD__core_get_eof() */

/*-------------------------------------------------------------------------
 * Function:       H5FD__core_get_handle
 *
 * Purpose:        Gets the file handle of CORE file driver.
 *
 * Returns:        SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__core_get_handle(H5FD_t *_file, hid_t fapl, void **file_handle)
{
    H5FD_core_t *file      = (H5FD_core_t *)_file; /* core VFD info */
    herr_t       ret_value = SUCCEED;              /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    if (!file_handle)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "file handle not valid");

    /* Check for non-default FAPL */
    if (H5P_FILE_ACCESS_DEFAULT != fapl && H5P_DEFAULT != fapl) {
        H5P_genplist_t *plist; /* Property list pointer */

        /* Get the FAPL */
        if (NULL == (plist = (H5P_genplist_t *)H5I_object(fapl)))
            HGOTO_ERROR(H5E_VFL, H5E_BADTYPE, FAIL, "not a file access property list");

        /* Check if private property for retrieving the backing store POSIX
         * file descriptor is set.  (This should not be set except within the
         * library)  QAK - 2009/12/04
         */
        if (H5P_exist_plist(plist, H5F_ACS_WANT_POSIX_FD_NAME) > 0) {
            bool want_posix_fd; /* Setting for retrieving file descriptor from core VFD */

            /* Get property */
            if (H5P_get(plist, H5F_ACS_WANT_POSIX_FD_NAME, &want_posix_fd) < 0)
                HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "can't get property of retrieving file descriptor");

            /* If property is set, pass back the file descriptor instead of the memory address */
            if (want_posix_fd)
                *file_handle = &(file->fd);
            else
                *file_handle = &(file->mem);
        } /* end if */
        else
            *file_handle = &(file->mem);
    } /* end if */
    else
        *file_handle = &(file->mem);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__core_get_handle() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__core_read
 *
 * Purpose:     Reads SIZE bytes of data from FILE beginning at address ADDR
 *              into buffer BUF according to data transfer properties in
 *              DXPL_ID.
 *
 * Return:      Success:    SUCCEED. Result is stored in caller-supplied
 *                          buffer BUF.
 *              Failure:    FAIL, Contents of buffer BUF are undefined.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__core_read(H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type, hid_t H5_ATTR_UNUSED dxpl_id, haddr_t addr,
                size_t size, void *buf /*out*/)
{
    H5FD_core_t *file      = (H5FD_core_t *)_file;
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(file && file->pub.cls);
    assert(buf);

    /* Check for overflow conditions */
    if (HADDR_UNDEF == addr)
        HGOTO_ERROR(H5E_IO, H5E_OVERFLOW, FAIL, "file address overflowed");
    if (REGION_OVERFLOW(addr, size))
        HGOTO_ERROR(H5E_IO, H5E_OVERFLOW, FAIL, "file address overflowed");

    /* Read the part which is before the EOF marker */
    if (addr < file->eof) {
        size_t nbytes;
#ifndef NDEBUG
        hsize_t temp_nbytes;

        temp_nbytes = file->eof - addr;
        H5_CHECK_OVERFLOW(temp_nbytes, hsize_t, size_t);
        nbytes = MIN(size, (size_t)temp_nbytes);
#else  /* NDEBUG */
        nbytes = MIN(size, (size_t)(file->eof - addr));
#endif /* NDEBUG */

        H5MM_memcpy(buf, file->mem + addr, nbytes);
        size -= nbytes;
        addr += nbytes;
        buf = (char *)buf + nbytes;
    }

    /* Read zeros for the part which is after the EOF markers */
    if (size > 0)
        memset(buf, 0, size);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__core_read() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__core_write
 *
 * Purpose:     Writes SIZE bytes of data to FILE beginning at address ADDR
 *              from buffer BUF according to data transfer properties in
 *              DXPL_ID.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__core_write(H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type, hid_t H5_ATTR_UNUSED dxpl_id, haddr_t addr,
                 size_t size, const void *buf)
{
    H5FD_core_t *file      = (H5FD_core_t *)_file;
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(file && file->pub.cls);
    assert(buf);

    /* Check for overflow conditions */
    if (REGION_OVERFLOW(addr, size))
        HGOTO_ERROR(H5E_IO, H5E_OVERFLOW, FAIL, "file address overflowed");

    /*
     * Allocate more memory if necessary, careful of overflow. Also, if the
     * allocation fails then the file should remain in a usable state.  Be
     * careful of non-Posix realloc() that doesn't understand what to do when
     * the first argument is null.
     */
    if (addr + size > file->eof) {
        unsigned char *x;
        size_t         new_eof;

        /* Determine new size of memory buffer */
        H5_CHECKED_ASSIGN(new_eof, size_t, file->increment * ((addr + size) / file->increment), hsize_t);
        if ((addr + size) % file->increment)
            new_eof += file->increment;

        /* (Re)allocate memory for the file buffer, using callbacks if available */
        if (file->fi_callbacks.image_realloc) {
            if (NULL == (x = (unsigned char *)file->fi_callbacks.image_realloc(
                             file->mem, new_eof, H5FD_FILE_IMAGE_OP_FILE_RESIZE, file->fi_callbacks.udata)))
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                            "unable to allocate memory block of %llu bytes with callback",
                            (unsigned long long)new_eof);
        } /* end if */
        else {
            if (NULL == (x = (unsigned char *)H5MM_realloc(file->mem, new_eof)))
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                            "unable to allocate memory block of %llu bytes", (unsigned long long)new_eof);
        } /* end else */

        memset(x + file->eof, 0, (size_t)(new_eof - file->eof));
        file->mem = x;

        file->eof = new_eof;
    } /* end if */

    /* Add the buffer region to the dirty list if using that optimization */
    if (file->dirty_list) {
        haddr_t start = addr;
        haddr_t end   = addr + (haddr_t)size - 1;

        if (H5FD__core_add_dirty_region(file, start, end) != SUCCEED)
            HGOTO_ERROR(
                H5E_VFL, H5E_CANTINSERT, FAIL,
                "unable to add core VFD dirty region during write call - addresses: start=%llu end=%llu",
                (unsigned long long)start, (unsigned long long)end);
    }

    /* Write from BUF to memory */
    H5MM_memcpy(file->mem + addr, buf, size);

    /* Mark memory buffer as modified */
    file->dirty = true;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__core_write() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__core_flush
 *
 * Purpose:     Flushes the file to backing store if there is any and if the
 *              dirty flag is set.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__core_flush(H5FD_t *_file, hid_t H5_ATTR_UNUSED dxpl_id, bool H5_ATTR_UNUSED closing)
{
    H5FD_core_t *file      = (H5FD_core_t *)_file;
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Write to backing store */
    if (file->dirty && file->fd >= 0 && file->backing_store) {

        /* Use the dirty list, if available */
        if (file->dirty_list) {
            H5FD_core_region_t *item = NULL;
            size_t              size;

            while (NULL != (item = (H5FD_core_region_t *)H5SL_remove_first(file->dirty_list))) {

                /* The file may have been truncated, so check for that
                 * and skip or adjust as necessary.
                 */
                if (item->start < file->eof) {
                    if (item->end >= file->eof)
                        item->end = file->eof - 1;

                    size = (size_t)((item->end - item->start) + 1);

                    if (H5FD__core_write_to_bstore(file, item->start, size) != SUCCEED)
                        HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, FAIL, "unable to write to backing store");
                } /* end if */

                item = H5FL_FREE(H5FD_core_region_t, item);
            } /* end while */

        } /* end if */
        /* Otherwise, write the entire file out at once */
        else {
            if (H5FD__core_write_to_bstore(file, (haddr_t)0, (size_t)file->eof) != SUCCEED)
                HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, FAIL, "unable to write to backing store");
        } /* end else */

        file->dirty = false;
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__core_flush() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__core_truncate
 *
 * Purpose:     Makes sure that the true file size is the same (or larger)
 *              than the end-of-address.
 *
 *              Addendum -- 12/2/11
 *              For file images opened with the core file driver, it is
 *              necessary that we avoid reallocating the core file driver's
 *              buffer unnecessarily.
 *
 *              To this end, I have made the following functional changes
 *              to this function.
 *
 *              If we are closing, and there is no backing store, this
 *              function becomes a no-op.
 *
 *              If we are closing, and there is backing store, we set the
 *              eof to equal the eoa, and truncate the backing store to
 *              the new eof
 *
 *              If we are not closing, we realloc the buffer to size equal
 *              to the smallest multiple of the allocation increment that
 *              equals or exceeds the eoa and set the eof accordingly.
 *              Note that we no longer truncate    the backing store to the
 *              new eof if applicable.
 *                                                                  -- JRM
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__core_truncate(H5FD_t *_file, hid_t H5_ATTR_UNUSED dxpl_id, bool closing)
{
    H5FD_core_t *file = (H5FD_core_t *)_file;
    size_t       new_eof;             /* New size of memory buffer */
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(file);

    /* if we are closing and not using backing store, do nothing */
    if (!closing || file->backing_store) {
        if (closing) /* set eof to eoa */
            new_eof = file->eoa;
        else { /* set eof to smallest multiple of increment that exceeds eoa */
            /* Determine new size of memory buffer */
            H5_CHECKED_ASSIGN(new_eof, size_t, file->increment * (file->eoa / file->increment), hsize_t);
            if (file->eoa % file->increment)
                new_eof += file->increment;
        } /* end else */

        /* Extend the file to make sure it's large enough */
        if (!H5_addr_eq(file->eof, (haddr_t)new_eof)) {
            unsigned char *x; /* Pointer to new buffer for file data */

            /* (Re)allocate memory for the file buffer, using callback if available */
            if (file->fi_callbacks.image_realloc) {
                if (NULL ==
                    (x = (unsigned char *)file->fi_callbacks.image_realloc(
                         file->mem, new_eof, H5FD_FILE_IMAGE_OP_FILE_RESIZE, file->fi_callbacks.udata)))
                    HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                                "unable to allocate memory block with callback");
            } /* end if */
            else {
                if (NULL == (x = (unsigned char *)H5MM_realloc(file->mem, new_eof)))
                    HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "unable to allocate memory block");
            } /* end else */

            if (file->eof < new_eof)
                memset(x + file->eof, 0, (size_t)(new_eof - file->eof));
            file->mem = x;

            /* Update backing store, if using it and if closing */
            if (closing && (file->fd >= 0) && file->backing_store) {
#ifdef H5_HAVE_WIN32_API
                LARGE_INTEGER li;       /* 64-bit (union) integer for SetFilePointer() call */
                DWORD         dwPtrLow; /* Low-order pointer bits from SetFilePointer()
                                         * Only used as an error code here.
                                         */
                DWORD dwError;          /* DWORD error code from GetLastError() */
                BOOL  bError;           /* Boolean error flag */

                /* Windows uses this odd QuadPart union for 32/64-bit portability */
                li.QuadPart = (LONGLONG)file->eoa;

                /* Extend the file to make sure it's large enough.
                 *
                 * Since INVALID_SET_FILE_POINTER can technically be a valid return value
                 * from SetFilePointer(), we also need to check GetLastError().
                 */
                dwPtrLow = SetFilePointer(file->hFile, li.LowPart, &li.HighPart, FILE_BEGIN);
                if (INVALID_SET_FILE_POINTER == dwPtrLow) {
                    dwError = GetLastError();
                    if (dwError != NO_ERROR)
                        HGOTO_ERROR(H5E_FILE, H5E_FILEOPEN, FAIL, "unable to set file pointer");
                }

                bError = SetEndOfFile(file->hFile);
                if (0 == bError)
                    HGOTO_ERROR(H5E_IO, H5E_SEEKERROR, FAIL, "unable to extend file properly");
#else  /* H5_HAVE_WIN32_API */
                if (-1 == HDftruncate(file->fd, (HDoff_t)new_eof))
                    HSYS_GOTO_ERROR(H5E_IO, H5E_SEEKERROR, FAIL, "unable to extend file properly")
#endif /* H5_HAVE_WIN32_API */

            } /* end if */

            /* Update the eof value */
            file->eof = new_eof;
        } /* end if */
    }     /* end if(file->eof < file->eoa) */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__core_truncate() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__core_lock
 *
 * Purpose:     To place an advisory lock on a file.
 *        The lock type to apply depends on the parameter "rw":
 *            true--opens for write: an exclusive lock
 *            false--opens for read: a shared lock
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__core_lock(H5FD_t *_file, bool rw)
{
    H5FD_core_t *file = (H5FD_core_t *)_file; /* VFD file struct          */
    int          lock_flags;                  /* file locking flags       */
    herr_t       ret_value = SUCCEED;         /* Return value             */

    FUNC_ENTER_PACKAGE

    assert(file);

    /* Only set the lock if there is a file descriptor. If no file
     * descriptor, this is a no-op.
     */
    if (file->fd >= 0) {
        /* Set exclusive or shared lock based on rw status */
        lock_flags = rw ? LOCK_EX : LOCK_SH;

        /* Place a non-blocking lock on the file */
        if (HDflock(file->fd, lock_flags | LOCK_NB) < 0) {
            if (file->ignore_disabled_file_locks && ENOSYS == errno) {
                /* When errno is set to ENOSYS, the file system does not support
                 * locking, so ignore it.
                 */
                errno = 0;
            }
            else
                HSYS_GOTO_ERROR(H5E_FILE, H5E_BADFILE, FAIL, "unable to lock file")
        } /* end if */
    }     /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__core_lock() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__core_unlock
 *
 * Purpose:     To remove the existing lock on the file
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__core_unlock(H5FD_t *_file)
{
    H5FD_core_t *file      = (H5FD_core_t *)_file; /* VFD file struct */
    herr_t       ret_value = SUCCEED;              /* Return value */

    FUNC_ENTER_PACKAGE

    assert(file);

    if (file->fd >= 0)
        if (HDflock(file->fd, LOCK_UN) < 0) {
            if (file->ignore_disabled_file_locks && ENOSYS == errno) {
                /* When errno is set to ENOSYS, the file system does not support
                 * locking, so ignore it.
                 */
                errno = 0;
            }
            else
                HSYS_GOTO_ERROR(H5E_FILE, H5E_BADFILE, FAIL, "unable to unlock file")
        }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__core_unlock() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__core_delete
 *
 * Purpose:     Delete a file
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__core_delete(const char *filename, hid_t fapl_id)
{
    const H5FD_core_fapl_t *fa = NULL;
    H5P_genplist_t         *plist;               /* Property list pointer */
    herr_t                  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(filename);

    if (NULL == (plist = (H5P_genplist_t *)H5I_object(fapl_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list");
    if (NULL == (fa = (const H5FD_core_fapl_t *)H5P_peek_driver_info(plist)))
        fa = H5FD__core_get_default_config();

    if (fa->backing_store)
        if (HDremove(filename) < 0)
            HSYS_GOTO_ERROR(H5E_VFL, H5E_CANTDELETEFILE, FAIL, "unable to delete file")

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__core_delete() */
