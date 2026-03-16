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
 * Purpose:  The Direct I/O file driver forces the data to be written to
 *    the file directly without being copied into system kernel
 *    buffer.  The main system support this feature is Linux.
 */

#include "H5FDdrvr_module.h" /* This source code file is part of the H5FD driver module */

#include "H5private.h"   /* Generic Functions        */
#include "H5Eprivate.h"  /* Error handling           */
#include "H5Fprivate.h"  /* File access              */
#include "H5FDprivate.h" /* File drivers             */
#include "H5FDdirect.h"  /* Direct file driver       */
#include "H5FLprivate.h" /* Free Lists               */
#include "H5Iprivate.h"  /* IDs                      */
#include "H5MMprivate.h" /* Memory management        */
#include "H5Pprivate.h"  /* Property lists           */

#ifdef H5_HAVE_DIRECT

/* The driver identification number, initialized at runtime */
static hid_t H5FD_DIRECT_g = 0;

/* Whether to ignore file locks when disabled (env var value) */
static htri_t ignore_disabled_file_locks_s = FAIL;

/* File operations */
#define OP_UNKNOWN 0
#define OP_READ    1
#define OP_WRITE   2

/* Driver-specific file access properties */
typedef struct H5FD_direct_fapl_t {
    size_t mboundary;  /* Memory boundary for alignment    */
    size_t fbsize;     /* File system block size      */
    size_t cbsize;     /* Maximal buffer size for copying user data  */
    bool   must_align; /* Decides if data alignment is required        */
} H5FD_direct_fapl_t;

/*
 * The description of a file belonging to this driver. The `eoa' and `eof'
 * determine the amount of hdf5 address space in use and the high-water mark
 * of the file (the current size of the underlying Unix file). The `pos'
 * value is used to eliminate file position updates when they would be a
 * no-op. Unfortunately we've found systems that use separate file position
 * indicators for reading and writing so the lseek can only be eliminated if
 * the current operation is the same as the previous operation.  When opening
 * a file the `eof' will be set to the current file size, `eoa' will be set
 * to zero, `pos' will be set to H5F_ADDR_UNDEF (as it is when an error
 * occurs), and `op' will be set to H5F_OP_UNKNOWN.
 */
typedef struct H5FD_direct_t {
    H5FD_t             pub; /*public stuff, must be first  */
    int                fd;  /*the unix file      */
    haddr_t            eoa; /*end of allocated region  */
    haddr_t            eof; /*end of file; current file size*/
    haddr_t            pos; /*current file I/O position  */
    int                op;  /*last operation    */
    H5FD_direct_fapl_t fa;  /*file access properties  */
    bool               ignore_disabled_file_locks;
#ifndef H5_HAVE_WIN32_API
    /*
     * On most systems the combination of device and i-node number uniquely
     * identify a file.
     */
    dev_t device; /*file device number    */
    ino_t inode;  /*file i-node number    */
#else
    /*
     * On H5_HAVE_WIN32_API the low-order word of a unique identifier associated with the
     * file and the volume serial number uniquely identify a file. This number
     * (which, both? -rpm) may change when the system is restarted or when the
     * file is opened. After a process opens a file, the identifier is
     * constant until the file is closed. An application can use this
     * identifier and the volume serial number to determine whether two
     * handles refer to the same file.
     */
    DWORD fileindexlo;
    DWORD fileindexhi;
#endif

} H5FD_direct_t;

/*
 * These macros check for overflow of various quantities.  These macros
 * assume that HDoff_t is signed and haddr_t and size_t are unsigned.
 *
 * ADDR_OVERFLOW:  Checks whether a file address of type `haddr_t'
 *      is too large to be represented by the second argument
 *      of the file seek function.
 *
 * SIZE_OVERFLOW:  Checks whether a buffer size of type `hsize_t' is too
 *      large to be represented by the `size_t' type.
 *
 * REGION_OVERFLOW:  Checks whether an address and size pair describe data
 *      which can be addressed entirely by the second
 *      argument of the file seek function.
 */
#define MAXADDR          (((haddr_t)1 << (8 * sizeof(HDoff_t) - 1)) - 1)
#define ADDR_OVERFLOW(A) (HADDR_UNDEF == (A) || ((A) & ~(haddr_t)MAXADDR))
#define SIZE_OVERFLOW(Z) ((Z) & ~(hsize_t)MAXADDR)
#define REGION_OVERFLOW(A, Z)                                                                                \
    (ADDR_OVERFLOW(A) || SIZE_OVERFLOW(Z) || HADDR_UNDEF == (A) + (Z) || (HDoff_t)((A) + (Z)) < (HDoff_t)(A))

/* Prototypes */
static herr_t  H5FD__direct_term(void);
static herr_t  H5FD__direct_populate_config(size_t boundary, size_t block_size, size_t cbuf_size,
                                            H5FD_direct_fapl_t *fa_out);
static void   *H5FD__direct_fapl_get(H5FD_t *file);
static void   *H5FD__direct_fapl_copy(const void *_old_fa);
static H5FD_t *H5FD__direct_open(const char *name, unsigned flags, hid_t fapl_id, haddr_t maxaddr);
static herr_t  H5FD__direct_close(H5FD_t *_file);
static int     H5FD__direct_cmp(const H5FD_t *_f1, const H5FD_t *_f2);
static herr_t  H5FD__direct_query(const H5FD_t *_f1, unsigned long *flags);
static haddr_t H5FD__direct_get_eoa(const H5FD_t *_file, H5FD_mem_t type);
static herr_t  H5FD__direct_set_eoa(H5FD_t *_file, H5FD_mem_t type, haddr_t addr);
static haddr_t H5FD__direct_get_eof(const H5FD_t *_file, H5FD_mem_t type);
static herr_t  H5FD__direct_get_handle(H5FD_t *_file, hid_t fapl, void **file_handle);
static herr_t  H5FD__direct_read(H5FD_t *_file, H5FD_mem_t type, hid_t fapl_id, haddr_t addr, size_t size,
                                 void *buf);
static herr_t  H5FD__direct_write(H5FD_t *_file, H5FD_mem_t type, hid_t fapl_id, haddr_t addr, size_t size,
                                  const void *buf);
static herr_t  H5FD__direct_truncate(H5FD_t *_file, hid_t dxpl_id, bool closing);
static herr_t  H5FD__direct_lock(H5FD_t *_file, bool rw);
static herr_t  H5FD__direct_unlock(H5FD_t *_file);
static herr_t  H5FD__direct_delete(const char *filename, hid_t fapl_id);

static const H5FD_class_t H5FD_direct_g = {
    H5FD_CLASS_VERSION,         /* struct version       */
    H5FD_DIRECT_VALUE,          /* value                */
    "direct",                   /* name                 */
    MAXADDR,                    /* maxaddr              */
    H5F_CLOSE_WEAK,             /* fc_degree            */
    H5FD__direct_term,          /* terminate            */
    NULL,                       /* sb_size              */
    NULL,                       /* sb_encode            */
    NULL,                       /* sb_decode            */
    sizeof(H5FD_direct_fapl_t), /* fapl_size            */
    H5FD__direct_fapl_get,      /* fapl_get             */
    H5FD__direct_fapl_copy,     /* fapl_copy            */
    NULL,                       /* fapl_free            */
    0,                          /* dxpl_size            */
    NULL,                       /* dxpl_copy            */
    NULL,                       /* dxpl_free            */
    H5FD__direct_open,          /* open                 */
    H5FD__direct_close,         /* close                */
    H5FD__direct_cmp,           /* cmp                  */
    H5FD__direct_query,         /* query                */
    NULL,                       /* get_type_map         */
    NULL,                       /* alloc                */
    NULL,                       /* free                 */
    H5FD__direct_get_eoa,       /* get_eoa              */
    H5FD__direct_set_eoa,       /* set_eoa              */
    H5FD__direct_get_eof,       /* get_eof              */
    H5FD__direct_get_handle,    /* get_handle           */
    H5FD__direct_read,          /* read                 */
    H5FD__direct_write,         /* write                */
    NULL,                       /* read_vector          */
    NULL,                       /* write_vector         */
    NULL,                       /* read_selection       */
    NULL,                       /* write_selection      */
    NULL,                       /* flush                */
    H5FD__direct_truncate,      /* truncate             */
    H5FD__direct_lock,          /* lock                 */
    H5FD__direct_unlock,        /* unlock               */
    H5FD__direct_delete,        /* del                  */
    NULL,                       /* ctl                  */
    H5FD_FLMAP_DICHOTOMY        /* fl_map               */
};

/* Declare a free list to manage the H5FD_direct_t struct */
H5FL_DEFINE_STATIC(H5FD_direct_t);

/*-------------------------------------------------------------------------
 * Function:    H5FD_direct_init
 *
 * Purpose:     Initialize this driver by registering the driver with the
 *              library.
 *
 * Return:      Success:    The driver ID for the direct driver
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5FD_direct_init(void)
{
    char *lock_env_var = NULL;            /* Environment variable pointer */
    hid_t ret_value    = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_NOAPI(H5I_INVALID_HID)

    /* Check the use disabled file locks environment variable */
    lock_env_var = getenv(HDF5_USE_FILE_LOCKING);
    if (lock_env_var && !strcmp(lock_env_var, "BEST_EFFORT"))
        ignore_disabled_file_locks_s = true; /* Override: Ignore disabled locks */
    else if (lock_env_var && (!strcmp(lock_env_var, "TRUE") || !strcmp(lock_env_var, "1")))
        ignore_disabled_file_locks_s = false; /* Override: Don't ignore disabled locks */
    else
        ignore_disabled_file_locks_s = FAIL; /* Environment variable not set, or not set correctly */

    if (H5I_VFL != H5I_get_type(H5FD_DIRECT_g)) {
        H5FD_DIRECT_g = H5FD_register(&H5FD_direct_g, sizeof(H5FD_class_t), false);
        if (H5I_INVALID_HID == H5FD_DIRECT_g)
            HGOTO_ERROR(H5E_ID, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register direct");
    }

    /* Set return value */
    ret_value = H5FD_DIRECT_g;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_direct_init() */

/*---------------------------------------------------------------------------
 * Function:  H5FD__direct_term
 *
 * Purpose:  Shut down the VFD
 *
 * Returns:     Non-negative on success or negative on failure
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5FD__direct_term(void)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Reset VFL ID */
    H5FD_DIRECT_g = 0;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FD__direct_term() */

/*-------------------------------------------------------------------------
 * Function:  H5Pset_fapl_direct
 *
 * Purpose:  Modify the file access property list to use the H5FD_DIRECT
 *    driver defined in this source file.  There are no driver
 *    specific properties.
 *
 * Return:  Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_fapl_direct(hid_t fapl_id, size_t boundary, size_t block_size, size_t cbuf_size)
{
    H5P_genplist_t    *plist; /* Property list pointer */
    H5FD_direct_fapl_t fa;
    herr_t             ret_value;

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "izzz", fapl_id, boundary, block_size, cbuf_size);

    if (NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list");

    if (H5FD__direct_populate_config(boundary, block_size, cbuf_size, &fa) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTSET, FAIL, "can't initialize driver configuration info");

    ret_value = H5P_set_driver(plist, H5FD_DIRECT, &fa, NULL);

done:
    FUNC_LEAVE_API(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:  H5Pget_fapl_direct
 *
 * Purpose:  Returns information about the direct file access property
 *    list though the function arguments.
 *
 * Return:  Success:  Non-negative
 *
 *    Failure:  Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_fapl_direct(hid_t fapl_id, size_t *boundary /*out*/, size_t *block_size /*out*/,
                   size_t *cbuf_size /*out*/)
{
    H5P_genplist_t           *plist; /* Property list pointer */
    const H5FD_direct_fapl_t *fa;
    herr_t                    ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "ixxx", fapl_id, boundary, block_size, cbuf_size);

    if (NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access list");
    if (H5FD_DIRECT != H5P_peek_driver(plist))
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "incorrect VFL driver");
    if (NULL == (fa = H5P_peek_driver_info(plist)))
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "bad VFL driver info");
    if (boundary)
        *boundary = fa->mboundary;
    if (block_size)
        *block_size = fa->fbsize;
    if (cbuf_size)
        *cbuf_size = fa->cbsize;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_fapl_direct() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__direct_populate_config
 *
 * Purpose:    Populates a H5FD_direct_fapl_t structure with the provided
 *             values, supplying defaults where values are not provided.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__direct_populate_config(size_t boundary, size_t block_size, size_t cbuf_size, H5FD_direct_fapl_t *fa_out)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(fa_out);

    memset(fa_out, 0, sizeof(H5FD_direct_fapl_t));

    if (boundary != 0)
        fa_out->mboundary = boundary;
    else
        fa_out->mboundary = MBOUNDARY_DEF;

    if (block_size != 0)
        fa_out->fbsize = block_size;
    else
        fa_out->fbsize = FBSIZE_DEF;

    if (cbuf_size != 0)
        fa_out->cbsize = cbuf_size;
    else
        fa_out->cbsize = CBSIZE_DEF;

    /* Set the default to be true for data alignment */
    fa_out->must_align = true;

    /* Copy buffer size must be a multiple of file block size */
    if (fa_out->cbsize % fa_out->fbsize != 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "copy buffer size must be a multiple of block size");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__direct_populate_config() */

/*-------------------------------------------------------------------------
 * Function:  H5FD__direct_fapl_get
 *
 * Purpose:  Returns a file access property list which indicates how the
 *    specified file is being accessed. The return list could be
 *    used to access another file the same way.
 *
 * Return:  Success:  Ptr to new file access property list with all
 *        members copied from the file struct.
 *
 *    Failure:  NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5FD__direct_fapl_get(H5FD_t *_file)
{
    H5FD_direct_t *file      = (H5FD_direct_t *)_file;
    void          *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Set return value */
    ret_value = H5FD__direct_fapl_copy(&(file->fa));

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__direct_fapl_get() */

/*-------------------------------------------------------------------------
 * Function:  H5FD__direct_fapl_copy
 *
 * Purpose:  Copies the direct-specific file access properties.
 *
 * Return:  Success:  Ptr to a new property list
 *
 *    Failure:  NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5FD__direct_fapl_copy(const void *_old_fa)
{
    const H5FD_direct_fapl_t *old_fa = (const H5FD_direct_fapl_t *)_old_fa;
    H5FD_direct_fapl_t       *new_fa = H5MM_calloc(sizeof(H5FD_direct_fapl_t));

    FUNC_ENTER_PACKAGE_NOERR

    assert(new_fa);

    /* Copy the general information */
    H5MM_memcpy(new_fa, old_fa, sizeof(H5FD_direct_fapl_t));

    FUNC_LEAVE_NOAPI(new_fa)
} /* end H5FD__direct_fapl_copy() */

/*-------------------------------------------------------------------------
 * Function:  H5FD__direct_open
 *
 * Purpose:  Create and/or opens a Unix file for direct I/O as an HDF5 file.
 *
 * Return:  Success:  A pointer to a new file data structure. The
 *        public fields will be initialized by the
 *        caller, which is always H5FD_open().
 *
 *    Failure:  NULL
 *
 *-------------------------------------------------------------------------
 */
static H5FD_t *
H5FD__direct_open(const char *name, unsigned flags, hid_t fapl_id, haddr_t maxaddr)
{
    int                       o_flags;
    int                       fd   = (-1);
    H5FD_direct_t            *file = NULL;
    const H5FD_direct_fapl_t *fa;
    H5FD_direct_fapl_t        default_fa;
#ifdef H5_HAVE_WIN32_API
    HFILE                              filehandle;
    struct _BY_HANDLE_FILE_INFORMATION fileinfo;
#endif
    h5_stat_t       sb;
    H5P_genplist_t *plist; /* Property list */
    void           *buf1, *buf2;
    H5FD_t         *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    /* Sanity check on file offsets */
    assert(sizeof(HDoff_t) >= sizeof(size_t));

    /* Check arguments */
    if (!name || !*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "invalid file name");
    if (0 == maxaddr || HADDR_UNDEF == maxaddr)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, NULL, "bogus maxaddr");
    if (ADDR_OVERFLOW(maxaddr))
        HGOTO_ERROR(H5E_ARGS, H5E_OVERFLOW, NULL, "bogus maxaddr");

    /* Build the open flags */
    o_flags = (H5F_ACC_RDWR & flags) ? O_RDWR : O_RDONLY;
    if (H5F_ACC_TRUNC & flags)
        o_flags |= O_TRUNC;
    if (H5F_ACC_CREAT & flags)
        o_flags |= O_CREAT;
    if (H5F_ACC_EXCL & flags)
        o_flags |= O_EXCL;

    /* Flag for Direct I/O */
    o_flags |= O_DIRECT;

    /* Open the file */
    if ((fd = HDopen(name, o_flags, H5_POSIX_CREATE_MODE_RW)) < 0)
        HSYS_GOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, NULL, "unable to open file")

    if (HDfstat(fd, &sb) < 0)
        HSYS_GOTO_ERROR(H5E_FILE, H5E_BADFILE, NULL, "unable to fstat file")

    /* Create the new file struct */
    if (NULL == (file = H5FL_CALLOC(H5FD_direct_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "unable to allocate file struct");

    /* Get the driver specific information */
    if (NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a file access property list");
    if (NULL == (fa = (const H5FD_direct_fapl_t *)H5P_peek_driver_info(plist))) {
        if (H5FD__direct_populate_config(0, 0, 0, &default_fa) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTSET, NULL, "can't initialize driver configuration info");
        fa = &default_fa;
    }

    file->fd = fd;
    H5_CHECKED_ASSIGN(file->eof, haddr_t, sb.st_size, h5_stat_size_t);
    file->pos = HADDR_UNDEF;
    file->op  = OP_UNKNOWN;
#ifdef H5_HAVE_WIN32_API
    filehandle = _get_osfhandle(fd);
    (void)GetFileInformationByHandle((HANDLE)filehandle, &fileinfo);
    file->fileindexhi = fileinfo.nFileIndexHigh;
    file->fileindexlo = fileinfo.nFileIndexLow;
#else
    file->device = sb.st_dev;
    file->inode  = sb.st_ino;
#endif /*H5_HAVE_WIN32_API*/
    file->fa.mboundary = fa->mboundary;
    file->fa.fbsize    = fa->fbsize;
    file->fa.cbsize    = fa->cbsize;

    /* Check the file locking flags in the fapl */
    if (ignore_disabled_file_locks_s != FAIL)
        /* The environment variable was set, so use that preferentially */
        file->ignore_disabled_file_locks = ignore_disabled_file_locks_s;
    else {
        /* Use the value in the property list */
        if (H5P_get(plist, H5F_ACS_IGNORE_DISABLED_FILE_LOCKS_NAME, &file->ignore_disabled_file_locks) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTGET, NULL, "can't get ignore disabled file locks property");
    }

    /* Try to decide if data alignment is required.  The reason to check it here
     * is to handle correctly the case that the file is in a different file system
     * than the one where the program is running.
     */
    /* NOTE: Use malloc and free here to ensure compatibility with
     *       posix_memalign().
     */
    buf1 = malloc(sizeof(int));
    if (posix_memalign(&buf2, file->fa.mboundary, file->fa.fbsize) != 0)
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "posix_memalign failed");

    if (o_flags & O_CREAT) {
        if (HDwrite(file->fd, buf1, sizeof(int)) < 0) {
            if (HDwrite(file->fd, buf2, file->fa.fbsize) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_WRITEERROR, NULL, "file system may not support Direct I/O");
            else
                file->fa.must_align = true;
        }
        else {
            file->fa.must_align = false;
            if (-1 == HDftruncate(file->fd, (HDoff_t)0))
                HSYS_GOTO_ERROR(H5E_IO, H5E_SEEKERROR, NULL, "unable to truncate file")
        }
    }
    else {
        if (HDread(file->fd, buf1, sizeof(int)) < 0) {
            if (HDread(file->fd, buf2, file->fa.fbsize) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_READERROR, NULL, "file system may not support Direct I/O");
            else
                file->fa.must_align = true;
        }
        else {
            if (o_flags & O_RDWR) {
                if (HDlseek(file->fd, (HDoff_t)0, SEEK_SET) < 0)
                    HSYS_GOTO_ERROR(H5E_IO, H5E_SEEKERROR, NULL, "unable to seek to proper position")
                if (HDwrite(file->fd, buf1, sizeof(int)) < 0)
                    file->fa.must_align = true;
                else
                    file->fa.must_align = false;
            }
            else
                file->fa.must_align = false;
        }
    }

    if (buf1)
        free(buf1);
    if (buf2)
        free(buf2);

    /* Set return value */
    ret_value = (H5FD_t *)file;

done:
    if (ret_value == NULL) {
        if (fd >= 0)
            HDclose(fd);
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:  H5FD__direct_close
 *
 * Purpose:  Closes the file.
 *
 * Return:  Success:  0
 *
 *    Failure:  -1, file not closed.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__direct_close(H5FD_t *_file)
{
    H5FD_direct_t *file      = (H5FD_direct_t *)_file;
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    if (HDclose(file->fd) < 0)
        HSYS_GOTO_ERROR(H5E_IO, H5E_CANTCLOSEFILE, FAIL, "unable to close file")

    H5FL_FREE(H5FD_direct_t, file);

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:  H5FD__direct_cmp
 *
 * Purpose:  Compares two files belonging to this driver using an
 *    arbitrary (but consistent) ordering.
 *
 * Return:  Success:  A value like strcmp()
 *
 *    Failure:  never fails (arguments were checked by the
 *        caller).
 *
 *-------------------------------------------------------------------------
 */
static int
H5FD__direct_cmp(const H5FD_t *_f1, const H5FD_t *_f2)
{
    const H5FD_direct_t *f1        = (const H5FD_direct_t *)_f1;
    const H5FD_direct_t *f2        = (const H5FD_direct_t *)_f2;
    int                  ret_value = 0;

    FUNC_ENTER_PACKAGE_NOERR

#ifdef H5_HAVE_WIN32_API
    if (f1->fileindexhi < f2->fileindexhi)
        HGOTO_DONE(-1);
    if (f1->fileindexhi > f2->fileindexhi)
        HGOTO_DONE(1);

    if (f1->fileindexlo < f2->fileindexlo)
        HGOTO_DONE(-1);
    if (f1->fileindexlo > f2->fileindexlo)
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

#endif

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:  H5FD__direct_query
 *
 * Purpose:  Set the flags that this VFL driver is capable of supporting.
 *              (listed in H5FDpublic.h)
 *
 * Return:  Success:  non-negative
 *
 *    Failure:  negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__direct_query(const H5FD_t H5_ATTR_UNUSED *_f, unsigned long *flags /* out */)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Set the VFL feature flags that this driver supports */
    if (flags) {
        *flags = 0;
        *flags |= H5FD_FEAT_AGGREGATE_METADATA;  /* OK to aggregate metadata allocations  */
        *flags |= H5FD_FEAT_ACCUMULATE_METADATA; /* OK to accumulate metadata for faster writes */
        *flags |= H5FD_FEAT_DATA_SIEVE; /* OK to perform data sieving for faster raw data reads & writes    */
        *flags |= H5FD_FEAT_AGGREGATE_SMALLDATA;    /* OK to aggregate "small" raw data allocations    */
        *flags |= H5FD_FEAT_DEFAULT_VFD_COMPATIBLE; /* VFD creates a file which can be opened with the default
                                                       VFD      */
    }

    FUNC_LEAVE_NOAPI(SUCCEED)
}

/*-------------------------------------------------------------------------
 * Function:  H5FD__direct_get_eoa
 *
 * Purpose:  Gets the end-of-address marker for the file. The EOA marker
 *    is the first address past the last byte allocated in the
 *    format address space.
 *
 * Return:  Success:  The end-of-address marker.
 *
 *    Failure:  HADDR_UNDEF
 *
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD__direct_get_eoa(const H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type)
{
    const H5FD_direct_t *file = (const H5FD_direct_t *)_file;

    FUNC_ENTER_PACKAGE_NOERR

    FUNC_LEAVE_NOAPI(file->eoa)
}

/*-------------------------------------------------------------------------
 * Function:  H5FD__direct_set_eoa
 *
 * Purpose:  Set the end-of-address marker for the file. This function is
 *    called shortly after an existing HDF5 file is opened in order
 *    to tell the driver where the end of the HDF5 data is located.
 *
 * Return:  Success:  0
 *
 *    Failure:  -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__direct_set_eoa(H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type, haddr_t addr)
{
    H5FD_direct_t *file = (H5FD_direct_t *)_file;

    FUNC_ENTER_PACKAGE_NOERR

    file->eoa = addr;

    FUNC_LEAVE_NOAPI(SUCCEED)
}

/*-------------------------------------------------------------------------
 * Function:  H5FD__direct_get_eof
 *
 * Purpose:  Returns the end-of-file marker, which is the greater of
 *    either the Unix end-of-file or the HDF5 end-of-address
 *    markers.
 *
 * Return:  Success:  End of file address, the first address past
 *        the end of the "file", either the Unix file
 *        or the HDF5 file.
 *
 *    Failure:  HADDR_UNDEF
 *
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD__direct_get_eof(const H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type)
{
    const H5FD_direct_t *file = (const H5FD_direct_t *)_file;

    FUNC_ENTER_PACKAGE_NOERR

    FUNC_LEAVE_NOAPI(file->eof)
}

/*-------------------------------------------------------------------------
 * Function:       H5FD_diect_get_handle
 *
 * Purpose:        Returns the file handle of direct file driver.
 *
 * Returns:        Non-negative if succeed or negative if fails.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__direct_get_handle(H5FD_t *_file, hid_t H5_ATTR_UNUSED fapl, void **file_handle)
{
    H5FD_direct_t *file      = (H5FD_direct_t *)_file;
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    if (!file_handle)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "file handle not valid");
    *file_handle = &(file->fd);

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:  H5FD__direct_read
 *
 * Purpose:  Reads SIZE bytes of data from FILE beginning at address ADDR
 *    into buffer BUF according to data transfer properties in
 *    DXPL_ID.
 *
 * Return:  Success:  Zero. Result is stored in caller-supplied
 *        buffer BUF.
 *
 *    Failure:  -1, Contents of buffer BUF are undefined.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__direct_read(H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type, hid_t H5_ATTR_UNUSED dxpl_id, haddr_t addr,
                  size_t size, void *buf /*out*/)
{
    H5FD_direct_t *file = (H5FD_direct_t *)_file;
    ssize_t        nbytes;
    bool           _must_align = true;
    herr_t         ret_value   = SUCCEED; /* Return value */
    size_t         alloc_size;
    void          *copy_buf = NULL, *p2;
    size_t         _boundary;
    size_t         _fbsize;
    size_t         _cbsize;
    haddr_t        read_size;        /* Size to read into copy buffer */
    size_t         copy_size = size; /* Size remaining to read when using copy buffer */
    size_t         copy_offset;      /* Offset into copy buffer of the requested data */

    FUNC_ENTER_PACKAGE

    assert(file && file->pub.cls);
    assert(buf);

    /* Check for overflow conditions */
    if (HADDR_UNDEF == addr)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "addr undefined");
    if (REGION_OVERFLOW(addr, size))
        HGOTO_ERROR(H5E_ARGS, H5E_OVERFLOW, FAIL, "addr overflow");

    /* If the system doesn't require data to be aligned, read the data in
     * the same way as sec2 driver.
     */
    _must_align = file->fa.must_align;

    /* Get the memory boundary for alignment, file system block size, and maximal
     * copy buffer size.
     */
    _boundary = file->fa.mboundary;
    _fbsize   = file->fa.fbsize;
    _cbsize   = file->fa.cbsize;

    /* if the data is aligned or the system doesn't require data to be aligned,
     * read it directly from the file.  If not, read a bigger
     * and aligned data first, then copy the data into memory buffer.
     */
    if (!_must_align || ((addr % _fbsize == 0) && (size % _fbsize == 0) && ((size_t)buf % _boundary == 0))) {
        /* Seek to the correct location */
        if ((addr != file->pos || OP_READ != file->op) && HDlseek(file->fd, (HDoff_t)addr, SEEK_SET) < 0)
            HSYS_GOTO_ERROR(H5E_IO, H5E_SEEKERROR, FAIL, "unable to seek to proper position")
        /* Read the aligned data in file first, being careful of interrupted
         * system calls and partial results. */
        while (size > 0) {
            do {
                nbytes = HDread(file->fd, buf, size);
            } while (-1 == nbytes && EINTR == errno);
            if (-1 == nbytes) /* error */
                HSYS_GOTO_ERROR(H5E_IO, H5E_READERROR, FAIL, "file read failed")
            if (0 == nbytes) {
                /* end of file but not end of format address space */
                memset(buf, 0, size);
                break;
            }
            assert(nbytes >= 0);
            assert((size_t)nbytes <= size);
            H5_CHECK_OVERFLOW(nbytes, ssize_t, size_t);
            size -= (size_t)nbytes;
            H5_CHECK_OVERFLOW(nbytes, ssize_t, haddr_t);
            addr += (haddr_t)nbytes;
            buf = (char *)buf + nbytes;
        }
    }
    else {
        /* Calculate where we will begin copying from the copy buffer */
        copy_offset = (size_t)(addr % _fbsize);

        /* allocate memory needed for the Direct IO option up to the maximal
         * copy buffer size. Make a bigger buffer for aligned I/O if size is
         * smaller than maximal copy buffer. */
        alloc_size = ((copy_offset + size - 1) / _fbsize + 1) * _fbsize;
        if (alloc_size > _cbsize)
            alloc_size = _cbsize;
        assert(!(alloc_size % _fbsize));
        if (posix_memalign(&copy_buf, _boundary, alloc_size) != 0)
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "posix_memalign failed");

        /* look for the aligned position for reading the data */
        assert(!(((addr / _fbsize) * _fbsize) % _fbsize));
        if (HDlseek(file->fd, (HDoff_t)((addr / _fbsize) * _fbsize), SEEK_SET) < 0)
            HSYS_GOTO_ERROR(H5E_IO, H5E_SEEKERROR, FAIL, "unable to seek to proper position")

        /*
         * Read the aligned data in file into aligned buffer first, then copy the data
         * into the final buffer.  If the data size is bigger than maximal copy buffer
         * size, do the reading by segment (the outer while loop).  If not, do one step
         * reading.
         */
        do {
            /* Read the aligned data in file first.  Not able to handle interrupted
             * system calls and partial results like sec2 driver does because the
             * data may no longer be aligned. It's especially true when the data in
             * file is smaller than ALLOC_SIZE. */
            memset(copy_buf, 0, alloc_size);

            /* Calculate how much data we have to read in this iteration
             * (including unused parts of blocks) */
            if ((copy_size + copy_offset) < alloc_size)
                read_size = ((copy_size + copy_offset - 1) / _fbsize + 1) * _fbsize;
            else
                read_size = alloc_size;

            assert(!(read_size % _fbsize));
            do {
                nbytes = HDread(file->fd, copy_buf, read_size);
            } while (-1 == nbytes && EINTR == errno);

            if (-1 == nbytes) /* error */
                HSYS_GOTO_ERROR(H5E_IO, H5E_READERROR, FAIL, "file read failed")

            /* Copy the needed data from the copy buffer to the output
             * buffer, and update copy_size.  If the copy buffer does not
             * contain the rest of the data, just copy what's in the copy
             * buffer and also update read_addr and copy_offset to read the
             * next section of data. */
            p2 = (unsigned char *)copy_buf + copy_offset;
            if ((copy_size + copy_offset) <= alloc_size) {
                H5MM_memcpy(buf, p2, copy_size);
                buf       = (unsigned char *)buf + copy_size;
                copy_size = 0;
            } /* end if */
            else {
                H5MM_memcpy(buf, p2, alloc_size - copy_offset);
                buf = (unsigned char *)buf + alloc_size - copy_offset;
                copy_size -= alloc_size - copy_offset;
                copy_offset = 0;
            } /* end else */
        } while (copy_size > 0);

        /*Final step: update address*/
        addr = (haddr_t)(((addr + size - 1) / _fbsize + 1) * _fbsize);

        if (copy_buf) {
            /* Free with free since it came from posix_memalign */
            free(copy_buf);
            copy_buf = NULL;
        } /* end if */
    }

    /* Update current position */
    file->pos = addr;
    file->op  = OP_READ;

done:
    if (ret_value < 0) {
        /* Free with free since it came from posix_memalign */
        if (copy_buf)
            free(copy_buf);

        /* Reset last file I/O information */
        file->pos = HADDR_UNDEF;
        file->op  = OP_UNKNOWN;
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:  H5FD__direct_write
 *
 * Purpose:  Writes SIZE bytes of data to FILE beginning at address ADDR
 *    from buffer BUF according to data transfer properties in
 *    DXPL_ID.
 *
 * Return:  Success:  Zero
 *
 *    Failure:  -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__direct_write(H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type, hid_t H5_ATTR_UNUSED dxpl_id, haddr_t addr,
                   size_t size, const void *buf)
{
    H5FD_direct_t *file = (H5FD_direct_t *)_file;
    ssize_t        nbytes;
    bool           _must_align = true;
    herr_t         ret_value   = SUCCEED; /* Return value */
    size_t         alloc_size;
    void          *copy_buf = NULL, *p1;
    const void    *p3;
    size_t         _boundary;
    size_t         _fbsize;
    size_t         _cbsize;
    haddr_t        write_addr;       /* Address to write copy buffer */
    haddr_t        write_size;       /* Size to write from copy buffer */
    haddr_t        read_size;        /* Size to read into copy buffer */
    size_t         copy_size = size; /* Size remaining to write when using copy buffer */
    size_t         copy_offset;      /* Offset into copy buffer of the data to write */

    FUNC_ENTER_PACKAGE

    assert(file && file->pub.cls);
    assert(buf);

    /* Check for overflow conditions */
    if (HADDR_UNDEF == addr)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "addr undefined");
    if (REGION_OVERFLOW(addr, size))
        HGOTO_ERROR(H5E_ARGS, H5E_OVERFLOW, FAIL, "addr overflow");

    /* If the system doesn't require data to be aligned, read the data in
     * the same way as sec2 driver.
     */
    _must_align = file->fa.must_align;

    /* Get the memory boundary for alignment, file system block size, and maximal
     * copy buffer size.
     */
    _boundary = file->fa.mboundary;
    _fbsize   = file->fa.fbsize;
    _cbsize   = file->fa.cbsize;

    /* if the data is aligned or the system doesn't require data to be aligned,
     * write it directly to the file.  If not, read a bigger and aligned data
     * first, update buffer with user data, then write the data out.
     */
    if (!_must_align || ((addr % _fbsize == 0) && (size % _fbsize == 0) && ((size_t)buf % _boundary == 0))) {
        /* Seek to the correct location */
        if ((addr != file->pos || OP_WRITE != file->op) && HDlseek(file->fd, (HDoff_t)addr, SEEK_SET) < 0)
            HSYS_GOTO_ERROR(H5E_IO, H5E_SEEKERROR, FAIL, "unable to seek to proper position")

        while (size > 0) {
            do {
                nbytes = HDwrite(file->fd, buf, size);
            } while (-1 == nbytes && EINTR == errno);
            if (-1 == nbytes) /* error */
                HSYS_GOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "file write failed")
            assert(nbytes > 0);
            assert((size_t)nbytes <= size);
            H5_CHECK_OVERFLOW(nbytes, ssize_t, size_t);
            size -= (size_t)nbytes;
            H5_CHECK_OVERFLOW(nbytes, ssize_t, haddr_t);
            addr += (haddr_t)nbytes;
            buf = (const char *)buf + nbytes;
        }
    }
    else {
        /* Calculate where we will begin reading from (on disk) and where we
         * will begin copying from the copy buffer */
        write_addr  = (addr / _fbsize) * _fbsize;
        copy_offset = (size_t)(addr % _fbsize);

        /* allocate memory needed for the Direct IO option up to the maximal
         * copy buffer size. Make a bigger buffer for aligned I/O if size is
         * smaller than maximal copy buffer.
         */
        alloc_size = ((copy_offset + size - 1) / _fbsize + 1) * _fbsize;
        if (alloc_size > _cbsize)
            alloc_size = _cbsize;
        assert(!(alloc_size % _fbsize));

        if (posix_memalign(&copy_buf, _boundary, alloc_size) != 0)
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "posix_memalign failed");

        /* look for the right position for reading or writing the data */
        if (HDlseek(file->fd, (HDoff_t)write_addr, SEEK_SET) < 0)
            HSYS_GOTO_ERROR(H5E_IO, H5E_SEEKERROR, FAIL, "unable to seek to proper position")

        p3 = buf;
        do {
            /* Calculate how much data we have to write in this iteration
             * (including unused parts of blocks) */
            if ((copy_size + copy_offset) < alloc_size)
                write_size = ((copy_size + copy_offset - 1) / _fbsize + 1) * _fbsize;
            else
                write_size = alloc_size;

            /*
             * Read the aligned data first if the aligned region doesn't fall
             * entirely in the range to be written.  Not able to handle interrupted
             * system calls and partial results like sec2 driver does because the
             * data may no longer be aligned. It's especially true when the data in
             * file is smaller than ALLOC_SIZE.  Only read the entire section if
             * both ends are misaligned, otherwise only read the block on the
             * misaligned end.
             */
            memset(copy_buf, 0, _fbsize);

            if (copy_offset > 0) {
                if ((write_addr + write_size) > (addr + size)) {
                    assert((write_addr + write_size) - (addr + size) < _fbsize);
                    read_size = write_size;
                    p1        = copy_buf;
                } /* end if */
                else {
                    read_size = _fbsize;
                    p1        = copy_buf;
                } /* end else */
            }     /* end if */
            else if ((write_addr + write_size) > (addr + size)) {
                assert((write_addr + write_size) - (addr + size) < _fbsize);
                read_size = _fbsize;
                p1        = (unsigned char *)copy_buf + write_size - _fbsize;

                /* Seek to the last block, for reading */
                assert(!((write_addr + write_size - _fbsize) % _fbsize));
                if (HDlseek(file->fd, (HDoff_t)(write_addr + write_size - _fbsize), SEEK_SET) < 0)
                    HSYS_GOTO_ERROR(H5E_IO, H5E_SEEKERROR, FAIL, "unable to seek to proper position")
            } /* end if */
            else
                p1 = NULL;

            if (p1) {
                assert(!(read_size % _fbsize));
                do {
                    nbytes = HDread(file->fd, p1, read_size);
                } while (-1 == nbytes && EINTR == errno);

                if (-1 == nbytes) /* error */
                    HSYS_GOTO_ERROR(H5E_IO, H5E_READERROR, FAIL, "file read failed")
            } /* end if */

            /* look for the right position and append or copy the data to be written to
             * the aligned buffer.
             * Consider all possible situations here: file address is not aligned on
             * file block size; the end of data address is not aligned; the end of data
             * address is aligned; data size is smaller or bigger than maximal copy size.
             */
            p1 = (unsigned char *)copy_buf + copy_offset;
            if ((copy_size + copy_offset) <= alloc_size) {
                H5MM_memcpy(p1, p3, copy_size);
                copy_size = 0;
            } /* end if */
            else {
                H5MM_memcpy(p1, p3, alloc_size - copy_offset);
                p3 = (const unsigned char *)p3 + (alloc_size - copy_offset);
                copy_size -= alloc_size - copy_offset;
                copy_offset = 0;
            } /* end else */

            /*look for the aligned position for writing the data*/
            assert(!(write_addr % _fbsize));
            if (HDlseek(file->fd, (HDoff_t)write_addr, SEEK_SET) < 0)
                HSYS_GOTO_ERROR(H5E_IO, H5E_SEEKERROR, FAIL, "unable to seek to proper position")

            /*
             * Write the data. It doesn't truncate the extra data introduced by
             * alignment because that step is done in H5FD_direct_flush.
             */
            assert(!(write_size % _fbsize));
            do {
                nbytes = HDwrite(file->fd, copy_buf, write_size);
            } while (-1 == nbytes && EINTR == errno);

            if (-1 == nbytes) /* error */
                HSYS_GOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "file write failed")

            /* update the write address */
            write_addr += write_size;
        } while (copy_size > 0);

        /*Update the address and size*/
        addr = write_addr;
        buf  = (const char *)buf + size;

        if (copy_buf) {
            /* Free with free since it came from posix_memalign */
            free(copy_buf);
            copy_buf = NULL;
        } /* end if */
    }

    /* Update current position and eof */
    file->pos = addr;
    file->op  = OP_WRITE;
    if (file->pos > file->eof)
        file->eof = file->pos;

done:
    if (ret_value < 0) {
        /* Free with free since it came from posix_memalign */
        if (copy_buf)
            free(copy_buf);

        /* Reset last file I/O information */
        file->pos = HADDR_UNDEF;
        file->op  = OP_UNKNOWN;
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:  H5FD__direct_truncate
 *
 * Purpose:  Makes sure that the true file size is the same (or larger)
 *    than the end-of-address.
 *
 * Return:  Success:  Non-negative
 *
 *    Failure:  Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__direct_truncate(H5FD_t *_file, hid_t H5_ATTR_UNUSED dxpl_id, bool H5_ATTR_UNUSED closing)
{
    H5FD_direct_t *file      = (H5FD_direct_t *)_file;
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(file);

    /* Extend the file to make sure it's large enough */
    if (file->eoa != file->eof) {
#ifdef H5_HAVE_WIN32_API
        HFILE         filehandle; /* Windows file handle */
        LARGE_INTEGER li;         /* 64-bit integer for SetFilePointer() call */

        /* Map the posix file handle to a Windows file handle */
        filehandle = _get_osfhandle(file->fd);

        /* Translate 64-bit integers into form Windows wants */
        /* [This algorithm is from the Windows documentation for SetFilePointer()] */
        li.QuadPart = (LONGLONG)file->eoa;
        (void)SetFilePointer((HANDLE)filehandle, li.LowPart, &li.HighPart, FILE_BEGIN);
        if (SetEndOfFile((HANDLE)filehandle) == 0)
            HGOTO_ERROR(H5E_IO, H5E_SEEKERROR, FAIL, "unable to extend file properly");
#else  /* H5_HAVE_WIN32_API */
        if (-1 == HDftruncate(file->fd, (HDoff_t)file->eoa))
            HSYS_GOTO_ERROR(H5E_IO, H5E_SEEKERROR, FAIL, "unable to extend file properly")
#endif /* H5_HAVE_WIN32_API */

        /* Update the eof value */
        file->eof = file->eoa;

        /* Reset last file I/O information */
        file->pos = HADDR_UNDEF;
        file->op  = OP_UNKNOWN;
    }
    else if (file->fa.must_align) {
        /*Even though eof is equal to eoa, file is still truncated because Direct I/O
         *write introduces some extra data for alignment.
         */
        if (-1 == HDftruncate(file->fd, (HDoff_t)file->eof))
            HSYS_GOTO_ERROR(H5E_IO, H5E_SEEKERROR, FAIL, "unable to extend file properly")
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__direct_truncate() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__direct_lock
 *
 * Purpose:     To place an advisory lock on a file.
 *		The lock type to apply depends on the parameter "rw":
 *			true--opens for write: an exclusive lock
 *			false--opens for read: a shared lock
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__direct_lock(H5FD_t *_file, bool rw)
{
    H5FD_direct_t *file = (H5FD_direct_t *)_file; /* VFD file struct      */
    int            lock_flags;                    /* file locking flags   */
    herr_t         ret_value = SUCCEED;           /* Return value         */

    FUNC_ENTER_PACKAGE

    assert(file);

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
            HSYS_GOTO_ERROR(H5E_VFL, H5E_CANTLOCKFILE, FAIL, "unable to lock file")
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__direct_lock() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__direct_unlock
 *
 * Purpose:     To remove the existing lock on the file
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__direct_unlock(H5FD_t *_file)
{
    H5FD_direct_t *file      = (H5FD_direct_t *)_file; /* VFD file struct */
    herr_t         ret_value = SUCCEED;                /* Return value */

    FUNC_ENTER_PACKAGE

    assert(file);

    if (HDflock(file->fd, LOCK_UN) < 0) {
        if (file->ignore_disabled_file_locks && ENOSYS == errno) {
            /* When errno is set to ENOSYS, the file system does not support
             * locking, so ignore it.
             */
            errno = 0;
        }
        else
            HSYS_GOTO_ERROR(H5E_VFL, H5E_CANTUNLOCKFILE, FAIL, "unable to unlock file")
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__direct_unlock() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__direct_delete
 *
 * Purpose:     Delete a file
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__direct_delete(const char *filename, hid_t H5_ATTR_UNUSED fapl_id)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(filename);

    if (HDremove(filename) < 0)
        HSYS_GOTO_ERROR(H5E_VFL, H5E_CANTDELETEFILE, FAIL, "unable to delete file")

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__direct_delete() */

#endif /* H5_HAVE_DIRECT */
