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
 * Purpose: The POSIX unbuffered file driver using only the HDF5 public
 *          API and with a few optimizations: the lseek() call is made
 *          only when the current file position is unknown or needs to be
 *          changed based on previous I/O through this driver (don't mix
 *          I/O from this driver with I/O from other parts of the
 *          application to the same file).
 */

#include "H5FDdrvr_module.h" /* This source code file is part of the H5FD driver module */

#include "H5private.h"   /* Generic Functions        */
#include "H5Eprivate.h"  /* Error handling           */
#include "H5Fprivate.h"  /* File access              */
#include "H5FDprivate.h" /* File drivers             */
#include "H5FDsec2.h"    /* Sec2 file driver         */
#include "H5FLprivate.h" /* Free Lists               */
#include "H5Iprivate.h"  /* IDs                      */
#include "H5MMprivate.h" /* Memory management        */
#include "H5Pprivate.h"  /* Property lists           */

/* The driver identification number, initialized at runtime */
static hid_t H5FD_SEC2_g = 0;

/* Whether to ignore file locks when disabled (env var value) */
static htri_t ignore_disabled_file_locks_s = FAIL;

/* The description of a file belonging to this driver. The 'eoa' and 'eof'
 * determine the amount of hdf5 address space in use and the high-water mark
 * of the file (the current size of the underlying filesystem file). The
 * 'pos' value is used to eliminate file position updates when they would be a
 * no-op. Unfortunately we've found systems that use separate file position
 * indicators for reading and writing so the lseek can only be eliminated if
 * the current operation is the same as the previous operation.  When opening
 * a file the 'eof' will be set to the current file size, `eoa' will be set
 * to zero, 'pos' will be set to H5F_ADDR_UNDEF (as it is when an error
 * occurs), and 'op' will be set to H5F_OP_UNKNOWN.
 */
typedef struct H5FD_sec2_t {
    H5FD_t         pub; /* public stuff, must be first      */
    int            fd;  /* the filesystem file descriptor   */
    haddr_t        eoa; /* end of allocated region          */
    haddr_t        eof; /* end of file; current file size   */
    haddr_t        pos; /* current file I/O position        */
    H5FD_file_op_t op;  /* last operation                   */
    bool           ignore_disabled_file_locks;
    char           filename[H5FD_MAX_FILENAME_LEN]; /* Copy of file name from open operation */
#ifndef H5_HAVE_WIN32_API
    /* On most systems the combination of device and i-node number uniquely
     * identify a file.  Note that Cygwin, MinGW and other Windows POSIX
     * environments have the stat function (which fakes inodes)
     * and will use the 'device + inodes' scheme as opposed to the
     * Windows code further below.
     */
    dev_t device; /* file device number   */
    ino_t inode;  /* file i-node number   */
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
#endif /* H5_HAVE_WIN32_API */

    /* Information from properties set by 'h5repart' tool
     *
     * Whether to eliminate the family driver info and convert this file to
     * a single file.
     */
    bool fam_to_single;
} H5FD_sec2_t;

/*
 * These macros check for overflow of various quantities.  These macros
 * assume that HDoff_t is signed and haddr_t and size_t are unsigned.
 *
 * ADDR_OVERFLOW:   Checks whether a file address of type `haddr_t'
 *                  is too large to be represented by the second argument
 *                  of the file seek function.
 *
 * SIZE_OVERFLOW:   Checks whether a buffer size of type `hsize_t' is too
 *                  large to be represented by the `size_t' type.
 *
 * REGION_OVERFLOW: Checks whether an address and size pair describe data
 *                  which can be addressed entirely by the second
 *                  argument of the file seek function.
 */
#define MAXADDR          (((haddr_t)1 << (8 * sizeof(HDoff_t) - 1)) - 1)
#define ADDR_OVERFLOW(A) (HADDR_UNDEF == (A) || ((A) & ~(haddr_t)MAXADDR))
#define SIZE_OVERFLOW(Z) ((Z) & ~(hsize_t)MAXADDR)
#define REGION_OVERFLOW(A, Z)                                                                                \
    (ADDR_OVERFLOW(A) || SIZE_OVERFLOW(Z) || HADDR_UNDEF == (A) + (Z) || (HDoff_t)((A) + (Z)) < (HDoff_t)(A))

/* Prototypes */
static herr_t  H5FD__sec2_term(void);
static H5FD_t *H5FD__sec2_open(const char *name, unsigned flags, hid_t fapl_id, haddr_t maxaddr);
static herr_t  H5FD__sec2_close(H5FD_t *_file);
static int     H5FD__sec2_cmp(const H5FD_t *_f1, const H5FD_t *_f2);
static herr_t  H5FD__sec2_query(const H5FD_t *_f1, unsigned long *flags);
static haddr_t H5FD__sec2_get_eoa(const H5FD_t *_file, H5FD_mem_t type);
static herr_t  H5FD__sec2_set_eoa(H5FD_t *_file, H5FD_mem_t type, haddr_t addr);
static haddr_t H5FD__sec2_get_eof(const H5FD_t *_file, H5FD_mem_t type);
static herr_t  H5FD__sec2_get_handle(H5FD_t *_file, hid_t fapl, void **file_handle);
static herr_t  H5FD__sec2_read(H5FD_t *_file, H5FD_mem_t type, hid_t fapl_id, haddr_t addr, size_t size,
                               void *buf);
static herr_t  H5FD__sec2_write(H5FD_t *_file, H5FD_mem_t type, hid_t fapl_id, haddr_t addr, size_t size,
                                const void *buf);
static herr_t  H5FD__sec2_truncate(H5FD_t *_file, hid_t dxpl_id, bool closing);
static herr_t  H5FD__sec2_lock(H5FD_t *_file, bool rw);
static herr_t  H5FD__sec2_unlock(H5FD_t *_file);
static herr_t  H5FD__sec2_delete(const char *filename, hid_t fapl_id);
static herr_t  H5FD__sec2_ctl(H5FD_t *_file, uint64_t op_code, uint64_t flags, const void *input,
                              void **output);

static const H5FD_class_t H5FD_sec2_g = {
    H5FD_CLASS_VERSION,    /* struct version       */
    H5FD_SEC2_VALUE,       /* value                */
    "sec2",                /* name                 */
    MAXADDR,               /* maxaddr              */
    H5F_CLOSE_WEAK,        /* fc_degree            */
    H5FD__sec2_term,       /* terminate            */
    NULL,                  /* sb_size              */
    NULL,                  /* sb_encode            */
    NULL,                  /* sb_decode            */
    0,                     /* fapl_size            */
    NULL,                  /* fapl_get             */
    NULL,                  /* fapl_copy            */
    NULL,                  /* fapl_free            */
    0,                     /* dxpl_size            */
    NULL,                  /* dxpl_copy            */
    NULL,                  /* dxpl_free            */
    H5FD__sec2_open,       /* open                 */
    H5FD__sec2_close,      /* close                */
    H5FD__sec2_cmp,        /* cmp                  */
    H5FD__sec2_query,      /* query                */
    NULL,                  /* get_type_map         */
    NULL,                  /* alloc                */
    NULL,                  /* free                 */
    H5FD__sec2_get_eoa,    /* get_eoa              */
    H5FD__sec2_set_eoa,    /* set_eoa              */
    H5FD__sec2_get_eof,    /* get_eof              */
    H5FD__sec2_get_handle, /* get_handle           */
    H5FD__sec2_read,       /* read                 */
    H5FD__sec2_write,      /* write                */
    NULL,                  /* read_vector          */
    NULL,                  /* write_vector         */
    NULL,                  /* read_selection       */
    NULL,                  /* write_selection      */
    NULL,                  /* flush                */
    H5FD__sec2_truncate,   /* truncate             */
    H5FD__sec2_lock,       /* lock                 */
    H5FD__sec2_unlock,     /* unlock               */
    H5FD__sec2_delete,     /* del                  */
    H5FD__sec2_ctl,        /* ctl                  */
    H5FD_FLMAP_DICHOTOMY   /* fl_map               */
};

/* Declare a free list to manage the H5FD_sec2_t struct */
H5FL_DEFINE_STATIC(H5FD_sec2_t);

/*-------------------------------------------------------------------------
 * Function:    H5FD_sec2_init
 *
 * Purpose:     Initialize this driver by registering the driver with the
 *              library.
 *
 * Return:      Success:    The driver ID for the sec2 driver
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5FD_sec2_init(void)
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

    if (H5I_VFL != H5I_get_type(H5FD_SEC2_g))
        H5FD_SEC2_g = H5FD_register(&H5FD_sec2_g, sizeof(H5FD_class_t), false);

    /* Set return value */
    ret_value = H5FD_SEC2_g;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_sec2_init() */

/*---------------------------------------------------------------------------
 * Function:    H5FD__sec2_term
 *
 * Purpose:     Shut down the VFD
 *
 * Returns:     SUCCEED (Can't fail)
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5FD__sec2_term(void)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Reset VFL ID */
    H5FD_SEC2_g = 0;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FD__sec2_term() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_fapl_sec2
 *
 * Purpose:     Modify the file access property list to use the H5FD_SEC2
 *              driver defined in this source file.  There are no driver
 *              specific properties.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_fapl_sec2(hid_t fapl_id)
{
    H5P_genplist_t *plist; /* Property list pointer */
    herr_t          ret_value;

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", fapl_id);

    if (NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list");

    ret_value = H5P_set_driver(plist, H5FD_SEC2, NULL, NULL);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_fapl_sec2() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__sec2_open
 *
 * Purpose:     Create and/or opens a file as an HDF5 file.
 *
 * Return:      Success:    A pointer to a new file data structure. The
 *                          public fields will be initialized by the
 *                          caller, which is always H5FD_open().
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static H5FD_t *
H5FD__sec2_open(const char *name, unsigned flags, hid_t fapl_id, haddr_t maxaddr)
{
    H5FD_sec2_t *file = NULL; /* sec2 VFD info            */
    int          fd   = -1;   /* File descriptor          */
    int          o_flags;     /* Flags for open() call    */
#ifdef H5_HAVE_WIN32_API
    struct _BY_HANDLE_FILE_INFORMATION fileinfo;
#endif
    h5_stat_t       sb;
    H5P_genplist_t *plist;            /* Property list pointer */
    H5FD_t         *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check on file offsets */
    HDcompile_assert(sizeof(HDoff_t) >= sizeof(size_t));

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

    /* Open the file */
    if ((fd = HDopen(name, o_flags, H5_POSIX_CREATE_MODE_RW)) < 0) {
        int myerrno = errno;
        HGOTO_ERROR(
            H5E_FILE, H5E_CANTOPENFILE, NULL,
            "unable to open file: name = '%s', errno = %d, error message = '%s', flags = %x, o_flags = %x",
            name, myerrno, strerror(myerrno), flags, (unsigned)o_flags);
    } /* end if */

    if (HDfstat(fd, &sb) < 0)
        HSYS_GOTO_ERROR(H5E_FILE, H5E_BADFILE, NULL, "unable to fstat file")

    /* Create the new file struct */
    if (NULL == (file = H5FL_CALLOC(H5FD_sec2_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "unable to allocate file struct");

    file->fd = fd;
    H5_CHECKED_ASSIGN(file->eof, haddr_t, sb.st_size, h5_stat_size_t);
    file->pos = HADDR_UNDEF;
    file->op  = OP_UNKNOWN;
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

    /* Get the FAPL */
    if (NULL == (plist = (H5P_genplist_t *)H5I_object(fapl_id)))
        HGOTO_ERROR(H5E_VFL, H5E_BADTYPE, NULL, "not a file access property list");

    /* Check the file locking flags in the fapl */
    if (ignore_disabled_file_locks_s != FAIL)
        /* The environment variable was set, so use that preferentially */
        file->ignore_disabled_file_locks = ignore_disabled_file_locks_s;
    else {
        /* Use the value in the property list */
        if (H5P_get(plist, H5F_ACS_IGNORE_DISABLED_FILE_LOCKS_NAME, &file->ignore_disabled_file_locks) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTGET, NULL, "can't get ignore disabled file locks property");
    }

    /* Retain a copy of the name used to open the file, for possible error reporting */
    strncpy(file->filename, name, sizeof(file->filename));
    file->filename[sizeof(file->filename) - 1] = '\0';

    /* Check for non-default FAPL */
    if (H5P_FILE_ACCESS_DEFAULT != fapl_id) {

        /* This step is for h5repart tool only. If user wants to change file driver from
         * family to one that uses single files (sec2, etc.) while using h5repart, this
         * private property should be set so that in the later step, the library can ignore
         * the family driver information saved in the superblock.
         */
        if (H5P_exist_plist(plist, H5F_ACS_FAMILY_TO_SINGLE_NAME) > 0)
            if (H5P_get(plist, H5F_ACS_FAMILY_TO_SINGLE_NAME, &file->fam_to_single) < 0)
                HGOTO_ERROR(H5E_VFL, H5E_CANTGET, NULL, "can't get property of changing family to single");
    } /* end if */

    /* Set return value */
    ret_value = (H5FD_t *)file;

done:
    if (NULL == ret_value) {
        if (fd >= 0)
            HDclose(fd);
        if (file)
            file = H5FL_FREE(H5FD_sec2_t, file);
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__sec2_open() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__sec2_close
 *
 * Purpose:     Closes an HDF5 file.
 *
 * Return:      Success:    SUCCEED
 *              Failure:    FAIL, file not closed.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__sec2_close(H5FD_t *_file)
{
    H5FD_sec2_t *file      = (H5FD_sec2_t *)_file;
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(file);

    /* Close the underlying file */
    if (HDclose(file->fd) < 0)
        HSYS_GOTO_ERROR(H5E_IO, H5E_CANTCLOSEFILE, FAIL, "unable to close file")

    /* Release the file info */
    file = H5FL_FREE(H5FD_sec2_t, file);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__sec2_close() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__sec2_cmp
 *
 * Purpose:     Compares two files belonging to this driver using an
 *              arbitrary (but consistent) ordering.
 *
 * Return:      Success:    A value like strcmp()
 *              Failure:    never fails (arguments were checked by the
 *                          caller).
 *
 *-------------------------------------------------------------------------
 */
static int
H5FD__sec2_cmp(const H5FD_t *_f1, const H5FD_t *_f2)
{
    const H5FD_sec2_t *f1        = (const H5FD_sec2_t *)_f1;
    const H5FD_sec2_t *f2        = (const H5FD_sec2_t *)_f2;
    int                ret_value = 0;

    FUNC_ENTER_PACKAGE_NOERR

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
#else /* H5_HAVE_WIN32_API */
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
#endif /* H5_HAVE_WIN32_API */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__sec2_cmp() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__sec2_query
 *
 * Purpose:     Set the flags that this VFL driver is capable of supporting.
 *              (listed in H5FDpublic.h)
 *
 * Return:      SUCCEED (Can't fail)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__sec2_query(const H5FD_t *_file, unsigned long *flags /* out */)
{
    const H5FD_sec2_t *file = (const H5FD_sec2_t *)_file; /* sec2 VFD info */

    FUNC_ENTER_PACKAGE_NOERR

    /* Set the VFL feature flags that this driver supports */
    /* Notice: the Mirror VFD Writer currently uses only the Sec2 driver as
     * the underlying driver -- as such, the Mirror VFD implementation copies
     * these feature flags as its own. Any modifications made here must be
     * reflected in H5FDmirror.c
     * -- JOS 2020-01-13
     */
    if (flags) {
        *flags = 0;
        *flags |= H5FD_FEAT_AGGREGATE_METADATA;  /* OK to aggregate metadata allocations  */
        *flags |= H5FD_FEAT_ACCUMULATE_METADATA; /* OK to accumulate metadata for faster writes */
        *flags |= H5FD_FEAT_DATA_SIEVE; /* OK to perform data sieving for faster raw data reads & writes    */
        *flags |= H5FD_FEAT_AGGREGATE_SMALLDATA; /* OK to aggregate "small" raw data allocations */
        *flags |= H5FD_FEAT_POSIX_COMPAT_HANDLE; /* get_handle callback returns a POSIX file descriptor */
        *flags |=
            H5FD_FEAT_SUPPORTS_SWMR_IO; /* VFD supports the single-writer/multiple-readers (SWMR) pattern   */
        *flags |= H5FD_FEAT_DEFAULT_VFD_COMPATIBLE; /* VFD creates a file which can be opened with the default
                                                       VFD      */

        /* Check for flags that are set by h5repart */
        if (file && file->fam_to_single)
            *flags |= H5FD_FEAT_IGNORE_DRVRINFO; /* Ignore the driver info when file is opened (which
                                                    eliminates it) */
    }                                            /* end if */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FD__sec2_query() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__sec2_get_eoa
 *
 * Purpose:     Gets the end-of-address marker for the file. The EOA marker
 *              is the first address past the last byte allocated in the
 *              format address space.
 *
 * Return:      The end-of-address marker.
 *
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD__sec2_get_eoa(const H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type)
{
    const H5FD_sec2_t *file = (const H5FD_sec2_t *)_file;

    FUNC_ENTER_PACKAGE_NOERR

    FUNC_LEAVE_NOAPI(file->eoa)
} /* end H5FD__sec2_get_eoa() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__sec2_set_eoa
 *
 * Purpose:     Set the end-of-address marker for the file. This function is
 *              called shortly after an existing HDF5 file is opened in order
 *              to tell the driver where the end of the HDF5 data is located.
 *
 * Return:      SUCCEED (Can't fail)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__sec2_set_eoa(H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type, haddr_t addr)
{
    H5FD_sec2_t *file = (H5FD_sec2_t *)_file;

    FUNC_ENTER_PACKAGE_NOERR

    file->eoa = addr;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FD__sec2_set_eoa() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__sec2_get_eof
 *
 * Purpose:     Returns the end-of-file marker, which is the greater of
 *              either the filesystem end-of-file or the HDF5 end-of-address
 *              markers.
 *
 * Return:      End of file address, the first address past the end of the
 *              "file", either the filesystem file or the HDF5 file.
 *
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD__sec2_get_eof(const H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type)
{
    const H5FD_sec2_t *file = (const H5FD_sec2_t *)_file;

    FUNC_ENTER_PACKAGE_NOERR

    FUNC_LEAVE_NOAPI(file->eof)
} /* end H5FD__sec2_get_eof() */

/*-------------------------------------------------------------------------
 * Function:       H5FD__sec2_get_handle
 *
 * Purpose:        Returns the file handle of sec2 file driver.
 *
 * Returns:        SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__sec2_get_handle(H5FD_t *_file, hid_t H5_ATTR_UNUSED fapl, void **file_handle)
{
    H5FD_sec2_t *file      = (H5FD_sec2_t *)_file;
    herr_t       ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    if (!file_handle)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "file handle not valid");

    *file_handle = &(file->fd);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__sec2_get_handle() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__sec2_read
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
H5FD__sec2_read(H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type, hid_t H5_ATTR_UNUSED dxpl_id, haddr_t addr,
                size_t size, void *buf /*out*/)
{
    H5FD_sec2_t *file      = (H5FD_sec2_t *)_file;
    HDoff_t      offset    = (HDoff_t)addr;
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(file && file->pub.cls);
    assert(buf);

    /* Check for overflow conditions */
    if (!H5_addr_defined(addr))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "addr undefined, addr = %llu", (unsigned long long)addr);
    if (REGION_OVERFLOW(addr, size))
        HGOTO_ERROR(H5E_ARGS, H5E_OVERFLOW, FAIL, "addr overflow, addr = %llu", (unsigned long long)addr);

#ifndef H5_HAVE_PREADWRITE
    /* Seek to the correct location (if we don't have pread) */
    if (addr != file->pos || OP_READ != file->op)
        if (HDlseek(file->fd, (HDoff_t)addr, SEEK_SET) < 0)
            HSYS_GOTO_ERROR(H5E_IO, H5E_SEEKERROR, FAIL, "unable to seek to proper position")
#endif /* H5_HAVE_PREADWRITE */

    /* Read data, being careful of interrupted system calls, partial results,
     * and the end of the file.
     */
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
            bytes_read = HDpread(file->fd, buf, bytes_in, offset);
            if (bytes_read > 0)
                offset += bytes_read;
#else
            bytes_read  = HDread(file->fd, buf, bytes_in);
#endif /* H5_HAVE_PREADWRITE */
        } while (-1 == bytes_read && EINTR == errno);

        if (-1 == bytes_read) { /* error */
            int    myerrno = errno;
            time_t mytime  = HDtime(NULL);

            offset = HDlseek(file->fd, (HDoff_t)0, SEEK_CUR);

            HGOTO_ERROR(H5E_IO, H5E_READERROR, FAIL,
                        "file read failed: time = %s, filename = '%s', file descriptor = %d, errno = %d, "
                        "error message = '%s', buf = %p, total read size = %llu, bytes this sub-read = %llu, "
                        "bytes actually read = %llu, offset = %llu",
                        HDctime(&mytime), file->filename, file->fd, myerrno, strerror(myerrno), buf,
                        (unsigned long long)size, (unsigned long long)bytes_in,
                        (unsigned long long)bytes_read, (unsigned long long)offset);
        } /* end if */

        if (0 == bytes_read) {
            /* end of file but not end of format address space */
            memset(buf, 0, size);
            break;
        } /* end if */

        assert(bytes_read >= 0);
        assert((size_t)bytes_read <= size);

        size -= (size_t)bytes_read;
        addr += (haddr_t)bytes_read;
        buf = (char *)buf + bytes_read;
    } /* end while */

    /* Update current position */
    file->pos = addr;
    file->op  = OP_READ;

done:
    if (ret_value < 0) {
        /* Reset last file I/O information */
        file->pos = HADDR_UNDEF;
        file->op  = OP_UNKNOWN;
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__sec2_read() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__sec2_write
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
H5FD__sec2_write(H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type, hid_t H5_ATTR_UNUSED dxpl_id, haddr_t addr,
                 size_t size, const void *buf)
{
    H5FD_sec2_t *file      = (H5FD_sec2_t *)_file;
    HDoff_t      offset    = (HDoff_t)addr;
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(file && file->pub.cls);
    assert(buf);

    /* Check for overflow conditions */
    if (!H5_addr_defined(addr))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "addr undefined, addr = %llu", (unsigned long long)addr);
    if (REGION_OVERFLOW(addr, size))
        HGOTO_ERROR(H5E_ARGS, H5E_OVERFLOW, FAIL, "addr overflow, addr = %llu, size = %llu",
                    (unsigned long long)addr, (unsigned long long)size);

#ifndef H5_HAVE_PREADWRITE
    /* Seek to the correct location (if we don't have pwrite) */
    if (addr != file->pos || OP_WRITE != file->op)
        if (HDlseek(file->fd, (HDoff_t)addr, SEEK_SET) < 0)
            HSYS_GOTO_ERROR(H5E_IO, H5E_SEEKERROR, FAIL, "unable to seek to proper position")
#endif /* H5_HAVE_PREADWRITE */

    /* Write the data, being careful of interrupted system calls and partial
     * results
     */
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
            bytes_wrote = HDpwrite(file->fd, buf, bytes_in, offset);
            if (bytes_wrote > 0)
                offset += bytes_wrote;
#else
            bytes_wrote = HDwrite(file->fd, buf, bytes_in);
#endif /* H5_HAVE_PREADWRITE */
        } while (-1 == bytes_wrote && EINTR == errno);

        if (-1 == bytes_wrote) { /* error */
            int    myerrno = errno;
            time_t mytime  = HDtime(NULL);

            offset = HDlseek(file->fd, (HDoff_t)0, SEEK_CUR);

            HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL,
                        "file write failed: time = %s, filename = '%s', file descriptor = %d, errno = %d, "
                        "error message = '%s', buf = %p, total write size = %llu, bytes this sub-write = "
                        "%llu, bytes actually written = %llu, offset = %llu",
                        HDctime(&mytime), file->filename, file->fd, myerrno, strerror(myerrno), buf,
                        (unsigned long long)size, (unsigned long long)bytes_in,
                        (unsigned long long)bytes_wrote, (unsigned long long)offset);
        } /* end if */

        assert(bytes_wrote > 0);
        assert((size_t)bytes_wrote <= size);

        size -= (size_t)bytes_wrote;
        addr += (haddr_t)bytes_wrote;
        buf = (const char *)buf + bytes_wrote;
    } /* end while */

    /* Update current position and eof */
    file->pos = addr;
    file->op  = OP_WRITE;
    if (file->pos > file->eof)
        file->eof = file->pos;

done:
    if (ret_value < 0) {
        /* Reset last file I/O information */
        file->pos = HADDR_UNDEF;
        file->op  = OP_UNKNOWN;
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__sec2_write() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__sec2_truncate
 *
 * Purpose:     Makes sure that the true file size is the same (or larger)
 *              than the end-of-address.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__sec2_truncate(H5FD_t *_file, hid_t H5_ATTR_UNUSED dxpl_id, bool H5_ATTR_UNUSED closing)
{
    H5FD_sec2_t *file      = (H5FD_sec2_t *)_file;
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(file);

    /* Extend the file to make sure it's large enough */
    if (!H5_addr_eq(file->eoa, file->eof)) {
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
        if (-1 == HDftruncate(file->fd, (HDoff_t)file->eoa))
            HSYS_GOTO_ERROR(H5E_IO, H5E_SEEKERROR, FAIL, "unable to extend file properly")
#endif /* H5_HAVE_WIN32_API */

        /* Update the eof value */
        file->eof = file->eoa;

        /* Reset last file I/O information */
        file->pos = HADDR_UNDEF;
        file->op  = OP_UNKNOWN;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__sec2_truncate() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__sec2_lock
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
H5FD__sec2_lock(H5FD_t *_file, bool rw)
{
    H5FD_sec2_t *file = (H5FD_sec2_t *)_file; /* VFD file struct          */
    int          lock_flags;                  /* file locking flags       */
    herr_t       ret_value = SUCCEED;         /* Return value             */

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
} /* end H5FD__sec2_lock() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__sec2_unlock
 *
 * Purpose:     To remove the existing lock on the file
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__sec2_unlock(H5FD_t *_file)
{
    H5FD_sec2_t *file      = (H5FD_sec2_t *)_file; /* VFD file struct          */
    herr_t       ret_value = SUCCEED;              /* Return value             */

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
} /* end H5FD__sec2_unlock() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__sec2_delete
 *
 * Purpose:     Delete a file
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__sec2_delete(const char *filename, hid_t H5_ATTR_UNUSED fapl_id)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(filename);

    if (HDremove(filename) < 0)
        HSYS_GOTO_ERROR(H5E_VFL, H5E_CANTDELETEFILE, FAIL, "unable to delete file")

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__sec2_delete() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__sec2_ctl
 *
 * Purpose:     Sec2 VFD version of the ctl callback.
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
 *              At present, no op codes are supported by this VFD.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__sec2_ctl(H5FD_t H5_ATTR_UNUSED *_file, uint64_t H5_ATTR_UNUSED op_code, uint64_t flags,
               const void H5_ATTR_UNUSED *input, void H5_ATTR_UNUSED **output)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* No op codes are understood. */
    if (flags & H5FD_CTL_FAIL_IF_UNKNOWN_FLAG)
        HGOTO_ERROR(H5E_VFL, H5E_FCNTL, FAIL, "unknown op_code and fail if unknown flag is set");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__sec2_ctl() */
