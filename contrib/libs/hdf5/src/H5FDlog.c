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
 * Purpose:     The POSIX unbuffered file driver using only the HDF5 public
 *              API and with a few optimizations: the lseek() call is made
 *              only when the current file position is unknown or needs to be
 *              changed based on previous I/O through this driver (don't mix
 *              I/O from this driver with I/O from other parts of the
 *              application to the same file).
 *              With custom modifications...
 */

#include "H5FDdrvr_module.h" /* This source code file is part of the H5FD driver module */

#include "H5private.h"   /* Generic Functions    */
#include "H5Eprivate.h"  /* Error handling       */
#include "H5Fprivate.h"  /* File access          */
#include "H5FDprivate.h" /* File drivers         */
#include "H5FDlog.h"     /* Logging file driver  */
#include "H5FLprivate.h" /* Free Lists           */
#include "H5Iprivate.h"  /* IDs                  */
#include "H5MMprivate.h" /* Memory management    */
#include "H5Pprivate.h"  /* Property lists       */

/* The driver identification number, initialized at runtime */
static hid_t H5FD_LOG_g = 0;

/* Whether to ignore file locks when disabled (env var value) */
static htri_t ignore_disabled_file_locks_s = FAIL;

/* Driver-specific file access properties */
typedef struct H5FD_log_fapl_t {
    char              *logfile; /* Allocated log file name */
    unsigned long long flags;   /* Flags for logging behavior */
    size_t buf_size; /* Size of buffers for track flavor and number of times each byte is accessed */
} H5FD_log_fapl_t;

/* Define strings for the different file memory types
 * These are defined in the H5F_mem_t enum from H5Fpublic.h
 * Note that H5FD_MEM_NOLIST is not listed here since it has
 * a negative value.
 */
static const char *flavors[] = {
    "H5FD_MEM_DEFAULT", "H5FD_MEM_SUPER", "H5FD_MEM_BTREE", "H5FD_MEM_DRAW",
    "H5FD_MEM_GHEAP",   "H5FD_MEM_LHEAP", "H5FD_MEM_OHDR",
};

/* The description of a file belonging to this driver. The `eoa' and `eof'
 * determine the amount of hdf5 address space in use and the high-water mark
 * of the file (the current size of the underlying filesystem file). The
 * `pos' value is used to eliminate file position updates when they would be a
 * no-op. Unfortunately we've found systems that use separate file position
 * indicators for reading and writing so the lseek can only be eliminated if
 * the current operation is the same as the previous operation.  When opening
 * a file the `eof' will be set to the current file size, `eoa' will be set
 * to zero, `pos' will be set to H5F_ADDR_UNDEF (as it is when an error
 * occurs), and `op' will be set to H5F_OP_UNKNOWN.
 */
typedef struct H5FD_log_t {
    H5FD_t         pub; /* public stuff, must be first      */
    int            fd;  /* the unix file                    */
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
     * a single file
     */
    bool fam_to_single;

    /* Fields for tracking I/O operations */
    unsigned char     *nread;               /* Number of reads from a file location             */
    unsigned char     *nwrite;              /* Number of write to a file location               */
    unsigned char     *flavor;              /* Flavor of information written to file location   */
    unsigned long long total_read_ops;      /* Total number of read operations                  */
    unsigned long long total_write_ops;     /* Total number of write operations                 */
    unsigned long long total_seek_ops;      /* Total number of seek operations                  */
    unsigned long long total_truncate_ops;  /* Total number of truncate operations              */
    double             total_read_time;     /* Total time spent in read operations              */
    double             total_write_time;    /* Total time spent in write operations             */
    double             total_seek_time;     /* Total time spent in seek operations              */
    double             total_truncate_time; /* Total time spent in truncate operations              */
    size_t             iosize;              /* Size of I/O information buffers                  */
    FILE              *logfp;               /* Log file pointer                                 */
    H5FD_log_fapl_t    fa;                  /* Driver-specific file access properties           */
} H5FD_log_t;

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
static herr_t  H5FD__log_term(void);
static void   *H5FD__log_fapl_get(H5FD_t *file);
static void   *H5FD__log_fapl_copy(const void *_old_fa);
static herr_t  H5FD__log_fapl_free(void *_fa);
static H5FD_t *H5FD__log_open(const char *name, unsigned flags, hid_t fapl_id, haddr_t maxaddr);
static herr_t  H5FD__log_close(H5FD_t *_file);
static int     H5FD__log_cmp(const H5FD_t *_f1, const H5FD_t *_f2);
static herr_t  H5FD__log_query(const H5FD_t *_f1, unsigned long *flags);
static haddr_t H5FD__log_alloc(H5FD_t *_file, H5FD_mem_t type, hid_t dxpl_id, hsize_t size);
static herr_t  H5FD__log_free(H5FD_t *_file, H5FD_mem_t type, hid_t dxpl_id, haddr_t addr, hsize_t size);
static haddr_t H5FD__log_get_eoa(const H5FD_t *_file, H5FD_mem_t type);
static herr_t  H5FD__log_set_eoa(H5FD_t *_file, H5FD_mem_t type, haddr_t addr);
static haddr_t H5FD__log_get_eof(const H5FD_t *_file, H5FD_mem_t type);
static herr_t  H5FD__log_get_handle(H5FD_t *_file, hid_t fapl, void **file_handle);
static herr_t  H5FD__log_read(H5FD_t *_file, H5FD_mem_t type, hid_t fapl_id, haddr_t addr, size_t size,
                              void *buf);
static herr_t  H5FD__log_write(H5FD_t *_file, H5FD_mem_t type, hid_t fapl_id, haddr_t addr, size_t size,
                               const void *buf);
static herr_t  H5FD__log_truncate(H5FD_t *_file, hid_t dxpl_id, bool closing);
static herr_t  H5FD__log_lock(H5FD_t *_file, bool rw);
static herr_t  H5FD__log_unlock(H5FD_t *_file);
static herr_t  H5FD__log_delete(const char *filename, hid_t fapl_id);

static const H5FD_class_t H5FD_log_g = {
    H5FD_CLASS_VERSION,      /* struct version      */
    H5FD_LOG_VALUE,          /* value               */
    "log",                   /* name                */
    MAXADDR,                 /* maxaddr             */
    H5F_CLOSE_WEAK,          /* fc_degree           */
    H5FD__log_term,          /* terminate           */
    NULL,                    /* sb_size             */
    NULL,                    /* sb_encode           */
    NULL,                    /* sb_decode           */
    sizeof(H5FD_log_fapl_t), /* fapl_size           */
    H5FD__log_fapl_get,      /* fapl_get            */
    H5FD__log_fapl_copy,     /* fapl_copy           */
    H5FD__log_fapl_free,     /* fapl_free           */
    0,                       /* dxpl_size           */
    NULL,                    /* dxpl_copy           */
    NULL,                    /* dxpl_free           */
    H5FD__log_open,          /* open                */
    H5FD__log_close,         /* close               */
    H5FD__log_cmp,           /* cmp                 */
    H5FD__log_query,         /* query               */
    NULL,                    /* get_type_map        */
    H5FD__log_alloc,         /* alloc               */
    H5FD__log_free,          /* free                */
    H5FD__log_get_eoa,       /* get_eoa             */
    H5FD__log_set_eoa,       /* set_eoa             */
    H5FD__log_get_eof,       /* get_eof             */
    H5FD__log_get_handle,    /* get_handle          */
    H5FD__log_read,          /* read                */
    H5FD__log_write,         /* write               */
    NULL,                    /* read vector         */
    NULL,                    /* write vector        */
    NULL,                    /* read_selection      */
    NULL,                    /* write_selection     */
    NULL,                    /* flush               */
    H5FD__log_truncate,      /* truncate            */
    H5FD__log_lock,          /* lock                */
    H5FD__log_unlock,        /* unlock              */
    H5FD__log_delete,        /* del                 */
    NULL,                    /* ctl                 */
    H5FD_FLMAP_DICHOTOMY     /* fl_map              */
};

/* Default configuration, if none provided */
static const H5FD_log_fapl_t H5FD_log_default_config_g = {NULL, H5FD_LOG_LOC_IO | H5FD_LOG_ALLOC, 4096};

/* Declare a free list to manage the H5FD_log_t struct */
H5FL_DEFINE_STATIC(H5FD_log_t);

/*-------------------------------------------------------------------------
 * Function:    H5FD_log_init
 *
 * Purpose:     Initialize this driver by registering the driver with the
 *              library.
 *
 * Return:      Success:    The driver ID for the log driver
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5FD_log_init(void)
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

    if (H5I_VFL != H5I_get_type(H5FD_LOG_g))
        H5FD_LOG_g = H5FD_register(&H5FD_log_g, sizeof(H5FD_class_t), false);

    /* Set return value */
    ret_value = H5FD_LOG_g;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_log_init() */

/*---------------------------------------------------------------------------
 * Function:    H5FD__log_term
 *
 * Purpose:     Shut down the VFD
 *
 * Returns:     SUCCEED (Can't fail)
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5FD__log_term(void)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Reset VFL ID */
    H5FD_LOG_g = 0;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FD__log_term() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_fapl_log
 *
 * Purpose:     Modify the file access property list to use the H5FD_LOG
 *              driver defined in this source file.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_fapl_log(hid_t fapl_id, const char *logfile, unsigned long long flags, size_t buf_size)
{
    H5FD_log_fapl_t fa;        /* File access property list information */
    H5P_genplist_t *plist;     /* Property list pointer */
    herr_t          ret_value; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "i*sULz", fapl_id, logfile, flags, buf_size);

    /* Do this first, so that we don't try to free a wild pointer if
     * H5P_object_verify() fails.
     */
    memset(&fa, 0, sizeof(H5FD_log_fapl_t));

    /* Check arguments */
    if (NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list");

    /* Duplicate the log file string
     * A little wasteful, since this string will just be copied later, but
     * passing it in as a pointer sets off a chain of impossible-to-resolve
     * const cast warnings.
     */
    if (logfile != NULL && NULL == (fa.logfile = H5MM_xstrdup(logfile)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "unable to copy log file name");

    fa.flags    = flags;
    fa.buf_size = buf_size;
    ret_value   = H5P_set_driver(plist, H5FD_LOG, &fa, NULL);

done:
    if (fa.logfile)
        H5MM_free(fa.logfile);

    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_fapl_log() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__log_fapl_get
 *
 * Purpose:     Returns a file access property list which indicates how the
 *              specified file is being accessed. The return list could be
 *              used to access another file the same way.
 *
 * Return:      Success:    Ptr to new file access property list with all
 *                          members copied from the file struct.
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5FD__log_fapl_get(H5FD_t *_file)
{
    H5FD_log_t *file      = (H5FD_log_t *)_file;
    void       *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Set return value */
    ret_value = H5FD__log_fapl_copy(&(file->fa));

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__log_fapl_get() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__log_fapl_copy
 *
 * Purpose:     Copies the log-specific file access properties.
 *
 * Return:      Success:    Ptr to a new property list
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5FD__log_fapl_copy(const void *_old_fa)
{
    const H5FD_log_fapl_t *old_fa    = (const H5FD_log_fapl_t *)_old_fa;
    H5FD_log_fapl_t       *new_fa    = NULL; /* New FAPL info */
    void                  *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(old_fa);

    /* Allocate the new FAPL info */
    if (NULL == (new_fa = (H5FD_log_fapl_t *)H5MM_calloc(sizeof(H5FD_log_fapl_t))))
        HGOTO_ERROR(H5E_FILE, H5E_CANTALLOC, NULL, "unable to allocate log file FAPL");

    /* Copy the general information */
    H5MM_memcpy(new_fa, old_fa, sizeof(H5FD_log_fapl_t));

    /* Deep copy the log file name */
    if (old_fa->logfile != NULL)
        if (NULL == (new_fa->logfile = H5MM_strdup(old_fa->logfile)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "unable to allocate log file name");

    /* Set return value */
    ret_value = new_fa;

done:
    if (NULL == ret_value)
        if (new_fa) {
            if (new_fa->logfile)
                new_fa->logfile = (char *)H5MM_xfree(new_fa->logfile);
            H5MM_free(new_fa);
        }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__log_fapl_copy() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__log_fapl_free
 *
 * Purpose:     Frees the log-specific file access properties.
 *
 * Return:      SUCCEED (Can't fail)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__log_fapl_free(void *_fa)
{
    H5FD_log_fapl_t *fa = (H5FD_log_fapl_t *)_fa;

    FUNC_ENTER_PACKAGE_NOERR

    /* Free the fapl information */
    if (fa->logfile)
        fa->logfile = (char *)H5MM_xfree(fa->logfile);
    H5MM_xfree(fa);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FD__log_fapl_free() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__log_open
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
H5FD__log_open(const char *name, unsigned flags, hid_t fapl_id, haddr_t maxaddr)
{
    H5FD_log_t            *file = NULL;
    H5P_genplist_t        *plist; /* Property list */
    const H5FD_log_fapl_t *fa;    /* File access property list information */
    H5FD_log_fapl_t        default_fa = H5FD_log_default_config_g;
    int                    fd         = -1; /* File descriptor */
    int                    o_flags;         /* Flags for open() call */
#ifdef H5_HAVE_WIN32_API
    struct _BY_HANDLE_FILE_INFORMATION fileinfo;
#endif
    H5_timer_t open_timer; /* Timer for open() call */
    H5_timer_t stat_timer; /* Timer for stat() call */
    h5_stat_t  sb;
    H5FD_t    *ret_value = NULL; /* Return value */

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

    /* Initialize timers */
    H5_timer_init(&open_timer);
    H5_timer_init(&stat_timer);

    /* Build the open flags */
    o_flags = (H5F_ACC_RDWR & flags) ? O_RDWR : O_RDONLY;
    if (H5F_ACC_TRUNC & flags)
        o_flags |= O_TRUNC;
    if (H5F_ACC_CREAT & flags)
        o_flags |= O_CREAT;
    if (H5F_ACC_EXCL & flags)
        o_flags |= O_EXCL;

    /* Get the driver specific information */
    if (NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a file access property list");
    if (NULL == (fa = (const H5FD_log_fapl_t *)H5P_peek_driver_info(plist))) {
        /* Use default driver configuration*/
        fa = &default_fa;
    }

    /* Start timer for open() call */
    if (fa->flags & H5FD_LOG_TIME_OPEN)
        H5_timer_start(&open_timer);

    /* Open the file */
    if ((fd = HDopen(name, o_flags, H5_POSIX_CREATE_MODE_RW)) < 0) {
        int myerrno = errno;

        HGOTO_ERROR(
            H5E_FILE, H5E_CANTOPENFILE, NULL,
            "unable to open file: name = '%s', errno = %d, error message = '%s', flags = %x, o_flags = %x",
            name, myerrno, strerror(myerrno), flags, (unsigned)o_flags);
    }

    /* Stop timer for open() call */
    if (fa->flags & H5FD_LOG_TIME_OPEN)
        H5_timer_stop(&open_timer);

    /* Start timer for stat() call */
    if (fa->flags & H5FD_LOG_TIME_STAT)
        H5_timer_start(&stat_timer);

    /* Get the file stats */
    if (HDfstat(fd, &sb) < 0)
        HSYS_GOTO_ERROR(H5E_FILE, H5E_BADFILE, NULL, "unable to fstat file")

    /* Stop timer for stat() call */
    if (fa->flags & H5FD_LOG_TIME_STAT)
        H5_timer_stop(&stat_timer);

    /* Create the new file struct */
    if (NULL == (file = H5FL_CALLOC(H5FD_log_t)))
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

    /* Retain a copy of the name used to open the file, for possible error reporting */
    strncpy(file->filename, name, sizeof(file->filename));
    file->filename[sizeof(file->filename) - 1] = '\0';

    /* Get the flags for logging */
    file->fa.flags = fa->flags;
    if (fa->logfile)
        file->fa.logfile = H5MM_strdup(fa->logfile);
    else
        file->fa.logfile = NULL;
    file->fa.buf_size = fa->buf_size;

    /* Check if we are doing any logging at all */
    if (file->fa.flags != 0) {
        /* Allocate buffers for tracking file accesses and data "flavor" */
        file->iosize = fa->buf_size;
        if (file->fa.flags & H5FD_LOG_FILE_READ) {
            file->nread = (unsigned char *)H5MM_calloc(file->iosize);
            assert(file->nread);
        }
        if (file->fa.flags & H5FD_LOG_FILE_WRITE) {
            file->nwrite = (unsigned char *)H5MM_calloc(file->iosize);
            assert(file->nwrite);
        }
        if (file->fa.flags & H5FD_LOG_FLAVOR) {
            file->flavor = (unsigned char *)H5MM_calloc(file->iosize);
            assert(file->flavor);
        }

        /* Set the log file pointer */
        if (fa->logfile)
            file->logfp = fopen(fa->logfile, "w");
        else
            file->logfp = stderr;

        /* Log the timer values */
        if (file->fa.flags & H5FD_LOG_TIME_OPEN) {
            H5_timevals_t open_times; /* Elapsed time for open() call */

            H5_timer_get_times(open_timer, &open_times);
            fprintf(file->logfp, "Open took: (%f s)\n", open_times.elapsed);
        }
        if (file->fa.flags & H5FD_LOG_TIME_STAT) {
            H5_timevals_t stat_times; /* Elapsed time for stat() call */

            H5_timer_get_times(stat_timer, &stat_times);
            fprintf(file->logfp, "Stat took: (%f s)\n", stat_times.elapsed);
        }
    }

    /* Check the file locking flags in the fapl */
    if (ignore_disabled_file_locks_s != FAIL)
        /* The environment variable was set, so use that preferentially */
        file->ignore_disabled_file_locks = ignore_disabled_file_locks_s;
    else {
        /* Use the value in the property list */
        if (H5P_get(plist, H5F_ACS_IGNORE_DISABLED_FILE_LOCKS_NAME, &file->ignore_disabled_file_locks) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTGET, NULL, "can't get ignore disabled file locks property");
    }

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
    }

    /* Set return value */
    ret_value = (H5FD_t *)file;

done:
    if (NULL == ret_value) {
        if (fd >= 0)
            HDclose(fd);
        if (file)
            file = H5FL_FREE(H5FD_log_t, file);
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__log_open() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__log_close
 *
 * Purpose:     Closes an HDF5 file.
 *
 * Return:      Success:    SUCCEED
 *              Failure:    FAIL, file not closed.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__log_close(H5FD_t *_file)
{
    H5FD_log_t *file = (H5FD_log_t *)_file;
    H5_timer_t  close_timer;         /* Timer for close() call */
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(file);

    /* Initialize timer */
    H5_timer_init(&close_timer);

    /* Start timer for close() call */
    if (file->fa.flags & H5FD_LOG_TIME_CLOSE)
        H5_timer_start(&close_timer);

    /* Close the underlying file */
    if (HDclose(file->fd) < 0)
        HSYS_GOTO_ERROR(H5E_IO, H5E_CANTCLOSEFILE, FAIL, "unable to close file")

    /* Stop timer for close() call */
    if (file->fa.flags & H5FD_LOG_TIME_CLOSE)
        H5_timer_stop(&close_timer);

    /* Dump I/O information */
    if (file->fa.flags != 0) {
        haddr_t       addr;
        haddr_t       last_addr;
        unsigned char last_val;

        if (file->fa.flags & H5FD_LOG_TIME_CLOSE) {
            H5_timevals_t close_times; /* Elapsed time for close() call */

            H5_timer_get_times(close_timer, &close_times);
            fprintf(file->logfp, "Close took: (%f s)\n", close_times.elapsed);
        }

        /* Dump the total number of seek/read/write operations */
        if (file->fa.flags & H5FD_LOG_NUM_READ)
            fprintf(file->logfp, "Total number of read operations: %llu\n", file->total_read_ops);
        if (file->fa.flags & H5FD_LOG_NUM_WRITE)
            fprintf(file->logfp, "Total number of write operations: %llu\n", file->total_write_ops);
        if (file->fa.flags & H5FD_LOG_NUM_SEEK)
            fprintf(file->logfp, "Total number of seek operations: %llu\n", file->total_seek_ops);
        if (file->fa.flags & H5FD_LOG_NUM_TRUNCATE)
            fprintf(file->logfp, "Total number of truncate operations: %llu\n", file->total_truncate_ops);

        /* Dump the total time in seek/read/write */
        if (file->fa.flags & H5FD_LOG_TIME_READ)
            fprintf(file->logfp, "Total time in read operations: %f s\n", file->total_read_time);
        if (file->fa.flags & H5FD_LOG_TIME_WRITE)
            fprintf(file->logfp, "Total time in write operations: %f s\n", file->total_write_time);
        if (file->fa.flags & H5FD_LOG_TIME_SEEK)
            fprintf(file->logfp, "Total time in seek operations: %f s\n", file->total_seek_time);
        if (file->fa.flags & H5FD_LOG_TIME_TRUNCATE)
            fprintf(file->logfp, "Total time in truncate operations: %f s\n", file->total_truncate_time);

        /* Dump the write I/O information */
        if (file->fa.flags & H5FD_LOG_FILE_WRITE) {
            fprintf(file->logfp, "Dumping write I/O information:\n");
            last_val  = file->nwrite[0];
            last_addr = 0;
            addr      = 1;
            while (addr < file->eoa) {
                if (file->nwrite[addr] != last_val) {
                    fprintf(file->logfp,
                            "\tAddr %10" PRIuHADDR "-%10" PRIuHADDR " (%10lu bytes) written to %3d times\n",
                            last_addr, (addr - 1), (unsigned long)(addr - last_addr), (int)last_val);
                    last_val  = file->nwrite[addr];
                    last_addr = addr;
                }
                addr++;
            }
            fprintf(file->logfp,
                    "\tAddr %10" PRIuHADDR "-%10" PRIuHADDR " (%10lu bytes) written to %3d times\n",
                    last_addr, (addr - 1), (unsigned long)(addr - last_addr), (int)last_val);
        }

        /* Dump the read I/O information */
        if (file->fa.flags & H5FD_LOG_FILE_READ) {
            fprintf(file->logfp, "Dumping read I/O information:\n");
            last_val  = file->nread[0];
            last_addr = 0;
            addr      = 1;
            while (addr < file->eoa) {
                if (file->nread[addr] != last_val) {
                    fprintf(file->logfp,
                            "\tAddr %10" PRIuHADDR "-%10" PRIuHADDR " (%10lu bytes) read from %3d times\n",
                            last_addr, (addr - 1), (unsigned long)(addr - last_addr), (int)last_val);
                    last_val  = file->nread[addr];
                    last_addr = addr;
                }
                addr++;
            }
            fprintf(file->logfp,
                    "\tAddr %10" PRIuHADDR "-%10" PRIuHADDR " (%10lu bytes) read from %3d times\n", last_addr,
                    (addr - 1), (unsigned long)(addr - last_addr), (int)last_val);
        }

        /* Dump the I/O flavor information */
        if (file->fa.flags & H5FD_LOG_FLAVOR) {
            fprintf(file->logfp, "Dumping I/O flavor information:\n");
            last_val  = file->flavor[0];
            last_addr = 0;
            addr      = 1;
            while (addr < file->eoa) {
                if (file->flavor[addr] != last_val) {
                    fprintf(file->logfp,
                            "\tAddr %10" PRIuHADDR "-%10" PRIuHADDR " (%10lu bytes) flavor is %s\n",
                            last_addr, (addr - 1), (unsigned long)(addr - last_addr), flavors[last_val]);
                    last_val  = file->flavor[addr];
                    last_addr = addr;
                }
                addr++;
            }
            fprintf(file->logfp, "\tAddr %10" PRIuHADDR "-%10" PRIuHADDR " (%10lu bytes) flavor is %s\n",
                    last_addr, (addr - 1), (unsigned long)(addr - last_addr), flavors[last_val]);
        }

        /* Free the logging information */
        if (file->fa.flags & H5FD_LOG_FILE_WRITE)
            file->nwrite = (unsigned char *)H5MM_xfree(file->nwrite);
        if (file->fa.flags & H5FD_LOG_FILE_READ)
            file->nread = (unsigned char *)H5MM_xfree(file->nread);
        if (file->fa.flags & H5FD_LOG_FLAVOR)
            file->flavor = (unsigned char *)H5MM_xfree(file->flavor);
        if (file->logfp != stderr)
            fclose(file->logfp);
    } /* end if */

    if (file->fa.logfile)
        file->fa.logfile = (char *)H5MM_xfree(file->fa.logfile);

    /* Release the file info */
    file = H5FL_FREE(H5FD_log_t, file);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__log_close() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__log_cmp
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
H5FD__log_cmp(const H5FD_t *_f1, const H5FD_t *_f2)
{
    const H5FD_log_t *f1        = (const H5FD_log_t *)_f1;
    const H5FD_log_t *f2        = (const H5FD_log_t *)_f2;
    int               ret_value = 0;

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
} /* end H5FD__log_cmp() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__log_query
 *
 * Purpose:     Set the flags that this VFL driver is capable of supporting.
 *              (listed in H5FDpublic.h)
 *
 * Return:      SUCCEED (Can't fail)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__log_query(const H5FD_t *_file, unsigned long *flags /* out */)
{
    const H5FD_log_t *file = (const H5FD_log_t *)_file;

    FUNC_ENTER_PACKAGE_NOERR

    /* Set the VFL feature flags that this driver supports */
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
    }

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FD__log_query() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__log_alloc
 *
 * Purpose:     Allocate file memory.
 *
 * Return:      Success:    Address of new memory
 *              Failure:    HADDR_UNDEF
 *
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD__log_alloc(H5FD_t *_file, H5FD_mem_t type, hid_t H5_ATTR_UNUSED dxpl_id, hsize_t size)
{
    H5FD_log_t *file = (H5FD_log_t *)_file;
    haddr_t     addr;
    haddr_t     ret_value = HADDR_UNDEF; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Compute the address for the block to allocate */
    addr = file->eoa;

    /* Extend the end-of-allocated space address */
    file->eoa = addr + size;

    /* Retain the (first) flavor of the information written to the file */
    if (file->fa.flags != 0) {
        if (file->fa.flags & H5FD_LOG_FLAVOR) {
            assert(addr < file->iosize);
            H5_CHECK_OVERFLOW(size, hsize_t, size_t);
            memset(&file->flavor[addr], (int)type, (size_t)size);
        }

        if (file->fa.flags & H5FD_LOG_ALLOC)
            fprintf(file->logfp,
                    "%10" PRIuHADDR "-%10" PRIuHADDR " (%10" PRIuHSIZE " bytes) (%s) Allocated\n", addr,
                    (haddr_t)((addr + size) - 1), size, flavors[type]);
    }

    /* Set return value */
    ret_value = addr;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__log_alloc() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__log_free
 *
 * Purpose:     Release file memory.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__log_free(H5FD_t *_file, H5FD_mem_t type, hid_t H5_ATTR_UNUSED dxpl_id, haddr_t addr, hsize_t size)
{
    H5FD_log_t *file = (H5FD_log_t *)_file;

    FUNC_ENTER_PACKAGE_NOERR

    if (file->fa.flags != 0) {
        /* Reset the flavor of the information in the file */
        if (file->fa.flags & H5FD_LOG_FLAVOR) {
            assert(addr < file->iosize);
            H5_CHECK_OVERFLOW(size, hsize_t, size_t);
            memset(&file->flavor[addr], H5FD_MEM_DEFAULT, (size_t)size);
        }

        /* Log the file memory freed */
        if (file->fa.flags & H5FD_LOG_FREE)
            fprintf(file->logfp, "%10" PRIuHADDR "-%10" PRIuHADDR " (%10" PRIuHSIZE " bytes) (%s) Freed\n",
                    addr, (haddr_t)((addr + size) - 1), size, flavors[type]);
    }

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FD__log_free() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__log_get_eoa
 *
 * Purpose:     Gets the end-of-address marker for the file. The EOA marker
 *              is the first address past the last byte allocated in the
 *              format address space.
 *
 * Return:      Success:    The end-of-address marker.
 *              Failure:    HADDR_UNDEF
 *
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD__log_get_eoa(const H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type)
{
    const H5FD_log_t *file = (const H5FD_log_t *)_file;

    FUNC_ENTER_PACKAGE_NOERR

    FUNC_LEAVE_NOAPI(file->eoa)
} /* end H5FD__log_get_eoa() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__log_set_eoa
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
H5FD__log_set_eoa(H5FD_t *_file, H5FD_mem_t type, haddr_t addr)
{
    H5FD_log_t *file = (H5FD_log_t *)_file;

    FUNC_ENTER_PACKAGE_NOERR

    if (file->fa.flags != 0) {
        /* Check for increasing file size */
        if (H5_addr_gt(addr, file->eoa) && H5_addr_gt(addr, 0)) {
            hsize_t size = addr - file->eoa;

            /* Retain the flavor of the space allocated by the extension */
            if (file->fa.flags & H5FD_LOG_FLAVOR) {
                assert(addr < file->iosize);
                H5_CHECK_OVERFLOW(size, hsize_t, size_t);
                memset(&file->flavor[file->eoa], (int)type, (size_t)size);
            }

            /* Log the extension like an allocation */
            if (file->fa.flags & H5FD_LOG_ALLOC)
                fprintf(file->logfp,
                        "%10" PRIuHADDR "-%10" PRIuHADDR " (%10" PRIuHSIZE " bytes) (%s) Allocated\n",
                        file->eoa, addr, size, flavors[type]);
        }

        /* Check for decreasing file size */
        if (H5_addr_lt(addr, file->eoa) && H5_addr_gt(addr, 0)) {
            hsize_t size = file->eoa - addr;

            /* Reset the flavor of the space freed by the shrink */
            if (file->fa.flags & H5FD_LOG_FLAVOR) {
                assert((addr + size) < file->iosize);
                H5_CHECK_OVERFLOW(size, hsize_t, size_t);
                memset(&file->flavor[addr], H5FD_MEM_DEFAULT, (size_t)size);
            }

            /* Log the shrink like a free */
            if (file->fa.flags & H5FD_LOG_FREE)
                fprintf(file->logfp,
                        "%10" PRIuHADDR "-%10" PRIuHADDR " (%10" PRIuHSIZE " bytes) (%s) Freed\n", file->eoa,
                        addr, size, flavors[type]);
        }
    }

    file->eoa = addr;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FD__log_set_eoa() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__log_get_eof
 *
 * Purpose:     Returns the end-of-file marker, which is the greater of
 *              either the filesystem end-of-file or the HDF5 end-of-address
 *              markers.
 *
 * Return:      Success:    End of file address, the first address past
 *                          the end of the "file", either the filesystem file
 *                          or the HDF5 file.
 *              Failure:    HADDR_UNDEF
 *
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD__log_get_eof(const H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type)
{
    const H5FD_log_t *file = (const H5FD_log_t *)_file;

    FUNC_ENTER_PACKAGE_NOERR

    FUNC_LEAVE_NOAPI(file->eof)
} /* end H5FD__log_get_eof() */

/*-------------------------------------------------------------------------
 * Function:       H5FD__log_get_handle
 *
 * Purpose:        Returns the file handle of LOG file driver.
 *
 * Returns:        SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__log_get_handle(H5FD_t *_file, hid_t H5_ATTR_UNUSED fapl, void **file_handle)
{
    H5FD_log_t *file      = (H5FD_log_t *)_file;
    herr_t      ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    if (!file_handle)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "file handle not valid");

    *file_handle = &(file->fd);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__log_get_handle() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__log_read
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
H5FD__log_read(H5FD_t *_file, H5FD_mem_t type, hid_t H5_ATTR_UNUSED dxpl_id, haddr_t addr, size_t size,
               void *buf /*out*/)
{
    H5FD_log_t   *file      = (H5FD_log_t *)_file;
    size_t        orig_size = size; /* Save the original size for later */
    haddr_t       orig_addr = addr;
    H5_timer_t    read_timer; /* Timer for read operation */
    H5_timevals_t read_times; /* Elapsed time for read operation */
    HDoff_t       offset    = (HDoff_t)addr;
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(file && file->pub.cls);
    assert(buf);

    /* Initialize timer */
    H5_timer_init(&read_timer);

    /* Check for overflow conditions */
    if (!H5_addr_defined(addr))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "addr undefined, addr = %llu", (unsigned long long)addr);
    if (REGION_OVERFLOW(addr, size))
        HGOTO_ERROR(H5E_ARGS, H5E_OVERFLOW, FAIL, "addr overflow, addr = %llu", (unsigned long long)addr);

    /* Log the I/O information about the read */
    if (file->fa.flags != 0) {
        size_t  tmp_size = size;
        haddr_t tmp_addr = addr;

        /* Log information about the number of times these locations are read */
        if (file->fa.flags & H5FD_LOG_FILE_READ) {
            assert((addr + size) < file->iosize);
            while (tmp_size-- > 0)
                file->nread[tmp_addr++]++;
        }
    }

#ifndef H5_HAVE_PREADWRITE
    /* Seek to the correct location (if we don't have pread) */
    if (addr != file->pos || OP_READ != file->op) {
        H5_timer_t    seek_timer; /* Timer for seek operation */
        H5_timevals_t seek_times; /* Elapsed time for seek operation */

        /* Initialize timer */
        H5_timer_init(&seek_timer);

        /* Start timer for seek() call */
        if (file->fa.flags & H5FD_LOG_TIME_SEEK)
            H5_timer_start(&seek_timer);

        if (HDlseek(file->fd, (HDoff_t)addr, SEEK_SET) < 0)
            HSYS_GOTO_ERROR(H5E_IO, H5E_SEEKERROR, FAIL, "unable to seek to proper position")

        /* Stop timer for seek() call */
        if (file->fa.flags & H5FD_LOG_TIME_SEEK)
            H5_timer_stop(&seek_timer);

        /* Add to the number of seeks, when tracking that */
        if (file->fa.flags & H5FD_LOG_NUM_SEEK)
            file->total_seek_ops++;

        /* Add to the total seek time, when tracking that */
        if (file->fa.flags & H5FD_LOG_TIME_SEEK) {
            H5_timer_get_times(seek_timer, &seek_times);
            file->total_seek_time += seek_times.elapsed;
        }

        /* Emit log string if we're tracking individual seek events. */
        if (file->fa.flags & H5FD_LOG_LOC_SEEK) {
            fprintf(file->logfp, "Seek: From %10" PRIuHADDR " To %10" PRIuHADDR, file->pos, addr);

            /* Add the seek time, if we're tracking that.
             * Note that the seek time is NOT emitted for when just H5FD_LOG_TIME_SEEK
             * is set.
             */
            if (file->fa.flags & H5FD_LOG_TIME_SEEK)
                fprintf(file->logfp, " (%fs @ %f)\n", seek_times.elapsed, seek_timer.initial.elapsed);
            else
                fprintf(file->logfp, "\n");
        }
    }
#endif /* H5_HAVE_PREADWRITE */

    /* Start timer for read operation */
    if (file->fa.flags & H5FD_LOG_TIME_READ)
        H5_timer_start(&read_timer);

    /*
     * Read data, being careful of interrupted system calls, partial results,
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

            if (file->fa.flags & H5FD_LOG_LOC_READ)
                fprintf(file->logfp, "Error! Reading: %10" PRIuHADDR "-%10" PRIuHADDR " (%10zu bytes)\n",
                        orig_addr, (orig_addr + orig_size) - 1, orig_size);

            HGOTO_ERROR(H5E_IO, H5E_READERROR, FAIL,
                        "file read failed: time = %s, filename = '%s', file descriptor = %d, errno = %d, "
                        "error message = '%s', buf = %p, total read size = %llu, bytes this sub-read = %llu, "
                        "bytes actually read = %llu, offset = %llu",
                        HDctime(&mytime), file->filename, file->fd, myerrno, strerror(myerrno), buf,
                        (unsigned long long)size, (unsigned long long)bytes_in,
                        (unsigned long long)bytes_read, (unsigned long long)offset);
        }

        if (0 == bytes_read) {
            /* End of file but not end of format address space */
            memset(buf, 0, size);
            break;
        }

        assert(bytes_read >= 0);
        assert((size_t)bytes_read <= size);

        size -= (size_t)bytes_read;
        addr += (haddr_t)bytes_read;
        buf = (char *)buf + bytes_read;
    }

    /* Stop timer for read operation */
    if (file->fa.flags & H5FD_LOG_TIME_READ)
        H5_timer_stop(&read_timer);

    /* Add to the number of reads, when tracking that */
    if (file->fa.flags & H5FD_LOG_NUM_READ)
        file->total_read_ops++;

    /* Add to the total read time, when tracking that */
    if (file->fa.flags & H5FD_LOG_TIME_READ) {
        H5_timer_get_times(read_timer, &read_times);
        file->total_read_time += read_times.elapsed;
    }

    /* Log information about the read */
    if (file->fa.flags & H5FD_LOG_LOC_READ) {
        fprintf(file->logfp, "%10" PRIuHADDR "-%10" PRIuHADDR " (%10zu bytes) (%s) Read", orig_addr,
                (orig_addr + orig_size) - 1, orig_size, flavors[type]);

        /* Verify that we are reading in the type of data we allocated in this location */
        if (file->flavor) {
            assert(type == H5FD_MEM_DEFAULT || type == (H5FD_mem_t)file->flavor[orig_addr] ||
                   (H5FD_mem_t)file->flavor[orig_addr] == H5FD_MEM_DEFAULT);
            assert(type == H5FD_MEM_DEFAULT ||
                   type == (H5FD_mem_t)file->flavor[(orig_addr + orig_size) - 1] ||
                   (H5FD_mem_t)file->flavor[(orig_addr + orig_size) - 1] == H5FD_MEM_DEFAULT);
        }

        /* Add the read time, if we're tracking that.
         * Note that the read time is NOT emitted for when just H5FD_LOG_TIME_READ
         * is set.
         */
        if (file->fa.flags & H5FD_LOG_TIME_READ)
            fprintf(file->logfp, " (%fs @ %f)\n", read_times.elapsed, read_timer.initial.elapsed);
        else
            fprintf(file->logfp, "\n");
    }

    /* Update current position */
    file->pos = addr;
    file->op  = OP_READ;

done:
    if (ret_value < 0) {
        /* Reset last file I/O information */
        file->pos = HADDR_UNDEF;
        file->op  = OP_UNKNOWN;
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__log_read() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__log_write
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
H5FD__log_write(H5FD_t *_file, H5FD_mem_t type, hid_t H5_ATTR_UNUSED dxpl_id, haddr_t addr, size_t size,
                const void *buf)
{
    H5FD_log_t   *file      = (H5FD_log_t *)_file;
    size_t        orig_size = size; /* Save the original size for later */
    haddr_t       orig_addr = addr;
    H5_timer_t    write_timer; /* Timer for write operation */
    H5_timevals_t write_times; /* Elapsed time for write operation */
    HDoff_t       offset    = (HDoff_t)addr;
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(file && file->pub.cls);
    assert(size > 0);
    assert(buf);

    /* Initialize timer */
    H5_timer_init(&write_timer);

    /* Verify that we are writing out the type of data we allocated in this location */
    if (file->flavor) {
        assert(type == H5FD_MEM_DEFAULT || type == (H5FD_mem_t)file->flavor[addr] ||
               (H5FD_mem_t)file->flavor[addr] == H5FD_MEM_DEFAULT);
        assert(type == H5FD_MEM_DEFAULT || type == (H5FD_mem_t)file->flavor[(addr + size) - 1] ||
               (H5FD_mem_t)file->flavor[(addr + size) - 1] == H5FD_MEM_DEFAULT);
    }

    /* Check for overflow conditions */
    if (!H5_addr_defined(addr))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "addr undefined, addr = %llu", (unsigned long long)addr);
    if (REGION_OVERFLOW(addr, size))
        HGOTO_ERROR(H5E_ARGS, H5E_OVERFLOW, FAIL, "addr overflow, addr = %llu, size = %llu",
                    (unsigned long long)addr, (unsigned long long)size);

    /* Log the I/O information about the write */
    if (file->fa.flags & H5FD_LOG_FILE_WRITE) {
        size_t  tmp_size = size;
        haddr_t tmp_addr = addr;

        /* Log information about the number of times these locations are read */
        assert((addr + size) < file->iosize);
        while (tmp_size-- > 0)
            file->nwrite[tmp_addr++]++;
    }

#ifndef H5_HAVE_PREADWRITE
    /* Seek to the correct location (if we don't have pwrite) */
    if (addr != file->pos || OP_WRITE != file->op) {
        H5_timer_t    seek_timer; /* Timer for seek operation */
        H5_timevals_t seek_times; /* Elapsed time for seek operation */

        /* Initialize timer */
        H5_timer_init(&seek_timer);

        /* Start timer for seek() call */
        if (file->fa.flags & H5FD_LOG_TIME_SEEK)
            H5_timer_start(&seek_timer);

        if (HDlseek(file->fd, (HDoff_t)addr, SEEK_SET) < 0)
            HSYS_GOTO_ERROR(H5E_IO, H5E_SEEKERROR, FAIL, "unable to seek to proper position")

        /* Stop timer for seek() call */
        if (file->fa.flags & H5FD_LOG_TIME_SEEK)
            H5_timer_stop(&seek_timer);

        /* Add to the number of seeks, when tracking that */
        if (file->fa.flags & H5FD_LOG_NUM_SEEK)
            file->total_seek_ops++;

        /* Add to the total seek time, when tracking that */
        if (file->fa.flags & H5FD_LOG_TIME_SEEK) {
            H5_timer_get_times(seek_timer, &seek_times);
            file->total_seek_time += seek_times.elapsed;
        }

        /* Emit log string if we're tracking individual seek events. */
        if (file->fa.flags & H5FD_LOG_LOC_SEEK) {
            fprintf(file->logfp, "Seek: From %10" PRIuHADDR " To %10" PRIuHADDR, file->pos, addr);

            /* Add the seek time, if we're tracking that.
             * Note that the seek time is NOT emitted for when just H5FD_LOG_TIME_SEEK
             * is set.
             */
            if (file->fa.flags & H5FD_LOG_TIME_SEEK)
                fprintf(file->logfp, " (%fs @ %f)\n", seek_times.elapsed, seek_timer.initial.elapsed);
            else
                fprintf(file->logfp, "\n");
        }
    }
#endif /* H5_HAVE_PREADWRITE */

    /* Start timer for write operation */
    if (file->fa.flags & H5FD_LOG_TIME_WRITE)
        H5_timer_start(&write_timer);

    /*
     * Write the data, being careful of interrupted system calls and partial
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

            if (file->fa.flags & H5FD_LOG_LOC_WRITE)
                fprintf(file->logfp, "Error! Writing: %10" PRIuHADDR "-%10" PRIuHADDR " (%10zu bytes)\n",
                        orig_addr, (orig_addr + orig_size) - 1, orig_size);

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

    /* Stop timer for write operation */
    if (file->fa.flags & H5FD_LOG_TIME_WRITE)
        H5_timer_stop(&write_timer);

    /* Add to the number of writes, when tracking that */
    if (file->fa.flags & H5FD_LOG_NUM_WRITE)
        file->total_write_ops++;

    /* Add to the total write time, when tracking that */
    if (file->fa.flags & H5FD_LOG_TIME_WRITE) {
        H5_timer_get_times(write_timer, &write_times);
        file->total_write_time += write_times.elapsed;
    }

    /* Log information about the write */
    if (file->fa.flags & H5FD_LOG_LOC_WRITE) {
        fprintf(file->logfp, "%10" PRIuHADDR "-%10" PRIuHADDR " (%10zu bytes) (%s) Written", orig_addr,
                (orig_addr + orig_size) - 1, orig_size, flavors[type]);

        /* Check if this is the first write into a "default" section, grabbed by the metadata aggregation
         * algorithm */
        if (file->fa.flags & H5FD_LOG_FLAVOR) {
            if ((H5FD_mem_t)file->flavor[orig_addr] == H5FD_MEM_DEFAULT) {
                memset(&file->flavor[orig_addr], (int)type, orig_size);
                fprintf(file->logfp, " (fresh)");
            }
        }

        /* Add the write time, if we're tracking that.
         * Note that the write time is NOT emitted for when just H5FD_LOG_TIME_WRITE
         * is set.
         */
        if (file->fa.flags & H5FD_LOG_TIME_WRITE)
            fprintf(file->logfp, " (%fs @ %f)\n", write_times.elapsed, write_timer.initial.elapsed);
        else
            fprintf(file->logfp, "\n");
    }

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
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__log_write() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__log_truncate
 *
 * Purpose:     Makes sure that the true file size is the same (or larger)
 *              than the end-of-address.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__log_truncate(H5FD_t *_file, hid_t H5_ATTR_UNUSED dxpl_id, bool H5_ATTR_UNUSED closing)
{
    H5FD_log_t *file      = (H5FD_log_t *)_file;
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(file);

    /* Extend the file to make sure it's large enough */
    if (!H5_addr_eq(file->eoa, file->eof)) {
        H5_timer_t    trunc_timer; /* Timer for truncate operation */
        H5_timevals_t trunc_times; /* Elapsed time for truncate operation */

        /* Initialize timer */
        H5_timer_init(&trunc_timer);

        /* Start timer for truncate operation */
        if (file->fa.flags & H5FD_LOG_TIME_TRUNCATE)
            H5_timer_start(&trunc_timer);

#ifdef H5_HAVE_WIN32_API
        {
            LARGE_INTEGER li;       /* 64-bit (union) integer for SetFilePointer() call */
            DWORD         dwPtrLow; /* Low-order pointer bits from SetFilePointer()
                                     * Only used as an error code here.
                                     */

            /* Windows uses this odd QuadPart union for 32/64-bit portability */
            li.QuadPart = (LONGLONG)file->eoa;

            /* Extend the file to make sure it's large enough.
             *
             * Since INVALID_SET_FILE_POINTER can technically be a valid return value
             * from SetFilePointer(), we also need to check GetLastError().
             */
            dwPtrLow = SetFilePointer(file->hFile, li.LowPart, &li.HighPart, FILE_BEGIN);
            if (INVALID_SET_FILE_POINTER == dwPtrLow) {
                DWORD dwError; /* DWORD error code from GetLastError() */

                dwError = GetLastError();
                if (dwError != NO_ERROR)
                    HGOTO_ERROR(H5E_FILE, H5E_FILEOPEN, FAIL, "unable to set file pointer");
            }

            if (0 == SetEndOfFile(file->hFile))
                HGOTO_ERROR(H5E_IO, H5E_SEEKERROR, FAIL, "unable to extend file properly");
        }
#else  /* H5_HAVE_WIN32_API */
        /* Truncate/extend the file */
        if (-1 == HDftruncate(file->fd, (HDoff_t)file->eoa))
            HSYS_GOTO_ERROR(H5E_IO, H5E_SEEKERROR, FAIL, "unable to extend file properly")
#endif /* H5_HAVE_WIN32_API */

        /* Stop timer for truncate operation */
        if (file->fa.flags & H5FD_LOG_TIME_TRUNCATE)
            H5_timer_stop(&trunc_timer);

        /* Add to the number of truncates, when tracking that */
        if (file->fa.flags & H5FD_LOG_NUM_TRUNCATE)
            file->total_truncate_ops++;

        /* Add to the total truncate time, when tracking that */
        if (file->fa.flags & H5FD_LOG_TIME_TRUNCATE) {
            H5_timer_get_times(trunc_timer, &trunc_times);
            file->total_truncate_time += trunc_times.elapsed;
        }

        /* Emit log string if we're tracking individual truncate events. */
        if (file->fa.flags & H5FD_LOG_TRUNCATE) {
            fprintf(file->logfp, "Truncate: To %10" PRIuHADDR, file->eoa);

            /* Add the truncate time, if we're tracking that.
             * Note that the truncate time is NOT emitted for when just H5FD_LOG_TIME_TRUNCATE
             * is set.
             */
            if (file->fa.flags & H5FD_LOG_TIME_TRUNCATE)
                fprintf(file->logfp, " (%fs @ %f)\n", trunc_times.elapsed, trunc_timer.initial.elapsed);
            else
                fprintf(file->logfp, "\n");
        }

        /* Update the eof value */
        file->eof = file->eoa;

        /* Reset last file I/O information */
        file->pos = HADDR_UNDEF;
        file->op  = OP_UNKNOWN;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__log_truncate() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__log_lock
 *
 * Purpose:     Place a lock on the file
 *
 * Return:      Success:    SUCCEED
 *              Failure:    FAIL, file not locked.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__log_lock(H5FD_t *_file, bool rw)
{
    H5FD_log_t *file = (H5FD_log_t *)_file; /* VFD file struct          */
    int         lock_flags;                 /* file locking flags       */
    herr_t      ret_value = SUCCEED;        /* Return value             */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
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
} /* end H5FD__log_lock() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__log_unlock
 *
 * Purpose:     Remove the existing lock on the file
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__log_unlock(H5FD_t *_file)
{
    H5FD_log_t *file      = (H5FD_log_t *)_file; /* VFD file struct          */
    herr_t      ret_value = SUCCEED;             /* Return value             */

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
} /* end H5FD__log_unlock() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__log_delete
 *
 * Purpose:     Delete a file
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__log_delete(const char *filename, hid_t H5_ATTR_UNUSED fapl_id)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(filename);

    if (HDremove(filename) < 0)
        HSYS_GOTO_ERROR(H5E_VFL, H5E_CANTDELETEFILE, FAIL, "unable to delete file")

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__log_delete() */
