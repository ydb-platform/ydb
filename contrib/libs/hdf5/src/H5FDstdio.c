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
 * Purpose: The C STDIO virtual file driver which only uses calls from stdio.h.
 *          This also serves as an example of coding a simple file driver,
 *          therefore, it should not use any non-public definitions.
 *
 * NOTE:    This driver is not as well tested as the standard SEC2 driver
 *          and is not intended for production use!
 */

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "hdf5.h"

#ifdef H5_HAVE_FLOCK
/* Needed for lock type definitions (e.g., LOCK_EX) */
#include <sys/file.h>
#endif /* H5_HAVE_FLOCK */

#ifdef H5_HAVE_UNISTD_H
#include <unistd.h>
#endif

#ifdef H5_HAVE_WIN32_API
/* The following two defines must be before any windows headers are included */
#define WIN32_LEAN_AND_MEAN /* Exclude rarely-used stuff from Windows headers */
#define NOGDI               /* Exclude Graphic Display Interface macros */

#include <windows.h>
#include <io.h>

#endif /* H5_HAVE_WIN32_API */

/* The driver identification number, initialized at runtime */
static hid_t H5FD_STDIO_g = 0;

/* Whether to ignore file locks when disabled (env var value) */
static htri_t ignore_disabled_file_locks_s = -1;

/* The maximum number of bytes which can be written in a single I/O operation */
static size_t H5_STDIO_MAX_IO_BYTES_g = (size_t)-1;

/* File operations */
typedef enum {
    H5FD_STDIO_OP_UNKNOWN = 0,
    H5FD_STDIO_OP_READ    = 1,
    H5FD_STDIO_OP_WRITE   = 2,
    H5FD_STDIO_OP_SEEK    = 3
} H5FD_stdio_file_op;

/* The description of a file belonging to this driver. The 'eoa' and 'eof'
 * determine the amount of hdf5 address space in use and the high-water mark
 * of the file (the current size of the underlying Unix file). The 'pos'
 * value is used to eliminate file position updates when they would be a
 * no-op. Unfortunately we've found systems that use separate file position
 * indicators for reading and writing so the lseek can only be eliminated if
 * the current operation is the same as the previous operation.  When opening
 * a file the 'eof' will be set to the current file size, 'eoa' will be set
 * to zero, 'pos' will be set to H5F_ADDR_UNDEF (as it is when an error
 * occurs), and 'op' will be set to H5F_OP_UNKNOWN.
 */
typedef struct H5FD_stdio_t {
    H5FD_t             pub;          /* public stuff, must be first      */
    FILE              *fp;           /* the file handle                  */
    int                fd;           /* file descriptor (for truncate)   */
    haddr_t            eoa;          /* end of allocated region          */
    haddr_t            eof;          /* end of file; current file size   */
    haddr_t            pos;          /* current file I/O position        */
    unsigned           write_access; /* Flag to indicate the file was opened with write access */
    bool               ignore_disabled_file_locks;
    H5FD_stdio_file_op op; /* last operation */
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

    HANDLE      hFile; /* Native windows file handle */
#endif /* H5_HAVE_WIN32_API */
} H5FD_stdio_t;

/* Use similar structure as in H5private.h by defining Windows stuff first. */
#ifdef H5_HAVE_WIN32_API
#ifndef H5_HAVE_MINGW
#define file_fseek     _fseeki64
#define file_offset_t  __int64
#define file_ftruncate _chsize_s /* Supported in VS 2005 or newer */
#define file_ftell     _ftelli64
#endif /* H5_HAVE_MINGW */
#endif /* H5_HAVE_WIN32_API */

/* If these functions weren't re-defined for Windows, give them
 * more platform-independent names.
 */
#ifndef file_fseek
#define file_fseek     fseeko
#define file_offset_t  off_t
#define file_ftruncate ftruncate
#define file_ftell     ftello
#endif /* file_fseek */

/* These macros check for overflow of various quantities.  These macros
 * assume that file_offset_t is signed and haddr_t and size_t are unsigned.
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
/* adding for windows NT filesystem support. */
#define MAXADDR          (((haddr_t)1 << (8 * sizeof(file_offset_t) - 1)) - 1)
#define ADDR_OVERFLOW(A) (HADDR_UNDEF == (A) || ((A) & ~(haddr_t)MAXADDR))
#define SIZE_OVERFLOW(Z) ((Z) & ~(hsize_t)MAXADDR)
#define REGION_OVERFLOW(A, Z)                                                                                \
    (ADDR_OVERFLOW(A) || SIZE_OVERFLOW(Z) || HADDR_UNDEF == (A) + (Z) ||                                     \
     (file_offset_t)((A) + (Z)) < (file_offset_t)(A))

/* Prototypes */
static herr_t  H5FD_stdio_term(void);
static H5FD_t *H5FD_stdio_open(const char *name, unsigned flags, hid_t fapl_id, haddr_t maxaddr);
static herr_t  H5FD_stdio_close(H5FD_t *lf);
static int     H5FD_stdio_cmp(const H5FD_t *_f1, const H5FD_t *_f2);
static herr_t  H5FD_stdio_query(const H5FD_t *_f1, unsigned long *flags);
static haddr_t H5FD_stdio_alloc(H5FD_t *_file, H5FD_mem_t type, hid_t dxpl_id, hsize_t size);
static haddr_t H5FD_stdio_get_eoa(const H5FD_t *_file, H5FD_mem_t type);
static herr_t  H5FD_stdio_set_eoa(H5FD_t *_file, H5FD_mem_t type, haddr_t addr);
static haddr_t H5FD_stdio_get_eof(const H5FD_t *_file, H5FD_mem_t type);
static herr_t  H5FD_stdio_get_handle(H5FD_t *_file, hid_t fapl, void **file_handle);
static herr_t  H5FD_stdio_read(H5FD_t *lf, H5FD_mem_t type, hid_t fapl_id, haddr_t addr, size_t size,
                               void *buf);
static herr_t  H5FD_stdio_write(H5FD_t *lf, H5FD_mem_t type, hid_t fapl_id, haddr_t addr, size_t size,
                                const void *buf);
static herr_t  H5FD_stdio_flush(H5FD_t *_file, hid_t dxpl_id, bool closing);
static herr_t  H5FD_stdio_truncate(H5FD_t *_file, hid_t dxpl_id, bool closing);
static herr_t  H5FD_stdio_lock(H5FD_t *_file, bool rw);
static herr_t  H5FD_stdio_unlock(H5FD_t *_file);
static herr_t  H5FD_stdio_delete(const char *filename, hid_t fapl_id);

static const H5FD_class_t H5FD_stdio_g = {
    H5FD_CLASS_VERSION,    /* struct version  */
    H5_VFD_STDIO,          /* value           */
    "stdio",               /* name            */
    MAXADDR,               /* maxaddr         */
    H5F_CLOSE_WEAK,        /* fc_degree       */
    H5FD_stdio_term,       /* terminate       */
    NULL,                  /* sb_size         */
    NULL,                  /* sb_encode       */
    NULL,                  /* sb_decode       */
    0,                     /* fapl_size       */
    NULL,                  /* fapl_get        */
    NULL,                  /* fapl_copy       */
    NULL,                  /* fapl_free       */
    0,                     /* dxpl_size       */
    NULL,                  /* dxpl_copy       */
    NULL,                  /* dxpl_free       */
    H5FD_stdio_open,       /* open            */
    H5FD_stdio_close,      /* close           */
    H5FD_stdio_cmp,        /* cmp             */
    H5FD_stdio_query,      /* query           */
    NULL,                  /* get_type_map    */
    H5FD_stdio_alloc,      /* alloc           */
    NULL,                  /* free            */
    H5FD_stdio_get_eoa,    /* get_eoa         */
    H5FD_stdio_set_eoa,    /* set_eoa         */
    H5FD_stdio_get_eof,    /* get_eof         */
    H5FD_stdio_get_handle, /* get_handle      */
    H5FD_stdio_read,       /* read            */
    H5FD_stdio_write,      /* write           */
    NULL,                  /* read_vector     */
    NULL,                  /* write_vector    */
    NULL,                  /* read_selection  */
    NULL,                  /* write_selection */
    H5FD_stdio_flush,      /* flush           */
    H5FD_stdio_truncate,   /* truncate        */
    H5FD_stdio_lock,       /* lock            */
    H5FD_stdio_unlock,     /* unlock          */
    H5FD_stdio_delete,     /* del             */
    NULL,                  /* ctl             */
    H5FD_FLMAP_DICHOTOMY   /* fl_map          */
};

/*-------------------------------------------------------------------------
 * Function:  H5FD_stdio_init
 *
 * Purpose:  Initialize this driver by registering the driver with the
 *    library.
 *
 * Return:  Success:  The driver ID for the stdio driver.
 *
 *    Failure:  Negative.
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5FD_stdio_init(void)
{
    char *lock_env_var = NULL; /* Environment variable pointer */

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    /* Check the use disabled file locks environment variable */
    lock_env_var = getenv(HDF5_USE_FILE_LOCKING);
    if (lock_env_var && !strcmp(lock_env_var, "BEST_EFFORT"))
        ignore_disabled_file_locks_s = 1; /* Override: Ignore disabled locks */
    else if (lock_env_var && (!strcmp(lock_env_var, "TRUE") || !strcmp(lock_env_var, "1")))
        ignore_disabled_file_locks_s = 0; /* Override: Don't ignore disabled locks */
    else
        ignore_disabled_file_locks_s = -1; /* Environment variable not set, or not set correctly */

    if (H5I_VFL != H5Iget_type(H5FD_STDIO_g))
        H5FD_STDIO_g = H5FDregister(&H5FD_stdio_g);

    return H5FD_STDIO_g;
} /* end H5FD_stdio_init() */

/*---------------------------------------------------------------------------
 * Function:  H5FD_stdio_term
 *
 * Purpose:  Shut down the VFD
 *
 * Returns:     Non-negative on success or negative on failure
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5FD_stdio_term(void)
{
    /* Reset VFL ID */
    H5FD_STDIO_g = 0;

    return 0;
} /* end H5FD_stdio_term() */

/*-------------------------------------------------------------------------
 * Function:  H5Pset_fapl_stdio
 *
 * Purpose:  Modify the file access property list to use the H5FD_STDIO
 *    driver defined in this source file.  There are no driver
 *    specific properties.
 *
 * Return:  Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_fapl_stdio(hid_t fapl_id)
{
    static const char *func = "H5FDset_fapl_stdio"; /*for error reporting*/

    /*NO TRACE*/

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    if (0 == H5Pisa_class(fapl_id, H5P_FILE_ACCESS))
        H5Epush_ret(func, H5E_ERR_CLS, H5E_PLIST, H5E_BADTYPE, "not a file access property list", -1);

    return H5Pset_driver(fapl_id, H5FD_STDIO, NULL);
} /* end H5Pset_fapl_stdio() */

/*-------------------------------------------------------------------------
 * Function:  H5FD_stdio_open
 *
 * Purpose:  Create and/or opens a Standard C file as an HDF5 file.
 *
 * Errors:
 *  IO  CANTOPENFILE    File doesn't exist and CREAT wasn't
 *                      specified.
 *  IO  CANTOPENFILE    fopen() failed.
 *  IO  FILEEXISTS      File exists but CREAT and EXCL were
 *                      specified.
 *
 * Return:
 *      Success:    A pointer to a new file data structure. The
 *                  public fields will be initialized by the
 *                  caller, which is always H5FD_open().
 *
 *      Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static H5FD_t *
H5FD_stdio_open(const char *name, unsigned flags, hid_t fapl_id, haddr_t maxaddr)
{
    FILE              *f            = NULL;
    unsigned           write_access = 0; /* File opened with write access? */
    H5FD_stdio_t      *file         = NULL;
    static const char *func         = "H5FD_stdio_open"; /* Function Name for error reporting */
#ifdef H5_HAVE_WIN32_API
    struct _BY_HANDLE_FILE_INFORMATION fileinfo;
#else  /* H5_HAVE_WIN32_API */
    struct stat sb;
#endif /* H5_HAVE_WIN32_API */

    /* Sanity check on file offsets */
    assert(sizeof(file_offset_t) >= sizeof(size_t));

    /* Quiet compiler */
    (void)fapl_id;

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    /* Check arguments */
    if (!name || !*name)
        H5Epush_ret(func, H5E_ERR_CLS, H5E_ARGS, H5E_BADVALUE, "invalid file name", NULL);
    if (0 == maxaddr || HADDR_UNDEF == maxaddr)
        H5Epush_ret(func, H5E_ERR_CLS, H5E_ARGS, H5E_BADRANGE, "bogus maxaddr", NULL);
    if (ADDR_OVERFLOW(maxaddr))
        H5Epush_ret(func, H5E_ERR_CLS, H5E_ARGS, H5E_OVERFLOW, "maxaddr too large", NULL);

    /* Tentatively open file in read-only mode, to check for existence */
    if (flags & H5F_ACC_RDWR)
        f = fopen(name, "rb+");
    else
        f = fopen(name, "rb");

    if (!f) {
        /* File doesn't exist */
        if (flags & H5F_ACC_CREAT) {
            assert(flags & H5F_ACC_RDWR);
            f            = fopen(name, "wb+");
            write_access = 1; /* Note the write access */
        }
        else
            H5Epush_ret(func, H5E_ERR_CLS, H5E_IO, H5E_CANTOPENFILE,
                        "file doesn't exist and CREAT wasn't specified", NULL);
    }
    else if (flags & H5F_ACC_EXCL) {
        /* File exists, but EXCL is passed.  Fail. */
        assert(flags & H5F_ACC_CREAT);
        fclose(f);
        H5Epush_ret(func, H5E_ERR_CLS, H5E_IO, H5E_FILEEXISTS,
                    "file exists but CREAT and EXCL were specified", NULL);
    }
    else if (flags & H5F_ACC_RDWR) {
        if (flags & H5F_ACC_TRUNC)
            f = freopen(name, "wb+", f);
        write_access = 1; /* Note the write access */
    }                     /* end if */
    /* Note there is no need to reopen if neither TRUNC nor EXCL are specified,
     * as the tentative open will work */

    if (!f)
        H5Epush_ret(func, H5E_ERR_CLS, H5E_IO, H5E_CANTOPENFILE, "fopen failed", NULL);

    /* Build the return value */
    if (NULL == (file = (H5FD_stdio_t *)calloc((size_t)1, sizeof(H5FD_stdio_t)))) {
        fclose(f);
        H5Epush_ret(func, H5E_ERR_CLS, H5E_RESOURCE, H5E_NOSPACE, "memory allocation failed", NULL);
    } /* end if */
    file->fp           = f;
    file->op           = H5FD_STDIO_OP_SEEK;
    file->pos          = HADDR_UNDEF;
    file->write_access = write_access; /* Note the write_access for later */
    if (file_fseek(file->fp, (file_offset_t)0, SEEK_END) < 0) {
        file->op = H5FD_STDIO_OP_UNKNOWN;
    }
    else {
        file_offset_t x = file_ftell(file->fp);
        assert(x >= 0);
        file->eof = (haddr_t)x;
    }

    /* Check the file locking flags in the fapl */
    if (ignore_disabled_file_locks_s != -1)
        /* The environment variable was set, so use that preferentially */
        file->ignore_disabled_file_locks = ignore_disabled_file_locks_s;
    else {
        bool unused;

        /* Use the value in the property list */
        if (H5Pget_file_locking(fapl_id, &unused, &file->ignore_disabled_file_locks) < 0) {
            free(file);
            fclose(f);
            H5Epush_ret(func, H5E_ERR_CLS, H5E_FILE, H5E_CANTGET,
                        "unable to get use disabled file locks property", NULL);
        }
    }

    /* Get the file descriptor (needed for truncate and some Windows information) */
#ifdef H5_HAVE_WIN32_API
    file->fd = _fileno(file->fp);
#else  /* H5_HAVE_WIN32_API */
    file->fd = fileno(file->fp);
#endif /* H5_HAVE_WIN32_API */
    if (file->fd < 0) {
        free(file);
        fclose(f);
        H5Epush_ret(func, H5E_ERR_CLS, H5E_FILE, H5E_CANTOPENFILE, "unable to get file descriptor", NULL);
    } /* end if */

#ifdef H5_HAVE_WIN32_API
    file->hFile = (HANDLE)_get_osfhandle(file->fd);
    if (INVALID_HANDLE_VALUE == file->hFile) {
        free(file);
        fclose(f);
        H5Epush_ret(func, H5E_ERR_CLS, H5E_FILE, H5E_CANTOPENFILE, "unable to get Windows file handle", NULL);
    } /* end if */

    if (!GetFileInformationByHandle((HANDLE)file->hFile, &fileinfo)) {
        free(file);
        fclose(f);
        H5Epush_ret(func, H5E_ERR_CLS, H5E_FILE, H5E_CANTOPENFILE,
                    "unable to get Windows file descriptor information", NULL);
    } /* end if */

    file->nFileIndexHigh       = fileinfo.nFileIndexHigh;
    file->nFileIndexLow        = fileinfo.nFileIndexLow;
    file->dwVolumeSerialNumber = fileinfo.dwVolumeSerialNumber;
#else  /* H5_HAVE_WIN32_API */
    if (fstat(file->fd, &sb) < 0) {
        free(file);
        fclose(f);
        H5Epush_ret(func, H5E_ERR_CLS, H5E_FILE, H5E_BADFILE, "unable to fstat file", NULL);
    } /* end if */
    file->device = sb.st_dev;
    file->inode  = sb.st_ino;
#endif /* H5_HAVE_WIN32_API */

    return ((H5FD_t *)file);
} /* end H5FD_stdio_open() */

/*-------------------------------------------------------------------------
 * Function:  H5F_stdio_close
 *
 * Purpose:  Closes a file.
 *
 * Errors:
 *    IO    CLOSEERROR  Fclose failed.
 *
 * Return:  Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_stdio_close(H5FD_t *_file)
{
    H5FD_stdio_t      *file = (H5FD_stdio_t *)_file;
    static const char *func = "H5FD_stdio_close"; /* Function Name for error reporting */

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    if (fclose(file->fp) < 0)
        H5Epush_ret(func, H5E_ERR_CLS, H5E_IO, H5E_CLOSEERROR, "fclose failed", -1);

    free(file);

    return 0;
} /* end H5FD_stdio_close() */

/*-------------------------------------------------------------------------
 * Function:  H5FD_stdio_cmp
 *
 * Purpose:  Compares two files belonging to this driver using an
 *    arbitrary (but consistent) ordering.
 *
 * Return:
 *      Success:    A value like strcmp()
 *
 *      Failure:    never fails (arguments were checked by the caller).
 *
 *-------------------------------------------------------------------------
 */
static int
H5FD_stdio_cmp(const H5FD_t *_f1, const H5FD_t *_f2)
{
    const H5FD_stdio_t *f1 = (const H5FD_stdio_t *)_f1;
    const H5FD_stdio_t *f2 = (const H5FD_stdio_t *)_f2;

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

#ifdef H5_HAVE_WIN32_API
    if (f1->dwVolumeSerialNumber < f2->dwVolumeSerialNumber)
        return -1;
    if (f1->dwVolumeSerialNumber > f2->dwVolumeSerialNumber)
        return 1;

    if (f1->nFileIndexHigh < f2->nFileIndexHigh)
        return -1;
    if (f1->nFileIndexHigh > f2->nFileIndexHigh)
        return 1;

    if (f1->nFileIndexLow < f2->nFileIndexLow)
        return -1;
    if (f1->nFileIndexLow > f2->nFileIndexLow)
        return 1;
#else /* H5_HAVE_WIN32_API */
#ifdef H5_DEV_T_IS_SCALAR
    if (f1->device < f2->device)
        return -1;
    if (f1->device > f2->device)
        return 1;
#else  /* H5_DEV_T_IS_SCALAR */
    /* If dev_t isn't a scalar value on this system, just use memcmp to
     * determine if the values are the same or not.  The actual return value
     * shouldn't really matter...
     */
    if (memcmp(&(f1->device), &(f2->device), sizeof(dev_t)) < 0)
        return -1;
    if (memcmp(&(f1->device), &(f2->device), sizeof(dev_t)) > 0)
        return 1;
#endif /* H5_DEV_T_IS_SCALAR */
    if (f1->inode < f2->inode)
        return -1;
    if (f1->inode > f2->inode)
        return 1;
#endif /* H5_HAVE_WIN32_API */

    return 0;
} /* H5FD_stdio_cmp() */

/*-------------------------------------------------------------------------
 * Function:  H5FD_stdio_query
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
H5FD_stdio_query(const H5FD_t *_f, unsigned long /*OUT*/ *flags)
{
    /* Quiet the compiler */
    (void)_f;

    /* Set the VFL feature flags that this driver supports.
     *
     * Note that this VFD does not support SWMR due to the unpredictable
     * nature of the buffering layer.
     */
    if (flags) {
        *flags = 0;
        *flags |= H5FD_FEAT_AGGREGATE_METADATA;  /* OK to aggregate metadata allocations  */
        *flags |= H5FD_FEAT_ACCUMULATE_METADATA; /* OK to accumulate metadata for faster writes */
        *flags |= H5FD_FEAT_DATA_SIEVE; /* OK to perform data sieving for faster raw data reads & writes    */
        *flags |= H5FD_FEAT_AGGREGATE_SMALLDATA;    /* OK to aggregate "small" raw data allocations    */
        *flags |= H5FD_FEAT_DEFAULT_VFD_COMPATIBLE; /* VFD creates a file which can be opened with the default
                                                       VFD      */
    }

    return 0;
} /* end H5FD_stdio_query() */

/*-------------------------------------------------------------------------
 * Function:  H5FD_stdio_alloc
 *
 * Purpose:     Allocates file memory. If fseeko isn't available, makes
 *              sure the file size isn't bigger than 2GB because the
 *              parameter OFFSET of fseek is of the type LONG INT, limiting
 *              the file size to 2GB.
 *
 * Return:
 *      Success:    Address of new memory
 *
 *      Failure:    HADDR_UNDEF
 *
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD_stdio_alloc(H5FD_t *_file, H5FD_mem_t /*UNUSED*/ type, hid_t /*UNUSED*/ dxpl_id, hsize_t size)
{
    H5FD_stdio_t *file = (H5FD_stdio_t *)_file;
    haddr_t       addr;

    /* Quiet compiler */
    (void)type;
    (void)dxpl_id;

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    /* Compute the address for the block to allocate */
    addr = file->eoa;

    file->eoa = addr + size;

    return addr;
} /* end H5FD_stdio_alloc() */

/*-------------------------------------------------------------------------
 * Function:  H5FD_stdio_get_eoa
 *
 * Purpose:  Gets the end-of-address marker for the file. The EOA marker
 *           is the first address past the last byte allocated in the
 *           format address space.
 *
 * Return:  Success:  The end-of-address marker.
 *
 *    Failure:  HADDR_UNDEF
 *
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD_stdio_get_eoa(const H5FD_t *_file, H5FD_mem_t /*UNUSED*/ type)
{
    const H5FD_stdio_t *file = (const H5FD_stdio_t *)_file;

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    /* Quiet compiler */
    (void)type;

    return file->eoa;
} /* end H5FD_stdio_get_eoa() */

/*-------------------------------------------------------------------------
 * Function:  H5FD_stdio_set_eoa
 *
 * Purpose:  Set the end-of-address marker for the file. This function is
 *    called shortly after an existing HDF5 file is opened in order
 *    to tell the driver where the end of the HDF5 data is located.
 *
 * Return:  Success:  0
 *
 *    Failure:  Does not fail
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_stdio_set_eoa(H5FD_t *_file, H5FD_mem_t /*UNUSED*/ type, haddr_t addr)
{
    H5FD_stdio_t *file = (H5FD_stdio_t *)_file;

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    /* Quiet the compiler */
    (void)type;

    file->eoa = addr;

    return 0;
}

/*-------------------------------------------------------------------------
 * Function:  H5FD_stdio_get_eof
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
H5FD_stdio_get_eof(const H5FD_t *_file, H5FD_mem_t /*UNUSED*/ type)
{
    const H5FD_stdio_t *file = (const H5FD_stdio_t *)_file;

    /* Quiet the compiler */
    (void)type;

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    /* Quiet the compiler */
    (void)type;

    return (file->eof);
} /* end H5FD_stdio_get_eof() */

/*-------------------------------------------------------------------------
 * Function:       H5FD_stdio_get_handle
 *
 * Purpose:        Returns the file handle of stdio file driver.
 *
 * Returns:        Non-negative if succeed or negative if fails.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_stdio_get_handle(H5FD_t *_file, hid_t /*UNUSED*/ fapl, void **file_handle)
{
    H5FD_stdio_t      *file = (H5FD_stdio_t *)_file;
    static const char *func = "H5FD_stdio_get_handle"; /* Function Name for error reporting */

    /* Quiet the compiler */
    (void)fapl;

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    *file_handle = &(file->fp);
    if (*file_handle == NULL)
        H5Epush_ret(func, H5E_ERR_CLS, H5E_IO, H5E_WRITEERROR, "get handle failed", -1);

    return 0;
} /* end H5FD_stdio_get_handle() */

/*-------------------------------------------------------------------------
 * Function:  H5FD_stdio_read
 *
 * Purpose:  Reads SIZE bytes beginning at address ADDR in file LF and
 *    places them in buffer BUF.  Reading past the logical or
 *    physical end of file returns zeros instead of failing.
 *
 * Errors:
 *    IO    READERROR  fread failed.
 *    IO    SEEKERROR  fseek failed.
 *
 * Return:  Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_stdio_read(H5FD_t *_file, H5FD_mem_t /*UNUSED*/ type, hid_t /*UNUSED*/ dxpl_id, haddr_t addr,
                size_t size, void /*OUT*/ *buf)
{
    H5FD_stdio_t      *file = (H5FD_stdio_t *)_file;
    static const char *func = "H5FD_stdio_read"; /* Function Name for error reporting */

    /* Quiet the compiler */
    (void)type;
    (void)dxpl_id;

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    /* Check for overflow */
    if (HADDR_UNDEF == addr)
        H5Epush_ret(func, H5E_ERR_CLS, H5E_IO, H5E_OVERFLOW, "file address overflowed", -1);
    if (REGION_OVERFLOW(addr, size))
        H5Epush_ret(func, H5E_ERR_CLS, H5E_IO, H5E_OVERFLOW, "file address overflowed", -1);

    /* Check easy cases */
    if (0 == size)
        return 0;
    if ((haddr_t)addr >= file->eof) {
        memset(buf, 0, size);
        return 0;
    }

    /* Seek to the correct file position. */
    if (!(file->op == H5FD_STDIO_OP_READ || file->op == H5FD_STDIO_OP_SEEK) || file->pos != addr) {
        if (file_fseek(file->fp, (file_offset_t)addr, SEEK_SET) < 0) {
            file->op  = H5FD_STDIO_OP_UNKNOWN;
            file->pos = HADDR_UNDEF;
            H5Epush_ret(func, H5E_ERR_CLS, H5E_IO, H5E_SEEKERROR, "fseek failed", -1);
        }
        file->pos = addr;
    }

    /* Read zeros past the logical end of file (physical is handled below) */
    if (addr + size > file->eof) {
        size_t nbytes = (size_t)(addr + size - file->eof);
        memset((unsigned char *)buf + size - nbytes, 0, nbytes);
        size -= nbytes;
    }

    /* Read the data.  Since we're reading single-byte values, a partial read
     * will advance the file position by N.  If N is zero or an error
     * occurs then the file position is undefined.
     */
    while (size > 0) {

        size_t bytes_in   = 0; /* # of bytes to read       */
        size_t bytes_read = 0; /* # of bytes actually read */
        size_t item_size  = 1; /* size of items in bytes */

        if (size > H5_STDIO_MAX_IO_BYTES_g)
            bytes_in = H5_STDIO_MAX_IO_BYTES_g;
        else
            bytes_in = size;

        bytes_read = fread(buf, item_size, bytes_in, file->fp);

        if (0 == bytes_read && ferror(file->fp)) { /* error */
            file->op  = H5FD_STDIO_OP_UNKNOWN;
            file->pos = HADDR_UNDEF;
            H5Epush_ret(func, H5E_ERR_CLS, H5E_IO, H5E_READERROR, "fread failed", -1);
        } /* end if */

        if (0 == bytes_read && feof(file->fp)) {
            /* end of file but not end of format address space */
            memset((unsigned char *)buf, 0, size);
            break;
        } /* end if */

        size -= bytes_read;
        addr += (haddr_t)bytes_read;
        buf = (char *)buf + bytes_read;
    } /* end while */

    /* Update the file position data. */
    file->op  = H5FD_STDIO_OP_READ;
    file->pos = addr;

    return 0;
}

/*-------------------------------------------------------------------------
 * Function:  H5FD_stdio_write
 *
 * Purpose:  Writes SIZE bytes from the beginning of BUF into file LF at
 *    file address ADDR.
 *
 * Errors:
 *    IO    SEEKERROR   fseek failed.
 *    IO    WRITEERROR  fwrite failed.
 *
 * Return:  Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_stdio_write(H5FD_t *_file, H5FD_mem_t /*UNUSED*/ type, hid_t /*UNUSED*/ dxpl_id, haddr_t addr,
                 size_t size, const void *buf)
{
    H5FD_stdio_t      *file = (H5FD_stdio_t *)_file;
    static const char *func = "H5FD_stdio_write"; /* Function Name for error reporting */

    /* Quiet the compiler */
    (void)dxpl_id;
    (void)type;

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    /* Check for overflow conditions */
    if (HADDR_UNDEF == addr)
        H5Epush_ret(func, H5E_ERR_CLS, H5E_IO, H5E_OVERFLOW, "file address overflowed", -1);
    if (REGION_OVERFLOW(addr, size))
        H5Epush_ret(func, H5E_ERR_CLS, H5E_IO, H5E_OVERFLOW, "file address overflowed", -1);

    /* Seek to the correct file position. */
    if ((file->op != H5FD_STDIO_OP_WRITE && file->op != H5FD_STDIO_OP_SEEK) || file->pos != addr) {
        if (file_fseek(file->fp, (file_offset_t)addr, SEEK_SET) < 0) {
            file->op  = H5FD_STDIO_OP_UNKNOWN;
            file->pos = HADDR_UNDEF;
            H5Epush_ret(func, H5E_ERR_CLS, H5E_IO, H5E_SEEKERROR, "fseek failed", -1);
        }
        file->pos = addr;
    }

    /* Write the buffer.  On successful return, the file position will be
     * advanced by the number of bytes read.  On failure, the file position is
     * undefined.
     */
    while (size > 0) {

        size_t bytes_in    = 0; /* # of bytes to write  */
        size_t bytes_wrote = 0; /* # of bytes written   */
        size_t item_size   = 1; /* size of items in bytes */

        if (size > H5_STDIO_MAX_IO_BYTES_g)
            bytes_in = H5_STDIO_MAX_IO_BYTES_g;
        else
            bytes_in = size;

        bytes_wrote = fwrite(buf, item_size, bytes_in, file->fp);

        if (bytes_wrote != bytes_in || (0 == bytes_wrote && ferror(file->fp))) { /* error */
            file->op  = H5FD_STDIO_OP_UNKNOWN;
            file->pos = HADDR_UNDEF;
            H5Epush_ret(func, H5E_ERR_CLS, H5E_IO, H5E_WRITEERROR, "fwrite failed", -1);
        } /* end if */

        assert(bytes_wrote > 0);
        assert((size_t)bytes_wrote <= size);

        size -= bytes_wrote;
        addr += (haddr_t)bytes_wrote;
        buf = (const char *)buf + bytes_wrote;
    }

    /* Update seek optimizing data. */
    file->op  = H5FD_STDIO_OP_WRITE;
    file->pos = addr;

    /* Update EOF if necessary */
    if (file->pos > file->eof)
        file->eof = file->pos;

    return 0;
}

/*-------------------------------------------------------------------------
 * Function:  H5FD_stdio_flush
 *
 * Purpose:  Makes sure that all data is on disk.
 *
 * Errors:
 *    IO    SEEKERROR     fseek failed.
 *    IO    WRITEERROR    fflush or fwrite failed.
 *
 * Return:  Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_stdio_flush(H5FD_t *_file, hid_t /*UNUSED*/ dxpl_id, bool closing)
{
    H5FD_stdio_t      *file = (H5FD_stdio_t *)_file;
    static const char *func = "H5FD_stdio_flush"; /* Function Name for error reporting */

    /* Quiet the compiler */
    (void)dxpl_id;

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    /* Only try to flush the file if we have write access */
    if (file->write_access) {
        if (!closing) {
            if (fflush(file->fp) < 0)
                H5Epush_ret(func, H5E_ERR_CLS, H5E_IO, H5E_WRITEERROR, "fflush failed", -1);

            /* Reset last file I/O information */
            file->pos = HADDR_UNDEF;
            file->op  = H5FD_STDIO_OP_UNKNOWN;
        } /* end if */
    }     /* end if */

    return 0;
} /* end H5FD_stdio_flush() */

/*-------------------------------------------------------------------------
 * Function:  H5FD_stdio_truncate
 *
 * Purpose:  Makes sure that the true file size is the same (or larger)
 *    than the end-of-address.
 *
 * Errors:
 *    IO    SEEKERROR     fseek failed.
 *    IO    WRITEERROR    fflush or fwrite failed.
 *
 * Return:  Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_stdio_truncate(H5FD_t *_file, hid_t /*UNUSED*/ dxpl_id, bool /*UNUSED*/ closing)
{
    H5FD_stdio_t      *file = (H5FD_stdio_t *)_file;
    static const char *func = "H5FD_stdio_truncate"; /* Function Name for error reporting */

    /* Quiet the compiler */
    (void)dxpl_id;
    (void)closing;

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    /* Only try to flush the file if we have write access */
    if (file->write_access) {
        /* Makes sure that the true file size is the same as the end-of-address. */
        if (file->eoa != file->eof) {

#ifdef H5_HAVE_WIN32_API
            LARGE_INTEGER li;       /* 64-bit (union) integer for SetFilePointer() call */
            DWORD         dwPtrLow; /* Low-order pointer bits from SetFilePointer()
                                     * Only used as an error code here.
                                     */
            DWORD dwError;          /* DWORD error code from GetLastError() */
            BOOL  bError;           /* Boolean error flag */

            /* Reset seek offset to beginning of file, so that file isn't re-extended later */
            rewind(file->fp);

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
                    H5Epush_ret(func, H5E_ERR_CLS, H5E_FILE, H5E_FILEOPEN, "unable to set file pointer", -1);
            }

            bError = SetEndOfFile(file->hFile);
            if (0 == bError)
                H5Epush_ret(func, H5E_ERR_CLS, H5E_IO, H5E_SEEKERROR,
                            "unable to truncate/extend file properly", -1);
#else  /* H5_HAVE_WIN32_API */
            /* Reset seek offset to beginning of file, so that file isn't re-extended later */
            rewind(file->fp);

            /* Truncate file to proper length */
            if (-1 == file_ftruncate(file->fd, (file_offset_t)file->eoa))
                H5Epush_ret(func, H5E_ERR_CLS, H5E_IO, H5E_SEEKERROR,
                            "unable to truncate/extend file properly", -1);
#endif /* H5_HAVE_WIN32_API */

            /* Update the eof value */
            file->eof = file->eoa;

            /* Reset last file I/O information */
            file->pos = HADDR_UNDEF;
            file->op  = H5FD_STDIO_OP_UNKNOWN;
        } /* end if */
    }     /* end if */
    else {
        /* Double-check for problems */
        if (file->eoa > file->eof)
            H5Epush_ret(func, H5E_ERR_CLS, H5E_IO, H5E_TRUNCATED, "eoa > eof!", -1);
    } /* end else */

    return 0;
} /* end H5FD_stdio_truncate() */

/*-------------------------------------------------------------------------
 * Function:    H5FD_stdio_lock
 *
 * Purpose:     Lock a file via flock
 *              NOTE: This function is a no-op if flock() is not present.
 *
 * Errors:
 *    IO    FCNTL    flock failed.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_stdio_lock(H5FD_t *_file, bool rw)
{
#ifdef H5_HAVE_FLOCK
    H5FD_stdio_t      *file = (H5FD_stdio_t *)_file; /* VFD file struct                      */
    int                lock_flags;                   /* file locking flags                   */
    static const char *func = "H5FD_stdio_lock";     /* Function Name for error reporting    */

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    assert(file);

    /* Set exclusive or shared lock based on rw status */
    lock_flags = rw ? LOCK_EX : LOCK_SH;

    /* Place a non-blocking lock on the file */
    if (flock(file->fd, lock_flags | LOCK_NB) < 0) {
        if (file->ignore_disabled_file_locks && ENOSYS == errno)
            /* When errno is set to ENOSYS, the file system does not support
             * locking, so ignore it.
             */
            errno = 0;
        else
            H5Epush_ret(func, H5E_ERR_CLS, H5E_VFL, H5E_CANTLOCKFILE, "file lock failed", -1);
    } /* end if */

    /* Flush the stream */
    if (fflush(file->fp) < 0)
        H5Epush_ret(func, H5E_ERR_CLS, H5E_IO, H5E_WRITEERROR, "fflush failed", -1);

#endif /* H5_HAVE_FLOCK */

    return 0;
} /* end H5FD_stdio_lock() */

/*-------------------------------------------------------------------------
 * Function:    H5F_stdio_unlock
 *
 * Purpose:     Unlock a file via flock
 *              NOTE: This function is a no-op if flock() is not present.
 *
 * Errors:
 *    IO    FCNTL    flock failed.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_stdio_unlock(H5FD_t *_file)
{
#ifdef H5_HAVE_FLOCK
    H5FD_stdio_t      *file = (H5FD_stdio_t *)_file; /* VFD file struct                      */
    static const char *func = "H5FD_stdio_unlock";   /* Function Name for error reporting    */

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    assert(file);

    /* Flush the stream */
    if (fflush(file->fp) < 0)
        H5Epush_ret(func, H5E_ERR_CLS, H5E_IO, H5E_WRITEERROR, "fflush failed", -1);

    /* Place a non-blocking lock on the file */
    if (flock(file->fd, LOCK_UN) < 0) {
        if (file->ignore_disabled_file_locks && ENOSYS == errno)
            /* When errno is set to ENOSYS, the file system does not support
             * locking, so ignore it.
             */
            errno = 0;
        else
            H5Epush_ret(func, H5E_ERR_CLS, H5E_VFL, H5E_CANTUNLOCKFILE, "file unlock failed", -1);
    } /* end if */

#endif /* H5_HAVE_FLOCK */

    return 0;
} /* end H5FD_stdio_unlock() */

/*-------------------------------------------------------------------------
 * Function:    H5FD_stdio_delete
 *
 * Purpose:     Delete a file
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_stdio_delete(const char *filename, hid_t /*UNUSED*/ fapl_id)
{
    static const char *func = "H5FD_stdio_delete"; /* Function Name for error reporting    */

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    assert(filename);

    /* Quiet compiler */
    (void)fapl_id;

    if (remove(filename) < 0)
        H5Epush_ret(func, H5E_ERR_CLS, H5E_VFL, H5E_CANTDELETEFILE, "can't delete file)", -1);

    return 0;
} /* end H5FD_stdio_delete() */

#ifdef H5private_H
/*
 * This is not related to the functionality of the driver code.
 * It is added here to trigger warning if HDF5 private definitions are included
 * by mistake.  The code should use only HDF5 public API and definitions.
 */
#error "Do not use HDF5 private definitions"
#endif
