/*********************************************************************
*    Copyright 2018, UCAR/Unidata
*    See netcdf/COPYRIGHT file for copying and redistribution conditions.
* ********************************************************************/

/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://support.hdfgroup.org/ftp/HDF5/releases.  *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* Programmer:  Dennis Heimbigner dmh@ucar.edu
 *
 * Purpose:  Access remote datasets using byte range requests.
 * Derived from the HDF5 H5FDstdio.c file.
 *
 * NOTE:    This driver is not as well tested as the standard SEC2 driver
 *          and is not intended for production use!
 */

#include "config.h"

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include <hdf5.h>
#include <curl/curl.h>

#ifdef H5_HAVE_FLOCK
/* Needed for lock type definitions (e.g., LOCK_EX) */
#include <sys/file.h>
#endif /* H5_HAVE_FLOCK */

#ifdef H5_HAVE_UNISTD_H
#include <unistd.h>
#endif

#ifdef H5_HAVE_WIN32_API
/* The following two defines must be before any windows headers are included */
#define WIN32_LEAN_AND_MEAN    /* Exclude rarely-used stuff from Windows headers */
#define NOGDI                  /* Exclude Graphic Display Interface macros */

#include <windows.h>
#include <io.h>

#endif /* H5_HAVE_WIN32_API */

#include "netcdf.h"
#include "ncbytes.h"
#include "nclist.h"
#include "nchttp.h"

#include "H5FDhttp.h"

typedef off_t file_offset_t;

/* The driver identification number, initialized at runtime */
static hid_t H5FD_HTTP_g = 0;

/* File operations */
typedef enum {
    H5FD_HTTP_OP_UNKNOWN=0,
    H5FD_HTTP_OP_READ=1,
    H5FD_HTTP_OP_WRITE=2,
    H5FD_HTTP_OP_SEEK=3
} H5FD_http_file_op;

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
typedef struct H5FD_http_t {
    H5FD_t      pub;            /* public stuff, must be first      */
    haddr_t     eoa;            /* end of allocated region          */
    haddr_t     eof;            /* end of file; current file size   */
    haddr_t     pos;            /* current file I/O position        */
    unsigned    write_access;   /* Flag to indicate the file was opened with write access */
    H5FD_http_file_op op;	/* last operation */
    NC_HTTP_STATE*  state;       /* Curl handle + extra */
    char*           url;        /* The URL (minus any fragment) for the dataset */ 
} H5FD_http_t;


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
#define MAXADDR (((haddr_t)1<<(8*sizeof(file_offset_t)-1))-1)
#define ADDR_OVERFLOW(A)  (HADDR_UNDEF==(A) || ((A) & ~(haddr_t)MAXADDR))
#define SIZE_OVERFLOW(Z)  ((Z) & ~(hsize_t)MAXADDR)
#define REGION_OVERFLOW(A,Z)  (ADDR_OVERFLOW(A) || SIZE_OVERFLOW(Z) || \
    HADDR_UNDEF==(A)+(Z) || (file_offset_t)((A)+(Z))<(file_offset_t)(A))

/* Prototypes */
static H5FD_t *H5FD_http_open(const char *name, unsigned flags,
                 hid_t fapl_id, haddr_t maxaddr);
static herr_t H5FD_http_close(H5FD_t *lf);
static int H5FD_http_cmp(const H5FD_t *_f1, const H5FD_t *_f2);
static herr_t H5FD_http_query(const H5FD_t *_f1, unsigned long *flags);
static haddr_t H5FD_http_alloc(H5FD_t *_file, H5FD_mem_t type, hid_t dxpl_id, hsize_t size);
static haddr_t H5FD_http_get_eoa(const H5FD_t *_file, H5FD_mem_t type);
static herr_t H5FD_http_set_eoa(H5FD_t *_file, H5FD_mem_t type, haddr_t addr);
static herr_t  H5FD_http_get_handle(H5FD_t *_file, hid_t fapl, void** file_handle);
static herr_t H5FD_http_read(H5FD_t *lf, H5FD_mem_t type, hid_t fapl_id, haddr_t addr,
                size_t size, void *buf);
static herr_t H5FD_http_write(H5FD_t *lf, H5FD_mem_t type, hid_t fapl_id, haddr_t addr,
                size_t size, const void *buf);
static herr_t H5FD_http_term(void);

/* The H5FD_class_t structure has different versions */
static haddr_t H5FD_http_get_eof(const H5FD_t *_file, H5FD_mem_t type);
static herr_t H5FD_http_flush(H5FD_t *_file, hid_t dxpl_id, hbool_t closing);
static herr_t H5FD_http_lock(H5FD_t *_file, hbool_t rw);
static herr_t H5FD_http_unlock(H5FD_t *_file);

/* Beware, not same as H5FD_HTTP_g */
static const H5FD_class_t H5FD_http_g = {
#if H5FD_CLASS_VERSION > 0
    H5FD_CLASS_VERSION,		/* struct version  */
    H5_VFD_HTTP,		/* value           */
#endif
    "http",			/* name         */
    MAXADDR,			/* maxaddr      */
    H5F_CLOSE_WEAK,		/* fc_degree    */
    H5FD_http_term,		/* terminate    */
    NULL,			/* sb_size      */
    NULL,			/* sb_encode    */
    NULL,			/* sb_decode    */
    0,				/* fapl_size    */
    NULL,			/* fapl_get     */
    NULL,			/* fapl_copy    */
    NULL,			/* fapl_free    */
    0,				/* dxpl_size    */
    NULL,			/* dxpl_copy    */
    NULL,			/* dxpl_free    */
    H5FD_http_open,		/* open         */
    H5FD_http_close,		/* close        */
    H5FD_http_cmp,		/* cmp          */
    H5FD_http_query,		/* query        */
    NULL,			/* get_type_map */
    H5FD_http_alloc,		/* alloc        */
    NULL,			/* free         */
    H5FD_http_get_eoa,		/* get_eoa      */
    H5FD_http_set_eoa,		/* set_eoa      */
    H5FD_http_get_eof,		/* get_eof      */
    H5FD_http_get_handle,	/* get_handle   */
    H5FD_http_read,		/* read         */
    H5FD_http_write,		/* write        */
#if H5FD_CLASS_VERSION > 0
    NULL,			/* read_vector     */
    NULL,			/* write_vector    */
    NULL,			/* read_selection  */
    NULL,			/* write_selection */
#endif
    H5FD_http_flush,		/* flush        */
    NULL,			/* truncate     */
    H5FD_http_lock,		/* lock         */
    H5FD_http_unlock,		/* unlock       */
#if H5FD_CLASS_VERSION > 0
    NULL,			/* del          */
    NULL,			/* ctl	        */
#endif
    H5FD_FLMAP_DICHOTOMY	/* fl_map       */
};


/*-------------------------------------------------------------------------
 * Function:  H5FD_http_init
 *
 * Purpose:  Initialize this driver by registering the driver with the
 *    library.
 *
 * Return:  Success:  The driver ID for the driver.
 *
 *    Failure:  Negative.
 *
 * Programmer:  Robb Matzke
 *              Thursday, July 29, 1999
 *
 *-------------------------------------------------------------------------
 */
EXTERNL hid_t
H5FD_http_init(void)
{
    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    if (H5I_VFL!=H5Iget_type(H5FD_HTTP_g))
        H5FD_HTTP_g = H5FDregister(&H5FD_http_g);
    return H5FD_HTTP_g;
} /* end H5FD_http_init() */


/*-------------------------------------------------------------------------
 * Function:  H5FD_http_finalize
 *
 * Purpose:  Free this driver by unregistering the driver with the
 *    library.
 *
 * Returns:     Non-negative on success or negative on failure
 *
 * Programmer:  John Donoghue
 *              Tuesday, December 12, 2023
 *
 *-------------------------------------------------------------------------
 */
EXTERNL hid_t
H5FD_http_finalize(void)
{
    /* Reset VFL ID */
    if (H5FD_HTTP_g && (H5Iis_valid(H5FD_HTTP_g) > 0))
         H5FDunregister(H5FD_HTTP_g);
    H5FD_HTTP_g = 0;

    return H5FD_HTTP_g;
} /* end H5FD_http_finalize() */


/*---------------------------------------------------------------------------
 * Function:  H5FD_http_term
 *
 * Purpose:  Shut down the VFD
 *
 * Returns:     Non-negative on success or negative on failure
 *
 * Programmer:  Quincey Koziol
 *              Friday, Jan 30, 2004
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5FD_http_term(void)
{
    return 0;
} /* end H5FD_http_term() */


/*-------------------------------------------------------------------------
 * Function:  H5Pset_fapl_http
 *
 * Purpose:  Modify the file access property list to use the H5FD_HTTP
 *    driver defined in this source file.  There are no driver
 *    specific properties.
 *
 * Return:  Non-negative on success/Negative on failure
 *
 * Programmer:  Robb Matzke
 *    Thursday, February 19, 1998
 *
 *-------------------------------------------------------------------------
 */
EXTERNL herr_t
H5Pset_fapl_http(hid_t fapl_id)
{
    static const char *func = "H5FDset_fapl_http";  /*for error reporting*/

    /*NO TRACE*/

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    if(0 == H5Pisa_class(fapl_id, H5P_FILE_ACCESS))
        H5Epush_ret(func, H5E_ERR_CLS, H5E_PLIST, H5E_BADTYPE, "not a file access property list", -1);

    return H5Pset_driver(fapl_id, H5FD_HTTP, NULL);
} /* end H5Pset_fapl_http() */


/*-------------------------------------------------------------------------
 * Function:  H5FD_http_open
 *
 * Purpose:  Opens a remote Object as an HDF5 file.
 *
 * Errors:
 *  IO  CANTOPENFILE    File doesn't exist and CREAT wasn't
 *                      specified.
 *
 * Return:
 *      Success:    A pointer to a new file data structure. The
 *                  public fields will be initialized by the
 *                  caller, which is always H5FD_open().
 *
 *      Failure:    NULL
 *
 * Programmer:  Dennis Heimbigner
 *
 *-------------------------------------------------------------------------
 */
static H5FD_t *
H5FD_http_open( const char *name, unsigned flags, hid_t /*UNUSED*/ fapl_id,
    haddr_t maxaddr)
{
    unsigned            write_access = 0;           /* File opened with write access? */
    H5FD_http_t *file = NULL;
    static const char   *func = "H5FD_http_open";  /* Function Name for error reporting */
    long long len = -1;
    int ncstat = NC_NOERR;
    NC_HTTP_STATE* state = NULL;

    /* Sanity check on file offsets */
    assert(sizeof(file_offset_t) >= sizeof(size_t));

    /* Quiet compiler */
    fapl_id = fapl_id;

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    /* Check arguments */
    if (!name || !*name)
        H5Epush_ret(func, H5E_ERR_CLS, H5E_ARGS, H5E_BADVALUE, "invalid URL", NULL);
    if (0 == maxaddr || HADDR_UNDEF == maxaddr)
        H5Epush_ret(func, H5E_ERR_CLS, H5E_ARGS, H5E_BADRANGE, "bogus maxaddr", NULL);
    if (ADDR_OVERFLOW(maxaddr))
        H5Epush_ret(func, H5E_ERR_CLS, H5E_ARGS, H5E_OVERFLOW, "maxaddr too large", NULL);

    /* Always read-only */
    write_access = 0;

   /* Open file in read-only mode, to check for existence  and get length */
    if((ncstat = nc_http_open(name,&state))) {
        H5Epush_ret(func, H5E_ERR_CLS, H5E_IO, H5E_CANTOPENFILE, "cannot access object", NULL);
    }
    if((ncstat = nc_http_size(state,&len))) {
        H5Epush_ret(func, H5E_ERR_CLS, H5E_IO, H5E_CANTOPENFILE, "cannot access object", NULL);
    }

    /* Build the return value */
    if(NULL == (file = (H5FD_http_t *)H5allocate_memory(sizeof(H5FD_http_t),0))) {
	nc_http_close(state);
        H5Epush_ret(func, H5E_ERR_CLS, H5E_RESOURCE, H5E_NOSPACE, "memory allocation failed", NULL);
    } /* end if */
    memset(file,0,sizeof(H5FD_http_t));

    file->op = H5FD_HTTP_OP_SEEK;
    file->pos = HADDR_UNDEF;
    file->write_access = write_access;    /* Note the write_access for later */
    file->eof = (haddr_t)len;
    file->state = state; state = NULL;
    file->url = H5allocate_memory(strlen(name)+1,0);
    if(file->url == NULL) {
	nc_http_close(state);
        H5Epush_ret(func, H5E_ERR_CLS, H5E_RESOURCE, H5E_NOSPACE, "memory allocation failed", NULL);
    }
    memcpy(file->url,name,strlen(name)+1);

    return((H5FD_t*)file);
} /* end H5FD_HTTP_OPen() */


/*-------------------------------------------------------------------------
 * Function:  H5F_http_close
 *
 * Purpose:  Closes a file.
 *
 * Errors:
 *    IO    CLOSEERROR  Fclose failed.
 *
 * Return:  Non-negative on success/Negative on failure
 *
 * Programmer:  Dennis Heimbigner
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_http_close(H5FD_t *_file)
{
    H5FD_http_t  *file = (H5FD_http_t*)_file;
#if 0
    static const char *func = "H5FD_http_close";  /* Function Name for error reporting */
#endif

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    /* Close the underlying curl handle*/
    if(file->state) nc_http_close(file->state);
    if(file->url) H5free_memory(file->url);

    H5free_memory(file);

    return 0;
} /* end H5FD_http_close() */


/*-------------------------------------------------------------------------
 * Function:  H5FD_http_cmp
 *
 * Purpose:  Compares two files belonging to this driver using an
 *    arbitrary (but consistent) ordering.
 *
 * Return:
 *      Success:    A value like strcmp()
 *
 *      Failure:    never fails (arguments were checked by the caller).
 *
 * Programmer:  Robb Matzke
 *              Thursday, July 29, 1999
 *
 *-------------------------------------------------------------------------
 */
static int
H5FD_http_cmp(const H5FD_t *_f1, const H5FD_t *_f2)
{
    const H5FD_http_t  *f1 = (const H5FD_http_t*)_f1;
    const H5FD_http_t  *f2 = (const H5FD_http_t*)_f2;

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    if(strcmp(f1->url,f2->url) < 0) return -1;
    if(strcmp(f1->url,f2->url) > 0) return 1;
    return 0;
} /* H5FD_http_cmp() */


/*-------------------------------------------------------------------------
 * Function:  H5FD_http_query
 *
 * Purpose:  Set the flags that this VFL driver is capable of supporting.
 *              (listed in H5FDpublic.h)
 *
 * Return:  Success:  non-negative
 *
 *    Failure:  negative
 *
 * Programmer:  Quincey Koziol
 *              Friday, August 25, 2000
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_http_query(const H5FD_t *_f, unsigned long /*OUT*/ *flags)
{
    /* Quiet the compiler */
    _f=_f;

    /* Set the VFL feature flags that this driver supports.
     *
     * Note that this VFD does not support SWMR due to the unpredictable
     * nature of the buffering layer.
     */
    if(flags) {
        *flags = 0;
        *flags |= H5FD_FEAT_AGGREGATE_METADATA;     /* OK to aggregate metadata allocations                             */
        *flags |= H5FD_FEAT_ACCUMULATE_METADATA;    /* OK to accumulate metadata for faster writes                      */
        *flags |= H5FD_FEAT_DATA_SIEVE;             /* OK to perform data sieving for faster raw data reads & writes    */
        *flags |= H5FD_FEAT_AGGREGATE_SMALLDATA;    /* OK to aggregate "small" raw data allocations                     */
#if H5FD_CLASS_VERSION > 0
        *flags |= H5FD_FEAT_DEFAULT_VFD_COMPATIBLE; /* VFD creates a file which can be opened with the default VFD      */
#endif
    }

    return 0;
} /* end H5FD_http_query() */


/*-------------------------------------------------------------------------
 * Function:  H5FD_http_alloc
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
 * Programmer:  Raymond Lu
 *              30 March 2007
 *
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD_http_alloc(H5FD_t *_file, H5FD_mem_t /*UNUSED*/ type, hid_t /*UNUSED*/ dxpl_id, hsize_t size)
{
    H5FD_http_t    *file = (H5FD_http_t*)_file;
    haddr_t         addr;

    /* Quiet compiler */
    type = type;
    dxpl_id = dxpl_id;

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    /* Compute the address for the block to allocate */
    addr = file->eoa;

    file->eoa = addr + size;

    return addr;
} /* end H5FD_http_alloc() */


/*-------------------------------------------------------------------------
 * Function:  H5FD_http_get_eoa
 *
 * Purpose:  Gets the end-of-address marker for the file. The EOA marker
 *           is the first address past the last byte allocated in the
 *           format address space.
 *
 * Return:  Success:  The end-of-address marker.
 *
 *    Failure:  HADDR_UNDEF
 *
 * Programmer:  Robb Matzke
 *              Monday, August  2, 1999
 *
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD_http_get_eoa(const H5FD_t *_file, H5FD_mem_t /*UNUSED*/ type)
{
    const H5FD_http_t *file = (const H5FD_http_t *)_file;

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    /* Quiet compiler */
    type = type;

    return file->eoa;
} /* end H5FD_http_get_eoa() */


/*-------------------------------------------------------------------------
 * Function:  H5FD_http_set_eoa
 *
 * Purpose:  Set the end-of-address marker for the file. This function is
 *    called shortly after an existing HDF5 file is opened in order
 *    to tell the driver where the end of the HDF5 data is located.
 *
 * Return:  Success:  0
 *
 *    Failure:  Does not fail
 *
 * Programmer:  Robb Matzke
 *              Thursday, July 29, 1999
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_http_set_eoa(H5FD_t *_file, H5FD_mem_t /*UNUSED*/ type, haddr_t addr)
{
    H5FD_http_t  *file = (H5FD_http_t*)_file;

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    /* Quiet the compiler */
    type = type;

    file->eoa = addr;

    return 0;
}


/*-------------------------------------------------------------------------
 * Function:  H5FD_http_get_eof
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
 * Programmer:  Robb Matzke
 *              Thursday, July 29, 1999
 *
 *-------------------------------------------------------------------------
 */

static haddr_t
H5FD_http_get_eof(const H5FD_t *_file, H5FD_mem_t type)
{
    const H5FD_http_t  *file = (const H5FD_http_t *)_file;

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    return(file->eof);
} /* end H5FD_http_get_eof() */


/*-------------------------------------------------------------------------
 * Function:       H5FD_http_get_handle
 *
 * Purpose:        Returns the file handle of file driver.
 *
 * Returns:        Non-negative if succeed or negative if fails.
 *
 * Programmer:     Raymond Lu
 *                 Sept. 16, 2002
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_http_get_handle(H5FD_t *_file, hid_t /*UNUSED*/ fapl, void **file_handle)
{
    H5FD_http_t       *file = (H5FD_http_t *)_file;
    static const char  *func = "H5FD_http_get_handle";  /* Function Name for error reporting */

    /* Quiet the compiler */
    fapl = fapl;

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    *file_handle = file->state;
    if(*file_handle == NULL)
        H5Epush_ret(func, H5E_ERR_CLS, H5E_IO, H5E_WRITEERROR, "get handle failed", -1);

    return 0;
} /* end H5FD_http_get_handle() */


/*-------------------------------------------------------------------------
 * Function:  H5FD_http_read
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
 * Programmer:  Robb Matzke
 *    Wednesday, October 22, 1997
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_http_read(H5FD_t *_file, H5FD_mem_t /*UNUSED*/ type, hid_t /*UNUSED*/ dxpl_id,
    haddr_t addr, size_t size, void /*OUT*/ *buf)
{
    H5FD_http_t    *file = (H5FD_http_t*)_file;
    static const char *func = "H5FD_http_read";  /* Function Name for error reporting */
    int ncstat = NC_NOERR;

    /* Quiet the compiler */
    type = type;
    dxpl_id = dxpl_id;

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    /* Check for overflow */
    if (HADDR_UNDEF==addr)
        H5Epush_ret (func, H5E_ERR_CLS, H5E_IO, H5E_OVERFLOW, "file address overflowed", -1);
    if (REGION_OVERFLOW(addr, size))
        H5Epush_ret (func, H5E_ERR_CLS, H5E_IO, H5E_OVERFLOW, "file address overflowed", -1);

    /* Check easy cases */
    if (0 == size)
        return 0;
    if ((haddr_t)addr >= file->eof) {
        memset(buf, 0, size);
        return 0;
    }

    /* Seek to the correct file position. */
    if (!(file->op == H5FD_HTTP_OP_READ || file->op == H5FD_HTTP_OP_SEEK) ||
            file->pos != addr) {
#if 0
        if (file_fseek(file->fp, (file_offset_t)addr, SEEK_SET) < 0) {
            file->op = H5FD_HTTP_OP_UNKNOWN;
            file->pos = HADDR_UNDEF;
            H5Epush_ret(func, H5E_ERR_CLS, H5E_IO, H5E_SEEKERROR, "fseek failed", -1);
        }
#endif
        file->pos = addr;
    }

    /* Read zeros past the logical end of file (physical is handled below) */
    if (addr + size > file->eof) {
        size_t nbytes = (size_t) (addr + size - file->eof);
        memset((unsigned char *)buf + size - nbytes, 0, nbytes);
        size -= nbytes;
    }

    {
	NCbytes* bbuf = ncbytesnew();
        if((ncstat = nc_http_read(file->state,addr,size,bbuf))) {
            file->op = H5FD_HTTP_OP_UNKNOWN;
            file->pos = HADDR_UNDEF;
	    ncbytesfree(bbuf); bbuf = NULL;
            H5Epush_ret(func, H5E_ERR_CLS, H5E_IO, H5E_READERROR, "HTTP byte-range read failed", -1);
        } /* end if */

	/* Check that proper number of bytes was read */
	if(ncbyteslength(bbuf) != size) {
	    ncbytesfree(bbuf); bbuf = NULL;
            H5Epush_ret(func, H5E_ERR_CLS, H5E_IO, H5E_READERROR, "HTTP byte-range read mismatch ", -1);
	}	

	/* Extract the data from buf */
	memcpy(buf,ncbytescontents(bbuf),size);	        
	ncbytesfree(bbuf);
    }

    /* Update the file position data. */
    file->op = H5FD_HTTP_OP_READ;
    file->pos = addr;

    return 0;
}


/*-------------------------------------------------------------------------
 * Function:  H5FD_http_write
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
 * Programmer:  Dennis Heimbigner
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_http_write(H5FD_t *_file, H5FD_mem_t /*UNUSED*/ type, hid_t /*UNUSED*/ dxpl_id,
    haddr_t addr, size_t size, const void *buf)
{
    static const char *func = "H5FD_http_write";  /* Function Name for error reporting */

    /* Quiet the compiler */
    dxpl_id = dxpl_id;
    type = type;

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    /* Always Fails */
    H5Epush_ret (func, H5E_ERR_CLS, H5E_IO, H5E_WRITEERROR, "file is read-only", -1);

    return 0;
}


/*-------------------------------------------------------------------------
 * Function:  H5FD_http_flush
 *
 * Purpose:  Makes sure that all data is on disk.
 *
 * Errors:
 *    IO    SEEKERROR     fseek failed.
 *    IO    WRITEERROR    fflush or fwrite failed.
 *
 * Return:  Non-negative on success/Negative on failure
 *
 * Programmer:  Robb Matzke
 *    Wednesday, October 22, 1997
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_http_flush(H5FD_t *_file, hid_t dxpl_id, hbool_t closing)
{

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    return 0;
} /* end H5FD_http_flush() */


/*-------------------------------------------------------------------------
 * Function:    H5FD_http_lock
 *
 * Purpose:     Lock a file via flock
 *              NOTE: This function is a no-op if flock() is not present.
 *
 * Errors:
 *    IO    FCNTL    flock failed.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Programmer:  Vailin Choi; March 2015
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_http_lock(H5FD_t *_file, hbool_t rw)
{
    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    return 0;
} /* end H5FD_http_lock() */

/*-------------------------------------------------------------------------
 * Function:    H5F_http_unlock
 *
 * Purpose:     Unlock a file via flock
 *              NOTE: This function is a no-op if flock() is not present.
 *
 * Errors:
 *    IO    FCNTL    flock failed.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Programmer:  Vailin Choi; March 2015
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_http_unlock(H5FD_t *file)
{
    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    return 0;
} /* end H5FD_http_unlock() */


#ifdef _H5private_H
/*
 * This is not related to the functionality of the driver code.
 * It is added here to trigger warning if HDF5 private definitions are included
 * by mistake.  The code should use only HDF5 public API and definitions.
 */
#error "Do not use HDF5 private definitions"
#endif
