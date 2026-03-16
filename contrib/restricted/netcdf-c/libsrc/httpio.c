/*********************************************************************
*    Copyright 2018, UCAR/Unidata
*    See netcdf/COPYRIGHT file for copying and redistribution conditions.
* ********************************************************************/

#if HAVE_CONFIG_H
#include <config.h>
#endif

#include <assert.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#ifdef _MSC_VER /* Microsoft Compilers */
#include <io.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include "nc3internal.h"
#include "nclist.h"
#include "ncbytes.h"
#include "ncuri.h"

#undef DEBUG

#ifdef DEBUG
#include <stdio.h>
#endif

#include <curl/curl.h>

#include "ncio.h"
#include "fbits.h"
#include "rnd.h"
#include "ncbytes.h"
#include "nchttp.h"

#define DEFAULTPAGESIZE 16384

/* Private data */

typedef struct NCHTTP {
    NC_HTTP_STATE* state;
    long long size; /* of the object */
    NCbytes* interval;
    int verbose;
} NCHTTP;

/* Forward */
static int httpio_rel(ncio *const nciop, off_t offset, int rflags);
static int httpio_get(ncio *const nciop, off_t offset, size_t extent, int rflags, void **const vpp);
static int httpio_move(ncio *const nciop, off_t to, off_t from, size_t nbytes, int rflags);
static int httpio_sync(ncio *const nciop);
static int httpio_filesize(ncio* nciop, off_t* filesizep);
static int httpio_pad_length(ncio* nciop, off_t length);
static int httpio_close(ncio* nciop, int);

static size_t pagesize = 0;

/* Create a new ncio struct to hold info about the file. */
static int
httpio_new(const char* path, int ioflags, ncio** nciopp, NCHTTP** hpp)
{
    int status = NC_NOERR;
    ncio* nciop = NULL;
    NCHTTP* http = NULL;

    if(pagesize == 0)
        pagesize = DEFAULTPAGESIZE;
    errno = 0;

    nciop = (ncio* )calloc(1,sizeof(ncio));
    if(nciop == NULL) {status = NC_ENOMEM; goto fail;}
    
    nciop->ioflags = ioflags;

    *((char**)&nciop->path) = strdup(path);
    if(nciop->path == NULL) {status = NC_ENOMEM; goto fail;}

    *((ncio_relfunc**)&nciop->rel) = httpio_rel;
    *((ncio_getfunc**)&nciop->get) = httpio_get;
    *((ncio_movefunc**)&nciop->move) = httpio_move;
    *((ncio_syncfunc**)&nciop->sync) = httpio_sync;
    *((ncio_filesizefunc**)&nciop->filesize) = httpio_filesize;
    *((ncio_pad_lengthfunc**)&nciop->pad_length) = httpio_pad_length;
    *((ncio_closefunc**)&nciop->close) = httpio_close;

    http = (NCHTTP*)calloc(1,sizeof(NCHTTP));
    if(http == NULL) {status = NC_ENOMEM; goto fail;}
    *((void* *)&nciop->pvt) = http;

    http->verbose = (getenv("CURLOPT_VERBOSE") == NULL ? 0 : 1);

    if(nciopp) *nciopp = nciop;
    if(hpp) *hpp = http;

done:
    return status;

fail:
    if(http != NULL) {
	if(http->interval)
	    ncbytesfree(http->interval);
	free(http);
    }
    if(nciop != NULL) {
        if(nciop->path != NULL) free((char*)nciop->path);
    }
    goto done;
}

/* Create a file, and the ncio struct to go with it. This function is
   only called from nc__create_mp.

   path - path of file to create.
   ioflags - flags from nc_create
   initialsz - From the netcdf man page: "The argument
   Iinitialsize sets the initial size of the file at creation time."
   igeto - 
   igetsz - 
   sizehintp - the size of a page of data for buffered reads and writes.
   nciopp - pointer to a pointer that will get location of newly
   created and inited ncio struct.
   mempp - pointer to pointer to the initial memory read.
*/
int
httpio_create(const char* path, int ioflags,
    size_t initialsz,
    off_t igeto, size_t igetsz, size_t* sizehintp,
    void* parameters,
    ncio* *nciopp, void** const mempp)
{
    return NC_EPERM;
}

/* This function opens the data file. It is only called from nc.c,
   from nc__open_mp and nc_delete_mp.

   path - path of data file.
   ioflags - flags passed into nc_open.
   igeto - looks like this function can do an initial page get, and
   igeto is going to be the offset for that. But it appears to be
   unused 
   igetsz - the size in bytes of initial page get (a.k.a. extent). Not
   ever used in the library.
   sizehintp - the size of a page of data for buffered reads and writes.
   nciopp - pointer to pointer that will get address of newly created
   and inited ncio struct.
   mempp - pointer to pointer to the initial memory read.
*/
int
httpio_open(const char* path,
    int ioflags,
    /* ignored */ off_t igeto, size_t igetsz, size_t* sizehintp,
    /* ignored */ void* parameters,
    ncio* *nciopp,
    /* ignored */ void** const mempp)
{
    ncio* nciop;
    int status;
    NCHTTP* http = NULL;
    size_t sizehint;
    NCURI* uri = NULL;

    if(path == NULL ||* path == 0)
        return EINVAL;

    ncuriparse(path,&uri);
    if(uri == NULL) {status = NC_EURL; goto done;}

    /* Create private data */
    if((status = httpio_new(path, ioflags, &nciop, &http))) goto done;
    /* Open the path and get curl handle and object size */
    if((status = nc_http_open_verbose(path,http->verbose,&http->state))) goto done;
    if((status = nc_http_size(http->state,&http->size))) goto done;

    sizehint = pagesize;

    /* sizehint must be multiple of 8 */
    sizehint = (sizehint / 8) * 8;
    if(sizehint < 8) sizehint = 8;

    *sizehintp = sizehint;
    *nciopp = nciop;
done:
    if(status)
        httpio_close(nciop,0);
    return status;
}

/* 
 *  Get file size in bytes.
 */
static int
httpio_filesize(ncio* nciop, off_t* filesizep)
{
    NCHTTP* http;
    if(nciop == NULL || nciop->pvt == NULL) return NC_EINVAL;
    http = (NCHTTP*)nciop->pvt;
    if(filesizep != NULL) *filesizep = http->size;
    return NC_NOERR;
}

/*
 *  Sync any changes to disk, then truncate or extend file so its size
 *  is length.  This is only intended to be called before close, if the
 *  file is open for writing and the actual size does not match the
 *  calculated size, perhaps as the result of having been previously
 *  written in NOFILL mode.
 */
static int
httpio_pad_length(ncio* nciop, off_t length)
{
    return NC_NOERR; /* do nothing */
}

/* Write out any dirty buffers to disk and
   ensure that next read will get data from disk.
   Sync any changes, then close the open file associated with the ncio
   struct, and free its memory.
   nciop - pointer to ncio to close.
   doUnlink - if true, unlink file
*/

static int 
httpio_close(ncio* nciop, int doUnlink)
{
    int status = NC_NOERR;
    NCHTTP* http;
    if(nciop == NULL || nciop->pvt == NULL) return NC_NOERR;

    http = (NCHTTP*)nciop->pvt;
    assert(http != NULL);

    status = nc_http_close(http->state);

    /* do cleanup  */
    if(http != NULL) {
	ncbytesfree(http->interval);
	free(http);
    }
    if(nciop->path != NULL) free((char*)nciop->path);
    free(nciop);
    return status;
}

/*
 * Request that the interval (offset, extent)
 * be made available through *vpp.
 */
static int
httpio_get(ncio* const nciop, off_t offset, size_t extent, int rflags, void** const vpp)
{
    int status = NC_NOERR;
    NCHTTP* http;

    if(nciop == NULL || nciop->pvt == NULL) {status = NC_EINVAL; goto done;}
    http = (NCHTTP*)nciop->pvt;

    assert(http->interval == NULL);
    http->interval = ncbytesnew();
    ncbytessetalloc(http->interval,(unsigned long)extent);
    if((status = nc_http_read(http->state,(size64_t)offset,extent,http->interval)))
	goto done;
    assert(ncbyteslength(http->interval) == extent);
    if(vpp) *vpp = ncbytescontents(http->interval);
done:
    return status;
}

/*
 * Like memmove(), safely move possibly overlapping data.
 */
static int
httpio_move(ncio* const nciop, off_t to, off_t from, size_t nbytes, int ignored)
{
    return NC_EPERM;
}

static int
httpio_rel(ncio* const nciop, off_t offset, int rflags)
{
    int status = NC_NOERR;
    NCHTTP* http;

    if(nciop == NULL || nciop->pvt == NULL) {status = NC_EINVAL; goto done;}
    http = (NCHTTP*)nciop->pvt;
    ncbytesfree(http->interval);
    http->interval = NULL;
done:
    return status;
}

/*
 * Write out any dirty buffers to disk and
 * ensure that next read will get data from disk.
 */
static int
httpio_sync(ncio* const nciop)
{
    return NC_NOERR; /* do nothing */
}
