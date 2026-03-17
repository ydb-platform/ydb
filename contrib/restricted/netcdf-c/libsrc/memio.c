/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *	See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#include "config.h"
#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#ifdef HAVE_DIRENT_H
#include <dirent.h>
#endif
#ifdef _WIN32
#include <windows.h>
#include <winbase.h>
#include <io.h>
#endif

#include "ncdispatch.h"
#include "nc3internal.h"
#include "netcdf_mem.h"
#include "ncpathmgr.h"
#include "ncrc.h"
#include "ncbytes.h"
#include "ncutil.h"

#undef DEBUG

#ifndef HAVE_SSIZE_T
typedef int ssize_t;
#endif

#ifdef DEBUG
#include <stdio.h>
#endif

#ifndef SEEK_SET
#define SEEK_SET 0
#define SEEK_CUR 1
#define SEEK_END 2
#endif

/* Define the mode flags for create: let umask rule */
#define OPENMODE 0666

#include "ncio.h"
#include "fbits.h"
#include "rnd.h"

/* #define INSTRUMENT 1 */
#if INSTRUMENT /* debugging */
#undef NDEBUG
#include <stdio.h>
#error #include "instr.h"
#endif

#ifndef MEMIO_MAXBLOCKSIZE
#define MEMIO_MAXBLOCKSIZE 268435456 /* sanity check, about X_SIZE_T_MAX/8 */
#endif

#undef MIN  /* system may define MIN somewhere and complain */
#define MIN(mm,nn) (((mm) < (nn)) ? (mm) : (nn))

#if !defined(NDEBUG) && !defined(X_INT_MAX)
#define  X_INT_MAX 2147483647
#endif

#if 0 /* !defined(NDEBUG) && !defined(X_ALIGN) */
#define  X_ALIGN 4
#else
#undef X_ALIGN
#endif

#define REALLOCBUG
#ifdef REALLOCBUG
/* There is some kind of realloc bug that I cannot solve yet */
#define reallocx(m,new,old) realloc(m,new)
#else
static void*
reallocx(void* mem, size_t newsize, size_t oldsize)
{
    void* m = malloc(newsize);
    if(m != NULL) {
        memcpy(m,mem,oldsize);
	free(mem);
    }
    return m;
}
#endif

/* Private data for memio */

typedef struct NCMEMIO {
    int locked; /* => we cannot realloc or free*/
    int modified; /* => we realloc'd memory at least once */
    int persist; /* => save to a file; triggered by NC_PERSIST*/
    char* memory;
    size_t alloc;
    size_t size;
    size_t pos;
    /* Convenience flags */
    int diskless;
    int inmemory; /* assert(inmemory iff !diskless */
} NCMEMIO;

/* Forward */
static int memio_rel(ncio *const nciop, off_t offset, int rflags);
static int memio_get(ncio *const nciop, off_t offset, size_t extent, int rflags, void **const vpp);
static int memio_move(ncio *const nciop, off_t to, off_t from, size_t nbytes, int rflags);
static int memio_sync(ncio *const nciop);
static int memio_filesize(ncio* nciop, off_t* filesizep);
static int memio_pad_length(ncio* nciop, off_t length);
static int memio_close(ncio* nciop, int);
static int readfile(const char* path, NC_memio*);
static int writefile(const char* path, NCMEMIO*);
static int fileiswriteable(const char* path);
static int fileexists(const char* path);

/* Mnemonic */
#define DOOPEN 1

static size_t pagesize = 0;

/*! Create a new ncio struct to hold info about the file. */
static int
memio_new(const char* path, int ioflags, size_t initialsize, ncio** nciopp, NCMEMIO** memiop)
{
    int status = NC_NOERR;
    ncio* nciop = NULL;
    NCMEMIO* memio = NULL;
    size_t minsize = initialsize;

    /* Unlike netcdf-4, INMEMORY and DISKLESS share code */
    if(fIsSet(ioflags,NC_DISKLESS))
	fSet(ioflags,NC_INMEMORY);    

    /* use asserts because this is an internal function */
    assert(fIsSet(ioflags,NC_INMEMORY));
    assert(memiop != NULL && nciopp != NULL);
    assert(path != NULL);

    if(pagesize == 0) {
#if defined (_WIN32) || defined(_WIN64)
      SYSTEM_INFO info;
      GetSystemInfo (&info);
      pagesize = info.dwPageSize;
#elif defined HAVE_SYSCONF
      long pgval = -1;
      pgval = sysconf(_SC_PAGE_SIZE);
      if(pgval < 0) {
          status = NC_EIO;
          goto fail;
      }
      pagesize = (size_t)pgval;
#elif defined HAVE_GETPAGESIZE
      pagesize = (size_t)getpagesize();
#else
      pagesize = 4096; /* good guess */
#endif
    }

    errno = 0;

    /* Always force the allocated size to be a multiple of pagesize */
    if(initialsize == 0) initialsize = pagesize;
    if((initialsize % pagesize) != 0)
	initialsize += (pagesize - (initialsize % pagesize));

    nciop = (ncio* )calloc(1,sizeof(ncio));
    if(nciop == NULL) {status = NC_ENOMEM; goto fail;}

    nciop->ioflags = ioflags;
    *((int*)&nciop->fd) = -1; /* caller will fix */

    *((ncio_relfunc**)&nciop->rel) = memio_rel;
    *((ncio_getfunc**)&nciop->get) = memio_get;
    *((ncio_movefunc**)&nciop->move) = memio_move;
    *((ncio_syncfunc**)&nciop->sync) = memio_sync;
    *((ncio_filesizefunc**)&nciop->filesize) = memio_filesize;
    *((ncio_pad_lengthfunc**)&nciop->pad_length) = memio_pad_length;
    *((ncio_closefunc**)&nciop->close) = memio_close;

    memio = (NCMEMIO*)calloc(1,sizeof(NCMEMIO));
    if(memio == NULL) {status = NC_ENOMEM; goto fail;}
    *((void* *)&nciop->pvt) = memio;

    *((char**)&nciop->path) = strdup(path);
    if(nciop->path == NULL) {status = NC_ENOMEM; goto fail;}

    if(memiop && memio) *memiop = memio; else free(memio);
    if(nciopp && nciop) *nciopp = nciop;
    else {
        if(nciop->path != NULL) free((char*)nciop->path);
	/* Fix 38699 */
	nciop->path = NULL;
        free(nciop);
    }
    memio->alloc = initialsize;
    memio->pos = 0;
    memio->size = minsize;
    memio->memory = NULL; /* filled in by caller */

    if(fIsSet(ioflags,NC_DISKLESS))
	memio->diskless = 1;
    if(fIsSet(ioflags,NC_INMEMORY))
	memio->inmemory = 1;
    if(fIsSet(ioflags,NC_PERSIST))
	memio->persist = 1;

done:
    return status;

fail:
    if(memio != NULL) free(memio);
    if(nciop != NULL) {
        if(nciop->path != NULL) free((char*)nciop->path);
	/* Fix 38699 */
	nciop->path = NULL;
        free(nciop);
    }
    goto done;
}

/* Create a file, and the ncio struct to go with it.

   path - path of file to create.
   ioflags - flags from nc_create
   initialsz - From the netcdf man page: "The argument
               initialsize sets the initial size of the file at creation time."
   igeto -
   igetsz -
   sizehintp - the size of a page of data for buffered reads and writes.
   parameters - arbitrary data
   nciopp - pointer to a pointer that will get location of newly
   created and inited ncio struct.
   mempp - pointer to pointer to the initial memory read.
*/
int
memio_create(const char* path, int ioflags,
    size_t initialsz,
    off_t igeto, size_t igetsz, size_t* sizehintp,
    void* parameters /*ignored*/,
    ncio* *nciopp, void** const mempp)
{
    ncio* nciop;
    int fd;
    int status;
    NCMEMIO* memio = NULL;

    if(path == NULL ||* path == 0)
        return NC_EINVAL;
    
    status = memio_new(path, ioflags, initialsz, &nciop, &memio);
    if(status != NC_NOERR)
        return status;

    if(memio->persist) {
	/* Verify the file is writeable or does not exist*/
	if(fileexists(path) && !fileiswriteable(path))
	    {status = EPERM; goto unwind_open;}	
    }

    /* Allocate the memory for this file */
    memio->memory = (char*)malloc((size_t)memio->alloc);
    if(memio->memory == NULL) {status = NC_ENOMEM; goto unwind_open;}
    memio->locked = 0;

#ifdef DEBUG
fprintf(stderr,"memio_create: initial memory: %lu/%lu\n",(unsigned long)memio->memory,(unsigned long)memio->alloc);
#endif

    fd = nc__pseudofd();
    *((int* )&nciop->fd) = fd;

    fSet(nciop->ioflags, NC_WRITE); /* Always writeable */

    if(igetsz != 0)
    {
        status = nciop->get(nciop,
                igeto, igetsz,
                RGN_WRITE,
                mempp);
        if(status != NC_NOERR)
            goto unwind_open;
    }

    /* Pick a default sizehint */
    if(sizehintp) *sizehintp = (size_t)pagesize;

    *nciopp = nciop;
    return NC_NOERR;

unwind_open:
    memio_close(nciop,1);
    return status;
}

/* This function opens the data file or inmemory data
   path - path of data file.
   ioflags - flags passed into nc_open.
   igeto - looks like this function can do an initial page get, and
   igeto is going to be the offset for that. But it appears to be
   unused
   igetsz - the size in bytes of initial page get (a.k.a. extent). Not
   ever used in the library.
   sizehintp - the size of a page of data for buffered reads and writes.
   parameters - arbitrary data
   nciopp - pointer to pointer that will get address of newly created
   and inited ncio struct.
   mempp - pointer to pointer to the initial memory read.
*/
int
memio_open(const char* path,
    int ioflags,
    off_t igeto, size_t igetsz, size_t* sizehintp,
    void* parameters,
    ncio* *nciopp, void** const mempp)
{
    ncio* nciop = NULL;
    int fd = -1;
    int status = NC_NOERR;
    size_t sizehint = 0;
    NC_memio meminfo; /* use struct to avoid worrying about free'ing it */
    NCMEMIO* memio = NULL;
    size_t initialsize;
    /* Should be the case that diskless => inmemory but not converse */
    int diskless = (fIsSet(ioflags,NC_DISKLESS));
    int inmemory = fIsSet(ioflags,NC_INMEMORY);
    int locked = 0;

    assert(inmemory ? !diskless : 1);

    if(path == NULL || strlen(path) == 0)
        return NC_EINVAL;

    assert(sizehintp != NULL);

    sizehint = *sizehintp;

    memset(&meminfo,0,sizeof(meminfo));

    if(inmemory) { /* parameters provide the memory chunk */
	NC_memio* memparams = (NC_memio*)parameters;
        meminfo = *memparams;
        locked = fIsSet(meminfo.flags,NC_MEMIO_LOCKED);
	/* As a safeguard, if !locked and NC_WRITE is set,
           then we must take control of the incoming memory */
        if(!locked && fIsSet(ioflags,NC_WRITE)) {
	    memparams->memory = NULL;	    
	}	
    } else { /* read the file into a chunk of memory*/
	assert(diskless);
	status = readfile(path,&meminfo);
	if(status != NC_NOERR)
	    {goto unwind_open;}
    }

    /* Fix up initial size */
    initialsize = meminfo.size;

    /* create the NCMEMIO structure */
    status = memio_new(path, ioflags, initialsize, &nciop, &memio);
    if(status != NC_NOERR)
	{goto unwind_open;}
    memio->locked = locked;

    /* Initialize the memio memory */
    memio->memory = meminfo.memory;

    /* memio_new may have modified the allocated size, in which case,
       reallocate the memory unless the memory is locked. */    
    if(memio->alloc > meminfo.size) {
	if(memio->locked)
	    memio->alloc = meminfo.size; /* force it back to what it was */
	else {
	   void* oldmem = memio->memory;
	   memio->memory = reallocx(oldmem,memio->alloc,meminfo.size);
	   if(memio->memory == NULL)
	       {status = NC_ENOMEM; goto unwind_open;}
	}
    }

#ifdef DEBUG
fprintf(stderr,"memio_open: initial memory: %lu/%lu\n",(unsigned long)memio->memory,(unsigned long)memio->alloc);
#endif

    if(memio->persist) {
	/* Verify the file is writeable and exists */
	if(!fileexists(path))
	    {status = ENOENT; goto unwind_open;}	
	if(!fileiswriteable(path))
	    {status = EACCES; goto unwind_open;}	
    }

    /* Use half the filesize as the blocksize ; why? */
    sizehint = (size_t)(memio->alloc/2);

    /* sizehint must be multiple of 8 */
    sizehint = (sizehint / 8) * 8;
    if(sizehint < 8) sizehint = 8;

    fd = nc__pseudofd();
    *((int* )&nciop->fd) = fd;

    if(igetsz != 0)
    {
        status = nciop->get(nciop,
                igeto, igetsz,
                0,
                mempp);
        if(status != NC_NOERR)
            goto unwind_open;
    }

    if(sizehintp) *sizehintp = sizehint;
    if(nciopp) *nciopp = nciop; else {ncio_close(nciop,0);}
    return NC_NOERR;

unwind_open:
    if(fd >= 0)
      close(fd);
    memio_close(nciop,0);
    return status;
}

/*
 *  Get file size in bytes.
 */
static int
memio_filesize(ncio* nciop, off_t* filesizep)
{
    NCMEMIO* memio;
    if(nciop == NULL || nciop->pvt == NULL) return NC_EINVAL;
    memio = (NCMEMIO*)nciop->pvt;
    if(filesizep != NULL) *filesizep = (off_t)memio->size;
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
memio_pad_length(ncio* nciop, off_t length)
{
    NCMEMIO* memio;
    size_t len = (size_t)length;
    if(nciop == NULL || nciop->pvt == NULL) return NC_EINVAL;
    memio = (NCMEMIO*)nciop->pvt;

    if(!fIsSet(nciop->ioflags,NC_WRITE))
        return EPERM; /* attempt to write readonly file*/
    if(memio->locked)
	return NC_EINMEMORY;

    if(len > memio->alloc) {
        /* Realloc the allocated memory to a multiple of the pagesize*/
	size_t newsize = (size_t)len;
	void* newmem = NULL;
	/* Round to a multiple of pagesize */
	if((newsize % pagesize) != 0)
	    newsize += (pagesize - (newsize % pagesize));

        newmem = (char*)reallocx(memio->memory,newsize,memio->alloc);
        if(newmem == NULL) return NC_ENOMEM;
	/* If not copy is set, then fail if the newmem address is different
           from old address */
	if(newmem != memio->memory) {
	    memio->modified++;
	    if(memio->locked) {
		free(newmem);
		return NC_EINMEMORY;
	    }
        }
	/* zero out the extra memory */
        memset((void*)((char*)newmem+memio->alloc),0,(size_t)(newsize - memio->alloc));

#ifdef DEBUG
fprintf(stderr,"realloc: %lu/%lu -> %lu/%lu\n",
(unsigned long)memio->memory,(unsigned long)memio->alloc,
(unsigned long)newmem,(unsigned long)newsize);
#endif
	memio->memory = newmem;
	memio->alloc = newsize;
	memio->modified = 1;
    }
    memio->size = len;
    return NC_NOERR;
}

/*! Write out any dirty buffers to disk.

  Write out any dirty buffers to disk and ensure that next read will get data from disk.
  Sync any changes, then close the open file associated with the ncio struct, and free its memory.

  @param[in] nciop pointer to ncio to close.
  @param[in] doUnlink if true, unlink file
  @return NC_NOERR on success, error code on failure.
*/

static int
memio_close(ncio* nciop, int doUnlink)
{
    int status = NC_NOERR;
    NCMEMIO* memio ;

    if(nciop == NULL || nciop->pvt == NULL) return NC_NOERR;

    memio = (NCMEMIO*)nciop->pvt;
    assert(memio != NULL);

    /* See if the user wants the contents persisted to a file */
    if(memio->persist && memio->memory != NULL) {
	status = writefile(nciop->path,memio);		
    }

    /* We only free the memio memory if file is not locked or has been modified */
    if(memio->memory != NULL && (!memio->locked || memio->modified)) {
	free(memio->memory);
	memio->memory = NULL;
    }
    /* do cleanup  */
    if(memio != NULL) free(memio);
    if(nciop->path != NULL) free((char*)nciop->path);
    /* Fix 38699 */
    nciop->path = NULL;
    free(nciop);
    return status;
}

static int
guarantee(ncio* nciop, off_t endpoint0)
{
    NCMEMIO* memio = (NCMEMIO*)nciop->pvt;
    if(endpoint0 > memio->alloc) {
	/* extend the allocated memory and size */
	int status = memio_pad_length(nciop,endpoint0);
	if(status != NC_NOERR) return status;
    }
    if(memio->size < endpoint0)
	memio->size = (size_t)endpoint0;
    return NC_NOERR;
}

/*
 * Request that the region (offset, extent)
 * be made available through *vpp.
 */
static int
memio_get(ncio* const nciop, off_t offset, size_t extent, int rflags, void** const vpp)
{
    int status = NC_NOERR;
    NCMEMIO* memio;
    if(nciop == NULL || nciop->pvt == NULL) return NC_EINVAL;
    memio = (NCMEMIO*)nciop->pvt;
    status = guarantee(nciop, offset+(off_t)extent);
    memio->locked++;
    if(status != NC_NOERR) return status;
    if(vpp) *vpp = memio->memory+offset;
    return NC_NOERR;
}

/*
 * Like memmove(), safely move possibly overlapping data.
 */
static int
memio_move(ncio* const nciop, off_t to, off_t from, size_t nbytes, int ignored)
{
    int status = NC_NOERR;
    NCMEMIO* memio;

    if(nciop == NULL || nciop->pvt == NULL) return NC_EINVAL;
    memio = (NCMEMIO*)nciop->pvt;
    if(from < to) {
       /* extend if "to" is not currently allocated */
       status = guarantee(nciop,to+(off_t)nbytes);
       if(status != NC_NOERR) return status;
    }
    /* check for overlap */
    if((to + (off_t)nbytes) > from || (from + (off_t)nbytes) > to) {
	/* Ranges overlap */
#ifdef HAVE_MEMMOVE
        memmove((void*)(memio->memory+to),(void*)(memio->memory+from),nbytes);
#else
        off_t overlap;
	off_t nbytes1;
        if((from + nbytes) > to) {
	    overlap = ((from + nbytes) - to); /* # bytes of overlap */
	    nbytes1 = (nbytes - overlap); /* # bytes of non-overlap */
	    /* move the non-overlapping part */
            memcpy((void*)(memio->memory+(to+overlap)),
                   (void*)(memio->memory+(from+overlap)),
		   nbytes1);
	    /* move the overlapping part */
	    memcpy((void*)(memio->memory+to),
                   (void*)(memio->memory+from),
		   overlap);
	} else { /*((to + nbytes) > from) */
	    overlap = ((to + nbytes) - from); /* # bytes of overlap */
	    nbytes1 = (nbytes - overlap); /* # bytes of non-overlap */
	    /* move the non-overlapping part */
            memcpy((void*)(memio->memory+to),
                   (void*)(memio->memory+from),
		   nbytes1);
	    /* move the overlapping part */
	    memcpy((void*)(memio->memory+(to+nbytes1)),
                   (void*)(memio->memory+(from+nbytes1)),
		   overlap);
	}
#endif
    } else {/* no overlap */
	memcpy((void*)(memio->memory+to),(void*)(memio->memory+from),nbytes);
    }
    return status;
}

static int
memio_rel(ncio* const nciop, off_t offset, int rflags)
{
    NCMEMIO* memio;
    if(nciop == NULL || nciop->pvt == NULL) return NC_EINVAL;
    memio = (NCMEMIO*)nciop->pvt;
    memio->locked--;
    return NC_NOERR; /* do nothing */
}

/*
 * Write out any dirty buffers to disk and
 * ensure that next read will get data from disk.
 */
static int
memio_sync(ncio* const nciop)
{
    return NC_NOERR; /* do nothing */
}

/* "Hidden" Internal function to extract the 
   the size and/or contents of the memory.
*/
int
memio_extract(ncio* const nciop, size_t* sizep, void** memoryp)
{
    int status = NC_NOERR;
    NCMEMIO* memio = NULL;

    if(nciop == NULL || nciop->pvt == NULL) return NC_NOERR;
    memio = (NCMEMIO*)nciop->pvt;
    assert(memio != NULL);
    if(sizep) *sizep = memio->size;

    if(memoryp && memio->memory != NULL) {
	*memoryp = memio->memory;
	memio->memory = NULL; /* make sure it does not get free'd */
    }
    return status;
}

/* Return 1 if file exists, 0 otherwise */
static int
fileexists(const char* path)
{
    int ok;
    /* See if the file exists at all */
    ok = NCaccess(path,ACCESS_MODE_EXISTS);
    if(ok < 0) /* file does not exist */
      return 0;
    return 1;
}

/* Return 1 if file is writeable, return 0 otherwise;
   assumes fileexists has been checked already */
static int
fileiswriteable(const char* path)
{
    int ok;
    /* if W is ok */
    ok = NCaccess(path,ACCESS_MODE_W);
    if(ok < 0)
	return 0;
    return 1;
}

#if 0 /* not used */
/* Return 1 if file is READABLE, return 0 otherwise;
   assumes fileexists has been checked already */
static int
fileisreadable(const char* path)
{
    int ok;
    /* if RW is ok */
    ok = NCaccess(path,ACCESS_MODE_R);
    if(ok < 0)
	return 0;
    return 1;
}
#endif

/* Read contents of a disk file into a memory chunk */
static int
readfile(const char* path, NC_memio* memio)
{
    int status = NC_NOERR;
    FILE* f = NULL;
    NCbytes* buf = ncbytesnew();

    if((status = NC_readfile(path,buf))) goto done;
    if(memio) {
	memio->size = ncbyteslength(buf);
	memio->memory = ncbytesextract(buf);
    }

done:
    ncbytesfree(buf);
    if(f != NULL) fclose(f);
    return status;    
}

/* write contents of a memory chunk back into a disk file */
static int
writefile(const char* path, NCMEMIO* memio)
{
    int status = NC_NOERR;

    if(memio) {
        if((status = NC_writefile(path,memio->size,memio->memory))) goto done;
    }
done:
    return status;    
}
