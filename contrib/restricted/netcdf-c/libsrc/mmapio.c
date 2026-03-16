/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *	See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

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
#include "ncpathmgr.h"

#undef DEBUG

#ifdef DEBUG
#include <stdio.h>
#endif

#include <sys/mman.h>

#ifndef MAP_ANONYMOUS
#  ifdef MAP_ANON
#    define MAP_ANONYMOUS MAP_ANON
#  endif
#endif

/* !MAP_ANONYMOUS => !HAVE_MMAP */
#ifndef MAP_ANONYMOUS
#error mmap not fully implemented: missing MAP_ANONYMOUS
#endif

#ifdef HAVE_MREMAP
# ifndef MREMAP_MAYMOVE
#   define MREMAP_MAYMOVE 1
# endif
#endif /*HAVE_MREMAP*/

#ifndef SEEK_SET
#define SEEK_SET 0
#define SEEK_CUR 1
#define SEEK_END 2
#endif

/* Define the mode flags for create: let umask decide */
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

#ifndef MMAP_MAXBLOCKSIZE
#define MMAP_MAXBLOCKSIZE 268435456 /* sanity check, about X_SIZE_T_MAX/8 */
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

/* Private data for mmap */

typedef struct NCMMAPIO {
    int locked; /* => we cannot realloc */
    int persist; /* => save to a file; triggered by NC_PERSIST */
    char* memory;
    size_t alloc;
    off_t size;
    off_t pos;
    int mapfd;
} NCMMAPIO;

/* Forward */
static int mmapio_rel(ncio *const nciop, off_t offset, int rflags);
static int mmapio_get(ncio *const nciop, off_t offset, size_t extent, int rflags, void **const vpp);
static int mmapio_move(ncio *const nciop, off_t to, off_t from, size_t nbytes, int rflags);
static int mmapio_sync(ncio *const nciop);
static int mmapio_filesize(ncio* nciop, off_t* filesizep);
static int mmapio_pad_length(ncio* nciop, off_t length);
static int mmapio_close(ncio* nciop, int);

/* Mnemonic */
#define DOOPEN 1

static size_t pagesize = 0;

/* Create a new ncio struct to hold info about the file. */
static int
mmapio_new(const char* path, int ioflags, size_t initialsize, ncio** nciopp, NCMMAPIO** mmapp)
{
    int status = NC_NOERR;
    ncio* nciop = NULL;
    NCMMAPIO* mmapio = NULL;
    int openfd = -1;

    if(pagesize == 0) {
#if defined HAVE_SYSCONF
        pagesize = (size_t)sysconf(_SC_PAGE_SIZE);
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

    *((char**)&nciop->path) = strdup(path);
    if(nciop->path == NULL) {status = NC_ENOMEM; goto fail;}

    *((ncio_relfunc**)&nciop->rel) = mmapio_rel;
    *((ncio_getfunc**)&nciop->get) = mmapio_get;
    *((ncio_movefunc**)&nciop->move) = mmapio_move;
    *((ncio_syncfunc**)&nciop->sync) = mmapio_sync;
    *((ncio_filesizefunc**)&nciop->filesize) = mmapio_filesize;
    *((ncio_pad_lengthfunc**)&nciop->pad_length) = mmapio_pad_length;
    *((ncio_closefunc**)&nciop->close) = mmapio_close;

    mmapio = (NCMMAPIO*)calloc(1,sizeof(NCMMAPIO));
    if(mmapio == NULL) {status = NC_ENOMEM; goto fail;}
    *((void* *)&nciop->pvt) = mmapio;

    mmapio->alloc = initialsize;

    mmapio->memory = NULL;
    mmapio->size = 0;
    mmapio->pos = 0;
    mmapio->persist = fIsSet(ioflags,NC_PERSIST);

    /* See if ok to use mmap */
    if(sizeof(void*) < 8 &&
       (fIsSet(ioflags,NC_64BIT_OFFSET) || fIsSet(ioflags,NC_64BIT_DATA)))
	return NC_DISKLESS; /* cannot support */
    mmapio->mapfd = -1;

    if(nciopp) *nciopp = nciop;
    if(mmapp) *mmapp = mmapio;

done:
    if(openfd >= 0) close(openfd);
    return status;

fail:
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
mmapio_create(const char* path, int ioflags,
    size_t initialsz,
    off_t igeto, size_t igetsz, size_t* sizehintp,
    void* parameters,
    ncio* *nciopp, void** const mempp)
{
    ncio* nciop;
    int fd;
    int status;
    NCMMAPIO* mmapio = NULL;
    int persist = (ioflags & NC_PERSIST?1:0);
    int oflags;

    if(path == NULL ||* path == 0)
        return NC_EINVAL;

    /* For diskless open has, the file must be classic version 1 or 2.*/
    if(fIsSet(ioflags,NC_NETCDF4))
        return NC_EDISKLESS; /* violates constraints */

    status = mmapio_new(path, ioflags, initialsz, &nciop, &mmapio);
    if(status != NC_NOERR)
        return status;
    mmapio->size = 0;

    if(!persist) {
        mmapio->mapfd = -1;
	mmapio->memory = (char*)mmap(NULL,mmapio->alloc,
                                    PROT_READ|PROT_WRITE,
				    MAP_PRIVATE|MAP_ANONYMOUS,
                                    mmapio->mapfd,0);
	{mmapio->memory[0] = 0;} /* test writing of the mmap'd memory */
    } else { /*persist */
        /* Open the file to get fd,  but make sure we can write it if needed */
        oflags = O_RDWR;
#ifdef O_BINARY
        fSet(oflags, O_BINARY);
#endif
    	oflags |= (O_CREAT|O_TRUNC);
        if(fIsSet(ioflags,NC_NOCLOBBER))
	    oflags |= O_EXCL;
        fd  = NCopen3(path, oflags, OPENMODE);
        if(fd < 0) {status = errno; goto unwind_open;}
	mmapio->mapfd = fd;

        { /* Cause the output file to have enough allocated space */
            lseek(fd,(off_t)mmapio->alloc-1,SEEK_SET); /* cause file to appear */
            write(fd,"",1);
            lseek(fd,0,SEEK_SET); /* rewind */
        }
        mmapio->memory = (char*)mmap(NULL,mmapio->alloc,
                                    PROT_READ|PROT_WRITE,
				    MAP_SHARED,
                                    mmapio->mapfd,0);
	if(mmapio->memory == NULL) {
	    return NC_EDISKLESS;
	}
    } /*!persist*/

#ifdef DEBUG
fprintf(stderr,"mmap_create: initial memory: %lu/%lu\n",(unsigned long)mmapio->memory,(unsigned long)mmapio->alloc);
#endif

    fd = nc__pseudofd();
    *((int* )&nciop->fd) = fd; 

    fSet(nciop->ioflags, NC_WRITE);

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
    if(sizehintp) *sizehintp = pagesize;

    *nciopp = nciop;
    return NC_NOERR;

unwind_open:
    mmapio_close(nciop,1);
    return status;
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
mmapio_open(const char* path,
    int ioflags,
    off_t igeto, size_t igetsz, size_t* sizehintp,
    void* parameters,
    ncio* *nciopp, void** const mempp)
{
    ncio* nciop = NULL;
    int fd;
    int status;
    int oflags;
    NCMMAPIO* mmapio = NULL;
    size_t sizehint;
    off_t filesize;
    int readwrite = (fIsSet(ioflags,NC_WRITE)?1:0);

    if(path == NULL ||* path == 0)
        return EINVAL;

    assert(sizehintp != NULL);
    sizehint = *sizehintp;

    /* Open the file, but make sure we can write it if needed */
    oflags = (readwrite ? O_RDWR : O_RDONLY);    
#ifdef O_BINARY
    fSet(oflags, O_BINARY);
#endif
    oflags |= O_EXCL;
    fd  = NCopen3(path, oflags, OPENMODE);
    if(fd < 0) {status = errno; goto unwind_open;}

    /* get current filesize  = max(|file|,initialize)*/
    filesize = lseek(fd,0,SEEK_END);
    if(filesize < 0) {status = errno; goto unwind_open;}
    /* move pointer back to beginning of file */
    (void)lseek(fd,0,SEEK_SET);
    if(filesize < (off_t)sizehint)
        filesize = (off_t)sizehint;

    status = mmapio_new(path, ioflags, (size_t)filesize, &nciop, &mmapio);
    if(status != NC_NOERR)
	return status;
    mmapio->size = filesize;

    mmapio->mapfd = fd;
    mmapio->memory = (char*)mmap(NULL,mmapio->alloc,
                                    readwrite?(PROT_READ|PROT_WRITE):(PROT_READ),
				    MAP_SHARED,
                                    mmapio->mapfd,0);
#ifdef DEBUG
fprintf(stderr,"mmapio_open: initial memory: %lu/%lu\n",(unsigned long)mmapio->memory,(unsigned long)mmapio->alloc);
#endif

    /* Use half the filesize as the blocksize */
    sizehint = (size_t)filesize/2;

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

    *sizehintp = sizehint;
    *nciopp = nciop;
    return NC_NOERR;

unwind_open:
    mmapio_close(nciop,0);
    return status;
}


/* 
 *  Get file size in bytes.
 */
static int
mmapio_filesize(ncio* nciop, off_t* filesizep)
{
    NCMMAPIO* mmapio;
    if(nciop == NULL || nciop->pvt == NULL) return NC_EINVAL;
    mmapio = (NCMMAPIO*)nciop->pvt;
    if(filesizep != NULL) *filesizep = mmapio->size;
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
mmapio_pad_length(ncio* nciop, off_t length)
{
    NCMMAPIO* mmapio;

    if(nciop == NULL || nciop->pvt == NULL) return NC_EINVAL;
    mmapio = (NCMMAPIO*)nciop->pvt;

    if(!fIsSet(nciop->ioflags, NC_WRITE))
        return EPERM; /* attempt to write readonly file*/

    if(mmapio->locked > 0)
	return NC_EDISKLESS;

    if(length > mmapio->alloc) {
        /* Realloc the allocated memory to a multiple of the pagesize*/
	size_t newsize = (size_t)length;
	void* newmem = NULL;
	/* Round to a multiple of pagesize */
	if((newsize % pagesize) != 0)
	    newsize += (pagesize - (newsize % pagesize));

	/* Force file size to be properly extended */
	{ /* Cause the output file to have enough allocated space */
	off_t pos = lseek(mmapio->mapfd,0,SEEK_CUR); /* save current position*/
	/* cause file to be extended in size */
	lseek(mmapio->mapfd,(off_t)newsize-1,SEEK_SET);
        write(mmapio->mapfd,"",mmapio->alloc);
	lseek(mmapio->mapfd,pos,SEEK_SET); /* reset position */
	}

#ifdef HAVE_MREMAP
	newmem = (char*)mremap(mmapio->memory,mmapio->alloc,newsize,MREMAP_MAYMOVE);
	if(newmem == NULL) return NC_ENOMEM;
#else
        /* note: mmapio->mapfd >= 0 => persist */
        newmem = (char*)mmap(NULL,newsize,
                                    mmapio->mapfd >= 0?(PROT_READ|PROT_WRITE):(PROT_READ),
				    MAP_SHARED,
                                    mmapio->mapfd,0);
	if(newmem == NULL) return NC_ENOMEM;
	memcpy(newmem,mmapio->memory,mmapio->alloc);
        munmap(mmapio->memory,mmapio->alloc);
#endif

#ifdef DEBUG
fprintf(stderr,"realloc: %lu/%lu -> %lu/%lu\n",
(unsigned long)mmapio->memory,(unsigned long)mmapio->alloc,
(unsigned long)newmem,(unsigned long)newsize);
#endif
	mmapio->memory = newmem;
	mmapio->alloc = newsize;
    }  
    mmapio->size = length;
    return NC_NOERR;
}

/* Write out any dirty buffers to disk and
   ensure that next read will get data from disk.
   Sync any changes, then close the open file associated with the ncio
   struct, and free its memory.
   nciop - pointer to ncio to close.
   doUnlink - if true, unlink file
*/

static int 
mmapio_close(ncio* nciop, int doUnlink)
{
    int status = NC_NOERR;
    NCMMAPIO* mmapio;
    if(nciop == NULL || nciop->pvt == NULL) return NC_NOERR;

    mmapio = (NCMMAPIO*)nciop->pvt;
    assert(mmapio != NULL);

    /* Since we are using mmap, persisting to a file should be automatic */
    status = munmap(mmapio->memory,mmapio->alloc);
    mmapio->memory = NULL; /* so we do not try to free it */

    /* Close file if it was open */
    if(mmapio->mapfd >= 0)
	close(mmapio->mapfd);

    /* do cleanup  */
    if(mmapio != NULL) free(mmapio);
    if(nciop->path != NULL) free((char*)nciop->path);
    free(nciop);
    return status;
}

static int
guarantee(ncio* nciop, off_t endpoint)
{
    NCMMAPIO* mmapio = (NCMMAPIO*)nciop->pvt;
    if(endpoint > mmapio->alloc) {
	/* extend the allocated memory and size */
	int status = mmapio_pad_length(nciop,endpoint);
	if(status != NC_NOERR) return status;
    }
    if(mmapio->size < endpoint)
	mmapio->size = endpoint;
    return NC_NOERR;
}

/*
 * Request that the region (offset, extent)
 * be made available through *vpp.
 */
static int
mmapio_get(ncio* const nciop, off_t offset, size_t extent, int rflags, void** const vpp)
{
    int status = NC_NOERR;
    NCMMAPIO* mmapio;
    if(nciop == NULL || nciop->pvt == NULL) return NC_EINVAL;
    mmapio = (NCMMAPIO*)nciop->pvt;
    status = guarantee(nciop, offset+(off_t)extent);
    mmapio->locked++;
    if(status != NC_NOERR) return status;
    if(vpp) *vpp = mmapio->memory+offset;
    return NC_NOERR;
}

/*
 * Like memmove(), safely move possibly overlapping data.
 */
static int
mmapio_move(ncio* const nciop, off_t to, off_t from, size_t nbytes, int ignored)
{
    int status = NC_NOERR;
    NCMMAPIO* mmapio;

    if(nciop == NULL || nciop->pvt == NULL) return NC_EINVAL;
    mmapio = (NCMMAPIO*)nciop->pvt;
    if(from < to) {
       /* extend if "to" is not currently allocated */
       status = guarantee(nciop, to + (off_t)nbytes);
       if(status != NC_NOERR) return status;
    }
    /* check for overlap */
    if((to + (off_t)nbytes) > from || (from + (off_t)nbytes) > to) {
	/* Ranges overlap */
#ifdef HAVE_MEMMOVE
        memmove((void*)(mmapio->memory+to),(void*)(mmapio->memory+from),nbytes);
#else
        off_t overlap;
	off_t nbytes1;
        if((from + nbytes) > to) {
	    overlap = ((from + nbytes) - to); /* # bytes of overlap */
	    nbytes1 = (nbytes - overlap); /* # bytes of non-overlap */
	    /* move the non-overlapping part */
            memcpy((void*)(mmapio->memory+(to+overlap)),
                   (void*)(mmapio->memory+(from+overlap)),
		   nbytes1);
	    /* move the overlapping part */
	    memcpy((void*)(mmapio->memory+to),
                   (void*)(mmapio->memory+from),
		   overlap);
	} else { /*((to + nbytes) > from) */
	    overlap = ((to + nbytes) - from); /* # bytes of overlap */
	    nbytes1 = (nbytes - overlap); /* # bytes of non-overlap */
	    /* move the non-overlapping part */
            memcpy((void*)(mmapio->memory+to),
                   (void*)(mmapio->memory+from),
		   nbytes1);
	    /* move the overlapping part */
	    memcpy((void*)(mmapio->memory+(to+nbytes1)),
                   (void*)(mmapio->memory+(from+nbytes1)),
		   overlap);
	}
#endif
    } else {/* no overlap */
	memcpy((void*)(mmapio->memory+to),(void*)(mmapio->memory+from),nbytes);
    }
    return status;
}

static int
mmapio_rel(ncio* const nciop, off_t offset, int rflags)
{
    NCMMAPIO* mmapio;
    if(nciop == NULL || nciop->pvt == NULL) return NC_EINVAL;
    mmapio = (NCMMAPIO*)nciop->pvt;
    mmapio->locked--;
    return NC_NOERR; /* do nothing */
}

/*
 * Write out any dirty buffers to disk and
 * ensure that next read will get data from disk.
 */
static int
mmapio_sync(ncio* const nciop)
{
    return NC_NOERR; /* do nothing */
}
