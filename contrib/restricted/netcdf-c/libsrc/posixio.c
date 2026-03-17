/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *	See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */
/* $Id: posixio.c,v 1.89 2010/05/22 21:59:08 dmh Exp $ */

/* For MinGW Build */

#if HAVE_CONFIG_H
#include <config.h>
#endif

#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <stdint.h>

#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif

/* Windows platforms, including MinGW, Cygwin, Visual Studio */
#if defined(_WIN32) || defined(_WIN64)
#include <windows.h>
#include <winbase.h>
#include <io.h>
#endif

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#ifndef NC_NOERR
#define NC_NOERR 0
#endif

#ifndef SEEK_SET
#define SEEK_SET 0
#define SEEK_CUR 1
#define SEEK_END 2
#endif

#include "ncpathmgr.h"
#include "ncio.h"
#include "fbits.h"
#include "rnd.h"

/* #define INSTRUMENT 1 */
#if INSTRUMENT /* debugging */
#undef NDEBUG
#include <stdio.h>
#error #include "instr.h"
#endif

#undef MIN  /* system may define MIN somewhere and complain */
#define MIN(mm,nn) (((mm) < (nn)) ? (mm) : (nn))

#if /*!defined(NDEBUG) &&*/ !defined(X_INT_MAX)
#define  X_INT_MAX 2147483647
#endif

#if 0 /* !defined(NDEBUG) && !defined(X_ALIGN) */
#define  X_ALIGN 4
#else
#undef X_ALIGN
#endif

/* These are needed on mingw to get a dll to compile. They really
 * should be provided in sys/stats.h, but what the heck. Let's not be
 * too picky! */
#ifndef S_IRGRP
#define S_IRGRP   0000040
#endif
#ifndef S_IROTH
#define S_IROTH   0000004
#endif
#ifndef S_IWGRP
#define S_IWGRP   0000020
#endif
#ifndef S_IWOTH
#define S_IWOTH   0000002
#endif

/*Forward*/
static int ncio_px_filesize(ncio *nciop, off_t *filesizep);
static int ncio_px_pad_length(ncio *nciop, off_t length);
static int ncio_px_close(ncio *nciop, int doUnlink);
static int ncio_spx_close(ncio *nciop, int doUnlink);


/*
 * Define the following for debugging.
 */
/* #define ALWAYS_NC_SHARE 1 */

/* Begin OS */

#ifndef POSIXIO_DEFAULT_PAGESIZE
#define POSIXIO_DEFAULT_PAGESIZE 4096
#endif

/*! Cross-platform file length.
 *
 * Some versions of Visual Studio are throwing errno 132
 * when fstat is used on large files.  This function is
 * an attempt to get around that.
 *
 * @par fd File Descriptor.
 * @return -1 on error, length of file (in bytes) otherwise.
 */
static off_t nc_get_filelen(const int fd) {

  off_t flen;

#ifdef HAVE_FILE_LENGTH_I64
  int64_t file_len = 0;
  if ((file_len = _filelengthi64(fd)) < 0) {
    return file_len;
  }
  flen = (off_t)file_len;

#else
  int res = 0;
  struct stat sb;
  if((res = fstat(fd,&sb)) <0)
    return res;

  flen = sb.st_size;
#endif

  return flen;

}


/*
 * What is the system pagesize?
 */
static size_t
pagesize(void)
{
  size_t pgsz;
#if defined(_WIN32) || defined(_WIN64)
  SYSTEM_INFO info;
#endif
/* Hmm, aren't standards great? */
#if defined(_SC_PAGE_SIZE) && !defined(_SC_PAGESIZE)
#define _SC_PAGESIZE _SC_PAGE_SIZE
#endif

  /* For MinGW Builds */
#if defined(_WIN32) || defined(_WIN64)
  GetSystemInfo(&info);
  pgsz = (size_t)info.dwPageSize;
#elif defined(_SC_PAGESIZE)
  pgsz = (size_t)sysconf(_SC_PAGESIZE);
#elif defined(HAVE_GETPAGESIZE)
  pgsz = (size_t) getpagesize();
#endif
  if(pgsz > 0)
    return (size_t) pgsz;
   return (size_t)POSIXIO_DEFAULT_PAGESIZE;
}

/*
 * What is the preferred I/O block size?
 */
static size_t
blksize(int fd)
{
#ifdef HAVE_STRUCT_STAT_ST_BLKSIZE
#ifdef HAVE_SYS_STAT_H
	struct stat sb;
	if (fstat(fd, &sb) > -1)
	{
		if(sb.st_blksize >= 8192)
			return (size_t) sb.st_blksize;
		return 8192;
	}
	/* else, silent in the face of error */
#else
	NC_UNUSED(fd);
#endif
#else
	NC_UNUSED(fd);
#endif
	return (size_t) 2 * pagesize();
}


/*
 * Sortof like ftruncate, except won't make the
 * file shorter.
 */
static int
fgrow(const int fd, const off_t len)
{
	struct stat sb;
	if (fstat(fd, &sb) < 0)
		return errno;
	if (len < sb.st_size)
		return NC_NOERR;
	{
	    const long dumb = 0;
	    /* we don't use ftruncate() due to problem with FAT32 file systems */
	    /* cache current position */
	    const off_t pos = lseek(fd, 0, SEEK_CUR);
	    if(pos < 0)
		return errno;
	    if (lseek(fd, len-(off_t)sizeof(dumb), SEEK_SET) < 0)
		return errno;
	    if(write(fd, &dumb, sizeof(dumb)) < 0)
		return errno;
	    if (lseek(fd, pos, SEEK_SET) < 0)
		return errno;
	}
	return NC_NOERR;
}


/*
 * Sortof like ftruncate, except won't make the file shorter.  Differs
 * from fgrow by only writing one byte at designated seek position, if
 * needed.
 */
static int
fgrow2(const int fd, const off_t len)
{


  /* There is a problem with fstat on Windows based systems
     which manifests (so far) when Config RELEASE is built.
     Use _filelengthi64 instead.

     See https://github.com/Unidata/netcdf-c/issues/188

  */


  off_t file_len = nc_get_filelen(fd);
  if(file_len < 0) return errno;
  if(len <= file_len)
    return NC_NOERR;
  {
    const char dumb = 0;
	    /* we don't use ftruncate() due to problem with FAT32 file systems */
	    /* cache current position */
	    const off_t pos = lseek(fd, 0, SEEK_CUR);
	    if(pos < 0)
		return errno;
	    if (lseek(fd, len-1, SEEK_SET) < 0)
		return errno;
	    if(write(fd, &dumb, sizeof(dumb)) < 0)
		return errno;
	    if (lseek(fd, pos, SEEK_SET) < 0)
		return errno;
	}
	return NC_NOERR;
}
/* End OS */
/* Begin px */

/* The px_ functions are for posix systems, when NC_SHARE is not in
   effect. */

/* Write out a "page" of data to the file. The size of the page
   (i.e. the extent) varies.

   nciop - pointer to the file metadata.
   offset - where in the file should this page be written.
   extent - how many bytes should be written.
   vp - pointer to the data to write.
   posp - pointer to current position in file, updated after write.
*/
static int
px_pgout(ncio *const nciop,
	off_t const offset,  const size_t extent,
	void *const vp, off_t *posp)
{
    ssize_t partial;
    size_t nextent;
    char *nvp;
#ifdef X_ALIGN
	assert(offset % X_ALIGN == 0);
#endif

	assert(*posp == OFF_NONE || *posp == lseek(nciop->fd, 0, SEEK_CUR));

	if(*posp != offset)
	{
		if(lseek(nciop->fd, offset, SEEK_SET) != offset)
		{
			return errno;
		}
		*posp = offset;
	}
	/* Old write, didn't handle partial writes correctly */
	/* if(write(nciop->fd, vp, extent) != (ssize_t) extent) */
	/* { */
	/* 	return errno; */
	/* } */
	nextent = extent;
        nvp = vp;
	while((partial = write(nciop->fd, nvp, nextent)) != -1) {
	    if(partial == nextent)
		break;
	    nvp += partial;
	    nextent -= (size_t)partial;
	}
	if(partial == -1)
	    return errno;
	*posp += (off_t)extent;

	return NC_NOERR;
}

/*! Read in a page of data.

  @param[in] nciop  A pointer to the ncio struct for this file.
  @param[in] offset The byte offset in file where read starts.
  @param[in] extent The size of the page that will be read.
  @param[in] vp     A pointer to where the data will end up.

  @param[in,out] nreadp Returned number of bytes actually read (may be less than extent).
  @param[in,out] posp The pointer to current position in file, updated after read.
  @return Return 0 on success, otherwise an error code.
*/
static int
px_pgin(ncio *const nciop,
	off_t const offset, const size_t extent,
	void *const vp, size_t *nreadp, off_t *posp)
{
	int status;
	ssize_t nread;
#ifdef X_ALIGN
	assert(offset % X_ALIGN == 0);
	assert(extent % X_ALIGN == 0);
#endif
    /* *posp == OFF_NONE (-1) on first call. This
       is problematic because lseek also returns -1
       on error. Use errno instead. */
    if(*posp != OFF_NONE && *posp != lseek(nciop->fd, 0, SEEK_CUR)) {
      if(errno) {
        status = errno;
        printf("Error %d: %s\n",errno,strerror(errno));
        return status;
      }
    }

	if(*posp != offset)
	{
		if(lseek(nciop->fd, offset, SEEK_SET) != offset)
		{
			status = errno;
			return status;
		}
		*posp = offset;
	}

	errno = 0;
    /* Handle the case where the read is interrupted
       by a signal (see NCF-337,
       http://pubs.opengroup.org/onlinepubs/009695399/functions/read.html)

       When this happens, nread will (should) be the bytes read, and
       errno will be set to EINTR.  On older systems nread might be -1.
       If this is the case, there's not a whole lot we can do about it
       as we can't compute any offsets, so we will attempt to read again.
       This *feels* like it could lead to an infinite loop, but it shouldn't
       unless the read is being constantly interrupted by a signal, and is
       on an older system which returns -1 instead of bytexs read.

       The case where it's a short read is already handled by the function
       (according to the comment below, at least). */
    do {
      nread = read(nciop->fd,vp,extent);
    } while (nread == -1 && errno == EINTR);


    if(nread != (ssize_t)extent) {
      status = errno;
      if( nread == -1 || (status != EINTR && status != NC_NOERR))
        return status;
      /* else it's okay we read less than asked for */
      (void) memset((char *)vp + nread, 0, (size_t)((ssize_t)extent - nread));
    }

    *nreadp = (size_t)nread;
    *posp += nread;

    return NC_NOERR;
}

/* This struct is for POSIX systems, with NC_SHARE not in effect. If
   NC_SHARE is used, see ncio_spx.

   blksz - block size for reads and writes to file.
   pos - current read/write position in file.
   bf_offset - file offset corresponding to start of memory buffer
   bf_extent - number of bytes in I/O request
   bf_cnt - number of bytes available in buffer
   bf_base - pointer to beginning of buffer.
   bf_rflags - buffer region flags (defined in ncio.h) tell the lock
   status, read/write permissions, and modification status of regions
   of data in the buffer.
   bf_refcount - buffer reference count.
   slave - used in moves.
*/
typedef struct ncio_px {
	size_t blksz;
	off_t pos;
	/* buffer */
	off_t	bf_offset;
	size_t	bf_extent;
	size_t	bf_cnt;
	void	*bf_base;
	int	bf_rflags;
	int	bf_refcount;
	/* chain for double buffering in px_move */
	struct ncio_px *slave;
} ncio_px;


/*ARGSUSED*/
/* This function indicates the file region starting at offset may be
   released.

   This is for POSIX, without NC_SHARE.  If called with RGN_MODIFIED
   flag, sets the modified flag in pxp->bf_rflags and decrements the
   reference count.

   pxp - pointer to posix non-share ncio_px struct.

   offset - file offset for beginning of to region to be
   released.

   rflags - only RGN_MODIFIED is relevant to this function, others ignored
*/
static int
px_rel(ncio_px *const pxp, off_t offset, int rflags)
{
	assert(pxp->bf_offset <= offset
		 && offset < pxp->bf_offset + (off_t) pxp->bf_extent);
	assert(pIf(fIsSet(rflags, RGN_MODIFIED),
		fIsSet(pxp->bf_rflags, RGN_WRITE)));
	NC_UNUSED(offset);

	if(fIsSet(rflags, RGN_MODIFIED))
	{
		fSet(pxp->bf_rflags, RGN_MODIFIED);
	}
	pxp->bf_refcount--;

	return NC_NOERR;
}

/* This function indicates the file region starting at offset may be
   released.  Each read or write to the file is bracketed by a call to
   the "get" region function and a call to the "rel" region function.
   If you only read from the memory region, release it with a flag of
   0, if you modify the region, release it with a flag of
   RGN_MODIFIED.

   For POSIX system, without NC_SHARE, this becomes the rel function
   pointed to by the ncio rel function pointer. It merely checks for
   file write permission, then calls px_rel to do everything.

   nciop - pointer to ncio struct.
   offset - num bytes from beginning of buffer to region to be
   released.
   rflags - only RGN_MODIFIED is relevant to this function, others ignored
*/
static int
ncio_px_rel(ncio *const nciop, off_t offset, int rflags)
{
	ncio_px *const pxp = (ncio_px *)nciop->pvt;

	if(fIsSet(rflags, RGN_MODIFIED) && !fIsSet(nciop->ioflags, NC_WRITE))
		return EPERM; /* attempt to write readonly file */

	return px_rel(pxp, offset, rflags);
}

/* POSIX get. This will "make a region available." Since we're using
   buffered IO, this means that if needed, we'll fetch a new page from
   the file, otherwise, just return a pointer to what's in memory
   already.

   nciop - pointer to ncio struct, containing file info.
   pxp - pointer to ncio_px struct, which contains special metadate
   for posix files without NC_SHARE.
   offset - start byte of region to get.
   extent - how many bytes to read.
   rflags - One of the RGN_* flags defined in ncio.h.
   vpp - pointer to pointer that will receive data.

   NOTES:

   * For blkoffset round offset down to the nearest pxp->blksz. This
   provides the offset (in bytes) to the beginning of the block that
   holds the current offset.

   * diff tells how far into the current block we are.

   * For blkextent round up to the number of bytes at the beginning of
   the next block, after the one that holds our current position, plus
   whatever extra (i.e. the extent) that we are about to grab.

   * The blkextent can't be more than twice the pxp->blksz. That's
   because the pxp->blksize is the sizehint, and in ncio_px_init2 the
   buffer (pointed to by pxp->bf-base) is allocated with 2 *
   *sizehintp. This is checked (unnecessarily) more than once in
   asserts.

   * If this is called on a newly opened file, pxp->bf_offset will be
   OFF_NONE and we'll jump to label pgin to immediately read in a
   page.
*/
static int
px_get(ncio *const nciop, ncio_px *const pxp,
		off_t offset, size_t extent,
		int rflags,
		void **const vpp)
{
	int status = NC_NOERR;

	const off_t blkoffset = _RNDDOWN(offset, (off_t)pxp->blksz);
	off_t diff = offset - blkoffset;
	size_t blkextent = _RNDUP((size_t)diff + extent, pxp->blksz);

	if(!(extent != 0 && extent < X_INT_MAX && offset >= 0)) /* sanity check */
	    return NC_ENOTNC;

	if(2 * pxp->blksz < blkextent)
		return E2BIG; /* TODO: temporary kludge */
	if(pxp->bf_offset == OFF_NONE)
	{
		/* Uninitialized */
		if(pxp->bf_base == NULL)
		{
			assert(pxp->bf_extent == 0);
			assert(blkextent <= 2 * pxp->blksz);
			pxp->bf_base = malloc(2 * pxp->blksz);
			if(pxp->bf_base == NULL)
				return ENOMEM;
		}
		goto pgin;
	}
	/* else */
	assert(blkextent <= 2 * pxp->blksz);

	if(blkoffset == pxp->bf_offset)
	{
		/* hit */
 		if(blkextent > pxp->bf_extent)
		{
			/* page in upper */
			void *const middle =
			 	(void *)((char *)pxp->bf_base + pxp->blksz);
			assert(pxp->bf_extent == pxp->blksz);
			status = px_pgin(nciop,
				 pxp->bf_offset + (off_t)pxp->blksz,
				 pxp->blksz,
				 middle,
				 &pxp->bf_cnt,
				 &pxp->pos);
			if(status != NC_NOERR)
				return status;
			pxp->bf_extent = 2 * pxp->blksz;
			pxp->bf_cnt += pxp->blksz;
		}
		goto done;
	}
	/* else */

	if(pxp->bf_extent > pxp->blksz
		 && blkoffset == pxp->bf_offset + (off_t)pxp->blksz)
	{
		/* hit in upper half */
		if(blkextent == pxp->blksz)
		{
			/* all in upper half, no fault needed */
			diff += (off_t)pxp->blksz;
			goto done;
		}
		/* else */
		if(pxp->bf_cnt > pxp->blksz)
		{
			/* data in upper half */
			void *const middle =
				(void *)((char *)pxp->bf_base + pxp->blksz);
			assert(pxp->bf_extent == 2 * pxp->blksz);
			if(fIsSet(pxp->bf_rflags, RGN_MODIFIED))
			{
				/* page out lower half */
				assert(pxp->bf_refcount <= 0);
				status = px_pgout(nciop,
					pxp->bf_offset,
					pxp->blksz,
					pxp->bf_base,
					&pxp->pos);
				if(status != NC_NOERR)
					return status;
			}
			pxp->bf_cnt -= pxp->blksz;
			/* copy upper half into lower half */
			(void) memcpy(pxp->bf_base, middle, pxp->bf_cnt);
		}
		else		/* added to fix nofill bug */
		{
			assert(pxp->bf_extent == 2 * pxp->blksz);
			/* still have to page out lower half, if modified */
			if(fIsSet(pxp->bf_rflags, RGN_MODIFIED))
			{
				assert(pxp->bf_refcount <= 0);
				status = px_pgout(nciop,
					pxp->bf_offset,
					pxp->blksz,
					pxp->bf_base,
					&pxp->pos);
				if(status != NC_NOERR)
					return status;
			}
		}
		pxp->bf_offset = blkoffset;
		/* pxp->bf_extent = pxp->blksz; */

 		assert(blkextent == 2 * pxp->blksz);
		{
			/* page in upper */
			void *const middle =
			 	(void *)((char *)pxp->bf_base + pxp->blksz);
			status = px_pgin(nciop,
				 pxp->bf_offset + (off_t)pxp->blksz,
				 pxp->blksz,
				 middle,
				 &pxp->bf_cnt,
				 &pxp->pos);
			if(status != NC_NOERR)
				return status;
			pxp->bf_extent = 2 * pxp->blksz;
			pxp->bf_cnt += pxp->blksz;
		}
		goto done;
	}
	/* else */

	if(blkoffset == pxp->bf_offset - (off_t)pxp->blksz)
	{
		/* wants the page below */
		void *const middle =
			(void *)((char *)pxp->bf_base + pxp->blksz);
		size_t upper_cnt = 0;
		if(pxp->bf_cnt > pxp->blksz)
		{
			/* data in upper half */
			assert(pxp->bf_extent == 2 * pxp->blksz);
			if(fIsSet(pxp->bf_rflags, RGN_MODIFIED))
			{
				/* page out upper half */
				assert(pxp->bf_refcount <= 0);
				status = px_pgout(nciop,
					pxp->bf_offset + (off_t)pxp->blksz,
					pxp->bf_cnt - pxp->blksz,
					middle,
					&pxp->pos);
				if(status != NC_NOERR)
					return status;
			}
			pxp->bf_cnt = pxp->blksz;
			pxp->bf_extent = pxp->blksz;
		}
		if(pxp->bf_cnt > 0)
		{
			/* copy lower half into upper half */
			(void) memcpy(middle, pxp->bf_base, pxp->blksz);
			upper_cnt = pxp->bf_cnt;
		}
		/* read page below into lower half */
		status = px_pgin(nciop,
			 blkoffset,
			 pxp->blksz,
			 pxp->bf_base,
			 &pxp->bf_cnt,
			 &pxp->pos);
		if(status != NC_NOERR)
			return status;
		pxp->bf_offset = blkoffset;
		if(upper_cnt != 0)
		{
			pxp->bf_extent = 2 * pxp->blksz;
			pxp->bf_cnt = pxp->blksz + upper_cnt;
		}
		else
		{
			pxp->bf_extent = pxp->blksz;
		}
		goto done;
	}
	/* else */

	/* no overlap */
	if(fIsSet(pxp->bf_rflags, RGN_MODIFIED))
	{
		assert(pxp->bf_refcount <= 0);
		status = px_pgout(nciop,
			pxp->bf_offset,
			pxp->bf_cnt,
			pxp->bf_base,
			&pxp->pos);
		if(status != NC_NOERR)
			return status;
		pxp->bf_rflags = 0;
	}

pgin:
	status = px_pgin(nciop,
		 blkoffset,
		 blkextent,
		 pxp->bf_base,
		 &pxp->bf_cnt,
		 &pxp->pos);
	if(status != NC_NOERR)
		return status;
        pxp->bf_offset = blkoffset;
        pxp->bf_extent = blkextent;

done:
	extent += (size_t)diff;
	if(pxp->bf_cnt < extent)
		pxp->bf_cnt = extent;
	assert(pxp->bf_cnt <= pxp->bf_extent);

	pxp->bf_rflags |= rflags;
	pxp->bf_refcount++;

    *vpp = (void *)((signed char*)pxp->bf_base + diff);
	return NC_NOERR;
}

/* Request that the region (offset, extent) be made available through
   *vpp.

   This function converts a file region specified by an offset and
   extent to a memory pointer. The region may be locked until the
   corresponding call to rel().

   For POSIX systems, without NC_SHARE. This function gets a page of
   size extent?

   This is a wrapper for the function px_get, which does all the heavy
   lifting.

   nciop - pointer to ncio struct for this file.
   offset - offset (from beginning of file?) to the data we want to
   read.
   extent - the number of bytes to read from the file.
   rflags - One of the RGN_* flags defined in ncio.h.
   vpp - handle to point at data when it's been read.
*/
static int
ncio_px_get(ncio *const nciop,
		off_t offset, size_t extent,
		int rflags,
		void **const vpp)
{
	ncio_px *const pxp = (ncio_px *)nciop->pvt;

	if(fIsSet(rflags, RGN_WRITE) && !fIsSet(nciop->ioflags, NC_WRITE))
		return EPERM; /* attempt to write readonly file */

	/* reclaim space used in move */
	if(pxp->slave != NULL)
	{
		if(pxp->slave->bf_base != NULL)
		{
			free(pxp->slave->bf_base);
			pxp->slave->bf_base = NULL;
			pxp->slave->bf_extent = 0;
			pxp->slave->bf_offset = OFF_NONE;
		}
		free(pxp->slave);
		pxp->slave = NULL;
	}
	return px_get(nciop, pxp, offset, extent, rflags, vpp);
}


/* ARGSUSED */
static int
px_double_buffer(ncio *const nciop, off_t to, off_t from,
			size_t nbytes, int rflags)
{
	ncio_px *const pxp = (ncio_px *)nciop->pvt;
	int status = NC_NOERR;
	void *src;
	void *dest;
	NC_UNUSED(rflags);

#if INSTRUMENT
fprintf(stderr, "\tdouble_buffr %ld %ld %ld\n",
		 (long)to, (long)from, (long)nbytes);
#endif
	status = px_get(nciop, pxp, to, nbytes, RGN_WRITE,
			&dest);
	if(status != NC_NOERR)
		return status;

	if(pxp->slave == NULL)
	{
		pxp->slave = (ncio_px *) malloc(sizeof(ncio_px));
		if(pxp->slave == NULL)
			return ENOMEM;

		pxp->slave->blksz = pxp->blksz;
		/* pos done below */
		pxp->slave->bf_offset = pxp->bf_offset;
		pxp->slave->bf_extent = pxp->bf_extent;
		pxp->slave->bf_cnt = pxp->bf_cnt;
		pxp->slave->bf_base = malloc(2 * pxp->blksz);
		if(pxp->slave->bf_base == NULL)
			return ENOMEM;
		(void) memcpy(pxp->slave->bf_base, pxp->bf_base,
			 pxp->bf_extent);
		pxp->slave->bf_rflags = 0;
		pxp->slave->bf_refcount = 0;
		pxp->slave->slave = NULL;
	}

	pxp->slave->pos = pxp->pos;
	status = px_get(nciop, pxp->slave, from, nbytes, 0,
			&src);
	if(status != NC_NOERR)
		return status;
	if(pxp->pos != pxp->slave->pos)
	{
		/* position changed, sync */
		pxp->pos = pxp->slave->pos;
	}

	(void) memcpy(dest, src, nbytes);

	(void)px_rel(pxp->slave, from, 0);
	(void)px_rel(pxp, to, RGN_MODIFIED);

	return status;
}

/* Like memmove(), safely move possibly overlapping data.

   Copy one region to another without making anything available to
   higher layers. May be just implemented in terms of get() and rel(),
   or may be tricky to be efficient. Only used in by nc_enddef()
   after redefinition.

   nciop - pointer to ncio struct with file info.
   to - src for move?
   from - dest for move?
   nbytes - number of bytes to move.
   rflags - One of the RGN_* flags defined in ncio.h. The only
   reasonable flag value is RGN_NOLOCK.
*/
static int
ncio_px_move(ncio *const nciop, off_t to, off_t from,
			size_t nbytes, int rflags)
{
	ncio_px *const pxp = (ncio_px *)nciop->pvt;
	int status = NC_NOERR;
	off_t lower;
	off_t upper;
	char *base;
	size_t diff;
	size_t extent;

	if(to == from)
		return NC_NOERR; /* NOOP */

	if(fIsSet(rflags, RGN_WRITE) && !fIsSet(nciop->ioflags, NC_WRITE))
		return EPERM; /* attempt to write readonly file */

	rflags &= RGN_NOLOCK; /* filter unwanted flags */

	if(to > from)
	{
		/* growing */
		lower = from;
		upper = to;
	}
	else
	{
		/* shrinking */
		lower = to;
		upper = from;
	}
	diff = (size_t)(upper - lower);
	extent = diff + nbytes;

#if INSTRUMENT
fprintf(stderr, "ncio_px_move %ld %ld %ld %ld %ld\n",
		 (long)to, (long)from, (long)nbytes, (long)lower, (long)extent);
#endif
	if(extent > pxp->blksz)
	{
		size_t remaining = nbytes;

if(to > from)
{
		off_t frm = from + (off_t)nbytes;
		off_t toh = to + (off_t)nbytes;
		for(;;)
		{
			size_t loopextent = MIN(remaining, pxp->blksz);
			frm -= (off_t)loopextent;
                        toh -= (off_t)loopextent;

			status = px_double_buffer(nciop, toh, frm,
				 	loopextent, rflags) ;
			if(status != NC_NOERR)
				return status;
			remaining -= loopextent;

			if(remaining == 0)
				break; /* normal loop exit */
		}
}
else
{
		for(;;)
		{
			size_t loopextent = MIN(remaining, pxp->blksz);

			status = px_double_buffer(nciop, to, from,
				 	loopextent, rflags) ;
			if(status != NC_NOERR)
				return status;
			remaining -= loopextent;

			if(remaining == 0)
				break; /* normal loop exit */
			to += (off_t)loopextent;
                        from += (off_t)loopextent;
		}
}
		return NC_NOERR;
	}

#if INSTRUMENT
fprintf(stderr, "\tncio_px_move small\n");
#endif
	status = px_get(nciop, pxp, lower, extent, RGN_WRITE|rflags,
			(void **)&base);

	if(status != NC_NOERR)
		return status;

	if(to > from)
		(void) memmove(base + diff, base, nbytes);
	else
		(void) memmove(base, base + diff, nbytes);

	(void) px_rel(pxp, lower, RGN_MODIFIED);

	return status;
}


/* Flush any buffers to disk. May be a no-op on if I/O is unbuffered.
   This function is used when NC_SHARE is NOT used.
*/
static int
ncio_px_sync(ncio *const nciop)
{
	ncio_px *const pxp = (ncio_px *)nciop->pvt;
	int status = NC_NOERR;
	if(fIsSet(pxp->bf_rflags, RGN_MODIFIED))
	{
		assert(pxp->bf_refcount <= 0);
		status = px_pgout(nciop, pxp->bf_offset,
			pxp->bf_cnt,
			pxp->bf_base, &pxp->pos);
		if(status != NC_NOERR)
			return status;
		pxp->bf_rflags = 0;
	}
	else if (!fIsSet(pxp->bf_rflags, RGN_WRITE))
	{
	    /*
	     * The dataset is readonly.  Invalidate the buffers so
	     * that the next ncio_px_get() will actually read data.
	     */
	    pxp->bf_offset = OFF_NONE;
	    pxp->bf_cnt = 0;
	}
	return status;
}

/* Internal function called at close to
   free up anything hanging off pvt.
*/
static void
ncio_px_freepvt(void *const pvt)
{
	ncio_px *const pxp = (ncio_px *)pvt;
	if(pxp == NULL)
		return;

	if(pxp->slave != NULL)
	{
		if(pxp->slave->bf_base != NULL)
		{
			free(pxp->slave->bf_base);
			pxp->slave->bf_base = NULL;
			pxp->slave->bf_extent = 0;
			pxp->slave->bf_offset = OFF_NONE;
		}
		free(pxp->slave);
		pxp->slave = NULL;
	}

	if(pxp->bf_base != NULL)
	{
		free(pxp->bf_base);
		pxp->bf_base = NULL;
		pxp->bf_extent = 0;
		pxp->bf_offset = OFF_NONE;
	}
}


/* This is the second half of the ncio initialization. This is called
   after the file has actually been opened.

   The most important thing that happens is the allocation of a block
   of memory at pxp->bf_base. This is going to be twice the size of
   the chunksizehint (rounded up to the nearest sizeof(double)) passed
   in from nc__create or nc__open. The rounded chunksizehint (passed
   in here in sizehintp) is going to be stored as pxp->blksize.

   According to our "contract" we are not allowed to ask for an extent
   larger than this chunksize/sizehint/blksize from the ncio get
   function.

   nciop - pointer to the ncio struct
   sizehintp - pointer to a size hint that will be rounded up and
   passed back to the caller.
   isNew - true if this is being called from ncio_create for a new
   file.
*/
static int
ncio_px_init2(ncio *const nciop, size_t *sizehintp, int isNew)
{
	ncio_px *const pxp = (ncio_px *)nciop->pvt;
	const size_t bufsz = 2 * *sizehintp;

	assert(nciop->fd >= 0);

	pxp->blksz = *sizehintp;

	assert(pxp->bf_base == NULL);

	/* this is separate allocation because it may grow */
	pxp->bf_base = malloc(bufsz);
	if(pxp->bf_base == NULL)
		return ENOMEM;
	/* else */
	pxp->bf_cnt = 0;
	if(isNew)
	{
		/* save a read */
		pxp->pos = 0;
		pxp->bf_offset = 0;
		pxp->bf_extent = bufsz;
		(void) memset(pxp->bf_base, 0, pxp->bf_extent);
	}
	return NC_NOERR;
}


/* This is the first of a two-part initialization of the ncio struct.
   Here the rel, get, move, sync, and free function pointers are set
   to their POSIX non-NC_SHARE functions (ncio_px_*).

   The ncio_px struct is also partially initialized.
*/
static void
ncio_px_init(ncio *const nciop)
{
	ncio_px *const pxp = (ncio_px *)nciop->pvt;

	*((ncio_relfunc **)&nciop->rel) = ncio_px_rel; /* cast away const */
	*((ncio_getfunc **)&nciop->get) = ncio_px_get; /* cast away const */
	*((ncio_movefunc **)&nciop->move) = ncio_px_move; /* cast away const */
	*((ncio_syncfunc **)&nciop->sync) = ncio_px_sync; /* cast away const */
	*((ncio_filesizefunc **)&nciop->filesize) = ncio_px_filesize; /* cast away const */
	*((ncio_pad_lengthfunc **)&nciop->pad_length) = ncio_px_pad_length; /* cast away const */
	*((ncio_closefunc **)&nciop->close) = ncio_px_close; /* cast away const */

	pxp->blksz = 0;
	pxp->pos = -1;
	pxp->bf_offset = OFF_NONE;
	pxp->bf_extent = 0;
	pxp->bf_rflags = 0;
	pxp->bf_refcount = 0;
	pxp->bf_base = NULL;
	pxp->slave = NULL;

}

/* Begin spx */

/* This is the struct that gets hung of ncio->pvt(?) when the NC_SHARE
   flag is used.
*/
typedef struct ncio_spx {
	off_t pos;
	/* buffer */
	off_t	bf_offset;
	size_t	bf_extent;
	size_t	bf_cnt;
	void	*bf_base;
} ncio_spx;


/*ARGSUSED*/
/* This function releases the region specified by offset.

   For POSIX system, with NC_SHARE, this becomes the rel function
   pointed to by the ncio rel function pointer. It merely checks for
   file write permission, then calls px_rel to do everything.

   nciop - pointer to ncio struct.

   offset - beginning of region.

   rflags - One of the RGN_* flags defined in ncio.h. If set to
   RGN_MODIFIED it means that the data in this region were modified,
   and it needs to be written out to the disk immediately (since we
   are not buffering with NC_SHARE on).

*/
static int
ncio_spx_rel(ncio *const nciop, off_t offset, int rflags)
{
	ncio_spx *const pxp = (ncio_spx *)nciop->pvt;
	int status = NC_NOERR;

	assert(pxp->bf_offset <= offset);
	assert(pxp->bf_cnt != 0);
	assert(pxp->bf_cnt <= pxp->bf_extent);
#ifdef X_ALIGN
	assert(offset < pxp->bf_offset + X_ALIGN);
	assert(pxp->bf_cnt % X_ALIGN == 0 );
#endif
	NC_UNUSED(offset);

	if(fIsSet(rflags, RGN_MODIFIED))
	{
		if(!fIsSet(nciop->ioflags, NC_WRITE))
			return EPERM; /* attempt to write readonly file */

		status = px_pgout(nciop, pxp->bf_offset,
			pxp->bf_cnt,
			pxp->bf_base, &pxp->pos);
		/* if error, invalidate buffer anyway */
	}
	pxp->bf_offset = OFF_NONE;
	pxp->bf_cnt = 0;
	return status;
}


/* Request that the region (offset, extent) be made available through
   *vpp.

   This function converts a file region specified by an offset and
   extent to a memory pointer. The region may be locked until the
   corresponding call to rel().

   For POSIX systems, with NC_SHARE.

   nciop - pointer to ncio struct for this file.
   offset - offset (from beginning of file?) to the data we want to
   read.
   extent - the number of bytes we want.
   rflags - One of the RGN_* flags defined in ncio.h. May be RGN_NOLOCK.
   vpp - handle to point at data when it's been read.
*/
static int
ncio_spx_get(ncio *const nciop,
		off_t offset, size_t extent,
		int rflags,
		void **const vpp)
{
	ncio_spx *const pxp = (ncio_spx *)nciop->pvt;
	int status = NC_NOERR;
#ifdef X_ALIGN
	size_t rem;
#endif

	if(fIsSet(rflags, RGN_WRITE) && !fIsSet(nciop->ioflags, NC_WRITE))
		return EPERM; /* attempt to write readonly file */

	assert(extent != 0);
	assert(extent < X_INT_MAX); /* sanity check */

	assert(pxp->bf_cnt == 0);

#ifdef X_ALIGN
	rem = (size_t)(offset % X_ALIGN);
	if(rem != 0)
	{
		offset -= rem;
		extent += rem;
	}

	{
      const size_t rndup = extent % X_ALIGN;
      if(rndup != 0)
        extent += X_ALIGN - rndup;
	}

	assert(offset % X_ALIGN == 0);
	assert(extent % X_ALIGN == 0);
#endif

	if(pxp->bf_extent < extent)
	{
		if(pxp->bf_base != NULL)
		{
			free(pxp->bf_base);
			pxp->bf_base = NULL;
			pxp->bf_extent = 0;
		}
		assert(pxp->bf_extent == 0);
		pxp->bf_base = malloc(extent+1);
		if(pxp->bf_base == NULL)
			return ENOMEM;
		pxp->bf_extent = extent;
	}

	status = px_pgin(nciop, offset,
		 extent,
		 pxp->bf_base,
		 &pxp->bf_cnt, &pxp->pos);
	if(status != NC_NOERR)
		return status;

	pxp->bf_offset = offset;

	if(pxp->bf_cnt < extent)
		pxp->bf_cnt = extent;

#ifdef X_ALIGN
	*vpp = (char *)pxp->bf_base + rem;
#else
	*vpp = pxp->bf_base;
#endif
	return NC_NOERR;
}


#if 0
/*ARGSUSED*/
static int
strategy(ncio *const nciop, off_t to, off_t offset,
			size_t extent, int rflags)
{
	static ncio_spx pxp[1];
	int status = NC_NOERR;
#ifdef X_ALIGN
	size_t rem;
#endif

	assert(extent != 0);
	assert(extent < X_INT_MAX); /* sanity check */
#if INSTRUMENT
fprintf(stderr, "strategy %ld at %ld to %ld\n",
	 (long)extent, (long)offset, (long)to);
#endif


#ifdef X_ALIGN
	rem = (size_t)(offset % X_ALIGN);
	if(rem != 0)
	{
		offset -= rem;
		extent += rem;
	}

	{
		const size_t rndup = extent % X_ALIGN;
		if(rndup != 0)
			extent += X_ALIGN - rndup;
	}

	assert(offset % X_ALIGN == 0);
	assert(extent % X_ALIGN == 0);
#endif

	if(pxp->bf_extent < extent)
	{
		if(pxp->bf_base != NULL)
		{
			free(pxp->bf_base);
			pxp->bf_base = NULL;
			pxp->bf_extent = 0;
		}
		assert(pxp->bf_extent == 0);
		pxp->bf_base = malloc(extent);
		if(pxp->bf_base == NULL)
			return ENOMEM;
		pxp->bf_extent = extent;
	}

	status = px_pgin(nciop, offset,
		 extent,
		 pxp->bf_base,
		 &pxp->bf_cnt, &pxp->pos);
	if(status != NC_NOERR)
		return status;

	pxp->bf_offset = to; /* TODO: XALIGN */

	if(pxp->bf_cnt < extent)
		pxp->bf_cnt = extent;

	status = px_pgout(nciop, pxp->bf_offset,
		pxp->bf_cnt,
		pxp->bf_base, &pxp->pos);
	/* if error, invalidate buffer anyway */
	pxp->bf_offset = OFF_NONE;
	pxp->bf_cnt = 0;
	return status;
}
#endif

/* Copy one region to another without making anything available to
   higher layers. May be just implemented in terms of get() and rel(),
   or may be tricky to be efficient.  Only used in by nc_enddef()
   after redefinition.

   nciop - pointer to ncio struct for this file.
   to - dest for move?
   from - src for move?
   nbytes - number of bytes to move.
   rflags - One of the RGN_* flags defined in ncio.h.
*/
static int
ncio_spx_move(ncio *const nciop, off_t to, off_t from,
			size_t nbytes, int rflags)
{
	int status = NC_NOERR;
	off_t lower = from;
	off_t upper = to;
	char *base;
	size_t diff;
	size_t extent;

	rflags &= RGN_NOLOCK; /* filter unwanted flags */

	if(to == from)
		return NC_NOERR; /* NOOP */

	if(to > from)
	{
		/* growing */
		lower = from;
		upper = to;
	}
	else
	{
		/* shrinking */
		lower = to;
		upper = from;
	}

	diff = (size_t)(upper - lower);
	extent = diff + nbytes;

	status = ncio_spx_get(nciop, lower, extent, RGN_WRITE|rflags,
			(void **)&base);

	if(status != NC_NOERR)
		return status;

	if(to > from)
		(void) memmove(base + diff, base, nbytes);
	else
		(void) memmove(base, base + diff, nbytes);

	(void) ncio_spx_rel(nciop, lower, RGN_MODIFIED);

	return status;
}


/*ARGSUSED*/
/* Flush any buffers to disk. May be a no-op on if I/O is unbuffered.
*/
static int
ncio_spx_sync(ncio *const nciop)
{
	NC_UNUSED(nciop);
	/* NOOP */
	return NC_NOERR;
}

static void
ncio_spx_freepvt(void *const pvt)
{
	ncio_spx *const pxp = (ncio_spx *)pvt;
	if(pxp == NULL)
		return;

	if(pxp->bf_base != NULL)
	{
		free(pxp->bf_base);
		pxp->bf_base = NULL;
		pxp->bf_offset = OFF_NONE;
		pxp->bf_extent = 0;
		pxp->bf_cnt = 0;
	}
}


/* This does the second half of the ncio_spx struct initialization for
   POSIX systems, with NC_SHARE on.

   nciop - pointer to ncio struct for this file. File has been opened.
   sizehintp - pointer to a size which will be rounded up to the
   nearest 8-byt boundary and then used as the max size "chunk" (or
   page) to read from the file.
*/
static int
ncio_spx_init2(ncio *const nciop, const size_t *const sizehintp)
{
	ncio_spx *const pxp = (ncio_spx *)nciop->pvt;

	assert(nciop->fd >= 0);

	pxp->bf_extent = *sizehintp;

	assert(pxp->bf_base == NULL);

	/* this is separate allocation because it may grow */
	pxp->bf_base = malloc(pxp->bf_extent);
	if(pxp->bf_base == NULL)
	{
		pxp->bf_extent = 0;
		return ENOMEM;
	}
	/* else */
	return NC_NOERR;
}


/* First half of init for ncio_spx struct, setting the rel, get, move,
   sync, and free function pointers to the NC_SHARE versions of these
   functions (i.e. the ncio_spx_* functions).
*/
static void
ncio_spx_init(ncio *const nciop)
{
	ncio_spx *const pxp = (ncio_spx *)nciop->pvt;

	*((ncio_relfunc **)&nciop->rel) = ncio_spx_rel; /* cast away const */
	*((ncio_getfunc **)&nciop->get) = ncio_spx_get; /* cast away const */
	*((ncio_movefunc **)&nciop->move) = ncio_spx_move; /* cast away const */
	*((ncio_syncfunc **)&nciop->sync) = ncio_spx_sync; /* cast away const */
	/* shared with _px_ */
	*((ncio_filesizefunc **)&nciop->filesize) = ncio_px_filesize; /* cast away const */
	*((ncio_pad_lengthfunc **)&nciop->pad_length) = ncio_px_pad_length; /* cast away const */
	*((ncio_closefunc **)&nciop->close) = ncio_spx_close; /* cast away const */

	pxp->pos = -1;
	pxp->bf_offset = OFF_NONE;
	pxp->bf_extent = 0;
	pxp->bf_cnt = 0;
	pxp->bf_base = NULL;
}


/* */

/* This will call whatever free function is attached to the free
   function pointer in ncio. It's called from ncio_close, and from
   ncio_open and ncio_create when an error occurs that the file
   metadata must be freed.
*/
static void
ncio_px_free(ncio *nciop)
{
	if(nciop == NULL)
		return;
	if(nciop->pvt != NULL)
		ncio_px_freepvt(nciop->pvt);
	free(nciop);
}

static void
ncio_spx_free(ncio *nciop)
{
	if(nciop == NULL)
		return;
	if(nciop->pvt != NULL)
		ncio_spx_freepvt(nciop->pvt);
	free(nciop);
}


/* Create a new ncio struct to hold info about the file. This will
   create and init the ncio_px or ncio_spx struct (the latter if
   NC_SHARE is used.)
*/
static ncio *
ncio_px_new(const char *path, int ioflags)
{
	size_t sz_ncio = M_RNDUP(sizeof(ncio));
	size_t sz_path = M_RNDUP(strlen(path) +1);
	size_t sz_ncio_pvt;
	ncio *nciop;

#if ALWAYS_NC_SHARE /* DEBUG */
	fSet(ioflags, NC_SHARE);
#endif

	if(fIsSet(ioflags, NC_SHARE))
		sz_ncio_pvt = sizeof(ncio_spx);
	else
		sz_ncio_pvt = sizeof(ncio_px);

	nciop = (ncio *) malloc(sz_ncio + sz_path + sz_ncio_pvt);
	if(nciop == NULL)
		return NULL;

	nciop->ioflags = ioflags;
	*((int *)&nciop->fd) = -1; /* cast away const */

	nciop->path = (char *) ((char *)nciop + sz_ncio);
	(void) strcpy((char *)nciop->path, path); /* cast away const */

				/* cast away const */
	*((void **)&nciop->pvt) = (void *)(nciop->path + sz_path);

	if(fIsSet(ioflags, NC_SHARE))
		ncio_spx_init(nciop);
	else
		ncio_px_init(nciop);

	return nciop;
}


/* Public below this point */
#ifndef NCIO_MINBLOCKSIZE
#define NCIO_MINBLOCKSIZE 256
#endif
#ifndef NCIO_MAXBLOCKSIZE
#define NCIO_MAXBLOCKSIZE 268435456 /* sanity check, about X_SIZE_T_MAX/8 */
#endif

#ifdef S_IRUSR
#define NC_DEFAULT_CREAT_MODE \
        (S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH) /* 0666 */

#else
#define NC_DEFAULT_CREAT_MODE 0666
#endif

/* Create a file, and the ncio struct to go with it. This function is
   only called from nc__create_mp.

   path - path of file to create.
   ioflags - flags from nc_create
   initialsz - From the netcdf man page: "The argument
   Iinitialsize sets the initial size of the file at creation time."
   igeto -
   igetsz -
   sizehintp - this eventually goes into pxp->blksz and is the size of
   a page of data for buffered reads and writes.
   nciopp - pointer to a pointer that will get location of newly
   created and inited ncio struct.
   igetvpp - pointer to pointer which will get the location of ?
*/
int
posixio_create(const char *path, int ioflags,
	size_t initialsz,
	off_t igeto, size_t igetsz, size_t *sizehintp,
	void* parameters,
	ncio **nciopp, void **const igetvpp)
{
	ncio *nciop;
	int oflags = (O_RDWR|O_CREAT);
	int fd;
	int status;
	NC_UNUSED(parameters);

	if(initialsz < (size_t)igeto + igetsz)
		initialsz = (size_t)igeto + igetsz;

	fSet(ioflags, NC_WRITE);

	if(path == NULL || *path == 0)
		return EINVAL;

	nciop = ncio_px_new(path, ioflags);
	if(nciop == NULL)
		return ENOMEM;

	if(fIsSet(ioflags, NC_NOCLOBBER))
		fSet(oflags, O_EXCL);
	else
		fSet(oflags, O_TRUNC);
#ifdef O_BINARY
	fSet(oflags, O_BINARY);
#endif
#ifdef vms
	fd = NCopen3(path, oflags, NC_DEFAULT_CREAT_MODE, "ctx=stm");
#else
	/* Should we mess with the mode based on NC_SHARE ?? */
	fd = NCopen3(path, oflags, NC_DEFAULT_CREAT_MODE);
#endif
#if 0
	(void) fprintf(stderr, "ncio_create(): path=\"%s\"\n", path);
	(void) fprintf(stderr, "ncio_create(): oflags=0x%x\n", oflags);
#endif
	if(fd < 0)
	{
		status = errno ? errno : ENOENT;
		goto unwind_new;
	}
	*((int *)&nciop->fd) = fd; /* cast away const */

	if(*sizehintp < NCIO_MINBLOCKSIZE)
	{
		/* Use default */
		*sizehintp = blksize(fd);
	}
	else if(*sizehintp >= NCIO_MAXBLOCKSIZE)
	{
		/* Use maximum allowed value */
		*sizehintp = NCIO_MAXBLOCKSIZE;
	}
	else
	{
		*sizehintp = M_RNDUP(*sizehintp);
	}

	if(fIsSet(nciop->ioflags, NC_SHARE))
		status = ncio_spx_init2(nciop, sizehintp);
	else
		status = ncio_px_init2(nciop, sizehintp, 1);

	if(status != NC_NOERR)
		goto unwind_open;

	if(initialsz != 0)
	{
		status = fgrow(fd, (off_t)initialsz);
		if(status != NC_NOERR)
			goto unwind_open;
	}

	if(igetsz != 0)
	{
		status = nciop->get(nciop,
				igeto, igetsz,
                        	RGN_WRITE,
                        	igetvpp);
		if(status != NC_NOERR)
			goto unwind_open;
	}

	*nciopp = nciop;
	return NC_NOERR;

unwind_open:
	(void) close(fd);
	/* ?? unlink */
	/*FALLTHRU*/
unwind_new:
	ncio_close(nciop,!fIsSet(ioflags, NC_NOCLOBBER));
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

   sizehintp - pointer to sizehint parameter from nc__open or
   nc__create. This is used to set pxp->blksz.

   Here's what the man page has to say:

   "The argument referenced by chunksize controls a space versus time
   tradeoff, memory allocated in the netcdf library versus number of
   system calls.

   Because of internal requirements, the value may not be set to
   exactly the value requested. The actual value chosen is returned by reference.

   Using the value NC_SIZEHINT_DEFAULT causes the library to choose a
   default. How the system choses the default depends on the
   system. On many systems, the "preferred I/O block size" is
   available from the stat() system call, struct stat member
   st_blksize. If this is available it is used. Lacking that, twice
   the system pagesize is used. Lacking a call to discover the system
   pagesize, we just set default chunksize to 8192.

   The chunksize is a property of a given open netcdf descriptor ncid,
   it is not a persistent property of the netcdf dataset."

   nciopp - pointer to pointer that will get address of newly created
   and inited ncio struct.

   igetvpp - handle to pass back pointer to data from initial page
   read, if this were ever used, which it isn't.
*/
int
posixio_open(const char *path,
	int ioflags,
	off_t igeto, size_t igetsz, size_t *sizehintp,
        void* parameters,
	ncio **nciopp, void **const igetvpp)
{
	ncio *nciop;
	int oflags = fIsSet(ioflags, NC_WRITE) ? O_RDWR : O_RDONLY;
	int fd = -1;
	int status = 0;
	NC_UNUSED(parameters);

	if(path == NULL || *path == 0)
		return EINVAL;

	nciop = ncio_px_new(path, ioflags);
	if(nciop == NULL)
		return ENOMEM;

#ifdef O_BINARY
	/*#if _MSC_VER*/
	fSet(oflags, O_BINARY);
#endif

#ifdef vms
	fd = NCopen3(path, oflags, 0, "ctx=stm");
#else
	fd = NCopen3(path, oflags, 0);
#endif
	if(fd < 0)
	{
		status = errno ? errno : ENOENT;
		goto unwind_new;
	}
	*((int *)&nciop->fd) = fd; /* cast away const */

	if(*sizehintp < NCIO_MINBLOCKSIZE)
	{
		/* Use default */
		*sizehintp = blksize(fd);
	}
	else if(*sizehintp >= NCIO_MAXBLOCKSIZE)
	{
		/* Use maximum allowed value */
		*sizehintp = NCIO_MAXBLOCKSIZE;
	}
	else
	{
		*sizehintp = M_RNDUP(*sizehintp);
	}

	if(fIsSet(nciop->ioflags, NC_SHARE))
		status = ncio_spx_init2(nciop, sizehintp);
	else
		status = ncio_px_init2(nciop, sizehintp, 0);

	if(status != NC_NOERR)
		goto unwind_open;

	if(igetsz != 0)
	{
		status = nciop->get(nciop,
				igeto, igetsz,
                        	0,
                        	igetvpp);
		if(status != NC_NOERR)
			goto unwind_open;
	}

	*nciopp = nciop;
	return NC_NOERR;

unwind_open:
	(void) close(fd); /* assert fd >= 0 */
	/*FALLTHRU*/
unwind_new:
	ncio_close(nciop,0);
	return status;
}

/*
 * Get file size in bytes.
 */
static int
ncio_px_filesize(ncio *nciop, off_t *filesizep)
{


	/* There is a problem with fstat on Windows based systems
		which manifests (so far) when Config RELEASE is built.
		Use _filelengthi64 instead. */
#ifdef HAVE_FILE_LENGTH_I64

	int64_t file_len = 0;
	if( (file_len = _filelengthi64(nciop->fd)) < 0) {
		return errno;
	}

	*filesizep = file_len;

#else
    struct stat sb;
    assert(nciop != NULL);
    if (fstat(nciop->fd, &sb) < 0)
	return errno;
    *filesizep = sb.st_size;
#endif
	return NC_NOERR;
}

/*
 * Sync any changes to disk, then truncate or extend file so its size
 * is length.  This is only intended to be called before close, if the
 * file is open for writing and the actual size does not match the
 * calculated size, perhaps as the result of having been previously
 * written in NOFILL mode.
 */
static int
ncio_px_pad_length(ncio *nciop, off_t length)
{

	int status = NC_NOERR;

	if(nciop == NULL)
		return EINVAL;

	if(!fIsSet(nciop->ioflags, NC_WRITE))
	        return EPERM; /* attempt to write readonly file */

	status = nciop->sync(nciop);
	if(status != NC_NOERR)
	        return status;

 	status = fgrow2(nciop->fd, length);
 	if(status != NC_NOERR)
	        return status;
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
ncio_px_close(ncio *nciop, int doUnlink)
{
	int status = NC_NOERR;
	if(nciop == NULL)
		return EINVAL;
	if(nciop->fd > 0) {
	    status = nciop->sync(nciop);
	    (void) close(nciop->fd);
	}
	if(doUnlink)
		(void) unlink(nciop->path);
	ncio_px_free(nciop);
	return status;
}

static int
ncio_spx_close(ncio *nciop, int doUnlink)
{
	int status = NC_NOERR;
	if(nciop == NULL)
		return EINVAL;
	if(nciop->fd > 0) {
	    status = nciop->sync(nciop);
	    (void) close(nciop->fd);
	}
	if(doUnlink)
		(void) unlink(nciop->path);
	ncio_spx_free(nciop);
	return status;
}
