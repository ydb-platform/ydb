/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *	See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */
/* $Id: ncio.h,v 1.27 2006/01/03 04:56:28 russ Exp $ */

#ifndef _NCIO_H_
#define _NCIO_H_

#include <stddef.h>	/* size_t */
#include <sys/types.h>	/* off_t */
#include "netcdf.h"

/* Define internal use only flags to signal use of byte ranges and S3. */
#define NC_HTTP  1
#define NC_S3SDK 2

typedef struct ncio ncio;	/* forward reference */

/*
 * A value which is an invalid off_t
 */
#define OFF_NONE  ((off_t)(-1))

/*
 * Flags used by the region layer,
 *  'rflags' argument to ncio.rel() and ncio.get().
 */
#define RGN_NOLOCK	0x1	/* Don't lock region.
				 * Used when contention control handled
				 * elsewhere.
				 */
#define RGN_NOWAIT	0x2	/* return immediate if can't lock, else wait */

#define RGN_WRITE	0x4	/* we intend to modify, else read only */

#define RGN_MODIFIED	0x8	/* we did modify, else, discard */


/*
 * The next four typedefs define the signatures
 * of function pointers in struct ncio below.
 * They are not used outside of this file and ncio.h,
 * They just make some casts in the ncio.c more readable.
 */
	/*
	 * Indicate that you are done with the region which begins
	 * at offset. Only reasonable flag value is RGN_MODIFIED.
	 */
typedef int ncio_relfunc(ncio *const nciop,
		 off_t offset, int rflags);

	/*
	 * Request that the region (offset, extent)
	 * be made available through *vpp.
	 */
typedef int ncio_getfunc(ncio *const nciop,
			off_t offset, size_t extent,
			int rflags,
			void **const vpp);

	/*
	 * Like memmove(), safely move possibly overlapping data.
	 * Only reasonable flag value is RGN_NOLOCK.
	 */
typedef int ncio_movefunc(ncio *const nciop, off_t to, off_t from,
			size_t nbytes, int rflags);

	/*
	 * Write out any dirty buffers to disk and
	 * ensure that next read will get data from disk.
	 */
typedef int ncio_syncfunc(ncio *const nciop);

/*
 *  Sync any changes to disk, then truncate or extend file so its size
 *  is length.  This is only intended to be called before close, if the
 *  file is open for writing and the actual size does not match the
 *  calculated size, perhaps as the result of having been previously
 *  written in NOFILL mode.
  */
typedef int ncio_pad_lengthfunc(ncio* nciop, off_t length);

/* 
 *  Get file size in bytes.
 */ 
typedef int ncio_filesizefunc(ncio *nciop, off_t *filesizep);

/* Write out any dirty buffers and
   ensure that next read will not get cached data.
   Sync any changes, then close the open file associated with the ncio
   struct, and free its memory.
   nciop - pointer to ncio to close.
   doUnlink - if true, unlink file
*/
typedef int ncio_closefunc(ncio *nciop, int doUnlink);

/* Get around cplusplus "const xxx in class ncio without constructor" error */
#if defined(__cplusplus)
#define NCIO_CONST
#else
#define NCIO_CONST const
#endif

/*
 * netcdf i/o abstraction
 */
struct ncio {
	/*
	 * A copy of the ioflags argument passed in to ncio_open()
	 * or ncio_create().
	 */
	int ioflags;

	/*
	 * The file descriptor of the netcdf file.
	 * This gets handed to the user as the netcdf id.
	 */
	NCIO_CONST int fd;

	/* member functions do the work */

	ncio_relfunc *NCIO_CONST rel;

	ncio_getfunc *NCIO_CONST get;

	ncio_movefunc *NCIO_CONST move;

	ncio_syncfunc *NCIO_CONST sync;

	ncio_pad_lengthfunc *NCIO_CONST pad_length;

	ncio_filesizefunc *NCIO_CONST filesize;
  
	ncio_closefunc *NCIO_CONST close;

	/*
	 * A copy of the 'path' argument passed in to ncio_open()
	 * or ncio_create(). Used by ncabort() to remove (unlink)
	 * the file and by error messages.
	 */
	const char *path;

	/* implementation private stuff */
	void *pvt;
};

#undef NCIO_CONST

/* Define wrappers around the ncio dispatch table */

extern int ncio_rel(ncio* const, off_t, int);
extern int ncio_get(ncio* const, off_t, size_t, int, void** const);
extern int ncio_move(ncio* const, off_t, off_t, size_t, int);
extern int ncio_sync(ncio* const);
extern int ncio_filesize(ncio* const, off_t*);
extern int ncio_pad_length(ncio* const, off_t);
extern int ncio_close(ncio* const, int);

extern int ncio_create(const char *path, int ioflags, size_t initialsz,
                       off_t igeto, size_t igetsz, size_t *sizehintp,
		       void* parameters, /* new */
                       ncio** nciopp, void** const mempp);

extern int ncio_open(const char *path, int ioflags,
                     off_t igeto, size_t igetsz, size_t *sizehintp,
		     void* parameters, /* new */
                     ncio** nciopp, void** const mempp);

/* With the advent of diskless io, we need to provide
   for multiple ncio packages at the same time,
   so we have multiple versions of ncio_create.
   If you create a new package, the you must do the following.
   1. add an extern definition for it in ncio.c
   2. modify ncio_create and ncio_open in ncio.c to invoke
      the new package when appropriate.
*/

#endif /* _NCIO_H_ */
