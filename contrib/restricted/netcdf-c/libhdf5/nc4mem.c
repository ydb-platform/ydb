/*********************************************************************
*    Copyright 2018, UCAR/Unidata
*    See netcdf/COPYRIGHT file for copying and redistribution conditions.
* ********************************************************************/

/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
* Copyright by The HDF Group.                                               *
* Copyright by the Board of Trustees of the University of Illinois.         *
* All rights reserved.                                                      *
*                                                                           *
* This file is part of HDF5.  The full HDF5 copyright notice, including     *
* terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://support.hdfgroup.org/ftp/HDF5/releases.  *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#include "config.h"
#include "hdf5internal.h"
#include <H5DSpublic.h> /* must be after nc4internal.h */
#include <H5Fpublic.h>
#include <H5LTpublic.h>

#include "netcdf.h"
#include "nc4internal.h"

#undef DEBUG

#ifndef HDrealloc
    #define HDrealloc(M,Z)    realloc(M,Z)
#endif /* HDrealloc */

int
NC4_open_image_file(NC_FILE_INFO_T* h5)
{
    int stat = NC_NOERR;
    hid_t hdfid;

    /* check arguments */
    if(h5->mem.memio.memory == NULL || h5->mem.memio.size == 0)
	{stat = NC_EINVAL; goto done;}

    /* Figure out the image flags */
    h5->mem.imageflags = 0;
    if(h5->mem.locked) {
	h5->mem.imageflags |= (H5LT_FILE_IMAGE_DONT_COPY | H5LT_FILE_IMAGE_DONT_RELEASE);
    }
    if(!h5->no_write)
	h5->mem.imageflags |= H5LT_FILE_IMAGE_OPEN_RW;

    /* Create the file but using our version of H5LTopen_file_image */
    hdfid = NC4_image_init(h5);
    if(hdfid < 0)
	{stat = NC_EHDFERR; goto done;}

    /* Remember HDF5 file identifier. */
    ((NC_HDF5_FILE_INFO_T *)h5->format_file_info)->hdfid = hdfid;

done:
    return stat;
}

int
NC4_create_image_file(NC_FILE_INFO_T* h5, size_t initialsz)
{
    int stat = NC_NOERR;
    hid_t hdfid;

    /* Create the file but using our version of H5LTopen_file_image */
    h5->mem.created = 1;
    h5->mem.initialsize = initialsz;
    h5->mem.imageflags |= H5LT_FILE_IMAGE_OPEN_RW;
    hdfid = NC4_image_init(h5);
    if(hdfid < 0)
	{stat = NC_EHDFERR; goto done;}
    /* Remember HDF5 file identifier. */
    ((NC_HDF5_FILE_INFO_T *)h5->format_file_info)->hdfid = hdfid;
done:
    return stat;
}
