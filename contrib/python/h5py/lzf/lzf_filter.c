/***** Preamble block *********************************************************
*
* This file is part of h5py, a low-level Python interface to the HDF5 library.
*
* Copyright (C) 2008 Andrew Collette
* http://h5py.org
* License: BSD  (See LICENSE.txt for full license)
*
* $Date$
*
****** End preamble block ****************************************************/

/*
    Implements an LZF filter module for HDF5, using the BSD-licensed library
    by Marc Alexander Lehmann (http://www.goof.com/pcg/marc/liblzf.html).

    No Python-specific code is used.  The filter behaves like the DEFLATE
    filter, in that it is called for every type and space, and returns 0
    if the data cannot be compressed.

    The only public function is (int) register_lzf(void), which passes on
    the result from H5Zregister.
*/

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include "hdf5.h"
#include "lzf.h"
#include "lzf_filter.h"

/* Our own versions of H5Epush_sim, as it changed in 1.8 */
#if H5_VERS_MAJOR == 1 && H5_VERS_MINOR < 7

#define PUSH_ERR(func, minor, str)  H5Epush(__FILE__, func, __LINE__, H5E_PLINE, minor, str)
#define H5PY_GET_FILTER H5Pget_filter_by_id

#else

#define PUSH_ERR(func, minor, str)  H5Epush1(__FILE__, func, __LINE__, H5E_PLINE, minor, str)
#define H5PY_GET_FILTER(a,b,c,d,e,f,g) H5Pget_filter_by_id2(a,b,c,d,e,f,g,NULL)

#endif

/*  Deal with the multiple definitions for H5Z_class_t.
    Note: HDF5 >=1.6 are supported.

    (1) The old class should always be used for HDF5 1.6
    (2) The new class should always be used for HDF5 1.8 < 1.8.3
    (3) The old class should be used for HDF5 >= 1.8.3 only if the
        macro H5_USE_16_API is set
*/

#if H5_VERS_MAJOR == 1 && H5_VERS_MINOR == 6
#define H5PY_H5Z_NEWCLS 0
#elif H5_VERS_MAJOR == 1 && H5_VERS_MINOR == 8 && H5_VERS_RELEASE < 3
#define H5PY_H5Z_NEWCLS 1
#elif H5_USE_16_API
#define H5PY_H5Z_NEWCLS 0
#else /* Default: use new class */
#define H5PY_H5Z_NEWCLS 1
#endif

size_t lzf_filter(unsigned flags, size_t cd_nelmts,
		    const unsigned cd_values[], size_t nbytes,
		    size_t *buf_size, void **buf);

herr_t lzf_set_local(hid_t dcpl, hid_t type, hid_t space);

#if H5PY_H5Z_NEWCLS
static const H5Z_class_t filter_class = {
    H5Z_CLASS_T_VERS,
    (H5Z_filter_t)(H5PY_FILTER_LZF),
    1, 1,
    "lzf",
    NULL,
    (H5Z_set_local_func_t)(lzf_set_local),
    (H5Z_func_t)(lzf_filter)
};
#else
static const H5Z_class_t filter_class = {
    (H5Z_filter_t)(H5PY_FILTER_LZF),
    "lzf",
    NULL,
    (H5Z_set_local_func_t)(lzf_set_local),
    (H5Z_func_t)(lzf_filter)
};
#endif

/* Support dynamical loading of LZF filter plugin */
#if defined(H5_VERSION_GE)
#if H5_VERSION_GE(1, 8, 11)

#include "H5PLextern.h"

H5PL_type_t H5PLget_plugin_type(void){ return H5PL_TYPE_FILTER; }

const void *H5PLget_plugin_info(void){ return &filter_class; }

#endif
#endif

/* Try to register the filter, passing on the HDF5 return value */
int register_lzf(void){

    int retval;

    retval = H5Zregister(&filter_class);
    if(retval<0){
        PUSH_ERR("register_lzf", H5E_CANTREGISTER, "Can't register LZF filter");
    }
    return retval;
}

/*  Filter setup.  Records the following inside the DCPL:

    1.  If version information is not present, set slots 0 and 1 to the filter
        revision and LZF API version, respectively.

    2. Compute the chunk size in bytes and store it in slot 2.
*/
herr_t lzf_set_local(hid_t dcpl, hid_t type, hid_t space){

    int ndims;
    int i;
    herr_t r;

    unsigned int bufsize;
    hsize_t chunkdims[32];

    unsigned int flags;
    size_t nelements = 8;
    unsigned values[] = {0,0,0,0,0,0,0,0};

    r = H5PY_GET_FILTER(dcpl, H5PY_FILTER_LZF, &flags, &nelements, values, 0, NULL);
    if(r<0) return -1;

    if(nelements < 3) nelements = 3;  /* First 3 slots reserved.  If any higher
                                      slots are used, preserve the contents. */

    /* It seems the H5Z_FLAG_REVERSE flag doesn't work here, so we have to be
       careful not to clobber any existing version info */
    if(values[0]==0) values[0] = H5PY_FILTER_LZF_VERSION;
    if(values[1]==0) values[1] = LZF_VERSION;

    ndims = H5Pget_chunk(dcpl, 32, chunkdims);
    if(ndims<0) return -1;
    if(ndims>32){
        PUSH_ERR("lzf_set_local", H5E_CALLBACK, "Chunk rank exceeds limit");
        return -1;
    }

    bufsize = H5Tget_size(type);
    if(bufsize==0) return -1;

    for(i=0;i<ndims;i++){
        bufsize *= chunkdims[i];
    }

    values[2] = bufsize;

#ifdef H5PY_LZF_DEBUG
    fprintf(stderr, "LZF: Computed buffer size %d\n", bufsize);
#endif

    r = H5Pmodify_filter(dcpl, H5PY_FILTER_LZF, flags, nelements, values);
    if(r<0) return -1;

    return 1;
}


/* The filter function */
size_t lzf_filter(unsigned flags, size_t cd_nelmts,
		    const unsigned cd_values[], size_t nbytes,
		    size_t *buf_size, void **buf){

    void* outbuf = NULL;
    size_t outbuf_size = 0;

    unsigned int status = 0;        /* Return code from lzf routines */

    /* We're compressing */
    if(!(flags & H5Z_FLAG_REVERSE)){

        /* Allocate an output buffer exactly as long as the input data; if
           the result is larger, we simply return 0.  The filter is flagged
           as optional, so HDF5 marks the chunk as uncompressed and
           proceeds.
        */

        outbuf_size = (*buf_size);
        outbuf = malloc(outbuf_size);

        if(outbuf == NULL){
            PUSH_ERR("lzf_filter", H5E_CALLBACK, "Can't allocate compression buffer");
            goto failed;
        }

        status = lzf_compress(*buf, nbytes, outbuf, outbuf_size);

    /* We're decompressing */
    } else {

        if((cd_nelmts>=3)&&(cd_values[2]!=0)){
            outbuf_size = cd_values[2];   /* Precomputed buffer guess */
        }else{
            outbuf_size = (*buf_size);
        }

#ifdef H5PY_LZF_DEBUG
        fprintf(stderr, "Decompress %d chunk w/buffer %d\n", nbytes, outbuf_size);
#endif

        while(!status){

            free(outbuf);
            outbuf = malloc(outbuf_size);

            if(outbuf == NULL){
                PUSH_ERR("lzf_filter", H5E_CALLBACK, "Can't allocate decompression buffer");
                goto failed;
            }

            status = lzf_decompress(*buf, nbytes, outbuf, outbuf_size);

            if(!status){    /* compression failed */

                if(errno == E2BIG){
                    outbuf_size += (*buf_size);
#ifdef H5PY_LZF_DEBUG
                    fprintf(stderr, "    Too small: %d\n", outbuf_size);
#endif
                } else if(errno == EINVAL) {

                    PUSH_ERR("lzf_filter", H5E_CALLBACK, "Invalid data for LZF decompression");
                    goto failed;

                } else {
                    PUSH_ERR("lzf_filter", H5E_CALLBACK, "Unknown LZF decompression error");
                    goto failed;
                }

            } /* if !status */

        } /* while !status */

    } /* compressing vs decompressing */

    if(status != 0){

        free(*buf);
        *buf = outbuf;
        *buf_size = outbuf_size;

        return status;  /* Size of compressed/decompressed data */
    }

    failed:

    free(outbuf);
    return 0;

} /* End filter function */
