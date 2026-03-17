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


#ifndef H5PY_LZF_H
#define H5PY_LZF_H

#ifdef __cplusplus
extern "C" {
#endif

/* Filter revision number, starting at 1 */
#define H5PY_FILTER_LZF_VERSION 4

/* Filter ID registered with the HDF Group as of 2/6/09.  For maintenance
   requests, contact the filter author directly. */
#define H5PY_FILTER_LZF 32000

/* Register the filter with the library. Returns a negative value on failure,
   and a non-negative value on success.
*/
int register_lzf(void);

#ifdef __cplusplus
}
#endif

#endif
