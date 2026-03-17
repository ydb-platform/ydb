/*********************************************************************
  Blosc - Blocked Shuffling and Compression Library

  Author: Francesc Alted <francesc@blosc.org>

  See LICENSE.txt for details about copyright and rights to use.
**********************************************************************/

#ifndef BLOSC_COMP_FEATURES_H
#define BLOSC_COMP_FEATURES_H

/* Use inlined functions for supported systems */
#if defined(_MSC_VER) && !defined(__cplusplus)   /* Visual Studio */
  #define BLOSC_INLINE __inline  /* Visual C is not C99, but supports some kind of inline */
#elif __STDC_VERSION__ >= 199901L
  #define BLOSC_INLINE inline
#else
  #define BLOSC_INLINE
#endif

#endif /* BLOSC_COMP_FEATURES_H */
