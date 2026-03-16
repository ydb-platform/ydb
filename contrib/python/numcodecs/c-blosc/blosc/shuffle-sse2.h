/*********************************************************************
  Blosc - Blocked Shuffling and Compression Library

  Author: Francesc Alted <francesc@blosc.org>

  See LICENSE.txt for details about copyright and rights to use.
**********************************************************************/

/* SSE2-accelerated shuffle/unshuffle routines. */
   
#ifndef SHUFFLE_SSE2_H
#define SHUFFLE_SSE2_H

#include "blosc-common.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
  SSE2-accelerated shuffle routine.
*/
BLOSC_NO_EXPORT void blosc_internal_shuffle_sse2(const size_t bytesoftype, const size_t blocksize,
                                                 const uint8_t* const _src, uint8_t* const _dest);

/**
  SSE2-accelerated unshuffle routine.
*/
BLOSC_NO_EXPORT void blosc_internal_unshuffle_sse2(const size_t bytesoftype, const size_t blocksize,
                                                   const uint8_t* const _src, uint8_t* const _dest);

#ifdef __cplusplus
}
#endif

#endif /* SHUFFLE_SSE2_H */
