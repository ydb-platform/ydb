/*********************************************************************
  Blosc - Blocked Shuffling and Compression Library

  Author: Francesc Alted <francesc@blosc.org>

  See LICENSE.txt for details about copyright and rights to use.
**********************************************************************/

/* Generic (non-hardware-accelerated) shuffle/unshuffle routines.
   These are used when hardware-accelerated functions aren't available
   for a particular platform; they are also used by the hardware-
   accelerated functions to handle any remaining elements in a block
   which isn't a multiple of the hardware's vector size. */

#ifndef SHUFFLE_GENERIC_H
#define SHUFFLE_GENERIC_H

#include "blosc-common.h"
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
  Generic (non-hardware-accelerated) shuffle routine.
  This is the pure element-copying nested loop. It is used by the
  generic shuffle implementation and also by the vectorized shuffle
  implementations to process any remaining elements in a block which
  is not a multiple of (type_size * vector_size).
*/
static void shuffle_generic_inline(const size_t type_size,
    const size_t vectorizable_blocksize, const size_t blocksize,
    const uint8_t* const _src, uint8_t* const _dest)
{
  size_t i, j;
  /* Calculate the number of elements in the block. */
  const size_t neblock_quot = blocksize / type_size;
  const size_t neblock_rem = blocksize % type_size;
  const size_t vectorizable_elements = vectorizable_blocksize / type_size;


  /* Non-optimized shuffle */
  for (j = 0; j < type_size; j++) {
    for (i = vectorizable_elements; i < (size_t)neblock_quot; i++) {
      _dest[j*neblock_quot+i] = _src[i*type_size+j];
    }
  }

  /* Copy any leftover bytes in the block without shuffling them. */
  memcpy(_dest + (blocksize - neblock_rem), _src + (blocksize - neblock_rem), neblock_rem);
}

/**
  Generic (non-hardware-accelerated) unshuffle routine.
  This is the pure element-copying nested loop. It is used by the
  generic unshuffle implementation and also by the vectorized unshuffle
  implementations to process any remaining elements in a block which
  is not a multiple of (type_size * vector_size).
*/
static void unshuffle_generic_inline(const size_t type_size,
  const size_t vectorizable_blocksize, const size_t blocksize,
  const uint8_t* const _src, uint8_t* const _dest)
{
  size_t i, j;

  /* Calculate the number of elements in the block. */
  const size_t neblock_quot = blocksize / type_size;
  const size_t neblock_rem = blocksize % type_size;
  const size_t vectorizable_elements = vectorizable_blocksize / type_size;

  /* Non-optimized unshuffle */
  for (i = vectorizable_elements; i < (size_t)neblock_quot; i++) {
    for (j = 0; j < type_size; j++) {
      _dest[i*type_size+j] = _src[j*neblock_quot+i];
    }
  }

  /* Copy any leftover bytes in the block without unshuffling them. */
  memcpy(_dest + (blocksize - neblock_rem), _src + (blocksize - neblock_rem), neblock_rem);
}

/**
  Generic (non-hardware-accelerated) shuffle routine.
*/
BLOSC_NO_EXPORT void blosc_internal_shuffle_generic(const size_t bytesoftype, const size_t blocksize,
                                                    const uint8_t* const _src, uint8_t* const _dest);

/**
  Generic (non-hardware-accelerated) unshuffle routine.
*/
BLOSC_NO_EXPORT void blosc_internal_unshuffle_generic(const size_t bytesoftype, const size_t blocksize,
                                                      const uint8_t* const _src, uint8_t* const _dest);

#ifdef __cplusplus
}
#endif

#endif /* SHUFFLE_GENERIC_H */
