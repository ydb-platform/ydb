/*********************************************************************
  Blosc - Blocked Shuffling and Compression Library

  Author: Francesc Alted <francesc@blosc.org>

  See LICENSE.txt for details about copyright and rights to use.
**********************************************************************/

#include "shuffle-generic.h"

/* Shuffle a block.  This can never fail. */
void blosc_internal_shuffle_generic(const size_t bytesoftype, const size_t blocksize,
		     const uint8_t* const _src, uint8_t* const _dest)
{
  /* Non-optimized shuffle */
  shuffle_generic_inline(bytesoftype, 0, blocksize, _src, _dest);
}

/* Unshuffle a block.  This can never fail. */
void blosc_internal_unshuffle_generic(const size_t bytesoftype, const size_t blocksize,
                                      const uint8_t* const _src, uint8_t* const _dest)
{
  /* Non-optimized unshuffle */
  unshuffle_generic_inline(bytesoftype, 0, blocksize, _src, _dest);
}
