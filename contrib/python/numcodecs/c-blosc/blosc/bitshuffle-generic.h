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

#ifndef BITSHUFFLE_GENERIC_H
#define BITSHUFFLE_GENERIC_H

#include "blosc-common.h"
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif


/*  Macros. */
#define CHECK_MULT_EIGHT(n) if (n % 8) return -80;
#define MIN(X,Y) ((X) < (Y) ? (X) : (Y))
#define MAX(X,Y) ((X) > (Y) ? (X) : (Y))
#define CHECK_ERR(count) if (count < 0) { return count; }


/* ---- Worker code not requiring special instruction sets. ----
 *
 * The following code does not use any x86 specific vectorized instructions
 * and should compile on any machine
 *
 */

/* Transpose 8x8 bit array packed into a single quadword *x*.
 * *t* is workspace. */
#define TRANS_BIT_8X8(x, t) {                                               \
        t = (x ^ (x >> 7)) & 0x00AA00AA00AA00AALL;                          \
        x = x ^ t ^ (t << 7);                                               \
        t = (x ^ (x >> 14)) & 0x0000CCCC0000CCCCLL;                         \
        x = x ^ t ^ (t << 14);                                              \
        t = (x ^ (x >> 28)) & 0x00000000F0F0F0F0LL;                         \
        x = x ^ t ^ (t << 28);                                              \
    }

/* Transpose 8x8 bit array along the diagonal from upper right
   to lower left */
#define TRANS_BIT_8X8_BE(x, t) {                                            \
        t = (x ^ (x >> 9)) & 0x0055005500550055LL;                          \
        x = x ^ t ^ (t << 9);                                               \
        t = (x ^ (x >> 18)) & 0x0000333300003333LL;                         \
        x = x ^ t ^ (t << 18);                                              \
        t = (x ^ (x >> 36)) & 0x000000000F0F0F0FLL;                         \
        x = x ^ t ^ (t << 36);                                              \
    }

/* Transpose of an array of arbitrarily typed elements. */
#define TRANS_ELEM_TYPE(in, out, lda, ldb, type_t) {                        \
        type_t* in_type = (type_t*) in;                                     \
        type_t* out_type = (type_t*) out;                                   \
        size_t ii, jj, kk;                                                  \
        for (ii = 0; ii + 7 < lda; ii += 8) {                               \
            for (jj = 0; jj < ldb; jj++) {                                  \
                for (kk = 0; kk < 8; kk++) {                                \
                    out_type[jj*lda + ii + kk] =                            \
                        in_type[ii*ldb + kk * ldb + jj];                    \
                }                                                           \
            }                                                               \
        }                                                                   \
        for (ii = lda - lda % 8; ii < lda; ii ++) {                         \
            for (jj = 0; jj < ldb; jj++) {                                  \
                out_type[jj*lda + ii] = in_type[ii*ldb + jj];               \
            }                                                               \
        }                                                                   \
    }


/* Private functions */
BLOSC_NO_EXPORT int64_t
blosc_internal_bshuf_trans_byte_elem_remainder(const void* in, void* out, const size_t size,
                                               const size_t elem_size, const size_t start);

BLOSC_NO_EXPORT int64_t
blosc_internal_bshuf_trans_byte_elem_scal(const void* in, void* out, const size_t size,
                                          const size_t elem_size);

BLOSC_NO_EXPORT int64_t
blosc_internal_bshuf_trans_bit_byte_remainder(const void* in, void* out, const size_t size,
                                              const size_t elem_size, const size_t start_byte);

BLOSC_NO_EXPORT int64_t
blosc_internal_bshuf_trans_elem(const void* in, void* out, const size_t lda,
                                const size_t ldb, const size_t elem_size);

BLOSC_NO_EXPORT int64_t
blosc_internal_bshuf_trans_bitrow_eight(const void* in, void* out, const size_t size,
                                        const size_t elem_size);

BLOSC_NO_EXPORT int64_t
blosc_internal_bshuf_shuffle_bit_eightelem_scal(const void* in, void* out,
                                                const size_t size, const size_t elem_size);


/* Bitshuffle the data.
 *
 * Transpose the bits within elements.
 *
 * Parameters
 * ----------
 *  in : input buffer, must be of size * elem_size bytes
 *  out : output buffer, must be of size * elem_size bytes
 *  size : number of elements in input
 *  elem_size : element size of typed data
 *  tmp_buffer : temporary buffer with the same `size` than `in` and `out`
 *
 * Returns
 * -------
 *  nothing -- this cannot fail
 *
 */

BLOSC_NO_EXPORT int64_t
blosc_internal_bshuf_trans_bit_elem_scal(const void* in, void* out, const size_t size,
                                         const size_t elem_size, void* tmp_buf);

/* Unshuffle bitshuffled data.
 *
 * Untranspose the bits within elements.
 *
 * To properly unshuffle bitshuffled data, *size* and *elem_size* must
 * match the parameters used to shuffle the data.
 *
 * Parameters
 * ----------
 *  in : input buffer, must be of size * elem_size bytes
 *  out : output buffer, must be of size * elem_size bytes
 *  size : number of elements in input
 *  elem_size : element size of typed data
 *  tmp_buffer : temporary buffer with the same `size` than `in` and `out`
 *
 * Returns
 * -------
 *  nothing -- this cannot fail
 *
 */

BLOSC_NO_EXPORT int64_t
blosc_internal_bshuf_untrans_bit_elem_scal(const void* in, void* out, const size_t size,
                                           const size_t elem_size, void* tmp_buf);


#ifdef __cplusplus
}
#endif

#endif /* BITSHUFFLE_GENERIC_H */
