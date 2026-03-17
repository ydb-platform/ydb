/*
 * Bitshuffle - Filter for improving compression of typed binary data.
 *
 * Author: Kiyoshi Masui <kiyo@physics.ubc.ca>
 * Website: https://github.com/kiyo-masui/bitshuffle
 * Created: 2014
 *
 * Note: Adapted for c-blosc by Francesc Alted.
 *
 * See LICENSES/BITSHUFFLE.txt file for details about copyright and
 * rights to use.
 *
 */

#include "bitshuffle-generic.h"
#include "bitshuffle-sse2.h"
#include "bitshuffle-avx2.h"


/* Define dummy functions if AVX2 is not available for the compilation target and compiler. */
#if !defined(__AVX2__)
#include <stdlib.h>

int64_t blosc_internal_bshuf_trans_bit_elem_avx2(void* in, void* out, const size_t size,
                                                 const size_t elem_size, void* tmp_buf) {
    abort();
}

int64_t blosc_internal_bshuf_untrans_bit_elem_avx2(void* in, void* out, const size_t size,
                                                   const size_t elem_size, void* tmp_buf) {
    abort();
}

#else /* defined(__AVX2__) */

#include <immintrin.h>

/* The next is useful for debugging purposes */
#if 0
#include <stdio.h>
#include <string.h>

static void printymm(__m256i ymm0)
{
  uint8_t buf[32];

  ((__m256i *)buf)[0] = ymm0;
  printf("%x,%x,%x,%x,%x,%x,%x,%x,%x,%x,%x,%x,%x,%x,%x,%x,%x,%x,%x,%x,%x,%x,%x,%x,%x,%x,%x,%x,%x,%x,%x,%x\n",
          buf[0], buf[1], buf[2], buf[3],
          buf[4], buf[5], buf[6], buf[7],
          buf[8], buf[9], buf[10], buf[11],
          buf[12], buf[13], buf[14], buf[15],
          buf[16], buf[17], buf[18], buf[19],
          buf[20], buf[21], buf[22], buf[23],
          buf[24], buf[25], buf[26], buf[27],
          buf[28], buf[29], buf[30], buf[31]);
}
#endif


/* ---- Code that requires AVX2. Intel Haswell (2013) and later. ---- */


/* Transpose bits within bytes. */
static int64_t bshuf_trans_bit_byte_avx2(void* in, void* out, const size_t size,
                                         const size_t elem_size) {

    char* in_b = (char*) in;
    char* out_b = (char*) out;
    int32_t* out_i32;

    size_t nbyte = elem_size * size;

    int64_t count;

    __m256i ymm;
    int32_t bt;
    size_t ii, kk;

    for (ii = 0; ii + 31 < nbyte; ii += 32) {
        ymm = _mm256_loadu_si256((__m256i *) &in_b[ii]);
        for (kk = 0; kk < 8; kk++) {
            bt = _mm256_movemask_epi8(ymm);
            ymm = _mm256_slli_epi16(ymm, 1);
            out_i32 = (int32_t*) &out_b[((7 - kk) * nbyte + ii) / 8];
            *out_i32 = bt;
        }
    }
    count = blosc_internal_bshuf_trans_bit_byte_remainder(in, out, size, elem_size,
            nbyte - nbyte % 32);
    return count;
}

/* Transpose bits within elements. */
int64_t blosc_internal_bshuf_trans_bit_elem_avx2(void* in, void* out, const size_t size,
                                                 const size_t elem_size, void* tmp_buf) {
    int64_t count;

    CHECK_MULT_EIGHT(size);

    count = blosc_internal_bshuf_trans_byte_elem_sse2(in, out, size, elem_size, tmp_buf);
    CHECK_ERR(count);
    count = bshuf_trans_bit_byte_avx2(out, tmp_buf, size, elem_size);
    CHECK_ERR(count);
    count = blosc_internal_bshuf_trans_bitrow_eight(tmp_buf, out, size, elem_size);

    return count;
}

/* For data organized into a row for each bit (8 * elem_size rows), transpose
 * the bytes. */
static int64_t bshuf_trans_byte_bitrow_avx2(void* in, void* out, const size_t size,
                                            const size_t elem_size) {

    char* in_b = (char*) in;
    char* out_b = (char*) out;

    size_t nrows = 8 * elem_size;
    size_t nbyte_row = size / 8;
    size_t ii, jj, kk, hh, mm;

    CHECK_MULT_EIGHT(size);

    if (elem_size % 4)
      return blosc_internal_bshuf_trans_byte_bitrow_sse2(in, out, size, elem_size);

    __m256i ymm_0[8];
    __m256i ymm_1[8];
    __m256i ymm_storeage[8][4];

    for (jj = 0; jj + 31 < nbyte_row; jj += 32) {
        for (ii = 0; ii + 3 < elem_size; ii += 4) {
            for (hh = 0; hh < 4; hh ++) {

                for (kk = 0; kk < 8; kk ++){
                    ymm_0[kk] = _mm256_loadu_si256((__m256i *) &in_b[
                            (ii * 8 + hh * 8 + kk) * nbyte_row + jj]);
                }

                for (kk = 0; kk < 4; kk ++){
                    ymm_1[kk] = _mm256_unpacklo_epi8(ymm_0[kk * 2],
                            ymm_0[kk * 2 + 1]);
                    ymm_1[kk + 4] = _mm256_unpackhi_epi8(ymm_0[kk * 2],
                            ymm_0[kk * 2 + 1]);
                }

                for (kk = 0; kk < 2; kk ++){
                    for (mm = 0; mm < 2; mm ++){
                        ymm_0[kk * 4 + mm] = _mm256_unpacklo_epi16(
                                ymm_1[kk * 4 + mm * 2],
                                ymm_1[kk * 4 + mm * 2 + 1]);
                        ymm_0[kk * 4 + mm + 2] = _mm256_unpackhi_epi16(
                                ymm_1[kk * 4 + mm * 2],
                                ymm_1[kk * 4 + mm * 2 + 1]);
                    }
                }

                for (kk = 0; kk < 4; kk ++){
                    ymm_1[kk * 2] = _mm256_unpacklo_epi32(ymm_0[kk * 2],
                            ymm_0[kk * 2 + 1]);
                    ymm_1[kk * 2 + 1] = _mm256_unpackhi_epi32(ymm_0[kk * 2],
                            ymm_0[kk * 2 + 1]);
                }

                for (kk = 0; kk < 8; kk ++){
                    ymm_storeage[kk][hh] = ymm_1[kk];
                }
            }

            for (mm = 0; mm < 8; mm ++) {

                for (kk = 0; kk < 4; kk ++){
                    ymm_0[kk] = ymm_storeage[mm][kk];
                }

                ymm_1[0] = _mm256_unpacklo_epi64(ymm_0[0], ymm_0[1]);
                ymm_1[1] = _mm256_unpacklo_epi64(ymm_0[2], ymm_0[3]);
                ymm_1[2] = _mm256_unpackhi_epi64(ymm_0[0], ymm_0[1]);
                ymm_1[3] = _mm256_unpackhi_epi64(ymm_0[2], ymm_0[3]);

                ymm_0[0] = _mm256_permute2x128_si256(ymm_1[0], ymm_1[1], 32);
                ymm_0[1] = _mm256_permute2x128_si256(ymm_1[2], ymm_1[3], 32);
                ymm_0[2] = _mm256_permute2x128_si256(ymm_1[0], ymm_1[1], 49);
                ymm_0[3] = _mm256_permute2x128_si256(ymm_1[2], ymm_1[3], 49);

                _mm256_storeu_si256((__m256i *) &out_b[
                        (jj + mm * 2 + 0 * 16) * nrows + ii * 8], ymm_0[0]);
                _mm256_storeu_si256((__m256i *) &out_b[
                        (jj + mm * 2 + 0 * 16 + 1) * nrows + ii * 8], ymm_0[1]);
                _mm256_storeu_si256((__m256i *) &out_b[
                        (jj + mm * 2 + 1 * 16) * nrows + ii * 8], ymm_0[2]);
                _mm256_storeu_si256((__m256i *) &out_b[
                        (jj + mm * 2 + 1 * 16 + 1) * nrows + ii * 8], ymm_0[3]);
            }
        }
    }
    for (ii = 0; ii < nrows; ii ++ ) {
        for (jj = nbyte_row - nbyte_row % 32; jj < nbyte_row; jj ++) {
            out_b[jj * nrows + ii] = in_b[ii * nbyte_row + jj];
        }
    }
    return size * elem_size;
}


/* Shuffle bits within the bytes of eight element blocks. */
static int64_t bshuf_shuffle_bit_eightelem_avx2(void* in, void* out, const size_t size,
                                                const size_t elem_size) {

    CHECK_MULT_EIGHT(size);

    /*  With a bit of care, this could be written such that such that it is */
    /*  in_buf = out_buf safe. */
    char* in_b = (char*) in;
    char* out_b = (char*) out;

    size_t nbyte = elem_size * size;
    size_t ii, jj, kk, ind;

    __m256i ymm;
    int32_t bt;

    if (elem_size % 4) {
        return blosc_internal_bshuf_shuffle_bit_eightelem_sse2(in, out, size, elem_size);
    } else {
        for (jj = 0; jj + 31 < 8 * elem_size; jj += 32) {
            for (ii = 0; ii + 8 * elem_size - 1 < nbyte;
                    ii += 8 * elem_size) {
                ymm = _mm256_loadu_si256((__m256i *) &in_b[ii + jj]);
                for (kk = 0; kk < 8; kk++) {
                    bt = _mm256_movemask_epi8(ymm);
                    ymm = _mm256_slli_epi16(ymm, 1);
                    ind = (ii + jj / 8 + (7 - kk) * elem_size);
                    * (int32_t *) &out_b[ind] = bt;
                }
            }
        }
    }
    return size * elem_size;
}


/* Untranspose bits within elements. */
int64_t blosc_internal_bshuf_untrans_bit_elem_avx2(void* in, void* out, const size_t size,
                                                   const size_t elem_size, void* tmp_buf) {

    int64_t count;

    CHECK_MULT_EIGHT(size);

    count = bshuf_trans_byte_bitrow_avx2(in, tmp_buf, size, elem_size);
    CHECK_ERR(count);
    count =  bshuf_shuffle_bit_eightelem_avx2(tmp_buf, out, size, elem_size);

    return count;
}

#endif /* !defined(__AVX2__) */
