/*********************************************************************
  Blosc - Blocked Shuffling and Compression Library

  Author: Francesc Alted <francesc@blosc.org>

  See LICENSE.txt for details about copyright and rights to use.
**********************************************************************/

#include "bitshuffle-generic.h"


/* Transpose bytes within elements, starting partway through input. */
int64_t blosc_internal_bshuf_trans_byte_elem_remainder(const void* in, void* out, const size_t size,
         const size_t elem_size, const size_t start) {

    char* in_b = (char*) in;
    char* out_b = (char*) out;
    size_t ii, jj, kk;

    CHECK_MULT_EIGHT(start);

    if (size > start) {
        /*  ii loop separated into 2 loops so the compiler can unroll */
        /*  the inner one. */
        for (ii = start; ii + 7 < size; ii += 8) {
            for (jj = 0; jj < elem_size; jj++) {
                for (kk = 0; kk < 8; kk++) {
                    out_b[jj * size + ii + kk]
                        = in_b[ii * elem_size + kk * elem_size + jj];
                }
            }
        }
        for (ii = size - size % 8; ii < size; ii ++) {
            for (jj = 0; jj < elem_size; jj++) {
                out_b[jj * size + ii] = in_b[ii * elem_size + jj];
            }
        }
    }
    return size * elem_size;
}


/* Transpose bytes within elements. */
int64_t blosc_internal_bshuf_trans_byte_elem_scal(const void* in, void* out, const size_t size,
				   const size_t elem_size) {

    return blosc_internal_bshuf_trans_byte_elem_remainder(in, out, size, elem_size, 0);
}


/* Transpose bits within bytes. */
int64_t blosc_internal_bshuf_trans_bit_byte_remainder(const void* in, void* out, const size_t size,
                                                      const size_t elem_size, const size_t start_byte) {

    const uint64_t* in_b = (const uint64_t*) in;
    uint8_t* out_b = (uint8_t*) out;

    uint64_t x, t;

    size_t ii, kk;
    size_t nbyte = elem_size * size;
    size_t nbyte_bitrow = nbyte / 8;

    uint64_t e=1;
    const int little_endian = *(uint8_t *) &e == 1;
    const size_t bit_row_skip = little_endian ? nbyte_bitrow : -nbyte_bitrow;
    const int64_t bit_row_offset = little_endian ? 0 : 7 * nbyte_bitrow;

    CHECK_MULT_EIGHT(nbyte);
    CHECK_MULT_EIGHT(start_byte);

    for (ii = start_byte / 8; ii < nbyte_bitrow; ii ++) {
        x = in_b[ii];
        if (little_endian) {
            TRANS_BIT_8X8(x, t);
        } else {
            TRANS_BIT_8X8_BE(x, t);
        }
        for (kk = 0; kk < 8; kk ++) {
            out_b[bit_row_offset + kk * bit_row_skip + ii] = x;
            x = x >> 8;
        }
    }
    return size * elem_size;
}


/* Transpose bits within bytes. */
static int64_t bshuf_trans_bit_byte_scal(const void* in, void* out, const size_t size,
         const size_t elem_size) {

    return blosc_internal_bshuf_trans_bit_byte_remainder(in, out, size, elem_size, 0);
}

/* General transpose of an array, optimized for large element sizes. */
int64_t blosc_internal_bshuf_trans_elem(const void* in, void* out, const size_t lda,
        const size_t ldb, const size_t elem_size) {

    char* in_b = (char*) in;
    char* out_b = (char*) out;
    size_t ii, jj;
    for (ii = 0; ii < lda; ii++) {
        for (jj = 0; jj < ldb; jj++) {
            memcpy(&out_b[(jj*lda + ii) * elem_size],
                   &in_b[(ii*ldb + jj) * elem_size], elem_size);
        }
    }
    return lda * ldb * elem_size;
}


/* Transpose rows of shuffled bits (size / 8 bytes) within groups of 8. */
int64_t blosc_internal_bshuf_trans_bitrow_eight(const void* in, void* out, const size_t size,
         const size_t elem_size) {

    size_t nbyte_bitrow = size / 8;

    CHECK_MULT_EIGHT(size);

    return blosc_internal_bshuf_trans_elem(in, out, 8, elem_size, nbyte_bitrow);
}


/* Transpose bits within elements. */
int64_t blosc_internal_bshuf_trans_bit_elem_scal(const void* in, void* out, const size_t size,
                                                 const size_t elem_size, void* tmp_buf) {

    int64_t count;

    CHECK_MULT_EIGHT(size);

    count = blosc_internal_bshuf_trans_byte_elem_scal(in, out, size, elem_size);
    CHECK_ERR(count);
    count = bshuf_trans_bit_byte_scal(out, tmp_buf, size, elem_size);
    CHECK_ERR(count);
    count = blosc_internal_bshuf_trans_bitrow_eight(tmp_buf, out, size, elem_size);

    return count;
}


/* For data organized into a row for each bit (8 * elem_size rows), transpose
 * the bytes. */
static int64_t bshuf_trans_byte_bitrow_scal(const void* in, void* out, const size_t size,
         const size_t elem_size) {
    char* in_b = (char*) in;
    char* out_b = (char*) out;

    size_t nbyte_row = size / 8;
    size_t ii, jj, kk;

    CHECK_MULT_EIGHT(size);

    for (jj = 0; jj < elem_size; jj++) {
        for (ii = 0; ii < nbyte_row; ii++) {
            for (kk = 0; kk < 8; kk++) {
                out_b[ii * 8 * elem_size + jj * 8 + kk] = \
                        in_b[(jj * 8 + kk) * nbyte_row + ii];
            }
        }
    }
    return size * elem_size;
}


/* Shuffle bits within the bytes of eight element blocks. */
int64_t blosc_internal_bshuf_shuffle_bit_eightelem_scal(const void* in, void* out,
        const size_t size, const size_t elem_size) {

    const char *in_b;
    char *out_b;
    uint64_t x, t;
    size_t ii, jj, kk;
    size_t nbyte, out_index;

    uint64_t e=1;
    const int little_endian = *(uint8_t *) &e == 1;
    const size_t elem_skip = little_endian ? elem_size : -elem_size;
    const uint64_t elem_offset = little_endian ? 0 : 7 * elem_size;

    CHECK_MULT_EIGHT(size);

    in_b = (const char*) in;
    out_b = (char*) out;

    nbyte = elem_size * size;

    for (jj = 0; jj < 8 * elem_size; jj += 8) {
        for (ii = 0; ii + 8 * elem_size - 1 < nbyte; ii += 8 * elem_size) {
            x = *((uint64_t*) &in_b[ii + jj]);
            if (little_endian) {
                TRANS_BIT_8X8(x, t);
            } else {
                TRANS_BIT_8X8_BE(x, t);
            }
            for (kk = 0; kk < 8; kk++) {
                out_index = ii + jj / 8 + elem_offset + kk * elem_skip;
                *((uint8_t*) &out_b[out_index]) = x;
                x = x >> 8;
            }
        }
    }
    return size * elem_size;
}


/* Untranspose bits within elements. */
int64_t blosc_internal_bshuf_untrans_bit_elem_scal(const void* in, void* out, const size_t size,
                                                   const size_t elem_size, void* tmp_buf) {

    int64_t count;

    CHECK_MULT_EIGHT(size);

    count = bshuf_trans_byte_bitrow_scal(in, tmp_buf, size, elem_size);
    CHECK_ERR(count);
    count =  blosc_internal_bshuf_shuffle_bit_eightelem_scal(tmp_buf, out, size, elem_size);

    return count;
}
