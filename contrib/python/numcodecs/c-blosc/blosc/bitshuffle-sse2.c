/*
 * Bitshuffle - Filter for improving compression of typed binary data.
 *
 * Author: Kiyoshi Masui <kiyo@physics.ubc.ca>
 * Website: http://www.github.com/kiyo-masui/bitshuffle
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

/* Define dummy functions if SSE2 is not available for the compilation target and compiler. */
#if !defined(__SSE2__)
#include <stdlib.h>

int64_t blosc_internal_bshuf_trans_byte_elem_sse2(void* in, void* out, const size_t size,
                                                  const size_t elem_size, void* tmp_buf) {
    abort();
}

int64_t blosc_internal_bshuf_untrans_bit_elem_sse2(void* in, void* out, const size_t size,
				                                   const size_t elem_size, void* tmp_buf) {
    abort();
}

#else /* defined(__SSE2__) */

#include <emmintrin.h>

/* The next is useful for debugging purposes */
#if 0
#include <stdio.h>
#include <string.h>


static void printxmm(__m128i xmm0)
{
  uint8_t buf[32];

  ((__m128i *)buf)[0] = xmm0;
  printf("%x,%x,%x,%x,%x,%x,%x,%x,%x,%x,%x,%x,%x,%x,%x,%x\n",
          buf[0], buf[1], buf[2], buf[3],
          buf[4], buf[5], buf[6], buf[7],
          buf[8], buf[9], buf[10], buf[11],
          buf[12], buf[13], buf[14], buf[15]);
}
#endif


/* ---- Worker code that requires SSE2. Intel Petium 4 (2000) and later. ---- */

/* Transpose bytes within elements for 16 bit elements. */
static int64_t bshuf_trans_byte_elem_SSE_16(void* in, void* out, const size_t size) {

    char* in_b = (char*) in;
    char* out_b = (char*) out;
    __m128i a0, b0, a1, b1;
    size_t ii;

    for (ii=0; ii + 15 < size; ii += 16) {
        a0 = _mm_loadu_si128((__m128i *) &in_b[2*ii + 0*16]);
        b0 = _mm_loadu_si128((__m128i *) &in_b[2*ii + 1*16]);

        a1 = _mm_unpacklo_epi8(a0, b0);
        b1 = _mm_unpackhi_epi8(a0, b0);

        a0 = _mm_unpacklo_epi8(a1, b1);
        b0 = _mm_unpackhi_epi8(a1, b1);

        a1 = _mm_unpacklo_epi8(a0, b0);
        b1 = _mm_unpackhi_epi8(a0, b0);

        a0 = _mm_unpacklo_epi8(a1, b1);
        b0 = _mm_unpackhi_epi8(a1, b1);

        _mm_storeu_si128((__m128i *) &out_b[0*size + ii], a0);
        _mm_storeu_si128((__m128i *) &out_b[1*size + ii], b0);
    }
    return blosc_internal_bshuf_trans_byte_elem_remainder(in, out, size, 2,
            size - size % 16);
}


/* Transpose bytes within elements for 32 bit elements. */
static int64_t bshuf_trans_byte_elem_SSE_32(void* in, void* out, const size_t size) {

    char* in_b = (char*) in;
    char* out_b = (char*) out;
    __m128i a0, b0, c0, d0, a1, b1, c1, d1;
    size_t ii;

    for (ii=0; ii + 15 < size; ii += 16) {
        a0 = _mm_loadu_si128((__m128i *) &in_b[4*ii + 0*16]);
        b0 = _mm_loadu_si128((__m128i *) &in_b[4*ii + 1*16]);
        c0 = _mm_loadu_si128((__m128i *) &in_b[4*ii + 2*16]);
        d0 = _mm_loadu_si128((__m128i *) &in_b[4*ii + 3*16]);

        a1 = _mm_unpacklo_epi8(a0, b0);
        b1 = _mm_unpackhi_epi8(a0, b0);
        c1 = _mm_unpacklo_epi8(c0, d0);
        d1 = _mm_unpackhi_epi8(c0, d0);

        a0 = _mm_unpacklo_epi8(a1, b1);
        b0 = _mm_unpackhi_epi8(a1, b1);
        c0 = _mm_unpacklo_epi8(c1, d1);
        d0 = _mm_unpackhi_epi8(c1, d1);

        a1 = _mm_unpacklo_epi8(a0, b0);
        b1 = _mm_unpackhi_epi8(a0, b0);
        c1 = _mm_unpacklo_epi8(c0, d0);
        d1 = _mm_unpackhi_epi8(c0, d0);

        a0 = _mm_unpacklo_epi64(a1, c1);
        b0 = _mm_unpackhi_epi64(a1, c1);
        c0 = _mm_unpacklo_epi64(b1, d1);
        d0 = _mm_unpackhi_epi64(b1, d1);

        _mm_storeu_si128((__m128i *) &out_b[0*size + ii], a0);
        _mm_storeu_si128((__m128i *) &out_b[1*size + ii], b0);
        _mm_storeu_si128((__m128i *) &out_b[2*size + ii], c0);
        _mm_storeu_si128((__m128i *) &out_b[3*size + ii], d0);
    }
    return blosc_internal_bshuf_trans_byte_elem_remainder(in, out, size, 4,
            size - size % 16);
}


/* Transpose bytes within elements for 64 bit elements. */
static int64_t bshuf_trans_byte_elem_SSE_64(void* in, void* out, const size_t size) {

    char* in_b = (char*) in;
    char* out_b = (char*) out;
    __m128i a0, b0, c0, d0, e0, f0, g0, h0;
    __m128i a1, b1, c1, d1, e1, f1, g1, h1;
    size_t ii;

    for (ii=0; ii + 15 < size; ii += 16) {
        a0 = _mm_loadu_si128((__m128i *) &in_b[8*ii + 0*16]);
        b0 = _mm_loadu_si128((__m128i *) &in_b[8*ii + 1*16]);
        c0 = _mm_loadu_si128((__m128i *) &in_b[8*ii + 2*16]);
        d0 = _mm_loadu_si128((__m128i *) &in_b[8*ii + 3*16]);
        e0 = _mm_loadu_si128((__m128i *) &in_b[8*ii + 4*16]);
        f0 = _mm_loadu_si128((__m128i *) &in_b[8*ii + 5*16]);
        g0 = _mm_loadu_si128((__m128i *) &in_b[8*ii + 6*16]);
        h0 = _mm_loadu_si128((__m128i *) &in_b[8*ii + 7*16]);

        a1 = _mm_unpacklo_epi8(a0, b0);
        b1 = _mm_unpackhi_epi8(a0, b0);
        c1 = _mm_unpacklo_epi8(c0, d0);
        d1 = _mm_unpackhi_epi8(c0, d0);
        e1 = _mm_unpacklo_epi8(e0, f0);
        f1 = _mm_unpackhi_epi8(e0, f0);
        g1 = _mm_unpacklo_epi8(g0, h0);
        h1 = _mm_unpackhi_epi8(g0, h0);

        a0 = _mm_unpacklo_epi8(a1, b1);
        b0 = _mm_unpackhi_epi8(a1, b1);
        c0 = _mm_unpacklo_epi8(c1, d1);
        d0 = _mm_unpackhi_epi8(c1, d1);
        e0 = _mm_unpacklo_epi8(e1, f1);
        f0 = _mm_unpackhi_epi8(e1, f1);
        g0 = _mm_unpacklo_epi8(g1, h1);
        h0 = _mm_unpackhi_epi8(g1, h1);

        a1 = _mm_unpacklo_epi32(a0, c0);
        b1 = _mm_unpackhi_epi32(a0, c0);
        c1 = _mm_unpacklo_epi32(b0, d0);
        d1 = _mm_unpackhi_epi32(b0, d0);
        e1 = _mm_unpacklo_epi32(e0, g0);
        f1 = _mm_unpackhi_epi32(e0, g0);
        g1 = _mm_unpacklo_epi32(f0, h0);
        h1 = _mm_unpackhi_epi32(f0, h0);

        a0 = _mm_unpacklo_epi64(a1, e1);
        b0 = _mm_unpackhi_epi64(a1, e1);
        c0 = _mm_unpacklo_epi64(b1, f1);
        d0 = _mm_unpackhi_epi64(b1, f1);
        e0 = _mm_unpacklo_epi64(c1, g1);
        f0 = _mm_unpackhi_epi64(c1, g1);
        g0 = _mm_unpacklo_epi64(d1, h1);
        h0 = _mm_unpackhi_epi64(d1, h1);

        _mm_storeu_si128((__m128i *) &out_b[0*size + ii], a0);
        _mm_storeu_si128((__m128i *) &out_b[1*size + ii], b0);
        _mm_storeu_si128((__m128i *) &out_b[2*size + ii], c0);
        _mm_storeu_si128((__m128i *) &out_b[3*size + ii], d0);
        _mm_storeu_si128((__m128i *) &out_b[4*size + ii], e0);
        _mm_storeu_si128((__m128i *) &out_b[5*size + ii], f0);
        _mm_storeu_si128((__m128i *) &out_b[6*size + ii], g0);
        _mm_storeu_si128((__m128i *) &out_b[7*size + ii], h0);
    }
    return blosc_internal_bshuf_trans_byte_elem_remainder(in, out, size, 8,
            size - size % 16);
}


/* Memory copy with bshuf call signature. */
static int64_t bshuf_copy(void* in, void* out, const size_t size,
                          const size_t elem_size) {

    char* in_b = (char*) in;
    char* out_b = (char*) out;

    memcpy(out_b, in_b, size * elem_size);
    return size * elem_size;
}


/* Transpose bytes within elements using best SSE algorithm available. */
int64_t blosc_internal_bshuf_trans_byte_elem_sse2(void* in, void* out, const size_t size,
                                                  const size_t elem_size, void* tmp_buf) {

    int64_t count;

    /*  Trivial cases: power of 2 bytes. */
    switch (elem_size) {
        case 1:
            count = bshuf_copy(in, out, size, elem_size);
            return count;
        case 2:
            count = bshuf_trans_byte_elem_SSE_16(in, out, size);
            return count;
        case 4:
            count = bshuf_trans_byte_elem_SSE_32(in, out, size);
            return count;
        case 8:
            count = bshuf_trans_byte_elem_SSE_64(in, out, size);
            return count;
    }

    /*  Worst case: odd number of bytes. Turns out that this is faster for */
    /*  (odd * 2) byte elements as well (hence % 4). */
    if (elem_size % 4) {
        count = blosc_internal_bshuf_trans_byte_elem_scal(in, out, size, elem_size);
        return count;
    }

    /*  Multiple of power of 2: transpose hierarchically. */
    {
        size_t nchunk_elem;

        if ((elem_size % 8) == 0) {
            nchunk_elem = elem_size / 8;
            TRANS_ELEM_TYPE(in, out, size, nchunk_elem, int64_t);
            count = bshuf_trans_byte_elem_SSE_64(out, tmp_buf,
                    size * nchunk_elem);
            blosc_internal_bshuf_trans_elem(tmp_buf, out, 8, nchunk_elem, size);
        } else if ((elem_size % 4) == 0) {
            nchunk_elem = elem_size / 4;
            TRANS_ELEM_TYPE(in, out, size, nchunk_elem, int32_t);
            count = bshuf_trans_byte_elem_SSE_32(out, tmp_buf,
                    size * nchunk_elem);
            blosc_internal_bshuf_trans_elem(tmp_buf, out, 4, nchunk_elem, size);
        } else {
            /*  Not used since scalar algorithm is faster. */
            nchunk_elem = elem_size / 2;
            TRANS_ELEM_TYPE(in, out, size, nchunk_elem, int16_t);
            count = bshuf_trans_byte_elem_SSE_16(out, tmp_buf,
                    size * nchunk_elem);
            blosc_internal_bshuf_trans_elem(tmp_buf, out, 2, nchunk_elem, size);
        }

        return count;
    }
}


/* Transpose bits within bytes. */
static int64_t bshuf_trans_bit_byte_sse2(void* in, void* out, const size_t size,
                                         const size_t elem_size) {

    char* in_b = (char*) in;
    char* out_b = (char*) out;
    uint16_t* out_ui16;
    int64_t count;
    size_t nbyte = elem_size * size;
    __m128i xmm;
    int32_t bt;
    size_t ii, kk;

    CHECK_MULT_EIGHT(nbyte);

    for (ii = 0; ii + 15 < nbyte; ii += 16) {
        xmm = _mm_loadu_si128((__m128i *) &in_b[ii]);
        for (kk = 0; kk < 8; kk++) {
            bt = _mm_movemask_epi8(xmm);
            xmm = _mm_slli_epi16(xmm, 1);
            out_ui16 = (uint16_t*) &out_b[((7 - kk) * nbyte + ii) / 8];
            *out_ui16 = bt;
        }
    }
    count = blosc_internal_bshuf_trans_bit_byte_remainder(in, out, size, elem_size,
            nbyte - nbyte % 16);
    return count;
}


/* Transpose bits within elements. */
int64_t blosc_internal_bshuf_trans_bit_elem_sse2(void* in, void* out, const size_t size,
				  const size_t elem_size, void* tmp_buf) {

    int64_t count;

    CHECK_MULT_EIGHT(size);

    count = blosc_internal_bshuf_trans_byte_elem_sse2(in, out, size, elem_size, tmp_buf);
    CHECK_ERR(count);
    count = bshuf_trans_bit_byte_sse2(out, tmp_buf, size, elem_size);
    CHECK_ERR(count);
    count = blosc_internal_bshuf_trans_bitrow_eight(tmp_buf, out, size, elem_size);

    return count;
}


/* For data organized into a row for each bit (8 * elem_size rows), transpose
 * the bytes. */
int64_t blosc_internal_bshuf_trans_byte_bitrow_sse2(void* in, void* out, const size_t size,
				     const size_t elem_size) {

    char* in_b = (char*) in;
    char* out_b = (char*) out;
    size_t nrows = 8 * elem_size;
    size_t nbyte_row = size / 8;
    size_t ii, jj;

    __m128i a0, b0, c0, d0, e0, f0, g0, h0;
    __m128i a1, b1, c1, d1, e1, f1, g1, h1;
    __m128 *as, *bs, *cs, *ds, *es, *fs, *gs, *hs;

    CHECK_MULT_EIGHT(size);

    for (ii = 0; ii + 7 < nrows; ii += 8) {
        for (jj = 0; jj + 15 < nbyte_row; jj += 16) {
            a0 = _mm_loadu_si128((__m128i *) &in_b[(ii + 0)*nbyte_row + jj]);
            b0 = _mm_loadu_si128((__m128i *) &in_b[(ii + 1)*nbyte_row + jj]);
            c0 = _mm_loadu_si128((__m128i *) &in_b[(ii + 2)*nbyte_row + jj]);
            d0 = _mm_loadu_si128((__m128i *) &in_b[(ii + 3)*nbyte_row + jj]);
            e0 = _mm_loadu_si128((__m128i *) &in_b[(ii + 4)*nbyte_row + jj]);
            f0 = _mm_loadu_si128((__m128i *) &in_b[(ii + 5)*nbyte_row + jj]);
            g0 = _mm_loadu_si128((__m128i *) &in_b[(ii + 6)*nbyte_row + jj]);
            h0 = _mm_loadu_si128((__m128i *) &in_b[(ii + 7)*nbyte_row + jj]);


            a1 = _mm_unpacklo_epi8(a0, b0);
            b1 = _mm_unpacklo_epi8(c0, d0);
            c1 = _mm_unpacklo_epi8(e0, f0);
            d1 = _mm_unpacklo_epi8(g0, h0);
            e1 = _mm_unpackhi_epi8(a0, b0);
            f1 = _mm_unpackhi_epi8(c0, d0);
            g1 = _mm_unpackhi_epi8(e0, f0);
            h1 = _mm_unpackhi_epi8(g0, h0);


            a0 = _mm_unpacklo_epi16(a1, b1);
            b0 = _mm_unpacklo_epi16(c1, d1);
            c0 = _mm_unpackhi_epi16(a1, b1);
            d0 = _mm_unpackhi_epi16(c1, d1);

            e0 = _mm_unpacklo_epi16(e1, f1);
            f0 = _mm_unpacklo_epi16(g1, h1);
            g0 = _mm_unpackhi_epi16(e1, f1);
            h0 = _mm_unpackhi_epi16(g1, h1);


            a1 = _mm_unpacklo_epi32(a0, b0);
            b1 = _mm_unpackhi_epi32(a0, b0);

            c1 = _mm_unpacklo_epi32(c0, d0);
            d1 = _mm_unpackhi_epi32(c0, d0);

            e1 = _mm_unpacklo_epi32(e0, f0);
            f1 = _mm_unpackhi_epi32(e0, f0);

            g1 = _mm_unpacklo_epi32(g0, h0);
            h1 = _mm_unpackhi_epi32(g0, h0);

            /*  We don't have a storeh instruction for integers, so interpret */
            /*  as a float. Have a storel (_mm_storel_epi64). */
            as = (__m128 *) &a1;
            bs = (__m128 *) &b1;
            cs = (__m128 *) &c1;
            ds = (__m128 *) &d1;
            es = (__m128 *) &e1;
            fs = (__m128 *) &f1;
            gs = (__m128 *) &g1;
            hs = (__m128 *) &h1;

            _mm_storel_pi((__m64 *) &out_b[(jj + 0) * nrows + ii], *as);
            _mm_storel_pi((__m64 *) &out_b[(jj + 2) * nrows + ii], *bs);
            _mm_storel_pi((__m64 *) &out_b[(jj + 4) * nrows + ii], *cs);
            _mm_storel_pi((__m64 *) &out_b[(jj + 6) * nrows + ii], *ds);
            _mm_storel_pi((__m64 *) &out_b[(jj + 8) * nrows + ii], *es);
            _mm_storel_pi((__m64 *) &out_b[(jj + 10) * nrows + ii], *fs);
            _mm_storel_pi((__m64 *) &out_b[(jj + 12) * nrows + ii], *gs);
            _mm_storel_pi((__m64 *) &out_b[(jj + 14) * nrows + ii], *hs);

            _mm_storeh_pi((__m64 *) &out_b[(jj + 1) * nrows + ii], *as);
            _mm_storeh_pi((__m64 *) &out_b[(jj + 3) * nrows + ii], *bs);
            _mm_storeh_pi((__m64 *) &out_b[(jj + 5) * nrows + ii], *cs);
            _mm_storeh_pi((__m64 *) &out_b[(jj + 7) * nrows + ii], *ds);
            _mm_storeh_pi((__m64 *) &out_b[(jj + 9) * nrows + ii], *es);
            _mm_storeh_pi((__m64 *) &out_b[(jj + 11) * nrows + ii], *fs);
            _mm_storeh_pi((__m64 *) &out_b[(jj + 13) * nrows + ii], *gs);
            _mm_storeh_pi((__m64 *) &out_b[(jj + 15) * nrows + ii], *hs);
        }
        for (jj = nbyte_row - nbyte_row % 16; jj < nbyte_row; jj ++) {
            out_b[jj * nrows + ii + 0] = in_b[(ii + 0)*nbyte_row + jj];
            out_b[jj * nrows + ii + 1] = in_b[(ii + 1)*nbyte_row + jj];
            out_b[jj * nrows + ii + 2] = in_b[(ii + 2)*nbyte_row + jj];
            out_b[jj * nrows + ii + 3] = in_b[(ii + 3)*nbyte_row + jj];
            out_b[jj * nrows + ii + 4] = in_b[(ii + 4)*nbyte_row + jj];
            out_b[jj * nrows + ii + 5] = in_b[(ii + 5)*nbyte_row + jj];
            out_b[jj * nrows + ii + 6] = in_b[(ii + 6)*nbyte_row + jj];
            out_b[jj * nrows + ii + 7] = in_b[(ii + 7)*nbyte_row + jj];
        }
    }
    return size * elem_size;
}


/* Shuffle bits within the bytes of eight element blocks. */
int64_t blosc_internal_bshuf_shuffle_bit_eightelem_sse2(void* in, void* out, const size_t size,
					 const size_t elem_size) {
    /*  With a bit of care, this could be written such that such that it is */
    /*  in_buf = out_buf safe. */
    char* in_b = (char*) in;
    uint16_t* out_ui16 = (uint16_t*) out;

    size_t nbyte = elem_size * size;

    __m128i xmm;
    int32_t bt;
    size_t ii, jj, kk;
    size_t ind;

    CHECK_MULT_EIGHT(size);

    if (elem_size % 2) {
        blosc_internal_bshuf_shuffle_bit_eightelem_scal(in, out, size, elem_size);
    } else {
        for (ii = 0; ii + 8 * elem_size - 1 < nbyte;
                ii += 8 * elem_size) {
            for (jj = 0; jj + 15 < 8 * elem_size; jj += 16) {
                xmm = _mm_loadu_si128((__m128i *) &in_b[ii + jj]);
                for (kk = 0; kk < 8; kk++) {
                    bt = _mm_movemask_epi8(xmm);
                    xmm = _mm_slli_epi16(xmm, 1);
                    ind = (ii + jj / 8 + (7 - kk) * elem_size);
                    out_ui16[ind / 2] = bt;
                }
            }
        }
    }
    return size * elem_size;
}


/* Untranspose bits within elements. */
int64_t blosc_internal_bshuf_untrans_bit_elem_sse2(void* in, void* out, const size_t size,
				    const size_t elem_size, void* tmp_buf) {

    int64_t count;

    CHECK_MULT_EIGHT(size);

    count = blosc_internal_bshuf_trans_byte_bitrow_sse2(in, tmp_buf, size, elem_size);
    CHECK_ERR(count);
    count = blosc_internal_bshuf_shuffle_bit_eightelem_sse2(tmp_buf, out, size, elem_size);

    return count;
}

#endif /* !defined(__SSE2__) */
