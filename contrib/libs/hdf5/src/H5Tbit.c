/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://www.hdfgroup.org/licenses.               *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* Module Info: Operations on bit vectors.  A bit vector is an array of bytes
 *              with the least-significant bits in the first byte.  That is,
 *              the bytes are in little-endian order.
 */

#include "H5Tmodule.h" /* This source code file is part of the H5T module */

#include "H5private.h"   /*generic functions			  */
#include "H5Eprivate.h"  /*error handling			  */
#include "H5MMprivate.h" /* Memory management			*/
#include "H5Tpkg.h"      /*data-type functions			  */
#include "H5WBprivate.h" /* Wrapped Buffers                      */

/*-------------------------------------------------------------------------
 * Function:    H5T__bit_copy
 *
 * Purpose:     Copies bits from one vector to another.
 *
 * Return:      void
 *
 *-------------------------------------------------------------------------
 */
void
H5T__bit_copy(uint8_t *dst, size_t dst_offset, const uint8_t *src, size_t src_offset, size_t size)
{
    size_t shift;
    size_t mask_lo, mask_hi;
    size_t s_idx, d_idx;

    FUNC_ENTER_PACKAGE_NOERR

    /* Normalize the offset to be a byte number and a bit offset within that
     * byte.
     */
    s_idx = src_offset / 8;
    d_idx = dst_offset / 8;
    src_offset %= 8;
    dst_offset %= 8;

    /* Get things rolling. This means copying bits until we're aligned on a
     * source byte.  This the following example, five bits are copied to the
     * destination.
     *
     *                      src[s_idx]
     *   +---------------+---------------+
     *   |7 6 5 4 3 2 1 0|7 6 5 4 3 2 1 0|
     *   +---------------+---------------+
     *      ... : : : : : | | | | |
     *      ... v v v v v V V V V V
     *      ...+---------------+---------------+
     *      ...|7 6 5 4 3 2 1 0|7 6 5 4 3 2 1 0|
     *      ...+---------------+---------------+
     *           dst[d_idx+1]      dst[d_idx]
     */
    while (src_offset && size > 0) {
        size_t nbits = MIN3(size, 8 - dst_offset, 8 - src_offset);
        size_t mask  = ((size_t)1 << nbits) - 1;

        dst[d_idx] &= (uint8_t) ~(mask << dst_offset);
        dst[d_idx] = (uint8_t)(dst[d_idx] | (((src[s_idx] >> src_offset) & (uint8_t)mask) << dst_offset));

        src_offset += nbits;
        if (src_offset >= 8) {
            s_idx++;
            src_offset %= 8;
        }

        dst_offset += nbits;
        if (dst_offset >= 8) {
            d_idx++;
            dst_offset %= 8;
        }

        size -= nbits;
    }

    /* The middle bits. We are aligned on a source byte which needs to be
     * copied to two (or one in the degenerate case) destination bytes.
     *
     * 		      src[s_idx]
     * 		   +---------------+
     *  	   |7 6 5 4 3 2 1 0|
     * 		   +---------------+
     *              | | | | | | | |
     * 		    V V V V V V V V
     *   +---------------+---------------+
     *   |7 6 5 4 3 2 1 0|7 6 5 4 3 2 1 0|
     *   +---------------+---------------+
     *     dst[d_idx+1]      dst[d_idx]
     *
     *
     * Calculate shifts and masks.  See diagrams below.  MASK_LO in this
     * example is 0x1f (the low five bits) and MASK_HI is 0xe0 (the high three
     * bits). SHIFT is three since the source must be shifted right three bits
     * to line up with the destination.
     */
    shift   = dst_offset;
    mask_lo = ((size_t)1 << (8 - shift)) - 1;
    mask_hi = (~mask_lo) & 0xff;

    for (/*void*/; size > 8; size -= 8, d_idx++, s_idx++) {
        if (shift) {
            dst[d_idx + 0] &= (uint8_t)(~(mask_lo << shift));
            dst[d_idx + 0] |= (uint8_t)((src[s_idx] & mask_lo) << shift);
            dst[d_idx + 1] &= (uint8_t)(~(mask_hi >> (8 - shift)));
            dst[d_idx + 1] |= (uint8_t)((src[s_idx] & mask_hi) >> (8 - shift));
        }
        else
            dst[d_idx] = src[s_idx];
    }

    /* Finish up */
    while (size > 0) {
        size_t nbits = (size_t)MIN3(size, 8 - dst_offset, 8 - src_offset);
        size_t mask  = ((size_t)1 << nbits) - 1;

        dst[d_idx] &= (uint8_t)(~(mask << dst_offset));
        dst[d_idx] = (uint8_t)(dst[d_idx] | (((src[s_idx] >> src_offset) & (uint8_t)mask) << dst_offset));

        src_offset += nbits;
        if (src_offset >= 8) {
            s_idx++;
            src_offset %= 8;
        }

        dst_offset += nbits;
        if (dst_offset >= 8) {
            d_idx++;
            dst_offset %= 8;
        }

        size -= nbits;
    }

    FUNC_LEAVE_NOAPI_VOID
} /* end H5T__bit_copy() */

/*-------------------------------------------------------------------------
 * Function:    H5T__bit_shift
 *
 * Purpose:     Simulation of hardware shifting.  Shifts a bit vector
 *              in a way similar to shifting a variable value, like
 *              value <<= 3, or value >>= 16.  SHIFT_DIST is positive for
 *              left shift, negative for right shift.  The bit vector starts
 *              at OFFSET and is SIZE long.  The caller has to make sure
 *              SIZE+OFFSET doesn't exceed the size of BUF.
 *
 *              For example, if we have a bit sequence 00011100, offset=2,
 *              size=3, shift_dist=2, the result will be 00010000.
 *
 * Return:      void
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5T__bit_shift(uint8_t *buf, ssize_t shift_dist, size_t offset, size_t size)
{
    uint8_t tmp_buf[512];        /* Temporary buffer */
    H5WB_t *wb        = NULL;    /* Wrapped buffer for temporary buffer */
    herr_t  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(buf);
    assert(size);

    if (shift_dist) {
        size_t abs_shift_dist = (size_t)ABS(shift_dist);

        if (abs_shift_dist >= size)
            H5T__bit_set(buf, offset, size, 0);
        else {
            size_t   buf_size = (size / 8) + 1; /* Size of shift buffer needed */
            uint8_t *shift_buf;                 /* Pointer to shift buffer */

            /* Wrap the local buffer for serialized header info */
            if (NULL == (wb = H5WB_wrap(tmp_buf, sizeof(tmp_buf))))
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't wrap buffer");

            /* Get a pointer to a buffer that's large enough  */
            if (NULL == (shift_buf = (uint8_t *)H5WB_actual(wb, buf_size)))
                HGOTO_ERROR(H5E_DATATYPE, H5E_NOSPACE, FAIL, "can't get actual buffer");

            /* Shift vector by making copies */
            if (shift_dist > 0) { /* left shift */
                /* Copy part to be shifted to a temporary buffer */
                H5T__bit_copy(shift_buf, (size_t)0, buf, offset, size - abs_shift_dist);

                /* Copy it back to the original buffer */
                H5T__bit_copy(buf, offset + abs_shift_dist, shift_buf, (size_t)0, size - abs_shift_dist);

                /* Zero-set the left part*/
                H5T__bit_set(buf, offset, abs_shift_dist, 0);
            }
            else { /* right shift */
                H5T__bit_copy(shift_buf, (size_t)0, buf, offset + abs_shift_dist, size - abs_shift_dist);
                H5T__bit_copy(buf, offset, shift_buf, (size_t)0, size - abs_shift_dist);
                H5T__bit_set(buf, offset + size - abs_shift_dist, abs_shift_dist, 0);
            }
        } /* end else */
    }     /* end if */

done:
    /* Release resources */
    if (wb && H5WB_unwrap(wb) < 0)
        HDONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "can't close wrapped buffer");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__bit_shift() */

/*-------------------------------------------------------------------------
 * Function:    H5T__bit_get_d
 *
 * Purpose:     Return a small bit sequence as a number.  Bit vector starts
 *              at OFFSET and is SIZE bits long.
 *
 * Return:      The bit sequence interpreted as an unsigned integer
 *
 *-------------------------------------------------------------------------
 */
uint64_t
H5T__bit_get_d(uint8_t *buf, size_t offset, size_t size)
{
    uint64_t val = 0;
    size_t   i, hs;
    uint64_t ret_value = 0; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    assert(8 * sizeof(val) >= size);

    H5T__bit_copy((uint8_t *)&val, (size_t)0, buf, offset, size);
    switch (H5T_native_order_g) {
        case H5T_ORDER_LE:
            break;

        case H5T_ORDER_BE:
            for (i = 0, hs = sizeof(val) / 2; i < hs; i++) {
                uint8_t tmp                              = ((uint8_t *)&val)[i];
                ((uint8_t *)&val)[i]                     = ((uint8_t *)&val)[sizeof(val) - (i + 1)];
                ((uint8_t *)&val)[sizeof(val) - (i + 1)] = tmp;
            }
            break;

        case H5T_ORDER_ERROR:
        case H5T_ORDER_VAX:
        case H5T_ORDER_NONE:
        case H5T_ORDER_MIXED:
        default:
            /* This function can't return errors */
            assert(0 && "unknown byte order");
    }

    /* Set return value */
    ret_value = val;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__bit_get_d() */

/*-------------------------------------------------------------------------
 * Function:    H5T__bit_set_d
 *
 * Purpose:     Sets part of a bit vector to the specified unsigned value.
 *
 * Return:      void
 *
 *-------------------------------------------------------------------------
 */
void
H5T__bit_set_d(uint8_t *buf, size_t offset, size_t size, uint64_t val)
{
    size_t i, hs;

    FUNC_ENTER_PACKAGE_NOERR

    assert(8 * sizeof(val) >= size);

    switch (H5T_native_order_g) {
        case H5T_ORDER_LE:
            break;

        case H5T_ORDER_BE:
            for (i = 0, hs = sizeof(val) / 2; i < hs; i++) {
                uint8_t tmp                              = ((uint8_t *)&val)[i];
                ((uint8_t *)&val)[i]                     = ((uint8_t *)&val)[sizeof(val) - (i + 1)];
                ((uint8_t *)&val)[sizeof(val) - (i + 1)] = tmp;
            }
            break;

        case H5T_ORDER_ERROR:
        case H5T_ORDER_VAX:
        case H5T_ORDER_NONE:
        case H5T_ORDER_MIXED:
        default:
            HDabort();
    }

    H5T__bit_copy(buf, offset, (uint8_t *)&val, (size_t)0, size);

    FUNC_LEAVE_NOAPI_VOID
} /* end H5T__bit_set_d() */

/*-------------------------------------------------------------------------
 * Function:    H5T__bit_set
 *
 * Purpose:     Sets or clears bits in a contiguous region of a vector
 *              beginning at bit OFFSET and continuing for SIZE bits.
 *
 * Return:      void
 *
 *-------------------------------------------------------------------------
 */
void
H5T__bit_set(uint8_t *buf, size_t offset, size_t size, bool value)
{
    int idx;

    FUNC_ENTER_PACKAGE_NOERR

    /* Normalize */
    idx = (int)offset / 8;
    offset %= 8;

    /* The first partial byte */
    if (size && offset % 8) {
        size_t   nbits = MIN(size, 8 - offset);
        unsigned mask  = ((unsigned)1 << nbits) - 1;

        if (value)
            buf[idx] = (uint8_t)(buf[idx] | (mask << offset));
        else
            buf[idx] &= (uint8_t)(~(mask << offset));

        idx++;
        size -= nbits;
    }

    /* The middle bytes */
    while (size >= 8) {
        buf[idx++] = (uint8_t)(value ? 0xff : 0x00);
        size -= 8;
    }

    /* The last partial byte */
    if (size) {
        if (value)
            buf[idx] |= (uint8_t)(((unsigned)1 << size) - 1);
        else
            buf[idx] &= (uint8_t)(~(((unsigned)1 << size) - 1));
    }

    FUNC_LEAVE_NOAPI_VOID
} /* end H5T__bit_set() */

/*-------------------------------------------------------------------------
 * Function:    H5T__bit_find
 *
 * Purpose:     Finds the first bit with the specified VALUE within a region
 *              of a bit vector.  The region begins at OFFSET and continues
 *              for SIZE bits, but the region can be searched from the least
 *              significant end toward the most significant end(H5T_BIT_LSB
 *              as DIRECTION), or from the most significant end to the least
 *              significant end(H5T_BIT_MSB as DIRECTION).
 *
 * Return:      Success:    The position of the bit found, relative to
 *                          the offset.
 *
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
ssize_t
H5T__bit_find(const uint8_t *buf, size_t offset, size_t size, H5T_sdir_t direction, bool value)
{
    ssize_t base = (ssize_t)offset;
    ssize_t idx, i;
    size_t  iu;
    ssize_t ret_value = (-1); /* Return value */

    /* Use FUNC_ENTER_PACKAGE_NOERR here to avoid performance issues */
    FUNC_ENTER_PACKAGE_NOERR

    /* Some functions call this with value=true */
    assert(true == 1);

    switch (direction) {
        case H5T_BIT_LSB:
            /* Calculate index */
            idx = (ssize_t)(offset / 8);
            offset %= 8;

            /* Beginning */
            if (offset) {
                for (iu = offset; iu < 8 && size > 0; iu++, size--)
                    if (value == (bool)((buf[idx] >> iu) & 0x01))
                        HGOTO_DONE(8 * idx + (ssize_t)iu - base);

                offset = 0;
                idx++;
            } /* end if */

            /* Middle */
            while (size >= 8) {
                if ((value ? 0x00 : 0xff) != buf[idx])
                    for (i = 0; i < 8; i++)
                        if (value == (bool)((buf[idx] >> i) & 0x01))
                            HGOTO_DONE(8 * idx + i - base);

                size -= 8;
                idx++;
            } /* end while */

            /* End */
            for (i = 0; i < (ssize_t)size; i++)
                if (value == (bool)((buf[idx] >> i) & 0x01))
                    HGOTO_DONE(8 * idx + i - base);
            break;

        case H5T_BIT_MSB:
            /* Calculate index */
            idx = (ssize_t)((offset + size - 1) / 8);
            offset %= 8;

            /* Beginning */
            if (size > 8 - offset && (offset + size) % 8) {
                for (iu = (offset + size) % 8; iu > 0; --iu, --size)
                    if (value == (bool)((buf[idx] >> (iu - 1)) & 0x01))
                        HGOTO_DONE(8 * idx + (ssize_t)(iu - 1) - base);

                --idx;
            } /* end if */

            /* Middle */
            while (size >= 8) {
                if ((value ? 0x00 : 0xff) != buf[idx]) {
                    for (i = 7; i >= 0; --i)
                        if (value == (bool)((buf[idx] >> i) & 0x01))
                            HGOTO_DONE(8 * idx + i - base);
                } /* end if */

                size -= 8;
                --idx;
            } /* end while */

            /* End */
            if (size > 0) {
                for (iu = offset + size; iu > offset; --iu)
                    if (value == (bool)((buf[idx] >> (iu - 1)) & 0x01))
                        HGOTO_DONE(8 * idx + (ssize_t)(iu - 1) - base);
            }
            break;

        default:
            assert(0 && "Unknown bit search direction");
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__bit_find() */

/*-------------------------------------------------------------------------
 * Function:    H5T__bit_inc
 *
 * Purpose:     Increment part of a bit field by adding 1.  The bit field
 *              starts with bit position START and is SIZE bits long.
 *
 * Return:      The carry-out value.  true if overflows, false otherwise.
 *
 *-------------------------------------------------------------------------
 */
bool
H5T__bit_inc(uint8_t *buf, size_t start, size_t size)
{
    size_t   idx   = start / 8;
    unsigned carry = 1;
    unsigned acc, mask;

    /* Use FUNC_ENTER_PACKAGE_NOERR here to avoid performance issues */
    FUNC_ENTER_PACKAGE_NOERR

    assert(buf);

    start %= 8;

    /* The first partial byte */
    if (start) {
        if (size + start < 8)
            mask = ((unsigned)1 << size) - 1;
        else
            mask = ((unsigned)1 << (8 - start)) - 1;
        acc = ((unsigned)buf[idx] >> start) & mask;
        acc++;
        carry = acc & ((unsigned)1 << MIN(size, 8 - start));
        buf[idx] &= (uint8_t)(~(mask << start));
        buf[idx] = (uint8_t)(buf[idx] | ((acc & mask) << start));
        size -= MIN(size, 8 - start);
        start = 0;
        idx++;
    }

    /* The middle */
    while (carry && size >= 8) {
        acc = buf[idx];
        acc++;
        carry    = acc & 0x100;
        buf[idx] = (uint8_t)(acc & 0xff);
        idx++;
        size -= 8;
    }

    /* The last bits */
    if (carry && size > 0) {
        mask = ((unsigned)1 << size) - 1;
        acc  = buf[idx] & mask;
        acc++;
        carry = acc & ((unsigned)1 << size);
        buf[idx] &= (uint8_t)(~mask);
        buf[idx] |= (uint8_t)(acc & mask);
    }

    FUNC_LEAVE_NOAPI(carry ? true : false)
} /* end H5T__bit_inc() */

/*-------------------------------------------------------------------------
 * Function:    H5T__bit_dec
 *
 * Purpose:     Decrement part of a bit field by subtracting 1.  The bit
 *              field starts with bit position START and is SIZE bits long.
 *
 * Return:      The "borrow-in" value. It's true if underflows, false
 *              otherwise.
 *
 *-------------------------------------------------------------------------
 */
bool
H5T__bit_dec(uint8_t *buf, size_t start, size_t size)
{
    size_t   idx = start / 8;
    size_t   pos = start % 8;
    uint8_t  tmp;
    unsigned borrow = 0;

    /* Use FUNC_ENTER_PACKAGE_NOERR here to avoid performance issues */
    FUNC_ENTER_PACKAGE_NOERR

    assert(buf);
    assert(size);

    /* The first partial byte */
    if ((size + start - 1) / 8 > idx) {
        /* The bit sequence doesn't end in the same byte as starts */

        /* Example:  a sequence like 11000100 and start = 3.  We subtract 00001000 from
         * it and get 10111100.  If a sequence is 00000111, we do right shift for START
         * bits and get 00000000.  So we need to borrow from higher byte when we subtract
         * 00001000.
         */
        if (!(buf[idx] >> pos))
            borrow = 1;
        buf[idx] = (uint8_t)(buf[idx] - (1 << pos));
        idx++;
        size -= (8 - pos);

        /* The middle bytes */
        while (borrow && size >= 8) {
            if (buf[idx])
                borrow = 0;
            buf[idx]--;

            idx++;
            size -= 8;
        }

        /* The last partial byte */
        if (borrow && size > 0) {
            /* Similar to the first byte case, where sequence ends in the same byte as starts */
            tmp = buf[idx];
            buf[idx]--;
            if ((buf[idx] >> size) != tmp >> size)
                buf[idx] = (uint8_t)(buf[idx] + (1 << size));
        }
    }
    else {
        /* The bit sequence ends in the same byte as starts */

        /* Example: a sequence like 11000100 and pos=3, size=3.  We subtract 00001000
         * and get 10111100.  A bit is borrowed from 6th bit(buf[idx]>>6=00000010, tmp>>6=00000011,
         * not equal).  We need to put this bit back by increment 1000000.
         */
        tmp      = buf[idx];
        buf[idx] = (uint8_t)(buf[idx] - (1 << pos));
        if ((buf[idx] >> (pos + size)) != tmp >> (pos + size)) {
            buf[idx] = (uint8_t)(buf[idx] + (1 << (pos + size)));
            borrow   = 1;
        }
    }

    FUNC_LEAVE_NOAPI(borrow ? true : false)
} /* end H5T__bit_dec() */

/*-------------------------------------------------------------------------
 * Function:    H5T__bit_neg
 *
 * Purpose:     Negate part of a bit sequence.  The bit field starts with
 *              bit position START and is SIZE bits long.
 *
 * Return:      void
 *
 *-------------------------------------------------------------------------
 */
void
H5T__bit_neg(uint8_t *buf, size_t start, size_t size)
{
    size_t  idx = start / 8;
    size_t  pos = start % 8;
    uint8_t tmp[1];

    /* Use FUNC_ENTER_PACKAGE_NOERR here to avoid performance issues */
    FUNC_ENTER_PACKAGE_NOERR

    assert(buf);
    assert(size);

    /* The first partial byte */
    tmp[0] = (uint8_t)~buf[idx];

    /* Simply copy the negated bit field back to the original byte */
    if ((size + start - 1) / 8 > idx) { /*bit sequence doesn't end in the same byte as starts*/
        H5T__bit_copy(&(buf[idx]), pos, tmp, pos, (8 - pos));
        idx++;
        size -= (8 - pos);

        /* The middle bytes */
        while (size >= 8) {
            buf[idx] = (uint8_t) ~(buf[idx]);
            idx++;
            size -= 8;
        }

        /* The last partial byte */
        if (size > 0) {
            /* Similar to the first byte case, where sequence ends in the same byte as starts */
            tmp[0] = (uint8_t)~buf[idx];
            H5T__bit_copy(&(buf[idx]), (size_t)0, tmp, (size_t)0, size);
        }
    }
    else {
        /* bit sequence ends in the same byte as starts */
        H5T__bit_copy(&(buf[idx]), pos, tmp, pos, size);
    }

    FUNC_LEAVE_NOAPI_VOID
} /* end H5T__bit_neg() */
