/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_g1_message_length.h"

grib_accessor_g1_message_length_t _grib_accessor_g1_message_length{};
grib_accessor* grib_accessor_g1_message_length = &_grib_accessor_g1_message_length;

void grib_accessor_g1_message_length_t::init(const long len, grib_arguments* args)
{
    grib_accessor_section_length_t::init(len, args);
    sec4_length_ = args->get_name(grib_handle_of_accessor(this), 0);
}

int grib_get_g1_message_size(grib_handle* h, grib_accessor* tl, grib_accessor* s4,
                             long* total_length, long* sec4_len)
{
    unsigned long tlen, slen;
    long off;

    if (!tl)
        return GRIB_NOT_FOUND;
    if (!s4) {
        *sec4_len     = 0;
        off           = tl->offset_ * 8;
        *total_length = grib_decode_unsigned_long(h->buffer->data, &off, tl->length_ * 8);
        return GRIB_SUCCESS;
    }

    off  = tl->offset_ * 8;
    tlen = grib_decode_unsigned_long(h->buffer->data, &off, tl->length_ * 8);

    off  = s4->offset_ * 8;
    slen = grib_decode_unsigned_long(h->buffer->data, &off, s4->length_ * 8);

    /* printf("\nlarge grib tlen=%ld slen=%ld diff=%ld\n",tlen&0x7fffff,slen,tlen-slen); */

    if (slen < 120 && (tlen & 0x800000)) {
        /* printf("DECODING large grib tlen=%ld slen=%ld\n",tlen,slen); */
        tlen &= 0x7fffff;
        tlen *= 120;
        tlen -= slen;
        tlen += 4;

        slen = tlen - s4->offset_ - 4; /* 4 is for 7777 */
    }

    *total_length = tlen;
    *sec4_len     = slen;

    return GRIB_SUCCESS;
}

int grib_accessor_g1_message_length_t::pack_long(const long* val, size_t* len)
{
    /*grib_accessor* super = *(cclass_ ->super);*/

    /* Here we assume that the totalLength will be coded AFTER the section4 length, and
       the section4 length will be overwritten by the totalLength accessor for large GRIBs */

    grib_accessor* s4 = grib_find_accessor(grib_handle_of_accessor(this), sec4_length_);
    long tlen, slen;
    long t120;
    int ret;

    tlen = *val;
    if ((tlen < 0x800000 || !context_->gribex_mode_on) && tlen < 0xFFFFFF) {
        /* printf("ENCODING small grib total = %ld\n",tlen); */
        /*return super->pack_long(a,val,len);*/

        /* Do not directly call pack_long on base class */
        /* because in this special case we want to skip the checks. */
        /* So we call the helper function which has an extra argument */
        return pack_long_unsigned_helper(val, len, /*check=*/0);
    }

    if (!s4)
        return GRIB_NOT_FOUND;

    /* special case for large GRIBs */
    tlen -= 4;
    t120 = (tlen + 119) / 120;
    slen = t120 * 120 - tlen;
    tlen = 0x800000 | t120;

    *len = 1;
    if ((ret = s4->pack_long(&slen, len)) != GRIB_SUCCESS)
        return ret;

    *len = 1;
    /* Do not do the length checks in this special case */
    if ((ret = pack_long_unsigned_helper(&tlen, len, /*check=*/0)) != GRIB_SUCCESS)
        return ret;

    // if((ret = super->pack_long(a,&tlen,len)) != GRIB_SUCCESS) return ret;

    {
        long total_length = -1, sec4_length = -1;
        grib_get_g1_message_size(grib_handle_of_accessor(this), this,
                                 grib_find_accessor(grib_handle_of_accessor(this), sec4_length_),
                                 &total_length, &sec4_length);
        if (total_length != *val) {
            grib_context_log(context_, GRIB_LOG_ERROR,
                             "%s %s: Failed to set GRIB1 message length to %ld"
                             " (actual length=%ld)",
                             class_name_, __func__, *val, total_length);
            grib_context_log(context_, GRIB_LOG_ERROR, "Hint: Try encoding as GRIB2\n");
            return GRIB_ENCODING_ERROR;
        }
    }

    return GRIB_SUCCESS;
}

int grib_accessor_g1_message_length_t::unpack_long(long* val, size_t* len)
{
    int ret;
    long total_length, sec4_length;

    if ((ret = grib_get_g1_message_size(grib_handle_of_accessor(this), this,
                                        grib_find_accessor(grib_handle_of_accessor(this), sec4_length_),
                                        &total_length, &sec4_length)) != GRIB_SUCCESS) {
        return ret;
    }

    *val = total_length;
    return GRIB_SUCCESS;
}
