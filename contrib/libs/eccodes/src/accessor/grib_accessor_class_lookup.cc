/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_lookup.h"

grib_accessor_lookup_t _grib_accessor_lookup{};
grib_accessor* grib_accessor_lookup = &_grib_accessor_lookup;

void grib_accessor_lookup_t::init(const long len, grib_arguments* arg)
{
    grib_accessor_long_t::init(len, arg);
    length_  = 0;
    llength_ = len;
    loffset_ = arg->get_long(grib_handle_of_accessor(this), 0);
    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
    real_name_ = arg->get_expression(grib_handle_of_accessor(this), 1);
}

void grib_accessor_lookup_t::post_init()
{
    if (real_name_) {
        grib_dependency_observe_expression(this, real_name_);
    }
}

void grib_accessor_lookup_t::dump(eccodes::Dumper* dumper)
{
    unsigned char bytes[1024] = {0,};
    char msg[1024] = {0,};
    char buf[2048];
    unsigned long v = 0;

    size_t llen = llength_;
    unpack_bytes(bytes, &llen);
    bytes[llen] = 0;
    for (size_t i = 0; i < llen; i++) {
        msg[i] = isprint(bytes[i]) ? bytes[i] : '?';
        v <<= 8;
        v |= bytes[i];
    }

    msg[llen] = 0;

    snprintf(buf, sizeof(buf), "%s %lu %ld-%ld", msg, v, (long)offset_ + loffset_, (long)llength_);

    dumper->dump_long(this, buf);
}

int grib_accessor_lookup_t::unpack_string(char* v, size_t* len)
{
    unsigned char bytes[1024] = {0,};

    size_t llen = llength_;
    unpack_bytes(bytes, &llen);
    bytes[llen] = 0;

    for (size_t i = 0; i < llen; i++) {
        v[i] = isprint(bytes[i]) ? bytes[i] : '?';
    }

    v[llen] = 0;
    if (llen == 1 && v[0] == '?') {
        /* Try unpack as long */
        size_t length = 10;
        long lval     = 0;
        int err       = unpack_long(&lval, &length);
        if (!err) {
            char str[5];
            int conv = snprintf(str, sizeof(str), "%ld", lval);
            if (conv == 1) {
                v[0] = str[0];
            }
        }
    }

    return GRIB_SUCCESS;
}

int grib_accessor_lookup_t::unpack_long(long* val, size_t* len)
{
    grib_handle* h = grib_handle_of_accessor(this);

    long pos = (offset_ + loffset_) * 8;

    if (len[0] < 1) {
        grib_context_log(context_, GRIB_LOG_ERROR, "Wrong size for %s it contains %d values ", name_, 1);
        len[0] = 0;
        return GRIB_ARRAY_TOO_SMALL;
    }

    /* This is used when reparsing or rebuilding */
    if (h->loader) {
        ECCODES_ASSERT(*len == 1);
        return h->loader->lookup_long(h->context, h->loader, name_, val);
    }

    val[0] = grib_decode_unsigned_long(h->buffer->data, &pos, llength_ * 8);
    len[0] = 1;

    /*printf("###########lookup unpack_long: %s %ld %ld\n",name_ , pos/8, val[0]);*/

    return GRIB_SUCCESS;
}

int grib_accessor_lookup_t::pack_long(const long* val, size_t* len)
{
    return GRIB_NOT_IMPLEMENTED;
}

long grib_accessor_lookup_t::byte_count()
{
    return llength_;
}

long grib_accessor_lookup_t::byte_offset()
{
    return loffset_;
}

int grib_accessor_lookup_t::notify_change(grib_accessor* changed)
{
    /* Forward changes */
    return grib_dependency_notify_change(this);
}
