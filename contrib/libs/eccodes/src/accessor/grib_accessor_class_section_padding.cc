/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_section_padding.h"

grib_accessor_section_padding_t _grib_accessor_section_padding{};
grib_accessor* grib_accessor_section_padding = &_grib_accessor_section_padding;

size_t grib_accessor_section_padding_t::preferred_size(int from_handle)
{
    grib_accessor* b              = this;
    grib_accessor* section_length = 0;
    long length                   = 0;
    size_t size                   = 1;
    long alength                  = 0;

    if (!from_handle) {
        if (preserve_)
            return length_;
        else
            return 0;
    }

    /* The section length should be a parameter */
    while (section_length == NULL && b != NULL) {
        section_length = b->parent_->aclength;
        b              = b->parent_->owner;
    }

    if (!section_length) {
        /* printf("PADDING is no !section_length\n"); */
        return 0;
    }

    if (section_length->unpack_long(&length, &size) == GRIB_SUCCESS) {
        if (length)
            alength = length - offset_ + section_length->parent_->owner->offset_;
        else
            alength = 0;

        /*ECCODES_ASSERT(length_ >=0);*/

        if (alength < 0)
            alength = 0;

        /* printf("PADDING is %ld\n",length_ ); */
    }
    else {
        /* printf("PADDING unpack fails\n"); */
    }

    return alength;
}

void grib_accessor_section_padding_t::init(const long len, grib_arguments* arg)
{
    grib_accessor_padding_t::init(len, arg);
    preserve_ = 1; /* This should be a parameter */
    length_   = preferred_size(1);
}
