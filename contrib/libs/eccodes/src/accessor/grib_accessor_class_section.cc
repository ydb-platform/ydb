/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_section.h"

grib_accessor_section_t _grib_accessor_section{};
grib_accessor* grib_accessor_section = &_grib_accessor_section;

void grib_accessor_section_t::init(const long len, grib_arguments* arg)
{
    grib_accessor_gen_t::init(len, arg);
    sub_section_ = grib_section_create(grib_handle_of_accessor(this), this);
    length_      = 0;
    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
}

void grib_accessor_section_t::dump(eccodes::Dumper* dumper)
{
    dumper->dump_section(this, sub_section_->block);
}

long grib_accessor_section_t::byte_count()
{
    if (!length_ || grib_handle_of_accessor(this)->loader) {
        if (name_[1] == '_')
            return 0;

        /* printf("adjusting sizes SECTION %s is %ld %ld\n",a->name,(long)a->offset,(long)length_ ); */
        grib_section_adjust_sizes(sub_section_, grib_handle_of_accessor(this)->loader != NULL, 0);
        /* printf("                SECTION %s is %ld %ld\n",a->name,(long)a->offset,(long)length_ );  */
    }

    /* printf("SECTION %s is %ld %d\n",a->name,a->length,sub_section_ ->aclength != NULL);  */
    return length_;
}

long grib_accessor_section_t::next_offset()
{
    return offset_ + byte_count();
}

void grib_accessor_section_t::destroy(grib_context* ct)
{
    grib_section_delete(ct, sub_section_);
    grib_accessor_gen_t::destroy(ct);
}

long grib_accessor_section_t::get_native_type()
{
    return GRIB_TYPE_SECTION;
}

grib_section* grib_accessor_section_t::sub_section()
{
    return sub_section_;
}

void grib_accessor_section_t::update_size(size_t length)
{
    size_t size = 1;
    long len    = length;
    ECCODES_ASSERT(length <= 0x7fffffff);
    if (sub_section_->aclength) {
        int e = sub_section_->aclength->pack_long(&len, &size);
        ECCODES_ASSERT(e == GRIB_SUCCESS);
        // printf("update_length %s %ld %ld\n", sub_section_ ->aclength->name,
        //        (long)sub_section_ ->aclength->offset,
        //        (long)sub_section_ ->aclength->length);
    }

    sub_section_->length = length_ = length;
    sub_section_->padding          = 0;

    // printf("update_size %s %ld\n", a->name, length_ );

    ECCODES_ASSERT(length_ >= 0);
}

grib_accessor* grib_accessor_section_t::next(grib_accessor* a, int explore)
{
    grib_accessor* next = NULL;
    if (explore) {
        next = a->sub_section_->block->first;
        if (!next)
            next = a->next_;
    }
    else {
        next = a->next_;
    }
    if (!next) {
        if (a->parent_->owner)
            next = a->parent_->owner->next(a->parent_->owner, 0);
    }
    return next;
}
