/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_section_pointer.h"

grib_accessor_section_pointer_t _grib_accessor_section_pointer{};
grib_accessor* grib_accessor_section_pointer = &_grib_accessor_section_pointer;

void grib_accessor_section_pointer_t::init(const long len, grib_arguments* arg)
{
    grib_accessor_gen_t::init(len, arg);

    int n          = 0;
    sectionOffset_ = arg->get_name(grib_handle_of_accessor(this), n++);
    sectionLength_ = arg->get_name(grib_handle_of_accessor(this), n++);
    sectionNumber_ = arg->get_long(grib_handle_of_accessor(this), n++);

    ECCODES_ASSERT(sectionNumber_ < MAX_NUM_SECTIONS);

    grib_handle_of_accessor(this)->section_offset[sectionNumber_] = (char*)sectionOffset_;
    grib_handle_of_accessor(this)->section_length[sectionNumber_] = (char*)sectionLength_;

    /* printf("++++++++++++++ GRIB_API:  creating section_pointer%d %s %s\n", */
    /* sectionNumber,sectionLength,sectionLength_ ); */

    if (grib_handle_of_accessor(this)->sections_count < sectionNumber_)
        grib_handle_of_accessor(this)->sections_count = sectionNumber_;

    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
    flags_ |= GRIB_ACCESSOR_FLAG_HIDDEN;
    flags_ |= GRIB_ACCESSOR_FLAG_FUNCTION;
    flags_ |= GRIB_ACCESSOR_FLAG_EDITION_SPECIFIC;
    length_ = 0;
}

long grib_accessor_section_pointer_t::get_native_type()
{
    return GRIB_TYPE_BYTES;
}

int grib_accessor_section_pointer_t::unpack_string(char* v, size_t* len)
{
    //   unsigned char* p=NULL;
    //   char* s=v;
    //   int i;
    //   long length=byte_count();
    //   if (*len < length) return GRIB_ARRAY_TOO_SMALL;
    //
    //   p  = grib_handle_of_accessor(this)->buffer->data + byte_offset();
    //   for (i = 0; i < length; i++)  {
    //     snprintf (s,64,"%02x", *(p++));
    //     s+=2;
    //   }
    //   *len=length;

    snprintf(v, 64, "%ld_%ld", byte_offset(), byte_count());
    return GRIB_SUCCESS;
}

long grib_accessor_section_pointer_t::byte_count()
{
    long sectionLength = 0;

    int ret = grib_get_long(grib_handle_of_accessor(this), sectionLength_, &sectionLength);
    if (ret) {
        grib_context_log(context_, GRIB_LOG_ERROR,
                         "Unable to get %s %s",
                         sectionLength_, grib_get_error_message(ret));
        return -1;
    }

    return sectionLength;
}

long grib_accessor_section_pointer_t::byte_offset()
{
    long sectionOffset = 0;

    int ret = grib_get_long(grib_handle_of_accessor(this), sectionOffset_, &sectionOffset);
    if (ret) {
        grib_context_log(context_, GRIB_LOG_ERROR,
                         "Unable to get %s %s",
                         sectionOffset_, grib_get_error_message(ret));
        return -1;
    }

    return sectionOffset;
}
