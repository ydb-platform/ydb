/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_bufr_string_values.h"
#include "grib_accessor_class_bufr_data_array.h"

grib_accessor_bufr_string_values_t _grib_accessor_bufr_string_values{};
grib_accessor* grib_accessor_bufr_string_values = &_grib_accessor_bufr_string_values;

void grib_accessor_bufr_string_values_t::init(const long len, grib_arguments* args)
{
    grib_accessor_ascii_t::init(len, args);
    int n             = 0;
    dataAccessorName_ = args->get_name(grib_handle_of_accessor(this), n++);
    dataAccessor_     = NULL;
    length_           = 0;
    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
}

void grib_accessor_bufr_string_values_t::dump(eccodes::Dumper* dumper)
{
    dumper->dump_string_array(this, NULL);
}

grib_accessor* grib_accessor_bufr_string_values_t::get_accessor()
{
    if (!dataAccessor_) {
        dataAccessor_ = grib_find_accessor(grib_handle_of_accessor(this), dataAccessorName_);
    }
    return dataAccessor_;
}

int grib_accessor_bufr_string_values_t::unpack_string_array(char** buffer, size_t* len)
{
    grib_context* c = context_;
    grib_vsarray* stringValues = NULL;
    size_t l = 0, tl;
    size_t i, j, n = 0;
    char** b = buffer;

    grib_accessor_bufr_data_array_t* data = dynamic_cast<grib_accessor_bufr_data_array_t*>(get_accessor());
    if (!data)
        return GRIB_NOT_FOUND;

    stringValues = data->accessor_bufr_data_array_get_stringValues();

    n = grib_vsarray_used_size(stringValues);

    tl = 0;
    for (j = 0; j < n; j++) {
        l = grib_sarray_used_size(stringValues->v[j]);
        tl += l;

        if (tl > *len)
            return GRIB_ARRAY_TOO_SMALL;

        for (i = 0; i < l; i++) {
            *(b++) = grib_context_strdup(c, stringValues->v[j]->v[i]);
        }
    }
    *len = tl;

    return GRIB_SUCCESS;
}

int grib_accessor_bufr_string_values_t::unpack_string(char* val, size_t* len)
{
    return GRIB_NOT_IMPLEMENTED;
}

int grib_accessor_bufr_string_values_t::value_count(long* rlen)
{
    grib_accessor* descriptors = get_accessor();
    return descriptors->value_count(rlen);
}

void grib_accessor_bufr_string_values_t::destroy(grib_context* c)
{
    grib_accessor_ascii_t::destroy(c);
}
