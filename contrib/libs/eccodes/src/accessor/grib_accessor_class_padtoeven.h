/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#pragma once

#include "grib_accessor_class_padding.h"

class grib_accessor_padtoeven_t : public grib_accessor_padding_t
{
public:
    grib_accessor_padtoeven_t() :
        grib_accessor_padding_t() { class_name_ = "padtoeven"; }
    grib_accessor* create_empty_accessor() override { return new grib_accessor_padtoeven_t{}; }
    void init(const long, grib_arguments*) override;
    size_t preferred_size(int) override;

private:
    const char* section_offset_ = nullptr;
    const char* section_length_ = nullptr;
};
