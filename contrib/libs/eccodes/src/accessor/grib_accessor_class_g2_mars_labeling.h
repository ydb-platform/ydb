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

#include "grib_accessor_class_gen.h"

class grib_accessor_g2_mars_labeling_t : public grib_accessor_gen_t
{
public:
    grib_accessor_g2_mars_labeling_t() :
        grib_accessor_gen_t() { class_name_ = "g2_mars_labeling"; }
    grib_accessor* create_empty_accessor() override { return new grib_accessor_g2_mars_labeling_t{}; }
    long get_native_type() override;
    int pack_long(const long* val, size_t* len) override;
    int pack_string(const char*, size_t* len) override;
    int unpack_long(long* val, size_t* len) override;
    int unpack_string(char*, size_t* len) override;
    int value_count(long*) override;
    void init(const long, grib_arguments*) override;

private:
    int index_ = 0;
    const char* the_class_ = nullptr;
    const char* stream_ = nullptr;
    const char* type_ = nullptr;
    const char* expver_ = nullptr;
    const char* typeOfProcessedData_ = nullptr;
    const char* productDefinitionTemplateNumber_ = nullptr;
    const char* stepType_ = nullptr;
    const char* derivedForecast_ = nullptr;
    const char* typeOfGeneratingProcess_ = nullptr;

    int extra_set(long val);
};
