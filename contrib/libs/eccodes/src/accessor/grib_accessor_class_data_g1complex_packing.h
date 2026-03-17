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

#include "grib_accessor_class_data_complex_packing.h"

class grib_accessor_data_g1complex_packing_t : public grib_accessor_data_complex_packing_t
{
public:
    grib_accessor_data_g1complex_packing_t() :
        grib_accessor_data_complex_packing_t() { class_name_ = "data_g1complex_packing"; }
    grib_accessor* create_empty_accessor() override { return new grib_accessor_data_g1complex_packing_t{}; }
    int pack_double(const double* val, size_t* len) override;
    void init(const long, grib_arguments*) override;

private:
    const char* N_ = nullptr;
    const char* half_byte_ = nullptr;
    const char* packingType_ = nullptr;
    const char* ieee_packing_ = nullptr;
    const char* precision_ = nullptr;
};
