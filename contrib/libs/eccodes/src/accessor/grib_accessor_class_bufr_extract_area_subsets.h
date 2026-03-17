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

class grib_accessor_bufr_extract_area_subsets_t : public grib_accessor_gen_t
{
public:
    grib_accessor_bufr_extract_area_subsets_t() :
        grib_accessor_gen_t() { class_name_ = "bufr_extract_area_subsets"; }
    grib_accessor* create_empty_accessor() override { return new grib_accessor_bufr_extract_area_subsets_t{}; }
    long get_native_type() override;
    int pack_long(const long* val, size_t* len) override;
    void init(const long, grib_arguments*) override;

private:
    const char* doExtractSubsets_ = nullptr;
    const char* numberOfSubsets_ = nullptr;
    const char* extractSubsetList_ = nullptr;
    const char* extractAreaWestLongitude_ = nullptr;
    const char* extractAreaEastLongitude_ = nullptr;
    const char* extractAreaNorthLatitude_ = nullptr;
    const char* extractAreaSouthLatitude_ = nullptr;
    const char* extractAreaLongitudeRank_ = nullptr;
    const char* extractAreaLatitudeRank_ = nullptr;
    const char* extractedAreaNumberOfSubsets_ = nullptr;

    int select_area();
};
