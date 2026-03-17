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

#include "grib_accessor_class_data_simple_packing.h"

class grib_accessor_data_g1second_order_general_extended_packing_t : public grib_accessor_data_simple_packing_t
{
public:
    grib_accessor_data_g1second_order_general_extended_packing_t() :
        grib_accessor_data_simple_packing_t() { class_name_ = "data_g1second_order_general_extended_packing"; }
    grib_accessor* create_empty_accessor() override { return new grib_accessor_data_g1second_order_general_extended_packing_t{}; }
    int pack_double(const double* val, size_t* len) override;
    int unpack_double(double* val, size_t* len) override;
    int unpack_float(float* val, size_t* len) override;
    int value_count(long*) override;
    void destroy(grib_context*) override;
    void init(const long, grib_arguments*) override;
    int unpack_double_element(size_t i, double* val) override;
    int unpack_double_element_set(const size_t* index_array, size_t len, double* val_array) override;

private:
    int unpack(double*, float*, size_t*);

private:
    const char* half_byte_ = nullptr;
    const char* packingType_ = nullptr;
    const char* ieee_packing_ = nullptr;
    const char* precision_ = nullptr;
    const char* widthOfFirstOrderValues_ = nullptr;
    const char* firstOrderValues_ = nullptr;
    const char* N1_ = nullptr;
    const char* N2_ = nullptr;
    const char* numberOfGroups_ = nullptr;
    const char* codedNumberOfGroups_ = nullptr;
    const char* numberOfSecondOrderPackedValues_ = nullptr;
    const char* extraValues_ = nullptr;
    const char* groupWidths_ = nullptr;
    const char* widthOfWidths_ = nullptr;
    const char* groupLengths_ = nullptr;
    const char* widthOfLengths_ = nullptr;
    const char* NL_ = nullptr;
    const char* SPD_ = nullptr;
    const char* widthOfSPD_ = nullptr;
    const char* orderOfSPD_ = nullptr;
    const char* numberOfPoints_ = nullptr;
    const char* dataFlag_ = nullptr;
    double* dvalues_ = nullptr;
    float* fvalues_ = nullptr;
    int double_dirty_ = 0;
    int float_dirty_ = 0;
    size_t size_ = 0;
};
