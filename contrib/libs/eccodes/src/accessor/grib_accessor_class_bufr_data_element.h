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

class grib_accessor_bufr_data_element_t : public grib_accessor_gen_t
{
public:
    grib_accessor_bufr_data_element_t() :
        grib_accessor_gen_t() { class_name_ = "bufr_data_element"; }
    grib_accessor* create_empty_accessor() override { return new grib_accessor_bufr_data_element_t{}; }
    long get_native_type() override;
    int pack_missing() override;
    int is_missing() override;
    int pack_double(const double* val, size_t* len) override;
    int pack_long(const long* val, size_t* len) override;
    int pack_string(const char*, size_t* len) override;
    int pack_string_array(const char**, size_t* len) override;
    int unpack_double(double* val, size_t* len) override;
    int unpack_long(long* val, size_t* len) override;
    int unpack_string(char*, size_t* len) override;
    int unpack_string_array(char**, size_t* len) override;
    int value_count(long*) override;
    void destroy(grib_context*) override;
    void dump(eccodes::Dumper*) override;
    void init(const long, grib_arguments*) override;
    int unpack_double_element(size_t i, double* val) override;
    grib_accessor* make_clone(grib_section*, int*) override;

    void index(long index) { index_ = index; }
    void type(int type) { type_ = type; }
    void numberOfSubsets(long numberOfSubsets) { numberOfSubsets_ = numberOfSubsets; }
    void subsetNumber(long subsetNumber) { subsetNumber_ = subsetNumber; }
    void compressedData(int compressedData) { compressedData_ = compressedData; }
    void descriptors(bufr_descriptors_array* descriptors) { descriptors_ = descriptors; }
    void numericValues(grib_vdarray* numericValues) { numericValues_ = numericValues; }
    void stringValues(grib_vsarray* stringValues) { stringValues_ = stringValues; }
    void elementsDescriptorsIndex(grib_viarray* elementsDescriptorsIndex) { elementsDescriptorsIndex_ = elementsDescriptorsIndex; }

private:
    long index_ = 0;
    int type_ = 0;
    long compressedData_ = 0;
    long subsetNumber_ = 0;
    long numberOfSubsets_ = 0;
    bufr_descriptors_array* descriptors_ = nullptr;
    grib_vdarray* numericValues_ = nullptr;
    grib_vsarray* stringValues_ = nullptr;
    grib_viarray* elementsDescriptorsIndex_ = nullptr;
    char* cname_ = nullptr;
};
