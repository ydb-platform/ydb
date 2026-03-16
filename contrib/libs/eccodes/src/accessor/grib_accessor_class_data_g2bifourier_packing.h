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

typedef unsigned long (*encode_float_proc)(double);
typedef double (*decode_float_proc)(unsigned long);


typedef struct bif_trunc_t
{
    long bits_per_value;
    long decimal_scale_factor;
    long binary_scale_factor;
    long ieee_floats;
    long laplacianOperatorIsSet;
    double laplacianOperator;
    double reference_value;
    long sub_i, sub_j, bif_i, bif_j;
    long biFourierTruncationType;
    long biFourierSubTruncationType;
    long keepaxes;
    long maketemplate;
    decode_float_proc decode_float;
    encode_float_proc encode_float;
    int bytes;
    long* itruncation_bif;
    long* jtruncation_bif;
    long* itruncation_sub;
    long* jtruncation_sub;
    size_t n_vals_bif, n_vals_sub;
} bif_trunc_t;


class grib_accessor_data_g2bifourier_packing_t : public grib_accessor_data_simple_packing_t
{
public:
    grib_accessor_data_g2bifourier_packing_t() :
        grib_accessor_data_simple_packing_t() { class_name_ = "data_g2bifourier_packing"; }
    grib_accessor* create_empty_accessor() override { return new grib_accessor_data_g2bifourier_packing_t{}; }
    int pack_double(const double* val, size_t* len) override;
    int unpack_double(double* val, size_t* len) override;
    int value_count(long*) override;
    void init(const long, grib_arguments*) override;

private:
    const char* ieee_floats_ = nullptr;
    const char* laplacianOperatorIsSet_ = nullptr;
    const char* laplacianOperator_ = nullptr;
    const char* biFourierTruncationType_ = nullptr;
    const char* sub_i_ = nullptr;
    const char* sub_j_ = nullptr;
    const char* bif_i_ = nullptr;
    const char* bif_j_ = nullptr;
    const char* biFourierSubTruncationType_ = nullptr;
    const char* biFourierDoNotPackAxes_ = nullptr;
    const char* biFourierMakeTemplate_ = nullptr;
    const char* totalNumberOfValuesInUnpackedSubset_ = nullptr;
    //const char* numberOfValues_ = nullptr;

    bif_trunc_t* new_bif_trunc();
};
