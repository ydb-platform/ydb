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

class grib_accessor_abstract_long_vector_t : public grib_accessor_gen_t
{
public:
    grib_accessor_abstract_long_vector_t() :
        grib_accessor_gen_t() { class_name_ = "abstract_long_vector"; }
    // grib_accessor* create_empty_accessor() override { return new grib_accessor_abstract_long_vector_t{}; }

public:
    // TODO(maee): make private
    long* v_ = nullptr;
    long pack_index_ = 0;
    int number_of_elements_ = 0;
};
