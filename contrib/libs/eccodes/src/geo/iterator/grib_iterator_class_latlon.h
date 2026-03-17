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

#include "grib_iterator_class_regular.h"

namespace eccodes::geo_iterator {

class Latlon : public Regular
{
public:
    Latlon() :
        Regular() { class_name_ = "latlon"; }
    Iterator* create() const override { return new Latlon(); }

    int init(grib_handle*, grib_arguments*) override;
    int next(double*, double*, double*) const override;
};

}  // namespace eccodes::geo_iterator
