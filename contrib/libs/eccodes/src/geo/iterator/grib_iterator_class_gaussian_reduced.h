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

#include "grib_iterator_class_gen.h"

namespace eccodes::geo_iterator {

class GaussianReduced : public Gen
{
public:
    GaussianReduced() :
        Gen() { class_name_ = "gaussian_reduced"; }
    Iterator* create() const override { return new GaussianReduced(); }

    int init(grib_handle*, grib_arguments*) override;
    int next(double*, double*, double*) const override;
    int destroy() override;

private:
    long isRotated_ = 0;
    double angleOfRotation_ = 0.;
    double southPoleLat_ = 0.;
    double southPoleLon_ = 0.;
    long disableUnrotate_ = 0;

    int iterate_reduced_gaussian_subarea_legacy(grib_handle*,
                                                double, double,
                                                double, double,
                                                double*, long*, size_t);

    int iterate_reduced_gaussian_subarea(grib_handle*,
                                         double, double,
                                         double, double,
                                         double*, long*, size_t, size_t);
};

}  // namespace eccodes::geo_iterator
