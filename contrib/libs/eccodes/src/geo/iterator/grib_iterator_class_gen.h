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

#include "grib_iterator.h"

namespace eccodes::geo_iterator {

class Gen : public Iterator
{
public:
    Gen() :
        Iterator() { class_name_ = "gen"; }
    // Iterator* create() const override { return new Gen(); }

    int init(grib_handle*, grib_arguments*) override;
    int next(double*, double*, double*) const override;
    int previous(double*, double*, double*) const override;
    int reset() override;
    int destroy() override;
    bool has_next() const override;

protected:
    int carg_ = 0;
    double* lats_ = nullptr;
    double* lons_ = nullptr;

private:
    //int get(double*, double*, double*);
};

}  // namespace eccodes::geo_iterator
