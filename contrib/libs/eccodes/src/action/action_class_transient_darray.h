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

#include "action_class_gen.h"

namespace eccodes::action
{

class TransientDArray : public Gen
{
public:
    TransientDArray(grib_context* context, const char* name, grib_darray* darray, int flags);
    ~TransientDArray() override;

    void dump(FILE*, int) override;
    int execute(grib_handle* h) override;

    grib_darray* darray_ = nullptr;
    char* name2_         = nullptr;
};

}  // namespace eccodes::action

grib_action* grib_action_create_transient_darray(grib_context* context, const char* name, grib_darray* darray, int flags);
