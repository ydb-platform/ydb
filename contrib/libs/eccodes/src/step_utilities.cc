/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "step_utilities.h"
#include <type_traits>

std::optional<eccodes::Step> get_step(grib_handle* h, const char* value_key, const char* unit_key)
{
    if (value_key && unit_key && grib_is_defined(h, unit_key) && grib_is_defined(h, value_key)) {
        long unit = 0;
        if (grib_get_long_internal(h, unit_key, &unit) != GRIB_SUCCESS)
            return {};

        long value = 0;
        if (grib_get_long_internal(h, value_key, &value) != GRIB_SUCCESS)
            return {};

        return eccodes::Step(value, unit);
    }
    else {
        return {};
    }
}

int set_step(grib_handle* h, const std::string& value_key, const std::string& unit_key, const eccodes::Step& step)
{
    int err;
    if ((err = grib_set_long_internal(h, value_key.c_str(), step.value<long>())) != GRIB_SUCCESS)
        return err;
    if ((err = grib_set_long_internal(h, unit_key.c_str(), step.unit().value<long>())) != GRIB_SUCCESS)
        return err;
    return GRIB_SUCCESS;
}
