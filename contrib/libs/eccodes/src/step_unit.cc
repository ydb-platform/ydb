/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "step_unit.h"

namespace eccodes {

std::vector<Unit::Value> Unit::grib_selected_units = {
    Unit::Value::SECOND,
    Unit::Value::MINUTE,
    Unit::Value::HOUR,
};

std::vector<Unit::Value> Unit::complete_unit_order_ = {
    Unit::Value::MISSING   ,
    Unit::Value::SECOND    ,
    Unit::Value::MINUTE    ,
    Unit::Value::MINUTES15 ,
    Unit::Value::MINUTES30 ,
    Unit::Value::HOUR      ,
    Unit::Value::HOURS3    ,
    Unit::Value::HOURS6    ,
    Unit::Value::HOURS12   ,
    Unit::Value::DAY       ,
    Unit::Value::MONTH     ,
    Unit::Value::YEAR      ,
    Unit::Value::YEARS10   ,
    Unit::Value::YEARS30   ,
    Unit::Value::CENTURY
};

template <> long Unit::value<long>() const {
    return get_converter().unit_to_long(internal_value_);
}

template <> Unit::Value Unit::value<Unit::Value>() const {
    return internal_value_;
}

template <> std::string Unit::value<std::string>() const {
    return get_converter().unit_to_name(internal_value_);
}

} // namespace eccodes
