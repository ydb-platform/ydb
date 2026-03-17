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

#include "grib_accessor_class_unsigned.h"

class grib_accessor_local_definition_t : public grib_accessor_unsigned_t
{
public:
    grib_accessor_local_definition_t() :
        grib_accessor_unsigned_t() { class_name_ = "local_definition"; }
    grib_accessor* create_empty_accessor() override { return new grib_accessor_local_definition_t{}; }
    int pack_long(const long* val, size_t* len) override;
    int unpack_long(long* val, size_t* len) override;
    int value_count(long*) override;
    void init(const long, grib_arguments*) override;

private:
    const char* productDefinitionTemplateNumber_ = nullptr;
    const char* productDefinitionTemplateNumberInternal_ = nullptr;
    const char* grib2LocalSectionNumber_ = nullptr;
    const char* type_ = nullptr;
    const char* stream_ = nullptr;
    const char* the_class_ = nullptr;
    const char* eps_ = nullptr;
    const char* stepType_ = nullptr;
    const char* derivedForecast_ = nullptr;
};
