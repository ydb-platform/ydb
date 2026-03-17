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

class Concept : public Gen
{
public:
    Concept(grib_context* context,
            const char* name,
            grib_concept_value* concept_value,
            const char* basename, const char* name_space, const char* defaultkey,
            const char* masterDir, const char* localDir, const char* ecmfDir, int flags, int nofail);
    ~Concept() override;

    void dump(FILE*, int) override;

    grib_concept_value* get_concept_impl(grib_handle* h);
    grib_concept_value* get_concept(grib_handle* h);

    grib_concept_value* concept_value_ = nullptr;
    char* basename_                    = nullptr;
    char* masterDir_                   = nullptr;
    char* localDir_                    = nullptr;
    int nofail_                        = 0;
};

}  // namespace eccodes::action

grib_concept_value* action_concept_get_concept(grib_accessor* a);
int action_concept_get_nofail(grib_accessor* a);
grib_action* grib_action_create_concept(grib_context* context, const char* name, grib_concept_value* concept_value, const char* basename, const char* name_space, const char* defaultkey, const char* masterDir, const char* localDir, const char* ecmfDir, int flags, int nofail);
int get_concept_condition_string(grib_handle* h, const char* key, const char* value, char* result);
