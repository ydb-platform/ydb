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

class HashArray : public Gen
{
public:
    HashArray(grib_context* context,
              const char* name,
              grib_hash_array_value* hash_array,
              const char* basename, const char* name_space, const char* defaultkey,
              const char* masterDir, const char* localDir, const char* ecmfDir, int flags, int nofail);
    ~HashArray() override;

    void dump(FILE*, int) override;

    grib_hash_array_value* get_hash_array_impl(grib_handle* h);
    const char* get_hash_array_full_path();
    grib_hash_array_value* get_hash_array(grib_handle* h);

    grib_hash_array_value* hash_array_ = nullptr;
    char* basename_                    = nullptr;
    char* masterDir_                   = nullptr;
    char* localDir_                    = nullptr;
    char* ecmfDir_                     = nullptr;
    char* full_path_                   = nullptr;
    int nofail_                        = 0;

};

}  // namespace eccodes::action

grib_action* grib_action_create_hash_array(grib_context* context, const char* name, grib_hash_array_value* hash_array, const char* basename, const char* name_space, const char* defaultkey, const char* masterDir, const char* localDir, const char* ecmfDir, int flags, int nofail);
