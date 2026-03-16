/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_g2_concept_dir.h"

grib_accessor_g2_concept_dir_t _grib_accessor_g2_concept_dir{};
grib_accessor* grib_accessor_g2_concept_dir = &_grib_accessor_g2_concept_dir;

void grib_accessor_g2_concept_dir_t::init(const long len, grib_arguments* arg)
{
    grib_accessor_gen_t::init(len, arg);
    grib_handle* h = grib_handle_of_accessor(this);

    this->preferLocal_ = arg->get_name(h, 0);
    this->masterDir_   = arg->get_name(h, 1);
    this->localDir_    = arg->get_name(h, 2);
    datasetForLocal_   = arg->get_name(h, 3);
    mode_              = arg->get_long(h, 4);
    ECCODES_ASSERT(mode_ == 1 || mode_ == 2);
    length_ = 0;
    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
    flags_ |= GRIB_ACCESSOR_FLAG_EDITION_SPECIFIC;
}

long grib_accessor_g2_concept_dir_t::get_native_type()
{
    return GRIB_TYPE_STRING;
}

int grib_accessor_g2_concept_dir_t::unpack_string(char* v, size_t* len)
{
    grib_handle* h = grib_handle_of_accessor(this);
    int err = 0;
    long preferLocal = 0;
    char masterDir[128] = {0,};
    char localDir[128] = {0,};
    char datasetForLocal[128] = {0,};

    err = grib_get_long(h, preferLocal_, &preferLocal);
    if (err) return err;

    size_t size = sizeof(masterDir);
    err = grib_get_string(h, masterDir_, masterDir, &size);
    if (err) return err;

    size = sizeof(localDir);
    err = grib_get_string(h, localDir_, localDir, &size);
    if (err) return err;

    size = sizeof(datasetForLocal);
    bool datasetForLocalExists = true;
    err = grib_get_string(h, datasetForLocal_, datasetForLocal, &size);
    if (err) {
        if (err == GRIB_NOT_FOUND) {
            // This can happen if accessor is called before section 4
            datasetForLocalExists = false;
            err = 0;
        }
        else {
            return err;
        }
    }

    const size_t dsize = string_length() - 1; // size for destination string "v"
    if (preferLocal) {
        // ECC-806: Local concepts precedence order
        if (mode_ == 1) {
            snprintf(v, dsize, "%s", masterDir); // conceptsDir1
        } else {
            snprintf(v, dsize, "%s", localDir);  // conceptsDir2
        }
    } else {
        if (mode_ == 1) {
            snprintf(v, dsize, "%s", localDir);  // conceptsDir1
        } else {
            snprintf(v, dsize, "%s", masterDir); // conceptsDir2
        }
    }

    // Override if datasetForLocal exists and is not unknown
    if (datasetForLocalExists && !STR_EQUAL(datasetForLocal, "unknown")) {
        if (mode_ == 1) {
            snprintf(v, dsize, "%s", masterDir); // conceptsDir1
        } else {
            snprintf(v, dsize, "grib2/localConcepts/%s", datasetForLocal); // conceptsDir2
        }
    }

    size = strlen(v);
    ECCODES_ASSERT(size > 0);
    *len = size + 1;
    return err;
}
