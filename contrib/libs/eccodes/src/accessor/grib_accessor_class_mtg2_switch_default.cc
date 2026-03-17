/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_mtg2_switch_default.h"

grib_accessor_mtg2_switch_default_t _grib_accessor_mtg2_switch_default{};
grib_accessor* grib_accessor_mtg2_switch_default = &_grib_accessor_mtg2_switch_default;

void grib_accessor_mtg2_switch_default_t::init(const long len, grib_arguments* arg)
{
    grib_accessor_long_t::init(len, arg);

    grib_handle* h = grib_handle_of_accessor(this);

    if (context_->debug) {
        const int numActualArgs   = arg->get_count();
        const int numExpectedArgs = 4;
        if (numActualArgs != numExpectedArgs) {
            grib_context_log(context_, GRIB_LOG_FATAL, "Accessor %s (key %s): %d arguments provided but expected %d",
                             class_name_, name_, numActualArgs, numExpectedArgs);
        }
    }

    tablesVersion_           = arg->get_name(h, 0);
    tablesVersionMTG2Switch_ = arg->get_name(h, 1);
    marsClass_               = arg->get_name(h, 2);
    datasetForLocal_         = arg->get_name(h, 3);

    length_ = 0;
    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
    flags_ |= GRIB_ACCESSOR_FLAG_EDITION_SPECIFIC;
}

// MTG2 behaviour based on tablesVersion
// Without accessing this logic (i.e. no ECMWF Section 2) the default is 1 = post-MTG2
//  0 = pre-MTG2 encoding used
//  1 = post-MTG2 encoding used
//  2 = post-MTG2 encoding with paramId + chemId used
int grib_accessor_mtg2_switch_default_t::unpack_long(long* val, size_t* len)
{
    grib_handle* h     = grib_handle_of_accessor(this);
    int err            = 0;
    long tablesVersion = 0, tablesVersionMTG2Switch = 0;
    char marsClass[32] = {0,};

    err = grib_get_long(h, tablesVersion_, &tablesVersion);
    if (err) return err;
    err = grib_get_long(h, tablesVersionMTG2Switch_, &tablesVersionMTG2Switch);
    if (err) return err;

    bool marsClassExists = true;
    size_t size = sizeof(marsClass);
    err = grib_get_string(h, marsClass_, marsClass, &size);
    if (err) {
        if (err == GRIB_NOT_FOUND) {
            marsClassExists = false;
            err = 0;
        }
        else {
            return err;
        }
    }

    char datasetForLocal[128] = {0,};
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

    long centre = 0;
    err = grib_get_long(h, "centre", &centre);
    if (err) return err;

    bool isSpecialDataset = false;
    if (centre == 98) isSpecialDataset = true; // ECMWF
    if (datasetForLocalExists) {
        if (STR_EQUAL(datasetForLocal, "s2s") || STR_EQUAL(datasetForLocal, "tigge") || STR_EQUAL(datasetForLocal, "uerra")) {
            isSpecialDataset = true;
        }
    }

    if (isSpecialDataset) {
        if (tablesVersion <= tablesVersionMTG2Switch) {
            *val = 0;  // Pre-MTG2
        }
        else {
            // For class mc and cr post MTG2 we always want the param + chem split (value 2)
            // For TIGGE, marsClass is not defined in the empty local Section 2, but is defined later on.
            if (marsClassExists && (STR_EQUAL(marsClass, "mc") || STR_EQUAL(marsClass, "cr"))) {
                *val = 2;
            }
            else {
                *val = 1;  // Post-MTG2
            }
        }
    }
    else {
        *val = 1;  // Post-MTG2
    }

    return err;
}
