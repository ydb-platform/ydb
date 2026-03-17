/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_select_step_template.h"

grib_accessor_select_step_template_t _grib_accessor_select_step_template{};
grib_accessor* grib_accessor_select_step_template = &_grib_accessor_select_step_template;

void grib_accessor_select_step_template_t::init(const long l, grib_arguments* c)
{
    grib_accessor_unsigned_t::init(l, c);
    grib_handle* hand = grib_handle_of_accessor(this);
    int n             = 0;

    productDefinitionTemplateNumber_ = c->get_name(hand, n++);
    instant_                         = c->get_long(hand, n++);
}

int grib_accessor_select_step_template_t::unpack_long(long* val, size_t* len)
{
    *val = 1;
    return GRIB_SUCCESS;
}

int grib_accessor_select_step_template_t::pack_long(const long* val, size_t* len)
{
    grib_handle* hand                       = grib_handle_of_accessor(this);
    long productDefinitionTemplateNumber    = 0;
    long productDefinitionTemplateNumberNew = 0;

    grib_get_long(hand, productDefinitionTemplateNumber_, &productDefinitionTemplateNumber);

    // DET = deterministic i.e., not ensemble
    // ENS = ensemble system
    if (instant_) {
        // Going from continuous or non-continuous interval to a point-in-time (instantaneous)
        switch (productDefinitionTemplateNumber) {
            case 8:
                productDefinitionTemplateNumberNew = 0;
                break;
            case 9:
                productDefinitionTemplateNumberNew = 5;
                break;
            case 10:
                productDefinitionTemplateNumberNew = 6;
                break;
            case 11:
                productDefinitionTemplateNumberNew = 1;
                break;
            case 12:
                productDefinitionTemplateNumberNew = 2;
                break;
            case 13:
                productDefinitionTemplateNumberNew = 3;
                break;
            case 14:
                productDefinitionTemplateNumberNew = 4;
                break;
            case 42:  // DET chemical
                productDefinitionTemplateNumberNew = 40;
                break;
            case 43:  // ENS chemical
                productDefinitionTemplateNumberNew = 41;
                break;
            case 46:                                      // DET aerosol
                productDefinitionTemplateNumberNew = 50;  // ECC-1963: 44 is deprecated
                break;
            case 47:  // ENS aerosol
                productDefinitionTemplateNumberNew = 45;
                break;

            case 67:  // DET chemical distrib func
                productDefinitionTemplateNumberNew = 57;
                break;
            case 68:  // ENS chemical distrib func
                productDefinitionTemplateNumberNew = 58;
                break;

            case 78:  // DET chemical source/sink
                productDefinitionTemplateNumberNew = 76;
                break;
            case 79:  // ENS chemical source/sink
                productDefinitionTemplateNumberNew = 77;
                break;

            case 72:  // DET post-processing
                productDefinitionTemplateNumberNew = 70;
                break;
            case 73:  // ENS post-processing */
                productDefinitionTemplateNumberNew = 71;
                break;
            case 0:
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
            case 15:
                productDefinitionTemplateNumberNew = productDefinitionTemplateNumber;
                break;
            default:
                productDefinitionTemplateNumberNew = productDefinitionTemplateNumber;
                break;
        }
    }
    else {
        // Going from point-in-time (instantaneous) to continuous or non-continuous interval
        switch (productDefinitionTemplateNumber) {
            case 0:
                productDefinitionTemplateNumberNew = 8;
                break;
            case 1:
                productDefinitionTemplateNumberNew = 11;
                break;
            case 2:
                productDefinitionTemplateNumberNew = 12;
                break;
            case 3:
                productDefinitionTemplateNumberNew = 13;
                break;
            case 4:
                productDefinitionTemplateNumberNew = 14;
                break;
            case 5:
                productDefinitionTemplateNumberNew = 9;
                break;
            case 6:
                productDefinitionTemplateNumberNew = 10;
                break;
            case 40:  // DET chemical
                productDefinitionTemplateNumberNew = 42;
                break;
            case 41:  // ENS chemical
                productDefinitionTemplateNumberNew = 43;
                break;
            case 45:                                      // ENS aerosol
                productDefinitionTemplateNumberNew = 85;  // 47 is  deprecated
                break;

            case 57:  // DET chemical distrib func
                productDefinitionTemplateNumberNew = 67;
                break;
            case 58:  // ENS chemical distrib func
                productDefinitionTemplateNumberNew = 68;
                break;

            case 76:  // DET chemical source/sink
                productDefinitionTemplateNumberNew = 78;
                break;
            case 77:  // ENS chemical source/sink
                productDefinitionTemplateNumberNew = 79;
                break;

            case 70:  // DET post-processing
                productDefinitionTemplateNumberNew = 72;
                break;
            case 71:  // ENS post-processing
                productDefinitionTemplateNumberNew = 73;
                break;
            case 7:
            case 8:
            case 9:
            case 10:
            case 11:
            case 12:
            case 13:
            case 14:
                productDefinitionTemplateNumberNew = productDefinitionTemplateNumber;
                break;
            default:
                productDefinitionTemplateNumberNew = productDefinitionTemplateNumber;
                break;
        }
    }

    if (productDefinitionTemplateNumber != productDefinitionTemplateNumberNew) {
        grib_set_long(hand, productDefinitionTemplateNumber_, productDefinitionTemplateNumberNew);
    }

    return GRIB_SUCCESS;
}

int grib_accessor_select_step_template_t::value_count(long* c)
{
    *c = 1;
    return 0;
}
