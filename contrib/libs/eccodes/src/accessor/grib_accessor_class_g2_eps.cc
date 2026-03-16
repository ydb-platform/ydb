/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_g2_eps.h"

grib_accessor_g2_eps_t _grib_accessor_g2_eps{};
grib_accessor* grib_accessor_g2_eps = &_grib_accessor_g2_eps;

void grib_accessor_g2_eps_t::init(const long l, grib_arguments* c)
{
    grib_accessor_unsigned_t::init(l, c);
    int n = 0;

    productDefinitionTemplateNumber_ = c->get_name(grib_handle_of_accessor(this), n++);
    type_                            = c->get_name(grib_handle_of_accessor(this), n++);
    stream_                          = c->get_name(grib_handle_of_accessor(this), n++);
    stepType_                        = c->get_name(grib_handle_of_accessor(this), n++);
    derivedForecast_                 = c->get_name(grib_handle_of_accessor(this), n++);
}

int grib_accessor_g2_eps_t::unpack_long(long* val, size_t* len)
{
    long productDefinitionTemplateNumber = 0;
    int err                              = 0;
    grib_handle* hand                    = grib_handle_of_accessor(this);

    err = grib_get_long(hand, productDefinitionTemplateNumber_, &productDefinitionTemplateNumber);
    if (err) return err;

    *val = 0;
    if (grib_is_defined(hand, "perturbationNumber")) {
        *val = 1;
    }
    // if (grib2_is_PDTN_EPS(productDefinitionTemplateNumber))
    //     *val = 1;

    return GRIB_SUCCESS;
}

int grib_accessor_g2_eps_t::pack_long(const long* val, size_t* len)
{
    grib_handle* hand                       = grib_handle_of_accessor(this);
    long productDefinitionTemplateNumber    = -1;
    long productDefinitionTemplateNumberNew = -1;
    long type                               = -1;
    long stream                             = -1;
    long chemical                           = -1;
    long aerosol                            = -1;
    char stepType[15]                       = {0,};
    size_t slen          = 15;
    int eps              = *val;
    int isInstant        = 0;
    long derivedForecast = -1;

    if (grib_get_long(hand, productDefinitionTemplateNumber_, &productDefinitionTemplateNumber) != GRIB_SUCCESS)
        return GRIB_SUCCESS;

    grib_get_long(hand, type_, &type);
    grib_get_long(hand, stream_, &stream);
    grib_get_string(hand, stepType_, stepType, &slen);
    if (!strcmp(stepType, "instant"))
        isInstant = 1;
    grib_get_long(hand, "is_chemical", &chemical);
    grib_get_long(hand, "is_aerosol", &aerosol);
    if (chemical == 1 && aerosol == 1) {
        grib_context_log(context_, GRIB_LOG_ERROR, "Parameter cannot be both chemical and aerosol!");
        return GRIB_ENCODING_ERROR;
    }

    // eps or stream=(enda or elda or ewla)
    if (eps || stream == 1030 || stream == 1249 || stream == 1250) {
        if (isInstant) {
            // type=em || type=es
            if (type == 17) {
                productDefinitionTemplateNumberNew = 2;
                derivedForecast                    = 0;
            }
            else if (type == 18) {
                productDefinitionTemplateNumberNew = 2;
                derivedForecast                    = 4;
            }
            else {
                // productDefinitionTemplateNumberNew = 1;
                productDefinitionTemplateNumberNew = grib2_choose_PDTN(productDefinitionTemplateNumber, false, isInstant);
            }
        }
        else {
            // type=em || type=es
            if (type == 17) {
                productDefinitionTemplateNumberNew = 12;
                derivedForecast                    = 0;
            }
            else if (type == 18) {
                productDefinitionTemplateNumberNew = 12;
                derivedForecast                    = 4;
            }
            else {
                // productDefinitionTemplateNumberNew = 11;
                productDefinitionTemplateNumberNew = grib2_choose_PDTN(productDefinitionTemplateNumber, false, false);
            }
        }
    }
    else {
        productDefinitionTemplateNumberNew = grib2_choose_PDTN(productDefinitionTemplateNumber, true, isInstant);
        // if (isInstant) {
        //     productDefinitionTemplateNumberNew = 0;
        // }
        // else {
        //     productDefinitionTemplateNumberNew = 8;
        // }
    }

    if (productDefinitionTemplateNumberNew >= 0 && productDefinitionTemplateNumber != productDefinitionTemplateNumberNew) {
        grib_set_long(hand, productDefinitionTemplateNumber_, productDefinitionTemplateNumberNew);
        if (derivedForecast >= 0)
            grib_set_long(hand, derivedForecast_, derivedForecast);
    }

    return 0;
}

int grib_accessor_g2_eps_t::value_count(long* count)
{
    *count = 1;
    return 0;
}
