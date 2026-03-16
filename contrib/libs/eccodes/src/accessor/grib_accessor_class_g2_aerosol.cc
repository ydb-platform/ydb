/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_g2_aerosol.h"

grib_accessor_g2_aerosol_t _grib_accessor_g2_aerosol{};
grib_accessor* grib_accessor_g2_aerosol = &_grib_accessor_g2_aerosol;

void grib_accessor_g2_aerosol_t::init(const long l, grib_arguments* c)
{
    grib_accessor_unsigned_t::init(l, c);
    grib_handle* hand = grib_handle_of_accessor(this);
    int n             = 0;

    productDefinitionTemplateNumber_ = c->get_name(hand, n++);
    stepType_                        = c->get_name(hand, n++);
    optical_                         = c->get_long(hand, n++);
}

int grib_accessor_g2_aerosol_t::unpack_long(long* val, size_t* len)
{
    long productDefinitionTemplateNumber = 0;
    grib_get_long(grib_handle_of_accessor(this), productDefinitionTemplateNumber_, &productDefinitionTemplateNumber);

    if (optical_)
        *val = grib2_is_PDTN_AerosolOptical(productDefinitionTemplateNumber);
    else
        *val = grib2_is_PDTN_Aerosol(productDefinitionTemplateNumber);

    return GRIB_SUCCESS;
}

int grib_accessor_g2_aerosol_t::pack_long(const long* val, size_t* len)
{
    grib_handle* hand                       = grib_handle_of_accessor(this);
    long productDefinitionTemplateNumber    = -1;
    long productDefinitionTemplateNumberNew = -1;
    // long type=-1;
    // long stream=-1;
    long eps          = -1;
    char stepType[15] = {0,};
    size_t slen = 15;
    // int aerosol = *val;
    int isInstant = 0;
    // long derivedForecast=-1;
    int ret = 0;

    if (grib_get_long(hand, productDefinitionTemplateNumber_, &productDefinitionTemplateNumber) != GRIB_SUCCESS)
        return GRIB_SUCCESS;

    /*
     grib_get_long(hand, type_ ,&type);
     grib_get_long(hand, stream_ ,&stream);
     */
    ret = grib_get_string(hand, stepType_, stepType, &slen);
    ECCODES_ASSERT(ret == GRIB_SUCCESS);

    // eps = grib2_is_PDTN_EPS(productDefinitionTemplateNumber);
    eps = grib_is_defined(hand, "perturbationNumber");

    if (!strcmp(stepType, "instant"))
        isInstant = 1;

    if (eps == 1) {
        if (isInstant) {
            productDefinitionTemplateNumberNew = 45;
        }
        else {
            // productDefinitionTemplateNumberNew = 47; // PDT deprecated
            productDefinitionTemplateNumberNew = 85;
        }
    }
    else {
        if (isInstant) {
            productDefinitionTemplateNumberNew = 50;  // ECC-1963: 44 is deprecated
        }
        else {
            productDefinitionTemplateNumberNew = 46;
        }
    }

    if (optical_) {
        // Note: There is no interval based template for optical properties of aerosol!
        if (eps)
            productDefinitionTemplateNumberNew = 49;
        else
            productDefinitionTemplateNumberNew = 48;
        if (!isInstant) {
            grib_context_log(hand->context, GRIB_LOG_ERROR,
                             "The product definition templates for optical properties of aerosol are for a point-in-time only");
        }
    }

    if (productDefinitionTemplateNumber != productDefinitionTemplateNumberNew) {
        grib_set_long(hand, productDefinitionTemplateNumber_, productDefinitionTemplateNumberNew);
        // if (derivedForecast>=0) grib_set_long(hand, derivedForecast_ ,derivedForecast);
    }

    return 0;
}

int grib_accessor_g2_aerosol_t::value_count(long* count)
{
    *count = 1;
    return 0;
}
