/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_g2_chemical.h"

grib_accessor_g2_chemical_t _grib_accessor_g2_chemical{};
grib_accessor* grib_accessor_g2_chemical = &_grib_accessor_g2_chemical;

// Meaning of data member chemical_type_:
//  0 = atmospheric chemical constituents
//  1 = atmospheric chemical constituents based on a distribution function
//  2 = atmospheric chemical constituents with source or sink
//
#define CHEM_PLAIN   0
#define CHEM_DISTRIB 1
#define CHEM_SRCSINK 2

void grib_accessor_g2_chemical_t::init(const long l, grib_arguments* c)
{
    grib_accessor_unsigned_t::init(l, c);
    grib_handle* hand = grib_handle_of_accessor(this);
    int n             = 0;

    productDefinitionTemplateNumber_ = c->get_name(hand, n++);
    stepType_                        = c->get_name(hand, n++);
    chemical_type_                   = c->get_long(hand, n++);
}

int grib_accessor_g2_chemical_t::unpack_long(long* val, size_t* len)
{
    long productDefinitionTemplateNumber = 0;
    grib_get_long(grib_handle_of_accessor(this), productDefinitionTemplateNumber_, &productDefinitionTemplateNumber);

    ECCODES_ASSERT(chemical_type_ == CHEM_PLAIN || chemical_type_ == CHEM_DISTRIB || chemical_type_ == CHEM_SRCSINK);
    if (chemical_type_ == CHEM_DISTRIB)
        *val = grib2_is_PDTN_ChemicalDistFunc(productDefinitionTemplateNumber);
    else if (chemical_type_ == CHEM_SRCSINK)
        *val = grib2_is_PDTN_ChemicalSourceSink(productDefinitionTemplateNumber);
    else
        *val = grib2_is_PDTN_Chemical(productDefinitionTemplateNumber);

    return GRIB_SUCCESS;
}

int grib_accessor_g2_chemical_t::pack_long(const long* val, size_t* len)
{
    grib_handle* hand                       = grib_handle_of_accessor(this);
    long productDefinitionTemplateNumber    = -1;
    long productDefinitionTemplateNumberNew = -1;
    // long type=-1;
    // long stream=-1;
    long eps          = -1;
    char stepType[15] = {0,};
    size_t slen = 15;
    // int chemical = *val;
    int isInstant = 0;
    // long derivedForecast=-1;
    int ret = 0;

    if (grib_get_long(hand, productDefinitionTemplateNumber_, &productDefinitionTemplateNumber) != GRIB_SUCCESS)
        return GRIB_SUCCESS;

    //  grib_get_long(hand, type_ ,&type);
    //  grib_get_long(hand, stream_ ,&stream);
    ret = grib_get_string(hand, stepType_, stepType, &slen);
    ECCODES_ASSERT(ret == GRIB_SUCCESS);

    eps = grib_is_defined(hand, "perturbationNumber");
    // eps = grib2_is_PDTN_EPS(productDefinitionTemplateNumber);

    if (!strcmp(stepType, "instant"))
        isInstant = 1;

    ECCODES_ASSERT(chemical_type_ == CHEM_PLAIN || chemical_type_ == CHEM_DISTRIB || chemical_type_ == CHEM_SRCSINK);

    if (eps == 1) {
        if (isInstant) {
            if (chemical_type_ == CHEM_PLAIN)
                productDefinitionTemplateNumberNew = 41;
            else if (chemical_type_ == CHEM_DISTRIB)
                productDefinitionTemplateNumberNew = 58;
            else if (chemical_type_ == CHEM_SRCSINK)
                productDefinitionTemplateNumberNew = 77;
        }
        else {
            if (chemical_type_ == CHEM_PLAIN)
                productDefinitionTemplateNumberNew = 43;
            else if (chemical_type_ == CHEM_DISTRIB)
                productDefinitionTemplateNumberNew = 68;
            else if (chemical_type_ == CHEM_SRCSINK)
                productDefinitionTemplateNumberNew = 79;
        }
    }
    else {
        // deterministic
        if (isInstant) {
            if (chemical_type_ == CHEM_PLAIN)
                productDefinitionTemplateNumberNew = 40;
            else if (chemical_type_ == CHEM_DISTRIB)
                productDefinitionTemplateNumberNew = 57;
            else if (chemical_type_ == CHEM_SRCSINK)
                productDefinitionTemplateNumberNew = 76;
        }
        else {
            if (chemical_type_ == CHEM_PLAIN)
                productDefinitionTemplateNumberNew = 42;
            else if (chemical_type_ == CHEM_DISTRIB)
                productDefinitionTemplateNumberNew = 67;
            else if (chemical_type_ == CHEM_SRCSINK)
                productDefinitionTemplateNumberNew = 78;
        }
    }

    if (productDefinitionTemplateNumber != productDefinitionTemplateNumberNew) {
        grib_set_long(hand, productDefinitionTemplateNumber_, productDefinitionTemplateNumberNew);
        /*if (derivedForecast>=0) grib_set_long(hand, derivedForecast_ ,derivedForecast);*/
    }

    return 0;
}

int grib_accessor_g2_chemical_t::value_count(long* count)
{
    *count = 1;
    return 0;
}
