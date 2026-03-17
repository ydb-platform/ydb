/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_local_definition.h"

grib_accessor_local_definition_t _grib_accessor_local_definition{};
grib_accessor* grib_accessor_local_definition = &_grib_accessor_local_definition;

void grib_accessor_local_definition_t::init(const long l, grib_arguments* c)
{
    grib_accessor_unsigned_t::init(l, c);
    grib_handle* hand = grib_handle_of_accessor(this);
    int n             = 0;

    grib2LocalSectionNumber_                 = c->get_name(hand, n++);
    productDefinitionTemplateNumber_         = c->get_name(hand, n++);
    productDefinitionTemplateNumberInternal_ = c->get_name(hand, n++);
    type_                                    = c->get_name(hand, n++);
    stream_                                  = c->get_name(hand, n++);
    the_class_                               = c->get_name(hand, n++);
    eps_                                     = c->get_name(hand, n++);
    stepType_                                = c->get_name(hand, n++);
    derivedForecast_                         = c->get_name(hand, n++);
}

int grib_accessor_local_definition_t::unpack_long(long* val, size_t* len)
{
    return grib_get_long(grib_handle_of_accessor(this), grib2LocalSectionNumber_, val);
}

int grib_accessor_local_definition_t::pack_long(const long* val, size_t* len)
{
    grib_handle* hand                            = grib_handle_of_accessor(this);
    long productDefinitionTemplateNumber         = -1;
    long productDefinitionTemplateNumberInternal = -1;
    long productDefinitionTemplateNumberNew      = -1;
    long grib2LocalSectionNumber                 = -1;
    long type                                    = -1;
    long stream                                  = -1;
    long the_class                               = -1;
    long eps                                     = -1;
    long chemical                                = -1;
    long aerosol                                 = -1;
    char stepType[15]                            = {0,};
    size_t slen               = 15;
    int localDefinitionNumber = *val;
    int isInstant             = 0;
    int tooEarly              = 0;
    long derivedForecast      = -1;
    long editionNumber        = 0;

    if (grib_get_long(hand, "editionNumber", &editionNumber) == GRIB_SUCCESS) {
        ECCODES_ASSERT(editionNumber != 1);
    }

    if (grib_get_long(hand, productDefinitionTemplateNumber_, &productDefinitionTemplateNumber) != GRIB_SUCCESS)
        tooEarly = 1;
    grib_get_long(hand, productDefinitionTemplateNumberInternal_, &productDefinitionTemplateNumberInternal);
    grib_get_long(hand, type_, &type);
    grib_get_long(hand, stream_, &stream);
    grib_get_long(hand, the_class_, &the_class);
    grib_get_long(hand, eps_, &eps);
    grib_get_string(hand, stepType_, stepType, &slen);
    if (!strcmp(stepType, "instant"))
        isInstant = 1;
    grib_get_long(hand, grib2LocalSectionNumber_, &grib2LocalSectionNumber);
    grib_get_long(hand, "is_chemical", &chemical);
    grib_get_long(hand, "is_aerosol", &aerosol);

    if (chemical == 1 && aerosol == 1) {
        grib_context_log(context_, GRIB_LOG_ERROR, "Parameter cannot be both chemical and aerosol!");
        return GRIB_ENCODING_ERROR;
    }

    if (grib_is_defined(hand, "perturbationNumber")) {
        eps = 1;
    }
    // if (grib2_is_PDTN_EPS(productDefinitionTemplateNumber))
    //     eps = 1;

    // Is this a plain vanilla product?
    const int is_plain = grib2_is_PDTN_Plain(productDefinitionTemplateNumber);

    switch (localDefinitionNumber) {
        case 0:
            productDefinitionTemplateNumberNew = productDefinitionTemplateNumber;
            break;

        case 300:
            grib_context_log(context_, GRIB_LOG_ERROR,
                             "Invalid localDefinitionNumber %d. This local definition has been deprecated.",
                             localDefinitionNumber);
            return GRIB_ENCODING_ERROR;
        case 500:
            productDefinitionTemplateNumberNew = 0;
            break;

        case 1:   // MARS labelling
        case 36:  // MARS labelling for long window 4Dvar system
        case 40:  // MARS labeling with domain and model (for LAM)
        case 42:  // LC-WFV: Wave forecast verification
            if (isInstant) {
                // type=em || type=es
                if (type == 17) {
                    productDefinitionTemplateNumberNew = 2;
                    derivedForecast                    = 0;
                }
                else if (type == 18) {
                    productDefinitionTemplateNumberNew = 2;
                    derivedForecast                    = 4;
                    // eps or enda or elda or ewla
                }
                else if (eps == 1 || stream == 1030 || stream == 1249 || stream == 1250) {
                    productDefinitionTemplateNumberNew = 1;
                }
                else {
                    productDefinitionTemplateNumberNew = 0;
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
                    // eps or enda or elda or ewla
                }
                else if (eps == 1 || stream == 1030 || stream == 1249 || stream == 1250) {
                    productDefinitionTemplateNumberNew = 11;
                }
                else {
                    productDefinitionTemplateNumberNew = 8;
                }
            }
            break;
        case 41:  // EFAS: uses post-processing templates
            if (isInstant) {
                if (eps == 1)
                    productDefinitionTemplateNumberNew = 71;
                else
                    productDefinitionTemplateNumberNew = 70;
            }
            else {
                // non-instantaneous: accum etc
                if (eps == 1)
                    productDefinitionTemplateNumberNew = 73;
                else
                    productDefinitionTemplateNumberNew = 72;
            }
            break;

        case 15:  // Seasonal forecast data
        case 16:  // Seasonal forecast monthly mean data
        case 12:  // Seasonal forecast monthly mean data for lagged systems
        case 18:  // Multianalysis ensemble data
        case 26:  // MARS labelling or ensemble forecast data
        case 30:  // Forecasting Systems with Variable Resolution
            if (isInstant) {
                productDefinitionTemplateNumberNew = 1;
            }
            else {
                productDefinitionTemplateNumberNew = 11;
            }
            break;

        case 5:   // Forecast probability data
        case 7:   // Sensitivity data
        case 9:   // Singular vectors and ensemble perturbations
        case 11:  // Supplementary data used by the analysis
        case 14:  // Brightness temperature
        case 20:  // 4D variational increments
        case 21:  // Sensitive area predictions
        case 23:  // Coupled atmospheric, wave and ocean means
        case 24:  // Satellite Channel Number Data
        case 25:
        case 28:   // COSMO local area EPS
        case 38:   // 4D variational increments for long window 4Dvar system
        case 39:   // 4DVar model errors for long window 4Dvar system
        case 60:   // Ocean data analysis
        case 192:  // Multiple ECMWF local definitions
            if (isInstant) {
                productDefinitionTemplateNumberNew = 0;
            }
            else {
                productDefinitionTemplateNumberNew = 8;
            }
            break;

        default:
#ifdef DEBUG
            // In test & development mode, fail so we remember to adjust PDTN
            grib_context_log(context_, GRIB_LOG_ERROR,
                             "grib_accessor_local_definition_t: Invalid localDefinitionNumber %d", localDefinitionNumber);
            // return GRIB_ENCODING_ERROR;
#endif
            // ECC-1253: Do not fail in operations. Leave PDTN as is
            productDefinitionTemplateNumberNew = productDefinitionTemplateNumber;
            break;
    }

    if (!is_plain) {
        // ECC-1875
        productDefinitionTemplateNumberNew = -1;  // disable PDT selection
    }

    if (productDefinitionTemplateNumberNew >= 0 && productDefinitionTemplateNumber != productDefinitionTemplateNumberNew) {
        if (context_->debug) {
            fprintf(stderr, "ECCODES DEBUG grib_accessor_local_definition_t: ldNumber=%d, newPDTN=%ld\n",
                    localDefinitionNumber, productDefinitionTemplateNumberNew);
        }
        if (tooEarly)
            grib_set_long(hand, productDefinitionTemplateNumberInternal_, productDefinitionTemplateNumberNew);
        else
            grib_set_long(hand, productDefinitionTemplateNumber_, productDefinitionTemplateNumberNew);
    }
    if (derivedForecast >= 0)
        grib_set_long(hand, derivedForecast_, derivedForecast);

    grib_set_long(hand, grib2LocalSectionNumber_, *val);

    return 0;
}

int grib_accessor_local_definition_t::value_count(long* count)
{
    *count = 1;
    return 0;
}
