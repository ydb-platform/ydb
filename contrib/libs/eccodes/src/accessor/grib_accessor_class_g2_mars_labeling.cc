/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_g2_mars_labeling.h"

grib_accessor_g2_mars_labeling_t _grib_accessor_g2_mars_labeling{};
grib_accessor* grib_accessor_g2_mars_labeling = &_grib_accessor_g2_mars_labeling;

void grib_accessor_g2_mars_labeling_t::init(const long l, grib_arguments* c)
{
    grib_accessor_gen_t::init(l, c);
    int n             = 0;
    grib_handle* hand = grib_handle_of_accessor(this);

    index_                           = c->get_long(hand, n++);
    the_class_                       = c->get_name(hand, n++);
    type_                            = c->get_name(hand, n++);
    stream_                          = c->get_name(hand, n++);
    expver_                          = c->get_name(hand, n++);
    typeOfProcessedData_             = c->get_name(hand, n++);
    productDefinitionTemplateNumber_ = c->get_name(hand, n++);
    stepType_                        = c->get_name(hand, n++);
    derivedForecast_                 = c->get_name(hand, n++);
    typeOfGeneratingProcess_         = c->get_name(hand, n++);
}

int grib_accessor_g2_mars_labeling_t::unpack_long(long* val, size_t* len)
{
    char* key = NULL;

    switch (index_) {
        case 0:
            key = (char*)the_class_;
            break;
        case 1:
            key = (char*)type_;
            break;
        case 2:
            key = (char*)stream_;
            break;
        default:
            grib_context_log(context_, GRIB_LOG_ERROR,
                             "invalid first argument of g2_mars_labeling in %s", name_);
            return GRIB_INTERNAL_ERROR;
    }

    return grib_get_long(grib_handle_of_accessor(this), key, val);
}

int grib_accessor_g2_mars_labeling_t::unpack_string(char* val, size_t* len)
{
    char* key = NULL;

    switch (index_) {
        case 0:
            key = (char*)the_class_;
            break;
        case 1:
            key = (char*)type_;
            break;
        case 2:
            key = (char*)stream_;
            break;
        default:
            grib_context_log(context_, GRIB_LOG_ERROR,
                             "invalid first argument of g2_mars_labeling in %s", name_);
            return GRIB_INTERNAL_ERROR;
    }

    return grib_get_string(grib_handle_of_accessor(this), key, val, len);
}

int grib_accessor_g2_mars_labeling_t::extra_set(long val)
{
    int ret                                = 0;
    grib_handle* hand                      = grib_handle_of_accessor(this);
    char stepType[30]                      = {0,};
    size_t stepTypelen                      = 30;
    long derivedForecast                    = -1;
    long productDefinitionTemplateNumberNew = -1;
    long productDefinitionTemplateNumber;
    long typeOfProcessedData     = -1;
    long typeOfGeneratingProcess = -1;
    int is_eps                   = -1;
    int is_instant               = -1;
    long is_chemical             = 0;
    long is_chemical_distfn      = 0;
    long is_chemical_srcsink     = 0;
    long is_aerosol              = 0;
    long is_aerosol_optical      = 0;

    grib_get_long(hand, "is_chemical", &is_chemical);
    grib_get_long(hand, "is_chemical_srcsink", &is_chemical_srcsink);
    grib_get_long(hand, "is_chemical_distfn", &is_chemical_distfn);
    grib_get_long(hand, "is_aerosol", &is_aerosol);
    grib_get_long(hand, "is_aerosol_optical", &is_aerosol_optical);

    const int is_wave        = grib_is_defined(hand, "waveDirectionNumber");
    const int is_wave_prange = grib_is_defined(hand, "typeOfWavePeriodInterval");

    switch (index_) {
        case 0:
            /* class */
            return ret;
        case 1:
            /* type */
            switch (val) {
                case 0: /* Unknown       (0) */
                    typeOfProcessedData     = 255;
                    typeOfGeneratingProcess = 255;
                    break;
                case 1: /* First guess          (fg) */
                case 3: /* Initialised analysis (ia) */
                    typeOfProcessedData     = 0;
                    typeOfGeneratingProcess = 1;
                    break;
                case 2:  /* Analysis                    (an) */
                case 4:  /* Oi analysis                 (oi) */
                case 5:  /* 3d variational analysis     (3v) */
                case 6:  /* 4d variational analysis     (4v) */
                case 7:  /* 3d variational gradients    (3g) */
                case 8:  /* 4d variational gradients    (4g) */
                case 90: /* Gridded analysis input     (gai) */
                    typeOfProcessedData     = 0;
                    typeOfGeneratingProcess = 0;
                    break;
                case 9: /* Forecast  (fc) */
                    typeOfProcessedData     = 1;
                    typeOfGeneratingProcess = 2;
                    break;
                case 10: /* Control forecast  (cf) */
                    typeOfProcessedData     = 3;
                    typeOfGeneratingProcess = 4;
                    break;
                case 11: /* Perturbed forecast    (pf) */
                    typeOfProcessedData     = 4;
                    typeOfGeneratingProcess = 4;
                    break;
                case 12: /* Errors in first guess  (ef) */
                case 13: /* Errors in analysis     (ea) */
                    typeOfProcessedData     = 255;
                    typeOfGeneratingProcess = 7;
                    break;
                case 14: /* Cluster means              (cm) */
                case 15: /* Cluster std deviations     (cs) */
                    typeOfProcessedData     = 255;
                    typeOfGeneratingProcess = 4;
                    break;
                case 16: /* Forecast probability  (fp) */
                    typeOfProcessedData     = 8;
                    typeOfGeneratingProcess = 5;
                    break;
                case 17: /* Ensemble mean  (em) */
                    derivedForecast = 0;
                    grib_get_string(hand, stepType_, stepType, &stepTypelen);
                    if (!strcmp(stepType, "instant")) {
                        productDefinitionTemplateNumberNew = 2;
                    }
                    else {
                        productDefinitionTemplateNumberNew = 12;
                    }
                    typeOfProcessedData     = 255;
                    typeOfGeneratingProcess = 4;
                    break;
                case 18: /* Ensemble standard deviation     (es) */
                    derivedForecast = 4;
                    grib_get_string(hand, stepType_, stepType, &stepTypelen);
                    if (!strcmp(stepType, "instant")) {
                        productDefinitionTemplateNumberNew = 2;
                    }
                    else {
                        productDefinitionTemplateNumberNew = 12;
                    }
                    typeOfProcessedData     = 255;
                    typeOfGeneratingProcess = 4;
                    break;
                case 19: /* Forecast accumulation           (fa)  */
                case 20: /* Climatology                     (cl)  */
                case 21: /* Climate simulation              (si)  */
                case 22: /* Climate 30 days simulation      (s3)  */
                case 23: /* Empirical distribution          (ed)  */
                case 24: /* Tubes                           (tu)  */
                case 25: /* Flux forcing realtime           (ff)  */
                case 26: /* Ocean forward                   (of)  */
                case 27: /* Extreme forecast index          (efi) */
                case 28: /* Extreme forecast index control  (efic)*/
                case 29: /* Probability boundaries          (pb)  */
                    typeOfProcessedData     = 255;
                    typeOfGeneratingProcess = 255;
                    break;
                case 30: /* Event probability      (ep) */
                    typeOfProcessedData     = 8;
                    typeOfGeneratingProcess = 5;
                    break;
                case 31: /* Bias-corrected forecast      (bf) */
                    typeOfProcessedData     = 1;
                    typeOfGeneratingProcess = 3;
                    break;
                case 32: /* Climate distribution      (cd)  */
                case 33: /* 4D analysis increments    (4i)  */
                case 34: /* Gridded observations      (go)  */
                case 35: /* Model errors              (me)  */
                case 36: /* Probability distribution  (pd)  */
                case 37: /* Cluster information       (ci)  */
                case 38: /* Shift of Tail             (sot) */
                case 39: /* Ensemble data assimilation model errors */
                case 40: /* Images                    (im)  */
                case 42: /* Simulated images          (sim) */
                    typeOfProcessedData     = 255;
                    typeOfGeneratingProcess = 255;
                    break;
                case 43: /* Weighted ensemble mean                   (wem)  */
                case 44: /* Weighted ensemble standard deviation     (wes)  */
                case 45: /* Cluster representative                   (cr)   */
                case 46: /* Scaled ensemble standard deviation       (ses)  */
                case 47: /* Time average ensemble mean               (taem) */
                case 48: /* Time average ensemble standard deviation (taes) */
                    typeOfProcessedData     = 255;
                    typeOfGeneratingProcess = 4;
                    break;
                case 50: /* Sensitivity gradient            (sg)   */
                case 52: /* Sensitivity forecast            (sf)   */
                case 60: /* Perturbed analysis              (pa)   */
                case 61: /* Initial condition perturbation  (icp)  */
                case 62: /* Singular vector                 (sv)   */
                case 63: /* Adjoint singular vector         (as)   */
                case 64: /* Signal variance                 (svar) */
                    typeOfProcessedData     = 255;
                    typeOfGeneratingProcess = 255;
                    break;
                case 65: /* Calibration/Validation forecast  (cv) */
                    typeOfProcessedData     = 5;
                    typeOfGeneratingProcess = 4;
                    break;
                case 70: /* Ocean reanalysis     (or) */
                case 71: /* Flux forcing         (fx) */
                case 72: /* Fill-up              (fu) */
                case 73: /* Simulation forced with observations (sfo) */
                case 80: /* Forecast mean        (fcmean) */
                case 81: /* Forecast maximum     (fcmax) */
                case 82: /* Forecast minimum     (fcmin) */
                case 83: /* Forecast standard deviation  (fcstdev) */
                case 86: /* Hindcast climate mean (hcmean) */
                case 87: /* Simulated satellite data */
                case 88: /* Gridded satellite data */
                case 89: /* GFAS analysis */
                    typeOfProcessedData     = 255;
                    typeOfGeneratingProcess = 255;
                    break;
                default:
                    grib_context_log(context_, GRIB_LOG_WARNING, "g2_mars_labeling: unknown mars.type %d", (int)val);
                    /*return GRIB_ENCODING_ERROR;*/
            }
            break;
        case 2:
            /* stream */
            switch (val) {
                case 1030:      /* enda */
                case 1249:      /* elda */
                case 1250:      /* ewla */
                    is_eps = 1; /* These streams are all for ensembles */
                    grib_get_string(hand, stepType_, stepType, &stepTypelen);
                    is_instant                         = (strcmp(stepType, "instant") == 0);
                    productDefinitionTemplateNumberNew = grib2_select_PDTN(
                        is_eps, is_instant,
                        is_chemical,
                        is_chemical_srcsink,
                        is_chemical_distfn,
                        is_aerosol,
                        is_aerosol_optical);
                    break;
            }
            break;
        default:
            grib_context_log(context_, GRIB_LOG_ERROR,
                             "invalid first argument of g2_mars_labeling in %s", name_);
            return GRIB_INTERNAL_ERROR;
    }

    if (is_wave || is_wave_prange) {
        // ECC-1867
        productDefinitionTemplateNumberNew = -1;  // disable PDT selection
    }

    if (productDefinitionTemplateNumberNew >= 0) {
        grib_get_long(hand, productDefinitionTemplateNumber_, &productDefinitionTemplateNumber);
        if (productDefinitionTemplateNumber != productDefinitionTemplateNumberNew)
            grib_set_long(hand, productDefinitionTemplateNumber_, productDefinitionTemplateNumberNew);
    }

    if (derivedForecast >= 0) {
        grib_set_long(hand, derivedForecast_, derivedForecast);
    }

    if (typeOfProcessedData > 0)
        grib_set_long(hand, typeOfProcessedData_, typeOfProcessedData);
    if (typeOfGeneratingProcess > 0)
        grib_set_long(hand, typeOfGeneratingProcess_, typeOfGeneratingProcess);

    return ret;
}

int grib_accessor_g2_mars_labeling_t::pack_string(const char* val, size_t* len)
{
    char* key = NULL;
    int ret   = 0;
    long lval = 0;

    switch (index_) {
        case 0:
            key = (char*)the_class_;
            break;
        case 1:
            key = (char*)type_;
            break;
        case 2:
            key = (char*)stream_;
            break;
        default:
            grib_context_log(context_, GRIB_LOG_ERROR,
                             "invalid first argument of g2_mars_labeling in %s", name_);
            return GRIB_INTERNAL_ERROR;
    }

    ret = grib_set_string(grib_handle_of_accessor(this), key, val, len);
    if (ret)
        return ret; /* failed */

    ret = grib_get_long(grib_handle_of_accessor(this), key, &lval);
    if (ret)
        return ret; /* failed */

    return extra_set(lval);
}

int grib_accessor_g2_mars_labeling_t::pack_long(const long* val, size_t* len)
{
    char* key = NULL;
    int ret   = 0;

    switch (index_) {
        case 0:
            key = (char*)the_class_;
            break;
        case 1:
            key = (char*)type_;
            break;
        case 2:
            key = (char*)stream_;
            break;
        default:
            grib_context_log(context_, GRIB_LOG_ERROR,
                             "invalid first argument of g2_mars_labeling in %s", name_);
            return GRIB_INTERNAL_ERROR;
    }

    ret = grib_set_long(grib_handle_of_accessor(this), key, *val);
    if (ret)
        return ret; /* failed */

    return extra_set(*val);
}

int grib_accessor_g2_mars_labeling_t::value_count(long* count)
{
    *count = 1;
    return 0;
}

long grib_accessor_g2_mars_labeling_t::get_native_type()
{
    char* key = NULL;
    int ret   = 0;
    int type  = 0;

    switch (index_) {
        case 0:
            key = (char*)the_class_;
            break;
        case 1:
            key = (char*)type_;
            break;
        case 2:
            key = (char*)stream_;
            break;
        default:
            grib_context_log(context_, GRIB_LOG_ERROR,
                             "invalid first argument of g2_mars_labeling in %s", name_);
            return GRIB_INTERNAL_ERROR;
    }

    ret = grib_get_native_type(grib_handle_of_accessor(this), key, &type);
    if (ret)
        grib_context_log(context_, GRIB_LOG_ERROR,
                         "unable to get native type for %s", key);
    return type;
}
