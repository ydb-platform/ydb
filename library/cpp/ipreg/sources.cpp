#include "sources.h"

#include <cstdint>
#include <stdexcept>

namespace NIPREG {

const ui32 ML_COEFF_DEFAULT = 50000;
ui32 ML_COEFFICIENT = ML_COEFF_DEFAULT;

void SetCoefficient(ui32 type, ui32 value) {
    switch (type) {
    case SOURCE_ML:
        ML_COEFFICIENT = value;
        break;
    default:
        throw std::runtime_error("unsupported setcoeff-type");
    }
}

double GetSourceCoefficient(ui32 type) {
    switch (type) {
    case SOURCE_MAIL:                     return 1;
    case SOURCE_PHONE:                    return 3;
    case SOURCE_GEO:                      return 4;
    case SOURCE_COUNTRY:                  return 100;
    case SOURCE_DOMAIN_NAME:              return 1;
    case SOURCE_MANUAL:                   return 1;
    case SOURCE_YANDEX_NETWORK:           return 1000; // NB: in yandex_noc source weight := 10K
    case SOURCE_SPECIAL_NETWORK:          return 1000000;
    case SOURCE_PROVIDERS:                return 50;
    case SOURCE_MAXMIND:                  return 4;
    case SOURCE_UNITED_UID_YANDEX_MAPS:   return 0.7;
    case SOURCE_RELIABILITY_AROUND:       return 1;
    case SOURCE_UNITED_UID_WEATHER:       return 0.9;
    case SOURCE_UNITED_UID_YANDEX_GID:    return 1;
    case SOURCE_UNITED_UID_SEARCH_QUERY:  return 1.5;
    case SOURCE_UNITED_UID_SEARCH_IN_REG: return 2;
    case SOURCE_BGP_ASPATH_COMMUNITY:     return 10;
    case SOURCE_ML:                       return ML_COEFFICIENT;
    }
    return 0;
}

bool SourceWantApplyDepthCoeff(ui32 source_type) {
    switch (source_type) {
        case SOURCE_MAIL:
        case SOURCE_PHONE:
        case SOURCE_GEO:
        case SOURCE_COUNTRY:
        case SOURCE_DOMAIN_NAME:
            return true;
        default:
            return false;
    }
}

bool SourceWantApplyNetsizeCoeff(ui32 source_type) {
    return SourceWantApplyDepthCoeff(source_type);
}

bool SourceIsHuman(ui32 source_type) {
    switch (source_type) {
        case SOURCE_UNITED_UID_SEARCH_QUERY:
        case SOURCE_UNITED_UID_SEARCH_IN_REG:
        case SOURCE_UNITED_UID_WEATHER:
        case SOURCE_UNITED_UID_YANDEX_GID:
        case SOURCE_UNITED_UID_YANDEX_MAPS:
            return true;
        default:
            return false;
    }
}

bool SourceIsForRegionNormalize(ui32 source_type) {
    return SourceIsHuman(source_type);
}

bool SourceIsForEnoughHumanData(ui32 source_type) {
    switch (source_type) {
        case SOURCE_COUNTRY:
        case SOURCE_MANUAL:
        case SOURCE_PROVIDERS:
        case SOURCE_YANDEX_NETWORK:
        case SOURCE_SPECIAL_NETWORK:
            return true;
        default:
            return SourceIsHuman(source_type);
    }
}

bool SourceIsForFewHumanData(ui32 source_type) {
    return !SourceIsHuman(source_type);
}

bool SourceIsForReliability(ui32 source_type) {
    return SourceIsHuman(source_type) || SOURCE_YANDEX_NETWORK == source_type;
}

} // NIPREG
