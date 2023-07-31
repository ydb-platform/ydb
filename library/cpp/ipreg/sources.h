#pragma once

#include <util/system/types.h>

namespace NIPREG {

// TODO(dieash@) make some automation/spicification via enabled sources (with full list)
enum ESourceType {
    // TODO(dieash@) full list of known src-types in choice-region-data:
    // https://yql.yandex-team.ru/Operations/XEo-amim9Z2_PCkcZgQ0Wu-sqXAm1K8NMPesswuPzbk=
    SOURCE_UNKNOWN                  = 0,  // stub
    SOURCE_MAIL                     = 1 /* "MAIL" */,                           // ripe src
    SOURCE_PHONE                    = 2 /* "PHONE" */,                          // ripe src
    SOURCE_GEO                      = 3 /* "GEO" */,                            // ripe src
    SOURCE_COUNTRY                  = 4 /* "COUNTRY" */,                        // ripe, delegated, maxmind src
    SOURCE_DOMAIN_NAME              = 5 /* "DOMAIN_NAME" */,                    // ripe src
    SOURCE_MANUAL                   = 6 /* "MANUAL" */,                         // manual src
    SOURCE_YANDEX_NETWORK           = 9 /* "YANDEX_NETWORK" */,                 // yandex-noc src
    SOURCE_SPECIAL_NETWORK          = 10 /* "SPECIAL_NETWORK" */,               // spec-net src
    SOURCE_PROVIDERS                = 15 /* "PROVIDERS" */,                     // ripe src
    SOURCE_MAXMIND                  = 17 /* "MAXMIND" */,                       // maxmind src
    SOURCE_UNITED_UID_YANDEX_MAPS   = 19 /* "UNITED_UID_YANDEX_MAPS" */,        // uuid src
    SOURCE_RELIABILITY_AROUND       = 20 /* "RELIABILITY_AROUND" */,            // rel-around src
    SOURCE_UNITED_UID_WEATHER       = 21 /* "UNITED_UID_WEATHER" */,            // uuid src
    SOURCE_UNITED_UID_YANDEX_GID    = 22 /* "UNITED_UID_YANDEX_GID" */,         // uuid src
    SOURCE_UNITED_UID_SEARCH_QUERY  = 23 /* "UNITED_UID_SEARCH_QUERY" */,       // uuid src
    SOURCE_UNITED_UID_SEARCH_IN_REG = 24 /* "UNITED_UID_SEARCH_IN_REG" */,      // uuid src
    SOURCE_BGP_ASPATH_COMMUNITY     = 25 /* "BGP_ASPATH_COMMUNITY" */,          // bgp src // NOTA BENE: clash with https://st.yandex-team.ru/IPREG-3722#5b367ec214778c001a5a3f7c
    SOURCE_ML_INT_26                = 26 /* "ML_INT_26" */,
    SOURCE_ML_INT_27                = 27 /* "ML_INT_27" */,
    SOURCE_ML_INT_28                = 28 /* "ML_INT_28" */,
    SOURCE_ML_INT_29                = 29 /* "ML_INT_29" */,
    SOURCE_ML_INT_30                = 30 /* "ML_INT_30" */,
    SOURCE_ML_INT_31                = 31 /* "ML_INT_31" */,
    SOURCE_ML_INT_32                = 32 /* "ML_INT_32" */,
    SOURCE_ML_INT_33                = 33 /* "ML_INT_33" */,
    SOURCE_ML_INT_34                = 34 /* "ML_INT_34" */,
    SOURCE_PRECISE_GEO_ML           = 35 /* "ML_INT_35" */,
    SOURCE_ML                       = 36 /* "ML" */,                            // ml src
};

double GetSourceCoefficient(ui32 type);
bool SourceWantApplyDepthCoeff(ui32 source_type);
bool SourceWantApplyNetsizeCoeff(ui32 source_type);
bool SourceIsHuman(ui32 source_type);
bool SourceExcludeFromReliability(ui32 source_type);
bool SourceIsForRegionNormalize(ui32 source_type);
bool SourceIsForEnoughHumanData(ui32 source_type);
bool SourceIsForFewHumanData(ui32 source_type);
bool SourceIsForReliability(ui32 source_type);

void SetCoefficient(ui32 type, ui32 value);
} // namespace NIPREG
