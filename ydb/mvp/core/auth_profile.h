#pragma once
#include <util/generic/hash.h>

namespace NMVP {

enum EAuthProfile {
    YandexV2 = 1,
    NebiusV1 = 2
};

const THashMap<TString, EAuthProfile> AuthProfileByName = {
    { "yandex_v2", EAuthProfile::YandexV2 },
    { "nebius_v1", EAuthProfile::NebiusV1 }
};
}
