#pragma once
#include <util/generic/hash.h>

namespace NMVP {

enum EAccessServiceType {
    YandexV2 = 1,
    NebiusV1 = 2
};

const THashMap<TString, EAccessServiceType> AccessServiceTypeByName = {
    { "yandex_v2", EAccessServiceType::YandexV2 },
    { "nebius_v1", EAccessServiceType::NebiusV1 }
};
}
