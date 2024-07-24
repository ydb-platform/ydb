#pragma once
#include <util/generic/hash.h>
#include <util/generic/yexception.h>

namespace NMVP {

enum EAccessServiceType {
    YandexV2 = 1,
    NebiusV1 = 2
};

inline EAccessServiceType GetAccessServiceTypeFromString(const TString& name) {
    static const THashMap<TString, EAccessServiceType> AccessServiceTypeByName = {
        { "yandex_v2", EAccessServiceType::YandexV2 },
        { "nebius_v1", EAccessServiceType::NebiusV1 }
    };
    if (name.empty()) {
        return EAccessServiceType::YandexV2;
    }

    auto lower = to_lower(name);
    auto it = AccessServiceTypeByName.find(lower);
    if (it != AccessServiceTypeByName.end()) {
        return it->second;
    } else {
        ythrow yexception() << "Unknown auth profile: " << name;
    }
}
} // namespace NMVP
