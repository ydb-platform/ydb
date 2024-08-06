#pragma once

#include "resource_pool_settings.h"

#include <unordered_map>


namespace NKikimr::NResourcePool {

inline constexpr i64 CLASSIFIER_RANK_OFFSET = 1000;
inline constexpr i64 CLASSIFIER_COUNT_LIMIT = 1000;

struct TClassifierSettings : public TSettingsBase {
    using TBase = TSettingsBase;
    using TProperty = std::variant<i64*, TString*>;

    struct TParser : public TBase::TParser {
        void operator()(i64* setting) const;
        void operator()(TString* setting) const;
    };

    struct TExtractor : public TBase::TExtractor {
        TString operator()(i64* setting) const;
        TString operator()(TString* setting) const;
    };

    bool operator==(const TClassifierSettings& other) const = default;
    std::unordered_map<TString, TProperty> GetPropertiesMap();

    i64 Rank = -1;  // -1 = max rank + CLASSIFIER_RANK_OFFSET
    TString ResourcePool = DEFAULT_POOL_ID;
    TString Membername = "";
};

}  // namespace NKikimr::NResourcePool
