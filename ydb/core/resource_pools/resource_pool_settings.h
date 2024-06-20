#pragma once

#include <util/datetime/base.h>
#include <util/string/cast.h>


namespace NKikimr::NResourcePool {

struct TPoolSettings {
    ui64 ConcurrentQueryLimit = 0;  // 0 = infinity
    ui64 QueryCountLimit = 0;  // 0 = infinity
    TDuration QueryCancelAfter = TDuration::Zero();  // 0 = disabled

    typedef double TRatio;
    TRatio QueryMemoryLimitRatioPerNode = 100;  // Percent from node memory capacity
};

struct TSettingsParser {
    const TString& value;

    template <typename T>
    void operator()(T* setting) {
        *setting = FromString<T>(value);
    }
};

struct TSettingsExtractor {
    template <typename T>
    TString operator()(T* setting) {
        return ToString(*setting);
    }
};

using TProperty = std::variant<ui64*, TDuration*, TPoolSettings::TRatio*>;
std::unordered_map<TString, TProperty> GetPropertiesMap(TPoolSettings& settings);

}  // namespace NKikimr::NResourcePool
