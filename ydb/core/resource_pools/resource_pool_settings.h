#pragma once

#include <util/datetime/base.h>
#include <util/string/cast.h>


namespace NKikimr::NResourcePool {

typedef double TRatio;

struct TPoolSettings {
    ui64 ConcurrentQueryLimit = 0;  // 0 = infinity
    ui64 QueryCountLimit = 0;  // 0 = infinity
    TDuration QueryCancelAfter = TDuration::Zero();  // 0 = disabled

    TRatio QueryMemoryLimitRatioPerNode = 100;  // Percent from node memory capacity
};

struct TSettingsParser {
    const TString& value;

    template <typename T>
    void operator()(T* setting) {
        *setting = FromString<T>(value);
    }

    template <>
    void operator()(TDuration* setting) {
        *setting = TDuration::Seconds(FromString<ui64>(value));
    }

    template <>
    void operator()(TRatio* setting) {
        *setting = FromString<double>(value);
        if (*setting < 0 || 100 < *setting) {
            throw yexception() << "Invalid ratio value " << *setting << ", it is should be between 0 and 100";
        }
    }
};

struct TSettingsExtractor {
    template <typename T>
    TString operator()(T* setting) {
        return ToString(*setting);
    }

    template <>
    TString operator()(TDuration* setting) {
        return ToString(setting->Seconds());
    }
};

using TProperty = std::variant<ui64*, TDuration*, TRatio*>;
std::unordered_map<TString, TProperty> GetPropertiesMap(TPoolSettings& settings);

}  // namespace NKikimr::NResourcePool
