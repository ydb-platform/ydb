#pragma once

#include <util/datetime/base.h>
#include <util/string/cast.h>


namespace NKikimr::NResourcePool {

inline constexpr char DEFAULT_POOL_ID[] = "default";

typedef double TPercent;

struct TPoolSettings {
    i32 ConcurrentQueryLimit = -1;  // -1 = disabled
    i32 QueueSize = -1;  // -1 = disabled
    TDuration QueryCancelAfter = TDuration::Zero();  // 0 = disabled

    TPercent QueryMemoryLimitPercentPerNode = -1;  // Percent from node memory capacity, -1 = disabled

    TPercent DatabaseLoadCpuThreshold = -1;  // -1 = disabled

    bool operator==(const TPoolSettings& other) const = default;
};

struct TSettingsParser {
    const TString& value;

    void operator()(i32* setting) const {
        *setting = FromString<i32>(value);
        if (*setting < -1) {
            throw yexception() << "Invalid integer value " << *setting << ", it is should be greater or equal -1";
        }
    }

    void operator()(TDuration* setting) const {
        ui64 seconds = FromString<ui64>(value);
        if (seconds > std::numeric_limits<ui64>::max() / 1000) {
            throw yexception() << "Invalid seconds value " << seconds << ", it is should be less or equal than " << std::numeric_limits<ui64>::max() / 1000;
        }
        *setting = TDuration::Seconds(seconds);
    }

    void operator()(TPercent* setting) const {
        *setting = FromString<TPercent>(value);
        if (*setting != -1 && (*setting < 0 || 100 < *setting)) {
            throw yexception() << "Invalid percent value " << *setting << ", it is should be between 0 and 100 or -1";
        }
    }
};

struct TSettingsExtractor {
    template <typename T>
    TString operator()(T* setting) const {
        return ToString(*setting);
    }

    template <>
    TString operator()(TDuration* setting) const {
        return ToString(setting->Seconds());
    }
};

using TProperty = std::variant<i32*, TDuration*, TPercent*>;
std::unordered_map<TString, TProperty> GetPropertiesMap(TPoolSettings& settings, bool restricted = false);

}  // namespace NKikimr::NResourcePool
