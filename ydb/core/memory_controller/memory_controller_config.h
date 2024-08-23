#include <ydb/core/mon_alloc/memory_info.h>
#include <ydb/core/protos/memory_controller_config.pb.h>
#include <util/generic/size_literals.h>

#pragma once

namespace NKikimr::NMemory {

namespace {
    
ui64 GetPercent(float percent, ui64 value) {
    return static_cast<ui64>(static_cast<double>(value) * (percent / 100.0));
}

#define GET_LIMIT(name) \
    inline ui64 Get##name##Bytes(const NKikimrConfig::TMemoryControllerConfig& config, ui64 hardLimitBytes) { \
        if (config.Has##name##Percent() && config.Has##name##Bytes()) { \
            return Min(GetPercent(config.Get##name##Percent(), hardLimitBytes), config.Get##name##Bytes()); \
        } \
        if (config.Has##name##Bytes()) { \
            return config.Get##name##Bytes(); \
        } \
        return GetPercent(config.Get##name##Percent(), hardLimitBytes); \
    }

#define GET_MIN_LIMIT(name) \
    inline ui64 Get##name##MinBytes(const NKikimrConfig::TMemoryControllerConfig& config, ui64 hardLimitBytes) { \
        if (config.Has##name##MinPercent() && config.Has##name##MinBytes()) { \
            return Max(GetPercent(config.Get##name##MinPercent(), hardLimitBytes), config.Get##name##MinBytes()); \
        } \
        if (config.Has##name##MinBytes()) { \
            return config.Get##name##MinBytes(); \
        } \
        return GetPercent(config.Get##name##MinPercent(), hardLimitBytes); \
    }

#define GET_MAX_LIMIT(name) \
    inline ui64 Get##name##MaxBytes(const NKikimrConfig::TMemoryControllerConfig& config, ui64 hardLimitBytes) { \
        if (config.Has##name##MaxPercent() && config.Has##name##MaxBytes()) { \
            return Min(GetPercent(config.Get##name##MaxPercent(), hardLimitBytes), config.Get##name##MaxBytes()); \
        } \
        if (config.Has##name##MaxBytes()) { \
            return config.Get##name##MaxBytes(); \
        } \
        return GetPercent(config.Get##name##MaxPercent(), hardLimitBytes); \
    }

};

inline ui64 GetHardLimitBytes(const NKikimrConfig::TMemoryControllerConfig& config, const TProcessMemoryInfo& info, bool& hasMemTotalHardLimit) {
    if (config.HasHardLimitBytes()) {
        ui64 hardLimitBytes = config.GetHardLimitBytes();
        if (info.CGroupLimit.has_value()) {
            hardLimitBytes = Min(hardLimitBytes, info.CGroupLimit.value());
        }
        return hardLimitBytes;
    }
    if (info.CGroupLimit.has_value()) {
        return info.CGroupLimit.value();
    }
    if (info.MemTotal) {
        hasMemTotalHardLimit = true;
        return info.MemTotal.value();
    }
    return 2_GB; // fallback
}

GET_LIMIT(SoftLimit)
GET_LIMIT(TargetUtilization)
GET_LIMIT(ActivitiesLimit)

GET_MIN_LIMIT(MemTable)
GET_MAX_LIMIT(MemTable)

GET_MIN_LIMIT(SharedCache)
GET_MAX_LIMIT(SharedCache)

GET_LIMIT(QueryExecutionLimit)

}