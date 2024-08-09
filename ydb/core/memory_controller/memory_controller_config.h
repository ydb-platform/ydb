#include <ydb/core/mon_alloc/memory_info.h>
#include <ydb/core/protos/memory_controller_config.pb.h>
#include <util/generic/size_literals.h>

#pragma once

namespace NKikimr::NMemory {

namespace {
    
ui64 GetPercent(float percent, ui64 value) {
    return static_cast<ui64>(static_cast<double>(value) * (percent / 100.0));
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
    return 512_MB; // fallback
}

inline ui64 GetSoftLimitBytes(const NKikimrConfig::TMemoryControllerConfig& config, ui64 hardLimitBytes) {
    if (config.HasSoftLimitPercent() && config.HasSoftLimitBytes()) {
        return Min(GetPercent(config.GetSoftLimitPercent(), hardLimitBytes), config.GetSoftLimitBytes());
    }
    if (config.HasSoftLimitBytes()) {
        return config.GetSoftLimitBytes();
    }
    return GetPercent(config.GetSoftLimitPercent(), hardLimitBytes);
}

inline ui64 GetTargetUtilizationBytes(const NKikimrConfig::TMemoryControllerConfig& config, ui64 hardLimitBytes) {
    if (config.HasTargetUtilizationPercent() && config.HasTargetUtilizationBytes()) {
        return Min(GetPercent(config.GetTargetUtilizationPercent(), hardLimitBytes), config.GetTargetUtilizationBytes());
    }
    if (config.HasTargetUtilizationBytes()) {
        return config.GetTargetUtilizationBytes();
    }
    return GetPercent(config.GetTargetUtilizationPercent(), hardLimitBytes);
}

inline ui64 GetActivitiesLimitBytes(const NKikimrConfig::TMemoryControllerConfig& config, ui64 hardLimitBytes) {
    if (config.HasActivitiesLimitPercent() && config.HasActivitiesLimitBytes()) {
        return Min(GetPercent(config.GetActivitiesLimitPercent(), hardLimitBytes), config.GetActivitiesLimitBytes());
    }
    if (config.HasActivitiesLimitBytes()) {
        return config.GetActivitiesLimitBytes();
    }
    return GetPercent(config.GetActivitiesLimitPercent(), hardLimitBytes);
}

inline ui64 GetMemTableMinBytes(const NKikimrConfig::TMemoryControllerConfig& config, ui64 hardLimitBytes) {
    if (config.HasMemTableMinPercent() && config.HasMemTableMinBytes()) {
        return Max(GetPercent(config.GetMemTableMinPercent(), hardLimitBytes), config.GetMemTableMinBytes());
    }
    if (config.HasMemTableMinBytes()) {
        return config.GetMemTableMinBytes();
    }
    return GetPercent(config.GetMemTableMinPercent(), hardLimitBytes);
}

inline ui64 GetMemTableMaxBytes(const NKikimrConfig::TMemoryControllerConfig& config, ui64 hardLimitBytes) {
    if (config.HasMemTableMaxPercent() && config.HasMemTableMaxBytes()) {
        return Min(GetPercent(config.GetMemTableMaxPercent(), hardLimitBytes), config.GetMemTableMaxBytes());
    }
    if (config.HasMemTableMaxBytes()) {
        return config.GetMemTableMaxBytes();
    }
    return GetPercent(config.GetMemTableMaxPercent(), hardLimitBytes);
}

inline ui64 GetSharedCacheMinBytes(const NKikimrConfig::TMemoryControllerConfig& config, ui64 hardLimitBytes) {
    if (config.HasSharedCacheMinPercent() && config.HasSharedCacheMinBytes()) {
        return Max(GetPercent(config.GetSharedCacheMinPercent(), hardLimitBytes), config.GetSharedCacheMinBytes());
    }
    if (config.HasSharedCacheMinBytes()) {
        return config.GetSharedCacheMinBytes();
    }
    return GetPercent(config.GetSharedCacheMinPercent(), hardLimitBytes);
}

inline ui64 GetSharedCacheMaxBytes(const NKikimrConfig::TMemoryControllerConfig& config, ui64 hardLimitBytes) {
    if (config.HasSharedCacheMaxPercent() && config.HasSharedCacheMaxBytes()) {
        return Min(GetPercent(config.GetSharedCacheMaxPercent(), hardLimitBytes), config.GetSharedCacheMaxBytes());
    }
    if (config.HasSharedCacheMaxBytes()) {
        return config.GetSharedCacheMaxBytes();
    }
    return GetPercent(config.GetSharedCacheMaxPercent(), hardLimitBytes);
}

inline ui64 GetQueryExecutionLimitBytes(const NKikimrConfig::TMemoryControllerConfig& config, ui64 hardLimitBytes) {
    if (config.HasQueryExecutionLimitPercent() && config.HasQueryExecutionLimitBytes()) {
        return Min(GetPercent(config.GetQueryExecutionLimitPercent(), hardLimitBytes), config.GetQueryExecutionLimitBytes());
    }
    if (config.HasQueryExecutionLimitBytes()) {
        return config.GetQueryExecutionLimitBytes();
    }
    return GetPercent(config.GetQueryExecutionLimitPercent(), hardLimitBytes);
}

}