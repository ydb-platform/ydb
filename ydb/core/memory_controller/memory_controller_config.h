#include <ydb/core/mon_alloc/memory_info.h>
#include <ydb/core/protos/memory_controller_config.pb.h>
#include <ydb/core/tx/columnshard/common/limits.h>
#include <util/generic/size_literals.h>

#pragma once

namespace NKikimr::NMemory {

namespace {

ui64 GetPercent(float percent, ui64 value) {
    return static_cast<ui64>(static_cast<double>(value) * (static_cast<double>(percent) / 100.0));
}

ui64 GetFraction(float fraction, ui64 value) {
    return static_cast<ui64>(static_cast<double>(value) * static_cast<double>(fraction));
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
        if (info.MemTotal.has_value()) {
            hardLimitBytes = Min(hardLimitBytes, info.MemTotal.value());
        }
        return hardLimitBytes;
    }
    if (info.CGroupLimit.has_value()) {
        return info.CGroupLimit.value();
    }
    if (info.MemTotal.has_value()) {
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

GET_LIMIT(ColumnTablesReadExecutionLimit)
GET_LIMIT(ColumnTablesCompactionLimit)
GET_LIMIT(ColumnTablesCacheLimit)

// ColumnTablesReadExecution memory is split into:
// - ColumnTablesScanGroupedMemory
// - ColumnTablesDeduplicationGroupedMemory

// keep fractions as power of 2 to avoid precision loss
static constexpr float ColumnTablesReadExecutionFraction = 0.5f; // 1/2
static constexpr float ColumnTablesDeduplicationGroupedMemoryFraction = 0.5f; // 1/2
static_assert(ColumnTablesReadExecutionFraction + ColumnTablesDeduplicationGroupedMemoryFraction == 1);

inline ui64 GetColumnTablesScanGroupedMemoryLimitBytes(const NKikimrConfig::TMemoryControllerConfig& config, const ui64 hardLimitBytes) {
    return GetFraction(ColumnTablesReadExecutionFraction,
        GetColumnTablesReadExecutionLimitBytes(config, hardLimitBytes));
}
inline ui64 GetColumnTablesDeduplicationGroupedMemoryLimitBytes(const NKikimrConfig::TMemoryControllerConfig& config, const ui64 hardLimitBytes) {
    return GetFraction(ColumnTablesDeduplicationGroupedMemoryFraction,
        GetColumnTablesReadExecutionLimitBytes(config, hardLimitBytes));
}

// ColumnTablesCompaction memory is split into:
// - ColumnTablesCompGroupedMemory
// and resource broker queues:
// - ColumnTablesCompactionIndexationQueue
// - ColumnTablesTtlQueue
// - ColumnTablesGeneralQueue
// - ColumnTablesNormalizerQueue

// keep fractions as power of 2 to avoid precision loss
static constexpr float ColumnTablesCompactionIndexationQueueFraction = 0.125f; // 2/16
static constexpr float ColumnTablesTtlQueueFraction = 0.125f; // 2/16
static constexpr float ColumnTablesGeneralQueueFraction = 0.375f; // 6/16
static constexpr float ColumnTablesNormalizerQueueFraction = 0.375f; // 6/16
static_assert(ColumnTablesCompactionIndexationQueueFraction
    + ColumnTablesTtlQueueFraction
    + ColumnTablesGeneralQueueFraction
    + ColumnTablesNormalizerQueueFraction == 1);

inline ui64 GetColumnTablesCompGroupedMemoryLimitBytes(const NKikimrConfig::TMemoryControllerConfig& config, const ui64 hardLimitBytes) {
    return GetColumnTablesCompactionLimitBytes(config, hardLimitBytes);
}

inline ui64 GetColumnTablesCompactionIndexationQueueLimitBytes(const NKikimrConfig::TMemoryControllerConfig& config, const ui64 hardLimitBytes) {
    return GetFraction(ColumnTablesCompactionIndexationQueueFraction,
        GetColumnTablesCompactionLimitBytes(config, hardLimitBytes));
}

inline ui64 GetColumnTablesTtlQueueLimitBytes(const NKikimrConfig::TMemoryControllerConfig& config, const ui64 hardLimitBytes) {
    return GetFraction(ColumnTablesTtlQueueFraction,
        GetColumnTablesCompactionLimitBytes(config, hardLimitBytes));
}

inline ui64 GetColumnTablesGeneralQueueQueueLimitBytes(const NKikimrConfig::TMemoryControllerConfig& config, const ui64 hardLimitBytes) {
    return GetFraction(ColumnTablesGeneralQueueFraction,
        GetColumnTablesCompactionLimitBytes(config, hardLimitBytes));
}

inline ui64 GetColumnTablesNormalizerQueueLimitBytes(const NKikimrConfig::TMemoryControllerConfig& config, const ui64 hardLimitBytes) {
    return GetFraction(ColumnTablesNormalizerQueueFraction,
        GetColumnTablesCompactionLimitBytes(config, hardLimitBytes));
}

// ColumnTablesCache memory is split into:
// - ColumnTablesBlobCache
// - ColumnTablesDeduplicationGroupedMemory
// - ColumnTablesColumnDataCache

// keep fractions as power of 2 to avoid precision loss
static constexpr float ColumnTablesBlobCacheFraction = 0.125f; // 2/16
static constexpr float ColumnTablesColumnTablesDataAccessorCacheFraction = 0.125f; // 2/16
static constexpr float ColumnTablesColumnDataCacheFraction = 0.125f; // 2/16
static constexpr float ColumnTablesPortionsMetaDataCacheFraction = 0.625f; // 10/16
static_assert(ColumnTablesBlobCacheFraction
    + ColumnTablesColumnTablesDataAccessorCacheFraction
    + ColumnTablesColumnDataCacheFraction
    + ColumnTablesPortionsMetaDataCacheFraction == 1);

inline ui64 GetColumnTablesBlobCacheLimitBytes(const NKikimrConfig::TMemoryControllerConfig& config, const ui64 hardLimitBytes) {
    return GetFraction(ColumnTablesBlobCacheFraction,
        GetColumnTablesCacheLimitBytes(config, hardLimitBytes));
}

inline ui64 GetColumnTablesDataAccessorCacheLimitBytes(const NKikimrConfig::TMemoryControllerConfig& config, const ui64 hardLimitBytes) {
    return GetFraction(ColumnTablesDeduplicationGroupedMemoryFraction,
        GetColumnTablesCacheLimitBytes(config, hardLimitBytes));
}

inline ui64 GetColumnTablesColumnDataCacheLimitBytes(const NKikimrConfig::TMemoryControllerConfig& config, const ui64 hardLimitBytes) {
    return GetFraction(ColumnTablesColumnDataCacheFraction,
        GetColumnTablesCacheLimitBytes(config, hardLimitBytes));
}

inline ui64 GetPortionsMetaDataCacheLimitBytes(const NKikimrConfig::TMemoryControllerConfig& config, const ui64 hardLimitBytes) {
    return GetFraction(ColumnTablesPortionsMetaDataCacheFraction,
        GetColumnTablesCacheLimitBytes(config, hardLimitBytes));
}
}
