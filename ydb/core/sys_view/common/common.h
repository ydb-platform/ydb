#pragma once

#include <ydb/library/actors/core/log.h>
#include <ydb/core/protos/config.pb.h>

#include <util/generic/yexception.h>

namespace NKikimr {
namespace NSysView {

inline TDuration QueryStatsInterval(const NKikimrConfig::TSysViewConfig& config) {
    const auto seconds = config.GetQueryStatsIntervalSeconds();
    Y_ENSURE(seconds > 0 && seconds <= 60 && 60 % seconds == 0,
        "SysViewConfig.QueryStatsIntervalSeconds must be a positive divisor of 60");
    return TDuration::Seconds(seconds);
}

inline NKikimrConfig::TSysViewConfig SysViewConfigForTests() {
    NKikimrConfig::TSysViewConfig config;
    config.SetQueryStatsIntervalSeconds(5);
    config.SetLabeledCountersIntervalSeconds(5);
    return config;
}

constexpr size_t TOP_PARTITIONS_COUNT = 10;

} // NSysView
} // NKikimr
