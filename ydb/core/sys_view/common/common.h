#pragma once

#include <ydb/library/actors/core/log.h>
#include <ydb/core/protos/config.pb.h>

#include <util/generic/yexception.h>

namespace NKikimr {
namespace NSysView {

inline TDuration QueryStatsInterval(const NKikimrConfig::TSysViewConfig& config) {
    const auto seconds = config.GetQueryStatsIntervalSeconds();
    Y_ENSURE(seconds >= 5 && seconds <= 60 && 60 % seconds == 0,
        "SysViewConfig.QueryStatsIntervalSeconds must divide 60 and be at least 5");
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
