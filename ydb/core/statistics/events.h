#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/protos/statistics.pb.h>
#include <library/cpp/actors/core/events.h>

namespace NKikimr {
namespace NStat {

struct TStatSimple {
    ui64 RowCount = 0;
    ui64 BytesSize = 0;
};

struct TStatHyperLogLog {
    // TODO:
};

// TODO: other stats
enum EStatType {
    SIMPLE = 0,
    HYPER_LOG_LOG = 1,
    // TODO...
};

struct TRequest {
    EStatType StatType;
    TPathId PathId;
    std::optional<TString> ColumnName; // not used for simple stat
};

struct TResponse {
    bool Success = true;
    TRequest Req;
    std::variant<TStatSimple, TStatHyperLogLog> Statistics;
};

struct TEvStatistics {
    enum EEv {
        EvGetStatistics = EventSpaceBegin(TKikimrEvents::ES_STATISTICS),
        EvGetStatisticsResult,

        EvGetStatisticsFromSS, // deprecated
        EvGetStatisticsFromSSResult, // deprecated

        EvBroadcastStatistics,
        EvRegisterNode,

        EvEnd
    };

    struct TEvGetStatistics : public TEventLocal<TEvGetStatistics, EvGetStatistics> {
        std::vector<TRequest> StatRequests;
    };

    struct TEvGetStatisticsResult : public TEventLocal<TEvGetStatisticsResult, EvGetStatisticsResult> {
        bool Success = true;
        std::vector<TResponse> StatResponses;
    };

    struct TEvBroadcastStatistics : public TEventPreSerializedPB<
        TEvBroadcastStatistics,
        NKikimrStat::TEvBroadcastStatistics,
        EvBroadcastStatistics>
    {};

    struct TEvRegisterNode : public TEventPB<
        TEvRegisterNode,
        NKikimrStat::TEvRegisterNode,
        EvRegisterNode>
    {};
};

} // NStat
} // NKikimr
