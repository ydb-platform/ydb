#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/protos/statistics.pb.h>
#include <ydb/library/actors/core/events.h>

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

        EvBroadcastStatistics, // deprecated
        EvRegisterNode, // deprecated

        EvConfigureAggregator,

        EvConnectSchemeShard,
        EvSchemeShardStats,
        EvConnectNode,
        EvRequestStats,
        EvPropagateStatistics,
        EvStatisticsIsDisabled,
        EvPropagateStatisticsResponse,

        EvStatTableCreationResponse,
        EvSaveStatisticsQueryResponse,
        EvLoadStatisticsQueryResponse,

        EvEnd
    };

    struct TEvGetStatistics : public TEventLocal<TEvGetStatistics, EvGetStatistics> {
        std::vector<TRequest> StatRequests;
    };

    struct TEvGetStatisticsResult : public TEventLocal<TEvGetStatisticsResult, EvGetStatisticsResult> {
        bool Success = true;
        std::vector<TResponse> StatResponses;
    };
    
    struct TEvConfigureAggregator : public TEventPB<
        TEvConfigureAggregator,
        NKikimrStat::TEvConfigureAggregator,
        EvConfigureAggregator>
    {
        TEvConfigureAggregator() = default;

        explicit TEvConfigureAggregator(const TString& database) {
            Record.SetDatabase(database);
        }
    };

    struct TEvConnectSchemeShard : public TEventPB<
        TEvConnectSchemeShard,
        NKikimrStat::TEvConnectSchemeShard,
        EvConnectSchemeShard>
    {};

    struct TEvSchemeShardStats : public TEventPB<
        TEvSchemeShardStats,
        NKikimrStat::TEvSchemeShardStats,
        EvSchemeShardStats>
    {};

    struct TEvConnectNode : public TEventPB<
        TEvConnectNode,
        NKikimrStat::TEvConnectNode,
        EvConnectNode>
    {};

    struct TEvRequestStats : public TEventPB<
        TEvRequestStats,
        NKikimrStat::TEvRequestStats,
        EvRequestStats>
    {};

    struct TEvPropagateStatistics : public TEventPreSerializedPB<
        TEvPropagateStatistics,
        NKikimrStat::TEvPropagateStatistics,
        EvPropagateStatistics>
    {};

    struct TEvStatisticsIsDisabled : public TEventPB<
        TEvStatisticsIsDisabled,
        NKikimrStat::TEvStatisticsIsDisabled,
        EvStatisticsIsDisabled>
    {};

    struct TEvPropagateStatisticsResponse : public TEventPB<
        TEvPropagateStatisticsResponse,
        NKikimrStat::TEvPropagateStatisticsResponse,
        EvPropagateStatisticsResponse>
    {};

    struct TEvStatTableCreationResponse : public TEventLocal<
        TEvStatTableCreationResponse,
        EvStatTableCreationResponse>
    {
        bool Success = true;
    };

    struct TEvSaveStatisticsQueryResponse : public TEventLocal<
        TEvSaveStatisticsQueryResponse,
        EvSaveStatisticsQueryResponse>
    {
        bool Success = true;
    };

    struct TEvLoadStatisticsQueryResponse : public TEventLocal<
        TEvLoadStatisticsQueryResponse,
        EvLoadStatisticsQueryResponse>
    {
        bool Success = true;
        TMaybe<TString> Data;
    };

};

} // NStat
} // NKikimr
