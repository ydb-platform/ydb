#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/protos/statistics.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/library/minsketch/count_min_sketch.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/yql/public/issue/yql_issue.h>


namespace NKikimr {
namespace NStat {

struct TStatSimple {
    ui64 RowCount = 0;
    ui64 BytesSize = 0;
};

struct TStatHyperLogLog {
    // TODO:
};

struct TStatCountMinSketch {
    std::shared_ptr<TCountMinSketch> CountMin;
};

enum EStatType {
    SIMPLE = 0,
    HYPER_LOG_LOG = 1,
    COUNT_MIN_SKETCH = 2,
};

struct TRequest {
    TPathId PathId;
    std::optional<ui32> ColumnTag; // not used for simple stat
};

struct TResponse {
    bool Success = true;
    TRequest Req;
    TStatSimple Simple;
    TStatHyperLogLog HyperLogLog;
    TStatCountMinSketch CountMinSketch;
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
        EvDeleteStatisticsQueryResponse,
        EvLoadStatisticsQueryResponse,

        EvAnalyze,
        EvAnalyzeResponse,
        EvAnalyzeStatus,
        EvAnalyzeStatusResponse,

        EvAnalyzeTable,
        EvAnalyzeTableResponse,

        EvStatisticsRequest,
        EvStatisticsResponse,

        EvAggregateStatistics,
        EvAggregateStatisticsResponse,
        EvAggregateKeepAlive,
        EvAggregateKeepAliveAck,

        EvFinishTraversal,

        EvEnd
    };

    struct TEvAggregateKeepAlive : public TEventPB<
        TEvAggregateKeepAlive,
        NKikimrStat::TEvAggregateKeepAlive,
        EvAggregateKeepAlive>
    {};

    struct TEvAggregateKeepAliveAck : public TEventPB<
        TEvAggregateKeepAliveAck,
        NKikimrStat::TEvAggregateKeepAliveAck,
        EvAggregateKeepAliveAck>
    {};

    struct TEvAggregateStatistics : public TEventPB<
        TEvAggregateStatistics,
        NKikimrStat::TEvAggregateStatistics,
        EvAggregateStatistics>
    {};

    struct TEvAggregateStatisticsResponse : public TEventPB<
        TEvAggregateStatisticsResponse,
        NKikimrStat::TEvAggregateStatisticsResponse,
        EvAggregateStatisticsResponse>
    {};

    struct TEvGetStatistics : public TEventLocal<TEvGetStatistics, EvGetStatistics> {
        EStatType StatType;
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
        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        bool Success = true;
    };

    struct TEvLoadStatisticsQueryResponse : public TEventLocal<
        TEvLoadStatisticsQueryResponse,
        EvLoadStatisticsQueryResponse>
    {
        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        bool Success = true;
        ui64 Cookie = 0;
        std::optional<TString> Data;
    };

    struct TEvDeleteStatisticsQueryResponse : public TEventLocal<
        TEvDeleteStatisticsQueryResponse,
        EvDeleteStatisticsQueryResponse>
    {
        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        bool Success = true;
    };

    struct TEvAnalyze : public TEventPB<
        TEvAnalyze,
        NKikimrStat::TEvAnalyze,
        EvAnalyze>
    {};

    struct TEvAnalyzeResponse : public TEventPB<
        TEvAnalyzeResponse,
        NKikimrStat::TEvAnalyzeResponse,
        EvAnalyzeResponse>
    {};

    struct TEvAnalyzeStatus : public TEventPB<
        TEvAnalyzeStatus,
        NKikimrStat::TEvAnalyzeStatus,
        EvAnalyzeStatus>
    {};

    struct TEvAnalyzeStatusResponse : public TEventPB<
        TEvAnalyzeStatusResponse,
        NKikimrStat::TEvAnalyzeStatusResponse,
        EvAnalyzeStatusResponse>
    {};

    struct TEvAnalyzeTable : public TEventPB<
        TEvAnalyzeTable,
        NKikimrStat::TEvAnalyzeTable,
        EvAnalyzeTable>
    {};

    struct TEvAnalyzeTableResponse : public TEventPB<
        TEvAnalyzeTableResponse,
        NKikimrStat::TEvAnalyzeTableResponse,
        EvAnalyzeTableResponse>
    {};    

    struct TEvStatisticsRequest : public TEventPB<
        TEvStatisticsRequest,
        NKikimrStat::TEvStatisticsRequest,
        EvStatisticsRequest>
    {};

    struct TEvStatisticsResponse : public TEventPB<
        TEvStatisticsResponse,
        NKikimrStat::TEvStatisticsResponse,
        EvStatisticsResponse>
    {};
};

} // NStat
} // NKikimr
