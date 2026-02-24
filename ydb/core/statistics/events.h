#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/protos/statistics.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/library/actors/core/events.h>
#include <yql/essentials/public/issue/yql_issue.h>


namespace NKikimr {

class TCountMinSketch;
class TEqWidthHistogram;

namespace NStat {

struct TStatSimple {
    ui64 RowCount = 0;
    ui64 BytesSize = 0;
};

struct TStatSimpleColumn {
    std::optional<NKikimrStat::TSimpleColumnStatistics> Data;
};

struct TStatCountMinSketch {
    std::shared_ptr<TCountMinSketch> CountMin;
};

struct TStatEqWidthHistogram {
    std::shared_ptr<TEqWidthHistogram> Data;
};

struct TStatTableSummary {
    std::optional<NKikimrStat::TTableSummaryStatistics> Data;
};

// NB: enum values are serialized into the .metadata/_statistics table.
enum class EStatType {
    // Simple table statistics calculated by aggregating shard statistics reports
    // (row count may be incorrect if the table is not fully compacted as it counts all row versions).
    SIMPLE = 0,
    // Simple column statistics (number of distinct values, min/max).
    SIMPLE_COLUMN = 1,
    // Count-min sketch column statistics.
    COUNT_MIN_SKETCH = 2,
    // Equi-width histogram column statistics.
    EQ_WIDTH_HISTOGRAM = 3,
    // Correct table row count calculated during ANALYZE.
    TABLE_SUMMARY = 4,
};

struct TRequest {
    TPathId PathId;
    std::optional<ui32> ColumnTag; // not used for SIMPLE or TABLE_SUMMARY stats
};

struct TResponse {
    bool Success = true;
    TRequest Req;
    TStatSimple Simple;
    TStatSimpleColumn SimpleColumn;
    TStatCountMinSketch CountMinSketch;
    TStatEqWidthHistogram EqWidthHistogram;
    TStatTableSummary TableSummary;
};

// A single item of columnar statistics ready to be saved in the internal table.
struct TStatisticsItem {
    TStatisticsItem(
            std::optional<ui32> columnTag,
            EStatType type,
            TString data)
        : ColumnTag(columnTag)
        , Type(type)
        , Data(std::move(data))
    {}

    std::optional<ui32> ColumnTag;
    EStatType Type;
    TString Data;
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

        EvAnalyzeShard,
        EvAnalyzeShardResponse,

        EvStatisticsRequest,
        EvStatisticsResponse,

        EvAggregateStatistics,
        EvAggregateStatisticsResponse,
        EvAggregateKeepAlive,
        EvAggregateKeepAliveAck,

        EvAnalyzeActorResult,

        EvAnalyzeCancel,

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
        TString Database;
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
        TEvSaveStatisticsQueryResponse(
                Ydb::StatusIds::StatusCode status,
                NYql::TIssues issues,
                TPathId pathId)
            : Status(status)
            , Issues(std::move(issues))
            , PathId(std::move(pathId))
            , Success(Status == Ydb::StatusIds::SUCCESS)
        {}

        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        TPathId PathId;
        bool Success = true;
    };

    struct TEvLoadStatisticsQueryResponse : public TEventLocal<
        TEvLoadStatisticsQueryResponse,
        EvLoadStatisticsQueryResponse>
    {
        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        bool Success = true;
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

    struct TEvAnalyzeCancel : public TEventPB<
        TEvAnalyzeCancel,
        NKikimrStat::TEvAnalyzeCancel,
        EvAnalyzeCancel>
    {};

    struct TEvAnalyzeShard : public TEventPB<
        TEvAnalyzeShard,
        NKikimrStat::TEvAnalyzeShard,
        EvAnalyzeShard>
    {};

    struct TEvAnalyzeShardResponse : public TEventPB<
        TEvAnalyzeShardResponse,
        NKikimrStat::TEvAnalyzeShardResponse,
        EvAnalyzeShardResponse>
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

    struct TEvAnalyzeActorResult : public TEventLocal<
        TEvAnalyzeActorResult, EvAnalyzeActorResult>
    {
        enum class EStatus {
            Success,
            InternalError,
            TableNotFound,
        };
        EStatus Status;
        NYql::TIssues Issues;
        std::vector<TStatisticsItem> Statistics;
        bool Final; // Indicates that the actor has finished.

        TEvAnalyzeActorResult(std::vector<TStatisticsItem> statistics, bool final)
            : Status(EStatus::Success)
            , Statistics(std::move(statistics))
            , Final(final)
        {}

        explicit TEvAnalyzeActorResult(EStatus status) : Status(status), Final(true) {}
    };
};

} // NStat
} // NKikimr
