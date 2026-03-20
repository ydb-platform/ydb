#pragma once

#include "column_statistic_eval.h"
#include "select_builder.h"

#include <ydb/core/statistics/events.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorid.h>

#include <queue>

namespace NKikimr::NStat {

class TAnalyzeActor : public NActors::TActorBootstrapped<TAnalyzeActor> {
public:
    struct TConfig {
        ui64 MaxTotalScanActorsInFlight = 100;
        i64 MaxPerNodeScanActorsInFlight = 1;
    };

private:
    TActorId Parent;
    TString OperationId;
    TString DatabaseName;
    TPathId PathId;
    TVector<ui32> RequestedColumnTags;
    TConfig Config;

    struct TEvPrivate {
        enum EEv {
            EvAnalyzeScanResult = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
            EvRetryTableDistributionRequest,
            EvEnd,
        };

        static_assert(
            EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE),
            "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

        struct TEvAnalyzeScanResult : public TEventLocal<
            TEvAnalyzeScanResult, EvAnalyzeScanResult>
        {
            Ydb::StatusIds::StatusCode Status;
            NYql::TIssues Issues;
            TVector<NYdb::TValue> AggColumns;

            explicit TEvAnalyzeScanResult(TVector<NYdb::TValue> aggColumns)
            : Status(Ydb::StatusIds::SUCCESS)
            , AggColumns(std::move(aggColumns))
            {}

            TEvAnalyzeScanResult(
                Ydb::StatusIds::StatusCode status,
                NYql::TIssues&& issues)
            : Status(status)
            , Issues(std::move(issues))
            {}
        };

        struct TEvRequestTableDistribution : public TEventLocal<
            TEvRequestTableDistribution, EvRetryTableDistributionRequest>
        {};
    };

    void FinishWithFailure(TEvStatistics::TEvAnalyzeActorResult::EStatus, NYql::TIssue);

    // StateNavigate

    struct TColumnDesc {
        ui32 Tag;
        NScheme::TTypeInfo Type;
        TString PgTypeMod;
        TString Name;

        TColumnDesc(ui32 tag, NScheme::TTypeInfo type, TString pgTypeMod, TString name)
            : Tag(tag)
            , Type(type)
            , PgTypeMod(std::move(pgTypeMod))
            , Name(std::move(name))
        {}
        TColumnDesc(TColumnDesc&&) noexcept = default;
        TColumnDesc& operator=(TColumnDesc&&) noexcept = default;
    };

    TString TableName;
    bool IsColumnTable = false;
    TVector<TColumnDesc> Columns;
    TVector<NScheme::TTypeInfo> KeyColumnTypes;
    ui64 HiveId = 0;

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
    void Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev);

    // StateLocateTablets

    // In this stage tablet -> node correspondence is resolved. Currently this is only needed
    // to avoid scheduling too many queries that scan tablets on one node.

    THashSet<ui64> TabletIdsToLocate;
    THashMap<ui64, ui32> TabletId2NodeId;

    size_t HiveRetryCount = 0;
    static constexpr size_t MaxHiveRetryCount = 3;
    static constexpr TDuration HiveRetryInterval = TDuration::Seconds(5);

    void Handle(TEvPrivate::TEvRequestTableDistribution::TPtr& ev);
    void Handle(TEvHive::TEvResponseTabletDistribution::TPtr& ev);
    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev);
    void TryScheduleHiveRetry(const TStringBuf& issue);

    // StateScan

    // In the simplest case, the table is scanned 2 times:
    // First, simple column statistics (count distinct, min/max, etc.) for each column are
    // calculated and used to determine parameters for stage 2 statistics.
    // Second, stage 2 statistics such as count-min sketches and histograms are calculated.
    //
    // In more complicated cases, we have to split scans both horizontally (scan different
    // columns with different SELECTs) and vertically (scan different parts of the table
    // with different selects). Horizontal splits are needed to prevent the single scan
    // result row from becoming too big, and vertical splits are needed if the table is
    // so big that we cant scan the whole table at once.
    //
    // The order of scanning is as follows: we start a batch of TColumnStatEvalTasks
    // and dispatch individual scans for parts of the table until we scan the whole table.
    // Then we finalize the results, send them to StatisticsAggregator to save and
    // move on to the next batch of of TColumnStatEvalTasks.

    // Represents a task to calculate a single (column, statistic type) pair.
    struct TColumnStatEvalTask {
        size_t ColumnIdx = -1;

        // One of the following
        TSimpleColumnStatisticEval::TPtr SimpleStatEval;
        IStage2ColumnStatisticEval::TPtr Stage2StatEval;
    };

    std::queue<TColumnStatEvalTask> PendingTasks;

    std::optional<TSelectBuilder> SelectBuilder;
    std::optional<ui32> CountSeq;
    std::vector<TColumnStatEvalTask> InProgressTasks;

    struct TNodeState {
        ui32 Id = 0;
        i64 TabletsInFlight = 0;
        TVector<ui64> PendingTablets;

        explicit TNodeState(ui32 id) : Id(id) {}
    };
    THashMap<ui32, TNodeState> NodeId2State;

    struct TScanActorInfo {
        ui32 TabletNodeId = 0;
    };
    THashMap<TActorId, TScanActorInfo> ScanActorsInFlight;

    std::optional<ui64> RowCount;

    class TScanActor;

    void StartColumnStatEvalTasks();
    void DispatchSomeScanActors();

    void HandleImpl(TEvPrivate::TEvAnalyzeScanResult::TPtr& ev);
    void Handle(TEvPrivate::TEvAnalyzeScanResult::TPtr& ev);

public:
    TAnalyzeActor(
        TActorId parent,
        TString operationId,
        TString databaseName,
        TPathId pathId,
        TVector<ui32> columnTags,
        const TConfig& config)
    : Parent(parent)
    , OperationId(std::move(operationId))
    , DatabaseName(std::move(databaseName))
    , PathId(std::move(pathId))
    , RequestedColumnTags(std::move(columnTags))
    , Config(config)
    {}

    void Bootstrap();
    void PassAway() final;

    STFUNC(StateNavigate) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

    STFUNC(StateLocateTablets) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvRequestTableDistribution, Handle);
            hFunc(TEvHive::TEvResponseTabletDistribution, Handle);
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

    STFUNC(StateScan) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvAnalyzeScanResult, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }
};

} // NKikimr::NStat
