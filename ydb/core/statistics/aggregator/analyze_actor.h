#pragma once

#include "column_statistic_eval.h"
#include "select_builder.h"

#include <ydb/core/statistics/events.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorid.h>

#include <queue>

namespace NKikimr::NStat {

class TAnalyzeActor : public NActors::TActorBootstrapped<TAnalyzeActor> {
    TActorId Parent;
    TString OperationId;
    TString DatabaseName;
    TPathId PathId;
    TVector<ui32> RequestedColumnTags;

    void FinishWithFailure(TEvStatistics::TEvAnalyzeActorResult::EStatus, NYql::TIssue);

    // StateNavigate

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);

    // StateResolve

    void Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev);
    void Handle(TEvHive::TEvResponseTabletDistribution::TPtr& ev);

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

    TVector<ui64> TabletIds;
    THashMap<ui32, TVector<ui64>> NodeId2Tablets;

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

    THashMap<ui32, TVector<ui64>> NodeId2PendingTablets;

    struct TScanActorInfo {
        ui32 TabletNodeId = 0;
    };
    THashMap<TActorId, TScanActorInfo> ScanActorsInFlight;

    std::optional<ui64> RowCount;

    struct TEvPrivate {
        enum EEv {
            EvAnalyzeScanResult = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
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
    };

    class TScanActor;

    void StartColumnStatEvalTasks();

    void Handle(TEvPrivate::TEvAnalyzeScanResult::TPtr& ev);
    void Handle(TEvents::TEvPoison::TPtr& ev);

public:
    TAnalyzeActor(
        TActorId parent,
        TString operationId,
        TString databaseName,
        TPathId pathId,
        TVector<ui32> columnTags)
    : Parent(parent)
    , OperationId(std::move(operationId))
    , DatabaseName(std::move(databaseName))
    , PathId(std::move(pathId))
    , RequestedColumnTags(std::move(columnTags))
    {}

    void Bootstrap();

    STFUNC(StateNavigate) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvents::TEvPoison, Handle);
        }
    }

    STFUNC(StateResolve) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, Handle);
            hFunc(TEvHive::TEvResponseTabletDistribution, Handle);
            hFunc(TEvents::TEvPoison, Handle);
        }
    }

    STFUNC(StateScan) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvAnalyzeScanResult, Handle);
            hFunc(TEvents::TEvPoison, Handle);
        }
    }
};

} // NKikimr::NStat
