#pragma once

#include "column_statistic_eval.h"

#include <ydb/core/statistics/events.h>
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

    // StateQuery

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

    struct TEvalTask {
        size_t ColumnIdx = -1;

        // One of the following
        TSimpleColumnStatisticEval::TPtr SimpleStatEval;
        IStage2ColumnStatisticEval::TPtr Stage2StatEval;
    };

    TString TableName;
    TVector<TColumnDesc> Columns;

    std::queue<TEvalTask> PendingTasks;

    std::optional<ui32> CountSeq;
    std::vector<TEvalTask> InProgressTasks;
    TActorId ScanActorId;

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

    void DispatchScanActor();

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

    STFUNC(StateQuery) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvAnalyzeScanResult, Handle);
            hFunc(TEvents::TEvPoison, Handle);
        }
    }
};

} // NKikimr::NStat
