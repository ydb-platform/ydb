#pragma once

#include "column_statistic_eval.h"

#include <ydb/core/statistics/events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorid.h>

namespace NKikimr::NStat {

class TAnalyzeActor : public NActors::TActorBootstrapped<TAnalyzeActor> {
    TActorId Parent;
    TString OperationId;
    TString DatabaseName;
    TPathId PathId;
    TVector<ui32> RequestedColumnTags;

    TActorId ScanActorId;

    void FinishWithFailure(TEvStatistics::TEvFinishTraversal::EStatus, NYql::TIssue);

    // StateNavigate

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);

    // StateQuery

    struct TColumnDesc {
        ui32 Tag;
        NScheme::TTypeInfo Type;
        TString PgTypeMod;
        TString Name;

        std::optional<ui32> CountDistinctSeq;
        std::optional<ui32> MinSeq;
        std::optional<ui32> MaxSeq;
        TVector<IColumnStatisticEval::TPtr> Statistics;

        explicit TColumnDesc(ui32 tag, NScheme::TTypeInfo type, TString pgTypeMod, TString name)
            : Tag(tag)
            , Type(type)
            , PgTypeMod(std::move(pgTypeMod))
            , Name(std::move(name))
        {}
        TColumnDesc(TColumnDesc&&) noexcept = default;
        TColumnDesc& operator=(TColumnDesc&&) noexcept = default;

        NKikimrStat::TSimpleColumnStatistics ExtractSimpleStats(
            ui64 count, const TVector<NYdb::TValue>& aggColumns) const;
    };

    TString TableName;
    std::optional<ui32> CountSeq;
    TVector<TColumnDesc> Columns;
    TVector<TStatisticsItem> Results;

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

    void HandleStage1(TEvPrivate::TEvAnalyzeScanResult::TPtr& ev);
    void HandleStage2(TEvPrivate::TEvAnalyzeScanResult::TPtr& ev);

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

    // Calculate simple column statistics (count, count distinct) and
    // determine the parameters for heavy statistics.
    STFUNC(StateQueryStage1) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvAnalyzeScanResult, HandleStage1);
            hFunc(TEvents::TEvPoison, Handle);
        }
    }

    // Calculate "heavy" statistics requested from stage 1.
    STFUNC(StateQueryStage2) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvAnalyzeScanResult, HandleStage2);
            hFunc(TEvents::TEvPoison, Handle);
        }
    }
};

} // NKikimr::NStat
