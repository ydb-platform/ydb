#pragma once

#include <ydb/core/statistics/events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorid.h>

namespace NKikimr::NStat {

class TSelectBuilder;

class IColumnStatisticEval {
public:
    using TPtr = std::unique_ptr<IColumnStatisticEval>;

    static TVector<EStatType> SupportedTypes();
    static TPtr MaybeCreate(
        EStatType,
        const NKikimrStat::TSimpleColumnStatistics&,
        const NScheme::TTypeInfo&);

    virtual EStatType GetType() const = 0;
    virtual size_t EstimateSize() const = 0;
    virtual void AddAggregations(const TString& columnName, TSelectBuilder&) = 0;
    virtual TString ExtractData(const TVector<NYdb::TValue>& aggColumns) const = 0;
    virtual ~IColumnStatisticEval() = default;
};

class TAnalyzeActor : public NActors::TActorBootstrapped<TAnalyzeActor> {
    TActorId Parent;
    TString OperationId;
    TString DatabaseName;
    TPathId PathId;
    TVector<ui32> RequestedColumnTags;

    void FinishWithFailure(TEvStatistics::TEvFinishTraversal::EStatus);

    // StateNavigate

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);

    // StateQuery

    struct TColumnDesc {
        ui32 Tag;
        NScheme::TTypeInfo Type;
        TString Name;

        std::optional<ui32> CountDistinctSeq;
        TVector<IColumnStatisticEval::TPtr> Statistics;

        explicit TColumnDesc(ui32 tag, NScheme::TTypeInfo type, TString name)
            : Tag(tag)
            , Type(type)
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
        }
    }

    // Calculate simple column statistics (count, count distinct) and
    // determine the parameters for heavy statistics.
    STFUNC(StateQueryStage1) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvAnalyzeScanResult, HandleStage1);
        }
    }

    // Calculate "heavy" statistics requested from stage 1.
    STFUNC(StateQueryStage2) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvAnalyzeScanResult, HandleStage2);
        }
    }
};

} // NKikimr::NStat
