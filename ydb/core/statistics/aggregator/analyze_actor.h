#pragma once

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
    TVector<ui32> ColumnTags;

    void FinishWithFailure();

    // StateNavigate

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);

    // StateQuery

    ui32 CountSeq = -1;
    struct TColumnDesc {
        explicit TColumnDesc(ui32 tag, ui32 seq)
        : Tag(tag), Seq(seq)
        {}

        ui32 Tag;
        ui32 Seq;
    };
    TVector<TColumnDesc> Columns;

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

    void Handle(TEvPrivate::TEvAnalyzeScanResult::TPtr& ev);

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
    , ColumnTags(std::move(columnTags))
    {}

    void Bootstrap();

    STFUNC(StateNavigate) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
        }
    }

    STFUNC(StateQuery) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvAnalyzeScanResult, Handle);
        }
    }
};

} // NKikimr::NStat
