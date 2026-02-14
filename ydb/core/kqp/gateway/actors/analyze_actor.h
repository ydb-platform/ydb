#pragma once

#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/statistics/events.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>


namespace NKikimr::NKqp {


struct TEvAnalyzePrivate {
    enum EEv {
        EvAnalyzeRetry = EventSpaceBegin(TEvents::ES_PRIVATE),
        EvEnd
    };

    struct TEvAnalyzeRetry : public TEventLocal<TEvAnalyzeRetry, EvAnalyzeRetry> {};
};

class TAnalyzeActor : public NActors::TActorBootstrapped<TAnalyzeActor> {
public:
    TAnalyzeActor(const TString& database,const TString& tablePath,
        const TVector<TString>& columns, NThreading::TPromise<NYql::IKikimrGateway::TGenericResult> promise);

    void Bootstrap();

    STFUNC(StateWork) {
        switch(ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            HFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            HFunc(TEvAnalyzePrivate::TEvAnalyzeRetry, Handle);
            HFunc(NStat::TEvStatistics::TEvAnalyzeResponse, Handle);
            HFunc(TEvKqp::TEvAbortExecution, Handle);
            default:
                HandleUnexpectedEvent(ev->GetTypeRewrite());
        }
    }

private:
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvAnalyzePrivate::TEvAnalyzeRetry::TPtr& ev, const TActorContext& ctx);
    void Handle(NStat::TEvStatistics::TEvAnalyzeResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvKqp::TEvAbortExecution::TPtr& ev, const TActorContext& ctx);
    void HandleUnexpectedEvent(ui32 typeRewrite);

    void PassAway() final;

private:
    void SendStatisticsAggregatorAnalyze(const NSchemeCache::TSchemeCacheNavigate::TEntry&, const TActorContext&);

    TDuration CalcBackoffTime();

private:
    const TString Database;
    const TString TablePath;
    const TVector<TString> Columns;
    NThreading::TPromise<NYql::IKikimrGateway::TGenericResult> Promise;
    // For Statistics Aggregator
    std::optional<ui64> StatisticsAggregatorId;
    TULIDGenerator UlidGen;
    TPathId PathId;
    const TString OperationId;

    // for retries
    NStat::TEvStatistics::TEvAnalyze Request;
    TDuration RetryInterval = TDuration::MilliSeconds(5);
    size_t RetryCount = 0;

    constexpr static size_t MaxRetryCount = 10;
    constexpr static double UncertainRatio = 0.5;
    constexpr static double MaxBackoffDurationMs = TDuration::Seconds(15).MilliSeconds();
};

} // end of NKikimr::NKqp
