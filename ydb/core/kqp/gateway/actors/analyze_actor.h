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
    TAnalyzeActor(TString tablePath, TVector<TString> columns, NThreading::TPromise<NYql::IKikimrGateway::TGenericResult> promise);

    void Bootstrap();

    STFUNC(StateWork) {
        switch(ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            HFunc(NStat::TEvStatistics::TEvAnalyzeResponse, Handle);
            HFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            HFunc(TEvAnalyzePrivate::TEvAnalyzeRetry, Handle);
            default: 
                HandleUnexpectedEvent(ev->GetTypeRewrite());
        }
    }

private:
    void Handle(NStat::TEvStatistics::TEvAnalyzeResponse::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvAnalyzePrivate::TEvAnalyzeRetry::TPtr& ev, const TActorContext& ctx);

    void HandleUnexpectedEvent(ui32 typeRewrite);

private:
    void SendStatisticsAggregatorAnalyze(const NSchemeCache::TSchemeCacheNavigate::TEntry&, const TActorContext&);

private:
    TString TablePath;
    TVector<TString> Columns;
    NThreading::TPromise<NYql::IKikimrGateway::TGenericResult> Promise;
    // For Statistics Aggregator
    std::optional<ui64> StatisticsAggregatorId;
    TPathId PathId;
    TString OperationId;

    // for retries
    NSchemeCache::TSchemeCacheNavigate::TEntry Entry;
    NStat::TEvStatistics::TEvAnalyze Request;
    TDuration RetryInterval = TDuration::Seconds(2);
    size_t RetryCount = 0;

    const static size_t MaxRetryCount = 7;
    const static size_t RetryIntervalMultiplier = 2;
};

} // end of NKikimr::NKqp
