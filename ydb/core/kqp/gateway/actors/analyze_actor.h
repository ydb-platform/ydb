#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/statistics/events.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>


namespace NKikimr::NKqp {


struct TEvAnalyzePrivate {
    enum EEv {
        EvAnalyzeStatusCheck = EventSpaceBegin(TEvents::ES_PRIVATE),
        EvEnd
    };

    struct TEvAnalyzeStatusCheck : public TEventLocal<TEvAnalyzeStatusCheck, EvAnalyzeStatusCheck> {};
};

class TAnalyzeActor : public NActors::TActorBootstrapped<TAnalyzeActor> { 
public:
    TAnalyzeActor(TString tablePath, TVector<TString> columns, NThreading::TPromise<NYql::IKikimrGateway::TGenericResult> promise);

    void Bootstrap();

    STFUNC(StateWork) {
        switch(ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            HFunc(NStat::TEvStatistics::TEvAnalyzeResponse, Handle);
            HFunc(NStat::TEvStatistics::TEvAnalyzeStatusResponse, Handle);
            HFunc(TEvAnalyzePrivate::TEvAnalyzeStatusCheck, Handle);
            default: 
                HandleUnexpectedEvent(ev->GetTypeRewrite());
        }
    }

private:
    void Handle(NStat::TEvStatistics::TEvAnalyzeResponse::TPtr& ev, const TActorContext& ctx);

    void Handle(NStat::TEvStatistics::TEvAnalyzeStatusResponse::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvAnalyzePrivate::TEvAnalyzeStatusCheck::TPtr& ev, const TActorContext& ctx);

    void HandleUnexpectedEvent(ui32 typeRewrite);

    void SendStatisticsAggregatorAnalyze(const NSchemeCache::TSchemeCacheNavigate::TEntry&, const TActorContext&);

    void SendAnalyzeStatus();

private:
    TString TablePath;
    TVector<TString> Columns;
    NThreading::TPromise<NYql::IKikimrGateway::TGenericResult> Promise;
    // For Statistics Aggregator
    std::optional<ui64> StatisticsAggregatorId;
    TPathId PathId;
    TString OperationId;
};

} // end of NKikimr::NKqp
