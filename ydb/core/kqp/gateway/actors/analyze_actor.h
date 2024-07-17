#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/statistics/events.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>


namespace NKikimr::NKqp {

class TAnalyzeActor : public NActors::TActorBootstrapped<TAnalyzeActor> { 
public:
    TAnalyzeActor(const TString& tablePath, NThreading::TPromise<NYql::IKikimrGateway::TGenericResult> promise)
        : TablePath(tablePath) 
        , Promise(promise)
    {}

    void Bootstrap();

    STFUNC(StateWork) {
        switch(ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            HFunc(NStat::TEvStatistics::TEvScanTableResponse, Handle);
            IgnoreFunc(NStat::TEvStatistics::TEvScanTableAccepted);
            default: 
                HandleUnexpectedEvent(ev->GetTypeRewrite());
        }
    }

private:
    void Handle(NStat::TEvStatistics::TEvScanTableResponse::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx);

    void HandleUnexpectedEvent(ui32 typeRewrite);

    void SendStatisticsAggregatorAnalyze(ui64 statisticsAggregatorId);

private:
    TString TablePath;
    NThreading::TPromise<NYql::IKikimrGateway::TGenericResult> Promise;
    
    // For Statistics Aggregator
    TPathId PathId;
};

} // end of NKikimr::NKqp
