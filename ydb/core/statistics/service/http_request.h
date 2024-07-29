#include <ydb/core/statistics/events.h>

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr {
namespace NStat {

class THttpRequest : public NActors::TActorBootstrapped<THttpRequest> {
public:
    using TBase = TActorBootstrapped<THttpRequest>;

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::STAT_SERVICE_HTTP_REQUEST;
    }

    void Bootstrap();
   
    enum EType {
        ANALYZE,
        STATUS
    };

    THttpRequest(EType type, const TString& path, TActorId replyToActorId);

private:
    STFUNC(StateWork) {
        switch(ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvStatistics::TEvAnalyzeStatusResponse, Handle);
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            IgnoreFunc(TEvStatistics::TEvAnalyzeResponse);
            default:
                LOG_CRIT_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
                    "NStat::THttpRequest: unexpected event# " << ev->GetTypeRewrite());
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
    void Handle(TEvStatistics::TEvAnalyzeStatusResponse::TPtr& ev);
    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&);

    void ResolveSuccess();
    void HttpReply(const TString& msg);

    void PassAway();

private:
    const EType Type;
    const TString Path;
    const TActorId ReplyToActorId;

    TPathId PathId;
    ui64 StatisticsAggregatorId = 0;

    static const ui64 FirstRoundCookie = 1;
    static const ui64 SecondRoundCookie = 2;    
};

} // NStat
} // NKikimr