#include <ydb/core/statistics/events.h>

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/util/ulid.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <unordered_map>

namespace NKikimr {
namespace NStat {

class THttpRequest : public NActors::TActorBootstrapped<THttpRequest> {
public:
    using TBase = TActorBootstrapped<THttpRequest>;

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::STAT_SERVICE_HTTP_REQUEST;
    }

    void Bootstrap();

    enum class ERequestType {
        ANALYZE,
        STATUS,
        PROBE_COUNT_MIN_SKETCH,
        PROBE_BASE_STATS,
    };

    enum class EParamType {
        PATH,
        OPERATION_ID,
        COLUMN_NAME,
        CELL_VALUE
    };

    enum class EResponseContentType {
        HTML,
        JSON,
    };

    THttpRequest(
        ERequestType requestType,
        const std::unordered_map<EParamType, TString>& params,
        EResponseContentType contentType,
        const TActorId& replyToActorId);

private:
    using TNavigate = NSchemeCache::TSchemeCacheNavigate;

    STFUNC(StateWork) {
        switch(ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvStatistics::TEvAnalyzeStatusResponse, Handle);
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            hFunc(TEvStatistics::TEvGetStatisticsResult, Handle);
            IgnoreFunc(TEvStatistics::TEvAnalyzeResponse);
            default:
                LOG_CRIT_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
                    "NStat::THttpRequest: unexpected event# " << ev->GetTypeRewrite());
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
    void Handle(TEvStatistics::TEvAnalyzeStatusResponse::TPtr& ev);
    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev);
    void Handle(TEvStatistics::TEvGetStatisticsResult::TPtr& ev);

    void DoRequest(const TNavigate::TEntry& entry);
    void DoAnalyze(const TNavigate::TEntry& entry);
    void DoStatus(const TNavigate::TEntry& entry);
    void DoProbeDoCountMinSketch(const TNavigate::TEntry& entry);
    void DoProbeBaseStats(const TNavigate::TEntry& entry);

    void HttpReply(const TString& msg);
    void HttpReplyError(const TString& msg);

    void PassAway();

private:
    const ERequestType RequestType;
    std::unordered_map<EParamType, TString> Params;
    const EResponseContentType ContentType;
    const TActorId ReplyToActorId;
    TULIDGenerator UlidGen;
};

} // NStat
} // NKikimr