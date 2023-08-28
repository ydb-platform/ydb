#include "stat_service.h"
#include "events.h"

#include <ydb/library/services/services.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/base/tablet_pipecache.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>

namespace NKikimr {
namespace NStat {

class TStatService : public TActorBootstrapped<TStatService> {
public:
    using TBase = TActorBootstrapped<TStatService>;

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::STAT_SERVICE;
    }

    void Bootstrap() {
        Become(&TStatService::StateWork);
    }

    STFUNC(StateWork) {
        switch(ev->GetTypeRewrite()) {
            hFunc(TEvStatistics::TEvGetStatistics, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvStatistics::TEvGetStatisticsFromSSResult, Handle);
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                LOG_CRIT_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
                    "NStat::TStatService: unexpected event# " << ev->GetTypeRewrite());
        }
    }

private:
    void Handle(TEvStatistics::TEvGetStatistics::TPtr& ev) {
        ui64 requestId = NextRequestId++;

        auto& request = InFlight[requestId];
        request.ReplyToActorId = ev->Sender;
        request.EvCookie = ev->Cookie;
        request.StatRequests.swap(ev->Get()->StatRequests);

        using TNavigate = NSchemeCache::TSchemeCacheNavigate;
        auto navigate = MakeHolder<TNavigate>();
        for (const auto& req : request.StatRequests) {
            TNavigate::TEntry entry;
            entry.TableId = TTableId(req.PathId.OwnerId, req.PathId.LocalPathId);
            entry.Operation = TNavigate::EOp::OpPath;
            entry.RequestType = TNavigate::TEntry::ERequestType::ByTableId;
            entry.RedirectRequired = false;
            navigate->ResultSet.push_back(entry);
        }

        navigate->Cookie = requestId;

        Send(MakeSchemeCacheID(),
            new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.Release()));
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        using TNavigate = NSchemeCache::TSchemeCacheNavigate;
        THolder<TNavigate> navigate(ev->Get()->Request.Release());

        ui64 requestId = navigate->Cookie;
        auto requestFound = InFlight.find(requestId);
        if (requestFound == InFlight.end()) {
            return;
        }
        auto& request = requestFound->second;

        std::unordered_set<ui64> ssIds;
        for (const auto& entry : navigate->ResultSet) {
            if (entry.Status != TNavigate::EStatus::Ok) {
                continue;
            }
            ssIds.insert(entry.DomainInfo->ExtractSchemeShard());
        }
        if (ssIds.size() != 1) {
            ReplyFailed(requestId);
            return;
        }
        ui64 schemeShardId = *ssIds.begin();

        auto requestSS = MakeHolder<TEvStatistics::TEvGetStatisticsFromSS>();
        requestSS->Record.SetRequestId(requestId);
        for (const auto& entry : request.StatRequests) {
            auto& pathId = *requestSS->Record.AddPathIds();
            pathId.SetOwnerId(entry.PathId.OwnerId);
            pathId.SetLocalId(entry.PathId.LocalPathId);
        }

        Send(MakePipePeNodeCacheID(false), new TEvPipeCache::TEvForward(
            requestSS.Release(), schemeShardId, true, requestId));
    }

    void Handle(TEvStatistics::TEvGetStatisticsFromSSResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        ui64 requestId = record.GetRequestId();
        auto requestFound = InFlight.find(requestId);
        if (requestFound == InFlight.end()) {
            return;
        }
        auto& request = requestFound->second;

        auto result = MakeHolder<TEvStatistics::TEvGetStatisticsResult>();
        result->Success = true;
        size_t i = 0;
        for (auto& req : request.StatRequests) {
            const auto& entry = record.GetEntries(i++);
            if (entry.GetPathId().GetOwnerId() != req.PathId.OwnerId ||
                entry.GetPathId().GetLocalId() != req.PathId.LocalPathId)
            {
                result->Success = false;
                break;
            }
            TResponse rsp;
            rsp.Success = entry.GetSuccess();
            rsp.Req = req;
            TStatSimple stat;
            stat.RowCount = entry.GetRowCount();
            stat.BytesSize = entry.GetBytesSize();
            rsp.Statistics = stat;
            result->StatResponses.push_back(rsp);
        }

        Send(request.ReplyToActorId, result.Release(), 0, request.EvCookie);

        InFlight.erase(requestId);
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        auto upperBound = InFlight.upper_bound(ev->Cookie);
        for (auto it = InFlight.begin(); it != upperBound; ++it) {
            ReplyFailed(it->first, false);
        }
        InFlight.erase(InFlight.begin(), upperBound);
    }

    void ReplyFailed(ui64 requestId, bool removeRequest = true) {
        auto requestFound = InFlight.find(requestId);
        if (requestFound == InFlight.end()) {
            return;
        }
        auto& request = requestFound->second;

        auto result = MakeHolder<TEvStatistics::TEvGetStatisticsResult>();
        result->Success = false;
        for (auto& req : request.StatRequests) {
            TResponse rsp;
            rsp.Success = false;
            rsp.Req = req;
        }

        Send(request.ReplyToActorId, std::move(result), 0, request.EvCookie);

        if (removeRequest) {
            InFlight.erase(requestId);
        }
    }

    void PassAway() override {
        Send(MakePipePeNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));
        TBase::PassAway();
    }

private:
    struct TRequestState {
        NActors::TActorId ReplyToActorId;
        ui64 EvCookie = 0;
        std::vector<TRequest> StatRequests;
    };
    std::map<ui64, TRequestState> InFlight; // id -> state
    ui64 NextRequestId = 1;
};

THolder<IActor> CreateStatService() {
    return MakeHolder<TStatService>();
}

} // NStat
} // NKikimr

