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
            hFunc(TEvStatistics::TEvBroadcastStatistics, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
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
        auto navigate = std::make_unique<TNavigate>();
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
            new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.release()));
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        using TNavigate = NSchemeCache::TSchemeCacheNavigate;
        std::unique_ptr<TNavigate> navigate(ev->Get()->Request.Release());

        ui64 requestId = navigate->Cookie;
        auto itRequest = InFlight.find(requestId);
        if (itRequest == InFlight.end()) {
            return;
        }

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

        if (SchemeShardId) {
            if (SchemeShardId != *ssIds.begin()) {
                ReplyFailed(requestId);
                return;
            }
        } else {
            SchemeShardId = *ssIds.begin();
            EstablishPipe();
            RegisterNode();
        }

        if (!HasStatistics) {
            return; // reply on incoming broadcast
        }

        ReplySuccess(requestId, true);
    }

    void Handle(TEvStatistics::TEvBroadcastStatistics::TPtr& ev) {
        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
            "Handle TEvBroadcastStatistics, node id = " << SelfId().NodeId());

        StatisticsMap.clear();

        auto* record = ev->Get()->MutableRecord();
        for (const auto& entry : record->GetEntries()) {
            TPathId pathId(entry.GetPathId().GetOwnerId(), entry.GetPathId().GetLocalId());
            auto& mapEntry = StatisticsMap[pathId];
            mapEntry.RowCount = entry.GetRowCount();
            mapEntry.BytesSize = entry.GetBytesSize();
        }

        HasStatistics = true;

        for (const auto& [requestId, _] : InFlight) {
            ReplySuccess(requestId, false);
        }
        InFlight.clear();

        if (record->NodeIdsSize() == 0) {
            return;
        }

        std::vector<ui32> nodeIds;
        nodeIds.reserve(record->NodeIdsSize());
        for (const auto nodeId : record->GetNodeIds()) {
            nodeIds.push_back(nodeId);
        }

        size_t step = 0;
        if (nodeIds.size() <= STAT_FAN_OUT + 1) {
            step = 0;
        } else if (nodeIds.size() <= STAT_FAN_OUT * (STAT_FAN_OUT + 1)) {
            step = STAT_FAN_OUT;
        } else {
            step = nodeIds.size() / STAT_FAN_OUT;
        }

        auto serialized = std::make_unique<NStat::TEvStatistics::TEvBroadcastStatistics>();
        serialized->MutableRecord()->MutableEntries()->Swap(record->MutableEntries());
        TString preSerializedStats;
        Y_PROTOBUF_SUPPRESS_NODISCARD serialized->GetRecord().SerializeToString(&preSerializedStats);

        for (size_t i = 0; i < nodeIds.size(); ) {
            ui32 leadingNodeId = nodeIds[i++];

            auto broadcast = std::make_unique<TEvStatistics::TEvBroadcastStatistics>();
            broadcast->MutableRecord()->MutableNodeIds()->Reserve(step);
            for (size_t j = 0; i < nodeIds.size() && j < step; ++i, ++j) {
                broadcast->MutableRecord()->AddNodeIds(nodeIds[i]);
            }
            broadcast->PreSerializedData = preSerializedStats;
            Send(MakeStatServiceID(leadingNodeId), broadcast.release());
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
            "ClientConnected"
            << ", node id = " << ev->Get()->ClientId.NodeId()
            << ", client id = " << ev->Get()->ClientId
            << ", server id = " << ev->Get()->ServerId
            << ", status = " << ev->Get()->Status);

        if (ev->Get()->Status != NKikimrProto::OK) {
            SchemeShardPipeClient = TActorId();
            EstablishPipe();
            RegisterNode();
        }        
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
            "ClientDestroyed"
            << ", node id = " << ev->Get()->ClientId.NodeId()
            << ", client id = " << ev->Get()->ClientId
            << ", server id = " << ev->Get()->ServerId);

        SchemeShardPipeClient = TActorId();
        EstablishPipe();
        RegisterNode();
    }

    void EstablishPipe() {
        if (!SchemeShardPipeClient && SchemeShardId) {
            auto policy = NTabletPipe::TClientRetryPolicy::WithRetries();
            NTabletPipe::TClientConfig pipeConfig{policy};
            SchemeShardPipeClient = Register(NTabletPipe::CreateClient(SelfId(), SchemeShardId, pipeConfig));

            LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
                "EstablishPipe, pipe client id = " << SchemeShardPipeClient);
        }
    }

    void RegisterNode() {
        if (!SchemeShardPipeClient) {
            return;
        }

        auto registerNode = std::make_unique<TEvStatistics::TEvRegisterNode>();
        registerNode->Record.SetNodeId(SelfId().NodeId());
        registerNode->Record.SetHasStatistics(HasStatistics);

        NTabletPipe::SendData(SelfId(), SchemeShardPipeClient, registerNode.release());

        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
            "Send register node"
            << ", node id = " << SelfId().NodeId()
            << ", has statistics = " << HasStatistics);
    }

    void ReplySuccess(ui64 requestId, bool eraseRequest) {
        auto itRequest = InFlight.find(requestId);
        if (itRequest == InFlight.end()) {
            return;
        }
        auto& request = itRequest->second;

        auto result = std::make_unique<TEvStatistics::TEvGetStatisticsResult>();
        result->Success = true;

        for (auto& req : request.StatRequests) {
            auto itStat = StatisticsMap.find(req.PathId);

            TResponse rsp;
            rsp.Success = true;
            rsp.Req = req;

            TStatSimple stat;
            if (itStat != StatisticsMap.end()) {
                stat.RowCount = itStat->second.RowCount;
                stat.BytesSize = itStat->second.BytesSize;
            } else {
                stat.RowCount = 0;
                stat.BytesSize = 0;
            }
            rsp.Statistics = stat;

            result->StatResponses.push_back(rsp);
        }

        Send(request.ReplyToActorId, result.release(), 0, request.EvCookie);

        if (eraseRequest) {
            InFlight.erase(requestId);
        }
    }

    void ReplyFailed(ui64 requestId) {
        auto itRequest = InFlight.find(requestId);
        if (itRequest == InFlight.end()) {
            return;
        }
        auto& request = itRequest->second;

        auto result = std::make_unique<TEvStatistics::TEvGetStatisticsResult>();
        result->Success = false;

        for (auto& req : request.StatRequests) {
            TResponse rsp;
            rsp.Success = false;
            rsp.Req = req;
            result->StatResponses.push_back(rsp);
        }

        Send(request.ReplyToActorId, result.release(), 0, request.EvCookie);

        InFlight.erase(requestId);
    }

    void PassAway() {
        if (SchemeShardPipeClient) {
            NTabletPipe::CloseClient(SelfId(), SchemeShardPipeClient);
        }
        TBase::PassAway();
    }

private:
    struct TRequestState {
        NActors::TActorId ReplyToActorId;
        ui64 EvCookie = 0;
        std::vector<TRequest> StatRequests;
        bool WaitingForStatistics = false;
    };
    std::map<ui64, TRequestState> InFlight; // request id -> state
    ui64 NextRequestId = 1;

    static const size_t STAT_FAN_OUT = 10;

    struct TStatEntry {
        ui64 RowCount = 0;
        ui64 BytesSize = 0;
    };
    std::unordered_map<TPathId, TStatEntry> StatisticsMap;
    bool HasStatistics = false;

    ui64 SchemeShardId = 0;
    TActorId SchemeShardPipeClient;
};

THolder<IActor> CreateStatService() {
    return MakeHolder<TStatService>();
}

} // NStat
} // NKikimr
