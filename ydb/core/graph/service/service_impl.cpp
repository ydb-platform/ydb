#include "log.h"
#include <ydb/core/graph/api/service.h>
#include <ydb/core/graph/api/events.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/path.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::GRAPH

namespace NKikimr {
namespace NGraph {

class TGraphService : public TActor<TGraphService> {
private:
    using TBase = TActor<TGraphService>;
    static constexpr TDuration RESOLVE_TIMEOUT = TDuration::Seconds(1);
    static constexpr TDuration CONNECT_TIMEOUT = TDuration::Seconds(1);
    static constexpr TDuration GET_TIMEOUT = TDuration::Seconds(10);
    static constexpr size_t MAX_INFLIGHT = 100;
    TString Database;
    TInstant ResolveTimestamp;
    ui64 GraphShardId = 0;
    TInstant ConnectTimestamp;
    TActorId GraphShardPipe = {};

    struct TGetMetricsRequest {
        ui64 Id;
        TInstant Deadline;
        TActorId Sender;
        ui64 Cookie;
        NKikimrGraph::TEvGetMetrics Request;
    };

    ui64 RequestId = 0;
    std::deque<TGetMetricsRequest> RequestsInFlight;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRAPH_SERVICE;
    }

    TGraphService(const TString& database)
        : TActor(&TGraphService::StateWork)
        , Database(database)
    {
    }

    TString GetLogPrefix() const {
        return "SVC ";
    }

    void ResolveDatabase() {
        if (ResolveTimestamp && (ResolveTimestamp + RESOLVE_TIMEOUT > TActivationContext::Now())) {
            YDB_LOG_TRACE("ResolveDatabase too soon",
                {"LogPrefix", GetLogPrefix()});
            return; // too soon
        }

        YDB_LOG_DEBUG("ResolveDatabase",
            {"LogPrefix", GetLogPrefix()},
            {"Database", Database});
        TAutoPtr<NSchemeCache::TSchemeCacheNavigate> request(new NSchemeCache::TSchemeCacheNavigate());
        request->DatabaseName = Database;

        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;
        entry.SyncVersion = false;
        entry.Path = SplitPath(Database);
        request->ResultSet.emplace_back(entry);
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request));
        ResolveTimestamp = TActivationContext::Now();
    }

    NTabletPipe::TClientConfig GetPipeClientConfig() {
        NTabletPipe::TClientConfig clientConfig;
        clientConfig.RetryPolicy = {.RetryLimitCount = 3};
        return clientConfig;
    }

    void ConnectShard() {
        if (GraphShardId) {
            if (ConnectTimestamp && (ConnectTimestamp + CONNECT_TIMEOUT > TActivationContext::Now())) {
                YDB_LOG_TRACE("ConnectShard too soon",
                    {"LogPrefix", GetLogPrefix()});
                return; // too soon
            }
            YDB_LOG_DEBUG("ConnectToShard",
                {"LogPrefix", GetLogPrefix()},
                {"GraphShardId", GraphShardId});
            IActor* pipeActor = NTabletPipe::CreateClient(TBase::SelfId(), GraphShardId, GetPipeClientConfig());
            GraphShardPipe = TBase::RegisterWithSameMailbox(pipeActor);
            ConnectTimestamp = TActivationContext::Now();
        } else {
            ResolveDatabase();
        }
    }

    void SendRequest(const TGetMetricsRequest& request) {
        if (GraphShardPipe) {
            TEvGraph::TEvGetMetrics* event = new TEvGraph::TEvGetMetrics();
            event->Record = request.Request;
            NTabletPipe::SendData(SelfId(), GraphShardPipe, event, request.Id);
        } else {
            ConnectShard();
        }
    }

    void EnqueueRequest(TEvGraph::TEvGetMetrics::TPtr& ev) {
        if (RequestsInFlight.size() >= MAX_INFLIGHT) {
            TEvGraph::TEvMetricsResult* response = new TEvGraph::TEvMetricsResult();
            response->Record.SetError("Maximum number of outstanding requests reached");
            Send(ev->Sender, response, 0, ev->Cookie);
            return;
        }
        if (RequestsInFlight.empty()) {
            Schedule(GET_TIMEOUT, new TEvents::TEvWakeup());
        }
        RequestsInFlight.push_back({
            .Id = ++RequestId,
            .Deadline = TActivationContext::Now() + GET_TIMEOUT,
            .Sender = ev->Sender,
            .Cookie = ev->Cookie,
            .Request = std::move(ev->Get()->Record)
        });
        SendRequest(RequestsInFlight.back());
    }

    void DiscardOldRequests(TInstant now) {
        while (!RequestsInFlight.empty() && RequestsInFlight.front().Deadline <= now) {
            YDB_LOG_WARN("Discarding request with id",
                {"LogPrefix", GetLogPrefix()},
                {"RequestsId", RequestsInFlight.front().Id});
            TEvGraph::TEvMetricsResult* response = new TEvGraph::TEvMetricsResult();
            response->Record.SetError("Request timed out");
            Send(RequestsInFlight.front().Sender, response, 0, RequestsInFlight.front().Cookie);
            RequestsInFlight.pop_front();
        }
    }

    void ResendRequests() {
        for (const TGetMetricsRequest& request : RequestsInFlight) {
            YDB_LOG_TRACE("Resending request",
                {"LogPrefix", GetLogPrefix()},
                {"RequestId", request.Id});
            NTabletPipe::SendData(SelfId(), GraphShardPipe, new TEvGraph::TEvGetMetrics(request.Request), request.Id);
        }
    }

    void Handle(TEvGraph::TEvSendMetrics::TPtr& ev) {
        YDB_LOG_TRACE("TEvSendMetrics",
            {"LogPrefix", GetLogPrefix()});
        if (GraphShardPipe) {
            NTabletPipe::SendData(SelfId(), GraphShardPipe, ev.Get()->Release());
        } else {
            ConnectShard();
            YDB_LOG_TRACE("Dropped metrics",
                {"LogPrefix", GetLogPrefix()});
        }
    }

    void Handle(TEvGraph::TEvGetMetrics::TPtr& ev) {
        YDB_LOG_TRACE("TEvGetMetrics",
            {"LogPrefix", GetLogPrefix()});
        if (!GraphShardPipe) {
            ConnectShard();
        }
        EnqueueRequest(ev);
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        TAutoPtr<NSchemeCache::TSchemeCacheNavigate> request = ev->Get()->Request;
        if (!request->ResultSet.empty() && request->ResultSet.front().Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            const NSchemeCache::TSchemeCacheNavigate::TEntry& response = request->ResultSet.front();
            if (response.DomainDescription) {
                if (response.DomainDescription->Description.GetProcessingParams().GetGraphShard() != 0) {
                    GraphShardId = response.DomainDescription->Description.GetProcessingParams().GetGraphShard();
                    YDB_LOG_DEBUG("Database resolved to shard",
                        {"LogPrefix", GetLogPrefix()},
                        {"Database", Database},
                        {"GraphShardId", GraphShardId});
                    ConnectShard();
                    return;
                } else {
                    YDB_LOG_DEBUG("Error resolving database - no graph shard (switching to pumpkin mode)",
                        {"LogPrefix", GetLogPrefix()},
                        {"Database", Database});
                    return Become(&TGraphService::StatePumpkin);
                }
            }
            YDB_LOG_WARN("Error resolving database incomplete response",
                {"LogPrefix", GetLogPrefix()},
                {"Database", Database});
        } else {
            if (!request->ResultSet.empty()) {
                YDB_LOG_WARN("Error resolving database error",
                    {"LogPrefix", GetLogPrefix()},
                    {"Database", Database},
                    {"Status", request->ResultSet.front().Status});
            } else {
                YDB_LOG_WARN("Error resolving database no response",
                    {"LogPrefix", GetLogPrefix()},
                    {"Database", Database});
            }
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status == NKikimrProto::OK) {
            YDB_LOG_DEBUG("Connected to shard",
                {"LogPrefix", GetLogPrefix()},
                {"GraphShardId", GraphShardId});
            ResendRequests();
        } else {
            YDB_LOG_WARN("Error connecting to shard error",
                {"LogPrefix", GetLogPrefix()},
                {"GraphShardId", GraphShardId},
                {"Status", ev->Get()->Status});
            NTabletPipe::CloseClient(TBase::SelfId(), GraphShardPipe);
            GraphShardPipe = {};
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        YDB_LOG_WARN("Connection to shard was destroyed",
            {"LogPrefix", GetLogPrefix()});
        NTabletPipe::CloseClient(TBase::SelfId(), GraphShardPipe);
        GraphShardPipe = {};
    }

    void Handle(TEvGraph::TEvMetricsResult::TPtr& ev) {
        auto id(ev->Cookie);
        YDB_LOG_TRACE("TEvMetricsResult",
            {"LogPrefix", GetLogPrefix()},
            {"Id", id});
        for (auto it = RequestsInFlight.begin(); it != RequestsInFlight.end(); ++it) {
            if (it->Id == id) {
                YDB_LOG_TRACE("TEvMetricsResult found request resending to",
                    {"LogPrefix", GetLogPrefix()},
                    {"Id", id},
                    {"Sender", it->Sender});
                Send(it->Sender, ev->Release().Release(), 0, it->Cookie);
                RequestsInFlight.erase(it);
                return;
            }
        }
        YDB_LOG_WARN("Couldn't find request with id",
            {"LogPrefix", GetLogPrefix()},
            {"Id", id});
    }

    void HandleTimeout() {
        TInstant now(TActivationContext::Now());
        DiscardOldRequests(now);
        if (!RequestsInFlight.empty()) {
            Schedule(RequestsInFlight.front().Deadline - now, new TEvents::TEvWakeup());
        }
    }

    void HandlePumpkin(TEvGraph::TEvGetMetrics::TPtr& ev) {
        YDB_LOG_TRACE("TEvGetMetrics(Pumpkin)",
            {"LogPrefix", GetLogPrefix()});
        TEvGraph::TEvMetricsResult* response = new TEvGraph::TEvMetricsResult();
        response->Record.SetError("GraphShard is not enabled on the database");
        Send(ev->Sender, response, 0, ev->Cookie);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvGraph::TEvSendMetrics, Handle);
            hFunc(TEvGraph::TEvGetMetrics, Handle);
            hFunc(TEvGraph::TEvMetricsResult, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    STATEFN(StatePumpkin) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvGraph::TEvGetMetrics, HandlePumpkin);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};


IActor* CreateGraphService(const TString& database) {
    return new TGraphService(database);
}

} // NGraph
} // NKikimr
