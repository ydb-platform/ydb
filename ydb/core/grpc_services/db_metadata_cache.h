#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/health_check/health_check.h>
#include <ydb/core/protos/db_metadata_cache.pb.h>

#include <algorithm>

namespace NKikimr {

inline TString MakeDatabaseMetadataCacheBoardPath(const TString& database) {
    return "metadatacache+" + database;
}

class TDatabaseMetadataCache : public TActorBootstrapped<TDatabaseMetadataCache> {
public:
    enum EEv {
        EvRefreshCache = EventSpaceBegin(TKikimrEvents::ES_DB_METADATA_CACHE),
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_DB_METADATA_CACHE),
        "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_DB_METADATA_CACHE)");

    struct TEvRefreshCache : TEventLocal<TEvRefreshCache, EvRefreshCache> {};

    using TBoardInfoEntries = TMap<TActorId, TEvStateStorage::TBoardInfoEntry>;

private:
    static constexpr TDuration TIMEOUT = TDuration::Seconds(15);
    const TString Path;
    const TString BoardPath;
    ui32 ActiveNode;
    std::optional<Ydb::Monitoring::SelfCheckResult> Result;
    TInstant LastResultUpdate;
    std::vector<TActorId> Clients;
    TBoardInfoEntries BoardInfo;
    TActorId PublishActor;
    TActorId SubscribeActor;
    bool RequestInProgress = false;
    ::NMonitoring::TDynamicCounterPtr Counters;
    static const inline TString HEALTHCHECK_REQUESTS_MADE_COUNTER = "DbMetadataCache/HealthCheckRequestsMade";
    static const inline TString HEALTHCHECK_REQUESTS_ANSWERED_COUNTER = "DbMetadataCache/HealthCheckRequestsAnswered";

    void SendRequest() {
        if (RequestInProgress) {
            return;
        }
        RequestInProgress = true;
        auto request = std::make_unique<NHealthCheck::TEvSelfCheckRequest>();
        request->Database = Path;
        request->Request.set_return_verbose_status(true);
        Send(NHealthCheck::MakeHealthCheckID(), request.release());
        Counters->GetCounter(HEALTHCHECK_REQUESTS_MADE_COUNTER, true)->Inc();
    }

    void Reply(TActorId client) {
        auto response = std::make_unique<NHealthCheck::TEvSelfCheckResultProto>();
        response->Record = *Result;
        Send(client, response.release());
        Counters->GetCounter(HEALTHCHECK_REQUESTS_ANSWERED_COUNTER, true)->Inc();
    }

    void RefreshCache() {
        SendRequest();
        Schedule(TIMEOUT, new TEvRefreshCache());
    }

    void UpdateActiveNode() {
        ActiveNode = PickActiveNode(BoardInfo);
        LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::DB_METADATA_CACHE, "Active node is " << ActiveNode);
        bool areWeActive = (ActiveNode == SelfId().NodeId());
        if (areWeActive && CurrentStateFunc() != &TThis::StateActive) {
            RefreshCache();
            Become(&TThis::StateActive);
        } else if (!areWeActive && CurrentStateFunc() != &TThis::StateInactive) {
            Become(&TThis::StateInactive);
        }
    }

    void SubscribeToBoard() {
        SubscribeActor = RegisterWithSameMailbox(CreateBoardLookupActor(BoardPath,
                                                       SelfId(),
                                                       EBoardLookupMode::Subscription));
    }

    void Handle(TEvStateStorage::TEvBoardInfo::TPtr& ev) {
        if (ev->Get()->Status != TEvStateStorage::TEvBoardInfo::EStatus::Ok) {
            SubscribeToBoard();
            return;
        }
        BoardInfo = std::move(ev->Get()->InfoEntries);
        UpdateActiveNode();
    }

    void Handle(TEvStateStorage::TEvBoardInfoUpdate::TPtr& ev) {
        if (ev->Get()->Status != TEvStateStorage::TEvBoardInfo::EStatus::Ok) {
            SubscribeToBoard();
            return;
        }
        auto& updates = ev->Get()->Updates;
        for (auto& [actor, update] : updates) {
            if (update.Dropped) {
                BoardInfo.erase(actor);
            } else {
                BoardInfo.insert_or_assign(actor, std::move(update));
            }
        }
        UpdateActiveNode();
    }

    void Handle(NHealthCheck::TEvSelfCheckResult::TPtr& ev) {
        RequestInProgress = false;
        Result = ev->Get()->Result;
        LastResultUpdate = TActivationContext::Now();
        for (const auto& client : Clients) {
            Reply(client);
        }
        Clients.clear();
    }

    void Handle(NHealthCheck::TEvSelfCheckRequestProto::TPtr& ev) {
        LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::DB_METADATA_CACHE, "Got request");
        TInstant now = TActivationContext::Now();
        if (Result && now - LastResultUpdate <= 2 * TIMEOUT) {
            LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::DB_METADATA_CACHE, "Replying now");
            Reply(ev->Sender);
        } else {
            LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::DB_METADATA_CACHE, "Answer not ready, waiting");
            SendRequest();
            Clients.push_back(ev->Sender);
        }
    }

public:
    TDatabaseMetadataCache(const TString& path,
                           const ::NMonitoring::TDynamicCounterPtr& counters) : Path(path)
                                                                              , BoardPath(MakeDatabaseMetadataCacheBoardPath(Path))
    {
        Counters = GetServiceCounters(counters, "utils");
    }

    static ui32 PickActiveNode(const TBoardInfoEntries& infoEntries) {
        ui32 result = 0;
        TInstant minStartTime = TInstant::Max();
        for (const auto& [actor, entry] : infoEntries) {
            if (entry.Dropped) {
                continue;
            }
            NKikimrMetadataCache::TDatabaseMetadataCacheInfo info;
            if (!info.ParseFromString(entry.Payload) || !info.HasStartTimestamp()) {
                continue;
            }
            TInstant startTime = TInstant::MicroSeconds(info.GetStartTimestamp());
            if (startTime < minStartTime) {
                minStartTime = startTime;
                result = actor.NodeId();
            }
        }
        return result;
    }

    void Bootstrap() {
        LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::DB_METADATA_CACHE, "Starting db metadata cache actor");
        TInstant now = TActivationContext::Now();
        NKikimrMetadataCache::TDatabaseMetadataCacheInfo info;
        info.SetStartTimestamp(now.MicroSeconds());
        PublishActor = RegisterWithSameMailbox(CreateBoardPublishActor(BoardPath,
                                                        info.SerializeAsString(),
                                                        SelfId(),
                                                        0,
                                                        true));
        SubscribeToBoard();
        Become(&TThis::StateWait);
    }

    void PassAway() override {
        Send(PublishActor, new TEvents::TEvPoison);
        Send(SubscribeActor, new TEvents::TEvPoison);
        return TActor::PassAway();
    }

    STATEFN(StateWait) {
        switch (ev->GetTypeRewrite()) {
            IgnoreFunc(TEvRefreshCache);
            hFunc(TEvStateStorage::TEvBoardInfo, Handle);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            hFunc(NHealthCheck::TEvSelfCheckResult, Handle);
            hFunc(NHealthCheck::TEvSelfCheckRequestProto, Handle);
            default: Y_ABORT("Unexpected event: %s", ev->ToString().c_str());
        }
    }

    STATEFN(StateActive) {
        switch (ev->GetTypeRewrite()) {
            cFunc(EvRefreshCache, RefreshCache);
            hFunc(NHealthCheck::TEvSelfCheckResult, Handle);
            hFunc(NHealthCheck::TEvSelfCheckRequestProto, Handle);
            hFunc(TEvStateStorage::TEvBoardInfo, Handle);
            hFunc(TEvStateStorage::TEvBoardInfoUpdate, Handle);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default: Y_ABORT("Unexpected event: %s", ev->ToString().c_str());
        }
    }

    STATEFN(StateInactive) {
        switch (ev->GetTypeRewrite()) {
            IgnoreFunc(TEvRefreshCache);
            hFunc(NHealthCheck::TEvSelfCheckResult, Handle);
            hFunc(NHealthCheck::TEvSelfCheckRequestProto, Handle);
            hFunc(TEvStateStorage::TEvBoardInfo, Handle);
            hFunc(TEvStateStorage::TEvBoardInfoUpdate, Handle);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default: Y_ABORT("Unexpected event: %s", ev->ToString().c_str());
        }
    }
};

inline TActorId MakeDatabaseMetadataCacheId(ui32 nodeId) {
    return TActorId(nodeId, "METACACHE");
}

inline std::unique_ptr<IActor> CreateDatabaseMetadataCache(const TString& path, const ::NMonitoring::TDynamicCounterPtr& counters) {
    return std::unique_ptr<IActor>(new TDatabaseMetadataCache(path, counters));
}

inline std::optional<TActorId> ResolveActiveDatabaseMetadataCache(const TMap<TActorId, TEvStateStorage::TBoardInfoEntry>& infoEntries) {
    auto activeNode = TDatabaseMetadataCache::PickActiveNode(infoEntries);
    if (activeNode == 0) {
        return std::nullopt;
    }
    return MakeDatabaseMetadataCacheId(activeNode);
}

} // NKikimr
