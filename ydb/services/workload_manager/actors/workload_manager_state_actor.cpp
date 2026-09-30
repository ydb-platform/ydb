#include "workload_manager_state_actor.h"

#include <ydb/services/workload_manager/gateway_internal.h>
#include <ydb/services/workload_manager/service/service.h>

#include <ydb/services/workload_manager/actors/actors.h>
#include <ydb/services/workload_manager/common/helpers.h>
#include <ydb/services/workload_manager/events.h>
#include <ydb/services/workload_manager/metadata_subscription/resource_pool_classifier/fetcher.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/path.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_compute_scheduler_service.h>
#include <ydb/core/protos/console_config.pb.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/core/protos/workload_manager_config.pb.h>
#include <ydb/core/resource_pools/resource_pool_settings.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/services/metadata/service.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <unordered_map>
#include <unordered_set>


namespace NKikimr::NWorkloadManager {

std::shared_ptr<IQueryClassifier> NPrivate::TWorkloadManagerGateway::TryCreateQueryClassifier(
    const TString& databaseId, TClassifyContext context)
{
    TSnapshotPtr snapshot = Snapshot_;

    if (!snapshot || !snapshot->IsResourcePoolsEnabled(databaseId)) {
        return nullptr;
    }

    const TString effectivePoolId = context.PoolId
        ? context.PoolId
        : NResourcePool::DEFAULT_POOL_ID;

    if (!snapshot->Pools || !snapshot->Pools->contains(GetPoolKey(databaseId, effectivePoolId))) {
        NActors::TActivationContext::Send(new NActors::IEventHandle(
            StateActorId_, {},
            new TEvEnsurePoolSubscribed(databaseId, effectivePoolId)));
    }

    return CreateQueryClassifier(
        snapshot->Pools,
        TClassifierConfigsView(snapshot->Classifiers, databaseId),
        databaseId,
        std::move(context),
        *AppData());
}

void NPrivate::TWorkloadManagerGateway::DoWarmupRequest(const TString& databaseId) {
    if (!StateActorId_) {
        return;
    }

    NActors::TActivationContext::Send(new NActors::IEventHandle(
        StateActorId_, {}, new TEvWarmupDatabaseInfo(DatabaseIdToDatabase(databaseId))));
}

TReadyInfo NPrivate::TWorkloadManagerGateway::EnsureReady(const TString& databaseId) {
    TSnapshotPtr snapshot = Snapshot_;

    if (!snapshot) {
        if (!StateActorId_) {
            return TReadyInfo{.State = EReadyState::ClassificationDisabled};
        }

        DoWarmupRequest(databaseId);
        return TReadyInfo{.State = EReadyState::Pending};
    }

    if (!snapshot->EnableResourcePools) {
        return TReadyInfo{.State = EReadyState::ClassificationDisabled};
    }

    const auto it = snapshot->Databases.find(databaseId);
    if (it != snapshot->Databases.end()) {
        const auto& info = it->second;
        if (info.FetchStatus == Ydb::StatusIds::UNSUPPORTED) {
            return TReadyInfo{.State = EReadyState::ClassificationDisabled};
        }
        if (info.FetchStatus != Ydb::StatusIds::SUCCESS) {
            return TReadyInfo{.State = EReadyState::Failed, .FailureStatus = info.FetchStatus, .FailureMessage = info.FetchMessage};
        }
        if (!snapshot->EnableResourcePoolsOnServerless && info.Serverless) {
            return TReadyInfo{.State = EReadyState::ClassificationDisabled};
        }
        if (snapshot->ClassifierMetadataInitialized) {
            return TReadyInfo{.State = EReadyState::Ready};
        }
    }

    DoWarmupRequest(databaseId);
    return TReadyInfo{.State = EReadyState::Pending};
}

void NPrivate::TWorkloadManagerGateway::SubscribeOnReady(const TString& databaseId,
                                                          NActors::TActorId subscriber, ui64 cookie) {
    if (!StateActorId_) {
        NActors::TActivationContext::Send(new NActors::IEventHandle(
            subscriber, {},
            new TEvWorkloadManagerReady(cookie, Ydb::StatusIds::UNAVAILABLE,
                                         "Workload manager gateway not initialized")));
        return;
    }
    NActors::TActivationContext::Send(new NActors::IEventHandle(
        StateActorId_, subscriber,
        new TEvSubscribeOnWorkloadManagerReady(databaseId, subscriber, cookie)));
}

void NPrivate::TWorkloadManagerGateway::Warmup(const TString& databasePath) {
    if (!StateActorId_ || !databasePath) {
        return;
    }
    NActors::TActivationContext::Send(new NActors::IEventHandle(
        StateActorId_, {}, new TEvWarmupDatabaseInfo(CanonizePath(databasePath))));
}

namespace {

using namespace NActors;

///
/// Actor which owns the read-side workload manager state:
/// - pools,
/// - classifiers,
/// - configs,
/// - database info (via its own fetcher + scheme-cache watches).
///
/// Publishes an immutable snapshot to the shared `TWorkloadManagerGateway`,
/// serves EnsureReady / SubscribeOnReady / Warmup requests routed through
/// the gateway.
///
class TWorkloadManagerStateActor : public TActorBootstrapped<TWorkloadManagerStateActor> {
    struct TPoolInfo {
        NResourcePool::TPoolSettings Config;
        std::optional<NACLib::TSecurityObject> SecurityObject;
        bool Expired = false;
    };

    struct TDatabaseEntry {
        bool Serverless = false;
        Ydb::StatusIds::StatusCode FetchStatus = Ydb::StatusIds::SUCCESS;
        TString FetchMessage;
        TPathId PathId;
        ui32 WatchKey = 0;
    };

    struct TPendingSubscriber {
        TActorId Actor;
        ui64 Cookie = 0;
    };

public:
    explicit TWorkloadManagerStateActor(std::shared_ptr<NPrivate::TWorkloadManagerGateway> gateway)
        : Gateway_(std::move(gateway))
    {}

    void Registered(TActorSystem* sys, const TActorId& owner) override {
        TActorBootstrapped::Registered(sys, owner);
        Gateway_->OnRegistered(SelfId());
    }

    void Bootstrap() {
        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
             new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({
                 (ui32)NKikimrConsole::TConfigItem::FeatureFlagsItem,
                 (ui32)NKikimrConsole::TConfigItem::WorkloadManagerConfigItem
             }), IEventHandle::FlagTrackDelivery);

        FeatureFlags_ = AppData()->FeatureFlags;
        WorkloadManagerConfig_ = AppData()->WorkloadManagerConfig;
        RecomputeFlags();

        if (!NMetadata::NProvider::TServiceOperator::IsEnabled()) {
            ClassifierMetadataInitialized_ = true;
        } else if (EnableResourcePools_) {
            Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()),
                 new NMetadata::NProvider::TEvAskSnapshot(
                     std::make_shared<TResourcePoolClassifierSnapshotsFetcher>()));
        }

        Rebuild();

        Become(&TWorkloadManagerStateActor::StateFunc);
    }

    void PassAway() override {
        UnsubscribeFromResourcePoolClassifiers();
        for (const auto& [_, entry] : DatabasesCache_) {
            if (entry.WatchKey) {
                Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove(entry.WatchKey));
            }
        }
        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
             new NConsole::TEvConfigsDispatcher::TEvRemoveConfigSubscriptionRequest());
        TActorBootstrapped::PassAway();
    }

private:
    STRICT_STFUNC(StateFunc,
        sFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, HandleSetConfigSubscriptionResponse);
        hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);
        hFunc(TEvUpdatePoolInfo, Handle);
        hFunc(TEvEnsurePoolSubscribed, Handle);
        hFunc(NMetadata::NProvider::TEvRefreshSubscriberData, Handle);
        hFunc(TEvWarmupDatabaseInfo, Handle);
        hFunc(TEvSubscribeOnWorkloadManagerReady, Handle);
        hFunc(TEvFetchDatabaseResponse, Handle);
        hFunc(TEvTxProxySchemeCache::TEvWatchNotifyDeleted, Handle);
        IgnoreFunc(TEvTxProxySchemeCache::TEvWatchNotifyUpdated);
        IgnoreFunc(TEvTxProxySchemeCache::TEvWatchNotifyUnavailable);
        sFunc(TEvents::TEvPoison, PassAway);
        hFunc(TEvents::TEvUndelivered, Handle);
    )

    void HandleSetConfigSubscriptionResponse() const {
        LOG_D("State actor subscribed for config changes");
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        switch (ev->Get()->SourceType) {
            case NConsole::TEvConfigsDispatcher::EvSetConfigSubscriptionRequest:
                LOG_C("Failed to deliver config subscription request to configs dispatcher; "
                      "workload manager state actor will run with stale flags");
                break;
            default:
                LOG_W("Undelivered event, SourceType: " << ev->Get()->SourceType);
                break;
        }
    }

    void Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
        const auto& event = ev->Get()->Record;
        FeatureFlags_ = event.GetConfig().GetFeatureFlags();
        WorkloadManagerConfig_ = event.GetConfig().GetWorkloadManagerConfig();
        RecomputeFlags();
        Rebuild();

        auto responseEvent = std::make_unique<NConsole::TEvConsole::TEvConfigNotificationResponse>(event);
        Send(ev->Sender, responseEvent.release(), IEventHandle::FlagTrackDelivery, ev->Cookie);
    }

    void Handle(TEvUpdatePoolInfo::TPtr& ev) {
        if (UpdatePoolInfo(ev->Get()->DatabaseId, ev->Get()->PoolId, ev->Get()->Config, ev->Get()->SecurityObject)) {
            InFlightPoolFetches_.erase(GetPoolKey(ev->Get()->DatabaseId, ev->Get()->PoolId));
        }
        Rebuild();
    }

    // Returns true iff the caller may release the in-flight fetch lock for this pool.
    bool UpdatePoolInfo(const TString& databaseId, const TString& poolId,
                        const std::optional<NResourcePool::TPoolSettings>& config,
                        const std::optional<NACLib::TSecurityObject>& securityObject)
    {
        const TString& poolKey = GetPoolKey(databaseId, poolId);
        if (!config) {
            auto it = PoolsCache_.find(poolKey);
            if (it == PoolsCache_.end()) {
                // Our own fetch returned "not found" — release the in-flight lock for future retries.
                return true;
            }
            if (it->second.Expired) {
                // Second nullopt confirms the pool is gone
                PoolsCache_.erase(it);
                return true;
            }
            it->second.Expired = true;
            if (!InFlightPoolFetches_.insert(poolKey).second) {
                // An op is already in flight (armed elsewhere); leave that lock alone.
                return false;
            }

            // Arm a new op to verify the deletion — hold the lock until the response arrives.
            Send(MakeServiceId(SelfId().NodeId()), new TEvGetPoolInfo(databaseId, poolId));
            return false;
        }

        auto& poolInfo = PoolsCache_[poolKey];
        poolInfo.Config = *config;
        poolInfo.SecurityObject = securityObject;
        poolInfo.Expired = false;
        return true;
    }

    void Handle(TEvEnsurePoolSubscribed::TPtr& ev) {
        EnsurePoolSubscribed(ev->Get()->DatabaseId, ev->Get()->PoolId);
    }

    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
        LastClassifierSnapshot_ = ev->Get()->GetValidatedSnapshotAs<TResourcePoolClassifierSnapshot>();
        ClassifierMetadataInitialized_ = true;
        PreSubscribeOnClassifierPools();
        Rebuild();

        std::vector<TString> matchedDbIds;
        matchedDbIds.reserve(PendingSubscribers_.size());
        for (const auto& [dbId, _] : PendingSubscribers_) {
            if (DatabasesCache_.contains(dbId)) {
                matchedDbIds.push_back(dbId);
            }
        }
        for (const TString& dbId : matchedDbIds) {
            auto it = PendingSubscribers_.find(dbId);
            for (const auto& sub : it->second) {
                Send(sub.Actor, new TEvWorkloadManagerReady(sub.Cookie, Ydb::StatusIds::SUCCESS));
            }
            PendingSubscribers_.erase(it);
        }
    }

    void Handle(TEvWarmupDatabaseInfo::TPtr& ev) {
        const TString& path = ev->Get()->DatabasePath;
        if (!path) {
            return;
        }
        if (const auto pathIt = PathToId_.find(path); pathIt != PathToId_.end()) {
            if (DatabasesCache_.contains(pathIt->second)) {
                return;
            }
        }
        if (InFlightFetchesByPath_.contains(path)) {
            return;
        }
        LOG_D("Warmup: fetching database info for " << path);
        InFlightFetchesByPath_.emplace(path);
        Register(CreateDatabaseFetcherActor(SelfId(), path));
    }

    void Handle(TEvSubscribeOnWorkloadManagerReady::TPtr& ev) {
        const TString& databaseId = ev->Get()->DatabaseId;
        const TActorId subscriber = ev->Get()->Subscriber;
        const ui64 cookie = ev->Get()->Cookie;

        if (const auto it = DatabasesCache_.find(databaseId); it != DatabasesCache_.end()) {
            const auto& entry = it->second;

            if (entry.FetchStatus == Ydb::StatusIds::UNSUPPORTED) {
                Send(subscriber, new TEvWorkloadManagerReady(cookie, Ydb::StatusIds::SUCCESS));
                return;
            }

            if (entry.FetchStatus != Ydb::StatusIds::SUCCESS) {
                Send(subscriber, new TEvWorkloadManagerReady(cookie, entry.FetchStatus, entry.FetchMessage));
                return;
            }

            if (ClassifierMetadataInitialized_) {
                Send(subscriber, new TEvWorkloadManagerReady(cookie, Ydb::StatusIds::SUCCESS));
                return;
            }
        }

        PendingSubscribers_[databaseId].push_back(TPendingSubscriber{subscriber, cookie});

        const TString path = DatabaseIdToDatabase(databaseId);
        const bool pathRepresented = PathToId_.contains(path) && DatabasesCache_.contains(PathToId_.at(path));
        if (!pathRepresented && !InFlightFetchesByPath_.contains(path)) {
            LOG_D("SubscribeOnReady: fetching database info for " << path << " (db " << databaseId << ")");
            InFlightFetchesByPath_.emplace(path);
            Register(CreateDatabaseFetcherActor(SelfId(), path));
        }
    }

    void Handle(TEvFetchDatabaseResponse::TPtr& ev) {
        const auto* msg = ev->Get();
        const TString& path = msg->Database;

        InFlightFetchesByPath_.erase(path);

        // Drop any stale entry keyed by raw path — a prior failed fetch might have written one
        // before we knew the DB is serverless (differs only when composite prefix is present).
        if (msg->Database != msg->DatabaseId) {
            DatabasesCache_.erase(msg->Database);
        }

        if (msg->Status != Ydb::StatusIds::SUCCESS) {
            const TString message = msg->Issues.ToOneLineString();
            LOG_W("Failed to fetch database info, path: " << path << ", status: " << msg->Status << ", issues: " << message);
            auto& entry = DatabasesCache_[msg->DatabaseId];
            entry.Serverless = false;
            entry.FetchStatus = msg->Status;
            entry.FetchMessage = message;
            entry.PathId = {};
            const bool unsupported = msg->Status == Ydb::StatusIds::UNSUPPORTED;
            NotifyPendingSubscribersForPath(
                path,
                unsupported ? Ydb::StatusIds::SUCCESS : msg->Status,
                unsupported ? TString{} : message
            );
            Rebuild();
            return;
        }

        LOG_D("Fetched database info: " << path
            << ", DatabaseId: " << msg->DatabaseId
            << ", Serverless: " << msg->Serverless
            << ", PathId: " << msg->PathId);

        auto& entry = DatabasesCache_[msg->DatabaseId];
        entry.Serverless = msg->Serverless;
        entry.FetchStatus = Ydb::StatusIds::SUCCESS;
        entry.FetchMessage.clear();
        entry.PathId = msg->PathId;
        if (!entry.WatchKey) {
            entry.WatchKey = ++FreeWatchKey_;
            WatchKeyToDbId_[entry.WatchKey] = msg->DatabaseId;
            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchPathId(msg->PathId, entry.WatchKey));
        }
        PathToId_[path] = msg->DatabaseId;

        Rebuild();

        if (ClassifierMetadataInitialized_) {
            NotifyPendingSubscribersForPath(path, Ydb::StatusIds::SUCCESS, {});
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyDeleted::TPtr& ev) {
        const ui64 watchKey = ev->Get()->Key;
        const TString& path = ev->Get()->Path;

        auto keyIt = WatchKeyToDbId_.find(watchKey);
        if (keyIt == WatchKeyToDbId_.end()) {
            return;
        }
        const TString dbId = keyIt->second;
        WatchKeyToDbId_.erase(keyIt);

        LOG_I("Database deleted: " << path << ", DatabaseId: " << dbId);

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove(watchKey));
        DatabasesCache_.erase(dbId);
        PathToId_.erase(path);

        if (auto subsIt = PendingSubscribers_.find(dbId); subsIt != PendingSubscribers_.end()) {
            for (const auto& sub : subsIt->second) {
                Send(sub.Actor, new TEvWorkloadManagerReady(sub.Cookie, Ydb::StatusIds::NOT_FOUND, "Database was deleted"));
            }
            PendingSubscribers_.erase(subsIt);
        }

        Rebuild();
    }

private:
    void NotifyPendingSubscribersForPath(const TString& path, Ydb::StatusIds::StatusCode status, const TString& message) {
        std::vector<TString> matchedDbIds;
        matchedDbIds.reserve(PendingSubscribers_.size());
        for (const auto& [dbId, _] : PendingSubscribers_) {
            if (DatabaseIdToDatabase(dbId) == path) {
                matchedDbIds.push_back(dbId);
            }
        }
        for (const TString& dbId : matchedDbIds) {
            auto it = PendingSubscribers_.find(dbId);
            for (const auto& sub : it->second) {
                Send(sub.Actor, new TEvWorkloadManagerReady(sub.Cookie, status, message));
            }
            PendingSubscribers_.erase(it);
        }
    }

    void EnsurePoolSubscribed(const TString& databaseId, const TString& poolId) {
        const TString& poolKey = GetPoolKey(databaseId, poolId);
        if (auto it = PoolsCache_.find(poolKey); it != PoolsCache_.end() && !it->second.Expired) {
            return;
        }
        if (!InFlightPoolFetches_.insert(poolKey).second) {
            return;
        }
        Send(NKqp::MakeKqpSchedulerServiceId(SelfId().NodeId()),
             new NKqp::NScheduler::TEvAddPool(databaseId, poolId));
        Send(MakeServiceId(SelfId().NodeId()),
             new TEvGetPoolInfo(databaseId, poolId));
    }

    void PreSubscribeOnClassifierPools() {
        if (!LastClassifierSnapshot_) {
            return;
        }
        for (const auto& [databaseId, info] : LastClassifierSnapshot_->GetResourcePoolClassifierConfigs()) {
            for (const auto& [_, classifier] : info.ByName) {
                if (const auto& poolId = classifier.GetClassifierSettings().ResourcePool) {
                    EnsurePoolSubscribed(databaseId, *poolId);
                }
            }
        }
    }

    void RecomputeFlags() {
        const bool wasEnabled = EnableResourcePools_;
        EnableResourcePools_ = FeatureFlags_.GetEnableResourcePools() || WorkloadManagerConfig_.GetEnabled();
        EnableResourcePoolsOnServerless_ = FeatureFlags_.GetEnableResourcePoolsOnServerless() || WorkloadManagerConfig_.GetEnabled();

        if (EnableResourcePools_) {
            SubscribeOnResourcePoolClassifiers();
        } else {
            UnsubscribeFromResourcePoolClassifiers();
        }
        
        if (wasEnabled && !EnableResourcePools_) {
            ReleasePendingSubscribers();
        }
    }

    void ReleasePendingSubscribers() {
        for (auto& [_, subs] : PendingSubscribers_) {
            for (const auto& sub : subs) {
                Send(sub.Actor, new TEvWorkloadManagerReady(sub.Cookie, Ydb::StatusIds::SUCCESS));
            }
        }
        PendingSubscribers_.clear();
    }

    void SubscribeOnResourcePoolClassifiers() {
        if (!SubscribedOnResourcePoolClassifiers_ && NMetadata::NProvider::TServiceOperator::IsEnabled()) {
            SubscribedOnResourcePoolClassifiers_ = true;
            Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()),
                 new NMetadata::NProvider::TEvSubscribeExternal(std::make_shared<TResourcePoolClassifierSnapshotsFetcher>()));
        }
    }

    void UnsubscribeFromResourcePoolClassifiers() {
        if (SubscribedOnResourcePoolClassifiers_) {
            SubscribedOnResourcePoolClassifiers_ = false;
            Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()),
                 new NMetadata::NProvider::TEvUnsubscribeExternal(std::make_shared<TResourcePoolClassifierSnapshotsFetcher>()));
        }
    }

    TResourcePoolMapPtr BuildResourcePoolMapSnapshot() const {
        auto pools = std::make_shared<TResourcePoolMap>();
        pools->reserve(PoolsCache_.size());
        for (const auto& [key, info] : PoolsCache_) {
            if (!info.Expired) {
                pools->emplace(key, TResourcePoolEntry{info.Config, info.SecurityObject});
            }
        }
        return pools;
    }

    void Rebuild() {
        auto* snapshot = new NPrivate::TSnapshot();
        snapshot->Pools = BuildResourcePoolMapSnapshot();
        snapshot->Classifiers = LastClassifierSnapshot_;
        for (const auto& [databaseId, entry] : DatabasesCache_) {
            snapshot->Databases[databaseId] = NPrivate::TDatabaseInfo{
                .Serverless = entry.Serverless,
                .FetchStatus = entry.FetchStatus,
                .FetchMessage = entry.FetchMessage,
            };
        }
        snapshot->EnableResourcePools = EnableResourcePools_;
        snapshot->EnableResourcePoolsOnServerless = EnableResourcePoolsOnServerless_;
        snapshot->ClassifierMetadataInitialized = ClassifierMetadataInitialized_;
        Gateway_->PublishSnapshot(NPrivate::TSnapshotPtr(snapshot));
    }

    TString LogPrefix() const {
        return "[WorkloadManagerState] ";
    }

private:
    std::unordered_map<TString, TPoolInfo> PoolsCache_;
    std::unordered_set<TString> InFlightPoolFetches_;
    std::unordered_map<TString, TDatabaseEntry> DatabasesCache_;
    std::unordered_map<TString, TString> PathToId_;
    std::unordered_map<ui32, TString> WatchKeyToDbId_;
    std::unordered_map<TString, std::vector<TPendingSubscriber>> PendingSubscribers_;
    std::unordered_set<TString> InFlightFetchesByPath_;
    std::shared_ptr<const TResourcePoolClassifierSnapshot> LastClassifierSnapshot_;
    NKikimrConfig::TFeatureFlags FeatureFlags_;
    NKikimrConfig::TWorkloadManagerConfig WorkloadManagerConfig_;
    bool EnableResourcePools_ = false;
    bool EnableResourcePoolsOnServerless_ = false;
    bool SubscribedOnResourcePoolClassifiers_ = false;
    bool ClassifierMetadataInitialized_ = false;
    ui32 FreeWatchKey_ = 0;

    std::shared_ptr<NPrivate::TWorkloadManagerGateway> Gateway_;
};

}

NActors::IActor* CreateWorkloadManagerStateActor(
    std::shared_ptr<NPrivate::TWorkloadManagerGateway> gateway)
{
    return new TWorkloadManagerStateActor(std::move(gateway));
}

}
