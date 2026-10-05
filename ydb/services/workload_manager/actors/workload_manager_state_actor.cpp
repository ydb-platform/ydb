#include "workload_manager_state_actor.h"

#include <ydb/services/workload_manager/gateway_internal.h>
#include <ydb/services/workload_manager/service/service.h>

#include <ydb/services/workload_manager/actors/actors.h>
#include <ydb/services/workload_manager/actors/classifier_metadata_tracker.h>
#include <ydb/services/workload_manager/actors/database_readiness_tracker.h>
#include <ydb/services/workload_manager/actors/resource_pool_tracker.h>
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

namespace {

constexpr TDuration IN_FLIGHT_REQUEST_TIMEOUT = TDuration::Seconds(5);
constexpr TDuration IN_FLIGHT_REQUESTS_CHECK_PERIOD = TDuration::Seconds(1);

bool IsResourcePoolsEnabled(const NKikimrConfig::TFeatureFlags& featureFlags, const NKikimrConfig::TWorkloadManagerConfig& workloadManagerConfig) {
    return featureFlags.GetEnableResourcePools() || workloadManagerConfig.GetEnabled();
}

bool IsResourcePoolsOnServerlessEnabled(const NKikimrConfig::TFeatureFlags& featureFlags, const NKikimrConfig::TWorkloadManagerConfig& workloadManagerConfig) {
    return featureFlags.GetEnableResourcePoolsOnServerless() || workloadManagerConfig.GetEnabled();
}

}

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
            snapshot->StateActorId, {},
            new TEvEnsurePoolSubscribed(databaseId, effectivePoolId)));
    }

    return CreateQueryClassifier(
        snapshot->Pools,
        TClassifierConfigsView(snapshot->Classifiers, databaseId),
        databaseId,
        std::move(context),
        *AppData());
}

void NPrivate::TWorkloadManagerGateway::DoWarmupRequest(const NActors::TActorId& stateActorId, const TString& databaseId) {
    NActors::TActivationContext::Send(new NActors::IEventHandle(
        stateActorId, {}, new TEvWarmupDatabaseInfo(DatabaseIdToDatabase(databaseId))));
}

TReadyInfo NPrivate::TWorkloadManagerGateway::EnsureReady(const TString& databaseId) {
    TSnapshotPtr snapshot = Snapshot_;

    if (!snapshot) {
        return TReadyInfo{.State = EReadyState::Disabled};
    }

    if (!snapshot->EnableResourcePools) {
        return TReadyInfo{.State = EReadyState::Disabled};
    }

    const auto it = snapshot->Databases.find(databaseId);
    if (it == snapshot->Databases.end()) {
        return TReadyInfo{.State = EReadyState::Pending};
    }

    const auto& info = it->second;
    switch (info.State) {
        case EDatabaseState::Pending:
            return TReadyInfo{.State = EReadyState::Pending};

        case EDatabaseState::Unsupported:
            return TReadyInfo{.State = EReadyState::Disabled};

        case EDatabaseState::Failed:
            DoWarmupRequest(snapshot->StateActorId, databaseId);
            return TReadyInfo{.State = EReadyState::Failed, .FailureStatus = info.FailureStatus, .FailureMessage = info.FailureMessage};

        case EDatabaseState::TimedOut:
            DoWarmupRequest(snapshot->StateActorId, databaseId);
            return TReadyInfo{.State = EReadyState::Failed, .FailureStatus = Ydb::StatusIds::UNAVAILABLE, .FailureMessage = TString(WORKLOAD_MANAGER_NOT_READY_MESSAGE)};

        case EDatabaseState::Ready:
            break;
    }

    if (!snapshot->EnableResourcePoolsOnServerless && info.Serverless) {
        return TReadyInfo{.State = EReadyState::Disabled};
    }

    switch (snapshot->Metadata) {
        case EMetadataState::Ready:
            return TReadyInfo{.State = EReadyState::Ready};

        case EMetadataState::Pending:
            return TReadyInfo{.State = EReadyState::Pending};

        case EMetadataState::TimedOut:
            DoWarmupRequest(snapshot->StateActorId, databaseId);
            return TReadyInfo{.State = EReadyState::Failed, .FailureStatus = Ydb::StatusIds::UNAVAILABLE, .FailureMessage = TString(WORKLOAD_MANAGER_NOT_READY_MESSAGE)};
    }

    return TReadyInfo{.State = EReadyState::Pending};
}

void NPrivate::TWorkloadManagerGateway::SubscribeOnReady(const TString& databaseId,
                                                          NActors::TActorId subscriber, ui64 cookie) {
    TSnapshotPtr snapshot = Snapshot_;
    if (!snapshot) {
        NActors::TActivationContext::Send(new NActors::IEventHandle(
            subscriber, {},
            new TEvWorkloadManagerReady(cookie, Ydb::StatusIds::UNAVAILABLE,
                                         "Workload manager gateway not initialized")));
        return;
    }
    NActors::TActivationContext::Send(new NActors::IEventHandle(
        snapshot->StateActorId, subscriber,
        new TEvSubscribeOnWorkloadManagerReady(databaseId, subscriber, cookie)));
}

void NPrivate::TWorkloadManagerGateway::Warmup(const TString& databasePath) {
    TSnapshotPtr snapshot = Snapshot_;
    if (!snapshot || !databasePath) {
        return;
    }
    const TString path = CanonizePath(databasePath);
    if (!snapshot->NeedsWarmup(path)) {
        return;
    }
    NActors::TActivationContext::Send(new NActors::IEventHandle(
        snapshot->StateActorId, {}, new TEvWarmupDatabaseInfo(path)));
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
public:
    explicit TWorkloadManagerStateActor(std::shared_ptr<NPrivate::TWorkloadManagerGateway> gateway)
        : Gateway_(std::move(gateway))
        , MetadataTracker_(IN_FLIGHT_REQUEST_TIMEOUT)
        , DatabaseTracker_(IN_FLIGHT_REQUEST_TIMEOUT)
    {}

    void Registered(TActorSystem* sys, const TActorId& owner) override {
        TActorBootstrapped::Registered(sys, owner);
        const auto* appData = sys->AppData<TAppData>();
        auto* snapshot = new NPrivate::TSnapshot();
        snapshot->StateActorId = SelfId();
        snapshot->EnableResourcePools = IsResourcePoolsEnabled(appData->FeatureFlags, appData->WorkloadManagerConfig);
        snapshot->EnableResourcePoolsOnServerless = IsResourcePoolsOnServerlessEnabled(appData->FeatureFlags, appData->WorkloadManagerConfig);
        Gateway_->PublishSnapshot(NPrivate::TSnapshotPtr(snapshot));
    }

    void Bootstrap() {
        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
             new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({
                 (ui32)NKikimrConsole::TConfigItem::FeatureFlagsItem,
                 (ui32)NKikimrConsole::TConfigItem::WorkloadManagerConfigItem
             }), IEventHandle::FlagTrackDelivery);

        MetadataTracker_.Init(NMetadata::NProvider::TServiceOperator::IsEnabled(), TActivationContext::Now());
        ApplyConfig(AppData()->FeatureFlags, AppData()->WorkloadManagerConfig);

        Become(&TWorkloadManagerStateActor::StateFunc);
    }

    void PassAway() override {
        Gateway_->OnUnregistered();
        ReplyToAll(DatabaseTracker_.TakeAllSubscribers());
        UnsubscribeFromMetadataForClassifiers();

        for (const auto& [_, watchKey] : WatchKeys_) {
            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove(watchKey));
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
        sFunc(TEvents::TEvWakeup, HandleInFlightRequestsCheck);
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
        ApplyConfig(event.GetConfig().GetFeatureFlags(), event.GetConfig().GetWorkloadManagerConfig());

        auto responseEvent = std::make_unique<NConsole::TEvConsole::TEvConfigNotificationResponse>(event);
        Send(ev->Sender, responseEvent.release(), IEventHandle::FlagTrackDelivery, ev->Cookie);
    }

    void Handle(TEvUpdatePoolInfo::TPtr& ev) {
        const auto* msg = ev->Get();

        if (PoolTracker_.OnPoolInfo(msg->DatabaseId, msg->PoolId, msg->Config, msg->SecurityObject)) {
            Send(MakeServiceId(SelfId().NodeId()), new TEvSubscribeOnPoolChanges(msg->DatabaseId, msg->PoolId));
        }
        
        Rebuild();
    }

    void Handle(TEvEnsurePoolSubscribed::TPtr& ev) {
        if (PoolTracker_.TrySubscribe(ev->Get()->DatabaseId, ev->Get()->PoolId)) {
            SubscribeOnPool(ev->Get()->DatabaseId, ev->Get()->PoolId);
        }
    }

    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
        LastClassifierSnapshot_ = ev->Get()->GetValidatedSnapshotAs<TResourcePoolClassifierSnapshot>();
        MetadataTracker_.OnReady(TActivationContext::Now());

        if (LastClassifierSnapshot_) {
            for (const auto& [databaseId, poolId] : PoolTracker_.TrySubscribeClassifierPools(*LastClassifierSnapshot_)) {
                SubscribeOnPool(databaseId, poolId);
            }
        }
        
        PublishAndReply();
    }

    void Handle(TEvWarmupDatabaseInfo::TPtr& ev) {
        const TString& path = ev->Get()->DatabasePath;
        if (!path || !EnableResourcePools_) {
            return;
        }

        const TInstant now = TActivationContext::Now();
        if (MetadataTracker_.NeedsRequery(now)) {
            LOG_I("Requery classifier metadata after timeout");
            AskMetadataForClassifiers();
        }

        if (const auto fetchPath = DatabaseTracker_.OnWarmup(path, now)) {
            StartFetchDatabaseInfo(*fetchPath);
        }
    }

    void Handle(TEvSubscribeOnWorkloadManagerReady::TPtr& ev) {
        const auto* msg = ev->Get();
        if (!EnableResourcePools_) {
            Send(msg->Subscriber, new TEvWorkloadManagerReady(msg->Cookie, Ydb::StatusIds::SUCCESS));
            return;
        }

        const auto fetchPath = DatabaseTracker_.AddSubscriber(
            msg->DatabaseId, NPrivate::TPendingSubscriber{msg->Subscriber, msg->Cookie}, TActivationContext::Now());
        
            if (fetchPath) {
            StartFetchDatabaseInfo(*fetchPath);
        }

        ReplySettled();
        ScheduleInFlightRequestsCheck();
    }

    void Handle(TEvFetchDatabaseResponse::TPtr& ev) {
        const auto* msg = ev->Get();
        const TString message = msg->Issues.ToOneLineString();

        if (msg->Status == Ydb::StatusIds::SUCCESS) {
            LOG_D("Fetched database info: " << msg->Database
                << ", DatabaseId: " << msg->DatabaseId
                << ", Serverless: " << msg->Serverless
                << ", PathId: " << msg->PathId);
            WatchDatabase(msg->DatabaseId, msg->PathId);
        } else {
            LOG_W("Failed to fetch database info, path: " << msg->Database << ", status: " << msg->Status << ", issues: " << message);
        }

        DatabaseTracker_.OnFetchResult(msg->Database, msg->DatabaseId, msg->Status, message, msg->Serverless, TActivationContext::Now());
        PublishAndReply();
    }

    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyDeleted::TPtr& ev) {
        const ui32 watchKey = ev->Get()->Key;
        const TString& path = ev->Get()->Path;

        const auto keyIt = WatchKeyToDbId_.find(watchKey);
        if (keyIt == WatchKeyToDbId_.end()) {
            return;
        }
        const TString databaseId = keyIt->second;
        WatchKeyToDbId_.erase(keyIt);
        WatchKeys_.erase(databaseId);
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove(watchKey));

        LOG_I("Database deleted: " << path << ", DatabaseId: " << databaseId);

        const auto subscribers = DatabaseTracker_.OnDatabaseDeleted(databaseId, path);
        Rebuild();
        ReplyToAll(subscribers, Ydb::StatusIds::NOT_FOUND, "Database was deleted");
    }

    void HandleInFlightRequestsCheck() {
        InFlightRequestsCheckScheduled_ = false;
        const TInstant now = TActivationContext::Now();

        if (DatabaseTracker_.TimeOutPending(now)) {
            LOG_W("Database info request timed out, waiting queries get retryable UNAVAILABLE");
        }
        if (MetadataTracker_.TimeOutPending(now)) {
            LOG_W("Classifier metadata request timed out, waiting queries get retryable UNAVAILABLE");
        }
        PublishAndReply();

        if (HasInFlightRequests()) {
            ScheduleInFlightRequestsCheck();
        }
    }

private:
    void PublishAndReply() {
        Rebuild();
        ReplySettled();
    }

    void ReplySettled() {
        for (const auto& reply : DatabaseTracker_.TakeSettledSubscribers(MetadataTracker_.GetState())) {
            Send(reply.Actor, new TEvWorkloadManagerReady(reply.Cookie, reply.Status, reply.Message));
        }
    }

    void ReplyToAll(const std::vector<NPrivate::TPendingSubscriber>& subscribers,
                  Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS, const TString& message = {}) {
        for (const auto& sub : subscribers) {
            Send(sub.Actor, new TEvWorkloadManagerReady(sub.Cookie, status, message));
        }
    }

    bool HasInFlightRequests() const {
        return MetadataTracker_.IsInFlight() || DatabaseTracker_.HasPending();
    }

    void ScheduleInFlightRequestsCheck() {
        if (!InFlightRequestsCheckScheduled_ && HasInFlightRequests()) {
            InFlightRequestsCheckScheduled_ = true;
            Schedule(IN_FLIGHT_REQUESTS_CHECK_PERIOD, new TEvents::TEvWakeup());
        }
    }

    void StartFetchDatabaseInfo(const TString& path) {
        LOG_D("Fetching database info for " << path);
        Register(CreateDatabaseFetcherActor(SelfId(), path));
    }

    void WatchDatabase(const TString& databaseId, TPathId pathId) {
        if (WatchKeys_.contains(databaseId)) {
            return;
        }

        const ui32 watchKey = ++FreeWatchKey_;
        WatchKeys_[databaseId] = watchKey;
        WatchKeyToDbId_[watchKey] = databaseId;
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchPathId(pathId, watchKey));
    }

    void AskMetadataForClassifiers() {
        if (NMetadata::NProvider::TServiceOperator::IsEnabled()) {
            Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()),
                 new NMetadata::NProvider::TEvAskSnapshot(
                     std::make_shared<TResourcePoolClassifierSnapshotsFetcher>()));
        }
    }

    void SubscribeOnPool(const TString& databaseId, const TString& poolId) {
        Send(NKqp::MakeKqpSchedulerServiceId(SelfId().NodeId()),
             new NKqp::NScheduler::TEvAddPool(databaseId, poolId));
        Send(MakeServiceId(SelfId().NodeId()),
             new TEvSubscribeOnPoolChanges(databaseId, poolId));
    }

    void ApplyConfig(const NKikimrConfig::TFeatureFlags& featureFlags, const NKikimrConfig::TWorkloadManagerConfig& workloadManagerConfig) {
        EnableResourcePools_ = IsResourcePoolsEnabled(featureFlags, workloadManagerConfig);
        EnableResourcePoolsOnServerless_ = IsResourcePoolsOnServerlessEnabled(featureFlags, workloadManagerConfig);

        if (EnableResourcePools_) {
            SubscribeOnMetadataForClassifiers();
        } else {
            UnsubscribeFromMetadataForClassifiers();
        }

        if (MetadataTracker_.OnPoolsEnabled(EnableResourcePools_, TActivationContext::Now())) {
            ScheduleInFlightRequestsCheck();
        }

        Rebuild();

        if (!EnableResourcePools_) {
            ReplyToAll(DatabaseTracker_.TakeAllSubscribers());
        }
    }

    void SubscribeOnMetadataForClassifiers() {
        if (!SubscribedOnMetadataForClassifiers_ && NMetadata::NProvider::TServiceOperator::IsEnabled()) {
            SubscribedOnMetadataForClassifiers_ = true;
            Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()),
                 new NMetadata::NProvider::TEvSubscribeExternal(std::make_shared<TResourcePoolClassifierSnapshotsFetcher>()));
        }
    }

    void UnsubscribeFromMetadataForClassifiers() {
        if (SubscribedOnMetadataForClassifiers_) {
            SubscribedOnMetadataForClassifiers_ = false;
            Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()),
                 new NMetadata::NProvider::TEvUnsubscribeExternal(std::make_shared<TResourcePoolClassifierSnapshotsFetcher>()));
        }
    }

    void Rebuild() {
        auto* snapshot = new NPrivate::TSnapshot();
        snapshot->Pools = PoolTracker_.BuildSnapshot();
        snapshot->Classifiers = LastClassifierSnapshot_;
        DatabaseTracker_.Fill(*snapshot);
        snapshot->StateActorId = SelfId();
        snapshot->EnableResourcePools = EnableResourcePools_;
        snapshot->EnableResourcePoolsOnServerless = EnableResourcePoolsOnServerless_;
        snapshot->Metadata = MetadataTracker_.GetState();
        Gateway_->PublishSnapshot(NPrivate::TSnapshotPtr(snapshot));
    }

    TString LogPrefix() const {
        return "[WorkloadManagerState] ";
    }

private:
    std::shared_ptr<NPrivate::TWorkloadManagerGateway> Gateway_;
    NPrivate::TClassifierMetadataTracker MetadataTracker_;
    NPrivate::TDatabaseReadinessTracker DatabaseTracker_;
    NPrivate::TResourcePoolTracker PoolTracker_;

    std::unordered_map<TString, ui32> WatchKeys_;
    std::unordered_map<ui32, TString> WatchKeyToDbId_;
    std::shared_ptr<const TResourcePoolClassifierSnapshot> LastClassifierSnapshot_;

    bool EnableResourcePools_ = false;
    bool EnableResourcePoolsOnServerless_ = false;
    bool SubscribedOnMetadataForClassifiers_ = false;
    bool InFlightRequestsCheckScheduled_ = false;
    ui32 FreeWatchKey_ = 0;
};

}

NActors::TActorId MakeWorkloadManagerStateActorId(ui32 nodeId) {
    const char name[12] = "kqp_wm_stat";
    return NActors::TActorId(nodeId, TStringBuf(name, 12));
}

NActors::IActor* CreateWorkloadManagerStateActor(
    std::shared_ptr<NPrivate::TWorkloadManagerGateway> gateway)
{
    return new TWorkloadManagerStateActor(std::move(gateway));
}

}
