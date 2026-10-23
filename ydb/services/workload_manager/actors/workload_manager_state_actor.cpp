#include "workload_manager_state_actor.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_compute_scheduler_service.h>
#include <ydb/core/protos/console_config.pb.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/core/protos/workload_manager_config.pb.h>
#include <ydb/core/resource_pools/resource_pool_settings.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/services/metadata/service.h>
#include <ydb/services/workload_manager/actors/resource_pool_tracker.h>
#include <ydb/services/workload_manager/common/helpers.h>
#include <ydb/services/workload_manager/events.h>
#include <ydb/services/workload_manager/gateway_internal.h>
#include <ydb/services/workload_manager/metadata_subscription/resource_pool_classifier/fetcher.h>
#include <ydb/services/workload_manager/service/service.h>

namespace NKikimr::NWorkloadManager {

namespace {

bool IsResourcePoolsEnabled(
    const NKikimrConfig::TFeatureFlags& featureFlags, const NKikimrConfig::TWorkloadManagerConfig& workloadManagerConfig) {
    return featureFlags.GetEnableResourcePools() || workloadManagerConfig.GetEnabled();
}

bool IsResourcePoolsOnServerlessEnabled(
    const NKikimrConfig::TFeatureFlags& featureFlags, const NKikimrConfig::TWorkloadManagerConfig& workloadManagerConfig) {
    return featureFlags.GetEnableResourcePoolsOnServerless() || workloadManagerConfig.GetEnabled();
}

}   // namespace

bool NPrivate::TSnapshot::IsResourcePoolsEnabled(const TString& databaseId) const {
    return EnableResourcePools && !databaseId.empty() && (EnableResourcePoolsOnServerless || !IsServerlessDatabaseId(databaseId));
}

std::shared_ptr<IQueryClassifier> NPrivate::TWorkloadManagerGateway::TryCreateQueryClassifier(
    const TString& databaseId, TClassifyContext context)
{
    TSnapshotPtr snapshot = Snapshot_;

    if (!snapshot || !snapshot->IsResourcePoolsEnabled(databaseId)) {
        return nullptr;
    }

    const TString effectivePoolId = context.PoolId ? context.PoolId : NResourcePool::DEFAULT_POOL_ID;

    if (!snapshot->Pools || !snapshot->Pools->contains(GetPoolKey(databaseId, effectivePoolId))) {
        NActors::TActivationContext::Send(
            new NActors::IEventHandle(snapshot->StateActorId, {}, new TEvEnsurePoolSubscribed(databaseId, effectivePoolId)));
    }

    return CreateQueryClassifier(
        snapshot->Pools, TClassifierConfigsView(snapshot->Classifiers, databaseId), databaseId, std::move(context), *AppData());
}

namespace {

using namespace NActors;

///
/// Actor which owns the read-side workload manager state:
/// - pools,
/// - classifiers,
/// - configs.
///
/// Publishes an immutable snapshot to the shared `TWorkloadManagerGateway`.
///
class TWorkloadManagerStateActor: public TActorBootstrapped<TWorkloadManagerStateActor> {
public:
    explicit TWorkloadManagerStateActor(std::shared_ptr<NPrivate::TWorkloadManagerGateway> gateway)
        : Gateway_(std::move(gateway))
    {
    }

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
            new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({ (ui32)NKikimrConsole::TConfigItem::FeatureFlagsItem,
                (ui32)NKikimrConsole::TConfigItem::WorkloadManagerConfigItem }), IEventHandle::FlagTrackDelivery);

        ApplyConfig(AppData()->FeatureFlags, AppData()->WorkloadManagerConfig);

        Become(&TWorkloadManagerStateActor::StateFunc);
    }

    void PassAway() override {
        Gateway_->OnUnregistered();
        UnsubscribeFromMetadataForClassifiers();

        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()), new NConsole::TEvConfigsDispatcher::TEvRemoveConfigSubscriptionRequest());

        TActorBootstrapped::PassAway();
    }

private:
    STRICT_STFUNC(StateFunc,
        sFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, HandleSetConfigSubscriptionResponse);
        hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);
        hFunc(TEvUpdatePoolInfo, Handle);
        hFunc(TEvEnsurePoolSubscribed, Handle);
        hFunc(NMetadata::NProvider::TEvRefreshSubscriberData, Handle);
        sFunc(TEvents::TEvPoison, PassAway);
        hFunc(TEvents::TEvUndelivered, Handle);
    )

    void HandleSetConfigSubscriptionResponse() const {
        LOG_D("State actor subscribed for config changes");
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        switch (ev->Get()->SourceType) {
            case NConsole::TEvConfigsDispatcher::EvSetConfigSubscriptionRequest:
                LOG_C(
                    "Failed to deliver config subscription request to configs dispatcher; "
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
        if (!SubscribedOnMetadataForClassifiers_) {
            return;
        }

        LastClassifierSnapshot_ = ev->Get()->GetValidatedSnapshotAs<TResourcePoolClassifierSnapshot>();

        for (const auto& [databaseId, poolId] : PoolTracker_.TrySubscribeClassifierPools(*LastClassifierSnapshot_)) {
            SubscribeOnPool(databaseId, poolId);
        }

        Rebuild();
    }

    void SubscribeOnPool(const TString& databaseId, const TString& poolId) {
        Send(NKqp::MakeKqpSchedulerServiceId(SelfId().NodeId()), new NKqp::NScheduler::TEvAddPool(databaseId, poolId));
        Send(MakeServiceId(SelfId().NodeId()), new TEvSubscribeOnPoolChanges(databaseId, poolId));
    }

    void ApplyConfig(const NKikimrConfig::TFeatureFlags& featureFlags, const NKikimrConfig::TWorkloadManagerConfig& workloadManagerConfig) {
        EnableResourcePools_ = IsResourcePoolsEnabled(featureFlags, workloadManagerConfig);
        EnableResourcePoolsOnServerless_ = IsResourcePoolsOnServerlessEnabled(featureFlags, workloadManagerConfig);

        if (EnableResourcePools_) {
            SubscribeOnMetadataForClassifiers();
        } else {
            UnsubscribeFromMetadataForClassifiers();
        }

        Rebuild();
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
            LastClassifierSnapshot_ = nullptr;
            Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()),
                new NMetadata::NProvider::TEvUnsubscribeExternal(std::make_shared<TResourcePoolClassifierSnapshotsFetcher>()));
        }
    }

    void Rebuild() {
        auto* snapshot = new NPrivate::TSnapshot();
        snapshot->Pools = PoolTracker_.BuildSnapshot();
        snapshot->Classifiers = LastClassifierSnapshot_;
        snapshot->StateActorId = SelfId();
        snapshot->EnableResourcePools = EnableResourcePools_;
        snapshot->EnableResourcePoolsOnServerless = EnableResourcePoolsOnServerless_;
        Gateway_->PublishSnapshot(NPrivate::TSnapshotPtr(snapshot));
    }

    TString LogPrefix() const {
        return "[WorkloadManagerState] ";
    }

private:
    std::shared_ptr<NPrivate::TWorkloadManagerGateway> Gateway_;
    NPrivate::TResourcePoolTracker PoolTracker_;

    std::shared_ptr<const TResourcePoolClassifierSnapshot> LastClassifierSnapshot_;

    bool EnableResourcePools_ = false;
    bool EnableResourcePoolsOnServerless_ = false;
    bool SubscribedOnMetadataForClassifiers_ = false;
};

}   // namespace

NActors::TActorId MakeWorkloadManagerStateActorId(ui32 nodeId) {
    const char name[12] = "kqp_wm_stat";
    return NActors::TActorId(nodeId, TStringBuf(name, 12));
}

NActors::IActor* CreateWorkloadManagerStateActor(std::shared_ptr<NPrivate::TWorkloadManagerGateway> gateway)
{
    return new TWorkloadManagerStateActor(std::move(gateway));
}

}   // namespace NKikimr::NWorkloadManager
