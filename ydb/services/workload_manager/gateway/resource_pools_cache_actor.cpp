#include "resource_pools_cache_actor.h"

#include <ydb/services/workload_manager/common/helpers.h>
#include <ydb/services/workload_manager/events.h>
#include <ydb/services/workload_manager/gateway/internal.h>
#include <ydb/services/workload_manager/metadata_subscription/resource_pool_classifier/fetcher.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_compute_scheduler_service.h>
#include <ydb/core/protos/console_config.pb.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/core/protos/workload_manager_config.pb.h>
#include <ydb/core/resource_pools/resource_pool_settings.h>
#include <ydb/services/metadata/service.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <unordered_map>


namespace NKikimr::NWorkloadManager {

namespace {

using namespace NActors;

class TResourcePoolsCacheActor : public TActorBootstrapped<TResourcePoolsCacheActor> {
    struct TPoolInfo {
        NResourcePool::TPoolSettings Config;
        std::optional<NACLib::TSecurityObject> SecurityObject;
        bool Expired = false;
    };

public:
    explicit TResourcePoolsCacheActor(TActorId workloadManagerServiceId)
        : WorkloadManagerServiceId_(workloadManagerServiceId)
        , Gateway_(std::make_shared<NPrivate::TWorkloadManagerGateway>())
    {}

    void Registered(TActorSystem* sys, const TActorId& owner) override {
        TActorBootstrapped::Registered(sys, owner);
        Gateway_->OnRegistered(SelfId(), WorkloadManagerServiceId_, SelfId().NodeId());
        NPrivate::RegisterGateway(Gateway_, SelfId().NodeId());
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
        UpdateResourcePoolClassifiersSubscription();
        Rebuild();

        Become(&TResourcePoolsCacheActor::StateFunc);
    }

    void PassAway() override {
        UnsubscribeFromResourcePoolClassifiers();
        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
             new NConsole::TEvConfigsDispatcher::TEvRemoveConfigSubscriptionRequest());
        TActorBootstrapped::PassAway();
    }

private:
    STRICT_STFUNC(StateFunc,
        sFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, HandleSetConfigSubscriptionResponse);
        hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);
        hFunc(TEvUpdatePoolInfo, Handle);
        hFunc(NKqp::TEvKqp::TEvUpdateDatabaseInfo, Handle);
        hFunc(NMetadata::NProvider::TEvRefreshSubscriberData, Handle);
        sFunc(TEvents::TEvPoison, PassAway);
        IgnoreFunc(TEvents::TEvUndelivered);
    )

    void HandleSetConfigSubscriptionResponse() const {
        LOG_D("Cache actor subscribed for config changes");
    }

    void Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
        const auto& event = ev->Get()->Record;
        FeatureFlags_ = event.GetConfig().GetFeatureFlags();
        WorkloadManagerConfig_ = event.GetConfig().GetWorkloadManagerConfig();
        RecomputeFlags();
        UpdateResourcePoolClassifiersSubscription();
        Rebuild();

        auto responseEvent = std::make_unique<NConsole::TEvConsole::TEvConfigNotificationResponse>(event);
        Send(ev->Sender, responseEvent.release(), IEventHandle::FlagTrackDelivery, ev->Cookie);
    }

    void Handle(TEvUpdatePoolInfo::TPtr& ev) {
        UpdatePoolInfo(ev->Get()->DatabaseId, ev->Get()->PoolId, ev->Get()->Config, ev->Get()->SecurityObject);
        Rebuild();
    }

    void Handle(NKqp::TEvKqp::TEvUpdateDatabaseInfo::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            return;
        }
        DatabasesCache_[ev->Get()->DatabaseId].Serverless = ev->Get()->Serverless;
        Rebuild();
    }

    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
        LastClassifierSnapshot_ = ev->Get()->GetValidatedSnapshotAs<TResourcePoolClassifierSnapshot>();
        PreSubscribeOnClassifierPools();
        Rebuild();
    }

private:
    void UpdatePoolInfo(const TString& databaseId, const TString& poolId,
                        const std::optional<NResourcePool::TPoolSettings>& config,
                        const std::optional<NACLib::TSecurityObject>& securityObject)
    {
        const TString& poolKey = GetPoolKey(databaseId, poolId);
        if (!config) {
            auto it = PoolsCache_.find(poolKey);
            if (it == PoolsCache_.end()) {
                return;
            }
            if (it->second.Expired) {
                PoolsCache_.erase(it);
            } else {
                it->second.Expired = true;
                Send(WorkloadManagerServiceId_, new TEvSubscribeOnPoolChanges(databaseId, poolId));
            }
            return;
        }

        auto& poolInfo = PoolsCache_[poolKey];
        poolInfo.Config = *config;
        poolInfo.SecurityObject = securityObject;
        poolInfo.Expired = false;
    }

    void PreSubscribeOnClassifierPools() {
        if (!LastClassifierSnapshot_) {
            return;
        }
        for (const auto& [databaseId, info] : LastClassifierSnapshot_->GetResourcePoolClassifierConfigs()) {
            for (const auto& [_, classifier] : info.ByName) {
                const auto& poolId = classifier.GetClassifierSettings().ResourcePool;
                if (!poolId) {
                    continue;
                }
                if (PoolsCache_.contains(GetPoolKey(databaseId, *poolId))) {
                    continue;
                }
                Send(NKqp::MakeKqpSchedulerServiceId(SelfId().NodeId()),
                     new NKqp::NScheduler::TEvAddPool(databaseId, *poolId));
                Send(WorkloadManagerServiceId_,
                     new TEvSubscribeOnPoolChanges(databaseId, *poolId));
            }
        }
    }

    void RecomputeFlags() {
        EnableResourcePools_ = FeatureFlags_.GetEnableResourcePools() || WorkloadManagerConfig_.GetEnabled();
        EnableResourcePoolsOnServerless_ = FeatureFlags_.GetEnableResourcePoolsOnServerless() || WorkloadManagerConfig_.GetEnabled();
    }

    void UpdateResourcePoolClassifiersSubscription() {
        if (EnableResourcePools_) {
            SubscribeOnResourcePoolClassifiers();
        } else {
            UnsubscribeFromResourcePoolClassifiers();
        }
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
        auto snapshot = std::make_shared<NPrivate::TSnapshot>();
        snapshot->Pools = BuildResourcePoolMapSnapshot();
        snapshot->Classifiers = LastClassifierSnapshot_;
        snapshot->FeatureFlags = FeatureFlags_;
        snapshot->WorkloadManagerConfig = WorkloadManagerConfig_;
        for (const auto& [databaseId, info] : DatabasesCache_) {
            snapshot->Databases[databaseId] = NPrivate::TDatabaseInfo{.Serverless = info.Serverless};
        }
        snapshot->EnableResourcePools = EnableResourcePools_;
        snapshot->EnableResourcePoolsOnServerless = EnableResourcePoolsOnServerless_;
        Gateway_->PublishSnapshot(std::move(snapshot));
    }

    TString LogPrefix() const {
        return "[ResourcePoolsCache] ";
    }

private:
    std::unordered_map<TString, TPoolInfo> PoolsCache_;
    std::unordered_map<TString, NPrivate::TDatabaseInfo> DatabasesCache_;
    std::shared_ptr<const TResourcePoolClassifierSnapshot> LastClassifierSnapshot_;
    NKikimrConfig::TFeatureFlags FeatureFlags_;
    NKikimrConfig::TWorkloadManagerConfig WorkloadManagerConfig_;
    bool EnableResourcePools_ = false;
    bool EnableResourcePoolsOnServerless_ = false;
    bool SubscribedOnResourcePoolClassifiers_ = false;

    TActorId WorkloadManagerServiceId_;
    std::shared_ptr<NPrivate::TWorkloadManagerGateway> Gateway_;
};

}

NActors::IActor* CreateResourcePoolsCacheActor(NActors::TActorId workloadManagerServiceId) {
    return new TResourcePoolsCacheActor(workloadManagerServiceId);
}

}
