#include "common.h"
#include "manager.h"

#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/tiering/fetcher.h>

#include <ydb/library/table_creator/table_creator.h>
#include <ydb/services/metadata/secret/fetcher.h>

#include <library/cpp/retry/retry_policy.h>
#include <util/string/vector.h>

namespace NKikimr::NColumnShard {

class TTiersManager::TActor: public TActorBootstrapped<TTiersManager::TActor> {
private:
    using IRetryPolicy = IRetryPolicy<>;

    std::shared_ptr<TTiersManager> Owner;
    IRetryPolicy::TPtr RetryPolicy;
    THashMap<TString, IRetryPolicy::IRetryState::TPtr> RetryStateByObject;
    NMetadata::NFetcher::ISnapshotsFetcher::TPtr SecretsFetcher;
    TActorId TiersFetcher;

private:
    TActorId GetExternalDataActorId() const {
        return NMetadata::NProvider::MakeServiceId(SelfId().NodeId());
    }

    void OnInvalidTierConfig(const TString& path) {
        if (!Owner->TierRefCount.contains(path)) {
            ResetRetryState(path);
            return;
        }
        AFL_DEBUG(NKikimrServices::TX_TIERING)("component", "tiers_manager")("event", "retry_watch_objects");
        auto findRetryState = RetryStateByObject.find(path);
        if (!findRetryState) {
            findRetryState = RetryStateByObject.emplace(path, RetryPolicy->CreateRetryState()).first;
        }
        auto retryDelay = findRetryState->second->GetNextRetryDelay();
        AFL_VERIFY(retryDelay)("object", path);
        ActorContext().Schedule(*retryDelay, std::make_unique<IEventHandle>(SelfId(), TiersFetcher, new NTiers::TEvWatchSchemeObject(std::vector<TString>({ path }))));
    }

    void ResetRetryState(const TString& path) {
        RetryStateByObject.erase(path);
    }

    void OnFetchingFailure(const TString& path) {
        if (Owner->TierRefCount.contains(path)) {
            OnInvalidTierConfig(path);
        }
    }

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NMetadata::NProvider::TEvRefreshSubscriberData, Handle);
            hFunc(NActors::TEvents::TEvPoison, Handle);
            hFunc(NTiers::TEvNotifySchemeObjectUpdated, Handle);
            hFunc(NTiers::TEvNotifySchemeObjectDeleted, Handle);
            hFunc(NTiers::TEvSchemeObjectResolutionFailed, Handle);
            hFunc(NTiers::TEvWatchSchemeObject, Handle);
            default:
                break;
        }
    }

    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
        auto snapshot = ev->Get()->GetSnapshot();
        if (auto secrets = std::dynamic_pointer_cast<NMetadata::NSecret::TSnapshot>(snapshot)) {
            AFL_DEBUG(NKikimrServices::TX_TIERING)("event", "TEvRefreshSubscriberData")("snapshot", "secrets");
            Owner->UpdateSecretsSnapshot(secrets);
        } else {
            AFL_VERIFY(false);
        }
    }

    void Handle(NActors::TEvents::TEvPoison::TPtr& /*ev*/) {
        Send(GetExternalDataActorId(), new NMetadata::NProvider::TEvUnsubscribeExternal(SecretsFetcher));
        PassAway();
    }

    void Handle(NTiers::TEvNotifySchemeObjectUpdated::TPtr& ev) {
        AFL_DEBUG(NKikimrServices::TX_TIERING)("component", "tiering_manager")("event", "object_updated")("path", ev->Get()->GetObjectPath());
        const TString& objectPath = ev->Get()->GetObjectPath();
        const auto& description = ev->Get()->GetDescription();
        ResetRetryState(objectPath);
        if (description.GetSelf().GetPathType() == NKikimrSchemeOp::EPathTypeExternalDataSource) {
            NTiers::TTierConfig tier;
            if (const auto status = tier.DeserializeFromProto(description.GetExternalDataSourceDescription()); status.IsFail()) {
                AFL_WARN(NKikimrServices::TX_TIERING)("event", "fetched_invalid_tier_settings")("error", status.GetErrorMessage());
                OnInvalidTierConfig(objectPath);
                return;
            }
            Owner->UpdateTierConfig(tier, objectPath);
        } else {
            AFL_WARN(false)("error", "invalid_object_type")("type", static_cast<ui64>(description.GetSelf().GetPathType()))("path", objectPath);
            OnInvalidTierConfig(objectPath);
        }
    }

    void Handle(NTiers::TEvNotifySchemeObjectDeleted::TPtr& ev) {
        AFL_DEBUG(NKikimrServices::TX_TIERING)("component", "tiering_manager")("event", "object_deleted")("name", ev->Get()->GetObjectPath());
        OnInvalidTierConfig(ev->Get()->GetObjectPath());
    }

    void Handle(NTiers::TEvSchemeObjectResolutionFailed::TPtr& ev) {
        const TString objectPath = ev->Get()->GetObjectPath();
        AFL_WARN(NKikimrServices::TX_TIERING)("event", "object_resolution_failed")("path", objectPath)(
            "reason", static_cast<ui64>(ev->Get()->GetReason()));
        OnInvalidTierConfig(objectPath);
    }

    void Handle(NTiers::TEvWatchSchemeObject::TPtr& ev) {
        Send(TiersFetcher, ev->Release());
    }

public:
    TActor(std::shared_ptr<TTiersManager> owner)
        : Owner(owner)
        , RetryPolicy(IRetryPolicy::GetExponentialBackoffPolicy(
              []() {
                  return ERetryErrorClass::ShortRetry;
              },
              TDuration::MilliSeconds(10), TDuration::MilliSeconds(200), TDuration::Seconds(30), 10))
        , SecretsFetcher(std::make_shared<NMetadata::NSecret::TSnapshotsFetcher>()) {
    }

    void Bootstrap() {
        AFL_INFO(NKikimrServices::TX_TIERING)("event", "start_subscribing_metadata");
        TiersFetcher = Register(new TSchemeObjectWatcher(SelfId()));
        Send(GetExternalDataActorId(), new NMetadata::NProvider::TEvSubscribeExternal(SecretsFetcher));
        Become(&TThis::StateMain);
    }

    ~TActor() {
        Owner->Stop(false);
    }
};

namespace NTiers {

TManager& TManager::Restart(const TTierConfig& config, std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets) {
    ALS_DEBUG(NKikimrServices::TX_TIERING) << "Restarting tier '" << TierName << "' at tablet " << TabletId;
    Stop();
    Start(config, secrets);
    return *this;
}

bool TManager::Stop() {
    S3Settings.reset();
    ALS_DEBUG(NKikimrServices::TX_TIERING) << "Tier '" << TierName << "' stopped at tablet " << TabletId;
    return true;
}

bool TManager::Start(const TTierConfig& config, std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets) {
    AFL_VERIFY(!S3Settings)("tier", TierName)("event", "already started");
    auto patchedConfig = config.GetPatchedConfig(secrets);
    if (patchedConfig.IsFail()) {
        AFL_ERROR(NKikimrServices::TX_TIERING)("error", "cannot_read_secrets")("reason", patchedConfig.GetErrorMessage());
        return false;
    }
    S3Settings = patchedConfig.DetachResult();
    ALS_DEBUG(NKikimrServices::TX_TIERING) << "Tier '" << TierName << "' started at tablet " << TabletId;
    return true;
}

TManager::TManager(const ui64 tabletId, const NActors::TActorId& tabletActorId, const TString& tierName)
    : TabletId(tabletId)
    , TabletActorId(tabletActorId)
    , TierName(tierName) {
}

NArrow::NSerialization::TSerializerContainer ConvertCompression(const NKikimrSchemeOp::TCompressionOptions& compressionProto) {
    NArrow::NSerialization::TSerializerContainer container;
    container.DeserializeFromProto(compressionProto).Validate();
    return container;
}

NArrow::NSerialization::TSerializerContainer ConvertCompression(const NKikimrSchemeOp::TOlapColumn::TSerializer& serializerProto) {
    NArrow::NSerialization::TSerializerContainer container;
    AFL_VERIFY(container.DeserializeFromProto(serializerProto));
    return container;
}
}

TTiersManager::TTierRefGuard::TTierRefGuard(const TString& tierName, TTiersManager& owner)
    : TierName(tierName)
    , Owner(&owner) {
    if (!Owner->TierRefCount.contains(TierName)) {
        Owner->RegisterTier(tierName);
    }
    ++Owner->TierRefCount[TierName];
}

TTiersManager::TTierRefGuard::~TTierRefGuard() {
    if (Owner) {
        auto findTier = Owner->TierRefCount.FindPtr(TierName);
        AFL_VERIFY(findTier);
        AFL_VERIFY(*findTier);
        --*findTier;
        if (!*findTier) {
            AFL_VERIFY(Owner->TierRefCount.erase(TierName));
            Owner->UnregisterTier(TierName);
        }
    }
}

void TTiersManager::OnConfigsUpdated(bool notifyShard) {
    for (auto& [tierName, manager] : Managers) {
        auto* findTierConfig = TierConfigs.FindPtr(tierName);
        if (Secrets && findTierConfig) {
            if (manager.IsReady()) {
                manager.Restart(*findTierConfig, Secrets);
            } else {
                manager.Start(*findTierConfig, Secrets);
            }
        } else {
            AFL_DEBUG(NKikimrServices::TX_TIERING)("event", "skip_tier_manager_reloading")("tier", tierName)("has_secrets", !!Secrets)(
                "found_tier_config", !!findTierConfig);
        }
    }

    if (notifyShard && ShardCallback && TlsActivationContext) {
        ShardCallback(TActivationContext::AsActorContext());
    }

    AFL_DEBUG(NKikimrServices::TX_TIERING)("event", "configs_updated")("configs", DebugString());
}

void TTiersManager::RegisterTier(const TString& name) {
    auto emplaced = Managers.emplace(name, NTiers::TManager(TabletId, TabletActorId, name));
    AFL_VERIFY(emplaced.second);

    auto* findConfig = TierConfigs.FindPtr(name);
    if (Secrets && findConfig) {
        emplaced.first->second.Start(*findConfig, Secrets);
    } else {
        AFL_DEBUG(NKikimrServices::TX_TIERING)("event", "skip_tier_manager_start")("tier", name)("has_secrets", !!Secrets)(
            "found_tier_config", !!findConfig);
    }
}

void TTiersManager::UnregisterTier(const TString& name) {
    auto findManager = Managers.find(name);
    AFL_VERIFY(findManager != Managers.end());
    findManager->second.Stop();
    Managers.erase(findManager);
}

TTiersManager& TTiersManager::Start(std::shared_ptr<TTiersManager> ownerPtr) {
    Y_ABORT_UNLESS(!Actor);
    Actor = new TTiersManager::TActor(ownerPtr);
    TActivationContext::AsActorContext().RegisterWithSameMailbox(Actor);
    return *this;
}

TTiersManager& TTiersManager::Stop(const bool needStopActor) {
    if (!Actor) {
        return *this;
    }
    if (TlsActivationContext && needStopActor) {
        TActivationContext::AsActorContext().Send(Actor->SelfId(), new NActors::TEvents::TEvPoison);
    }
    Actor = nullptr;
    for (auto&& i : Managers) {
        i.second.Stop();
    }
    return *this;
}

const NTiers::TManager* TTiersManager::GetManagerOptional(const TString& tierId) const {
    auto it = Managers.find(tierId);
    if (it != Managers.end()) {
        return &it->second;
    } else {
        return nullptr;
    }
}

void TTiersManager::EnablePathId(const ui64 pathId, const THashSet<TString>& usedTiers) {
    AFL_VERIFY(Actor)("error", "tiers_manager_is_not_started");
    auto& tierRefs = UsedTiers[pathId];
    tierRefs.clear();
    for (const TString& tierName : usedTiers) {
        AFL_VERIFY(tierName == CanonizePath(tierName))("current", tierName)("canonized", CanonizePath(tierName));
        tierRefs.emplace_back(tierName, *this);
        if (!TierConfigs.contains(tierName)) {
            const auto& actorContext = NActors::TActivationContext::AsActorContext();
            AFL_VERIFY(&actorContext)("error", "no_actor_context");
            actorContext.Send(Actor->SelfId(), new NTiers::TEvWatchSchemeObject({ tierName }));
        }
    }
    OnConfigsUpdated(false);
}

void TTiersManager::DisablePathId(const ui64 pathId) {
    UsedTiers.erase(pathId);
    OnConfigsUpdated(false);
}

void TTiersManager::UpdateSecretsSnapshot(std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets) {
    AFL_INFO(NKikimrServices::TX_TIERING)("event", "update_secrets")("tablet", TabletId);
    AFL_VERIFY(secrets);
    Secrets = secrets;
    OnConfigsUpdated();
}

void TTiersManager::UpdateTierConfig(const NTiers::TTierConfig& config, const TString& tierName, const bool notifyShard) {
    AFL_INFO(NKikimrServices::TX_TIERING)("event", "update_tier_config")("name", tierName)("tablet", TabletId);
    AFL_VERIFY(tierName == CanonizePath(tierName))("current", tierName)("canonized", CanonizePath(tierName));
    TierConfigs[tierName] = config;
    OnConfigsUpdated(notifyShard);
}

bool TTiersManager::AreConfigsComplete() const {
    for (const auto& [tier, cnt] : TierRefCount) {
        if (!TierConfigs.contains(tier)) {
            return false;
        }
    }
    return true;
}

TActorId TTiersManager::GetActorId() const {
    if (Actor) {
        return Actor->SelfId();
    } else {
        return {};
    }
}

TString TTiersManager::DebugString() {
    TStringBuilder sb;
    sb << "TIERS=";
    if (TierConfigs) {
        sb << "{";
        for (const auto& [name, config] : TierConfigs) {
            sb << name << ";";
        }
        sb << "}";
    }
    sb << ";USED_TIERS=";
    {
        sb << "{";
        for (const auto& [pathId, tiers] : UsedTiers) {
            sb << pathId << ":{";
            for (const auto& tierRef : tiers) {
                sb << tierRef.GetTierName() << ";";
            }
            sb << "}";
        }
        sb << "}";
    }
    sb << ";SECRETS=";
    if (Secrets) {
        sb << "{";
        for (const auto& [name, config] : Secrets->GetSecrets()) {
            sb << name.SerializeToString() << ";";
        }
        sb << "}";
    }
    return sb;
}
}
