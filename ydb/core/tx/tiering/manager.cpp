#include "common.h"
#include "manager.h"

#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/tiering/fetcher.h>

#include <ydb/library/table_creator/table_creator.h>
#include <ydb/services/metadata/secret/fetcher.h>

#include <util/string/vector.h>

namespace NKikimr::NColumnShard {

class TTiersManager::TActor: public TActorBootstrapped<TTiersManager::TActor> {
private:
    std::shared_ptr<TTiersManager> Owner;
    NMetadata::NFetcher::ISnapshotsFetcher::TPtr SecretsFetcher;
    TActorId TieredStorageFetcher;

private:
    TActorId GetExternalDataActorId() const {
        return NMetadata::NProvider::MakeServiceId(SelfId().NodeId());
    }

    void ScheduleRetryWatchObjects(std::unique_ptr<NTiers::TEvWatchSchemeObject> ev) const {
        AFL_DEBUG(NKikimrServices::TX_TIERING)("component", "tiers_manager")("event", "retry_watch_objects");
        constexpr static const TDuration RetryInterval = TDuration::Seconds(1);
        ActorContext().Schedule(RetryInterval, std::make_unique<IEventHandle>(SelfId(), TieredStorageFetcher, ev.release()));
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
            Y_ABORT_UNLESS(false, "unexpected snapshot");
        }
    }

    void Handle(NActors::TEvents::TEvPoison::TPtr& /*ev*/) {
        Send(GetExternalDataActorId(), new NMetadata::NProvider::TEvUnsubscribeExternal(SecretsFetcher));
        PassAway();
    }

    void Handle(NTiers::TEvNotifySchemeObjectUpdated::TPtr& ev) {
        AFL_DEBUG(NKikimrServices::TX_TIERING)("component", "tiering_manager")("event", "object_updated")("path", ev->Get()->GetObjectId());
        const TString& objectId = ev->Get()->GetObjectId();
        const auto& description = ev->Get()->GetDescription();
        if (description.GetSelf().GetPathType() == NKikimrSchemeOp::EPathTypeExternalDataSource) {
            NTiers::TTierConfig tier;
            if (const auto status = tier.DeserializeFromProto(description.GetExternalDataSourceDescription()); status.IsFail()) {
                AFL_VERIFY(false)("error", status.GetErrorMessage());
            }
            Owner->UpdateTierConfig(tier, objectId);
        } else {
            AFL_WARN(NKikimrServices::TX_TIERING)("error", "invalid_object_type")("type", static_cast<ui64>(description.GetSelf().GetPathType()))("path", objectId);
        }
    }

    void Handle(NTiers::TEvNotifySchemeObjectDeleted::TPtr& ev) {
        AFL_DEBUG(NKikimrServices::TX_TIERING)("component", "tiering_manager")("event", "object_deleted")("name", ev->Get()->GetObjectId());
    }

    void Handle(NTiers::TEvSchemeObjectResolutionFailed::TPtr& ev) {
        const TString objectId = ev->Get()->GetObjectId();
        switch (ev->Get()->GetReason()) {
            case NTiers::TEvSchemeObjectResolutionFailed::NOT_FOUND:
                AFL_WARN(NKikimrServices::TX_TIERING)("event", "object_not_found")("name", objectId);
                break;
            case NTiers::TEvSchemeObjectResolutionFailed::LOOKUP_ERROR:
                ScheduleRetryWatchObjects(std::make_unique<NTiers::TEvWatchSchemeObject>(std::vector<TString>({ objectId })));
                break;
        }
    }

    void Handle(NTiers::TEvWatchSchemeObject::TPtr& ev) {
        Send(TieredStorageFetcher, ev->Release());
    }

public:
    TActor(std::shared_ptr<TTiersManager> owner)
        : Owner(owner)
        , SecretsFetcher(std::make_shared<NMetadata::NSecret::TSnapshotsFetcher>())
    {
    }

    void Bootstrap() {
        AFL_INFO(NKikimrServices::TX_TIERING)("event", "start_subscribing_metadata");
        TieredStorageFetcher = Register(new TSchemeObjectWatcher(SelfId()));
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
    S3Settings = config.GetPatchedConfig(secrets);
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
    TierConfigs[tierName] = config;
    OnConfigsUpdated(notifyShard);
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
