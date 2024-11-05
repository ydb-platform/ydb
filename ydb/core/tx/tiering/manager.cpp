#include "common.h"
#include "manager.h"
#include "external_data.h"

#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/services/metadata/secret/fetcher.h>

namespace NKikimr::NColumnShard {

class TTiersManager::TActor: public TActorBootstrapped<TTiersManager::TActor> {
private:
    std::shared_ptr<TTiersManager> Owner;
    NMetadata::NFetcher::ISnapshotsFetcher::TPtr SecretsFetcher;
    std::shared_ptr<NMetadata::NSecret::TSnapshot> SecretsSnapshot;
    std::shared_ptr<NTiers::TConfigsSnapshot> ConfigsSnapshot;
    TActorId GetExternalDataActorId() const {
        return NMetadata::NProvider::MakeServiceId(SelfId().NodeId());
    }
public:
    TActor(std::shared_ptr<TTiersManager> owner)
        : Owner(owner)
        , SecretsFetcher(std::make_shared<NMetadata::NSecret::TSnapshotsFetcher>())
    {

    }
    ~TActor() {
        Owner->Stop(false);
    }

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NMetadata::NProvider::TEvRefreshSubscriberData, Handle);
            hFunc(NActors::TEvents::TEvPoison, Handle);
            default:
                break;
        }
    }

    void Bootstrap() {
        Become(&TThis::StateMain);
        AFL_INFO(NKikimrServices::TX_TIERING)("event", "start_subscribing_metadata");
        Send(GetExternalDataActorId(), new NMetadata::NProvider::TEvSubscribeExternal(Owner->GetExternalDataManipulation()));
        Send(GetExternalDataActorId(), new NMetadata::NProvider::TEvSubscribeExternal(SecretsFetcher));
    }

    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
        auto snapshot = ev->Get()->GetSnapshot();
        if (auto configs = std::dynamic_pointer_cast<NTiers::TConfigsSnapshot>(snapshot)) {
            AFL_DEBUG(NKikimrServices::TX_TIERING)("event", "TEvRefreshSubscriberData")("snapshot", "configs");
            ConfigsSnapshot = configs;
            if (SecretsSnapshot) {
                Owner->TakeConfigs(ConfigsSnapshot, SecretsSnapshot);
            } else {
                ALS_DEBUG(NKikimrServices::TX_TIERING) << "Waiting secrets for update at tablet " << Owner->TabletId;
            }
        } else if (auto secrets = std::dynamic_pointer_cast<NMetadata::NSecret::TSnapshot>(snapshot)) {
            AFL_DEBUG(NKikimrServices::TX_TIERING)("event", "TEvRefreshSubscriberData")("snapshot", "secrets");
            SecretsSnapshot = secrets;
            if (ConfigsSnapshot) {
                Owner->TakeConfigs(ConfigsSnapshot, SecretsSnapshot);
            } else {
                ALS_DEBUG(NKikimrServices::TX_TIERING) << "Waiting configs for update at tablet " << Owner->TabletId;
            }
        } else {
            Y_ABORT_UNLESS(false, "unexpected behaviour");
        }
    }

    void Handle(NActors::TEvents::TEvPoison::TPtr& /*ev*/) {
        Send(GetExternalDataActorId(), new NMetadata::NProvider::TEvUnsubscribeExternal(Owner->GetExternalDataManipulation()));
        Send(GetExternalDataActorId(), new NMetadata::NProvider::TEvUnsubscribeExternal(SecretsFetcher));
        PassAway();
    }
};

namespace NTiers {

TManager& TManager::Restart(const TTierConfig& config, std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets) {
    ALS_DEBUG(NKikimrServices::TX_TIERING) << "Restarting tier '" << GetTierName() << "' at tablet " << TabletId;
    if (Config.IsSame(config)) {
        return *this;
    }
    Stop();
    Config = config;
    Start(secrets);
    return *this;
}

bool TManager::Stop() {
    S3Settings.reset();
    ALS_DEBUG(NKikimrServices::TX_TIERING) << "Tier '" << GetTierName() << "' stopped at tablet " << TabletId;
    return true;
}

bool TManager::Start(std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets) {
    AFL_VERIFY(!S3Settings)("tier", GetTierName())("event", "already started");
    S3Settings = Config.GetPatchedConfig(secrets);
    ALS_DEBUG(NKikimrServices::TX_TIERING) << "Tier '" << GetTierName() << "' started at tablet " << TabletId;
    return true;
}

TManager::TManager(const ui64 tabletId, const NActors::TActorId& tabletActorId, const TTierConfig& config)
    : TabletId(tabletId)
    , TabletActorId(tabletActorId)
    , Config(config)
{
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

void TTiersManager::TakeConfigs(NMetadata::NFetcher::ISnapshot::TPtr snapshotExt, std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets) {
    ALS_INFO(NKikimrServices::TX_TIERING) << "Take configs:"
        << (snapshotExt ? " snapshots" : "") << (secrets ? " secrets" : "") << " at tablet " << TabletId;

    auto snapshotPtr = std::dynamic_pointer_cast<NTiers::TConfigsSnapshot>(snapshotExt);
    Y_ABORT_UNLESS(snapshotPtr);
    Snapshot = snapshotExt;
    Secrets = secrets;
    auto& snapshot = *snapshotPtr;
    for (auto itSelf = Managers.begin(); itSelf != Managers.end(); ) {
        auto it = snapshot.GetTierConfigs().find(itSelf->first);
        if (it == snapshot.GetTierConfigs().end()) {
            itSelf->second.Stop();
            itSelf = Managers.erase(itSelf);
        } else {
            itSelf->second.Restart(it->second, Secrets);
            ++itSelf;
        }
    }
    for (auto&& i : snapshot.GetTierConfigs()) {
        auto tierName = i.second.GetTierName();
        ALS_DEBUG(NKikimrServices::TX_TIERING) << "Take config for tier '" << tierName << "' at tablet " << TabletId;
        if (Managers.contains(tierName)) {
            ALS_DEBUG(NKikimrServices::TX_TIERING) << "Ignore tier '" << tierName << "' at tablet " << TabletId;
            continue;
        }
        NTiers::TManager localManager(TabletId, TabletActorId, i.second);
        auto itManager = Managers.emplace(tierName, std::move(localManager)).first;
        itManager->second.Start(Secrets);
    }

    if (ShardCallback && TlsActivationContext) {
        ShardCallback(TActivationContext::AsActorContext());
    }
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

NMetadata::NFetcher::ISnapshotsFetcher::TPtr TTiersManager::GetExternalDataManipulation() const {
    if (!ExternalDataManipulation) {
        ExternalDataManipulation = std::make_shared<NTiers::TSnapshotConstructor>();
    }
    return ExternalDataManipulation;
}

THashMap<ui64, NKikimr::NOlap::TTiering> TTiersManager::GetTiering() const {
    THashMap<ui64, NKikimr::NOlap::TTiering> result;
    AFL_VERIFY(IsReady());
    auto snapshotPtr = std::dynamic_pointer_cast<NTiers::TConfigsSnapshot>(Snapshot);
    Y_ABORT_UNLESS(snapshotPtr);
    auto& tierConfigs = snapshotPtr->GetTierConfigs();
    for (auto&& i : PathIdTiering) {
        auto* tieringRule = snapshotPtr->GetTieringById(i.second);
        if (tieringRule) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("path_id", i.first)("tiering_name", i.second)("event", "activation");
            NOlap::TTiering tiering = tieringRule->BuildOlapTiers();
            for (auto& [name, tier] : tiering.GetTierByName()) {
                AFL_VERIFY(name != NOlap::NTiering::NCommon::DeleteTierName);
                auto it = tierConfigs.find(name);
                if (it != tierConfigs.end()) {
                    tier->SetSerializer(NTiers::ConvertCompression(it->second.GetCompression()));
                }
            }
            result.emplace(i.first, std::move(tiering));
        } else {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("path_id", i.first)("tiering_name", i.second)("event", "not_found");
        }
    }
    return result;
}

TActorId TTiersManager::GetActorId() const {
    if (Actor) {
        return Actor->SelfId();
    } else {
        return {};
    }
}

}
