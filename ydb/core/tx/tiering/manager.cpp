#include "common.h"
#include "manager.h"
#include "external_data.h"
#include "s3_actor.h"

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
        Owner->Stop();
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
        Send(GetExternalDataActorId(), new NMetadata::NProvider::TEvSubscribeExternal(Owner->GetExternalDataManipulation()));
        Send(GetExternalDataActorId(), new NMetadata::NProvider::TEvSubscribeExternal(SecretsFetcher));
    }
    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
        auto snapshot = ev->Get()->GetSnapshot();
        if (auto configs = std::dynamic_pointer_cast<NTiers::TConfigsSnapshot>(snapshot)) {
            ConfigsSnapshot = configs;
            if (SecretsSnapshot) {
                Owner->TakeConfigs(ConfigsSnapshot, SecretsSnapshot);
            }
        } else if (auto secrets = std::dynamic_pointer_cast<NMetadata::NSecret::TSnapshot>(snapshot)) {
            SecretsSnapshot = secrets;
            if (ConfigsSnapshot) {
                Owner->TakeConfigs(ConfigsSnapshot, SecretsSnapshot);
            }
        } else {
            Y_VERIFY(false, "unexpected behaviour");
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
    if (Config.IsSame(config)) {
        return *this;
    }
    if (Config.NeedExport()) {
        Stop();
    }
    Config = config;
    Start(secrets);
    return *this;
}

bool TManager::Stop() {
    if (!StorageActorId) {
        return true;
    }
    if (TlsActivationContext) {
        TActivationContext::AsActorContext().Send(StorageActorId, new TEvents::TEvPoisonPill());
    }
    StorageActorId = {};
    return true;
}

bool TManager::Start(std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets) {
    if (!Config.NeedExport()) {
        return true;
    }
    if (!!StorageActorId) {
        return true;
    }
#ifndef KIKIMR_DISABLE_S3_OPS
    auto& ctx = TActivationContext::AsActorContext();
    const NActors::TActorId newActor = ctx.Register(
        CreateS3Actor(TabletId, TabletActorId, Config.GetTierName())
    );
    auto s3Config = Config.GetPatchedConfig(secrets);

    ctx.Send(newActor, new TEvPrivate::TEvS3Settings(s3Config));
    StorageActorId = newActor;
#endif
    return true;
}

TManager::TManager(const ui64 tabletId, const NActors::TActorId& tabletActorId, const TTierConfig& config)
    : TabletId(tabletId)
    , TabletActorId(tabletActorId)
    , Config(config)
{
}

NKikimr::NOlap::TCompression ConvertCompression(const NKikimrSchemeOp::TCompressionOptions& compression) {
    NOlap::TCompression out;
    if (compression.HasCompressionCodec()) {
        switch (compression.GetCompressionCodec()) {
            case NKikimrSchemeOp::EColumnCodec::ColumnCodecPlain:
                out.Codec = arrow::Compression::UNCOMPRESSED;
                break;
            case NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4:
                out.Codec = arrow::Compression::LZ4_FRAME;
                break;
            case NKikimrSchemeOp::EColumnCodec::ColumnCodecZSTD:
                out.Codec = arrow::Compression::ZSTD;
                break;
        }
    }

    if (compression.HasCompressionLevel()) {
        out.Level = compression.GetCompressionLevel();
    }
    return out;
}
}

void TTiersManager::TakeConfigs(NMetadata::NFetcher::ISnapshot::TPtr snapshotExt, std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets) {
    auto snapshotPtr = std::dynamic_pointer_cast<NTiers::TConfigsSnapshot>(snapshotExt);
    Y_VERIFY(snapshotPtr);
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
        if (Managers.contains(i.second.GetTierName())) {
            continue;
        }
        NTiers::TManager localManager(TabletId, TabletActorId, i.second);
        auto& manager = Managers.emplace(i.second.GetTierName(), std::move(localManager)).first->second;
        manager.Start(Secrets);
    }
}

TActorId TTiersManager::GetStorageActorId(const TString& tierId) {
    auto it = Managers.find(tierId);
    if (it == Managers.end()) {
        ALS_ERROR(NKikimrServices::TX_TIERING) << "No S3 actor for tier '" << tierId << "' at tablet " << TabletId;
        return {};
    }
    auto actorId = it->second.GetStorageActorId();
    if (!actorId) {
        ALS_ERROR(NKikimrServices::TX_TIERING) << "Not started storage actor for tier '" << tierId << "' at tablet " << TabletId;
        return {};
    }
    return actorId;
}

TTiersManager& TTiersManager::Start(std::shared_ptr<TTiersManager> ownerPtr) {
    Y_VERIFY(!Actor);
    Actor = new TTiersManager::TActor(ownerPtr);
    TActivationContext::AsActorContext().RegisterWithSameMailbox(Actor);
    return *this;
}

TTiersManager& TTiersManager::Stop() {
    if (!Actor) {
        return *this;
    }
    if (TlsActivationContext) {
        TActivationContext::AsActorContext().Send(Actor->SelfId(), new NActors::TEvents::TEvPoison);
    }
    Actor = nullptr;
    for (auto&& i : Managers) {
        i.second.Stop();
    }
    return *this;
}

const NTiers::TManager& TTiersManager::GetManagerVerified(const TString& tierId) const {
    auto it = Managers.find(tierId);
    Y_VERIFY(it != Managers.end());
    return it->second;
}

NMetadata::NFetcher::ISnapshotsFetcher::TPtr TTiersManager::GetExternalDataManipulation() const {
    if (!ExternalDataManipulation) {
        ExternalDataManipulation = std::make_shared<NTiers::TSnapshotConstructor>();
    }
    return ExternalDataManipulation;
}

THashMap<ui64, NKikimr::NOlap::TTiering> TTiersManager::GetTiering() const {
    THashMap<ui64, NKikimr::NOlap::TTiering> result;
    if (!IsReady()) {
        return result;
    }
    auto snapshotPtr = std::dynamic_pointer_cast<NTiers::TConfigsSnapshot>(Snapshot);
    Y_VERIFY(snapshotPtr);
    auto& tierConfigs = snapshotPtr->GetTierConfigs();
    for (auto&& i : PathIdTiering) {
        auto* tiering = snapshotPtr->GetTieringById(i.second);
        if (tiering) {
            result.emplace(i.first, tiering->BuildOlapTiers());
            for (auto& [pathId, pathTiering] : result) {
                for (auto& [name, tier] : pathTiering.TierByName) {
                    auto it = tierConfigs.find(name);
                    if (it != tierConfigs.end()) {
                        tier->Compression = NTiers::ConvertCompression(it->second.GetProtoConfig().GetCompression());
                        tier->NeedExport = it->second.NeedExport();
                    }
                }
            }
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
