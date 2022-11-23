#include "manager.h"
#include "external_data.h"
#include "s3_actor.h"

#include <ydb/core/tx/columnshard/columnshard_private_events.h>

namespace NKikimr::NColumnShard {

class TTiersManager::TActor: public TActorBootstrapped<TTiersManager::TActor> {
private:
    std::shared_ptr<TTiersManager> Owner;
    TActorId GetExternalDataActorId() const {
        return NMetadataProvider::MakeServiceId(SelfId().NodeId());
    }
public:
    TActor(std::shared_ptr<TTiersManager> owner)
        : Owner(owner) {

    }
    ~TActor() {
        Owner->Stop();
    }
    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NMetadataProvider::TEvRefreshSubscriberData, Handle);
            hFunc(NActors::TEvents::TEvPoison, Handle);
            default:
                break;
        }
    }
    void Bootstrap() {
        Become(&TThis::StateMain);
        Send(GetExternalDataActorId(), new NMetadataProvider::TEvSubscribeExternal(Owner->GetExternalDataManipulation()));
    }
    void Handle(NMetadataProvider::TEvRefreshSubscriberData::TPtr& ev) {
        auto snapshot = ev->Get()->GetSnapshot();
        Owner->TakeConfigs(snapshot);
    }
    void Handle(NActors::TEvents::TEvPoison::TPtr& /*ev*/) {
        Send(GetExternalDataActorId(), new NMetadataProvider::TEvUnsubscribeExternal(Owner->GetExternalDataManipulation()));
        PassAway();
    }
};

namespace NTiers {

TManager& TManager::Restart(const TTierConfig& config) {
    if (Config.IsSame(config)) {
        return *this;
    }
    if (Config.NeedExport()) {
        Stop();
    }
    Config = config;
    Start();
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

bool TManager::Start() {
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
    ctx.Send(newActor, new TEvPrivate::TEvS3Settings(Config.GetProtoConfig().GetObjectStorage()));
    Stop();
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

NOlap::TStorageTier TManager::BuildTierStorage() const {
    NOlap::TStorageTier result;
    result.Name = Config.GetTierName();
    if (Config.GetProtoConfig().HasCompression()) {
        result.Compression = ConvertCompression(Config.GetProtoConfig().GetCompression());
    }
    return result;
}

NKikimr::NOlap::TCompression TManager::ConvertCompression(const NKikimrSchemeOp::TCompressionOptions& compression) {
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

void TTiersManager::TakeConfigs(NMetadataProvider::ISnapshot::TPtr snapshotExt) {
    auto snapshotPtr = std::dynamic_pointer_cast<NTiers::TConfigsSnapshot>(snapshotExt);
    Y_VERIFY(snapshotPtr);
    Snapshot = snapshotExt;
    auto& snapshot = *snapshotPtr;
    for (auto itSelf = Managers.begin(); itSelf != Managers.end(); ) {
        auto it = snapshot.GetTierConfigs().find(itSelf->first);
        if (it == snapshot.GetTierConfigs().end()) {
            itSelf->second.Stop();
            itSelf = Managers.erase(itSelf);
        } else {
            itSelf->second.Restart(it->second);
            ++itSelf;
        }
    }
    for (auto&& i : snapshot.GetTierConfigs()) {
        if (Managers.contains(i.second.GetGlobalTierId())) {
            continue;
        }
        NTiers::TManager localManager(TabletId, TabletActorId, i.second);
        auto& manager = Managers.emplace(i.second.GetGlobalTierId(), std::move(localManager)).first->second;
        manager.Start();
    }
}

TActorId TTiersManager::GetStorageActorId(const NTiers::TGlobalTierId& tierId) {
    auto it = Managers.find(tierId);
    if (it == Managers.end()) {
        ALS_ERROR(NKikimrServices::TX_TIERING) << "No S3 actor for tier '" << tierId.ToString() << "' at tablet " << TabletId;
        return {};
    }
    auto actorId = it->second.GetStorageActorId();
    if (!actorId) {
        ALS_ERROR(NKikimrServices::TX_TIERING) << "Not started storage actor for tier '" << tierId.ToString() << "' at tablet " << TabletId;
        return {};
    }
    return actorId;
}

TTiersManager& TTiersManager::Start(std::shared_ptr<TTiersManager> ownerPtr) {
    ALS_ERROR(NKikimrServices::TX_TIERING) << "AAAAAAAAAAAAAA ";
    Y_VERIFY(!Actor);
    Actor = new TTiersManager::TActor(ownerPtr);
    TActivationContext::AsActorContext().RegisterWithSameMailbox(Actor);
    for (auto&& i : Managers) {
        i.second.Start();
    }
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

const NTiers::TManager& TTiersManager::GetManagerVerified(const NTiers::TGlobalTierId& tierId) const {
    auto it = Managers.find(tierId);
    Y_VERIFY(it != Managers.end());
    return it->second;
}

NMetadataProvider::ISnapshotParser::TPtr TTiersManager::GetExternalDataManipulation() const {
    if (!ExternalDataManipulation) {
        ExternalDataManipulation = std::make_shared<NTiers::TSnapshotConstructor>();
    }
    return ExternalDataManipulation;
}

THashMap<ui64, NKikimr::NOlap::TTiersInfo> TTiersManager::GetTiering() const {
    THashMap<ui64, NKikimr::NOlap::TTiersInfo> result;
    if (!Snapshot) {
        return result;
    }
    auto snapshotPtr = std::dynamic_pointer_cast<NTiers::TConfigsSnapshot>(Snapshot);
    Y_VERIFY(snapshotPtr);
    for (auto&& i : snapshotPtr->GetTableTierings()) {
        if (!EnabledPathId.contains(i.second.GetTablePathId())) {
            continue;
        }
        result.emplace(i.second.GetTablePathId(), i.second.BuildTiersInfo());
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
