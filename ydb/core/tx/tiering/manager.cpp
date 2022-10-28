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

TManager& TManager::Restart(const TTierConfig& config, const bool activity) {
    if (Config.IsSame(config)) {
        return *this;
    }
    if (Config.NeedExport()) {
        Stop();
    }
    Config = config;
    if (activity) {
        Start();
    }
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
        CreateS3Actor(TabletId, ctx.SelfID, Config.GetTierName())
    );
    ctx.Send(newActor, new TEvPrivate::TEvS3Settings(Config.GetProtoConfig().GetObjectStorage()));
    Stop();
    StorageActorId = newActor;
#endif
    return true;
}

TManager::TManager(const ui64 tabletId, const TTierConfig& config)
    : TabletId(tabletId)
    , Config(config) {
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
        auto it = snapshot.GetTierConfigs().find(OwnerPath + "." + itSelf->first);
        if (it == snapshot.GetTierConfigs().end()) {
            itSelf->second.Stop();
            itSelf = Managers.erase(itSelf);
        } else {
            itSelf->second.Restart(it->second, IsActive());
            ++itSelf;
        }
    }
    for (auto&& i : snapshot.GetTierConfigs()) {
        if (i.second.GetOwnerPath() != OwnerPath && !!OwnerPath) {
            continue;
        }
        if (Managers.contains(i.second.GetTierName())) {
            continue;
        }
        NTiers::TManager localManager(TabletId, i.second);
        auto& manager = Managers.emplace(i.second.GetTierName(), std::move(localManager)).first->second;
        if (IsActive()) {
            manager.Start();
        }
    }
}

TActorId TTiersManager::GetStorageActorId(const TString& tierName) {
    auto it = Managers.find(tierName);
    if (it == Managers.end()) {
        ALS_ERROR(NKikimrServices::TX_COLUMNSHARD) << "No S3 actor for tier '" << tierName << "' at tablet " << TabletId;
        return {};
    }
    auto actorId = it->second.GetStorageActorId();
    if (!actorId) {
        ALS_ERROR(NKikimrServices::TX_COLUMNSHARD) << "Not started storage actor for tier '" << tierName << "' at tablet " << TabletId;
        return {};
    }
    return actorId;
}

TTiersManager& TTiersManager::Start(std::shared_ptr<TTiersManager> ownerPtr) {
    if (ActiveFlag) {
        return *this;
    }
    ActiveFlag = true;
    Y_VERIFY(!Actor);
    Actor = new TTiersManager::TActor(ownerPtr);
    TActivationContext::AsActorContext().RegisterWithSameMailbox(Actor);
    for (auto&& i : Managers) {
        i.second.Start();
    }
    return *this;
}

TTiersManager& TTiersManager::Stop() {
    if (!ActiveFlag) {
        return *this;
    }
    ActiveFlag = false;
    Y_VERIFY(!!Actor);
    if (TlsActivationContext) {
        TActivationContext::AsActorContext().Send(Actor->SelfId(), new NActors::TEvents::TEvPoison);
    }
    Actor = nullptr;
    for (auto&& i : Managers) {
        i.second.Stop();
    }
    return *this;
}

const NKikimr::NColumnShard::NTiers::TManager& TTiersManager::GetManagerVerified(const TString& tierId) const {
    auto it = Managers.find(tierId);
    Y_VERIFY(it != Managers.end());
    return it->second;
}

NMetadataProvider::ISnapshotParser::TPtr TTiersManager::GetExternalDataManipulation() const {
    if (!ExternalDataManipulation) {
        ExternalDataManipulation = std::make_shared<NTiers::TSnapshotConstructor>();
        auto edmPtr = std::dynamic_pointer_cast<NTiers::TSnapshotConstructor>(ExternalDataManipulation);
        edmPtr->Start(edmPtr);
    }
    return ExternalDataManipulation;
}

THashMap<ui64, NKikimr::NOlap::TTiersInfo> TTiersManager::GetTiering() const {
    THashMap<ui64, NKikimr::NOlap::TTiersInfo> result;
    auto snapshotPtr = std::dynamic_pointer_cast<NTiers::TConfigsSnapshot>(Snapshot);
    Y_VERIFY(snapshotPtr);
    for (auto&& i : snapshotPtr->GetTableTierings()) {
        result.emplace(i.second.GetTablePathId(), i.second.BuildTiersInfo());
    }
    return result;
}

TTiersManager::~TTiersManager() {
    auto cs = std::dynamic_pointer_cast<NTiers::TSnapshotConstructor>(ExternalDataManipulation);
    if (!!cs) {
        cs->Stop();
    }
}

}
