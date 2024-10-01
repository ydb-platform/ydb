#include "common.h"
#include "manager.h"
#include "fetcher.h"

#include <ydb/core/tx/columnshard/columnshard_private_events.h>

#include <ydb/library/table_creator/table_creator.h>
#include <ydb/services/metadata/secret/fetcher.h>

namespace NKikimr::NColumnShard {

class TTiersManager::TActor: public TActorBootstrapped<TTiersManager::TActor> {
private:
    std::shared_ptr<TTiersManager> Owner;
    NMetadata::NFetcher::ISnapshotsFetcher::TPtr SecretsFetcher;
    TActorId TieringFetcher;

    std::shared_ptr<NMetadata::NSecret::TSnapshot> SecretsSnapshot;
    THashMap<TString, NTiers::TTierConfig> Tiers;
    THashMap<TString, NTiers::TTieringRule> Tierings;

private:
    TActorId GetExternalDataActorId() const {
        return NMetadata::NProvider::MakeServiceId(SelfId().NodeId());
    }

    void UpdateSnapshot() const {
        Owner->TakeConfigs(NTiers::TConfigsSnapshot(Tiers, Tierings), SecretsSnapshot);
    }

    void ScheduleRetryWatchObjects(std::unique_ptr<NTiers::TEvWatchTieringObjects> ev) const {
        constexpr static const TDuration RetryInterval = TDuration::Seconds(1);
        ActorContext().Schedule(RetryInterval, std::make_unique<IEventHandle>(SelfId(), TieringFetcher, ev.release()));
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
            hFunc(NTiers::TEvWatchTieringObjects, Handle);
            hFunc(NActors::TEvents::TEvPoison, Handle);
            hFunc(NTiers::TEvNotifyTieringUpdated, Handle);
            hFunc(NTiers::TEvNotifyTierUpdated, Handle);
            hFunc(NTiers::TEvNotifyObjectDeleted, Handle);
            hFunc(NTiers::TEvObjectResolutionFailed, Handle);
            default:
                break;
        }
    }

    void Bootstrap() {
        TieringFetcher = Register(new TTieringFetcher(SelfId()));
        Become(&TThis::StateMain);
        AFL_INFO(NKikimrServices::TX_TIERING)("event", "start_subscribing_metadata");
        Send(GetExternalDataActorId(), new NMetadata::NProvider::TEvSubscribeExternal(SecretsFetcher));
    }

    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
        auto snapshot = ev->Get()->GetSnapshot();
        if (auto secrets = std::dynamic_pointer_cast<NMetadata::NSecret::TSnapshot>(snapshot)) {
            AFL_DEBUG(NKikimrServices::TX_TIERING)("event", "TEvRefreshSubscriberData")("snapshot", "secrets");
            SecretsSnapshot = secrets;
            UpdateSnapshot();
        } else {
            Y_ABORT_UNLESS(false, "unexpected behaviour");
        }
    }

    void Handle(NActors::TEvents::TEvPoison::TPtr& /*ev*/) {
        Send(GetExternalDataActorId(), new NMetadata::NProvider::TEvUnsubscribeExternal(SecretsFetcher));
        PassAway();
    }

    void Handle(NTiers::TEvWatchTieringObjects::TPtr& ev) {
        Send(TieringFetcher, ev->Release());
    }

    void Handle(NTiers::TEvNotifyTierUpdated::TPtr& ev) {
        Tiers.emplace(ev->Get()->GetId(), ev->Get()->GetConfig());
        UpdateSnapshot();
    }

    void Handle(NTiers::TEvNotifyTieringUpdated::TPtr& ev) {
        const auto& config = ev->Get()->GetConfig();
        {
            std::vector<TString> uninitializedTiers;
            for (const auto& interval : config.GetIntervals()) {
                if (!Tiers.contains(interval.GetTierName())) {
                    uninitializedTiers.emplace_back(interval.GetTierName());
                }
            }
            Send(TieringFetcher, new NTiers::TEvWatchTieringObjects({}, std::move(uninitializedTiers)));
        }

        Tierings.emplace(ev->Get()->GetId(), config);
        UpdateSnapshot();
    }

    void Handle(NTiers::TEvNotifyObjectDeleted::TPtr& ev) {
        switch (ev->Get()->GetObjectType()) {
            case NTiers::UNKNOWN:
                AFL_VERIFY(false);
            case NTiers::TIER:
                Tiers.erase(ev->Get()->GetObjectId());
                break;
            case NTiers::TIERING:
                Tierings.erase(ev->Get()->GetObjectId());
                break;
        }
        UpdateSnapshot();
    }

    void Handle(NTiers::TEvObjectResolutionFailed::TPtr& ev) {
        const TString objectId = ev->Get()->GetObjectId();
        const bool isTransientError = ev->Get()->GetReason() == NTiers::TEvObjectResolutionFailed::LOOKUP_ERROR;
        if (isTransientError) {
            switch (ev->Get()->GetObjectType()) {
                case NTiers::UNKNOWN:
                    AFL_VERIFY(false);
                case NTiers::TIER:
                    ScheduleRetryWatchObjects(std::make_unique<NTiers::TEvWatchTieringObjects>(std::vector<TString>(), std::vector<TString>({ objectId })));
                    break;
                case NTiers::TIERING:
                    ScheduleRetryWatchObjects(std::make_unique<NTiers::TEvWatchTieringObjects>(std::vector<TString>({ objectId }), std::vector<TString>()));
                    break;
            }
        } else {
            if (ev->Get()->GetReason() == NTiers::TEvObjectResolutionFailed::NOT_FOUND) {
                AFL_WARN(NKikimrServices::TX_TIERING)("event", "object_not_found")("type", static_cast<ui64>(ev->Get()->GetObjectType()))("name", objectId);
            }
        }
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

void TTiersManager::TakeConfigs(NTiers::TConfigsSnapshot snapshot, std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets) {
    ALS_INFO(NKikimrServices::TX_TIERING) << "Take configs: snapshots" << (secrets ? " secrets" : "") << " at tablet " << TabletId;

    Snapshot = snapshot;
    Secrets = secrets;
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

    HasCompleteData = ValidateDependencies();

    if (ShardCallback && TlsActivationContext) {
        if (IsReady()) {
            ShardCallback(TActivationContext::AsActorContext());
        } else {
            AFL_DEBUG(NKikimrServices::TX_TIERING)("event", "skip_refresh_tiering_on_shard")("reason", "not_ready");
        }
    }

    AFL_DEBUG(NKikimrServices::TX_TIERING)("event", "configs_updated")("snapshot", snapshot.DebugString())("has_complete_data", HasCompleteData);
}

bool TTiersManager::ValidateDependencies() const {
    for (const auto& [id, config] : Snapshot.GetTableTierings()) {
        for (const auto& interval : config.GetIntervals()) {
            if (!Snapshot.GetTierById(interval.GetTierName())) {
                return false;
            }
        }
        return true;
    }
    for (const auto& [pathId, tieringId] : PathIdTiering) {
        if (!Snapshot.GetTieringById(tieringId)) {
            return false;
        }
    }
    return true;
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

THashMap<ui64, NKikimr::NOlap::TTiering> TTiersManager::GetTiering() const {
    THashMap<ui64, NKikimr::NOlap::TTiering> result;
    AFL_VERIFY(IsReady());
    auto& tierConfigs = Snapshot.GetTierConfigs();
    for (auto&& i : PathIdTiering) {
        auto* tieringRule = Snapshot.GetTieringById(i.second);
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

void TTiersManager::EnablePathId(const ui64 pathId, const TString& tieringId) {
    PathIdTiering.emplace(pathId, tieringId);
    if (!Snapshot.GetTieringById(tieringId)) {
        TActivationContext::AsActorContext().Send(Actor->SelfId(), new NTiers::TEvWatchTieringObjects({ tieringId }, {}));
        HasCompleteData = false;
    }
}

void TTiersManager::DisablePathId(const ui64 pathId) {
    PathIdTiering.erase(pathId);
}

TActorId TTiersManager::GetActorId() const {
    if (Actor) {
        return Actor->SelfId();
    } else {
        return {};
    }
}

}
