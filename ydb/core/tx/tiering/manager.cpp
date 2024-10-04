#include "common.h"
#include "manager.h"

#include <ydb/core/tx/tiering/external_data.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/tiering/fetcher/watch.h>

#include <ydb/library/table_creator/table_creator.h>
#include <ydb/services/metadata/secret/fetcher.h>

namespace NKikimr::NColumnShard {

class TTiersManager::TActor: public TActorBootstrapped<TTiersManager::TActor> {
private:
    std::shared_ptr<TTiersManager> Owner;
    NMetadata::NFetcher::ISnapshotsFetcher::TPtr SecretsFetcher;
    NMetadata::NFetcher::ISnapshotsFetcher::TPtr TiersFetcher;
    TActorId SchemeTieringFetcher;

    std::shared_ptr<NMetadata::NSecret::TSnapshot> SecretsSnapshot;
    std::shared_ptr<NTiers::TTiersSnapshot> TiersSnapshot;
    THashMap<TString, NTiers::TTieringRule> TieringRules;

private:
    TActorId GetExternalDataActorId() const {
        return NMetadata::NProvider::MakeServiceId(SelfId().NodeId());
    }

    void UpdateSnapshot() const {
        Owner->TakeConfigs(TiersSnapshot, TieringRules, SecretsSnapshot);
    }

    void ScheduleRetryWatchObjects(std::unique_ptr<NTiers::TEvWatchTieringRules> ev) const {
        constexpr static const TDuration RetryInterval = TDuration::Seconds(1);
        ActorContext().Schedule(RetryInterval, std::make_unique<IEventHandle>(SelfId(), SchemeTieringFetcher, ev.release()));
    }

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NMetadata::NProvider::TEvRefreshSubscriberData, Handle);
            hFunc(NActors::TEvents::TEvPoison, Handle);
            hFunc(NTiers::TEvNotifyTieringRuleUpdated, Handle);
            hFunc(NTiers::TEvNotifyTieringRuleDeleted, Handle);
            hFunc(NTiers::TEvTieringRuleResolutionFailed, Handle);
            default:
                break;
        }
    }

    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
        auto snapshot = ev->Get()->GetSnapshot();
        if (auto secrets = std::dynamic_pointer_cast<NMetadata::NSecret::TSnapshot>(snapshot)) {
            AFL_DEBUG(NKikimrServices::TX_TIERING)("event", "TEvRefreshSubscriberData")("snapshot", "secrets");
            SecretsSnapshot = secrets;
        } else if (auto tiers = std::dynamic_pointer_cast<NTiers::TTiersSnapshot>(snapshot)) {
            AFL_DEBUG(NKikimrServices::TX_TIERING)("event", "TEvRefreshSubscriberData")("snapshot", "tiers");
            TiersSnapshot = tiers;
        } else {
            Y_ABORT_UNLESS(false, "unexpected behaviour");
        }
        UpdateSnapshot();
    }

    void Handle(NActors::TEvents::TEvPoison::TPtr& /*ev*/) {
        Send(GetExternalDataActorId(), new NMetadata::NProvider::TEvUnsubscribeExternal(SecretsFetcher));
        Send(GetExternalDataActorId(), new NMetadata::NProvider::TEvUnsubscribeExternal(TiersFetcher));
        PassAway();
    }

    void Handle(NTiers::TEvNotifyTieringRuleUpdated::TPtr& ev) {
        TieringRules.emplace(ev->Get()->GetId(), ev->Get()->GetConfig());
        UpdateSnapshot();
    }

    void Handle(NTiers::TEvNotifyTieringRuleDeleted::TPtr& ev) {
        TieringRules.erase(ev->Get()->GetName());
        UpdateSnapshot();
    }

    void Handle(NTiers::TEvTieringRuleResolutionFailed::TPtr& ev) {
        const TString name = ev->Get()->GetName();
        switch (ev->Get()->GetReason()) {
            case NTiers::TBaseEvObjectResolutionFailed::NOT_FOUND:
                AFL_WARN(NKikimrServices::TX_TIERING)("event", "object_not_found")("name", name)("type", "TIERING_RULE");
                break;
            case NTiers::TBaseEvObjectResolutionFailed::LOOKUP_ERROR:
                ScheduleRetryWatchObjects(std::make_unique<NTiers::TEvWatchTieringRules>(std::vector<TString>({ name })));
                break;
            case NTiers::TBaseEvObjectResolutionFailed::UNEXPECTED_KIND:
                AFL_VERIFY(false)("name", name);
        }
    }

public:
    TActor(std::shared_ptr<TTiersManager> owner)
        : Owner(owner)
        , SecretsFetcher(std::make_shared<NMetadata::NSecret::TSnapshotsFetcher>())
        , TiersFetcher(std::make_shared<NTiers::TTierSnapshotConstructor>())
    {
    }

    void Bootstrap() {
        SchemeTieringFetcher = Register(new TTieringFetcher(SelfId()));
        Become(&TThis::StateMain);
        AFL_INFO(NKikimrServices::TX_TIERING)("event", "start_subscribing_metadata");
        Send(GetExternalDataActorId(), new NMetadata::NProvider::TEvSubscribeExternal(SecretsFetcher));
        Send(GetExternalDataActorId(), new NMetadata::NProvider::TEvSubscribeExternal(TiersFetcher));
    }

    void WatchTieringRules(std::vector<TString> names) {
        Send(SchemeTieringFetcher, new NTiers::TEvWatchTieringRules(std::move(names)));
    }

    ~TActor() {
        Owner->Stop(false);
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

void TTiersManager::TakeConfigs(
    std::shared_ptr<NTiers::TTiersSnapshot> tiers, TTieringRules tieringRules, std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets) {
    ALS_INFO(NKikimrServices::TX_TIERING) << "Take configs: " << (tiers ? " tiers" : "") << " snapshots" << (secrets ? " secrets" : "")
                                          << " at tablet " << TabletId;

    Tiers = tiers;
    TieringRules = tieringRules;
    Secrets = secrets;

    OnConfigsUpdated();
}

void TTiersManager::OnConfigsUpdated() {
    for (auto itSelf = Managers.begin(); itSelf != Managers.end(); ) {
        auto it = Tiers->GetTierConfigs().find(itSelf->first);
        if (it == Tiers->GetTierConfigs().end()) {
            itSelf->second.Stop();
            itSelf = Managers.erase(itSelf);
        } else {
            itSelf->second.Restart(it->second, Secrets);
            ++itSelf;
        }
    }
    for (auto&& i : Tiers->GetTierConfigs()) {
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

    AFL_DEBUG(NKikimrServices::TX_TIERING)("event", "configs_updated")("configs", DebugString())("has_complete_data", HasCompleteData);
}

bool TTiersManager::ValidateDependencies() const {
    for (const auto& [id, config] : TieringRules) {
        for (const auto& interval : config.GetIntervals()) {
            if (!Tiers || !Tiers->GetTierConfigs().contains(interval.GetTierName())) {
                return false;
            }
        }
    }
    for (const auto& [pathId, tieringId] : PathIdTiering) {
        if (!TieringRules.contains(tieringId)) {
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
    auto& tierConfigs = Tiers->GetTierConfigs();
    for (auto&& i : PathIdTiering) {
        auto* tieringRule = TieringRules.FindPtr(i.second);
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
    if (!TieringRules.contains(tieringId)) {
        Actor->WatchTieringRules({tieringId});
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

TString TTiersManager::DebugString() {
    TStringBuilder sb;
    sb << "TIERS=";
    if (Tiers) {
        sb << "{";
        for (const auto& [name, config] : Tiers->GetTierConfigs()) {
            sb << name << ";";
        }
        sb << "}";
    }
    sb << ";TIERING_RULES=";
    {
        sb << "{";
        for (const auto& [name, config] : TieringRules) {
            sb << name << ";";
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
    sb << Endl;
    return sb;
}
}
