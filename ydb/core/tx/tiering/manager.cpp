#include "common.h"
#include "manager.h"

#include <ydb/core/tx/columnshard/columnshard_private_events.h>

#include <ydb/library/table_creator/table_creator.h>
#include <ydb/services/metadata/secret/fetcher.h>

namespace NKikimr::NColumnShard {

class TTiersManager::TActor: public TActorBootstrapped<TTiersManager::TActor> {
private:
    enum ESchemeObject {
        UNKNOWN = 0,
        TIER = 1,
        TIERING = 2,
    };

    std::shared_ptr<TTiersManager> Owner;
    NMetadata::NFetcher::ISnapshotsFetcher::TPtr SecretsFetcher;
    std::shared_ptr<NMetadata::NSecret::TSnapshot> SecretsSnapshot;

    THashSet<TString> WatchedTiers;
    THashSet<TString> WatchedTierings;

private:
    template <typename T>
    static T DeserializeObject(const NKikimrSchemeOp::TAbstractObjectProperties& properties) {
        NMetadata::NInternal::TTableRecord record;
        AFL_VERIFY(record.DeserializeFromProto(properties.GetProperties()))("proto", properties.DebugString());

        const auto object = NMetadata::NModifications::TObjectManager<T>().DeserializeFromRecord(record);
        auto castObject = std::dynamic_pointer_cast<T>(object);
        AFL_VERIFY(castObject);

        return *castObject;
    }

    TActorId GetExternalDataActorId() const {
        return NMetadata::NProvider::MakeServiceId(SelfId().NodeId());
    }

    void InitializeTierings(const std::vector<TString>& tierings) {
        std::vector<TString> newTierings;
        for (const TString& tieringId : tierings) {
            if (WatchedTierings.emplace(tieringId).second) {
                newTierings.emplace_back(tieringId);
            }
        }
        RequestPaths(newTierings, {});
    }

    void InitializeTiers(const std::vector<TString>& tiers) {
        std::vector<TString> newTiers;
        for (const TString& tierName : tiers) {
            if (WatchedTiers.emplace(tierName).second) {
                newTiers.emplace_back(tierName);
            }
        }
        RequestPaths({}, newTiers);
    }

    void RequestPaths(const std::vector<TString>& tierings, const std::vector<TString>& tiers) {
        TVector<TVector<TString>> paths;
        {
            const TString storagePath = NTiers::TTieringRule::GetBehaviour()->GetStorageTablePath();
            for (const TString& tieringId : tierings) {
                paths.emplace_back(TVector<TString>({ storagePath, tieringId }));
            }
        }
        {
            const TString storagePath = NTiers::TTierConfig::GetBehaviour()->GetStorageTablePath();
            for (const TString& tierName : tiers) {
                paths.emplace_back(TVector<TString>({ storagePath, tierName }));
            }
        }

        auto event = NTableCreator::BuildSchemeCacheNavigateRequest(
            std::move(paths), AppData()->TenantName, MakeIntrusive<NACLib::TUserToken>(BUILTIN_ACL_METADATA, TVector<NACLib::TSID>{}));
        event->ResultSet[0].Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(event.Release()), IEventHandle::FlagTrackDelivery);
    }

    void OnPathFetched(const TPathId& pathId, const TString& objectType) {
        ESchemeObject type;
        if (objectType == NTiers::TTierConfig::GetTypeId()) {
            type = ESchemeObject::TIER;
        } else if (objectType == NTiers::TTieringRule::GetTypeId()) {
            type = ESchemeObject::TIERING;
        } else {
            Y_ABORT_S("object_type=" + objectType);
        }
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchPathId(pathId, static_cast<ui64>(type)), IEventHandle::FlagTrackDelivery);
    }

    void OnPathNotFound(const std::vector<TString>& path) {
        OnObjectResolutionFailure(path);
    }

    void OnLookupError(const std::vector<TString>& path) {
        OnObjectResolutionFailure(path);
    }

    void OnObjectResolutionFailure(const std::vector<TString>& path) {
        AFL_VERIFY(path.size() == 2)("size", path.size());
        const TString& storageDirectory = path[0];
        const TString& objectId = path[1];
        if (storageDirectory == NTiers::TTieringRule::GetBehaviour()->GetStorageTablePath()) {
            WatchedTierings.erase(objectId);
            Owner->OnTieringResolutionFailure(objectId);
        } else if (storageDirectory == NTiers::TTierConfig::GetBehaviour()->GetStorageTablePath()){
            WatchedTiers.erase(objectId);
            Owner->OnTierResolutionFailure(objectId);
        } else {
            AFL_VERIFY(false)("storage_dir", storageDirectory)("object_id", objectId);
        }
    }

    void OnTierFetched(const TString& tierName, NTiers::TTierConfig config) {
        Owner->TakeTierConfig(tierName, std::move(config));
    }

    void OnTieringFetched(const TString& tieringId, NTiers::TTieringRule config) {
        Owner->TakeTieringConfig(tieringId, std::move(config));
    }

    void OnTierDeleted(const TString& tierName) {
        Owner->EraseTier(tierName);
    }

    void OnTieringDeleted(const TString& tieringId) {
        Owner->EraseTiering(tieringId);
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
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvTxProxySchemeCache::TEvWatchNotifyUpdated, Handle);
            hFunc(TEvTxProxySchemeCache::TEvWatchNotifyDeleted, Handle);
            hFunc(TEvTxProxySchemeCache::TEvWatchNotifyUnavailable, Handle);
            hFunc(NTiers::TEvWatchTieringObjects, Handle);
            hFunc(NActors::TEvents::TEvPoison, Handle);
            hFunc(NActors::TEvents::TEvUndelivered, Handle);
            default:
                break;
        }
    }

    void Bootstrap() {
        Become(&TThis::StateMain);
        AFL_INFO(NKikimrServices::TX_TIERING)("event", "start_subscribing_metadata");
        Send(GetExternalDataActorId(), new NMetadata::NProvider::TEvSubscribeExternal(SecretsFetcher));
    }

    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
        auto snapshot = ev->Get()->GetSnapshot();
        if (auto secrets = std::dynamic_pointer_cast<NMetadata::NSecret::TSnapshot>(snapshot)) {
            AFL_DEBUG(NKikimrServices::TX_TIERING)("event", "TEvRefreshSubscriberData")("snapshot", "secrets");
            SecretsSnapshot = secrets;
            Owner->TakeSecretsConfig(SecretsSnapshot);
        } else {
            Y_ABORT_UNLESS(false, "unexpected behaviour");
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const NSchemeCache::TSchemeCacheNavigate* result = ev->Get()->Request.Get();
        for (auto entry : result->ResultSet) {
            switch (entry.Status) {
                case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
                    AFL_VERIFY(entry.Kind == NSchemeCache::TSchemeCacheNavigate::KindAbstractObject);
                    OnPathFetched(entry.TableId.PathId, entry.AbstractObjectInfo->Description.GetType());
                    break;

                case NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::RootUnknown:
                    OnPathNotFound(entry.Path);
                    break;

                case NSchemeCache::TSchemeCacheNavigate::EStatus::RedirectLookupError:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::LookupError:
                    OnLookupError(entry.Path);
                    break;

                case NSchemeCache::TSchemeCacheNavigate::EStatus::AccessDenied:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::PathNotTable:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::PathNotPath:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::TableCreationNotComplete:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::Unknown:
                    AFL_VERIFY(false)("entry", entry.ToString());
            }
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr& ev) {
        const auto& describeResult = *ev->Get()->Result;
        const auto& pathDescription = describeResult.GetPathDescription();

        AFL_VERIFY(pathDescription.GetSelf().GetPathType() == NKikimrSchemeOp::EPathTypeAbstractObject);
        AFL_VERIFY(pathDescription.HasAbstractObjectDescription());
        const auto& abstractObject = pathDescription.GetAbstractObjectDescription();
        const TString& typeId = abstractObject.GetType();
        const TString& objectId = abstractObject.GetName();

        if (typeId == NTiers::TTierConfig::GetTypeId()) {
            OnTierFetched(objectId, DeserializeObject<NTiers::TTierConfig>(abstractObject.GetProperties()));
        } else if (typeId == NTiers::TTieringRule::GetTypeId()) {
            OnTieringFetched(objectId, DeserializeObject<NTiers::TTieringRule>(abstractObject.GetProperties()));
        } else {
            AFL_VERIFY(false)("type_id", typeId);
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyDeleted::TPtr& ev) {
        const auto& record = ev->Get();
        const TString objectName = TString(ExtractBase(record->Path));
        switch (record->Key) {
            case ESchemeObject::TIER:
                OnTierDeleted(objectName);
                break;
            case ESchemeObject::TIERING:
                OnTieringDeleted(objectName);
                break;
            default:
                AFL_VERIFY(false)("key", record->Key);
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyUnavailable::TPtr& ev) {
        const auto& record = ev->Get();
        AFL_WARN(NKikimrServices::TX_TIERING)("event", "scheme object is unavailable")("path", record->Path);
    }

    void Handle(NTiers::TEvWatchTieringObjects::TPtr& ev) {
        if (const auto& tierings = ev->Get()->GetTierings(); !tierings.empty()) {
            InitializeTierings(tierings);
        }
        if (const auto& tiers = ev->Get()->GetTiers(); !tiers.empty()) {
            InitializeTiers(tiers);
        }
    }

    void Handle(NActors::TEvents::TEvPoison::TPtr& /*ev*/) {
        Send(GetExternalDataActorId(), new NMetadata::NProvider::TEvUnsubscribeExternal(SecretsFetcher));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove(static_cast<ui64>(ESchemeObject::TIER)));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove(static_cast<ui64>(ESchemeObject::TIERING)));
        PassAway();
    }

    void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
        AFL_ERROR(NKikimrServices::TX_TIERING)("event", "event undelivered to local service")("reason", ev->Get()->Reason);
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

void TTiersManager::TakeSecretsConfig(std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets) {
    ALS_INFO(NKikimrServices::TX_TIERING) << "Take secrets config at tablet " << TabletId;

    for (auto& [tierName, manager] : Managers) {
        auto tierConfig = Tiers.FindPtr(tierName);
        // TODO: fix possible data race (verify may be triggered)
        AFL_VERIFY(tierConfig);
        manager.Restart(*tierConfig, secrets);
    }

    Secrets = secrets;
    RefreshTieringOnShard();
}

bool TTiersManager::IsReady() const {
    for (const auto& [id, config] : Tierings) {
        if (!HasTieringDependencies(config)) {
            return false;
        }
    }
    for (const auto& [pathId, tieringId] : PathIdTiering) {
        if (!Tierings.contains(tieringId)) {
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
    AFL_VERIFY(IsReady());
    THashMap<ui64, NKikimr::NOlap::TTiering> result;
    for (auto&& i : PathIdTiering) {
        auto* tieringRule = Tierings.FindPtr(i.second);
        AFL_VERIFY(tieringRule);
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("path_id", i.first)("tiering_name", i.second)("event", "activation");
        NOlap::TTiering tiering = tieringRule->BuildOlapTiers();
        for (auto& [name, tier] : tiering.GetTierByName()) {
            AFL_VERIFY(name != NOlap::NTiering::NCommon::DeleteTierName);
            auto* tierConfig = Tiers.FindPtr(name);
            AFL_VERIFY(tierConfig);
            tier->SetSerializer(NTiers::ConvertCompression(tierConfig->GetCompression()));
        }
        result.emplace(i.first, std::move(tiering));
    }
    return result;
}

void TTiersManager::TakeTieringConfig(const TString& tieringId, NTiers::TTieringRule config) {
    ALS_DEBUG(NKikimrServices::TX_TIERING) << "Take config for tiering rule '" << tieringId << "' at tablet " << TabletId;

    {
        std::vector<TString> uninitializedTiers;
        for (const auto& interval : config.GetIntervals()) {
            if (!Tiers.contains(interval.GetTierName())) {
                uninitializedTiers.emplace_back(interval.GetTierName());
            }
        }
        if (!uninitializedTiers.empty()) {
            TActivationContext::AsActorContext().Send(
                Actor->SelfId(), new NTiers::TEvWatchTieringObjects({}, { std::move(uninitializedTiers) }));
        }
    }

    Tierings.emplace(tieringId, std::move(config));
    RefreshTieringOnShard();
}

void TTiersManager::TakeTierConfig(const TString& tierName, NTiers::TTierConfig config) {
    ALS_DEBUG(NKikimrServices::TX_TIERING) << "Take config for tier '" << tierName << "' at tablet " << TabletId;

    auto findManager = Managers.find(tierName);
    if (findManager != Managers.end()) {
        findManager->second.Restart(config, Secrets);
    } else {
        NTiers::TManager localManager(TabletId, TabletActorId, config);
        auto itManager = Managers.emplace(tierName, std::move(localManager)).first;
        itManager->second.Start(Secrets);
    }

    Tiers.emplace(tierName, std::move(config));
    RefreshTieringOnShard();
}

void TTiersManager::EraseTier(const TString& tierName) {
    Tiers.erase(tierName);
    if (auto findManager = Managers.find(tierName); findManager != Managers.end()) {
        findManager->second.Stop();
        Managers.erase(findManager);
    }
}

void TTiersManager::EraseTiering(const TString& tieringId) {
    Tierings.erase(tieringId);
}

void TTiersManager::OnTierResolutionFailure(const TString& tierName) {
    if (IsTierInUse(tierName)) {
        ScheduleRetryWatchObjects(std::make_unique<NTiers::TEvWatchTieringObjects>(std::vector<TString>(), std::vector<TString>({ tierName })));
    }
}

void TTiersManager::OnTieringResolutionFailure(const TString& tieringId) {
    if (IsTieringInUse(tieringId)) {
        ScheduleRetryWatchObjects(std::make_unique<NTiers::TEvWatchTieringObjects>(std::vector<TString>({ tieringId }), std::vector<TString>()));
    }
}

void TTiersManager::EnablePathId(const ui64 pathId, const TString& tieringId) {
    PathIdTiering.emplace(pathId, tieringId);
    if (!Tierings.contains(tieringId)) {
        TActivationContext::AsActorContext().Send(Actor->SelfId(), new NTiers::TEvWatchTieringObjects({ tieringId }, {}));
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

void TTiersManager::ScheduleRetryWatchObjects(std::unique_ptr<NTiers::TEvWatchTieringObjects> ev) const {
    constexpr static const TDuration RetryInterval = TDuration::Seconds(1);
    TActivationContext::AsActorContext().Schedule(RetryInterval,
        std::make_unique<IEventHandle>(TActivationContext::AsActorContext().SelfID, Actor->SelfId(), ev.release()));
}
}
