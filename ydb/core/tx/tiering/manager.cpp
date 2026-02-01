#include "common.h"
#include "manager.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/tiering/fetcher.h>
#include <ydb/core/tx/tiering/tier/identifier.h>
#include <ydb/core/tx/tiering/tier/object.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

#include <ydb/library/table_creator/table_creator.h>
#include <ydb/services/metadata/secret/accessor/secret_id.h>
#include <ydb/services/metadata/secret/accessor/snapshot.h>
#include <ydb/services/metadata/secret/fetcher.h>
#include <ydb/services/metadata/service.h>

#include <library/cpp/retry/retry_policy.h>
#include <util/generic/overloaded.h>
#include <util/string/vector.h>

namespace NKikimr::NColumnShard {

namespace {

class TSchemaSecretsAccessor : public NMetadata::NSecret::ISecretAccessor {
public:
    explicit TSchemaSecretsAccessor(THashMap<TString, TString> pathToValue)
        : PathToValue(std::move(pathToValue)) {
    }

    virtual ~TSchemaSecretsAccessor() = default;

    bool CheckSecretAccess(const NMetadata::NSecret::TSecretIdOrValue&, const NACLib::TUserToken&) const override {
        return true;
    }

    bool PatchString(TString& stringForPath) const override {
        auto idOrValue = NMetadata::NSecret::TSecretIdOrValue::DeserializeFromString(stringForPath);
        if (!idOrValue) {
            return false;
        }
        if (auto value = GetSecretValue(*idOrValue); value.IsSuccess()) {
            stringForPath = value.DetachResult();
            return true;
        }
        return false;
    }

    TConclusion<TString> GetSecretValue(const NMetadata::NSecret::TSecretIdOrValue& sId) const override {
        return std::visit(
            TOverloaded(
                [](std::monostate) -> TConclusion<TString> {
                    return TConclusionStatus::Fail("Empty secret id");
                },
                [this](const NMetadata::NSecret::TSecretName& name) -> TConclusion<TString> {
                    if (auto it = PathToValue.find(name.GetSecretId()); it != PathToValue.end()) {
                        return it->second;
                    }
                    return TConclusionStatus::Fail(TStringBuilder() << "Schema secret not resolved: " << name.GetSecretId());
                },
                [](const NMetadata::NSecret::TSecretId&) -> TConclusion<TString> {
                    return TConclusionStatus::Fail("Schema secrets use path-based resolution only");
                },
                [](const TString& value) -> TConclusion<TString> {
                    return value;
                }),
            sId.GetState());
    }

    std::vector<NMetadata::NSecret::TSecretId> GetSecretIds(const std::optional<NACLib::TUserToken>&, const TString&) const override {
        return {};
    }

private:
    THashMap<TString, TString> PathToValue;
};

} // namespace

class TTiersManager::TActor: public TActorBootstrapped<TTiersManager::TActor> {
private:
    using IRetryPolicy = IRetryPolicy<const NTiers::TEvSchemeObjectResolutionFailed::EReason>;

    struct TPendingTierSecrets {
        NTiers::TExternalStorageId TierId;
        NTiers::TTierConfig Config;
        TVector<TString> PathsOrder;
        THashMap<TString, TString> PathToValue;
        size_t ReceivedCount = 0;
        size_t TotalCount = 0;
    };

    std::shared_ptr<TTiersManager> Owner;
    IRetryPolicy::TPtr RetryPolicy;
    THashMap<NTiers::TExternalStorageId, IRetryPolicy::IRetryState::TPtr> RetryStateByObject;
    NMetadata::NFetcher::ISnapshotsFetcher::TPtr SecretsFetcher;
    TActorId TiersFetcher;
    ui64 NextResolveRequestId = 0;
    THashMap<ui64, TPendingTierSecrets> PendingTierSecrets;

private:
    TActorId GetExternalDataActorId() const {
        return NMetadata::NProvider::MakeServiceId(SelfId().NodeId());
    }

    void RetryTierRequest(const NTiers::TExternalStorageId& tier, const NTiers::TEvSchemeObjectResolutionFailed::EReason reason) {
        AFL_DEBUG(NKikimrServices::TX_TIERING)("component", "tiers_manager")("event", "retry_watch_objects");
        auto findRetryState = RetryStateByObject.find(tier);
        if (!findRetryState) {
            findRetryState = RetryStateByObject.emplace(tier, RetryPolicy->CreateRetryState()).first;
        }
        auto retryDelay = findRetryState->second->GetNextRetryDelay(reason);
        AFL_VERIFY(retryDelay)("object", tier.GetConfigPath());
        ActorContext().Schedule(*retryDelay, std::make_unique<IEventHandle>(SelfId(), TiersFetcher, new NTiers::TEvWatchSchemeObject(std::vector<TString>({ tier.GetConfigPath() }))));
    }

    void ResetRetryState(const NTiers::TExternalStorageId& tier) {
        RetryStateByObject.erase(tier);
    }

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NMetadata::NProvider::TEvRefreshSubscriberData, Handle);
            hFunc(NActors::TEvents::TEvPoison, Handle);
            hFunc(NTiers::TEvNotifySchemeObjectUpdated, Handle);
            hFunc(NTiers::TEvNotifySchemeObjectDeleted, Handle);
            hFunc(NTiers::TEvSchemeObjectResolutionFailed, Handle);
            hFunc(NTiers::TEvWatchSchemeObject, Handle);
            hFunc(NTiers::TEvResolveTierSecrets, Handle);
            hFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            default:
                break;
        }
    }

    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
        auto snapshot = ev->Get()->GetSnapshot();
        if (auto secrets = std::dynamic_pointer_cast<NMetadata::NSecret::TSnapshot>(snapshot)) {
            AFL_DEBUG(NKikimrServices::TX_TIERING)("event", "TEvRefreshSubscriberData")("snapshot", "secrets");
            Owner->UpdateSecretsSnapshot(secrets);
            TVector<TString> requestedPaths = Owner->GetRequestedTierConfigPaths();
            if (!requestedPaths.empty()) {
                Send(TiersFetcher, new NTiers::TEvWatchSchemeObject(std::move(requestedPaths)));
            }
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
        const NTiers::TExternalStorageId tierId(ev->Get()->GetObjectPath());
        const auto& description = ev->Get()->GetDescription();
        ResetRetryState(tierId);
        if (description.GetSelf().GetPathType() == NKikimrSchemeOp::EPathTypeExternalDataSource) {
            NTiers::TTierConfig tier;
            if (HasAppData() && AppDataVerified().ColumnShardConfig.HasS3Client()) {
                tier = NTiers::TTierConfig(AppDataVerified().ColumnShardConfig.GetS3Client());
            }

            if (const auto status = tier.DeserializeFromProto(description.GetExternalDataSourceDescription()); status.IsFail()) {
                AFL_WARN(NKikimrServices::TX_TIERING)("event", "fetched_invalid_tier_settings")("error", status.GetErrorMessage());
                Owner->UpdateTierConfig(std::nullopt, tierId);
                return;
            }

            Owner->UpdateTierConfig(tier, tierId);
        } else {
            AFL_WARN(NKikimrServices::TX_TIERING)("error", "invalid_object_type")("type", static_cast<ui64>(description.GetSelf().GetPathType()))("path", tierId.GetConfigPath());
            Owner->UpdateTierConfig(std::nullopt, tierId);
        }
    }

    void Handle(NTiers::TEvNotifySchemeObjectDeleted::TPtr& ev) {
        AFL_DEBUG(NKikimrServices::TX_TIERING)("component", "tiering_manager")("event", "object_deleted")("name", ev->Get()->GetObjectPath());
        Owner->UpdateTierConfig(std::nullopt, ev->Get()->GetObjectPath());
    }

    void Handle(NTiers::TEvSchemeObjectResolutionFailed::TPtr& ev) {
        const TString objectPath = ev->Get()->GetObjectPath();
        AFL_WARN(NKikimrServices::TX_TIERING)("event", "object_resolution_failed")("path", objectPath)(
            "reason", static_cast<ui64>(ev->Get()->GetReason()));
        switch (ev->Get()->GetReason()) {
            case NTiers::TEvSchemeObjectResolutionFailed::NOT_FOUND:
                Owner->UpdateTierConfig(std::nullopt, objectPath);
                break;
            case NTiers::TEvSchemeObjectResolutionFailed::LOOKUP_ERROR:
                break;
        }
        RetryTierRequest(objectPath, ev->Get()->GetReason());
    }

    void Handle(NTiers::TEvWatchSchemeObject::TPtr& ev) {
        Send(TiersFetcher, ev->Release());
    }

    void Handle(NTiers::TEvResolveTierSecrets::TPtr& ev) {
        const auto& tierId = ev->Get()->GetTierId();
        const auto& config = ev->Get()->GetConfig();
        TVector<TString> pathsToResolve = config.GetSchemaSecretPaths();
        if (pathsToResolve.empty()) {
            auto accessor = std::make_shared<TSchemaSecretsAccessor>(THashMap<TString, TString>{});
            Owner->OnTierSecretsResolved(tierId, config, accessor);
            return;
        }
        const ui64 requestId = NextResolveRequestId++;
        TPendingTierSecrets pending;
        pending.TierId = tierId;
        pending.Config = config;
        pending.PathsOrder = pathsToResolve;
        pending.TotalCount = pathsToResolve.size();
        PendingTierSecrets[requestId] = std::move(pending);
        const TString databaseName = HasAppData() ? AppDataVerified().TenantName : TString{};
        Y_ABORT_UNLESS(databaseName, "Database name is required for schema secret resolution");
        for (size_t i = 0; i < pathsToResolve.size(); ++i) {
            auto navigateRequest = MakeHolder<TEvTxUserProxy::TEvNavigate>();
            navigateRequest->Record.SetDatabaseName(databaseName);
            auto* describePath = navigateRequest->Record.MutableDescribePath();
            describePath->SetPath(pathsToResolve[i]);
            describePath->MutableOptions()->SetReturnSecretValue(true);
            const ui64 cookie = (requestId << 16) | i;
            Send(MakeTxProxyID(), navigateRequest.Release(), 0, cookie);
        }
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        const ui64 cookie = ev->Cookie;
        const ui64 requestId = cookie >> 16;
        const size_t pathIndex = cookie & 0xFFFF;
        auto it = PendingTierSecrets.find(requestId);
        if (it == PendingTierSecrets.end()) {
            return;
        }
        const auto& rec = ev->Get()->GetRecord();
        if (rec.GetStatus() != NKikimrScheme::EStatus::StatusSuccess) {
            AFL_WARN(NKikimrServices::TX_TIERING)("event", "schema_secret_resolution_failed")("path", rec.GetPath())("status", (ui64)rec.GetStatus());
            PendingTierSecrets.erase(it);
            return;
        }
        const TString& secretPath = pathIndex < it->second.PathsOrder.size() ? it->second.PathsOrder[pathIndex] : rec.GetPath();
        const TString secretValue = rec.GetPathDescription().GetSecretDescription().GetValue();
        it->second.PathToValue[secretPath] = secretValue;
        ++it->second.ReceivedCount;
        if (it->second.ReceivedCount >= it->second.TotalCount) {
            auto accessor = std::make_shared<TSchemaSecretsAccessor>(std::move(it->second.PathToValue));
            Owner->OnTierSecretsResolved(it->second.TierId, it->second.Config, accessor);
            PendingTierSecrets.erase(it);
        }
    }

public:
    TActor(std::shared_ptr<TTiersManager> owner)
        : Owner(owner)
        , RetryPolicy(IRetryPolicy::GetExponentialBackoffPolicy(
              [](const NTiers::TEvSchemeObjectResolutionFailed::EReason reason) {
                  switch (reason) {
                      case NTiers::TEvSchemeObjectResolutionFailed::NOT_FOUND:
                          return ERetryErrorClass::LongRetry;
                      case NTiers::TEvSchemeObjectResolutionFailed::LOOKUP_ERROR:
                          return ERetryErrorClass::ShortRetry;
                  }
              }, TDuration::MilliSeconds(10), TDuration::Seconds(29), TDuration::Seconds(30), 10000))
        , SecretsFetcher(std::make_shared<NMetadata::NSecret::TSnapshotsFetcher>())
    {
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

bool TManager::Restart(const TTierConfig& config, std::shared_ptr<NMetadata::NSecret::ISecretAccessor> secrets) {
    ALS_DEBUG(NKikimrServices::TX_TIERING) << "Restarting tier '" << TierId << "' at tablet " << TabletId;
    Stop();
    return Start(config, secrets);
}

bool TManager::Stop() {
    S3Settings.reset();
    ALS_DEBUG(NKikimrServices::TX_TIERING) << "Tier '" << TierId << "' stopped at tablet " << TabletId;
    return true;
}

bool TManager::Start(const TTierConfig& config, std::shared_ptr<NMetadata::NSecret::ISecretAccessor> secrets) {
    AFL_VERIFY(!S3Settings)("tier", TierId)("event", "already started");
    auto patchedConfig = config.GetPatchedConfig(secrets);
    if (patchedConfig.IsFail()) {
        AFL_ERROR(NKikimrServices::TX_TIERING)("error", "cannot_read_secrets")("reason", patchedConfig.GetErrorMessage());
        return false;
    }
    S3Settings = patchedConfig.DetachResult();
    ALS_DEBUG(NKikimrServices::TX_TIERING) << "Tier '" << TierId << "' started at tablet " << TabletId;
    return true;
}

TManager::TManager(const ui64 tabletId, const NActors::TActorId& tabletActorId, const TExternalStorageId& tierName)
    : TabletId(tabletId)
    , TabletActorId(tabletActorId)
    , TierId(tierName) {
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

void TTiersManager::OnConfigsUpdated(bool notifyShard) {
    for (auto& [tierId, manager] : Managers) {
        auto* findTier = Tiers.FindPtr(tierId);
        AFL_VERIFY(findTier)("id", tierId);
        if (findTier->HasConfig()) {
            bool started = false;
            if (Secrets) {
                if (manager.IsReady()) {
                    started = manager.Restart(findTier->GetConfigVerified(), Secrets);
                } else {
                    started = manager.Start(findTier->GetConfigVerified(), Secrets);
                }
            }
            if (!started && Actor && TlsActivationContext) {
                TActivationContext::AsActorContext().Send(Actor->SelfId(), new NTiers::TEvResolveTierSecrets(tierId, findTier->GetConfigVerified()));
            } else if (!started) {
                AFL_DEBUG(NKikimrServices::TX_TIERING)("event", "skip_tier_manager_no_actor")("tier", tierId);
            }
        } else {
            AFL_DEBUG(NKikimrServices::TX_TIERING)("event", "skip_tier_manager_reloading")("tier", tierId)("has_secrets", !!Secrets)(
                "found_tier_config", !!findTier);
        }
    }

    if (notifyShard && ShardCallback && TlsActivationContext) {
        ShardCallback(TActivationContext::AsActorContext());
    }

    AFL_DEBUG(NKikimrServices::TX_TIERING)("event", "configs_updated")("configs", DebugString());
}

void TTiersManager::RegisterTierManager(const NTiers::TExternalStorageId& tierId, std::optional<NTiers::TTierConfig> config) {
    auto emplaced = Managers.emplace(tierId, NTiers::TManager(TabletId, TabletActorId, tierId));
    AFL_VERIFY(emplaced.second);

    if (config) {
        bool started = false;
        if (Secrets) {
            started = emplaced.first->second.Start(*config, Secrets);
        }
        if (!started && Actor && TlsActivationContext) {
            TActivationContext::AsActorContext().Send(Actor->SelfId(), new NTiers::TEvResolveTierSecrets(tierId, *config));
        } else if (!started) {
            AFL_DEBUG(NKikimrServices::TX_TIERING)("event", "skip_tier_manager_start")("tier", tierId)("has_secrets", !!Secrets)(
                "tier_config", !!config);
        }
    } else {
        AFL_DEBUG(NKikimrServices::TX_TIERING)("event", "skip_tier_manager_start")("tier", tierId)("has_secrets", !!Secrets)(
            "tier_config", !!config);
    }
}

void TTiersManager::UnregisterTierManager(const NTiers::TExternalStorageId& tierId) {
    auto findManager = Managers.find(tierId);
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

const NTiers::TManager* TTiersManager::GetManagerOptional(const NTiers::TExternalStorageId& tierId) const {
    auto it = Managers.find(tierId);
    if (it != Managers.end()) {
        return &it->second;
    } else {
        return nullptr;
    }
}

void TTiersManager::ActivateTiers(const THashSet<NTiers::TExternalStorageId>& usedTiers, const bool resubscribeToConfig) {
    AFL_VERIFY(Actor)("error", "tiers_manager_is_not_started");
    for (const NTiers::TExternalStorageId& tierId : usedTiers) {
        auto findTier = Tiers.find(tierId);
        const bool newTier = (findTier == Tiers.end());
        if (findTier == Tiers.end()) {
            findTier = Tiers.emplace(tierId, TTierGuard(tierId, this)).first;
        }
        if (newTier || (findTier->second.GetState() != TTiersManager::ETierState::AVAILABLE && resubscribeToConfig)) {
            const auto& actorContext = NActors::TActivationContext::AsActorContext();
            AFL_VERIFY(&actorContext)("error", "no_actor_context");
            actorContext.Send(Actor->SelfId(), new NTiers::TEvWatchSchemeObject({ tierId.GetConfigPath() }));
        }
    }
    OnConfigsUpdated(false);
}

void TTiersManager::UpdateSecretsSnapshot(std::shared_ptr<NMetadata::NSecret::ISecretAccessor> secrets) {
    AFL_INFO(NKikimrServices::TX_TIERING)("event", "update_secrets")("tablet", TabletId);
    AFL_VERIFY(secrets);
    Secrets = secrets;
    OnConfigsUpdated();
}

void TTiersManager::OnTierSecretsResolved(const NTiers::TExternalStorageId& tierId, const NTiers::TTierConfig& config, std::shared_ptr<NMetadata::NSecret::ISecretAccessor> accessor) {
    auto it = Managers.find(tierId);
    if (it == Managers.end()) {
        return;
    }
    if (it->second.IsReady()) {
        it->second.Restart(config, accessor);
    } else {
        it->second.Start(config, accessor);
    }
    if (ShardCallback && TlsActivationContext) {
        ShardCallback(TActivationContext::AsActorContext());
    }
}

void TTiersManager::UpdateTierConfig(
    std::optional<NTiers::TTierConfig> config, const NTiers::TExternalStorageId& tierId, const bool notifyShard) {
    AFL_INFO(NKikimrServices::TX_TIERING)("event", "update_tier_config")("name", tierId.ToString())("tablet", TabletId)("has_config", !!config);
    TTierGuard* findTier = Tiers.FindPtr(tierId);
    AFL_VERIFY(findTier)("tier", tierId.ToString());
    if (config) {
        findTier->UpsertConfig(*config);
    } else {
        findTier->ResetConfig();
    }
    OnConfigsUpdated(notifyShard);
}

ui64 TTiersManager::GetAwaitedConfigsCount() const {
    ui64 count = 0;
    for (const auto& [id, tier] : Tiers) {
        if (tier.GetState() == ETierState::REQUESTED) {
            ++count;
        }
    }
    return count;
}

TVector<TString> TTiersManager::GetRequestedTierConfigPaths() const {
    TVector<TString> paths;
    for (auto&& [id, tier] : Tiers) {
        if (tier.GetState() == ETierState::REQUESTED) {
            paths.push_back(id.GetConfigPath());
        }
    }
    return paths;
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
    for (const auto& [id, tier] : Tiers) {
        sb << "{";
        sb << "id=" << id << ";";
        sb << "has_config=" << tier.HasConfig();
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
