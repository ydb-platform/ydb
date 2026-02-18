#include "common.h"
#include "manager.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/federated_query/actors/kqp_federated_query_actors.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/tiering/fetcher.h>
#include <ydb/core/tx/tiering/tier/identifier.h>

#include <ydb/library/table_creator/table_creator.h>
#include <ydb/services/metadata/secret/fetcher.h>

#include <library/cpp/retry/retry_policy.h>
#include <util/string/vector.h>

namespace NKikimr::NColumnShard {

class TTiersManager::TActor: public TActorBootstrapped<TTiersManager::TActor> {
private:
    struct TEvPrivate {
        enum EEv {
            EvSchemaSecretsResolved = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

        struct TEvSchemaSecretsResolved : TEventLocal<TEvSchemaSecretsResolved, EvSchemaSecretsResolved> {
            NTiers::TExternalStorageId TierId;
            NTiers::TTierConfig TierConfig;
            NKqp::TEvDescribeSecretsResponse::TDescription Description;

            TEvSchemaSecretsResolved(
                NTiers::TExternalStorageId tierId,
                NTiers::TTierConfig tierConfig,
                NKqp::TEvDescribeSecretsResponse::TDescription description)
                : TierId(std::move(tierId))
                , TierConfig(std::move(tierConfig))
                , Description(std::move(description))
            {}
        };
    };

    using IRetryPolicy = IRetryPolicy<const NTiers::TEvSchemeObjectResolutionFailed::EReason>;

    std::shared_ptr<TTiersManager> Owner;
    IRetryPolicy::TPtr RetryPolicy;
    THashMap<NTiers::TExternalStorageId, IRetryPolicy::IRetryState::TPtr> RetryStateByObject;
    NMetadata::NFetcher::ISnapshotsFetcher::TPtr SecretsFetcher;
    TActorId TiersFetcher;

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
            hFunc(TEvPrivate::TEvSchemaSecretsResolved, Handle);
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

            const auto& auth = description.GetExternalDataSourceDescription().GetAuth();
            if (auth.identity_case() == NKikimrSchemeOp::TAuth::kAws) {
                const auto& aws = auth.GetAws();
                TVector<TString> schemaSecretNames = {
                    aws.GetAwsAccessKeyIdSecretName(),
                    aws.GetAwsSecretAccessKeySecretName()
                };

                if (NKqp::UseSchemaSecrets(AppDataVerified().FeatureFlags, schemaSecretNames)) {
                    auto userToken = MakeIntrusive<NACLib::TUserToken>(BUILTIN_ACL_METADATA, TVector<NACLib::TSID>{});

                    auto future = NKqp::DescribeSecret(
                        schemaSecretNames,
                        userToken,
                        AppDataVerified().TenantName,
                        ActorContext().ActorSystem());

                        const auto selfId = SelfId();
                    const auto tierIdCopy = tierId;

                    future.Subscribe([actorSystem = ActorContext().ActorSystem(),
                                      selfId,
                                      tierIdCopy,
                                      tier](const NThreading::TFuture<NKqp::TEvDescribeSecretsResponse::TDescription>& result) {
                        actorSystem->Send(selfId, new TEvPrivate::TEvSchemaSecretsResolved(tierIdCopy, tier, result.GetValue()));
                    });

                    return;
                }
            } else {
                AFL_WARN(NKikimrServices::TX_TIERING)("event", "unsupported_auth_for_tiering")("path", tierId.GetConfigPath())("identity_case", static_cast<int>(auth.identity_case()));
            }

            Owner->UpdateTierConfig(tier, tierId);
        } else {
            AFL_WARN(NKikimrServices::TX_TIERING)("error", "invalid_object_type")("type", static_cast<ui64>(description.GetSelf().GetPathType()))("path", tierId.GetConfigPath());
            Owner->UpdateTierConfig(std::nullopt, tierId);
        }
    }

    void Handle(TEvPrivate::TEvSchemaSecretsResolved::TPtr& ev) {
        if (ev->Get()->Description.Status != Ydb::StatusIds::SUCCESS) {
            AFL_ERROR(NKikimrServices::TX_TIERING)(
                "event", "cannot_read_schema_secrets")("tier", ev->Get()->TierId.GetConfigPath())(
                "reason", ev->Get()->Description.Issues.ToOneLineString());
            Owner->UpdateTierConfig(std::nullopt, ev->Get()->TierId);
            return;
        }

        if (ev->Get()->Description.SecretValues.size() != 2) {
            AFL_ERROR(NKikimrServices::TX_TIERING)(
                "event", "cannot_read_schema_secrets")("tier", ev->Get()->TierId.GetConfigPath())(
                "reason", TStringBuilder() << "expected 2 secrets, got " << ev->Get()->Description.SecretValues.size());
            Owner->UpdateTierConfig(std::nullopt, ev->Get()->TierId);
            return;
        }

        Owner->UpdateTierConfig(
            ev->Get()->TierConfig.BuildWithPatchedSecrets(
                ev->Get()->Description.SecretValues[0],
                ev->Get()->Description.SecretValues[1]),
            ev->Get()->TierId);
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

TManager& TManager::Restart(const TTierConfig& config, std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets) {
    ALS_DEBUG(NKikimrServices::TX_TIERING) << "Restarting tier '" << TierId << "' at tablet " << TabletId;
    Stop();
    Start(config, secrets);
    return *this;
}

bool TManager::Stop() {
    S3Settings.reset();
    ALS_DEBUG(NKikimrServices::TX_TIERING) << "Tier '" << TierId << "' stopped at tablet " << TabletId;
    return true;
}

bool TManager::Start(const TTierConfig& config, std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets) {
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
        if (Secrets && findTier->HasConfig()) {
            if (manager.IsReady()) {
                manager.Restart(findTier->GetConfigVerified(), Secrets);
            } else {
                manager.Start(findTier->GetConfigVerified(), Secrets);
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

    if (Secrets && config) {
        emplaced.first->second.Start(*config, Secrets);
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

void TTiersManager::UpdateSecretsSnapshot(std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets) {
    AFL_INFO(NKikimrServices::TX_TIERING)("event", "update_secrets")("tablet", TabletId);
    AFL_VERIFY(secrets);
    Secrets = secrets;
    OnConfigsUpdated();
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
