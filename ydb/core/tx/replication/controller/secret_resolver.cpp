#include "logging.h"
#include "private_events.h"
#include "secret_resolver.h"

#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/services/metadata/secret/fetcher.h>
#include <ydb/services/metadata/secret/secret.h>
#include <ydb/services/metadata/secret/snapshot.h>
#include <ydb/services/metadata/service.h>

namespace NKikimr::NReplication::NController {

class TSecretResolver: public TActorBootstrapped<TSecretResolver> {
    static NMetadata::NFetcher::ISnapshotsFetcher::TPtr SnapshotFetcher() {
        return std::make_shared<NMetadata::NSecret::TSnapshotsFetcher>();
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto* response = ev->Get()->Request.Get();

        Y_ABORT_UNLESS(response->ResultSet.size() == 1);
        const auto& entry = response->ResultSet.front();

        LOG_T("Handle " << ev->Get()->ToString()
            << ": entry# " << entry.ToString());

        switch (entry.Status) {
        case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
            break;
        default:
            LOG_W("Unexpected status"
                << ": entry# " << entry.ToString());
            return Schedule(RetryInterval, new TEvents::TEvWakeup);
        }

        if (!entry.SecurityObject) {
            return Reply(false, "Empty security object");
        }

        SecretId = NMetadata::NSecret::TSecretId(entry.SecurityObject->GetOwnerSID(), SecretName);
        Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()),
            new NMetadata::NProvider::TEvAskSnapshot(SnapshotFetcher()));
    }

    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
        const auto* snapshot = ev->Get()->GetSnapshotAs<NMetadata::NSecret::TSnapshot>();

        TString secretValue;
        if (!snapshot->GetSecretValue(NMetadata::NSecret::TSecretIdOrValue::BuildAsId(SecretId), secretValue)) {
            return Reply(false, TStringBuilder() << "Secret '" << SecretName << "' not found");
        }

        Reply(secretValue);
    }

    template <typename... Args>
    void Reply(Args&&... args) {
        Send(Parent, new TEvPrivate::TEvResolveSecretResult(ReplicationId, std::forward<Args>(args)...));
        PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::REPLICATION_CONTROLLER_SECRET_RESOLVER;
    }

    explicit TSecretResolver(const TActorId& parent, ui64 rid, const TPathId& pathId, const TString& secretName)
        : Parent(parent)
        , ReplicationId(rid)
        , PathId(pathId)
        , SecretName(secretName)
        , LogPrefix("SecretResolver", ReplicationId)
    {
    }

    void Bootstrap() {
        if (!NMetadata::NProvider::TServiceOperator::IsEnabled()) {
            return Reply(false, "Metadata service is not active");
        }

        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();

        auto& entry = request->ResultSet.emplace_back();
        entry.TableId = PathId;
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        entry.RedirectRequired = false;

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(NMetadata::NProvider::TEvRefreshSubscriberData, Handle);
            sFunc(TEvents::TEvWakeup, Bootstrap);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const TActorId Parent;
    const ui64 ReplicationId;
    const TPathId PathId;
    const TString SecretName;
    const TActorLogPrefix LogPrefix;

    static constexpr auto RetryInterval = TDuration::Seconds(1);
    NMetadata::NSecret::TSecretId SecretId;

}; // TSecretResolver

IActor* CreateSecretResolver(const TActorId& parent, ui64 rid, const TPathId& pathId, const TString& secretName) {
    return new TSecretResolver(parent, rid, pathId, secretName);
}

}
