#include "logging.h"
#include "private_events.h"
#include "secret_resolver.h"

#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_actors.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/services/metadata/secret/fetcher.h>
#include <ydb/services/metadata/secret/secret.h>
#include <ydb/services/metadata/secret/snapshot.h>
#include <ydb/services/metadata/service.h>

#include <util/generic/ptr.h>

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
        if (NKqp::UseSchemaSecrets(AppData()->FeatureFlags, SecretId.GetSecretId())) {
            const TVector<TString> secretNames{SecretId.GetSecretId()};
            auto userToken = MakeIntrusiveConst<NACLib::TUserToken>(entry.SecurityObject->GetOwnerSID(), TVector<TString>());
            const auto actorSystem = ActorContext().ActorSystem();
            const auto replyActorId = SelfId();
            auto future = NKqp::DescribeSecret(secretNames, userToken, Database, actorSystem);
            future.Subscribe([actorSystem, replyActorId](const NThreading::TFuture<NKqp::TEvDescribeSecretsResponse::TDescription>& result) {
                actorSystem->Send(replyActorId, new NKqp::TEvDescribeSecretsResponse(result.GetValue()));
            });
            return;
        } else {
            Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()),
                new NMetadata::NProvider::TEvAskSnapshot(SnapshotFetcher()));
        }
    }

    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
        const auto* snapshot = ev->Get()->GetSnapshotAs<NMetadata::NSecret::TSnapshot>();

        auto secretValue = snapshot->GetSecretValue(NMetadata::NSecret::TSecretIdOrValue::BuildAsId(SecretId));
        if (secretValue.IsFail()) {
            return Reply(false, secretValue.GetErrorMessage());
        }

        Reply(secretValue.DetachResult());
    }

    void Handle(NKqp::TEvDescribeSecretsResponse::TPtr& ev) {
        if (ev->Get()->Description.Status != Ydb::StatusIds::SUCCESS) {
            return Reply(false, ev->Get()->Description.Issues.ToOneLineString());
        }

        Y_ENSURE(ev->Get()->Description.SecretValues.size() == 1);
        Reply(ev->Get()->Description.SecretValues[0]);
    }

    template <typename... Args>
    void Reply(Args&&... args) {
        Send(Parent, new TEvPrivate::TEvResolveSecretResult(ReplicationId, std::forward<Args>(args)...), 0, Cookie);
        PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::REPLICATION_CONTROLLER_SECRET_RESOLVER;
    }

    explicit TSecretResolver(const TActorId& parent, ui64 rid, const TPathId& pathId, const TString& secretName, const ui64 cookie, const TString& database)
        : Parent(parent)
        , ReplicationId(rid)
        , PathId(pathId)
        , SecretName(secretName)
        , Cookie(cookie)
        , Database(database)
        , LogPrefix("SecretResolver", ReplicationId)
    {
    }

    void Bootstrap() {
        if (!NMetadata::NProvider::TServiceOperator::IsEnabled()) {
            return Reply(false, "Metadata service is not active");
        }

        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = Database;

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
            hFunc(NKqp::TEvDescribeSecretsResponse, Handle);
            sFunc(TEvents::TEvWakeup, Bootstrap);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const TActorId Parent;
    const ui64 ReplicationId;
    const TPathId PathId;
    const TString SecretName;
    const ui64 Cookie;
    const TString Database;
    const TActorLogPrefix LogPrefix;

    static constexpr auto RetryInterval = TDuration::Seconds(1);
    NMetadata::NSecret::TSecretId SecretId;

}; // TSecretResolver

IActor* CreateSecretResolver(const TActorId& parent, ui64 rid, const TPathId& pathId, const TString& secretName, const ui64 cookie, const TString& database) {
    return new TSecretResolver(parent, rid, pathId, secretName, cookie, database);
}

}
