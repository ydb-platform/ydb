#include "logging.h"
#include "private_events.h"
#include "secret_resolver.h"

#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/services/scheme_secret/resolver.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/services/metadata/secret/fetcher.h>
#include <ydb/services/metadata/secret/secret.h>
#include <ydb/services/metadata/secret/snapshot.h>
#include <ydb/services/metadata/service.h>

#include <util/generic/ptr.h>
#include <library/cpp/openssl/crypto/sha.h>
#include <util/string/hex.h>
#include <util/string/cast.h>
#include <util/generic/scope.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/library/actors/async/sleep.h>
#include <ydb/library/actors/async/timeout.h>
#include <ydb/library/actors/async/wait_for_event.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

class TSecretResolver: public TActorBootstrapped<TSecretResolver> {
    static NMetadata::NFetcher::ISnapshotsFetcher::TPtr SnapshotFetcher() {
        return std::make_shared<NMetadata::NSecret::TSnapshotsFetcher>();
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto* response = ev->Get()->Request.Get();

        Y_ABORT_UNLESS(response->ResultSet.size() == 1);
        const auto& entry = response->ResultSet.front();

        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()},
            {"entry", entry});

        switch (entry.Status) {
        case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
            break;
        default:
            YDB_LOG_WARN("Unexpected status",
                {"entry", entry});
            return Schedule(RetryInterval, new TEvents::TEvWakeup);
        }

        if (!entry.SecurityObject) {
            return Reply(false, "Empty security object");
        }

        const auto& userSID = entry.SecurityObject->GetOwnerSID();
        SecretId = NMetadata::NSecret::TSecretId(userSID, SecretName);

        if (NSecret::UseSchemaSecrets(AppData()->FeatureFlags, SecretId.GetSecretId())) {
            const TVector<TString> secretNames{SecretId.GetSecretId()};
            auto userToken = MakeIntrusiveConst<NACLib::TUserToken>(userSID, TVector<TString>());
            const auto actorSystem = ActorContext().ActorSystem();
            const auto replyActorId = SelfId();
            auto future = NSecret::DescribeSecret(
                secretNames,
                userToken,
                Database,
                actorSystem,
                {.RetryPolicy = NSecret::MakeLongRetryPolicy()});
            future.Subscribe([actorSystem, replyActorId](const NThreading::TFuture<NKqp::TEvDescribeSecretsResponse::TDescription>& result) {
                actorSystem->Send(replyActorId, new NKqp::TEvDescribeSecretsResponse(result.GetValue()));
            });
        } else if (AppData()->FeatureFlags.GetDisableOldSecrets()) {
            // Just in case - when we disable old secrets, we'll make sure they are not needed any more
            return Reply(false, "Usage of old secrets is disabled now. Please use new secrets");
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

        auto value = secretValue.DetachResult();
        const auto digest = NOpenSsl::NSha256::Calc(value.data(), value.size());
        ReplyValue(value, TStringBuilder() << "legacy:" << HexEncode(digest.data(), digest.size()));
    }

    void Handle(NKqp::TEvDescribeSecretsResponse::TPtr& ev) {
        if (ev->Get()->Description.Status != Ydb::StatusIds::SUCCESS) {
            return Reply(false, ev->Get()->Description.Issues.ToOneLineString());
        }

        Y_ENSURE(ev->Get()->Description.SecretValues.size() == 1);
        const auto& description = ev->Get()->Description;
        TString revision;
        if (description.SecretRevisions.size() == 1) {
            const auto& value = description.SecretRevisions[0];
            if (value.OwnerId && value.LocalPathId && value.Version) {
                revision = TStringBuilder() << "schema:" << value.OwnerId << ':' << value.LocalPathId << ':' << value.Version;
            }
        }
        ReplyValue(description.SecretValues[0], revision);
    }

    void ReplyValue(const TString& value, const TString& revision) {
        auto result = MakeHolder<TEvPrivate::TEvResolveSecretResult>(ReplicationId, value);
        result->Revision = revision;
        Send(Parent, result.Release(), 0, Cookie);
        PassAway();
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

    explicit TSecretResolver(
            const TActorId& parent,
            ui64 rid,
            const TPathId& pathId,
            const TString& secretName,
            const ui64 cookie,
            const TString& database)
        : Parent(parent)
        , ReplicationId(rid)
        , PathId(pathId)
        , SecretName(secretName)
        , Cookie(cookie)
        , Database(database)
        , LogPrefix(CreateActorLogPrefix("SecretResolver", ReplicationId))
    {
    }

    void Bootstrap() {
        YDB_LOG_CREATE_CONTEXT(LogPrefix);
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
        YDB_LOG_CREATE_CONTEXT(LogPrefix,
            {"actorState", "StateWork"});
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
    const NActors::NStructuredLog::TStructuredMessage LogPrefix;

    static constexpr auto RetryInterval = TDuration::Seconds(1);
    NMetadata::NSecret::TSecretId SecretId;

}; // TSecretResolver

IActor* CreateSecretResolver(const TActorId& parent, ui64 rid,
        const TPathId& pathId, const TString& secretName, const ui64 cookie, const TString& database)
{
    return new TSecretResolver(parent, rid, pathId, secretName, cookie, database);
}

bool IsNewSecretRevision(const TString& previous, const TString& next) {
    if (previous.StartsWith("legacy:") && next.StartsWith("legacy:")) {
        return previous != next;
    }
    if (!previous.StartsWith("schema:") || !next.StartsWith("schema:")) {
        return false;
    }
    const auto oldEnd = previous.rfind(':');
    const auto newEnd = next.rfind(':');
    ui64 oldVersion = 0, newVersion = 0;
    return previous.substr(0, oldEnd) == next.substr(0, newEnd)
        && TryFromString(previous.substr(oldEnd + 1), oldVersion)
        && TryFromString(next.substr(newEnd + 1), newVersion) && newVersion > oldVersion;
}

// Only an authentication Error owns this actor. Each resolve/probe and its
// deadline live in the coroutine; cancelling the owner cancels outstanding work.
class TSecretRefresh: public TActorBootstrapped<TSecretRefresh> {
    const TActorId Parent;
    const ui64 ReplicationId;
    const TPathId PathId;
    const TString Database;
    const NKikimrReplication::TConnectionParams Params;
    const TString FailedRevision;
    const TVector<TString> Topics;

public:
    TSecretRefresh(TActorId parent, ui64 rid, TPathId pathId, TString database,
            NKikimrReplication::TConnectionParams params, TString failedRevision, TVector<TString> topics)
        : Parent(parent), ReplicationId(rid), PathId(pathId), Database(std::move(database))
        , Params(std::move(params)), FailedRevision(std::move(failedRevision)), Topics(std::move(topics)) {}

    void Bootstrap() {
        Become(&TThis::StateWork);
        TString lastRejectedRevision = FailedRevision;
        ui64 cookie = 0;
        for (;;) {
            co_await NActors::AsyncSleepFor(TDuration::Seconds(30));
            const auto resolver = Register(CreateSecretResolver(SelfId(), ReplicationId, PathId,
                Params.GetStaticCredentials().GetPasswordSecretName(), ++cookie, Database));
            Y_DEFER { SelfId().Send(resolver, new TEvents::TEvPoison()); };
            auto resolved = co_await NActors::WithTimeout(TDuration::Seconds(30), [&]() -> NActors::async<TEvPrivate::TEvResolveSecretResult::TPtr> {
                co_return co_await NActors::ActorWaitForEvent<TEvPrivate::TEvResolveSecretResult>(cookie);
            });
            if (!resolved) {
                continue;
            }
            const auto response = std::move(*resolved);
            const auto& secret = *response->Get();
            if (!secret.IsSuccess() || !IsNewSecretRevision(FailedRevision, secret.Revision)
                || secret.Revision == lastRejectedRevision || Topics.empty())
            {
                continue;
            }
            auto credentials = Params.GetStaticCredentials();
            credentials.SetPassword(secret.Value);
            const auto proxy = Register(CreateYdbProxy(Params.GetEndpoint(), Params.GetDatabase(),
                Params.GetEnableSsl(), Params.GetCaCert(), credentials));
            Y_DEFER { SelfId().Send(proxy, new TEvents::TEvPoison()); };
            bool authorized = true;
            for (const auto& topic : Topics) {
                auto checked = co_await NActors::WithTimeout(TDuration::Seconds(30), [&]() -> NActors::async<TEvYdbProxy::TEvDescribeTopicResponse::TPtr> {
                    co_return co_await NActors::ActorRequest<TEvYdbProxy::TEvDescribeTopicResponse>(proxy,
                        new TEvYdbProxy::TEvDescribeTopicRequest(topic, {}));
                });
                if (!checked || !(*checked)->Get()->Result.IsSuccess()) {
                    authorized = false;
                    if (checked) {
                        const auto status = (*checked)->Get()->Result.GetStatus();
                        if (status == NYdb::EStatus::UNAUTHORIZED || status == NYdb::EStatus::CLIENT_UNAUTHENTICATED) {
                            lastRejectedRevision = secret.Revision;
                        }
                    }
                    break;
                }
            }
            if (authorized) {
                Send(Parent, new TEvPrivate::TEvRefreshCredentials(ReplicationId, secret.Value, secret.Revision));
                PassAway();
                co_return;
            }
        }
    }

    STATEFN(StateWork) {
        if (ev->GetTypeRewrite() == TEvents::TEvPoison::EventType) {
            PassAway();
        }
    }
};

IActor* CreateSecretRefresh(const TActorId& parent, ui64 rid, const TPathId& pathId,
        const TString& database, const NKikimrReplication::TConnectionParams& params,
        const TString& failedRevision, TVector<TString> topics)
{
    return new TSecretRefresh(parent, rid, pathId, database, params, failedRevision, std::move(topics));
}

}
