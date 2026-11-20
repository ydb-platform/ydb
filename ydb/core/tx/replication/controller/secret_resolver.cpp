#include "logging.h"
#include "private_events.h"
#include "secret_resolver.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/services/scheme_secret/resolver.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/services/metadata/secret/fetcher.h>
#include <ydb/services/metadata/secret/secret.h>
#include <ydb/services/metadata/secret/snapshot.h>
#include <ydb/services/metadata/service.h>

#include <util/generic/is_in.h>
#include <util/generic/ptr.h>
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
                {.RetryPolicy = RetryPolicy});
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

        ReplyValue(secretValue.DetachResult(), LegacySecretRecoveryMarker);
    }

    void Handle(NKqp::TEvDescribeSecretsResponse::TPtr& ev) {
        if (ev->Get()->Description.Status != Ydb::StatusIds::SUCCESS) {
            return Reply(false, ev->Get()->Description.Issues.ToOneLineString());
        }

        Y_ENSURE(ev->Get()->Description.SecretValues.size() == 1);
        const auto& description = ev->Get()->Description;
        TString revision;
        if (description.SecretRevisions) {
            const auto& value = description.SecretRevisions->front();
            // CREATE SECRET has revision zero; metadata presence is explicit.
            revision = TStringBuilder() << "schema:" << value.PathOwnerId << ':' << value.LocalPathId << ':' << value.Version;
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
            const TString& database,
            IRetryPolicy<>::TPtr retryPolicy)
        : Parent(parent)
        , ReplicationId(rid)
        , PathId(pathId)
        , SecretName(secretName)
        , Cookie(cookie)
        , Database(database)
        , RetryPolicy(std::move(retryPolicy))
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
    const IRetryPolicy<>::TPtr RetryPolicy;
    const NActors::NStructuredLog::TStructuredMessage LogPrefix;

    static constexpr auto RetryInterval = TDuration::Seconds(1);
    NMetadata::NSecret::TSecretId SecretId;

}; // TSecretResolver

IActor* CreateSecretResolver(const TActorId& parent, ui64 rid,
        const TPathId& pathId, const TString& secretName, const ui64 cookie, const TString& database)
{
    return new TSecretResolver(parent, rid, pathId, secretName, cookie, database, NSecret::MakeLongRetryPolicy());
}

bool CanRefreshSecret(TStringBuf failed, TStringBuf resolved) {
    if (failed == LegacySecretRecoveryMarker && resolved == LegacySecretRecoveryMarker) {
        return true;
    }
    const auto parseSchemaRevision = [](TStringBuf marker, NKqp::TSecretRevision& revision) {
        return marker.SkipPrefix("schema:")
            && TryFromString(marker.NextTok(':'), revision.PathOwnerId)
            && TryFromString(marker.NextTok(':'), revision.LocalPathId)
            && TryFromString(marker, revision.Version);
    };
    NKqp::TSecretRevision previous, next;
    if (failed == MissingIndexStreamRecoveryMarker) {
        // After a controller restart, revalidate the current credentials on all
        // remaining streams before resuming the base workers.
        return resolved == LegacySecretRecoveryMarker || parseSchemaRevision(resolved, next);
    }
    return parseSchemaRevision(failed, previous) && parseSchemaRevision(resolved, next)
        && previous.PathOwnerId == next.PathOwnerId && previous.LocalPathId == next.LocalPathId
        && previous.Version < next.Version;
}

// Only an authentication Error owns this actor. It owns the retry cadence;
// single-attempt secret lookups cannot leave service-owned retry chains after
// the resolver is cancelled. Each resolve/probe deadline lives in the coroutine.
class TSecretRefresh: public TActorBootstrapped<TSecretRefresh> {
    const TActorId Parent;
    const ui64 ReplicationId;
    const TPathId PathId;
    const TString Database;
    const NKikimrReplication::TConnectionParams Params;
    const TString FailedRevision;
    const TVector<TSecretProbeTopic> Topics;

    static bool MayHaveImplTable(const NYdb::NTable::TIndexDescription& index, TStringBuf name) {
        using NYdb::NTable::EIndexType;
        if (IsIn({EIndexType::LocalBloomFilter, EIndexType::LocalBloomNgramFilter, EIndexType::LocalMinMax}, index.GetIndexType())) {
            return false;
        }
        TVector<TString> keys;
        for (const auto& key : index.GetIndexColumns()) {
            keys.emplace_back(key.data(), key.size());
        }
        // The remote source may use either fulltext layout. A type unknown to
        // this controller gives no evidence that the old table is gone.
        for (const bool compact : {false, true}) {
            const auto type = NTableIndex::TryConvertIndexType(index.GetIndexType(), compact);
            if (!type || IsIn(NTableIndex::GetImplTables(*type, keys), name)) {
                return true;
            }
        }
        return false;
    }

    static NActors::async<NYdb::EStatus> ProbeTopic(const TActorId& proxy, const TSecretProbeTopic& topic,
            THashSet<ui64>& missingIndexStreams) {
        auto described = co_await NActors::ActorRequest<TEvYdbProxy::TEvDescribeTopicResponse>(proxy,
            new TEvYdbProxy::TEvDescribeTopicRequest(topic.Path, {}));
        const auto status = described->Get()->Result.GetStatus();
        if (status != NYdb::EStatus::SCHEME_ERROR || !topic.SchemaManagedIndex) {
            co_return status;
        }

        // DescribeTopic conceals both missing paths and ACL denials as SCHEME_ERROR.
        // If the index name has already been reused, inspect the owned feed on
        // the implementation table itself, using the same candidate credentials.
        const auto& index = *topic.SchemaManagedIndex;
        auto table = co_await NActors::ActorRequest<TEvYdbProxy::TEvDescribeTableResponse>(proxy,
            new TEvYdbProxy::TEvDescribeTableRequest(index.TablePath, {}));
        const auto& result = table->Get()->Result;
        if (!result.IsSuccess()) {
            co_return result.GetStatus();
        }
        for (const auto& present : result.GetTableDescription().GetIndexDescriptions()) {
            if (TStringBuf(present.GetIndexName()) == index.Name) {
                const TString implPath(ExtractParent(topic.Path));
                auto implTable = co_await NActors::ActorRequest<TEvYdbProxy::TEvDescribeTableResponse>(proxy,
                    new TEvYdbProxy::TEvDescribeTableRequest(implPath, {}));
                const auto& impl = implTable->Get()->Result;
                if (!impl.IsSuccess()) {
                    if (impl.GetStatus() == NYdb::EStatus::SCHEME_ERROR && !MayHaveImplTable(present, ExtractBase(implPath))) {
                        // The authenticated index description excludes this
                        // implementation table even if its topic ACL is hidden.
                        missingIndexStreams.insert(index.TargetId);
                        break;
                    }
                    co_return impl.GetStatus();
                }
                for (const auto& feed : impl.GetTableDescription().GetChangefeedDescriptions()) {
                    if (TStringBuf(feed.GetName()) == ExtractBase(topic.Path)) {
                        co_return status;
                    }
                }
                // The GUID is gone. Only a base CDC record can establish that
                // the old index itself was dropped.
                missingIndexStreams.insert(index.TargetId);
                break;
            }
        }
        co_return NYdb::EStatus::SUCCESS;
    }

public:
    TSecretRefresh(TActorId parent, ui64 rid, TPathId pathId, TString database,
            NKikimrReplication::TConnectionParams params, TString failedRevision, TVector<TSecretProbeTopic> topics)
        : Parent(parent), ReplicationId(rid), PathId(pathId), Database(std::move(database))
        , Params(std::move(params)), FailedRevision(std::move(failedRevision)), Topics(std::move(topics)) {}

    void Bootstrap() {
        Become(&TThis::StateWork);
        const auto& failedCredentials = Params.GetStaticCredentials();
        TString rejectedRevision;
        TDuration retryDelay = TDuration::Zero();
        TMonotonic retryAt = TMonotonic::Zero();
        ui64 cookie = 0;
        for (;;) {
            co_await NActors::AsyncSleepFor(TDuration::Seconds(30));
            const auto resolver = Register(new TSecretResolver(SelfId(), ReplicationId, PathId,
                failedCredentials.GetPasswordSecretName(), ++cookie, Database,
                IRetryPolicy<>::GetNoRetryPolicy()));
            Y_DEFER { SelfId().Send(resolver, new TEvents::TEvPoison()); };
            auto resolved = co_await NActors::WithTimeout(TDuration::Seconds(30), [&]() -> NActors::async<TEvPrivate::TEvResolveSecretResult::TPtr> {
                co_return co_await NActors::ActorWaitForEvent<TEvPrivate::TEvResolveSecretResult>(cookie);
            });
            if (!resolved) {
                continue;
            }
            const auto response = std::move(*resolved);
            const auto& secret = *response->Get();
            if (!secret.IsSuccess() || !CanRefreshSecret(FailedRevision, secret.Revision)
                || (secret.Revision == rejectedRevision && TActivationContext::Monotonic() < retryAt) || Topics.empty())
            {
                continue;
            }
            // DescribeTopic can succeed for credentials that the worker cannot use to read.
            // The failed legacy password is known in memory until the controller restarts.
            if (FailedRevision == LegacySecretRecoveryMarker && failedCredentials.HasPassword()
                && secret.Value == failedCredentials.GetPassword())
            {
                continue;
            }
            auto credentials = failedCredentials;
            credentials.SetPassword(secret.Value);
            const auto proxy = Register(CreateYdbProxy(Params.GetEndpoint(), Params.GetDatabase(),
                Params.GetEnableSsl(), Params.GetCaCert(), credentials));
            Y_DEFER { SelfId().Send(proxy, new TEvents::TEvPoison()); };
            THashSet<ui64> missingIndexStreams;
            bool authorized = true;
            for (const auto& topic : Topics) {
                const auto status = co_await NActors::WithTimeout(TDuration::Seconds(30), ProbeTopic(proxy, topic, missingIndexStreams));
                if (status == NYdb::EStatus::SUCCESS) {
                    continue;
                }
                if (status == NYdb::EStatus::UNAUTHORIZED || status == NYdb::EStatus::CLIENT_UNAUTHENTICATED) {
                    retryDelay = secret.Revision == rejectedRevision
                        ? Min(retryDelay * 2, TDuration::Minutes(5)) : TDuration::Minutes(1);
                    rejectedRevision = secret.Revision;
                    retryAt = TActivationContext::Monotonic() + retryDelay;
                }
                authorized = false;
                break;
            }
            if (authorized) {
                Send(Parent, new TEvPrivate::TEvRefreshCredentials(ReplicationId, secret.Value, secret.Revision, std::move(missingIndexStreams)));
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
        const TString& failedRevision, TVector<TSecretProbeTopic> topics)
{
    return new TSecretRefresh(parent, rid, pathId, database, params, failedRevision, std::move(topics));
}

}
