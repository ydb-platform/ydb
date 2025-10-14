#include "kqp_federated_query_actors.h"

#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/tx/scheme_board/subscriber.h>
#include <ydb/services/metadata/secret/fetcher.h>
#include <ydb/services/metadata/secret/snapshot.h>
#include <ydb/library/actors/core/log.h>

#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::SCHEMA_SECRET_CACHE, stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::SCHEMA_SECRET_CACHE, stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::SCHEMA_SECRET_CACHE, stream)

namespace NKikimr::NKqp {

namespace {

class TDescribeSecretsActor: public NActors::TActorBootstrapped<TDescribeSecretsActor> {
    STRICT_STFUNC(StateFunc,
        hFunc(NMetadata::NProvider::TEvRefreshSubscriberData, Handle);
    )

    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
        auto snapshot = ev->Get()->GetSnapshotAs<NMetadata::NSecret::TSnapshot>();

        std::vector<TString> secretValues;
        secretValues.reserve(SecretIds.size());
        for (const auto& secretId: SecretIds) {
            auto secretValue = snapshot->GetSecretValue(NMetadata::NSecret::TSecretIdOrValue::BuildAsId(secretId));
            if (secretValue.IsSuccess()) {
                secretValues.push_back(secretValue.DetachResult());
                continue;
            }

            auto secretIds = snapshot->GetSecretIds(UserToken, secretId.GetSecretId());
            if (secretIds.size() > 1) {
                CompleteAndPassAway(TEvDescribeSecretsResponse::TDescription(Ydb::StatusIds::BAD_REQUEST, { NYql::TIssue("several secrets with name '" + secretId.GetSecretId() + "' were found") }));
                return;
            }

            if (!secretIds.empty()) {
                secretValue = snapshot->GetSecretValue(NMetadata::NSecret::TSecretIdOrValue::BuildAsId(secretIds[0]));
                if (secretValue.IsSuccess()) {
                    secretValues.push_back(secretValue.DetachResult());
                    continue;
                }
            }

            if (!AskSent) {
                AskSent = true;
                Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()), new NMetadata::NProvider::TEvAskSnapshot(GetSecretsSnapshotParser()));
            } else {
                CompleteAndPassAway(TEvDescribeSecretsResponse::TDescription(Ydb::StatusIds::BAD_REQUEST, { NYql::TIssue("secret with name '" + secretId.GetSecretId() + "' not found") }));
            }
            return;
        }

        CompleteAndPassAway(TEvDescribeSecretsResponse::TDescription(secretValues));
    }

    void CompleteAndPassAway(const TEvDescribeSecretsResponse::TDescription& response) {
        Promise.SetValue(response);

        Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()), new NMetadata::NProvider::TEvUnsubscribeExternal(GetSecretsSnapshotParser()));
        PassAway();
    }

    NMetadata::NFetcher::ISnapshotsFetcher::TPtr GetSecretsSnapshotParser() {
        return std::make_shared<NMetadata::NSecret::TSnapshotsFetcher>();
    }

public:
    TDescribeSecretsActor(const TString& ownerUserId, const std::vector<TString>& secretIds, NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> promise)
        : UserToken(NACLib::TUserToken{ownerUserId, TVector<NACLib::TSID>{}})
        , SecretIds(CreateSecretIds(ownerUserId, secretIds))
        , Promise(promise)
    {}

    void Bootstrap() {
        if (!NMetadata::NProvider::TServiceOperator::IsEnabled()) {
            Promise.SetValue(TEvDescribeSecretsResponse::TDescription(Ydb::StatusIds::INTERNAL_ERROR, { NYql::TIssue("metadata service is not active") }));
            PassAway();
            return;
        }

        Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()), new NMetadata::NProvider::TEvSubscribeExternal(GetSecretsSnapshotParser()));
        Become(&TDescribeSecretsActor::StateFunc);
    }

private:
    static std::vector<NMetadata::NSecret::TSecretId> CreateSecretIds(const TString& ownerUserId, const std::vector<TString>& secretIds) {
        std::vector<NMetadata::NSecret::TSecretId> result;
        for (const TString& secretId : secretIds) {
            result.emplace_back(ownerUserId, secretId);
        }
        return result;
    }

private:
    std::optional<NACLib::TUserToken> UserToken;
    const std::vector<NMetadata::NSecret::TSecretId> SecretIds;
    NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> Promise;
    bool AskSent = false;
};

IActor* CreateDescribeSecretsActor(const TString& ownerUserId, const std::vector<TString>& secretIds, NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> promise) {
    return new TDescribeSecretsActor(ownerUserId, secretIds, promise);
}

}  // anonymous namespace

void TDescribeSchemaSecretsService::HandleIncomingRequest(TEvResolveSecret::TPtr& ev) {
    LOG_D("TEvResolveSecret: names=" << JoinSeq(',', ev->Get()->SecretNames) << ", request cookie=" << LastCookie);

    if (ev->Get()->SecretNames.empty()) {
        LOG_W("TEvResolveSecret: request cookie=" << ev->Cookie << ", empty secret names list");
        static const auto emptyRequest = TEvDescribeSecretsResponse::TDescription(Ydb::StatusIds::BAD_REQUEST, { NYql::TIssue("empty secret names list") });
        ev->Get()->Promise.SetValue(emptyRequest);
        return;
    }

    SaveIncomingRequestInfo(*ev->Get());
    SendSchemeCacheRequests(*ev->Get());
}

void TDescribeSchemaSecretsService::HandleSchemeCacheResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    LOG_D("TEvNavigateKeySetResult: request cookie=" << ev->Cookie);

    auto respIt = ResolveInFlight.find(ev->Cookie);
    Y_ENSURE(respIt != ResolveInFlight.end(), "such request cookie is not registered");

    TAutoPtr<NSchemeCache::TSchemeCacheNavigate> request = ev->Get()->Request;
    if (HandleSchemeCacheErrorsIfAny(ev->Cookie, *request)) {
        return;
    }

    for (const auto& entry: request->ResultSet) {
        const auto& secretDescription = entry.SecretInfo->Description;
        Y_ENSURE(!secretDescription.HasValue(), "SchemeCache must never contain secret values");

        const TString secretPath = CanonizePath(entry.Path);
        const auto secretIt = VersionedSecrets.find(secretPath);

        if (secretIt != VersionedSecrets.end() &&
            (LocalCacheHasActualVersion(secretIt->second, secretDescription.GetVersion()) &&
            LocalCacheHasActualObject(secretIt->second, request->ResultSet.front().Self->Info.GetPathId())))
        {
            // some secret version is in cache
            ++respIt->second.FilledSecretsCnt;
        } else {
            // make TxProxy request
            TAutoPtr<TEvTxUserProxy::TEvNavigate> navigateRequest(new TEvTxUserProxy::TEvNavigate());
            Y_ENSURE(!request->DatabaseName.empty(), "Database name must be set in TxProxy requests");
            navigateRequest->Record.SetDatabaseName(request->DatabaseName);
            NKikimrSchemeOp::TDescribePath* record = navigateRequest->Record.MutableDescribePath();
            record->SetPath(secretPath);
            record->MutableOptions()->SetReturnSecretValue(true);
            Send(MakeTxProxyID(), navigateRequest.Release(), 0, ev->Cookie);
        }
    }

    FillResponseIfFinished(ev->Cookie, respIt->second);
}

void TDescribeSchemaSecretsService::HandleSchemeShardResponse(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
    LOG_D("TEvDescribeSchemeResult: request cookie=" << ev->Cookie);

    const auto respIt = ResolveInFlight.find(ev->Cookie);
    if (respIt == ResolveInFlight.end()) {        
        Y_ENSURE(respIt->second.Secrets.size() > 1, "This is possible only for batch requests");
        LOG_N("TEvDescribeSchemeResult: request cookie=" << ev->Cookie << "skipped response handling due to previous errors");
        // no need to fill response, since it has been filled on the first SchemeShard error
        return;
    }

    const auto& rec = ev->Get()->GetRecord();
    const auto& secretName = CanonizePath(rec.GetPath());
    if (rec.GetStatus() != NKikimrScheme::EStatus::StatusSuccess) {
        LOG_N("TEvDescribeSchemeResult: request cookie=" << ev->Cookie << ", SchemeShard error");
        FillResponse(ev->Cookie, TEvDescribeSecretsResponse::TDescription(Ydb::StatusIds::BAD_REQUEST, { NYql::TIssue("secret `" + secretName + "` not found") }));
        return;
    }

    if (const auto it = SchemeBoardSubscribers.find(secretName); it == SchemeBoardSubscribers.end()) {
        SchemeBoardSubscribers[secretName] = Register(CreateSchemeBoardSubscriber(SelfId(), secretName));
    }

    const auto& secretValue = rec.GetPathDescription().GetSecretDescription().GetValue();
    const auto& secretVersion = rec.GetPathDescription().GetSecretDescription().GetVersion();
    VersionedSecrets[secretName] = TVersionedSecret{
        .SecretVersion = secretVersion,
        .PathId = rec.GetPathId(),
        .Name = secretName,
        .Value = secretValue,
    };

    ++respIt->second.FilledSecretsCnt;

    FillResponseIfFinished(ev->Cookie, respIt->second);
}

void TDescribeSchemaSecretsService::FillResponse(const ui64& requestId, const TEvDescribeSecretsResponse::TDescription& response) {
    auto respIt = ResolveInFlight.find(requestId);
    respIt->second.Result.SetValue(response);
    ResolveInFlight.erase(respIt);
}

void TDescribeSchemaSecretsService::Bootstrap() {
    LOG_D("Bootstrap");
    Become(&TDescribeSchemaSecretsService::StateWait);
}

void TDescribeSchemaSecretsService::SaveIncomingRequestInfo(const TEvResolveSecret& ev) {
    TResponseContext ctx;
    for (size_t i = 0; i < ev.SecretNames.size(); ++i) {
        ctx.Secrets[ev.SecretNames[i]] = i;
        ctx.Result = ev.Promise;
    }
    ResolveInFlight[LastCookie] = std::move(ctx);
}

void TDescribeSchemaSecretsService::SendSchemeCacheRequests(const TEvResolveSecret& ev) {
    const auto userToken = ev.UserToken;
    TAutoPtr<NSchemeCache::TSchemeCacheNavigate> request(new NSchemeCache::TSchemeCacheNavigate());
    for (const auto& secretName : ev.SecretNames) {
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        entry.Path = SplitPath(secretName);    
        if (userToken && userToken->GetUserSID()) {
            entry.Access = NACLib::SelectRow;
        }
        request->ResultSet.emplace_back(entry);
    }
    if (userToken && userToken->GetUserSID()) {
        request->UserToken = userToken;
    }
    request->DatabaseName = ev.Database;

    Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request), 0, LastCookie++);
}

bool TDescribeSchemaSecretsService::LocalCacheHasActualVersion(const TVersionedSecret& secret, const ui64& cacheSecretVersion) {
    // altering secret value does not change secret path id, so have to check secret version
    return secret.SecretVersion == cacheSecretVersion;
}

bool TDescribeSchemaSecretsService::LocalCacheHasActualObject(const TVersionedSecret& secret, const ui64& cacheSecretPathId) {
    // This helps with the case when the secret was dropped and created again with the same name.
    // Secret version will become zero again, which would not lead to a secret cache update.
    return secret.PathId == cacheSecretPathId;
}

bool TDescribeSchemaSecretsService::HandleSchemeCacheErrorsIfAny(const ui64& requestId, NSchemeCache::TSchemeCacheNavigate& result) {
    if (result.ResultSet.empty()) {
        LOG_N("TEvNavigateKeySetResult: request cookie=" << requestId << ", SchemeCache error");
        FillResponse(requestId, TEvDescribeSecretsResponse::TDescription(Ydb::StatusIds::BAD_REQUEST, { NYql::TIssue("secrets were not found") }));
        return true;
    }

    for (const auto& entry: result.ResultSet) {
        if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            const auto secretPath = CanonizePath(entry.Path);
            LOG_N("TEvNavigateKeySetResult: request cookie=" << requestId << ", unauthorized SchemeCache request for secret=" << secretPath);
            FillResponse(requestId, TEvDescribeSecretsResponse::TDescription(Ydb::StatusIds::BAD_REQUEST, { NYql::TIssue("secret `" + secretPath + "` not found") }));

            return true;
        }
    }
    return false;
}

void TDescribeSchemaSecretsService::FillResponseIfFinished(const ui64& requestId, const TResponseContext& responseCtx) {
    if (responseCtx.FilledSecretsCnt != responseCtx.Secrets.size()) {
        return;
    }

    std::vector<TString> secretValues;
    secretValues.resize(responseCtx.Secrets.size());
    for (const auto& secret : responseCtx.Secrets) {
        const auto& secretPath = secret.first;
        auto it = VersionedSecrets.find(secret.first);
        if (it == VersionedSecrets.end()) {
            LOG_N("FillResponseIfFinished: request cookie=" << requestId << ", secret `" << secretPath << "` was dropped during request");
            FillResponse(requestId, TEvDescribeSecretsResponse::TDescription(Ydb::StatusIds::BAD_REQUEST, { NYql::TIssue("secret `" + secretPath + "` not found") }));
            return;
        }

        Y_ENSURE(secret.second < secretValues.size());
        secretValues[secret.second] = it->second.Value;
    }
    FillResponse(requestId, TEvDescribeSecretsResponse::TDescription(secretValues));
}

void TDescribeSchemaSecretsService::HandleNotifyUpdate(TSchemeBoardEvents::TEvNotifyUpdate::TPtr& ev) {
    Y_UNUSED(ev);
}

void TDescribeSchemaSecretsService::HandleNotifyDelete(TSchemeBoardEvents::TEvNotifyDelete::TPtr& ev) {
    const TString& secretName = CanonizePath(ev->Get()->Path);

    if (SecretUpdateListener) {
        SecretUpdateListener->HandleNotifyDelete(secretName);
    }

    VersionedSecrets.erase(secretName);

    const auto subscriberIt = SchemeBoardSubscribers.find(secretName);
    Y_ENSURE(subscriberIt != SchemeBoardSubscribers.end());
    Send(subscriberIt->second, new TEvents::TEvPoisonPill());
    SchemeBoardSubscribers.erase(subscriberIt);
}

NThreading::TFuture<TEvDescribeSecretsResponse::TDescription> DescribeSecret(
    const TVector<TString>& secretNames,
    const TIntrusiveConstPtr<NACLib::TUserToken> userToken,
    const TString& database,
    TActorSystem* actorSystem
) {
    auto promise = NThreading::NewPromise<TEvDescribeSecretsResponse::TDescription>();
    if (UseSchemaSecrets(AppData()->FeatureFlags, secretNames)) {
        actorSystem->Send(
            MakeKqpDescribeSchemaSecretServiceId(actorSystem->NodeId),
            new TDescribeSchemaSecretsService::TEvResolveSecret(userToken, database, secretNames, promise)
        );
        return promise.GetFuture();
    }

    actorSystem->Register(CreateDescribeSecretsActor(userToken ? userToken->GetUserSID() : "", secretNames, promise));
    return promise.GetFuture();
}

void RegisterDescribeSecretsActor(
    const NActors::TActorId& replyActorId,
    const TIntrusiveConstPtr<NACLib::TUserToken> userToken,
    const TString& database,
    const std::vector<TString>& secretIds,
    NActors::TActorSystem* actorSystem
) {
    TVector<TString> secretNames{secretIds.begin(), secretIds.end()};
    auto future = DescribeSecret(secretNames, userToken, database, actorSystem);
    future.Subscribe([actorSystem, replyActorId](const NThreading::TFuture<TEvDescribeSecretsResponse::TDescription>& result){
        actorSystem->Send(replyActorId, new TEvDescribeSecretsResponse(result.GetValue()));
    });
}

NThreading::TFuture<TEvDescribeSecretsResponse::TDescription> DescribeExternalDataSourceSecrets(
    const NKikimrSchemeOp::TAuth& authDescription,
    const TIntrusiveConstPtr<NACLib::TUserToken> userToken,
    const TString& database,
    TActorSystem* actorSystem
) {
    switch (authDescription.identity_case()) {
        case NKikimrSchemeOp::TAuth::kServiceAccount: {
            const TString& saSecretId = authDescription.GetServiceAccount().GetSecretName();
            return DescribeSecret({saSecretId}, userToken, database, actorSystem);
        }

        case NKikimrSchemeOp::TAuth::kNone:
            return NThreading::MakeFuture(TEvDescribeSecretsResponse::TDescription({}));

        case NKikimrSchemeOp::TAuth::kBasic: {
            const TString& passwordSecretId = authDescription.GetBasic().GetPasswordSecretName();
            return DescribeSecret({passwordSecretId}, userToken, database, actorSystem);
        }

        case NKikimrSchemeOp::TAuth::kMdbBasic: {
            const TString& saSecretId = authDescription.GetMdbBasic().GetServiceAccountSecretName();
            const TString& passwordSecreId = authDescription.GetMdbBasic().GetPasswordSecretName();
            return DescribeSecret({saSecretId, passwordSecreId}, userToken, database, actorSystem);
        }

        case NKikimrSchemeOp::TAuth::kAws: {
            const TString& awsAccessKeyIdSecretId = authDescription.GetAws().GetAwsAccessKeyIdSecretName();
            const TString& awsAccessKeyKeySecretId = authDescription.GetAws().GetAwsSecretAccessKeySecretName();
            return DescribeSecret({awsAccessKeyIdSecretId, awsAccessKeyKeySecretId}, userToken, database, actorSystem);
        }

        case NKikimrSchemeOp::TAuth::kToken: {
            const TString& tokenSecretId = authDescription.GetToken().GetTokenSecretName();
            return DescribeSecret({tokenSecretId}, userToken, database, actorSystem);
        }

        case NKikimrSchemeOp::TAuth::IDENTITY_NOT_SET:
            return NThreading::MakeFuture(TEvDescribeSecretsResponse::TDescription(Ydb::StatusIds::BAD_REQUEST, { NYql::TIssue("identity case is not specified") }));
    }
}

IActor* TDescribeSchemaSecretsServiceFactory::CreateService() {
    return new TDescribeSchemaSecretsService();
}

bool UseSchemaSecrets(const NKikimr::TFeatureFlags& flags, const TVector<TString>& secretNames) {
    if (!flags.GetEnableSchemaSecrets()) {
        return false;
    }

    for (const auto& secretName : secretNames) {
        if (!secretName.StartsWith('/')) {
            return false;
        }
    }

    return true; // New secrets are enabled and all of them start with '/'
}

bool UseSchemaSecrets(const NKikimr::TFeatureFlags& flags, const TString& secretName) {
    return flags.GetEnableSchemaSecrets() && secretName.StartsWith('/');
}

}  // namespace NKikimr::NKqp
