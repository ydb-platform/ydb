#include "kqp_federated_query_actors.h"

#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/services/metadata/secret/fetcher.h>
#include <ydb/services/metadata/secret/snapshot.h>
#include <ydb/library/actors/core/log.h>

#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::SCHEMA_SECRET_CACHE, stream)

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

}  // anonymous namespace

void TDescribeSchemaSecretsService::HandleIncomingRequest(TEvResolveSecret::TPtr& ev) {
    LOG_D("TEvResolveSecret: name=" << ev->Get()->SecretName << ", request cookie=" << LastCookie);

    SaveIncomingRequestInfo(*ev->Get());
    SendSchemeCacheRequest(ev->Get()->SecretName);
}

void TDescribeSchemaSecretsService::HandleSchemeCacheResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    LOG_D("TEvNavigateKeySetResult: request cookie=" << ev->Cookie);

    auto respIt = ResolveInFlight.find(ev->Cookie);
    Y_ENSURE(respIt != ResolveInFlight.end(), "such request cookie is not registered");
    const auto& secretName = respIt->second.Secret.Name;

    TAutoPtr<NSchemeCache::TSchemeCacheNavigate> request = ev->Get()->Request;
    if (request->ResultSet.empty() || request->ResultSet.front().Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
        LOG_D("TEvNavigateKeySetResult: request cookie=" << ev->Cookie << ", SchemeCache error");
        FillResponse(ev->Cookie, TEvDescribeSecretsResponse::TDescription(Ydb::StatusIds::BAD_REQUEST, { NYql::TIssue("secret `" + secretName + "` not found") }));
        return;
    }

    const auto& secretDescription = request->ResultSet.front().SecretInfo->Description;
    Y_ENSURE(!secretDescription.HasValue(), "SchemeCache must never contain secret values");

    const auto secretIt = VersionedSecrets.find(secretName);
    if (secretIt != VersionedSecrets.end()) { // some secret version is in cache
        if (
            LocalCacheHasActualVersion(secretIt->second, secretDescription.GetVersion()) &&
            LocalCacheHasActualObject(secretIt->second, request->ResultSet.front().Self->Info.GetPathId())
        ) { // cache contains the most recent version
            LOG_D("TEvNavigateKeySetResult: request cookie=" << ev->Cookie << ", fill value from secret cache");
            FillResponse(ev->Cookie, TEvDescribeSecretsResponse::TDescription(std::vector<TString>{secretIt->second.Value}));
            return;
        } else {
            LOG_D("TEvNavigateKeySetResult: request cookie=" << ev->Cookie << ", secret cache value is outdated");
        }
        VersionedSecrets.erase(secretIt); // no need to store outdated value
    }
    respIt->second.Secret.PathId = request->ResultSet.front().Self->Info.GetPathId();

    TAutoPtr<TEvTxUserProxy::TEvNavigate> req(new TEvTxUserProxy::TEvNavigate());
    NKikimrSchemeOp::TDescribePath* record = req->Record.MutableDescribePath();
    record->SetPath(secretName);
    record->MutableOptions()->SetReturnSecretValue(true);
    // TODO(yurikiselev): Deal with UserToken [issue:25472]
    Send(MakeTxProxyID(), req.Release(), 0, ev->Cookie);
}

void TDescribeSchemaSecretsService::HandleSchemeShardResponse(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
    LOG_D("TEvDescribeSchemeResult: request cookie=" << ev->Cookie);

    const auto respIt = ResolveInFlight.find(ev->Cookie);
    Y_ENSURE(respIt != ResolveInFlight.end(), "such request cookie is not registered");
    const auto& secretName = respIt->second.Secret.Name;

    const auto& rec = ev->Get()->GetRecord();
    if (rec.GetStatus() != NKikimrScheme::EStatus::StatusSuccess) {
        LOG_D("TEvDescribeSchemeResult: request cookie=" << ev->Cookie << ", SchemeShard error");
        FillResponse(ev->Cookie, TEvDescribeSecretsResponse::TDescription(Ydb::StatusIds::BAD_REQUEST, { NYql::TIssue("secret `" + secretName + "` not found") }));
        return;
    }

    const auto& secretValue = rec.GetPathDescription().GetSecretDescription().GetValue();
    const auto& secretVersion = rec.GetPathDescription().GetSecretDescription().GetVersion();
    VersionedSecrets[secretName] = TVersionedSecret{
        .SecretVersion = secretVersion,
        .PathId = respIt->second.Secret.PathId,
        .Name = secretName,
        .Value = secretValue,
    };
    FillResponse(ev->Cookie, TEvDescribeSecretsResponse::TDescription(std::vector<TString>{secretValue}));
}

void TDescribeSchemaSecretsService::FillResponse(const ui64 requestId, const TEvDescribeSecretsResponse::TDescription& response) {
    auto respIt = ResolveInFlight.find(requestId);
    respIt->second.Result.SetValue(response);
    ResolveInFlight.erase(respIt);
}

void TDescribeSchemaSecretsService::Bootstrap() {
    LOG_D("Bootstrap");
    Become(&TDescribeSchemaSecretsService::StateWait);
}

void TDescribeSchemaSecretsService::SaveIncomingRequestInfo(const TEvResolveSecret& req) {
    TResponseContext ctx;
    ctx.Secret.Name = req.SecretName;
    ctx.Result = req.Promise;
    ResolveInFlight[LastCookie] = std::move(ctx);
}

void TDescribeSchemaSecretsService::SendSchemeCacheRequest(const TString& secretName) {
    TAutoPtr<NSchemeCache::TSchemeCacheNavigate> request(new NSchemeCache::TSchemeCacheNavigate());
    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
    entry.Path = SplitPath(secretName);
    request->ResultSet.emplace_back(entry);
    // TODO(yurikiselev): Deal with UserToken [issue:25472]

    Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request), 0, LastCookie++);
}

bool TDescribeSchemaSecretsService::LocalCacheHasActualVersion(const TVersionedSecret& secret, const ui64& cacheSecretVersion) {
    // altering secret value does not change secret path id, so have to check secret version
    return secret.SecretVersion == cacheSecretVersion;
}

bool TDescribeSchemaSecretsService::LocalCacheHasActualObject(const TVersionedSecret& secret, const ui64& cacheSecretPathId) {
    // altering secret object, i.e. changing acl, should lead to secret cache update
    return secret.PathId == cacheSecretPathId;
}

NThreading::TFuture<TEvDescribeSecretsResponse::TDescription> DescribeSecret(const TString& secretName, const TString& ownerUserId, TActorSystem* actorSystem) {
    auto promise = NThreading::NewPromise<TEvDescribeSecretsResponse::TDescription>();
    if (actorSystem->AppData<TAppData>()->FeatureFlags.GetEnableSchemaSecrets() && TStringBuf(secretName).StartsWith("/")) {
        actorSystem->Send(
            MakeKqpDescribeSchemaSecretServiceId(actorSystem->NodeId),
            new TDescribeSchemaSecretsService::TEvResolveSecret(ownerUserId, secretName, promise));
    } else {
        actorSystem->Register(CreateDescribeSecretsActor(ownerUserId, {secretName}, promise));
    }
    return promise.GetFuture();
}

IActor* CreateDescribeSecretsActor(const TString& ownerUserId, const std::vector<TString>& secretIds, NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> promise) {
    return new TDescribeSecretsActor(ownerUserId, secretIds, promise);
}

void RegisterDescribeSecretsActor(const NActors::TActorId& replyActorId, const TString& ownerUserId, const std::vector<TString>& secretIds, NActors::TActorSystem* actorSystem) {
    auto promise = NThreading::NewPromise<TEvDescribeSecretsResponse::TDescription>();
    actorSystem->Register(CreateDescribeSecretsActor(ownerUserId, secretIds, promise));

    promise.GetFuture().Subscribe([actorSystem, replyActorId](const NThreading::TFuture<TEvDescribeSecretsResponse::TDescription>& result){
        actorSystem->Send(replyActorId, new TEvDescribeSecretsResponse(result.GetValue()));
    });
}

NThreading::TFuture<TEvDescribeSecretsResponse::TDescription> DescribeExternalDataSourceSecrets(const NKikimrSchemeOp::TAuth& authDescription, const TString& ownerUserId, TActorSystem* actorSystem) {
    switch (authDescription.identity_case()) {
        case NKikimrSchemeOp::TAuth::kServiceAccount: {
            const TString& saSecretId = authDescription.GetServiceAccount().GetSecretName();
            return DescribeSecret(saSecretId, ownerUserId, actorSystem);
        }

        case NKikimrSchemeOp::TAuth::kNone:
            return NThreading::MakeFuture(TEvDescribeSecretsResponse::TDescription({}));

        case NKikimrSchemeOp::TAuth::kBasic: {
            const TString& passwordSecretId = authDescription.GetBasic().GetPasswordSecretName();
            return DescribeSecret(passwordSecretId, ownerUserId, actorSystem);
        }

        case NKikimrSchemeOp::TAuth::kMdbBasic: {
            const TString& saSecretId = authDescription.GetMdbBasic().GetServiceAccountSecretName();
            const TString& passwordSecreId = authDescription.GetMdbBasic().GetPasswordSecretName();
            auto promise = NThreading::NewPromise<TEvDescribeSecretsResponse::TDescription>();
            actorSystem->Register(CreateDescribeSecretsActor(ownerUserId, {saSecretId, passwordSecreId}, promise));
            return promise.GetFuture();
        }

        case NKikimrSchemeOp::TAuth::kAws: {
            const TString& awsAccessKeyIdSecretId = authDescription.GetAws().GetAwsAccessKeyIdSecretName();
            const TString& awsAccessKeyKeySecretId = authDescription.GetAws().GetAwsSecretAccessKeySecretName();
            auto promise = NThreading::NewPromise<TEvDescribeSecretsResponse::TDescription>();
            actorSystem->Register(CreateDescribeSecretsActor(ownerUserId, {awsAccessKeyIdSecretId, awsAccessKeyKeySecretId}, promise));
            return promise.GetFuture();
        }

        case NKikimrSchemeOp::TAuth::kToken: {
            const TString& tokenSecretId = authDescription.GetToken().GetTokenSecretName();
            return DescribeSecret(tokenSecretId, ownerUserId, actorSystem);
        }

        case NKikimrSchemeOp::TAuth::IDENTITY_NOT_SET:
            return NThreading::MakeFuture(TEvDescribeSecretsResponse::TDescription(Ydb::StatusIds::BAD_REQUEST, { NYql::TIssue("identity case is not specified") }));
    }
}

IActor* CreateDescribeSchemaSecretsService() {
    return new TDescribeSchemaSecretsService();
}

}  // namespace NKikimr::NKqp
