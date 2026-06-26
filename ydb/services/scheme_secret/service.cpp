#include <ydb/services/scheme_secret/service.h>
#include <ydb/services/scheme_secret/resolver.h>

#include <ydb/core/base/appdata.h>

#include <ydb/core/tx/scheme_board/subscriber.h>
#include <ydb/services/metadata/secret/fetcher.h>
#include <ydb/services/metadata/secret/snapshot.h>
#include <ydb/services/metadata/service.h>
#include <ydb/library/actors/core/log.h>

#include <limits>

#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::SCHEMA_SECRET_CACHE, stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::SCHEMA_SECRET_CACHE, stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::SCHEMA_SECRET_CACHE, stream)

namespace NKikimr::NSecret {

namespace {

TStringBuilder GetLogLabel(const TString& label, const ui64 requestId) {
    return TStringBuilder() << label << " [" << requestId << "]: ";
}

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
                CompleteAndPassAway(NKqp::TEvDescribeSecretsResponse::TDescription(Ydb::StatusIds::BAD_REQUEST, { NYql::TIssue("several secrets with name '" + secretId.GetSecretId() + "' were found") }));
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
                CompleteAndPassAway(NKqp::TEvDescribeSecretsResponse::TDescription(Ydb::StatusIds::BAD_REQUEST, { NYql::TIssue("secret with name '" + secretId.GetSecretId() + "' not found") }));
            }
            return;
        }

        CompleteAndPassAway(NKqp::TEvDescribeSecretsResponse::TDescription(secretValues));
    }

    void CompleteAndPassAway(const NKqp::TEvDescribeSecretsResponse::TDescription& response) {
        Promise.SetValue(response);

        Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()), new NMetadata::NProvider::TEvUnsubscribeExternal(GetSecretsSnapshotParser()));
        PassAway();
    }

    NMetadata::NFetcher::ISnapshotsFetcher::TPtr GetSecretsSnapshotParser() {
        return std::make_shared<NMetadata::NSecret::TSnapshotsFetcher>();
    }

public:
    TDescribeSecretsActor(const TString& ownerUserId, const std::vector<TString>& secretIds, NThreading::TPromise<NKqp::TEvDescribeSecretsResponse::TDescription> promise)
        : UserToken(NACLib::TUserToken{ownerUserId, TVector<NACLib::TSID>{}})
        , SecretIds(CreateSecretIds(ownerUserId, secretIds))
        , Promise(promise)
    {}

    void Bootstrap() {
        if (!NMetadata::NProvider::TServiceOperator::IsEnabled()) {
            Promise.SetValue(NKqp::TEvDescribeSecretsResponse::TDescription(Ydb::StatusIds::INTERNAL_ERROR, { NYql::TIssue("metadata service is not active") }));
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
    NThreading::TPromise<NKqp::TEvDescribeSecretsResponse::TDescription> Promise;
    bool AskSent = false;
};

IActor* CreateDescribeSecretsActor(const TString& ownerUserId, const std::vector<TString>& secretIds, NThreading::TPromise<NKqp::TEvDescribeSecretsResponse::TDescription> promise) {
    return new TDescribeSecretsActor(ownerUserId, secretIds, promise);
}

// It's a hack so we can simulate Scheme Cache retryable errors in tests
NSchemeCache::TSchemeCacheNavigate::EStatus GetSchemeCacheEntryStatus(
    const TDescribeSchemaSecretsService::ISchemeCacheStatusGetter* schemeCacheStatusGetter,
    NSchemeCache::TSchemeCacheNavigate::TEntry& entry)
{
    if (schemeCacheStatusGetter) {
        return schemeCacheStatusGetter->GetStatus(entry);
    }
    return entry.Status;
}

NKikimrScheme::EStatus GetSchemeShardPathStatus(
    const TDescribeSchemaSecretsService::ISchemeShardStatusGetter* schemeShardStatusGetter,
    const NKikimrScheme::TEvDescribeSchemeResult& record)
{
    if (schemeShardStatusGetter) {
        return schemeShardStatusGetter->GetStatus(record);
    }
    return record.GetStatus();
}

bool IsRetryableSchemeShardStatus(NKikimrScheme::EStatus status) {
    return status == NKikimrScheme::EStatus::StatusNotAvailable;
}

Ydb::StatusIds::StatusCode MapSchemeShardStatus(NKikimrScheme::EStatus status) {
    switch (status) {
        case NKikimrScheme::EStatus::StatusNotAvailable:
            return Ydb::StatusIds::UNAVAILABLE;
        default:
            return Ydb::StatusIds::BAD_REQUEST;
    }
}

TString MakeSchemeShardErrorMessage(NKikimrScheme::EStatus status, const TString& secretName) {
    switch (status) {
        case NKikimrScheme::EStatus::StatusNotAvailable:
            return "Schemeshard is not available for secret `" + secretName + "`";
        case NKikimrScheme::EStatus::StatusPathDoesNotExist:
            return "Secret `" + secretName + "` not found";
        case NKikimrScheme::EStatus::StatusAccessDenied:
            return "Access denied to secret `" + secretName + "`";
        default:
            return "Failed to resolve secret `" + secretName + "`";
    }
}

TString ListSecrets(const TVector<TString>& paths) {
    if (paths.empty()) {
        return "";
    }
    if (paths.size() == 1) {
        return "secret `" + paths[0] + "`";
    }

    auto sb = TStringBuilder() << "secrets ";
    for (size_t i = 0; i < paths.size(); ++i) {
        sb << "`" << paths[i] << "`";
        if (i + 1 < paths.size()) {
            sb << ", ";
        }
    }
    return sb;
}

}  // anonymous namespace

void TDescribeSchemaSecretsService::HandleIncomingRequest(TEvResolveSecret::TPtr& ev) {
    LOG_D(GetLogLabel("TEvResolveSecret", LastRequestId) << "secrets=" << JoinSeq(',', ev->Get()->SecretNames));

    if (ev->Get()->SecretNames.empty()) {
        LOG_W(GetLogLabel("TEvResolveSecret", LastRequestId) << "empty secret names list");
        static const auto emptyRequest = NKqp::TEvDescribeSecretsResponse::TDescription(Ydb::StatusIds::BAD_REQUEST, { NYql::TIssue("empty secret names list") });
        ev->Get()->Promise.SetValue(emptyRequest);
        return;
    }

    SaveIncomingRequestInfo(*ev->Get());
    SendSchemeCacheRequests(*ev->Get(), LastRequestId);
    ++LastRequestId;
}

void TDescribeSchemaSecretsService::HandleIncomingSchemeCacheRetryRequest(TEvResolveSecretSchemeCacheRetry::TPtr& ev) {
    LOG_N(GetLogLabel("TEvResolveSecretSchemeCacheRetry", ev->Get()->InitialRequestId));

    const auto it = RequestsInFlight.find(ev->Get()->InitialRequestId);
    Y_ENSURE(it != RequestsInFlight.end(), "Unregistered requestId: " + ToString(ev->Get()->InitialRequestId));
    Y_ENSURE(it->second.Request.Get(), "Initial request was not saved");
    SendSchemeCacheRequests(*it->second.Request.Get(), ev->Get()->InitialRequestId);
}

void TDescribeSchemaSecretsService::HandleIncomingSchemeShardRetryRequest(TEvResolveSecretSchemeShardRetry::TPtr& ev) {
    LOG_N(GetLogLabel("TEvResolveSecretSchemeShardRetry", ev->Get()->InitialRequestId)
        << "secret=" << ev->Get()->SecretPath);

    const auto it = RequestsInFlight.find(ev->Get()->InitialRequestId);
    if (it == RequestsInFlight.end()) {
        LOG_N(GetLogLabel("TEvResolveSecretSchemeShardRetry", ev->Get()->InitialRequestId)
            << "retry handling was skipped due to previous errors");
        return;
    }
    Y_ENSURE(it->second.Request.Get(), "Initial request was not saved");
    SendSchemeShardRequest(*it->second.Request.Get(), ev->Get()->InitialRequestId, ev->Get()->SecretPath);
}

void TDescribeSchemaSecretsService::HandleSchemeCacheResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    const auto requestId = ev->Cookie;
    LOG_D(GetLogLabel("TEvNavigateKeySetResult", requestId));

    auto respIt = ResolveInFlight.find(requestId);
    Y_ENSURE(respIt != ResolveInFlight.end(), "such requestId is not registered");

    TAutoPtr<NSchemeCache::TSchemeCacheNavigate> request = ev->Get()->Request;
    if (HandleSchemeCacheErrorsIfAny(requestId, *request)) {
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
            const auto requestIt = RequestsInFlight.find(requestId);
            Y_ENSURE(requestIt != RequestsInFlight.end(), "Unregistered requestId: " + ToString(requestId));
            SendSchemeShardRequest(*requestIt->second.Request.Get(), requestId, secretPath);
        }
    }

    FillResponseIfFinished(requestId, respIt->second);
}

void TDescribeSchemaSecretsService::SendSchemeShardRequest(
    const TEvResolveSecret& ev,
    const ui64 requestId,
    const TString& secretPath)
{
    TAutoPtr<TEvTxUserProxy::TEvNavigate> navigateRequest(new TEvTxUserProxy::TEvNavigate());
    Y_ENSURE(!ev.Database.empty(), "Database name must be set in TxProxy requests");
    navigateRequest->Record.SetDatabaseName(ev.Database);
    NKikimrSchemeOp::TDescribePath* record = navigateRequest->Record.MutableDescribePath();
    record->SetPath(secretPath);
    record->MutableOptions()->SetReturnSecretValue(true);
    Send(MakeTxProxyID(), navigateRequest.Release(), 0, requestId);
}

void TDescribeSchemaSecretsService::HandleSchemeShardResponse(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
    const auto requestId = ev->Cookie;
    LOG_D(GetLogLabel("TEvDescribeSchemeResult", requestId));

    const auto respIt = ResolveInFlight.find(requestId);
    if (respIt == ResolveInFlight.end()) {
        LOG_N(GetLogLabel("TEvDescribeSchemeResult", requestId) << "response handling was skipped due to previous errors");
        return;
    }

    const auto& rec = ev->Get()->GetRecord();
    if (HandleSchemeShardErrorsIfAny(requestId, rec)) {
        return;
    }

    const auto& secretName = CanonizePath(rec.GetPath());

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

    FillResponseIfFinished(requestId, respIt->second);
}

void TDescribeSchemaSecretsService::FillResponse(const ui64& requestId, const NKqp::TEvDescribeSecretsResponse::TDescription& response) {
    auto respIt = ResolveInFlight.find(requestId);
    Y_ENSURE(respIt != ResolveInFlight.end(), "Unregistered requestId: " + ToString(requestId));
    respIt->second.Result.SetValue(response);
    ResolveInFlight.erase(respIt);
    RequestsInFlight.erase(requestId);
}

void TDescribeSchemaSecretsService::Bootstrap() {
    LOG_D("Bootstrap");
    Become(&TDescribeSchemaSecretsService::StateWait);
}

void TDescribeSchemaSecretsService::SaveIncomingRequestInfo(const TEvResolveSecret& ev) {
    TResponseContext ctx;
    for (size_t i = 0; i < ev.SecretNames.size(); ++i) {
        ctx.Secrets.emplace(ev.SecretNames[i], i);
        ctx.Result = ev.Promise;
    }
    ResolveInFlight[LastRequestId] = std::move(ctx);

    RequestsInFlight.emplace(LastRequestId, TRequestContext(ev.MakeCopy()));
}

void TDescribeSchemaSecretsService::SendSchemeCacheRequests(const TEvResolveSecret& ev, const ui64 requestId) {
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

    Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request), 0, requestId);
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
        LOG_N(GetLogLabel("TEvNavigateKeySetResult", requestId) << "SchemeCache error: empty response");
        FillResponse(requestId, NKqp::TEvDescribeSecretsResponse::TDescription(Ydb::StatusIds::BAD_REQUEST, { NYql::TIssue("secrets were not found") }));
        return true;
    }

    bool retryableError = false;
    TString firstUnresolvedPath;
    for (size_t i = 0; i < result.ResultSet.size(); ++i) {
        auto& entry = result.ResultSet[i];
        switch (GetSchemeCacheEntryStatus(SchemeCacheStatusGetter, entry)) {
            case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok: {
                break;
            }
            case NSchemeCache::TSchemeCacheNavigate::EStatus::LookupError: {
                retryableError = true;
                if (firstUnresolvedPath.empty()) {
                    firstUnresolvedPath = CanonizePath(entry.Path);
                }
                // we don't skip the remaining entries, since if there is a not retryable error, we should not retry anything
                break;
            }
            default: {
                // we look at the all remaining entries, to report about all failed secrets
                TVector<TString> unresolvedPaths;
                unresolvedPaths.reserve(result.ResultSet.size());
                unresolvedPaths.push_back(CanonizePath(entry.Path));
                for (size_t j = i + 1; j < result.ResultSet.size(); ++j) {
                    auto& nextEntry = result.ResultSet[j];
                    const auto status = GetSchemeCacheEntryStatus(SchemeCacheStatusGetter, nextEntry);
                    if (
                        status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok &&
                        status != NSchemeCache::TSchemeCacheNavigate::EStatus::LookupError
                    ) {
                        unresolvedPaths.push_back(CanonizePath(nextEntry.Path));
                    }
                }

                LOG_N(GetLogLabel("TEvNavigateKeySetResult", requestId) << "SchemeCache error "
                    << ToString(entry.Status) << " for " << ListSecrets(unresolvedPaths));
                FillResponse(
                    requestId,
                    NKqp::TEvDescribeSecretsResponse::TDescription(
                        Ydb::StatusIds::BAD_REQUEST,
                        { NYql::TIssue(ListSecrets(unresolvedPaths) + " not found") }));

                return true;
            }
        }
    }

    if (retryableError) {
        if (ScheduleSchemeCacheRetry(requestId, firstUnresolvedPath)) {
            return true;
        }

        // no more retries
        LOG_N(GetLogLabel("TEvNavigateKeySetResult", requestId)
            << "retry limit exceeded for secret `" + firstUnresolvedPath + "`");
        FillResponse(
            requestId,
            NKqp::TEvDescribeSecretsResponse::TDescription(
                Ydb::StatusIds::UNAVAILABLE,
                { NYql::TIssue("Retry limit exceeded for secret `" + firstUnresolvedPath + "`") }));

        return true;
    }

    return false; // no Scheme Cache errors
}

bool TDescribeSchemaSecretsService::HandleSchemeShardErrorsIfAny(
    const ui64& requestId,
    const NKikimrScheme::TEvDescribeSchemeResult& record)
{
    const auto status = GetSchemeShardPathStatus(SchemeShardStatusGetter, record);
    if (status == NKikimrScheme::EStatus::StatusSuccess) {
        return false;
    }

    const auto secretName = CanonizePath(record.GetPath());

    LOG_N(GetLogLabel("TEvDescribeSchemeResult", requestId) << "SchemeShard error: " << EStatus_Name(status));
    if (IsRetryableSchemeShardStatus(status)) {
        const auto requestIt = RequestsInFlight.find(requestId);
        Y_ENSURE(requestIt != RequestsInFlight.end(), "Unregistered requestId: " + ToString(requestId));
        if (requestIt->second.Request->Settings.RetryPolicy) {
            if (ScheduleSchemeShardRetry(requestId, secretName)) {
                return true;
            }

            LOG_N(GetLogLabel("TEvDescribeSchemeResult", requestId)
                << "retry limit exceeded for secret `" << secretName << "`");
            FillResponse(
                requestId,
                NKqp::TEvDescribeSecretsResponse::TDescription(
                    Ydb::StatusIds::UNAVAILABLE,
                    { NYql::TIssue("Retry limit exceeded for secret `" + secretName + "`") }));
            return true;
        }
    }

    const auto errorStatus = MapSchemeShardStatus(status);
    FillResponse(
        requestId,
        NKqp::TEvDescribeSecretsResponse::TDescription(
            errorStatus,
            { NYql::TIssue(MakeSchemeShardErrorMessage(status, secretName)) }));
    return true;
}

bool TDescribeSchemaSecretsService::ScheduleSchemeCacheRetry(const ui64& requestId, const TString& unresolvedSecretPath) {
    auto requestIt = RequestsInFlight.find(requestId);
    Y_ENSURE(requestIt != RequestsInFlight.end(), "Unregistered requestId: " + ToString(requestId));

    if (!requestIt->second.SchemeCacheRetryState) {
        static const auto defaultSchemeCacheRetryPolicy = MakeShortRetryPolicy();
        const auto& retryPolicy = requestIt->second.Request->Settings.RetryPolicy
            ? requestIt->second.Request->Settings.RetryPolicy
            : defaultSchemeCacheRetryPolicy;
        requestIt->second.SchemeCacheRetryState = retryPolicy->CreateRetryState();
    }

    if (const auto delay = requestIt->second.SchemeCacheRetryState->GetNextRetryDelay()) {
        LOG_N(GetLogLabel("TEvNavigateKeySetResult", requestId) << "secret `" << unresolvedSecretPath
            << "` not found. Request will be retried in: " << *delay);
        this->Schedule(*delay, new TEvResolveSecretSchemeCacheRetry(requestId));
        return true;
    }

    return false;
}

bool TDescribeSchemaSecretsService::ScheduleSchemeShardRetry(const ui64& requestId, const TString& secretPath) {
    const auto requestIt = RequestsInFlight.find(requestId);
    Y_ENSURE(requestIt != RequestsInFlight.end(), "Unregistered requestId: " + ToString(requestId));

    const auto& retryPolicy = requestIt->second.Request->Settings.RetryPolicy;
    if (!retryPolicy) {
        return false;
    }

    auto& retryState = requestIt->second.SchemeShardRetryStates[secretPath];
    if (!retryState) {
        retryState = retryPolicy->CreateRetryState();
    }

    if (const auto delay = retryState->GetNextRetryDelay()) {
        LOG_N(GetLogLabel("TEvDescribeSchemeResult", requestId) << "secret `" << secretPath
            << "` is unavailable. Request will be retried in: " << *delay);
        this->Schedule(*delay, new TEvResolveSecretSchemeShardRetry(requestId, secretPath));
        return true;
    }

    return false;
}

IRetryPolicy<>::TPtr MakeShortRetryPolicy() {
    return IRetryPolicy<>::GetExponentialBackoffPolicy(
        [](){ return ERetryErrorClass::ShortRetry; },
        /* minDelay */ TDuration::MilliSeconds(100),
        /* minLongRetryDelay */ TDuration::MilliSeconds(500),
        /* maxDelay */ TDuration::Seconds(5),
        /* maxRetries */ 10,
        /* maxTime */ TDuration::Seconds(10)
    );
}

IRetryPolicy<>::TPtr MakeLongRetryPolicy() {
    return IRetryPolicy<>::GetExponentialBackoffPolicy(
        [](){ return ERetryErrorClass::LongRetry; },
        /* minDelay */ TDuration::Seconds(1),
        /* minLongRetryDelay */ TDuration::Seconds(10),
        /* maxDelay */ TDuration::Seconds(30),
        /* maxRetries */ std::numeric_limits<size_t>::max(),
        /* maxTime */ TDuration::Minutes(60)
    );
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
            LOG_N(GetLogLabel("FillResponseIfFinished", requestId) << "secret `" << secretPath << "` was dropped during request");
            FillResponse(requestId, NKqp::TEvDescribeSecretsResponse::TDescription(Ydb::StatusIds::BAD_REQUEST, { NYql::TIssue("secret `" + secretPath + "` not found") }));
            return;
        }

        Y_ENSURE(secret.second < secretValues.size());
        secretValues[secret.second] = it->second.Value;
    }
    FillResponse(requestId, NKqp::TEvDescribeSecretsResponse::TDescription(secretValues));
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

NThreading::TFuture<NKqp::TEvDescribeSecretsResponse::TDescription> DescribeSecret(
    const TVector<TString>& secretNames,
    const TIntrusiveConstPtr<NACLib::TUserToken> userToken,
    const TString& database,
    NActors::TActorSystem* actorSystem,
    TDescribeSecretSettings settings
) {
    auto promise = NThreading::NewPromise<NKqp::TEvDescribeSecretsResponse::TDescription>();
    if (UseSchemaSecrets(AppData()->FeatureFlags, secretNames)) {
        actorSystem->Send(
            MakeDescribeSchemaSecretServiceId(actorSystem->NodeId),
            new TDescribeSchemaSecretsService::TEvResolveSecret(userToken, database, secretNames, promise, std::move(settings))
        );
        return promise.GetFuture();
    }

    actorSystem->Register(CreateDescribeSecretsActor(userToken ? userToken->GetUserSID() : "", secretNames, promise));
    return promise.GetFuture();
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

}  // namespace NKikimr::NSecret
