#pragma once

#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/scheme_board/events.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/aclib/aclib.h>

#include <library/cpp/retry/retry_policy.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/hash_multi_map.h>

namespace NKikimr::NKqp {

struct TDescribeSecretSettings {
    IRetryPolicy<>::TPtr RetryPolicy;
};

IRetryPolicy<>::TPtr MakeShortRetryPolicy();
IRetryPolicy<>::TPtr MakeLongRetryPolicy();

class TDescribeSchemaSecretsService: public NActors::TActorBootstrapped<TDescribeSchemaSecretsService> {
public:
    using TRetryPolicy = IRetryPolicy<>;

    enum ESecretEvents {
        EvResolveSecret = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
        EvResolveSecretSchemeCacheRetry,
        EvResolveSecretSchemeShardRetry,
        EvEnd,
    };

    struct TEvResolveSecret : public NActors::TEventLocal<TEvResolveSecret, EvResolveSecret> {
    public:
        TEvResolveSecret(
            const TIntrusiveConstPtr<NACLib::TUserToken> userToken,
            const TString& database,
            const TVector<TString>& secretNames,
            NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> promise,
            TDescribeSecretSettings settings = {}
        )
            : UserToken(userToken)
            , Database(database)
            , SecretNames(secretNames)
            , Promise(promise)
            , Settings(std::move(settings))
        {
            Y_ENSURE(!Database.empty(), "Database name must be set in secret requests");
        }

        THolder<TEvResolveSecret> MakeCopy() const {
            return MakeHolder<TEvResolveSecret>(UserToken, Database, SecretNames, Promise, Settings);
        }

    public:
        const TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
        const TString Database;
        const TVector<TString> SecretNames;
        NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> Promise;
        const TDescribeSecretSettings Settings;
    };

    struct TEvResolveSecretSchemeCacheRetry : public NActors::TEventLocal<TEvResolveSecretSchemeCacheRetry, EvResolveSecretSchemeCacheRetry> {
        TEvResolveSecretSchemeCacheRetry(ui64 initialRequestId)
            : InitialRequestId(initialRequestId)
        {}

        const ui64 InitialRequestId = 0;
    };

    struct TEvResolveSecretSchemeShardRetry : public NActors::TEventLocal<TEvResolveSecretSchemeShardRetry, EvResolveSecretSchemeShardRetry> {
        TEvResolveSecretSchemeShardRetry(ui64 initialRequestId, TString secretPath)
            : InitialRequestId(initialRequestId)
            , SecretPath(std::move(secretPath))
        {}

        const ui64 InitialRequestId = 0;
        const TString SecretPath;
    };

private:
    struct TVersionedSecret {
        ui64 SecretVersion = 0;
        ui64 PathId = 0;
        TString Name;
        TString Value;
    };

    struct TResponseContext {
        using TIncomingOrderId = ui64;
        THashMultiMap<TString, TIncomingOrderId> Secrets;
        NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> Result;
        size_t FilledSecretsCnt = 0;
    };

    struct TRequestContext {
        THolder<TEvResolveSecret> Request;
        // SchemeCache support batch requests, so there's a single retry state for the whole batch
        TRetryPolicy::IRetryState::TPtr SchemeCacheRetryState;
        // SchemeShard does not support batch requests, so there's a separate retry state for each secret
        THashMap<TString, TRetryPolicy::IRetryState::TPtr> SchemeShardRetryStates;

        TRequestContext(THolder<TEvResolveSecret> request)
            : Request(std::move(request))
        {
        }
    };

private:
    STRICT_STFUNC(StateWait,
        hFunc(TEvResolveSecret, HandleIncomingRequest);
        hFunc(TEvResolveSecretSchemeCacheRetry, HandleIncomingSchemeCacheRetryRequest);
        hFunc(TEvResolveSecretSchemeShardRetry, HandleIncomingSchemeShardRetryRequest);
        hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleSchemeCacheResponse);
        hFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, HandleSchemeShardResponse);
        hFunc(TSchemeBoardEvents::TEvNotifyDelete, HandleNotifyDelete);
        hFunc(TSchemeBoardEvents::TEvNotifyUpdate, HandleNotifyUpdate);
        cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
    )

    void HandleIncomingRequest(TEvResolveSecret::TPtr& ev);
    void HandleIncomingSchemeCacheRetryRequest(TEvResolveSecretSchemeCacheRetry::TPtr& ev);
    void HandleIncomingSchemeShardRetryRequest(TEvResolveSecretSchemeShardRetry::TPtr& ev);
    void HandleSchemeCacheResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
    void HandleSchemeShardResponse(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev);
    void HandleNotifyDelete(TSchemeBoardEvents::TEvNotifyDelete::TPtr& ev);
    void HandleNotifyUpdate(TSchemeBoardEvents::TEvNotifyUpdate::TPtr& ev);

    void FillResponse(const ui64& requestId, const TEvDescribeSecretsResponse::TDescription& response);
    void SaveIncomingRequestInfo(const TEvResolveSecret& ev);
    void SendSchemeCacheRequests(const TEvResolveSecret& ev, const ui64 requestId);
    void SendSchemeShardRequest(const TEvResolveSecret& ev, const ui64 requestId, const TString& secretPath);
    bool LocalCacheHasActualVersion(const TVersionedSecret& secret, const ui64& cacheSecretVersion);
    bool LocalCacheHasActualObject(const TVersionedSecret& secret, const ui64& cacheSecretPathId);
    bool HandleSchemeCacheErrorsIfAny(const ui64& requestId, NSchemeCache::TSchemeCacheNavigate& result);
    bool HandleSchemeShardErrorsIfAny(const ui64& requestId, const NKikimrScheme::TEvDescribeSchemeResult& record);
    void FillResponseIfFinished(const ui64& requestId, const TResponseContext& responseCtx);
    bool ScheduleSchemeCacheRetry(const ui64& requestId, const TString& unresolvedSecretPath);
    bool ScheduleSchemeShardRetry(const ui64& requestId, const TString& secretPath);

public:
    TDescribeSchemaSecretsService() = default;

    void Bootstrap();

public:
    // For tests only
    class ISecretUpdateListener : public TThrRefBase {
    public:
        virtual void HandleNotifyDelete(const TString& secretName) = 0;
        virtual ~ISecretUpdateListener() = default;
    };
    // For tests only
    void SetSecretUpdateListener(ISecretUpdateListener* secretUpdateListener) {
        SecretUpdateListener = secretUpdateListener;
    }

    // For tests only
    class ISchemeCacheStatusGetter : public TThrRefBase {
    public:
        virtual NSchemeCache::TSchemeCacheNavigate::EStatus GetStatus(
            NSchemeCache::TSchemeCacheNavigate::TEntry& entry) const = 0;
        virtual ~ISchemeCacheStatusGetter() = default;
    };
    // For tests only
    void SetSchemeCacheStatusGetter(ISchemeCacheStatusGetter* schemeCacheStatusGetter) {
        SchemeCacheStatusGetter = schemeCacheStatusGetter;
    }

    // For tests only
    class ISchemeShardStatusGetter : public TThrRefBase {
    public:
        virtual NKikimrScheme::EStatus GetStatus(
            const NKikimrScheme::TEvDescribeSchemeResult& record) const = 0;
        virtual ~ISchemeShardStatusGetter() = default;
    };
    // For tests only
    void SetSchemeShardStatusGetter(ISchemeShardStatusGetter* schemeShardStatusGetter) {
        SchemeShardStatusGetter = schemeShardStatusGetter;
    }

private:
    ui64 LastRequestId = 0;
    THashMap<ui64, TRequestContext> RequestsInFlight;
    THashMap<ui64, TResponseContext> ResolveInFlight;
    THashMap<TString, TVersionedSecret> VersionedSecrets;
    THashMap<TString, TActorId> SchemeBoardSubscribers;
    ISecretUpdateListener* SecretUpdateListener = nullptr;
    ISchemeCacheStatusGetter* SchemeCacheStatusGetter = nullptr;
    ISchemeShardStatusGetter* SchemeShardStatusGetter = nullptr;
};

void RegisterDescribeSecretsActor(
    const NActors::TActorId& replyActorId,
    const TIntrusiveConstPtr<NACLib::TUserToken> userToken,
    const TString& database,
    const std::vector<TString>& secretIds,
    NActors::TActorSystem* actorSystem
);

NThreading::TFuture<TEvDescribeSecretsResponse::TDescription> DescribeExternalDataSourceSecrets(
    const NKikimrSchemeOp::TAuth& authDescription,
    const TIntrusiveConstPtr<NACLib::TUserToken> userToken,
    const TString& database,
    TActorSystem* actorSystem
);

IActor* CreateDescribeSchemaSecretsService();

class IDescribeSchemaSecretsServiceFactory {
public:
    using TPtr = std::shared_ptr<IDescribeSchemaSecretsServiceFactory>;

    virtual IActor* CreateService() = 0;
    virtual ~IDescribeSchemaSecretsServiceFactory() = default;
};

class TDescribeSchemaSecretsServiceFactory : public IDescribeSchemaSecretsServiceFactory {
public:
    IActor* CreateService() override;
};

NThreading::TFuture<TEvDescribeSecretsResponse::TDescription> DescribeSecret(
    const TVector<TString>& secretNames,
    const TIntrusiveConstPtr<NACLib::TUserToken> userToken,
    const TString& database,
    TActorSystem* actorSystem,
    TDescribeSecretSettings settings = {}
);

bool UseSchemaSecrets(const NKikimr::TFeatureFlags& flags, const TVector<TString>& secretNames);
bool UseSchemaSecrets(const NKikimr::TFeatureFlags& flags, const TString& secretName);

NThreading::TFuture<TEvDescribeResourceIdResponse::TDescription> DescribeExternalDataSourceResourceId(
    const TString& endpoint,
    const TString& database,
    bool ssl,
    const TString& caCert,
    const TString& token,
    TActorSystem* actorSystem
);

IActor* CreateDescribeResourceIdServiceActor(const std::shared_ptr<NYdb::TDriver>& driver);

std::unique_ptr<NActors::IActor> NewCachingIamServiceCredentialsProviderService();

}  // namespace NKikimr::NKqp
