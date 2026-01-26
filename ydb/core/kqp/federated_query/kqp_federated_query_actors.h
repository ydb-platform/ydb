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

class TDescribeSchemaSecretsService: public NActors::TActorBootstrapped<TDescribeSchemaSecretsService> {
public:
    using TRetryPolicy = IRetryPolicy<>;

    enum ESecretEvents {
        EvResolveSecret = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
        EvResolveSecretRetry,
        EvEnd,
    };

    struct TEvResolveSecret : public NActors::TEventLocal<TEvResolveSecret, EvResolveSecret> {
    public:
        TEvResolveSecret(
            const TIntrusiveConstPtr<NACLib::TUserToken> userToken,
            const TString& database,
            const TVector<TString>& secretNames,
            NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> promise
        )
            : UserToken(userToken)
            , Database(database)
            , SecretNames(secretNames)
            , Promise(promise)
        {
            Y_ENSURE(!Database.empty(), "Database name must be set in secret requests");
        }

        THolder<TEvResolveSecret> MakeCopy() const {
            return MakeHolder<TEvResolveSecret>(UserToken, Database, SecretNames, Promise);
        }

    public:
        const TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
        const TString Database;
        const TVector<TString> SecretNames;
        NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> Promise;
    };

    struct TEvResolveSecretRetry : public NActors::TEventLocal<TEvResolveSecretRetry, EvResolveSecretRetry> {
        TEvResolveSecretRetry(ui64 initialRequestId)
            : InitialRequestId(initialRequestId)
        {}

        const ui64 InitialRequestId = 0;
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
        TRetryPolicy::IRetryState::TPtr RetryState;

        TRequestContext(THolder<TEvResolveSecret> request)
            : Request(std::move(request))
        {
        }
    };

private:
    STRICT_STFUNC(StateWait,
        hFunc(TEvResolveSecret, HandleIncomingRequest);
        hFunc(TEvResolveSecretRetry, HandleIncomingRetryRequest);
        hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleSchemeCacheResponse);
        hFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, HandleSchemeShardResponse);
        hFunc(TSchemeBoardEvents::TEvNotifyDelete, HandleNotifyDelete);
        hFunc(TSchemeBoardEvents::TEvNotifyUpdate, HandleNotifyUpdate);
        cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
    )

    void HandleIncomingRequest(TEvResolveSecret::TPtr& ev);
    void HandleIncomingRetryRequest(TEvResolveSecretRetry::TPtr& ev);
    void HandleSchemeCacheResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
    void HandleSchemeShardResponse(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev);
    void HandleNotifyDelete(TSchemeBoardEvents::TEvNotifyDelete::TPtr& ev);
    void HandleNotifyUpdate(TSchemeBoardEvents::TEvNotifyUpdate::TPtr& ev);

    void FillResponse(const ui64& requestId, const TEvDescribeSecretsResponse::TDescription& response);
    void SaveIncomingRequestInfo(const TEvResolveSecret& ev);
    void SendSchemeCacheRequests(const TEvResolveSecret& ev, const ui64 requestId);
    bool LocalCacheHasActualVersion(const TVersionedSecret& secret, const ui64& cacheSecretVersion);
    bool LocalCacheHasActualObject(const TVersionedSecret& secret, const ui64& cacheSecretPathId);
    bool HandleSchemeCacheErrorsIfAny(const ui64& requestId, NSchemeCache::TSchemeCacheNavigate& result);
    void FillResponseIfFinished(const ui64& requestId, const TResponseContext& responseCtx);
    bool ScheduleSchemeCacheRetry(const ui64& requestId, const TString& unresolvedSecretPath);

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

private:
    ui64 LastRequestId = 0;
    THashMap<ui64, TRequestContext> RequestsInFlight;
    THashMap<ui64, TResponseContext> ResolveInFlight;
    THashMap<TString, TVersionedSecret> VersionedSecrets;
    THashMap<TString, TActorId> SchemeBoardSubscribers;
    ISecretUpdateListener* SecretUpdateListener = nullptr;
    ISchemeCacheStatusGetter* SchemeCacheStatusGetter = nullptr;
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
    TActorSystem* actorSystem
);

bool UseSchemaSecrets(const NKikimr::TFeatureFlags& flags, const TVector<TString>& secretNames);
bool UseSchemaSecrets(const NKikimr::TFeatureFlags& flags, const TString& secretName);

}  // namespace NKikimr::NKqp
