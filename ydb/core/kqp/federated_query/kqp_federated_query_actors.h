#pragma once

#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/aclib/aclib.h>
#include <library/cpp/threading/future/future.h>

#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

namespace NKikimr::NKqp {

class TDescribeSchemaSecretsService: public NActors::TActorBootstrapped<TDescribeSchemaSecretsService> {
public:
    enum ESecretEvents {
        EvResolveSecret = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
        EvEnd,
    };

    struct TEvResolveSecret : public NActors::TEventLocal<TEvResolveSecret, EvResolveSecret> {
    public:
        TEvResolveSecret(
            const TString& ownerUserId,
            const TVector<TString>& secretNames,
            NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> promise
        )
            : UserToken(NACLib::TUserToken{ownerUserId, TVector<NACLib::TSID>{}})
            , SecretNames(secretNames)
            , Promise(promise)
        {
        }

    public:
        const NACLib::TUserToken UserToken;
        const TVector<TString> SecretNames;
        NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> Promise;
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
        THashMap<TString, TIncomingOrderId> Secrets;
        NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> Result;
        size_t FilledSecretsCnt = 0;
    };

private:
    STRICT_STFUNC(StateWait,
        hFunc(TEvResolveSecret, HandleIncomingRequest);
        hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleSchemeCacheResponse);
        hFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, HandleSchemeShardResponse);
        cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
    )

    void HandleIncomingRequest(TEvResolveSecret::TPtr& ev);
    void HandleSchemeCacheResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
    void HandleSchemeShardResponse(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev);
    void FillResponse(const ui64& requestId, const TEvDescribeSecretsResponse::TDescription& response);
    void SaveIncomingRequestInfo(const TEvResolveSecret& req);
    void SendSchemeCacheRequests(const TVector<TString>& secretNames, const NACLib::TUserToken& userToken);
    bool LocalCacheHasActualVersion(const TVersionedSecret& secret, const ui64& cacheSecretVersion);
    bool LocalCacheHasActualObject(const TVersionedSecret& secret, const ui64& cacheSecretPathId);
    bool HandleSchemeCacheErrorsIfAny(const ui64& requestId, NSchemeCache::TSchemeCacheNavigate& result);
    void FillResponseIfFinished(const ui64& requestId, const TResponseContext& responseCtx);

public:
    TDescribeSchemaSecretsService() = default;

    void Bootstrap();

private:
    ui64 LastCookie = 0;
    THashMap<ui64, TResponseContext> ResolveInFlight;
    THashMap<TString, TVersionedSecret> VersionedSecrets;
};

IActor* CreateDescribeSecretsActor(const TString& ownerUserId, const std::vector<TString>& secretIds, NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> promise);

void RegisterDescribeSecretsActor(const TActorId& replyActorId, const TString& ownerUserId, const std::vector<TString>& secretIds, TActorSystem* actorSystem);

NThreading::TFuture<TEvDescribeSecretsResponse::TDescription> DescribeExternalDataSourceSecrets(const NKikimrSchemeOp::TAuth& authDescription, const TString& ownerUserId, TActorSystem* actorSystem);

IActor* CreateDescribeSchemaSecretsService();

}  // namespace NKikimr::NKqp
