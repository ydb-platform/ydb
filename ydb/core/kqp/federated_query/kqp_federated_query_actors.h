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
            const TString& secretName,
            NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> promise
        )
            : UserToken(NACLib::TUserToken{ownerUserId, TVector<NACLib::TSID>{}})
            , SecretName(secretName)
            , Promise(promise)
        {
        }

    public:
        const NACLib::TUserToken UserToken;
        const TString SecretName;
        NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> Promise;
    };

private:
    STRICT_STFUNC(StateWait,
        hFunc(TEvResolveSecret, Handle);
        hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
        hFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
        cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
    )

    void Handle(TEvResolveSecret::TPtr& ev);
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev);
    void FillResponse(const ui64 requestId, const TEvDescribeSecretsResponse::TDescription& response);

public:
    TDescribeSchemaSecretsService() = default;

    void Bootstrap();

private:
    struct TVersionedSecret {
        ui64 Version;
        TString Value;
    };

    ui64 LastCookie = 0;
    THashMap<ui64, NThreading::TPromise<TEvDescribeSecretsResponse::TDescription>> ResolveInFlight;
    THashMap<ui64, TString> SecretNameInFlight;
    THashMap<TString, TVersionedSecret> SecretNameToValue;
};

IActor* CreateDescribeSecretsActor(const TString& ownerUserId, const std::vector<TString>& secretIds, NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> promise);

void RegisterDescribeSecretsActor(const TActorId& replyActorId, const TString& ownerUserId, const std::vector<TString>& secretIds, TActorSystem* actorSystem);

NThreading::TFuture<TEvDescribeSecretsResponse::TDescription> DescribeExternalDataSourceSecrets(const NKikimrSchemeOp::TAuth& authDescription, const TString& ownerUserId, TActorSystem* actorSystem);

IActor* CreateDescribeSchemaSecretsService();

}  // namespace NKikimr::NKqp
