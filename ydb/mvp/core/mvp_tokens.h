#pragma once
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/spinlock.h>
#include <util/generic/queue.h>
#include <ydb/library/grpc/client/grpc_client_low.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/json_reader.h>
#include <ydb/mvp/core/protos/mvp.pb.h>
#include <ydb/mvp/core/core_ydb.h>
#include <ydb/public/api/client/yc_private/iam/iam_token_service.grpc.pb.h>
#include <ydb/public/api/client/nc_private/iam/token_service.grpc.pb.h>
#include <ydb/public/api/client/nc_private/iam/token_exchange_service.grpc.pb.h>
#include <ydb/public/api/protos/ydb_auth.pb.h>
#include "grpc_log.h"

namespace NMVP {

enum EAuthProfile {
    Yandex = 1,
    Nebius = 2
};

const THashMap<TString, EAuthProfile> AuthProfileByName = {
    { "yandex", EAuthProfile::Yandex },
    { "nebius", EAuthProfile::Nebius }
};

class TMvpTokenator : public NActors::TActorBootstrapped<TMvpTokenator> {
public:
    using TBase = NActors::TActorBootstrapped<TMvpTokenator>;

    static TMvpTokenator* CreateTokenator(const NMvp::TTokensConfig& tokensConfig, const NActors::TActorId& httpProxy, const NMVP::EAuthProfile authProfile = NMVP::EAuthProfile::Yandex);
    TString GetToken(const TString& name);

protected:
    friend class NActors::TActorBootstrapped<TMvpTokenator>;
    static constexpr TDuration RPC_TIMEOUT = TDuration::Seconds(10);
    static constexpr TDuration PERIODIC_CHECK = TDuration::Seconds(30);
    static constexpr TDuration SUCCESS_REFRESH_PERIOD = TDuration::Hours(1);
    static constexpr TDuration ERROR_REFRESH_PERIOD = TDuration::Hours(1);

    struct TEvPrivate {
        enum EEv {
            EvRefreshToken = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
            EvUpdateIamTokenYandex,
            EvUpdateStaticCredentialsToken,
            EvUpdateIamTokenNebius,
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

        struct TEvRefreshToken : NActors::TEventLocal<TEvRefreshToken, EvRefreshToken> {
            TString Name;

            TEvRefreshToken(const TString& name)
                : Name(name)
            {}
        };

        template <ui32 TEventType, typename TResponse>
        struct TEvUpdateToken : NActors::TEventLocal<TEvUpdateToken<TEventType, TResponse>, TEventType> {
            TString Name;
            TString Subject;
            NYdbGrpc::TGrpcStatus Status;
            TResponse Response;

            TEvUpdateToken(const TString& name, const TString& subject, NYdbGrpc::TGrpcStatus&& status, TResponse&& response)
                : Name(name)
                , Subject(subject)
                , Status(status)
                , Response(response)
            {}
        };

        using TEvUpdateIamTokenYandex = TEvUpdateToken<EvUpdateIamTokenYandex, yandex::cloud::priv::iam::v1::CreateIamTokenResponse>;
        using TEvUpdateIamTokenNebius = TEvUpdateToken<EvUpdateIamTokenNebius, nebius::iam::v1::CreateTokenResponse>;
        using TEvUpdateStaticCredentialsToken = TEvUpdateToken<EvUpdateStaticCredentialsToken, Ydb::Auth::LoginResponse>;
    };

    TMvpTokenator(NMvp::TTokensConfig tokensConfig, const NActors::TActorId& httpProxy, const EAuthProfile authProfile);
    void Bootstrap();
    void HandlePeriodic();
    void Handle(TEvPrivate::TEvRefreshToken::TPtr event);
    void Handle(TEvPrivate::TEvUpdateIamTokenYandex::TPtr event);
    void Handle(TEvPrivate::TEvUpdateIamTokenNebius::TPtr event);
    void Handle(TEvPrivate::TEvUpdateStaticCredentialsToken::TPtr event);
    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event);

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvRefreshToken, Handle);
            hFunc(TEvPrivate::TEvUpdateIamTokenYandex, Handle);
            hFunc(TEvPrivate::TEvUpdateIamTokenNebius, Handle);
            hFunc(TEvPrivate::TEvUpdateStaticCredentialsToken, Handle);
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            cFunc(NActors::TEvents::TSystem::Wakeup, HandlePeriodic);
        }
    }

    struct TTokenRefreshRecord {
        TInstant RefreshTime;
        TString Name;

        bool operator <(const TTokenRefreshRecord& o) const {
            return RefreshTime > o.RefreshTime;
        }
    };

    struct TTokenConfigs {
        THashMap<TString, NMvp::TMetadataTokenInfo> MetadataTokenConfigs;
        THashMap<TString, NMvp::TJwtInfo> JwtTokenConfigs;
        THashMap<TString, NMvp::TOAuthInfo> OauthTokenConfigs;
        THashMap<TString, NMvp::TStaticCredentialsInfo> StaticCredentialsConfigs;

        const NMvp::TMetadataTokenInfo* GetMetadataTokenConfig(const TString& name);
        const NMvp::TJwtInfo* GetJwtTokenConfig(const TString& name);
        const NMvp::TOAuthInfo* GetOAuthTokenConfig(const TString& name);
        const NMvp::TStaticCredentialsInfo* GetStaticCredentialsTokenConfig(const TString& name);

    private:
        template <typename TTokenInfo>
        const TTokenInfo* GetTokenConfig(const THashMap<TString, TTokenInfo>& configs, const TString& name) {
            auto it = configs.find(name);
            if (it != configs.end()) {
                return &it->second;
            }
            return nullptr;
        }
    };

    NYdbGrpc::TGRpcClientLow GRpcClientLow;
    TPriorityQueue<TTokenRefreshRecord> RefreshQueue;
    THashMap<TString, TString> Tokens;
    TTokenConfigs TokenConfigs;
    TSpinLock TokensLock;
    NActors::TActorId HttpProxy;
    const NMVP::EAuthProfile AuthProfile;
    THashMap<NHttp::THttpRequest*, TString> HttpRequestNames;

    template <typename TGRpcService>
    std::unique_ptr<NMVP::TLoggedGrpcServiceConnection<TGRpcService>> CreateGRpcServiceConnection(const TString& endpoint) {
        TStringBuf scheme = "grpc";
        TStringBuf host;
        TStringBuf uri;
        NHttp::CrackURL(endpoint, scheme, host, uri);
        NYdbGrpc::TGRpcClientConfig config;
        config.Locator = host;
        config.EnableSsl = (scheme == "grpcs");
        return std::unique_ptr<NMVP::TLoggedGrpcServiceConnection<TGRpcService>>(new NMVP::TLoggedGrpcServiceConnection<TGRpcService>(config, GRpcClientLow.CreateGRpcServiceConnection<TGRpcService>(config)));
    }

    template <typename TGrpcService, typename TRequest, typename TResponse, typename TUpdateToken>
    void RequestCreateToken(const TString& name, const TString& endpoint, TRequest& request, typename NYdbGrpc::TSimpleRequestProcessor<typename TGrpcService::Stub, TRequest, TResponse>::TAsyncRequest asyncRequest, const TString& subject = "") {
        NActors::TActorId actorId = SelfId();
        NActors::TActorSystem* actorSystem = NActors::TActivationContext::ActorSystem();
        NYdbGrpc::TCallMeta meta;
        meta.Timeout = RPC_TIMEOUT;
        auto connection = CreateGRpcServiceConnection<TGrpcService>(endpoint);
        NYdbGrpc::TResponseCallback<TResponse> cb =
            [actorId, actorSystem, name, subject](NYdbGrpc::TGrpcStatus&& status, TResponse&& response) -> void {
                actorSystem->Send(actorId, new TUpdateToken(name, subject, std::move(status), std::move(response)));
        };
        connection->DoRequest(request, std::move(cb), asyncRequest, meta);
    }

    void UpdateMetadataToken(const NMvp::TMetadataTokenInfo* metadataTokenInfo);
    void UpdateJwtToken(const NMvp::TJwtInfo* iwtInfo);
    void UpdateOAuthToken(const NMvp::TOAuthInfo* oauthInfo);
    void UpdateStaticCredentialsToken(const NMvp::TStaticCredentialsInfo* staticCredentialsInfo);
    void UpdateStaffApiUserToken(const NMvp::TStaffApiUserTokenInfo* staffApiUserTokenInfo);
};

}
