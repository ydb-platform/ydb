#include "events.h"
#include "discovery_actor.h"

#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/cache/cache.h>

#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/string/vector.h>

namespace NKikimr::NHttpProxy {

    using namespace NActors;

    class TDiscoveryActor : public NActors::TActorBootstrapped<TDiscoveryActor> {
        using TBase = NActors::TActorBootstrapped<TDiscoveryActor>;
    public:
        explicit TDiscoveryActor(std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider, TDiscoverySettings&& settings)
            : Settings(std::move(settings))
            , CredentialsProvider(credentialsProvider)
        {
            NYdbGrpc::TGRpcClientConfig grpcConf;
            grpcConf.Locator = Settings.DiscoveryEndpoint;
            if (Settings.CaCert) {
                grpcConf.EnableSsl = true;
                grpcConf.SslCredentials.pem_root_certs = *Settings.CaCert;
            }
            Connection = GrpcClient.CreateGRpcServiceConnection<TProtoService>(grpcConf);
        }

        void Bootstrap(const TActorContext& ctx) {
            LOG_SP_INFO_S(ctx, NKikimrServices::PERSQUEUE, "discovery actor created");

            TBase::Become(&TDiscoveryActor::StateWork);
        }

        TStringBuilder LogPrefix() const {
            return TStringBuilder() << "database: " << Settings.Database << " endpoint: " << Settings.DiscoveryEndpoint;
        }
        ~TDiscoveryActor() {
            GrpcClient.Stop(true);
        }

    private:
        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvServerlessProxy::TEvListEndpointsRequest, Handle);
                HFunc(TEvServerlessProxy::TEvListEndpointsResponse, Handle);
            }
        }

        void Handle(TEvServerlessProxy::TEvListEndpointsRequest::TPtr& ev, const TActorContext& ctx);
        void Handle(TEvServerlessProxy::TEvListEndpointsResponse::TPtr& ev, const TActorContext& ctx);

        void MakeGRpcRequest(const TActorContext& ctx);

        using TProtoService = Ydb::Discovery::V1::DiscoveryService;
        using TDatabaseServiceConnection = NYdbGrpc::TServiceConnection<TProtoService>;

        TDiscoverySettings Settings;

        NYdbGrpc::TGRpcClientLow GrpcClient;
        std::unique_ptr<TDatabaseServiceConnection> Connection;
        std::shared_ptr<NYdb::ICredentialsProvider> CredentialsProvider;

        std::vector<TActorId> Waiters;

    };

    void TDiscoveryActor::Handle(TEvServerlessProxy::TEvListEndpointsRequest::TPtr& ev, const TActorContext& ctx) {
        Waiters.push_back(ev->Sender);

        if (Waiters.size() == 1) {
            MakeGRpcRequest(ctx);
        }
    }


    void TDiscoveryActor::Handle(TEvServerlessProxy::TEvListEndpointsResponse::TPtr& ev, const TActorContext& ctx) {
        for (auto& waiter : Waiters) {
            auto resp = std::make_unique<TEvServerlessProxy::TEvListEndpointsResponse>();
            resp->Status = ev->Get()->Status;
            if (resp->Status->Ok()) {
                resp->Record = std::make_unique<Ydb::Discovery::ListEndpointsResponse>();
                resp->Record->CopyFrom(*(ev->Get()->Record));
            }
            ctx.Send(waiter, resp.release());
        }
        Waiters.clear();
    }



    void TDiscoveryActor::MakeGRpcRequest(const TActorContext& ctx) {
        NYdbGrpc::TCallMeta callMeta;
        callMeta.Timeout = TDuration::Seconds(30);
        callMeta.Aux.emplace_back("x-ydb-auth-ticket", CredentialsProvider->GetAuthInfo());
        callMeta.Aux.emplace_back("x-ydb-database", Settings.Database);

        Ydb::Discovery::ListEndpointsRequest request;
        request.set_database(Settings.Database);
        LOG_SP_INFO_S(ctx, NKikimrServices::PERSQUEUE, "list endpoints request");

        NYdbGrpc::TResponseCallback<Ydb::Discovery::ListEndpointsResponse> responseCb =
                [actorSystem = ctx.ActorSystem(), actorId = ctx.SelfID](NYdbGrpc::TGrpcStatus&& status, Ydb::Discovery::ListEndpointsResponse&& response) -> void {
                    auto res = std::make_unique<TEvServerlessProxy::TEvListEndpointsResponse>();
                    LOG_INFO_S(*actorSystem, NKikimrServices::PERSQUEUE, "list endpoints result status: " << status.GRpcStatusCode << " " << status.Msg << " " << status.Details);
                    if (status.Ok()) {
                        res->Record = std::make_unique<Ydb::Discovery::ListEndpointsResponse>();
                        res->Record->CopyFrom(response);
                    }
                    res->Status = std::make_shared<NYdbGrpc::TGrpcStatus>(std::move(status));
                    actorSystem->Send(actorId, res.release());
                };
        Connection->DoRequest(request, std::move(responseCb), &Ydb::Discovery::V1::DiscoveryService::Stub::AsyncListEndpoints, callMeta);
    }

    NActors::IActor* CreateDiscoveryActor(std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider, TDiscoverySettings&& settings) {
        return new TDiscoveryActor(credentialsProvider, std::move(settings));
    }


    class TDiscoveryProxyActor : public NActors::TActorBootstrapped<TDiscoveryProxyActor> {
        using TBase = NActors::TActorBootstrapped<TDiscoveryProxyActor>;
    public:
        explicit TDiscoveryProxyActor(std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider, const NKikimrConfig::TServerlessProxyConfig& config)
            : CredentialsProvider(credentialsProvider)
            , Config(config)
        {
            TString certificate;
            if (config.GetCaCert()) {
                certificate = TFileInput(config.GetCaCert()).ReadAll();
                CaCert = certificate;
            }
        }

        void Bootstrap(const TActorContext&) {
            TBase::Become(&TDiscoveryProxyActor::StateWork);
        }

    private:
        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvServerlessProxy::TEvListEndpointsRequest, Handle);
            }
        }

        void Handle(TEvServerlessProxy::TEvListEndpointsRequest::TPtr& ev, const TActorContext& ctx);

        std::shared_ptr<NYdb::ICredentialsProvider> CredentialsProvider;
        NKikimrConfig::TServerlessProxyConfig Config;
        TMaybe<TString> CaCert;
        std::map<TString, TActorId> DiscoveryActors;
    };


    void TDiscoveryProxyActor::Handle(TEvServerlessProxy::TEvListEndpointsRequest::TPtr& ev, const TActorContext& ctx) {
        auto& endpoint = ev->Get()->Endpoint;
        if (DiscoveryActors.find(endpoint) == DiscoveryActors.end()) {
            TDiscoverySettings settings{endpoint, CaCert, ev->Get()->Database};
            DiscoveryActors[endpoint] = ctx.Register(CreateDiscoveryActor(CredentialsProvider, std::move(settings)));
        }
        ctx.Send(ev->Forward(DiscoveryActors[endpoint]));
    }


    NActors::IActor* CreateDiscoveryProxyActor(std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider, const NKikimrConfig::TServerlessProxyConfig& config) {
        return new TDiscoveryProxyActor(credentialsProvider, config);
    }
}
