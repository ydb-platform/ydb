#pragma once

#include <ydb/library/grpc/client/grpc_client_low.h>
#include <util/system/hp_timer.h>
#include "mvp_log.h"
#include "appdata.h"

namespace NMVP {

template <typename TGRpcService>
class TLoggedGrpcServiceConnection {
    std::unique_ptr<NYdbGrpc::TServiceConnection<TGRpcService>> Connection;
    const TString host;
    using TStub = typename TGRpcService::Stub;

    TString Prefix() const {
        return TStringBuilder() << '[' << (unsigned long)this << "] ";
    }

    static TString Trim(const TString& line) {
        if (line.size() > 512) {
            return line.substr(0, 512) + "...(truncated)";
        }
        return line;
    }

public:
    TLoggedGrpcServiceConnection(const NYdbGrpc::TGRpcClientConfig& config, std::unique_ptr<NYdbGrpc::TServiceConnection<TGRpcService>> connection)
        : Connection(std::move(connection)), host(config.Locator)
    {
        BLOG_GRPC_D(Prefix() << "Connect to "
                    << ((config.EnableSsl || !config.SslCredentials.pem_root_certs.empty()) ? "grpcs://" : "grpc://")
                    << config.Locator);
    }

    template<typename TRequest, typename TResponse>
    void DoRequest(const TRequest& request,
                   NYdbGrpc::TResponseCallback<TResponse>&& callback,
                   typename NYdbGrpc::TSimpleRequestProcessor<TStub, TRequest, TResponse>::TAsyncRequest asyncRequest,
                   const NYdbGrpc::TCallMeta& metas = {},
                   NYdbGrpc::IQueueClientContextProvider* provider = nullptr)
    {
        const TString& requestName = request.GetDescriptor()->name();
        BLOG_GRPC_D(Prefix() << "Request " << requestName << " " << Trim(request.ShortDebugString()));
        NActors::TActorSystem* actorSystem = NActors::TlsActivationContext->ActorSystem();
        THPTimer timer;
        NYdbGrpc::TResponseCallback<TResponse> cb =
                [actorSystem, requestName, host = host, timer = std::move(timer), prefix = Prefix(), callback = std::move(callback)](NYdbGrpc::TGrpcStatus&& status, TResponse&& response) -> void {
            if (status.Ok()) {
                BLOG_GRPC_DC(*actorSystem, prefix << "Response " << response.GetDescriptor()->name() << " " << Trim(response.ShortDebugString()));
            } else {
                BLOG_GRPC_DC(*actorSystem, prefix << "Status " << status.GRpcStatusCode << " " << status.Msg);
            }
            TMVPAppData* appData = actorSystem->template AppData<TMVPAppData>();
            appData->MetricRegistry->HistogramRate({
                                    {"sensor", "time_us"},
                                    {"request", requestName},
                                    {"peer", host},
                                    {"status", std::to_string(status.GRpcStatusCode)}
                                },
                                NMonitoring::ExplicitHistogram({1, 5, 10, 50, 100, 500, 1000, 5000, 10000, 30000, 60000})
                        )->Record(TDuration::Seconds(std::abs(timer.Passed())).MicroSeconds());
            callback(std::move(status), std::move(response));
        };
        Connection->DoRequest(request, std::move(cb), std::move(asyncRequest), metas, provider);
    }

    template<typename TRequest, typename TResponse>
    void DoAdvancedRequest(const TRequest& request,
                           NYdbGrpc::TAdvancedResponseCallback<TResponse>&& callback,
                           typename NYdbGrpc::TAdvancedRequestProcessor<TStub, TRequest, TResponse>::TAsyncRequest asyncRequest,
                           const NYdbGrpc::TCallMeta& metas = { },
                           NYdbGrpc::IQueueClientContextProvider* provider = nullptr)
    {
        const TString& requestName = request.GetDescriptor()->name();
        BLOG_GRPC_D(Prefix() << "Request " << requestName << " " << Trim(request.ShortDebugString()));
        NActors::TActorSystem* actorSystem = NActors::TlsActivationContext->ActorSystem();
        THPTimer timer;
        NYdbGrpc::TAdvancedResponseCallback<TResponse> cb =
            [actorSystem, requestName, host = host, timer = std::move(timer), prefix = Prefix(), callback = std::move(callback)](const grpc::ClientContext& context, NYdbGrpc::TGrpcStatus&& status, TResponse&& response) -> void {
            if (status.Ok()) {
                BLOG_GRPC_DC(*actorSystem, prefix << "Response " << response.GetDescriptor()->name() << " " << Trim(response.ShortDebugString()));
            } else {
                BLOG_GRPC_DC(*actorSystem, prefix << "Status " << status.GRpcStatusCode << " " << status.Msg);
            }
            TMVPAppData* appData = actorSystem->template AppData<TMVPAppData>();
            appData->MetricRegistry->HistogramRate({
                                    {"sensor", "time_us"},
                                    {"request", requestName},
                                    {"peer", host},
                                    {"status", std::to_string(status.GRpcStatusCode)}
                                },
                                NMonitoring::ExplicitHistogram({1, 5, 10, 50, 100, 500, 1000, 5000, 10000, 30000, 60000})
                        )->Record(TDuration::Seconds(std::abs(timer.Passed())).MicroSeconds());
            callback(context, std::move(status), std::move(response));
        };
        Connection->DoAdvancedRequest(request, std::move(cb), std::move(asyncRequest), metas, provider);
    }
};


}
